defmodule CodexPooler.Access.APIKeys.TouchDebounceTransientDatabaseTest do
  # `TouchDebounce` is a direct child of the application supervisor, whose
  # restart budget every child shares. A flush that met a transient database
  # failure used to crash it and drop every pending touch (findings#294); it
  # now keeps the keys it could not write for the next interval. PostgreSQL
  # raises the failure itself from a trigger on this test's own keys, inside
  # the test's sandbox transaction. A private instance under the test
  # supervisor (never restarted) carries the flushes, so the application's
  # instance is not involved.
  use CodexPooler.DataCase, async: false

  import ExUnit.CaptureLog
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Access.APIKey
  alias CodexPooler.Access.APIKeys.TouchDebounce
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  test "a flush that meets a transient database failure keeps the failed touch pending, stays alive and writes it once the database answers" do
    %{api_key: failing} = active_api_key_fixture()
    %{api_key: other} = active_api_key_fixture()
    server = start_private_debounce!()
    touched_at = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    fail_touches!(failing, "query_canceled")

    TouchDebounce.touch(failing, touched_at, server)
    TouchDebounce.touch(other, touched_at, server)

    {result, logs} = with_log([level: :warning], fn -> TouchDebounce.flush(server) end)

    assert result == {:error, :deferred}
    assert Process.alive?(server)
    assert logs =~ "api key touch flush deferred after a transient database failure pending_keys="
    assert logs =~ "reason_class=postgres_query_canceled"

    # The failed key stays pending with the timer armed again; the other key
    # was either written before the failure or is still pending, never lost.
    %{pending: pending, timer_ref: timer_ref} = :sys.get_state(server)
    assert Map.fetch!(pending, failing.id) == touched_at
    assert is_reference(timer_ref)
    assert Repo.get!(APIKey, failing.id).last_used_at == nil
    assert Map.has_key?(pending, other.id) != (Repo.get!(APIKey, other.id).last_used_at == touched_at)

    allow_touches!()
    assert TouchDebounce.flush(server) == :ok
    assert %{pending: pending} = :sys.get_state(server)
    assert pending == %{}
    assert Repo.get!(APIKey, failing.id).last_used_at == touched_at
    assert Repo.get!(APIKey, other.id).last_used_at == touched_at
  end

  test "a failure no retry can fix still crashes the flush" do
    %{api_key: key} = active_api_key_fixture()
    server = start_private_debounce!()
    monitor = Process.monitor(server)
    fail_touches!(key, "check_violation")

    TouchDebounce.touch(key, DateTime.utc_now() |> DateTime.truncate(:microsecond), server)

    capture_log(fn ->
      assert {{%Postgrex.Error{postgres: %{code: :check_violation}}, _stacktrace}, _call} = catch_exit(TouchDebounce.flush(server))
    end)

    assert_receive {:DOWN, ^monitor, :process, ^server, {%Postgrex.Error{}, _stacktrace}}, 5_000
  end

  # A long interval keeps the timer from flushing on its own during the test.
  defp start_private_debounce! do
    server = start_supervised!(Supervisor.child_spec({TouchDebounce, name: __MODULE__.Server, debounce_interval_ms: 3_600_000}, restart: :temporary))
    Sandbox.allow(Repo, self(), server)
    server
  end

  defp fail_touches!(%APIKey{id: id}, errcode) do
    Repo.query!("CREATE FUNCTION touch_debounce_test_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic touch failure' USING ERRCODE = '#{errcode}'; END $$")
    Repo.query!("CREATE TRIGGER touch_debounce_test_gate BEFORE UPDATE ON api_keys FOR EACH ROW WHEN (NEW.id = '#{id}'::uuid) EXECUTE FUNCTION touch_debounce_test_gate()")
    :ok
  end

  defp allow_touches! do
    Repo.query!("DROP TRIGGER touch_debounce_test_gate ON api_keys")
    :ok
  end
end
