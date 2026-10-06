defmodule CodexPooler.Accounting.NativeContentFilterRetryTest do
  use CodexPooler.DataCase, async: false
  import Ecto.Query

  import CodexPooler.AccountingTestSupport
  import CodexPooler.PoolerFixtures, only: [request_fixture: 2, attempt_fixture: 3]

  alias CodexPooler.Accounting.{ClientRetry, NativeContentFilterRetry, Request, RequestClientRetryLink}
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Repo

  test "strict metadata rejects old, partial, unknown and extra-field authority" do
    terminal = %{"version" => 1, "event_type" => "response.incomplete", "reason" => "content_filter"}
    assert CodexPooler.Accounting.sanitize_metadata(%{"native_content_filter_terminal" => terminal}) == %{"native_content_filter_terminal" => terminal}

    for invalid <- [nil, "synthetic", [], Map.put(terminal, "version", 2), Map.put(terminal, "extra", "synthetic"), Map.delete(terminal, "reason")] do
      assert CodexPooler.Accounting.sanitize_metadata(%{"native_content_filter_terminal" => invalid}) == %{"native_content_filter_terminal" => %{}}
    end

    for key <- ["native_content_filter_source", "native_content_filter_binding"], invalid <- [nil, "synthetic", [], %{"version" => 1}, %{"version" => 2}] do
      assert CodexPooler.Accounting.sanitize_metadata(%{key => invalid}) == %{key => %{}}
    end

    original = %{"version" => 1, "digest" => Base.url_encode64(:crypto.hash(:sha256, "synthetic original"), padding: false)}
    assert CodexPooler.Accounting.sanitize_metadata(%{"native_content_filter_original" => original}) == %{"native_content_filter_original" => original}

    for invalid <- [nil, "synthetic", [], Map.put(original, "version", 2), Map.put(original, "extra", true), Map.put(original, "digest", "invalid")] do
      assert CodexPooler.Accounting.sanitize_metadata(%{"native_content_filter_original" => invalid}) == %{"native_content_filter_original" => %{}}
    end
  end

  test "PostgreSQL binding is mandatory after reservation and checked again at attempt insertion" do
    setup = accounting_setup()
    now = db_now()
    predecessor = request_fixture(setup, %{model_id: setup.model.id, requested_model: setup.model.exposed_model_id, transport: "websocket"})
    attempt = attempt_fixture(predecessor, setup.assignment, %{upstream_model_id: setup.model.upstream_model_id})
    source = %{"version" => 1, "attempt_id" => attempt.id, "assignment_id" => setup.assignment.id, "identity_id" => setup.identity.id, "credential_epoch" => 1, "serving_mode" => "full", "requested_model" => setup.model.exposed_model_id, "effective_model" => setup.model.exposed_model_id, "upstream_model" => setup.model.upstream_model_id}
    terminal = %{"version" => 1, "event_type" => "response.incomplete", "reason" => "content_filter"}
    attempt |> Ecto.Changeset.change(response_metadata: %{"native_content_filter_source" => source, "native_content_filter_terminal" => terminal}) |> Repo.update!()
    session = Repo.insert!(%CodexSession{pool_id: setup.pool.id, api_key_id: setup.api_key.id, session_key: Ecto.UUID.generate(), status: "active", created_at: now, updated_at: now})
    turn = ClientRetry.insert_successor_turn!(session, predecessor, :crypto.hash(:sha256, "synthetic"), now)
    turn |> Ecto.Changeset.change(status: "succeeded", completed_at: now, final_attempt_id: attempt.id) |> Repo.update!()
    successor = request_fixture(setup, %{model_id: setup.model.id, requested_model: setup.model.exposed_model_id, transport: "websocket", status: "in_progress", completed_at: nil, request_metadata: %{"effective_model" => setup.model.exposed_model_id, "client_resend" => %{"predecessor_shape" => "content_filter_retry"}, "native_content_filter_binding" => source}})
    ClientRetry.insert_link!(predecessor, successor, now)
    scope = %{assignment_id: setup.assignment.id, identity_id: setup.identity.id, credential_epoch: 1, serving_mode: "full", effective_model: setup.model.exposed_model_id, upstream_model: setup.model.upstream_model_id}
    assert NativeContentFilterRetry.dispatch_allowed?(successor, scope)

    for metadata <- [Map.delete(successor.request_metadata, "native_content_filter_binding"), put_in(successor.request_metadata, ["native_content_filter_binding", "version"], 2), Map.drop(successor.request_metadata, ["native_content_filter_binding", "client_resend"])] do
      updated = Repo.get!(Request, successor.id) |> Ecto.Changeset.change(request_metadata: metadata) |> Repo.update!()
      refute NativeContentFilterRetry.dispatch_allowed?(updated, scope)
      assert {:error, %{code: :invalid_content_filter_retry_binding}} = CodexPooler.Accounting.create_attempt(updated, setup.assignment, %{model: setup.model, response_metadata: %{"routing" => %{"model_serving_mode" => "full"}}})
      refute Repo.get_by(CodexPooler.Accounting.Attempt, request_id: successor.id)
    end

    Repo.get!(Request, successor.id) |> Ecto.Changeset.change(request_metadata: successor.request_metadata) |> Repo.update!()
    changed_model = setup.model |> Ecto.Changeset.change(upstream_model_id: "synthetic-changed-mapping") |> Repo.update!()
    assert {:error, %{code: :invalid_content_filter_retry_binding}} = CodexPooler.Accounting.create_attempt(successor, setup.assignment, %{model: changed_model, response_metadata: %{"routing" => %{"model_serving_mode" => "full"}}})
    refute Repo.get_by(CodexPooler.Accounting.Attempt, request_id: successor.id)
    Repo.get!(CodexPooler.Catalog.Model, setup.model.id) |> Ecto.Changeset.change(upstream_model_id: setup.model.upstream_model_id) |> Repo.update!()
    assert {:ok, accepted} = CodexPooler.Accounting.create_attempt(successor, setup.assignment, %{model: setup.model, response_metadata: %{"routing" => %{"model_serving_mode" => "full"}}})
    assert accepted.pool_upstream_assignment_id == setup.assignment.id
    assert accepted.upstream_model_id == setup.model.upstream_model_id
  end

  # A guided retry refused before any attempt because its bound account was not eligible gives up its claim and its client-retry link, so the client's next retry chains onto the content-filtered request again (findings#318). The release is narrow: a row with an attempt, a request chained onto it, another refusal, no valid binding or another resend shape keeps both, and no row loses its accounting.
  test "only a bound guided retry refused 503 before any attempt gives up its claim and link" do
    setup = accounting_setup()
    now = db_now()
    source = %{"version" => 1, "attempt_id" => Ecto.UUID.generate(), "assignment_id" => setup.assignment.id, "identity_id" => setup.identity.id, "credential_epoch" => 1, "serving_mode" => "full", "requested_model" => setup.model.exposed_model_id, "effective_model" => setup.model.exposed_model_id, "upstream_model" => setup.model.upstream_model_id}
    assert NativeContentFilterRetry.bound_assignment(%Request{request_metadata: %{"native_content_filter_binding" => source}}) == {setup.assignment.id, setup.identity.id}

    for invalid <- [nil, %{}, Map.put(source, "version", 2), Map.delete(source, "identity_id"), Map.put(source, "assignment_id", "synthetic")] do
      assert NativeContentFilterRetry.bound_assignment(%Request{request_metadata: %{"native_content_filter_binding" => invalid}}) == nil
    end

    assert NativeContentFilterRetry.bound_assignment(%Request{request_metadata: %{}}) == nil

    refused = fn changes ->
      predecessor = request_fixture(setup, %{model_id: setup.model.id, requested_model: setup.model.exposed_model_id, transport: "websocket"})
      metadata = %{"client_resend" => %{"predecessor_request_id" => predecessor.id, "reason" => "failed_predecessor", "predecessor_shape" => "content_filter_retry"}, "native_content_filter_binding" => source}
      attrs = Map.merge(%{model_id: setup.model.id, requested_model: setup.model.exposed_model_id, transport: "websocket", status: "failed", response_status_code: 503, last_error_code: "no_eligible_backend", correlation_id: "codex-request-retry:synthetic-#{System.unique_integer([:positive])}", request_metadata: metadata}, Map.drop(changes, [:request_metadata]))
      attrs = Map.update!(attrs, :request_metadata, &Map.merge(&1, Map.get(changes, :request_metadata, %{})))
      successor = request_fixture(setup, attrs)
      ClientRetry.insert_link!(predecessor, successor, now)
      {predecessor, successor}
    end

    {predecessor, successor} = refused.(%{})
    assert :ok = NativeContentFilterRetry.release_refused_retry(successor)
    released = Repo.get!(Request, successor.id)
    assert released.request_metadata["released_turn_claim"] == successor.correlation_id
    assert released.correlation_id != successor.correlation_id
    assert {released.status, released.response_status_code, released.last_error_code, released.completed_at} == {successor.status, successor.response_status_code, successor.last_error_code, successor.completed_at}
    refute Repo.exists?(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^predecessor.id or l.successor_request_id == ^successor.id)

    # A later request of the turn that inherited the pin (findings#318 row 318-2) is released the same way.
    pin = %{"version" => 1, "assignment_id" => setup.assignment.id, "identity_id" => setup.identity.id}
    {pinned_predecessor, pinned} = refused.(%{request_metadata: %{"client_resend" => %{"predecessor_request_id" => Ecto.UUID.generate(), "reason" => "failed_predecessor"}, "native_content_filter_binding" => %{}, "native_content_filter_pin" => pin}})
    assert :ok = NativeContentFilterRetry.release_refused_retry(pinned)
    assert Repo.get!(Request, pinned.id).request_metadata["released_turn_claim"] == pinned.correlation_id
    refute Repo.exists?(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^pinned_predecessor.id)

    kept = [
      refused.(%{response_status_code: 409, last_error_code: "invalid_content_filter_retry_binding"}),
      refused.(%{status: "succeeded", response_status_code: 200, last_error_code: nil}),
      refused.(%{request_metadata: %{"native_content_filter_binding" => %{}}}),
      refused.(%{request_metadata: %{"native_content_filter_binding" => %{}, "native_content_filter_pin" => Map.put(pin, "version", 2)}}),
      refused.(%{request_metadata: %{"client_resend" => %{"predecessor_shape" => "content_filter_retry"}}})
    ]

    {attempted_predecessor, attempted} = refused.(%{})
    attempt_fixture(attempted, setup.assignment, %{upstream_model_id: setup.model.upstream_model_id})
    {chained_predecessor, chained} = refused.(%{})
    ClientRetry.insert_link!(chained, request_fixture(setup, %{model_id: setup.model.id, transport: "websocket"}), now)

    for {predecessor, successor} <- kept ++ [{attempted_predecessor, attempted}, {chained_predecessor, chained}] do
      assert :ok = NativeContentFilterRetry.release_refused_retry(successor)
      assert Repo.get!(Request, successor.id).correlation_id == successor.correlation_id
      assert Repo.exists?(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^predecessor.id and l.successor_request_id == ^successor.id)
    end
  end

  # The routing-only pin a later request of a guided retry's turn inherits keeps one exact shape, routes like the binding and is never read by dispatch (findings#318 row 318-2).
  test "the inherited pin keeps its exact shape and passes from a pinned request to its successor" do
    setup = accounting_setup()
    pin = %{"version" => 1, "assignment_id" => setup.assignment.id, "identity_id" => setup.identity.id}
    assert CodexPooler.Accounting.sanitize_metadata(%{"native_content_filter_pin" => pin}) == %{"native_content_filter_pin" => pin}

    for invalid <- [nil, "synthetic", [], %{}, Map.put(pin, "version", 2), Map.put(pin, "extra", true), Map.delete(pin, "identity_id"), Map.put(pin, "assignment_id", "synthetic")] do
      assert CodexPooler.Accounting.sanitize_metadata(%{"native_content_filter_pin" => invalid}) == %{"native_content_filter_pin" => %{}}
      assert NativeContentFilterRetry.pinned_assignment(%Request{request_metadata: %{"native_content_filter_pin" => invalid}}) == nil
    end

    source = %{"version" => 1, "attempt_id" => Ecto.UUID.generate(), "assignment_id" => setup.assignment.id, "identity_id" => setup.identity.id, "credential_epoch" => 1, "serving_mode" => "full", "requested_model" => setup.model.exposed_model_id, "effective_model" => setup.model.exposed_model_id, "upstream_model" => setup.model.upstream_model_id}
    bound = request_fixture(setup, %{request_metadata: %{"native_content_filter_binding" => source}})
    pinned = request_fixture(setup, %{request_metadata: %{"native_content_filter_pin" => pin}})
    plain = request_fixture(setup, %{request_metadata: %{}})
    expected = {setup.assignment.id, setup.identity.id}
    assert NativeContentFilterRetry.pinned_assignment(bound) == expected
    assert NativeContentFilterRetry.pinned_assignment(pinned) == expected
    assert NativeContentFilterRetry.pinned_assignment(plain) == nil
    assert NativeContentFilterRetry.put_successor_pin(%{"client_resend" => %{}}, bound.id) == %{"client_resend" => %{}, "native_content_filter_pin" => pin}
    assert NativeContentFilterRetry.put_successor_pin(%{}, pinned.id) == %{"native_content_filter_pin" => pin}
    assert NativeContentFilterRetry.put_successor_pin(%{}, plain.id) == %{}
    assert NativeContentFilterRetry.put_successor_pin(%{}, nil) == %{}

    # Dispatch reads only the binding: a request with nothing but the pin is not required to carry one.
    scope = %{assignment_id: Ecto.UUID.generate(), identity_id: Ecto.UUID.generate(), credential_epoch: 1, serving_mode: "full", effective_model: setup.model.exposed_model_id, upstream_model: setup.model.upstream_model_id}
    assert NativeContentFilterRetry.dispatch_allowed?(pinned, scope)
  end

  defp db_now do
    %{rows: [[timestamp]]} = Repo.query!("SELECT clock_timestamp()")
    DateTime.from_naive!(timestamp, "Etc/UTC")
  end
end
