defmodule CodexPooler.Gateway.Persistence.MailboxSessionAuthorityTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession, CodexTurn, SessionContinuity}

  setup do
    %{pool: pool, api_key: key} = active_api_key_fixture()
    auth = %{pool: pool, api_key: key}
    options = RequestOptions.for_websocket(%{session_header: "sample-authority-#{System.unique_integer([:positive, :monotonic])}"})
    {:ok, previous} = SessionContinuity.start_codex_session(auth, options)
    deadline = DateTime.add(db_now(), -1, :second)
    Repo.update_all(from(s in CodexSession, where: s.id == ^previous.id), set: [owner_lease_expires_at: deadline])
    Repo.update_all(from(l in BridgeOwnerLease, where: l.codex_session_id == ^previous.id), set: [expires_at: deadline])
    {:ok, current} = SessionContinuity.start_codex_session(auth, options)
    previous = Repo.get!(CodexSession, previous.id)
    assert previous.close_reason == "owner_lease_expired"
    %{auth: auth, previous: previous, current: current, scope: %{pool_id: pool.id, api_key_id: key.id}}
  end

  test "locked real expiry close and fresh replacement establish authority", ctx do
    assert verdict(ctx) == :expired_replacement
  end

  test "equal ids preserve old proof semantics without adding replacement authority", ctx do
    assert :same_session = SessionContinuity.mailbox_session_verdict(ctx.previous.id, ctx.previous.id, %{})
  end

  test "case folding is evaluated by PostgreSQL on the stored canonical keys", ctx do
    Repo.update_all(from(s in CodexSession, where: s.id == ^ctx.current.id), set: [session_key: String.upcase(ctx.current.session_key)])
    assert verdict(ctx) == :expired_replacement
  end

  test "replacement creation equal to the close time is allowed", ctx do
    Repo.update_all(from(s in CodexSession, where: s.id == ^ctx.current.id), set: [created_at: ctx.previous.closed_at])
    assert verdict(ctx) == :expired_replacement
  end

  test "different-session authority requires actual coordinator-held rows", ctx do
    assert {:ok, :rejected} = Repo.transaction(fn -> SessionContinuity.mailbox_session_verdict(ctx.previous.id, ctx.current.id, ctx.scope) end)
  end

  for mutation <- [:legacy_reason, :caller_forgery, :old_token_close, :earlier_creation, :different_key, :closed_replacement, :missing_deadline, :missing_previous, :wrong_pool_scope, :wrong_key_scope] do
    @tag mailbox_replacement_negative: true
    test "#{mutation} cannot establish authority", ctx do
      ctx = mutate(ctx, unquote(mutation))
      assert verdict(ctx) == :rejected
    end
  end

  @tag mailbox_replacement_negative: true
  test "active unrelated replacement work refuses authority until it completes", ctx do
    request = request_fixture(ctx.auth, %{status: "in_progress", completed_at: nil})
    now = db_now()
    turn = Repo.insert!(%CodexTurn{codex_session_id: ctx.current.id, request_id: request.id, turn_sequence: 1, transport_kind: "http_sse", status: "in_progress", started_at: now, created_at: now, updated_at: now})
    assert verdict(ctx) == :rejected
    Repo.update!(Ecto.Changeset.change(turn, status: "succeeded", completed_at: db_now()))
    assert verdict(ctx) == :expired_replacement
  end

  @tag mailbox_replacement_negative: true
  test "a foreign persisted key is rejected before requiring its unheld session", ctx do
    %{api_key: other_key} = active_api_key_fixture(ctx.auth.pool)
    Repo.update_all(from(s in CodexSession, where: s.id == ^ctx.previous.id), set: [api_key_id: other_key.id])
    assert {:ok, :rejected} = SessionContinuity.mailbox_admission_transaction(fn -> [ctx.current.id] end, fn -> SessionContinuity.mailbox_session_verdict(ctx.previous.id, ctx.current.id, ctx.scope) end, :unexpected_rediscovery)
  end

  defp verdict(ctx) do
    {:ok, verdict} = SessionContinuity.mailbox_admission_transaction(fn -> Repo.all(from s in CodexSession, where: s.id in ^[ctx.previous.id, ctx.current.id], select: s.id) end, fn -> SessionContinuity.mailbox_session_verdict(ctx.previous.id, ctx.current.id, ctx.scope) end, :unexpected_rediscovery)
    verdict
  end

  defp mutate(ctx, :legacy_reason) do
    update_previous(ctx, close_reason: nil)
    ctx
  end

  defp mutate(ctx, :caller_forgery) do
    update_previous(ctx, close_reason: nil)
    %{ctx | scope: Map.merge(ctx.scope, %{close_reason: "owner_lease_expired", closed_at: ctx.previous.closed_at, replacement?: true})}
  end

  defp mutate(ctx, :old_token_close) do
    update_previous(ctx, owner_lease_token: Ecto.UUID.generate())
    assert is_nil(Repo.get!(CodexSession, ctx.previous.id).close_reason)
    ctx
  end

  defp mutate(ctx, :earlier_creation) do
    Repo.update_all(from(s in CodexSession, where: s.id == ^ctx.current.id), set: [created_at: DateTime.add(ctx.previous.closed_at, -1, :microsecond)])
    ctx
  end

  defp mutate(ctx, :different_key) do
    Repo.update_all(from(s in CodexSession, where: s.id == ^ctx.current.id), set: [session_key: ctx.current.session_key <> "-unrelated"])
    ctx
  end

  defp mutate(ctx, :closed_replacement) do
    Repo.update_all(from(s in CodexSession, where: s.id == ^ctx.current.id), set: [status: "closed", closed_at: db_now(), close_reason: nil])
    ctx
  end

  defp mutate(ctx, :missing_deadline) do
    update_previous(ctx, owner_lease_expires_at: nil, owner_lease_token: nil, owner_instance_id: nil, owner_instance_boot_id: nil, last_heartbeat_at: nil)
    ctx
  end

  defp mutate(ctx, :missing_previous), do: %{ctx | previous: %{ctx.previous | id: Ecto.UUID.generate()}}
  defp mutate(ctx, :wrong_pool_scope), do: %{ctx | scope: %{ctx.scope | pool_id: Ecto.UUID.generate()}}
  defp mutate(ctx, :wrong_key_scope), do: %{ctx | scope: %{ctx.scope | api_key_id: Ecto.UUID.generate()}}

  defp update_previous(ctx, attributes), do: Repo.update_all(from(s in CodexSession, where: s.id == ^ctx.previous.id), set: attributes)
  defp db_now, do: Repo.query!("SELECT clock_timestamp()").rows |> hd() |> hd()
end
