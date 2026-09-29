defmodule CodexPooler.Accounting.MailboxResumeChainTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, ClientRetry, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.Gateway.Payloads.{NativeMailboxContinuation, NativeTurnContinuation, RequestOptions, WebsocketTurnIdentity}
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Repo

  @endpoint "/backend-api/codex/responses"

  setup do
    setup = accounting_setup()
    session = insert_session!(setup)
    semantic = :crypto.strong_rand_bytes(32)
    payload = payload(setup.model.exposed_model_id)
    {:post_compaction_resume, anchor} = NativeTurnContinuation.turn_role(payload)
    claim = WebsocketTurnIdentity.resume_claim_key(semantic, anchor)
    %{fixture: %{setup: setup, session: session, semantic: semantic, claim: claim, payload: payload}}
  end

  for successor_transport <- ["websocket", "http_sse"] do
    test "two mailbox cuts traverse historical edges through #{successor_transport} successors", %{fixture: fixture} do
      original = admit!(fixture, fixture.payload, "websocket")
      first_output = reasoning("first")
      cut!(fixture, original, first_output)
      first_payload = append(fixture.payload, [first_output | Enum.map(1..5, &mailbox/1)])
      first = admit!(fixture, first_payload, unquote(successor_transport))
      assert_edge!(original, first)

      second_output = reasoning("second")
      cut!(fixture, first, second_output)
      expire!(original)
      second_payload = append(first_payload, [second_output, mailbox(6)])
      second = admit!(fixture, second_payload, unquote(successor_transport))
      assert_edge!(first, second)

      assert original.correlation_id == fixture.claim
      assert claim_for(fixture, first_payload) == fixture.claim
      assert claim_for(fixture, second_payload) == fixture.claim
      assert counts(fixture) == %{requests: 3, attempts: 2, turns: 2, links: 2, settlements: 2}
      assert Repo.get!(Request, original.id).request_metadata == original.request_metadata
      refute Map.has_key?(original.request_metadata, "mailbox")
      assert Repo.get!(Request, first.id).native_client_retry_digest == witness(fixture, first_payload, unquote(successor_transport)).digest
    end
  end

  test "changed mailbox output stays fenced while settled identical resends chain", %{fixture: fixture} do
    original = admit!(fixture, fixture.payload, "websocket")
    output = reasoning("first")
    cut!(fixture, original, output)

    assert_refused!(fixture, append(fixture.payload, [reasoning("changed"), mailbox(1)]), :terminal_predecessor)
    assert_http_refused!(fixture, append(fixture.payload, [reasoning("changed"), mailbox(1)]), :terminal_predecessor)

    candidate = append(fixture.payload, [output, mailbox(1)])
    successor = admit!(fixture, candidate, "websocket")
    assert_edge!(original, successor)
    assert_refused!(fixture, candidate, :active_predecessor)
    cut!(fixture, successor, reasoning("second"))
    repeated = admit!(fixture, candidate, "websocket")
    assert_edge!(successor, repeated)
    assert_refused!(fixture, candidate, :active_predecessor)
    cut!(fixture, repeated, reasoning("third"))
    # This fixture's HTTP resume witness uses a different projection from its
    # websocket witness; the mismatch remains refused.
    assert_http_refused!(fixture, candidate, :terminal_predecessor)
  end

  for transport <- ["websocket", "http_sse"] do
    test "identical interrupted resume chains over #{transport}", %{fixture: fixture} do
      original = admit!(fixture, fixture.payload, unquote(transport))
      cut!(fixture, original, reasoning("first"))
      successor = admit!(fixture, fixture.payload, unquote(transport))
      assert_edge!(original, successor)
      if unquote(transport) == "websocket", do: assert_refused!(fixture, fixture.payload, :active_predecessor)
    end
  end

  test "a historical mailbox end must equal the successor's actual stored witness", %{fixture: fixture} do
    original = admit!(fixture, fixture.payload, "websocket")
    output = reasoning("first")
    cut!(fixture, original, output)
    first_payload = append(fixture.payload, [output, mailbox(1)])
    first = admit!(fixture, first_payload, "websocket")
    next_output = reasoning("second")
    cut!(fixture, first, next_output)

    altered_history = append(fixture.payload, [output, mailbox(2), next_output, mailbox(3)])
    assert_refused!(fixture, altered_history, :terminal_predecessor)
    assert_edge!(original, first)
  end

  for live_row <- [:request, :attempt, :turn] do
    test "a live #{live_row} retains the duplicate fence", %{fixture: fixture} do
      original = admit!(fixture, fixture.payload, "websocket")
      output = reasoning("first")
      cut!(fixture, original, output)
      make_live!(original, unquote(live_row))
      assert_refused!(fixture, append(fixture.payload, [output, mailbox(1)]), :active_predecessor)
    end
  end

  test "the current predecessor's expired retry window retains the fence", %{fixture: fixture} do
    original = admit!(fixture, fixture.payload, "websocket")
    output = reasoning("first")
    cut!(fixture, original, output)
    expire!(original)
    assert_refused!(fixture, append(fixture.payload, [output, mailbox(1)]), :retry_expired)
  end

  test "changed authorization epoch and session cannot redeem a mailbox continuation", %{fixture: fixture} do
    original = admit!(fixture, fixture.payload, "websocket")
    output = reasoning("first")
    cut!(fixture, original, output)
    candidate = append(fixture.payload, [output, mailbox(1)])
    opts = options(fixture, candidate, "websocket")
    changed_epoch = %{opts.native_client_retry_witness | auth_epoch: opts.native_client_retry_witness.auth_epoch + 1}
    assert_refused_opts!(fixture, %{opts | native_client_retry_witness: changed_epoch}, :terminal_predecessor)
    other_session = insert_session!(fixture.setup)
    assert_refused_opts!(fixture, %{opts | codex_session: other_session}, :terminal_predecessor)
  end

  test "a foreign successor link retains the fence", %{fixture: fixture} do
    original = admit!(fixture, fixture.payload, "websocket")
    output = reasoning("first")
    cut!(fixture, original, output)
    foreign_fixture = %{fixture | claim: "codex-turn:" <> Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)}
    foreign = admit!(foreign_fixture, fixture.payload, "websocket")
    ClientRetry.insert_link!(original, foreign, db_now())
    assert_refused!(fixture, append(fixture.payload, [output, mailbox(1)]), :terminal_predecessor)
  end

  defp admit!(fixture, payload, "websocket") do
    opts = options(fixture, payload, "websocket")
    assert {:ok, %{request: claim}} = Accounting.claim_websocket_turn(fixture.setup.auth, fixture.setup.model, opts)
    assert {:ok, %{request: request}} = Accounting.reserve(fixture.setup.auth, fixture.setup.model, payload, Map.put(opts, :turn_claim, claim))
    request
  end

  defp admit!(fixture, payload, "http_sse") do
    assert {:ok, %{request: request}} = Accounting.reserve(fixture.setup.auth, fixture.setup.model, payload, options(fixture, payload, "http_sse"))
    request
  end

  defp options(fixture, payload, transport) do
    metadata = if transport == "http_sse", do: %{"native_http_claim_arm" => "post_compaction_resume", "native_http_input_count" => length(payload["input"])}, else: %{}

    %{
      endpoint: @endpoint,
      transport: transport,
      correlation_id: fixture.claim,
      codex_session: fixture.session,
      requested_model: fixture.setup.model.exposed_model_id,
      native_client_retry_witness: witness(fixture, payload, transport),
      native_http_input_count: length(payload["input"]),
      native_http_semantic_turn_key: fixture.semantic,
      request_metadata: metadata,
      reservation_estimate: %{input_tokens: 10, output_tokens: 10}
    }
  end

  defp witness(fixture, payload, transport) do
    {:ok, digest} =
      case transport do
        "websocket" -> WebsocketTurnIdentity.replay_claim_digest(fixture.semantic, payload)
        "http_sse" -> WebsocketTurnIdentity.http_resume_input_digest(fixture.semantic, payload["input"])
      end

    ClientRetry.original_witness!(digest, fixture.setup.api_key.runtime_revocation_epoch)
    |> NativeMailboxContinuation.attach(fixture.semantic, payload, RequestOptions.build(%{}, @endpoint, %{}))
  end

  defp cut!(fixture, request, output) do
    now = db_now()
    {:ok, digest} = WebsocketTurnIdentity.completed_item_digest(output)
    receipt = %{"outcome" => "aborted", "terminal_class" => "none", "highest_frame_class" => "item_done", "completed_items" => 1, "completed_item_digests" => [digest]}
    progress = ClientRetry.new_native_http_progress() |> ClientRetry.observe_native_http_output_item(output) |> ClientRetry.native_http_progress_metadata()
    assert {:ok, attempt} = Accounting.create_attempt(request, fixture.setup.assignment, %{transport: request.transport})
    assert {:ok, _finalized} = Accounting.finalize_failure(request, attempt, %{last_error_code: "client_disconnected", response_status_code: 499, usage: %{status: "usage_unknown", source: "client_disconnected"}, attempt_metadata: %{"downstream_delivery" => receipt, "native_http_resume_progress" => progress}})
    sequence = Repo.one(from turn in CodexTurn, where: turn.codex_session_id == ^fixture.session.id, select: coalesce(max(turn.turn_sequence), 0)) + 1

    Repo.insert!(%CodexTurn{
      codex_session_id: fixture.session.id,
      request_id: request.id,
      turn_sequence: sequence,
      transport_kind: request.transport,
      semantic_turn_digest: fixture.semantic,
      status: "interrupted",
      error_code: "client_disconnected",
      final_attempt_id: attempt.id,
      first_visible_output_at: now,
      started_at: now,
      completed_at: now,
      created_at: now,
      updated_at: now
    })
  end

  defp assert_refused!(fixture, payload, disposition), do: assert_refused_opts!(fixture, options(fixture, payload, "websocket"), disposition)

  defp assert_refused_opts!(fixture, opts, disposition) do
    before = counts(fixture)
    assert {:error, %{code: :duplicate_request, resend_disposition: ^disposition}} = Accounting.claim_websocket_turn(fixture.setup.auth, fixture.setup.model, opts)
    assert counts(fixture) == before
  end

  defp assert_http_refused!(fixture, payload, disposition) do
    before = counts(fixture)
    assert {:error, %{code: :duplicate_request, resend_disposition: ^disposition}} = Accounting.reserve(fixture.setup.auth, fixture.setup.model, payload, options(fixture, payload, "http_sse"))
    assert counts(fixture) == before
  end

  defp assert_edge!(predecessor, successor) do
    assert {:ok, successor.correlation_id} == ClientRetry.deterministic_failed_predecessor_claim(predecessor.correlation_id, predecessor.id)
    assert successor.request_metadata["client_resend"]["predecessor_request_id"] == predecessor.id
    assert Repo.exists?(from link in RequestClientRetryLink, where: link.predecessor_request_id == ^predecessor.id and link.successor_request_id == ^successor.id)
  end

  defp counts(fixture) do
    requests = from request in Request, where: request.pool_id == ^fixture.setup.pool.id, select: request.id

    %{
      requests: Repo.aggregate(requests, :count),
      attempts: Repo.aggregate(from(a in Attempt, where: a.request_id in subquery(requests)), :count),
      turns: Repo.aggregate(from(t in CodexTurn, where: t.codex_session_id == ^fixture.session.id), :count),
      links: Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id in subquery(requests)), :count),
      settlements: Repo.aggregate(from(l in LedgerEntry, where: l.request_id in subquery(requests) and l.entry_kind == "settlement"), :count)
    }
  end

  defp make_live!(request, :request), do: Repo.update_all(from(r in Request, where: r.id == ^request.id), set: [status: "in_progress", completed_at: nil])
  defp make_live!(request, :attempt), do: Repo.update_all(from(a in Attempt, where: a.request_id == ^request.id), set: [status: "in_progress", completed_at: nil])
  defp make_live!(request, :turn), do: Repo.update_all(from(t in CodexTurn, where: t.request_id == ^request.id), set: [status: "in_progress", completed_at: nil])

  defp expire!(request) do
    expired = DateTime.add(db_now(), -31, :second)
    Repo.update_all(from(r in Request, where: r.id == ^request.id), set: [completed_at: expired])
    Repo.update_all(from(a in Attempt, where: a.request_id == ^request.id), set: [completed_at: expired])
    Repo.update_all(from(t in CodexTurn, where: t.request_id == ^request.id), set: [completed_at: expired])
  end

  defp insert_session!(setup) do
    now = db_now()
    Repo.insert!(%CodexSession{pool_id: setup.pool.id, api_key_id: setup.api_key.id, session_key: "mailbox-chain-#{System.unique_integer([:positive, :monotonic])}", pool_upstream_assignment_id: setup.assignment.id, status: "active", created_at: now, updated_at: now})
  end

  defp claim_for(fixture, payload) do
    {:post_compaction_resume, anchor} = NativeTurnContinuation.turn_role(payload)
    WebsocketTurnIdentity.resume_claim_key(fixture.semantic, anchor)
  end

  defp payload(model), do: %{"type" => "response.create", "model" => model, "stream" => true, "input" => [%{"type" => "message", "role" => "user", "content" => "synthetic"}, %{"type" => "compaction", "encrypted_content" => "synthetic-pivot"}], "client_metadata" => %{"x-codex-turn-metadata" => %{"turn_id" => "synthetic-turn", "request_kind" => "turn", "agent_name" => "/root"}}}
  defp append(payload, items), do: Map.update!(payload, "input", &(&1 ++ items))
  defp reasoning(id), do: %{"type" => "reasoning", "id" => "rs_" <> id, "summary" => [], "encrypted_content" => "synthetic-reasoning-" <> id}
  defp mailbox(id), do: %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update #{id}"}]}

  defp db_now do
    %{rows: [[now]]} = Repo.query!("SELECT clock_timestamp()", [])
    DateTime.truncate(now, :microsecond)
  end
end
