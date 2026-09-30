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

  for role <- [:opening, :local_resume], predecessor_transport <- ["websocket", "http_sse"], successor_transport <- ["websocket", "http_sse"] do
    @tag mailbox_general: true
    test "#{role} mailbox cuts chain from #{predecessor_transport} to #{successor_transport} without changing the original claim", %{fixture: fixture} do
      fixture = ordinary_fixture(fixture, unquote(role))
      original = admit!(fixture, fixture.payload, unquote(predecessor_transport))
      output = reasoning("ordinary-first")
      cut!(fixture, original, output)
      first_payload = append(fixture.payload, [output, mailbox(1)])
      first = admit!(fixture, first_payload, unquote(successor_transport))
      assert_edge!(original, first)
      next_output = reasoning("ordinary-second")
      cut!(fixture, first, next_output)
      expire!(original)
      second = admit!(fixture, append(first_payload, [next_output, mailbox(2)]), unquote(successor_transport))
      assert_edge!(first, second)
      assert original.correlation_id == fixture.claim
      assert counts(fixture) == %{requests: 3, attempts: 2, turns: 2, links: 2, settlements: 2}
    end
  end

  for predecessor_transport <- ["websocket", "http_sse"], successor_transport <- ["websocket", "http_sse"] do
    @tag mailbox_delivered: true
    test "a delivered #{predecessor_transport} resume admits one retained prefix with mailbox through #{successor_transport}", %{fixture: fixture} do
      original = admit!(fixture, fixture.payload, unquote(predecessor_transport))
      first = reasoning("delivered-first")
      second = reasoning("delivered-second")
      served!(fixture, original, [first, second])
      candidate = append(fixture.payload, [first, mailbox(1)])
      assert ClientRetry.verified_mailbox_continuation?(Repo.get_by!(CodexTurn, request_id: original.id), Repo.get!(Request, original.id), Repo.get_by!(Attempt, request_id: original.id), witness(fixture, candidate, unquote(successor_transport)), nil)
      assert_refused!(fixture, append(fixture.payload, [second, mailbox(1)]), :terminal_predecessor)
      assert_http_refused!(fixture, append(fixture.payload, [second, mailbox(1)]), :terminal_predecessor)
      admitted = admit!(fixture, candidate, unquote(successor_transport))
      assert_edge!(original, admitted)
      assert_refused!(fixture, candidate, :active_predecessor)
      assert Repo.get!(Request, original.id).status == "succeeded"
      assert counts(fixture) == %{requests: 2, attempts: 1, turns: 1, links: 1, settlements: 1}
    end
  end

  for transport <- ["websocket", "http_sse"] do
    test "a served #{transport} receipt still requires current authorization, full prefix evidence and the original session", %{fixture: fixture} do
      original = admit!(fixture, fixture.payload, unquote(transport))
      output = reasoning("served-negative")
      served!(fixture, original, [output])
      candidate = append(fixture.payload, [output, mailbox(1)])
      options = options(fixture, candidate, "websocket")
      witness = %{options.native_client_retry_witness | auth_epoch: options.native_client_retry_witness.auth_epoch + 1}
      assert_refused_opts!(fixture, %{options | native_client_retry_witness: witness}, :terminal_predecessor)
      other = insert_session!(fixture.setup)
      assert_refused_opts!(fixture, %{options | codex_session: other}, :terminal_predecessor)
      assert_refused!(fixture, append(fixture.payload, [mailbox(1)]), :terminal_predecessor)
      call = %{"type" => "function_call", "call_id" => "call_synthetic", "name" => "synthetic_tool", "arguments" => "{}"}
      assert_refused!(fixture, append(fixture.payload, [output, call, mailbox(1)]), :terminal_predecessor)
      attempt = Repo.get_by!(Attempt, request_id: original.id)
      attempt |> Ecto.Changeset.change(response_metadata: %{}) |> Repo.update!()
      assert_refused!(fixture, candidate, :terminal_predecessor)
    end

    test "a served #{transport} mailbox continuation keeps the current predecessor's retry window", %{fixture: fixture} do
      original = admit!(fixture, fixture.payload, unquote(transport))
      output = reasoning("served-expired")
      served!(fixture, original, [output])
      expire!(original)
      assert_refused!(fixture, append(fixture.payload, [output, mailbox(1)]), :retry_expired)
      assert_http_refused!(fixture, append(fixture.payload, [output, mailbox(1)]), :retry_expired)
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

  test "an HTTP consumed prefix requires its persisted prefix proof", %{fixture: fixture} do
    original = admit!(fixture, fixture.payload, "http_sse")
    first = reasoning("first")
    second = reasoning("second")
    cut!(fixture, original, first)
    progress = ClientRetry.new_native_http_progress() |> ClientRetry.observe_native_http_output_item(first) |> ClientRetry.observe_native_http_output_item(second)
    attempt = Repo.get_by!(Attempt, request_id: original.id)
    metadata = Map.put(attempt.response_metadata, "native_http_resume_progress", ClientRetry.native_http_progress_metadata(progress))
    attempt |> Ecto.Changeset.change(response_metadata: metadata) |> Repo.update!()
    candidate = append(fixture.payload, [Map.put(first, "content", nil), mailbox(1)])
    assert_http_refused!(fixture, candidate, :terminal_predecessor)
    metadata = Map.put(metadata, "native_http_mailbox_prefix", ClientRetry.native_http_mailbox_prefix_metadata(progress))
    attempt |> Ecto.Changeset.change(response_metadata: metadata) |> Repo.update!()
    successor = admit!(fixture, candidate, "http_sse")
    assert_edge!(original, successor)
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
    metadata = if transport == "http_sse", do: %{"native_http_claim_arm" => Map.get(fixture, :arm, "post_compaction_resume"), "native_http_input_count" => length(payload["input"])}, else: %{}

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
        "http_sse" -> if Map.get(fixture, :arm) == "opening", do: WebsocketTurnIdentity.replay_claim_digest(fixture.semantic, payload), else: WebsocketTurnIdentity.http_resume_input_digest(fixture.semantic, payload["input"])
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

  defp ordinary_fixture(fixture, role) do
    input = [hd(fixture.payload["input"])]
    input = if role == :local_resume, do: input ++ [%{"type" => "message", "role" => "user", "content" => "synthetic local summary"}], else: input
    payload = Map.put(fixture.payload, "input", input)
    claim = if role == :local_resume, do: WebsocketTurnIdentity.steered_claim_key(fixture.semantic, NativeTurnContinuation.turn_progress(payload, 2)), else: "codex-turn:" <> Base.url_encode64(fixture.semantic, padding: false)
    Map.merge(fixture, %{payload: payload, claim: claim, arm: "opening"})
  end

  defp served!(fixture, request, outputs) do
    now = db_now()

    digests =
      Enum.map(outputs, fn output ->
        {:ok, digest} = WebsocketTurnIdentity.completed_item_digest(output)
        digest
      end)

    receipt = %{"outcome" => "delivered", "terminal_class" => "response.completed", "highest_frame_class" => "terminal", "completed_items" => length(outputs), "completed_item_digests" => digests}
    progress = Enum.reduce(outputs, ClientRetry.new_native_http_progress(), &ClientRetry.observe_native_http_output_item(&2, &1))
    metadata = %{"downstream_delivery" => receipt, "native_http_resume_progress" => ClientRetry.native_http_progress_metadata(progress), "native_http_mailbox_prefix" => ClientRetry.native_http_mailbox_prefix_metadata(progress)}
    assert {:ok, attempt} = Accounting.create_attempt(request, fixture.setup.assignment, %{transport: request.transport})
    assert {:ok, _} = Accounting.finalize_success(request, attempt, %{status: "measured", source: "synthetic", input_tokens: 10, output_tokens: 2}, %{attempt_metadata: metadata})
    Repo.insert!(%CodexTurn{codex_session_id: fixture.session.id, request_id: request.id, turn_sequence: 1, transport_kind: request.transport, semantic_turn_digest: fixture.semantic, status: "succeeded", final_attempt_id: attempt.id, first_visible_output_at: now, started_at: now, completed_at: now, created_at: now, updated_at: now})
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
