defmodule CodexPoolerWeb.Runtime.HttpSessionStartPinSupport do
  @moduledoc false

  # The native HTTP session-start scenario of findings#324, shared by the
  # single-node and two-node modules. The thread's previous session served on
  # account P while the model listed only P, and its owner lease lapsed;
  # account B then joined the model and the Pool switched to
  # `least_recent_success`, so a request routed without a session preference
  # deterministically prefers B (no success yet) and one that recreates the
  # session prefers P (its predecessor's account). Requests carry the released
  # client's HTTP shape (rust-v0.160.1): the thread as `session-id` and
  # `thread-id`, the window as `x-codex-window-id`, the turn document in the
  # body and as `x-codex-turn-metadata`. Rows are committed (the callers run
  # the sandbox in auto mode), and the Pool graph is removed by
  # `register_unboxed_pool_cleanup!/1`.

  import Ecto.Query
  import ExUnit.Assertions
  import ExUnit.Callbacks

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [
      gateway_setup: 2,
      gateway_upstream: 4,
      native_text_input: 1,
      prime_routing_quota!: 1,
      put_model_source_assignments!: 2,
      register_unboxed_pool_cleanup!: 1,
      start_public_endpoint!: 0,
      start_upstream: 1,
      use_routing_strategy!: 3
    ]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession}
  alias CodexPooler.Repo

  @path "/backend-api/codex/responses"
  # Detection budget for HTTP shutdown and PostgreSQL settlement; successful
  # paths finish on socket and row signals.
  @budget 15_000

  @doc """
  The Pool, its two accounts and the thread's lapsed previous session on P.
  `upstreams` names each account's FakeUpstream entries after the
  predecessor's; `keep_session?: true` leaves the previous session live.
  """
  def session_start!(mode, upstreams, opts \\ []) do
    upstream_p = start_upstream(FakeUpstream.strict_sequence([completed_sse("resp_pin_predecessor") | Keyword.fetch!(upstreams, :p)]))
    upstream_b = start_upstream(FakeUpstream.strict_sequence(Keyword.fetch!(upstreams, :b)))
    setup = gateway_setup(upstream_p, compact?: true)
    register_unboxed_pool_cleanup!(setup)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    b = gateway_upstream(setup.pool, upstream_b, "synthetic-session-start-b-token", compact?: true)
    prime_routing_quota!(b.identity)

    s = %{
      setup: Map.put(setup, :serving_mode, mode),
      port: start_public_endpoint!(),
      thread: Ecto.UUID.generate(),
      p: setup.assignment,
      b: b.assignment,
      upstream_p: upstream_p,
      upstream_b: upstream_b
    }

    assert {200, body} = post!(s, "turn-predecessor")
    assert body =~ "response.completed"
    assert [predecessor] = await_settled!(s, 1)
    assert served_account(predecessor) == s.p.id

    model = put_model_source_assignments!(setup.model, [setup.assignment, b.assignment])
    use_routing_strategy!(setup.pool, "least_recent_success", 2)
    unless Keyword.get(opts, :keep_session?, false), do: expire_owner_lease!(predecessor.request_metadata["codex_session_id"])
    %{s | setup: %{s.setup | model: model}}
  end

  @doc """
  The session's next request followed the account its first request was
  served on: same recreated session, served by P, routed by the session's pin.
  """
  def assert_followed!(s) do
    [predecessor, first, second] = await_settled!(s, 3)
    assert first.request_metadata["routing"]["session_preference_kind"] == "recreated"
    assert served_account(first) == s.p.id
    refute first.request_metadata["codex_session_id"] == predecessor.request_metadata["codex_session_id"]
    assert second.request_metadata["codex_session_id"] == first.request_metadata["codex_session_id"]
    assert {served_account(second), FakeUpstream.count(s.upstream_b)} == {s.p.id, 0}
    assert second.request_metadata["routing"]["session_preference_kind"] == "pinned"
    assert second.request_metadata["routing"]["session_preference_status"] == "applied"
  end

  # provenance: synthetic_adversarial; invented lifecycles over the native
  # Responses SSE events the released client parses (a reasoning output item,
  # the terminal, a provider failure), held where a scenario needs the Pooler

  def completed_sse(id), do: FakeUpstream.sse_stream([{"response.completed", completed_event(id)}])

  @doc "Serves a reasoning item, then holds before the terminal."
  def held_output(gate, id), do: {:gated_terminal_sse, [reasoning_chunk(id)], [event(completed_event(id))], self(), gate}

  @doc "Holds after the headers, before any output."
  def held_before_output(gate, id), do: {:gated_terminal_sse, [], [reasoning_chunk(id), event(completed_event(id))], self(), gate}

  @doc "Serves the output and the terminal, then holds before the end of stream."
  def held_after_terminal(gate, id), do: {:gated_terminal_sse, [reasoning_chunk(id) <> event(completed_event(id))], ["data: [DONE]\n\n"], self(), gate}

  @doc "Serves a reasoning item, holds, then a delta large enough to fail the write to a client that reset."
  def cut_output(gate, id) do
    tail = event(%{"type" => "response.reasoning_text.delta", "delta" => String.duplicate("synthetic", 10_000)})
    {:gated_terminal_sse, [reasoning_chunk(id)], [tail], self(), gate}
  end

  @doc "A provider failure as the first event (`server_error`, retryable before output)."
  def refused_first_event(id),
    do: FakeUpstream.sse_stream([{"response.failed", %{"type" => "response.failed", "response" => %{"id" => id, "status" => "failed", "error" => %{"code" => "server_error", "message" => "synthetic provider failure"}}}}])

  defp completed_event(id), do: %{"type" => "response.completed", "response" => %{"id" => id, "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 6, "output_tokens" => 2, "total_tokens" => 8}}}

  defp reasoning_chunk(id) do
    item = %{"type" => "reasoning", "id" => "rs_" <> id, "summary" => [], "encrypted_content" => "synthetic-encrypted-" <> id}
    event(%{"type" => "response.output_item.done", "output_index" => 0, "item" => item})
  end

  defp event(data), do: "event: #{data["type"]}\ndata: " <> CodexPooler.JSON.encode!(data) <> "\n\n"

  def post!(s, turn), do: post!(s.port, s, turn)

  def post!(port, s, turn) do
    {conn, ref} = start_request!(port, s, turn)

    try do
      receive_all!(conn, ref, nil, "")
    after
      Mint.HTTP.close(conn)
    end
  end

  def start_request!(port, s, turn) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :passive)
    document = turn_document(s, turn)

    payload = %{
      "model" => s.setup.model.exposed_model_id,
      "input" => native_text_input("synthetic session start " <> turn),
      "stream" => true,
      "store" => false,
      "client_metadata" => %{"x-codex-turn-metadata" => document}
    }

    headers = [
      {"authorization", s.setup.authorization},
      {"content-type", "application/json"},
      {"accept", "text/event-stream"},
      {"session-id", s.thread},
      {"thread-id", s.thread},
      {"x-codex-window-id", "#{s.thread}:0"},
      {"x-codex-turn-metadata", document},
      {"originator", "Codex Desktop"}
    ]

    headers = if s.setup.serving_mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"} | headers], else: headers
    {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", @path, headers, CodexPooler.JSON.encode!(payload))
    {conn, ref}
  end

  defp turn_document(s, turn) do
    CodexPooler.JSON.encode!(%{
      "session_id" => s.thread,
      "thread_id" => s.thread,
      "turn_id" => turn,
      "root_turn_id" => turn,
      "window_id" => "#{s.thread}:0",
      "window_number" => 0,
      "request_kind" => "turn"
    })
  end

  @doc "A request on its own connection, read up to its first output item."
  def open_first!(s, turn, port \\ nil) do
    {conn, ref} = start_request!(port || s.port, s, turn)
    on_exit(fn -> Mint.HTTP.close(conn) end)
    {conn, item} = receive_first_item!(conn, ref, "")
    {conn, ref, item}
  end

  @doc """
  The released Codex Desktop cuts a stream for pending input once it holds
  output: the first output item, then a reset connection.
  """
  def consume_and_cut!(s, turn, gate, port \\ nil) do
    {conn, _ref, _item} = open_first!(s, turn, port)
    handler = await_gate!(gate)
    :ok = :inet.setopts(Mint.HTTP.get_socket(conn), linger: {true, 0})
    Mint.HTTP.close(conn)
    handler
  end

  def await_gate!(gate) do
    assert_receive {:fake_upstream_gate, :before_terminal, handler, ^gate}, @budget
    on_exit(fn -> send(handler, {:fake_upstream_release_gate, gate}) end)
    handler
  end

  def release_gate(handler, gate), do: send(handler, {:fake_upstream_release_gate, gate})

  defp receive_first_item!(conn, ref, acc) do
    {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)
    bytes = data_bytes(responses, ref, acc)

    if String.contains?(bytes, "\n\n") do
      [block | _unconsumed] = String.split(bytes, "\n\n")
      [data] = for "data: " <> json <- String.split(block, "\n"), do: json
      assert %{"type" => "response.output_item.done", "item" => item} = CodexPooler.JSON.decode!(data)
      {conn, item}
    else
      receive_first_item!(conn, ref, bytes)
    end
  end

  def receive_until!(conn, ref, marker, acc \\ "") do
    {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)
    bytes = data_bytes(responses, ref, acc)
    if String.contains?(bytes, marker), do: conn, else: receive_until!(conn, ref, marker, bytes)
  end

  defp data_bytes(responses, ref, acc) do
    Enum.reduce(responses, acc, fn
      {:data, ^ref, data}, bytes -> bytes <> data
      _other, bytes -> bytes
    end)
  end

  def receive_all!(conn, ref, status, body) do
    {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)

    {status, body, done} =
      Enum.reduce(responses, {status, body, false}, fn
        {:status, ^ref, s}, {_, b, d} -> {s, b, d}
        {:data, ^ref, data}, {s, b, d} -> {s, b <> data, d}
        {:done, ^ref}, {s, b, _} -> {s, b, true}
        _other, acc -> acc
      end)

    if done, do: {status, body}, else: receive_all!(conn, ref, status, body)
  end

  def start_client!(fun) do
    supervisor = start_supervised!({Task.Supervisor, []}, id: make_ref())
    task = Task.Supervisor.async_nolink(supervisor, fun)
    {task, Process.monitor(task.pid)}
  end

  def await_client!({task, monitor}) do
    result = Task.await(task, @budget)
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, @budget
    result
  end

  def requests(s), do: Repo.all(from(r in Request, where: r.pool_id == ^s.setup.pool.id, order_by: [asc: r.admitted_at]))

  def await_settled!(s, count), do: await_settled!(s, count, System.monotonic_time(:millisecond) + @budget)

  defp await_settled!(s, count, deadline) do
    rows = requests(s)

    cond do
      length(rows) == count and Enum.all?(rows, &(&1.completed_at != nil)) ->
        rows

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("expected #{count} settled requests, saw #{inspect(Enum.map(rows, &{&1.status, &1.last_error_code}))}")

      true ->
        Process.sleep(10)
        await_settled!(s, count, deadline)
    end
  end

  @doc "The pin of the live session the thread's window resolves to."
  def session_pin(s) do
    Repo.one!(
      from(session in CodexSession,
        where: session.pool_id == ^s.setup.pool.id and session.status in ["active", "interrupted"],
        order_by: [desc: session.created_at],
        limit: 1,
        select: session.pool_upstream_assignment_id
      )
    )
  end

  def served_account(%Request{id: id}) do
    Repo.one!(from(a in Attempt, where: a.request_id == ^id, order_by: [desc: a.attempt_number], limit: 1, select: a.pool_upstream_assignment_id))
  end

  def attempt_trail(%Request{id: id}),
    do: Repo.all(from(a in Attempt, where: a.request_id == ^id, order_by: [asc: a.attempt_number], select: {a.pool_upstream_assignment_id, a.status}))

  defp expire_owner_lease!(session_id) do
    expired_at = DateTime.utc_now() |> DateTime.add(-1, :second) |> DateTime.truncate(:microsecond)
    Repo.update_all(from(s in CodexSession, where: s.id == ^session_id), set: [owner_lease_expires_at: expired_at])
    Repo.update_all(from(l in BridgeOwnerLease, where: l.codex_session_id == ^session_id and l.status == "active"), set: [expires_at: expired_at])
  end

  @doc """
  Forwards every session claim statement naming one of the scenario's accounts
  to the test, with the rows it changed. The handler runs in the process that
  made the statement, on this node.
  """
  def watch_claims!(s) do
    id = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(id) end)
    accounts = %{Ecto.UUID.dump!(s.p.id) => :p, Ecto.UUID.dump!(s.b.id) => :b}
    :ok = :telemetry.attach(id, [:codex_pooler, :repo, :query], &__MODULE__.forward_claim/4, %{test: self(), accounts: accounts})
  end

  @doc false
  def forward_claim(_event, _measurements, %{query: query} = metadata, %{test: test, accounts: accounts}) do
    with true <- String.starts_with?(query, ~s(UPDATE "codex_sessions")) and String.contains?(query, ~s("pool_upstream_assignment_id" IS NULL)),
         [assignment | _rest] <- List.wrap(metadata[:params]),
         {:ok, account} <- Map.fetch(accounts, assignment),
         {:ok, %{num_rows: rows}} <- metadata[:result] do
      send(test, {:session_claim, account, rows})
    end

    :ok
  end
end
