defmodule CodexPooler.Accounting.NativeResampledCompletion do
  @moduledoc false

  # A Codex client samples a turn again when the response it read completed
  # with `end_turn: false` or input is pending (`session/turn.rs` `run_turn`;
  # every stable release since 0.156.0). Over HTTP SSE the next request of the
  # turn is the previous request's input, then that response's completed output
  # items, then the harness items the turn loop recorded in between
  # (`Gateway.Payloads.NativeContinuationTail`). Without a tool result or a user
  # message it derives the same payload-independent claim as the request it
  # follows (`codex-turn:`, a steered `codex-resume:`, or the post-compaction
  # `codex-resume:`), so the fence answered `409 duplicate_turn` where the
  # websocket form of the same request (an anchored frame with an empty delta)
  # is served (findings#311).
  #
  # Its bytes are those of a client's grown retry of a cut or failed request;
  # only the predecessor's state tells them apart. This proof therefore admits
  # it only after a settled, succeeded native HTTP SSE request whose own receipt
  # says it delivered `response.completed`, and only when the request is that
  # predecessor's input (an input-only digest, so turn metadata the client
  # fills late does not move it), followed by exactly the completed items the
  # predecessor wrote (all of them, in order, through the client's completed
  # item identity), followed by an allowed tail. Every input is persisted
  # (counts, digests and receipts on the rows), so any node can decide it. The
  # response's `end_turn` class is never read: what the client does next is
  # decided by the request it sends.

  alias CodexPooler.Accounting.{Attempt, ClientRetry, Request}
  alias CodexPooler.Gateway.Payloads.{NativeContinuationTail, WebsocketTurnIdentity}
  alias CodexPooler.Gateway.Persistence.CodexTurn

  @claim_arms ["opening", "steered_continuation", "post_compaction_resume"]
  # The native HTTP receipt keeps at most four completed-item identities
  # (`ClientRetry.native_http_mailbox_prefix_metadata/1`); a longer response
  # cannot be proved item by item and keeps the fence.
  @max_output_items 4

  @type stage :: :settlement | :authorization | :session | :delivery | :count | :witness | :output | :tail | :verified

  @type result :: %{
          required(:stage) => stage(),
          optional(:output_items) => non_neg_integer(),
          optional(:tail) => NativeContinuationTail.refusal()
        }

  @typedoc """
  The successor's side, all of it in the request being reserved: its input, its
  semantic turn key, the input count of the edge being proved (the request's own
  length, or the recorded count of the predecessor's existing successor), its
  session and its sealed witness.
  """
  @type request_side :: %{
          required(:input) => [term()],
          required(:semantic_turn_key) => <<_::256>>,
          required(:validation_count) => non_neg_integer() | :missing,
          required(:codex_session_id) => Ecto.UUID.t() | nil,
          required(:witness) => ClientRetry.OriginalWitness.t() | nil
        }

  @doc "The claim arms whose requests can be re-sampled: the turn's opener, a steered continuation and a post-compaction resume."
  @spec claim_arms() :: [String.t()]
  def claim_arms, do: @claim_arms

  @doc """
  The input-only witness an opening or steered native HTTP request records
  beside its `native_http_input_count`: `WebsocketTurnIdentity.http_resume_input_digest/2`
  of its input, the digest a post-compaction resume already stores as its
  sealed witness. The whole-frame witness binds the turn metadata document,
  which the client rebuilds for every request.
  """
  @spec input_witness_metadata(<<_::256>>) :: %{required(String.t()) => 1 | String.t()}
  def input_witness_metadata(<<_::256>> = digest), do: %{"version" => 1, "digest" => Base.url_encode64(digest, padding: false)}

  @doc "Keeps exactly the recorded shape of `input_witness_metadata/1`; anything else is dropped whole."
  @spec sanitize_input_witness(term()) :: map()
  def sanitize_input_witness(%{"version" => 1, "digest" => digest} = value) when map_size(value) == 2 and is_binary(digest) and byte_size(digest) == 43 do
    case Base.url_decode64(digest, padding: false) do
      {:ok, <<_::256>>} -> value
      _invalid -> %{}
    end
  end

  def sanitize_input_witness(_value), do: %{}

  @doc """
  The furthest stage the re-sample proof reaches for `request` (with its turn
  and final attempt) as the predecessor of the request described by
  `request_side`; `%{stage: :verified}` admits it. Session, authorization,
  lineage, live work, entitlements and the retry window stay the caller's
  checks too (`FailedPredecessorResend`).
  """
  @spec check(term(), term(), term(), request_side()) :: result()
  def check(turn, request, attempt, %{input: input} = side) when is_list(input) do
    with :ok <- settled(turn, request, attempt),
         :ok <- authorized(request, side.witness),
         :ok <- same_session(turn, side.codex_session_id),
         :ok <- delivered(attempt),
         {:ok, previous_count, digests} <- counted(request, attempt, input, side.validation_count),
         :ok <- prefix(request, input, previous_count, side.semantic_turn_key),
         :ok <- output(input, previous_count, digests) do
      tail(input, previous_count + length(digests), side.validation_count)
    else
      {:refused, result} -> result
    end
  end

  def check(_turn, _request, _attempt, _side), do: %{stage: :count}

  defp settled(
         %CodexTurn{request_id: id, status: "succeeded", transport_kind: "http_sse", final_attempt_id: attempt_id, completed_at: %DateTime{}},
         %Request{id: id, status: "succeeded", transport: "http_sse", endpoint: "/backend-api/codex/responses", completed_at: %DateTime{}, request_metadata: %{"native_http_claim_arm" => arm}},
         %Attempt{id: attempt_id, request_id: id, status: "succeeded", transport: "http_sse", replay_generation: 0, completed_at: %DateTime{}}
       )
       when is_binary(id) and is_binary(attempt_id) and arm in @claim_arms,
       do: :ok

  defp settled(_turn, _request, _attempt), do: {:refused, %{stage: :settlement}}

  defp authorized(%Request{native_client_retry_auth_epoch: epoch} = request, %ClientRetry.OriginalWitness{version: 1, auth_epoch: epoch}) do
    if ClientRetry.original_witness_eligible?(request), do: :ok, else: {:refused, %{stage: :authorization}}
  end

  defp authorized(_request, _witness), do: {:refused, %{stage: :authorization}}

  defp same_session(%CodexTurn{codex_session_id: session_id}, session_id) when is_binary(session_id), do: :ok
  defp same_session(_turn, _session_id), do: {:refused, %{stage: :session}}

  # The receipt is merged after the request settled, so a re-sample that met
  # the settlement before it refuses here and the client's next retry is
  # admitted.
  defp delivered(%Attempt{response_metadata: %{"downstream_delivery" => %{"outcome" => "delivered", "terminal_class" => "response.completed"}}}), do: :ok
  defp delivered(_attempt), do: {:refused, %{stage: :delivery}}

  defp counted(%Request{request_metadata: metadata}, %Attempt{response_metadata: attempt_metadata}, input, validation_count) do
    with previous_count when is_integer(previous_count) and previous_count >= 0 <- metadata["native_http_input_count"],
         %{"version" => 1, "output_item_done_count" => items, "item_digests" => digests} when is_integer(items) and items in 1..@max_output_items and is_list(digests) and length(digests) == items <-
           attempt_metadata["native_http_mailbox_prefix"],
         count when is_integer(count) and count >= previous_count + items <- validation_count,
         true <- length(input) >= count do
      {:ok, previous_count, digests}
    else
      _unproved -> {:refused, count_refusal(attempt_metadata)}
    end
  end

  # How many completed items the predecessor wrote, so a refusal above the
  # bound tells how far the next change would have to reach.
  defp count_refusal(%{"native_http_resume_progress" => %{"output_item_done_count" => items}}) when is_integer(items) and items >= 0, do: %{stage: :count, output_items: items}
  defp count_refusal(_metadata), do: %{stage: :count}

  defp prefix(request, input, previous_count, semantic_turn_key) do
    with <<_::256>> = stored <- stored_input_digest(request),
         {:ok, digest} <- WebsocketTurnIdentity.http_resume_input_digest(semantic_turn_key, Enum.take(input, previous_count)),
         true <- Plug.Crypto.secure_compare(stored, digest) do
      :ok
    else
      _mismatch -> {:refused, %{stage: :witness}}
    end
  end

  # A post-compaction resume already seals its input-only digest; an opener
  # and a steered continuation record it beside their input count.
  defp stored_input_digest(%Request{request_metadata: %{"native_http_claim_arm" => "post_compaction_resume"}, native_client_retry_digest: <<_::256>> = digest}), do: digest

  defp stored_input_digest(%Request{request_metadata: %{"native_http_input_witness" => %{"version" => 1, "digest" => encoded}}}) when is_binary(encoded) do
    case Base.url_decode64(encoded, padding: false) do
      {:ok, <<_::256>> = digest} -> digest
      _invalid -> nil
    end
  end

  defp stored_input_digest(_request), do: nil

  # Every delivered item, in order, none changed: a completed response wrote
  # all of them, so anything less is not a re-sample of it.
  defp output(input, previous_count, digests) do
    observed = input |> Enum.slice(previous_count, length(digests)) |> Enum.map(&WebsocketTurnIdentity.completed_item_digest/1)

    if observed == Enum.map(digests, &{:ok, &1}), do: :ok, else: {:refused, %{stage: :output}}
  end

  defp tail(input, from, count) do
    case NativeContinuationTail.check(Enum.slice(input, from, count - from)) do
      :ok -> %{stage: :verified}
      {:error, refusal} -> %{stage: :tail, tail: refusal}
    end
  end
end
