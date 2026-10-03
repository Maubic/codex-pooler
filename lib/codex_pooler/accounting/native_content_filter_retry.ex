defmodule CodexPooler.Accounting.NativeContentFilterRetry do
  @moduledoc false

  import Ecto.Query

  alias CodexPooler.Accounting.{Attempt, ClientRetry, Request, RequestClientRetryLink}
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing

  @terminal %{"version" => 1, "event_type" => "response.incomplete", "reason" => "content_filter"}

  @spec put_original_metadata(map(), term()) :: map()
  def put_original_metadata(metadata, %ClientRetry.OriginalWitness{version: 1, content_filter_original: <<_::256>> = digest}),
    do: Map.put(metadata, "native_content_filter_original", %{"version" => 1, "digest" => Base.url_encode64(digest, padding: false)})

  def put_original_metadata(metadata, _witness), do: Map.delete(metadata, "native_content_filter_original")

  @spec sanitize_original(term()) :: map()
  def sanitize_original(%{"version" => 1, "digest" => digest} = value) when map_size(value) == 2 and is_binary(digest) do
    case Base.url_decode64(digest, padding: false) do
      {:ok, <<_::256>>} -> value
      _invalid -> %{}
    end
  end

  def sanitize_original(_value), do: %{}

  @spec observation_metadata(map(), term()) :: map()
  def observation_metadata(metadata, %{} = state) do
    observation = Map.get(state, :native_content_filter_observation) || Map.get(state, :native_client_retry_observation)

    case observation do
      %ClientRetry.Observation{authority_poisoned?: false, output_item_done_count_saturated?: false, terminal_seen?: true} ->
        case ClientRetry.final_observation_metadata(ClientRetry.complete_without_terminal(observation)) do
          {:ok, summary} -> Map.put(metadata, "native_client_retry_observation", summary)
          :ineligible -> metadata
        end

      _unknown ->
        metadata
    end
  end

  def observation_metadata(metadata, _state), do: metadata

  @spec terminal_metadata(map(), term(), map()) :: map()
  def terminal_metadata(metadata, {:ok, %{kind: :incomplete, event_type: "response.incomplete", data_type: "response.incomplete", incomplete_reason: "content_filter"}}, context) do
    if context.endpoint == "/backend-api/codex/responses" and is_nil(context.request_options.openai_compatibility.source_endpoint) and complete_observation?(metadata) do
      source = %{"version" => 1, "attempt_id" => context.attempt.id, "assignment_id" => context.assignment.id, "identity_id" => context.identity.id, "credential_epoch" => CredentialFencing.credential_epoch(context.identity), "serving_mode" => context.request_options.routing.model_serving_mode, "requested_model" => context.reserved.request.requested_model, "effective_model" => context.request_options.routing.effective_model || context.model.exposed_model_id, "upstream_model" => context.attempt.upstream_model_id}
      metadata |> Map.put("native_content_filter_terminal", @terminal) |> Map.put("native_content_filter_source", source)
    else
      metadata
    end
  end

  def terminal_metadata(metadata, _outcome, _context), do: metadata

  @spec sanitize_terminal(term()) :: map()
  def sanitize_terminal(@terminal = value) when map_size(value) == 3, do: value
  def sanitize_terminal(_value), do: %{}

  @spec sanitize_source(term()) :: map()
  def sanitize_source(%{"version" => 1, "attempt_id" => attempt, "assignment_id" => assignment, "identity_id" => identity, "credential_epoch" => epoch, "serving_mode" => mode, "requested_model" => requested, "effective_model" => effective, "upstream_model" => upstream} = value)
      when map_size(value) == 9 and is_integer(epoch) and epoch > 0 and mode in ["full", "lite"] do
    if Enum.all?([attempt, assignment, identity], &match?({:ok, _}, Ecto.UUID.cast(&1))) and Enum.all?([requested, effective, upstream], &(is_binary(&1) and byte_size(&1) in 1..256 and String.valid?(&1))), do: value, else: %{}
  end

  def sanitize_source(_value), do: %{}

  @spec verified?(term(), term(), term(), term(), term()) :: boolean()
  def verified?(
        %CodexTurn{request_id: request_id, status: "succeeded", final_attempt_id: attempt_id, completed_at: %DateTime{}},
        %Request{id: request_id, status: "succeeded", endpoint: "/backend-api/codex/responses", completed_at: %DateTime{}, transport: transport} = request,
        %Attempt{id: attempt_id, request_id: request_id, status: "succeeded", transport: transport, replay_generation: 0, completed_at: %DateTime{}} = attempt,
        %ClientRetry.OriginalWitness{version: 1, auth_epoch: epoch, content_filter: candidates},
        successor
      )
      when transport in ["http_sse", "websocket"] and is_list(candidates) do
    metadata = attempt.response_metadata
    source = sanitize_source(metadata["native_content_filter_source"])

    ClientRetry.original_witness_eligible?(request) and request.native_client_retry_auth_epoch == epoch and
      source_matches?(source, request, attempt) and terminal_proof?(metadata, transport) and
      Enum.any?(candidates, &candidate_matches?(&1, request, successor, source, metadata, transport))
  end

  def verified?(_turn, _request, _attempt, _witness, _successor), do: false

  defp source_matches?(source, request, attempt) do
    source != %{} and source["attempt_id"] == attempt.id and source["assignment_id"] == attempt.pool_upstream_assignment_id and
      source["identity_id"] == attempt.upstream_identity_id and source["requested_model"] == request.requested_model and source["upstream_model"] == attempt.upstream_model_id
  end

  defp terminal_proof?(metadata, transport) do
    sanitize_terminal(metadata["native_content_filter_terminal"]) == @terminal and delivered?(metadata["downstream_delivery"], transport) and complete_observation?(metadata) and not Map.has_key?(metadata, "native_client_retry_authority_loss")
  end

  defp candidate_matches?(candidate, request, successor, source, metadata, transport) do
    witness_matches?(request, candidate.prefix) and ending_matches?(successor, candidate) and candidate.serving_mode == source["serving_mode"] and
      get_in(metadata, ["native_client_retry_observation", "output_item_done_count"]) == length(candidate.items) and output_matches?(metadata, transport, candidate)
  end

  @spec current_source?(Attempt.t()) :: boolean()
  def current_source?(%Attempt{response_metadata: metadata, model_id: model_id}) do
    case sanitize_source(metadata["native_content_filter_source"]) do
      %{"assignment_id" => assignment_id, "identity_id" => identity_id, "credential_epoch" => epoch, "upstream_model" => upstream} ->
        identity = Repo.one(from a in CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment, join: i in CodexPooler.Upstreams.Schemas.UpstreamIdentity, on: i.id == a.upstream_identity_id, where: a.id == ^assignment_id and i.id == ^identity_id and a.status == "active" and i.status == "active", select: i)
        model = Repo.get(CodexPooler.Catalog.Model, model_id)
        not is_nil(identity) and CredentialFencing.credential_epoch(identity) == epoch and not is_nil(model) and model.upstream_model_id == upstream

      _invalid ->
        false
    end
  end

  def current_source?(_attempt), do: false

  @spec binding(Request.t()) :: map()
  def binding(%Request{id: id}) do
    case Repo.one(from t in CodexTurn, join: a in Attempt, on: a.id == t.final_attempt_id, where: t.request_id == ^id, select: a.response_metadata) do
      %{} = metadata -> sanitize_source(metadata["native_content_filter_source"])
      _missing -> %{}
    end
  end

  @spec dispatch_allowed?(Request.t(), map()) :: boolean()
  def dispatch_allowed?(%Request{request_metadata: metadata} = request, scope) do
    predecessor = Repo.one(from l in RequestClientRetryLink, join: t in CodexTurn, on: t.request_id == l.predecessor_request_id, join: a in Attempt, on: a.id == t.final_attempt_id, where: l.successor_request_id == ^request.id, select: a.response_metadata)

    required? = binding_required?(metadata, predecessor)

    case Map.fetch(metadata || %{}, "native_content_filter_binding") do
      :error ->
        not required?

      {:ok, value} ->
        binding = sanitize_source(value)

        binding != %{} and binding_scope_matches?(binding, request, scope) and predecessor_binding_matches?(predecessor, binding) and current_final_attempt?(binding["attempt_id"])
    end
  end

  defp binding_required?(metadata, predecessor), do: get_in(metadata || %{}, ["client_resend", "predecessor_shape"]) == "content_filter_retry" or (is_map(predecessor) and Map.has_key?(predecessor, "native_content_filter_terminal"))

  defp binding_scope_matches?(binding, request, scope) do
    expected = %{"assignment_id" => scope.assignment_id, "identity_id" => scope.identity_id, "credential_epoch" => scope.credential_epoch, "serving_mode" => scope.serving_mode, "requested_model" => request.requested_model, "effective_model" => scope.effective_model, "upstream_model" => scope.upstream_model}
    Map.take(binding, Map.keys(expected)) == expected
  end

  defp predecessor_binding_matches?(%{} = metadata, binding), do: sanitize_source(metadata["native_content_filter_source"]) == binding and sanitize_terminal(metadata["native_content_filter_terminal"]) == @terminal
  defp predecessor_binding_matches?(_metadata, _binding), do: false

  defp current_final_attempt?(attempt_id) do
    Repo.exists?(from a in Attempt, as: :predecessor_attempt, join: t in CodexTurn, on: t.final_attempt_id == a.id, join: r in Request, on: r.id == a.request_id, where: a.id == ^attempt_id and a.status == "succeeded" and a.replay_generation == 0 and t.status == "succeeded" and r.status == "succeeded", where: not exists(from newer in Attempt, where: newer.request_id == parent_as(:predecessor_attempt).request_id and newer.attempt_number > parent_as(:predecessor_attempt).attempt_number))
  end

  @spec dispatch_context_allowed?(map()) :: boolean()
  def dispatch_context_allowed?(%{request_id: nil}), do: true

  def dispatch_context_allowed?(context) do
    case Repo.get(Request, context.request_id) do
      %Request{} = request -> dispatch_allowed?(request, %{assignment_id: context.pool_upstream_assignment_id, identity_id: context.upstream_identity_id, credential_epoch: context.credential_epoch, serving_mode: Atom.to_string(context.serving_mode), effective_model: context.model, upstream_model: context.upstream_model})
      nil -> false
    end
  end

  defp delivered?(%{"outcome" => "delivered", "terminal_class" => "response.incomplete", "incomplete_reason" => "content_filter", "transport" => transport} = receipt, transport), do: not Map.has_key?(receipt, "write_failure")
  defp delivered?(_receipt, _transport), do: false

  defp complete_observation?(%{"native_client_retry_observation" => %{"version" => 1, "authority_complete" => true, "output_item_done_count_saturated" => false, "terminal_seen" => true}}), do: true
  defp complete_observation?(_metadata), do: false

  defp witness_matches?(request, candidates) do
    with %{"digest" => encoded} <- sanitize_original((request.request_metadata || %{})["native_content_filter_original"]),
         {:ok, digest} <- Base.url_decode64(encoded, padding: false) do
      Enum.any?(candidates, &secure_compare(digest, &1))
    else
      _missing -> false
    end
  end

  defp ending_matches?(nil, %{current?: true}), do: true
  defp ending_matches?(%Request{} = successor, candidate), do: ClientRetry.original_witness_eligible?(successor) and witness_matches?(successor, candidate.ending)
  defp ending_matches?(_successor, _candidate), do: false

  defp output_matches?(metadata, "websocket", %{items: items}) do
    case metadata["downstream_delivery"] do
      %{"completed_items" => count, "completed_item_digests" => digests} when is_integer(count) and is_list(digests) -> count == length(items) and count == length(digests) and items == digests
      _unknown -> false
    end
  end

  defp output_matches?(metadata, "http_sse", %{http_progress: candidates}) do
    Enum.any?(candidates, fn expected ->
      case {metadata["native_http_resume_progress"], expected} do
        {%{"version" => 1, "output_item_done_count" => count, "digest" => digest}, %{"version" => 1, "output_item_done_count" => count, "digest" => other}} -> secure_compare(digest, other)
        _unknown -> false
      end
    end)
  end

  defp secure_compare(a, b) when is_binary(a) and is_binary(b) and byte_size(a) == byte_size(b), do: Plug.Crypto.secure_compare(a, b)
  defp secure_compare(_a, _b), do: false
end
