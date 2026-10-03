defmodule CodexPoolerWeb.Runtime.NativeContinuationMatrix do
  @moduledoc "Synthetic boundary contracts; these inputs do not certify a released client."

  @roles [:opening, :steered_continuation, :tool_continuation, :post_compaction_resume]
  @paths [:http_sse, :websocket_direct, :websocket_owner_local, :websocket_owner_remote, :websocket_to_http_fallback]
  @modes [:full, :lite]
  @states [:same, :replacement_fresh, :replacement_engaged]

  @spec cells() :: [map()]
  def cells do
    for role <- @roles, path <- @paths, mode <- @modes, state <- @states do
      %{id: Enum.join([role, path, mode, state], "/"), role: role, path: path, mode: mode, state: state, claim_domain: claim_domain(role), transport_pair: transport_pair(path), owner_topology: topology(path), receipt_family: if(path == :http_sse, do: :http_written_prefix, else: :websocket_delivery), lifecycle: state, outcome: :admission, oracle: :ancestry_dispatch_per_request_ledger, retry_link: role != :tool_continuation, provenance: :synthetic_boundary}
    end
  end

  @spec validate_census!([map()]) :: :ok
  def validate_census!(results) do
    expected = MapSet.new(cells(), & &1.id)
    actual = MapSet.new(results, & &1.id)
    unless length(results) == 120 and actual == expected, do: raise(ArgumentError, "matrix census: expected exactly 120 unique declared ids")

    for result <- results do
      cell = Enum.find(cells(), &(&1.id == result.id))
      validate_cell!(result, cell)
    end

    :ok
  end

  defp validate_cell!(result, cell) do
    if result.outcome == :not_applicable, do: raise(ArgumentError, "matrix census: no structural N/A is declared")
    checks = [{:path, :path, "matrix topology: wrong path label"}, {:outcome, :outcome, "matrix census: observed outcome does not satisfy cell contract"}, {:transport_pair, :transport_pair, "matrix topology: wrong transport pair"}, {:owner_topology, :owner_topology, "matrix topology: wrong owner topology"}, {:observed_role, :role, "matrix role: wrong observed role"}, {:observed_state, :state, "matrix lifecycle: wrong observed state"}, {:observed_mode, :mode, "matrix serving mode: wrong observed mode"}, {:observed_claim_domain, :claim_domain, "matrix role: wrong observed claim domain"}]

    Enum.each(checks, fn {observed, expected, message} ->
      unless Map.get(result, observed) == Map.fetch!(cell, expected), do: raise(ArgumentError, message)
    end)

    assert_work_set!(cell, result.role_request_ids, result.work_observation)
    if cell.state != :same, do: assert_replacement!(result.lifecycle_observation)
  end

  @spec assert_work_set!(map(), map(), map()) :: :ok
  def assert_work_set!(cell, roles, observed) do
    expected_roles = [:predecessor, :successor] ++ optional_roles(cell)
    actual_roles = roles |> Enum.reject(fn {_role, id} -> is_nil(id) end) |> Map.new()
    require!(MapSet.new(Map.keys(actual_roles)) == MapSet.new(expected_roles), "matrix work: wrong action roles")
    ids = Map.values(actual_roles)
    require!(Enum.all?(ids, &is_binary/1) and length(Enum.uniq(ids)) == length(expected_roles), "matrix work: action identities are not disjoint")
    require!(Enum.sort(observed.request_ids) == Enum.sort(ids), "matrix work: unexpected request identities")
    require!(Enum.sort(observed.attempt_request_ids) == Enum.sort(ids), "matrix work: unexpected attempt identities")
    require!(observed.dispatch_count == length(expected_roles), "matrix work: unexpected physical dispatch count")
    expected_edges = if cell.retry_link, do: [%{predecessor: roles.predecessor, successor: roles.successor}], else: []
    require!(Enum.sort(observed.retry_edges) == Enum.sort(expected_edges), "matrix work: unexpected retry edges")
    assert_ledger_set!(observed.ledger, ids)
    :ok
  end

  defp optional_roles(cell) do
    opener = if cell.role == :steered_continuation, do: [:opener], else: []
    preparation = if cell.state == :replacement_engaged, do: [:preparation], else: []
    opener ++ preparation
  end

  defp assert_ledger_set!(ledger, ids) do
    expected = for id <- ids, kind <- ["reservation", "release", "settlement"], do: %{request_id: id, kind: kind, count: 1}
    require!(Enum.sort(ledger) == Enum.sort(expected), "matrix work: unexpected per-request ledger identities")
  end

  @spec assert_replacement!(map()) :: :ok
  def assert_replacement!(observation) do
    closed = timestamp!(observation.previous_closed_at)
    created = timestamp!(observation.replacement_created_at)
    deadline = timestamp!(observation.previous_owner_deadline)
    require!(observation.close_reason == "owner_lease_expired", "matrix lifecycle: close is not trusted expiry")
    require!(DateTime.compare(deadline, closed) != :gt, "matrix lifecycle: close predates deadline")
    require!(DateTime.compare(created, closed) != :lt, "matrix lifecycle: replacement predates close")
    require!(observation.same_pool == true and observation.same_api_key == true, "matrix lifecycle: replacement scope differs")
    require!(observation.same_canonical_key == true, "matrix lifecycle: canonical key differs")
    require!(observation.genuine_expiry == true and observation.unchanged_deadlines == true, "matrix lifecycle: expiry was not observed")
    :ok
  end

  defp timestamp!(%DateTime{} = value), do: value

  defp timestamp!(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, time, _offset} -> time
      _invalid -> raise ArgumentError, "matrix lifecycle: timestamp is missing or invalid"
    end
  end

  defp timestamp!(_value), do: raise(ArgumentError, "matrix lifecycle: timestamp is missing or invalid")
  defp require!(true, _message), do: :ok
  defp require!(_value, message), do: raise(ArgumentError, message)

  @spec claim_domain(atom()) :: String.t()
  def claim_domain(:opening), do: "codex-turn"
  def claim_domain(:steered_continuation), do: "codex-resume"
  def claim_domain(:tool_continuation), do: "codex-request"
  def claim_domain(:post_compaction_resume), do: "codex-resume"

  @spec transport_pair(atom()) :: [atom()]
  def transport_pair(:http_sse), do: [:http_sse, :http_sse]
  def transport_pair(:websocket_to_http_fallback), do: [:websocket, :http_sse]
  def transport_pair(_path), do: [:websocket, :websocket]

  @spec topology(atom()) :: atom()
  def topology(:http_sse), do: :none
  def topology(:websocket_direct), do: :direct
  def topology(:websocket_owner_remote), do: :remote
  def topology(_path), do: :local

  @spec read_receipt!(String.t()) :: map()
  def read_receipt!(path) do
    json = path |> File.read!() |> CodexPooler.JSON.decode!()
    %{id: json["id"], path: String.to_existing_atom(json["path"]), outcome: String.to_existing_atom(json["outcome"]), transport_pair: Enum.map(json["transport_pair"], &String.to_existing_atom/1), owner_topology: String.to_existing_atom(json["owner_topology"]), observed_role: String.to_existing_atom(json["observed_role"]), observed_state: String.to_existing_atom(json["observed_state"]), observed_mode: String.to_existing_atom(json["observed_mode"]), observed_claim_domain: json["observed_claim_domain"], role_request_ids: atom_keys(json["role_request_ids"]), work_observation: read_work(json["work_observation"]), lifecycle_observation: atom_keys(json["lifecycle_observation"])}
  end

  defp read_work(work) do
    work = atom_keys(work)
    %{work | retry_edges: Enum.map(work.retry_edges, &atom_keys/1), ledger: Enum.map(work.ledger, &atom_keys/1)}
  end

  defp atom_keys(map), do: Map.new(map, fn {key, value} -> {String.to_existing_atom(key), value} end)
end
