defmodule CodexPooler.Gateway.Payloads.NativeContinuationTail do
  @moduledoc false

  # What a Codex client records after a response's completed output items and
  # before its next sampling request of the same turn (findings#311). When a
  # `response.completed` carries `end_turn: false`, or input is pending, the
  # released client samples the turn again: over HTTP SSE the next request is
  # the previous input, then that response's completed items, then whatever
  # harness items the turn loop recorded in between (`session/turn.rs`
  # `run_turn`): the current-time reminder and the rollout or token budget
  # (`ContextualUserFragment`s of role `developer`), the world-state updates of
  # the developer sections (collaboration mode, model catalog, top-level tools
  # and their `additional_tools` manifest, managed instructions) and a
  # `configuration_update` when the selected reasoning effort changed.
  #
  # The grammar is a positive list without text markers, so a new fragment of an
  # allowed role needs no change. Everything else ends the tail: a user message
  # (it moves the turn's progress, so such a request already takes the steered
  # claim), an assistant message or reasoning outside the verified output run,
  # a tool call or result, a compaction item, an `agent_message`, an unknown
  # type, a non-object item. The shape was derived from the client source, not
  # from a capture, so a refusal names the first item it stopped at in a closed
  # vocabulary (`describe/1`): the first real refusal says what to add, without
  # logging any client content.

  @max_items 16

  @known_types ~w(
    message
    reasoning
    function_call
    function_call_output
    custom_tool_call
    custom_tool_call_output
    local_shell_call
    local_shell_call_output
    tool_search_call
    tool_search_output
    web_search_call
    image_generation_call
    mcp_tool_call
    item_reference
    compaction
    compaction_summary
    context_compaction
    compaction_trigger
    agent_message
    configuration_update
    additional_tools
  )
  @known_roles ~w(user assistant developer system)

  @type item_description :: %{
          required(:item_type) => String.t(),
          required(:item_role) => String.t(),
          optional(:item_type_fingerprint) => String.t()
        }

  @type refusal ::
          %{required(:reason) => :too_long, required(:tail_length) => non_neg_integer()}
          | %{
              required(:reason) => :item,
              required(:tail_length) => non_neg_integer(),
              required(:tail_index) => non_neg_integer(),
              required(:item_type) => String.t(),
              required(:item_role) => String.t(),
              optional(:item_type_fingerprint) => String.t()
            }

  @doc "The longest tail accepted: each asynchronous hook context is its own developer message."
  @spec max_items() :: pos_integer()
  def max_items, do: @max_items

  @doc """
  `:ok` when every item of `tail` is one the client records between two
  sampling requests of a turn, else the first refused item, described in the
  closed vocabulary of `describe/1`, with its index and the tail's length.
  """
  @spec check([term()]) :: :ok | {:error, refusal()}
  def check(tail) when is_list(tail) do
    count = length(tail)

    if count > @max_items do
      {:error, %{reason: :too_long, tail_length: count}}
    else
      case Enum.find_index(tail, &(not allowed?(&1))) do
        nil -> :ok
        index -> {:error, tail |> Enum.at(index) |> describe() |> Map.merge(%{reason: :item, tail_index: index, tail_length: count})}
      end
    end
  end

  @doc "True for a developer message, a `configuration_update` or a developer `additional_tools` manifest."
  @spec allowed?(term()) :: boolean()
  def allowed?(%{"type" => "message", "role" => "developer"}), do: true
  def allowed?(%{"type" => "configuration_update"}), do: true
  def allowed?(%{"type" => "additional_tools", "role" => "developer"}), do: true
  def allowed?(_item), do: false

  @doc """
  An item's type and role in a closed vocabulary: a known item type and one of
  the four roles stay as they are, anything else reads `other` (a type also
  gets a 12-character SHA-256 fingerprint so two refusals can be compared), an
  item without a string type `untyped`, a role absent `none`, a non-object
  item `non_object`. Item content is never read.
  """
  @spec describe(term()) :: item_description()
  def describe(%{} = item) do
    item
    |> Map.get("type")
    |> describe_type()
    |> Map.put(:item_role, describe_role(Map.get(item, "role")))
  end

  def describe(_item), do: %{item_type: "non_object", item_role: "none"}

  defp describe_type(type) when type in @known_types, do: %{item_type: type}
  defp describe_type(type) when is_binary(type), do: %{item_type: "other", item_type_fingerprint: fingerprint(type)}
  defp describe_type(_type), do: %{item_type: "untyped"}

  defp describe_role(role) when role in @known_roles, do: role
  defp describe_role(nil), do: "none"
  defp describe_role(_role), do: "other"

  defp fingerprint(value), do: :sha256 |> :crypto.hash(value) |> Base.encode16(case: :lower) |> binary_part(0, 12)
end
