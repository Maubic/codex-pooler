defmodule CodexPooler.Platform.ExecutionIdentity do
  @moduledoc false

  alias CodexPooler.Platform.ExecutionRegistry
  alias CodexPooler.Platform.InstancePresence.Identity

  @key {__MODULE__, :execution_id}
  @producer_key {__MODULE__, :producer_id}

  @spec producer() :: map() | :unknown
  def producer do
    case Process.get(@producer_key) do
      nil -> register_producer()
      identity -> if status(identity) == :alive, do: identity, else: :unknown
    end
  end

  defp register_producer do
    id = Ecto.UUID.generate()

    if ExecutionRegistry.register_producer(id) == :ok do
      identity = %{owner_instance_id: Atom.to_string(node()), owner_instance_boot_id: Identity.boot_id(), owner_process_id: List.to_string(:erlang.pid_to_list(self())), owner_execution_id: id}
      Process.put(@producer_key, identity)
      identity
    else
      :unknown
    end
  end

  @spec local() :: %{owner_process_id: String.t(), owner_execution_id: Ecto.UUID.t()}
  def local do
    execution_id = Process.get(@key) || register_execution()

    %{
      owner_process_id: List.to_string(:erlang.pid_to_list(self())),
      owner_execution_id: execution_id
    }
  end

  @spec complete() :: :ok
  def complete do
    if id = Process.delete(@key), do: ExecutionRegistry.complete(id)
    :ok
  end

  defp register_execution do
    id = Ecto.UUID.generate()
    _result = ExecutionRegistry.register(id)
    Process.put(@key, id)
    id
  end

  @spec status(map()) :: :alive | :dead | :unknown
  def status(%{
        owner_instance_id: node_name,
        owner_instance_boot_id: boot_id,
        owner_process_id: process_id,
        owner_execution_id: execution_id
      })
      when is_binary(node_name) and is_binary(boot_id) and is_binary(process_id) and
             is_binary(execution_id) do
    case Enum.find([node() | Node.list()], &(Atom.to_string(&1) == node_name)) do
      nil ->
        :unknown

      target when target == node() ->
        local_status(boot_id, process_id, execution_id)

      target ->
        :erpc.call(target, __MODULE__, :local_status, [boot_id, process_id, execution_id], 1_000)
    end
  catch
    _, _ -> :unknown
  end

  def status(_identity), do: :unknown

  @spec local_status(String.t(), String.t(), String.t()) :: :alive | :dead | :unknown
  def local_status(boot_id, process_id, execution_id) do
    if boot_id == Identity.boot_id() and is_binary(process_id) and byte_size(process_id) <= 64 and
         Regex.match?(~r/^<0\.[0-9]+\.[0-9]+>$/, process_id) and
         is_binary(execution_id) and byte_size(execution_id) == 36 and
         match?({:ok, ^execution_id}, Ecto.UUID.cast(execution_id)) do
      pid = process_id |> String.to_charlist() |> :erlang.list_to_pid()

      ExecutionRegistry.status(execution_id, pid)
    else
      :unknown
    end
  rescue
    ArgumentError -> :unknown
  end
end
