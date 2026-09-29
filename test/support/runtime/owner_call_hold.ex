defmodule CodexPoolerWeb.Runtime.OwnerCallHold do
  @moduledoc false

  # Holds a websocket owner, or a socket's own upstream session, inside its own
  # process right after it replied to one call, so the next call its caller
  # makes meets a process that does not answer within the call budget, while
  # the call it answered stays answered. A `:sys` debug hook, installed on the
  # process's node (`install/3` is called there through `:erpc` for an owner on
  # a peer), matches the reply; the held process reports
  # `{ref, :held, pid}` to `test` and waits for `{ref, :release}`, or releases
  # itself after the detection budget.

  alias CodexPooler.Gateway.Transports.Websocket.NativeCompactionAdmission
  alias CodexPooler.Gateway.Transports.Websocket.NativeCompactionAdmission.FirstCompactCollection

  @detection_timeout_ms 15_000

  @doc """
  Holds `server` right after its next reply of the kind `reply` names: the
  authorization of a first full-history compaction (the default), or an
  owner's native compaction admission snapshot.
  """
  # The hook's state is a map: `:sys` reads an installed `{func, state}` whose
  # state is a two-tuple as `{func_id, {func, state}}`, and silently drops it.
  def install(server, ref, test, reply \\ :first_compact_authorization)
      when is_reference(ref) and is_pid(test) and reply in [:first_compact_authorization, :admission_snapshot],
      do: :sys.install(server, {&__MODULE__.hook/3, %{ref: ref, test: test, reply: reply}})

  @doc false
  def hook(%{ref: ref, test: test, reply: reply} = hold, event, _process_name) do
    if held_reply?(reply, event) do
      send(test, {ref, :held, self()})

      receive do
        {^ref, :release} -> :ok
      after
        @detection_timeout_ms -> :ok
      end

      :done
    else
      hold
    end
  end

  defp held_reply?(:first_compact_authorization, {:out, {:ok, %FirstCompactCollection{}}, _to}), do: true
  defp held_reply?(:first_compact_authorization, {:out, {:ok, %FirstCompactCollection{}}, _to, _state}), do: true
  defp held_reply?(:admission_snapshot, {:out, {:ok, %NativeCompactionAdmission{}}, _to}), do: true
  defp held_reply?(:admission_snapshot, {:out, {:ok, %NativeCompactionAdmission{}}, _to, _state}), do: true
  defp held_reply?(_reply, _event), do: false
end
