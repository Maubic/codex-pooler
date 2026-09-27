defmodule CodexPoolerWeb.DateTimeOptionsCacheTest do
  use ExUnit.Case, async: false
  alias CodexPoolerWeb.DateTimeDisplay

  test "repeated Settings inventories reuse the resolved timezone list" do
    options = DateTimeDisplay.timezone_options()
    assert {"Etc/UTC", "Etc/UTC"} in options
    caller = self()
    tracer = spawn_link(fn -> collect(caller) end)

    on_exit(fn ->
      :erlang.trace_pattern({Zoneinfo, :time_zones, 0}, false, [])
      if Process.alive?(tracer), do: send(tracer, :stop)
    end)

    :erlang.trace_pattern({Zoneinfo, :time_zones, 0}, true, [])
    :erlang.trace(self(), true, [:call, {:tracer, tracer}])
    assert DateTimeDisplay.timezone_options() == options
    # Positive control proves the tracer observes real enumeration calls.
    assert is_list(Zoneinfo.time_zones())
    :erlang.trace(self(), false, [:call])
    delivered = :erlang.trace_delivered(self())
    assert_receive {:trace_delivered, ^caller, ^delivered}, 15_000
    send(tracer, :finish)
    assert_receive {:timezone_enumerations, [{:trace, ^caller, :call, {Zoneinfo, :time_zones, []}}]}, 15_000
  end

  defp collect(caller, calls \\ []) do
    receive do
      {:trace, _pid, :call, {Zoneinfo, :time_zones, []}} = call -> collect(caller, [call | calls])
      :finish -> send(caller, {:timezone_enumerations, calls})
      :stop -> :ok
    end
  end
end
