defmodule CodexPooler.FakeRefreshTokenProvider do
  @moduledoc "Stateful loopback OAuth provider; synthetic policies are not claims about the real issuer."
  use Supervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    ledger_name = Keyword.fetch!(opts, :name)
    initial = %{generation: 0, consumed: 0, requests: 0, invalidated: false, policy: Keyword.get(opts, :policy, :old_token), steps: Keyword.get(opts, :steps, []), notify: Keyword.fetch!(opts, :notify)}

    Supervisor.init(
      [
        Supervisor.child_spec({Agent, fn -> initial end}, id: :ledger, start: {Agent, :start_link, [fn -> initial end, [name: ledger_name]]}),
        Supervisor.child_spec({Bandit, plug: {__MODULE__.Plug, ledger_name}, ip: {127, 0, 0, 1}, port: 0, startup_log: false}, id: :http)
      ],
      strategy: :one_for_all
    )
  end

  @spec url(pid()) :: String.t()
  def url(supervisor) do
    {_, server, _, _} = Enum.find(Supervisor.which_children(supervisor), &(elem(&1, 0) == :http))
    {:ok, {_, port}} = ThousandIsland.listener_info(server)
    "http://127.0.0.1:#{port}"
  end

  @spec snapshot(atom()) :: map()
  def snapshot(ledger), do: Agent.get(ledger, &Map.take(&1, [:generation, :consumed, :requests, :invalidated]))

  defmodule Plug do
    @moduledoc false
    @behaviour Elixir.Plug
    import Elixir.Plug.Conn

    @impl true
    def init(ledger), do: ledger

    @impl true
    def call(conn, ledger) do
      {:ok, body, conn} = read_body(conn)
      form = URI.decode_query(body)

      {ordinal, step, notify} =
        Agent.get_and_update(ledger, fn state ->
          {step, remaining} =
            case state.steps do
              [step | rest] -> {step, rest}
              [] -> {%{}, []}
            end

          {{state.requests + 1, step, state.notify}, %{state | requests: state.requests + 1, steps: remaining}}
        end)

      if barrier(step, :before_consume, ordinal, notify) == :cancel do
        send_resp(conn, 503, "{}")
      else
        {status, payload, generation} = Agent.get_and_update(ledger, &consume(&1, form, step))
        send(notify, {:rotation_decision, ordinal, status, generation})
        barrier(step, :after_consume, ordinal, notify)
        conn |> put_resp_content_type("application/json") |> send_resp(status, CodexPooler.JSON.encode!(payload))
      end
    end

    defp consume(state, form, step) do
      if not state.invalidated and form["grant_type"] == "refresh_token" and form["refresh_token"] == token(state.generation) do
        generation = state.generation + 1
        payload = %{"access_token" => Map.get(step, :access_token, "synthetic-access-#{generation}"), "refresh_token" => token(generation), "expires_in" => 3600}
        {{200, payload, generation}, %{state | generation: generation, consumed: state.consumed + 1}}
      else
        {{400, %{"error" => "refresh_token_reused"}, state.generation}, %{state | invalidated: state.policy == :family}}
      end
    end

    defp token(generation), do: "synthetic-refresh-#{generation}"

    defp barrier(%{hold: phase}, phase, ordinal, notify) do
      ref = make_ref()
      send(notify, {:rotation_barrier, phase, ordinal, self(), ref})

      receive do
        {:rotation_release, ^ref} -> :ok
        {:rotation_cancel, ^ref} -> :cancel
      after
        15_000 -> raise "synthetic provider barrier was not released"
      end
    end

    defp barrier(_, _, _, _), do: :ok
  end
end
