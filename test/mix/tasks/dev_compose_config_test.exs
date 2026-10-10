defmodule CodexPooler.MixTasks.DevComposeConfigTest do
  use CodexPooler.UnixIntegrationCase,
    async: true,
    tools: ~w(docker),
    docker_compose: true

  @compose Path.expand("../../../docker-compose.dev.yml", __DIR__)

  test "development PostgreSQL publishes only on loopback by default" do
    db = rendered_db([])

    assert [%{"host_ip" => "127.0.0.1", "published" => "5433", "target" => 5432, "protocol" => "tcp"}] = db["ports"]
    assert db["image"] == "postgres:18"
    assert [%{"type" => "volume", "target" => "/var/lib/postgresql"}] = db["volumes"]
  end

  test "empty bind and port overrides retain the loopback defaults" do
    db = rendered_db([{"POSTGRES_BIND_ADDRESS", ""}, {"POSTGRES_PORT", ""}])

    assert [%{"host_ip" => "127.0.0.1", "published" => "5433", "target" => 5432}] = db["ports"]
  end

  test "development PostgreSQL preserves the host port override on loopback" do
    db = rendered_db([{"POSTGRES_PORT", "55433"}])

    assert [%{"host_ip" => "127.0.0.1", "published" => "55433", "target" => 5432}] = db["ports"]
  end

  test "an explicit bind address permits intentional remote development" do
    db = rendered_db([{"POSTGRES_BIND_ADDRESS", "0.0.0.0"}, {"POSTGRES_PORT", "55434"}])

    assert [%{"host_ip" => "0.0.0.0", "published" => "55434", "target" => 5432}] = db["ports"]
  end

  defp rendered_db(overrides) do
    env = Map.merge(%{"POSTGRES_BIND_ADDRESS" => nil, "POSTGRES_PORT" => nil}, Map.new(overrides))

    {output, code} =
      System.cmd("docker", ["compose", "--env-file", "/dev/null", "-f", @compose, "config", "--format", "json"],
        env: Map.to_list(env),
        stderr_to_stdout: true
      )

    assert code == 0, output
    output |> CodexPooler.JSON.decode!() |> get_in(["services", "db"])
  end
end
