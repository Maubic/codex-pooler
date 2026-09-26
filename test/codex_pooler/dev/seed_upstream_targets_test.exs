defmodule CodexPooler.Dev.SeedUpstreamTargetsTest do
  # Seeded synthetic identities must point at a fake that exists: the local
  # perf fake for `make dev`, a replica's in-cluster fake when given. Real
  # identities go into their own seeded Pool, and the import task refuses a
  # Pool that serves from synthetic upstreams, so a real copy never shares a
  # model with a fake source.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [start_upstream: 1, stream_success_sse: 0]

  alias CodexPooler.Access.APIKey
  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.Accounts.{Scope, User}
  alias CodexPooler.Dev.{Seeds, UpstreamAccountBundle}
  alias CodexPooler.Dev.Seeds.RealTraffic
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.EndpointMetadata
  alias CodexPooler.Upstreams.Schemas.{EncryptedSecret, PoolUpstreamAssignment, UpstreamIdentity}
  alias Mix.Tasks.Dev.Seed, as: SeedTask

  @in_cluster "http://fake-upstream:4058"
  @password "synthetic-bundle-password-12345"

  setup do
    reset_bootstrap_state_fixture!()
    :ok
  end

  test "perf seeds every synthetic source at the given in-cluster fake" do
    result = Seeds.perf(upstream_base_url: @in_cluster)

    assert length(result.assignments) == 12
    assert resolved_base_urls(result.pool) == [@in_cluster]
    assert Enum.all?(result.assignments, &(&1.metadata["websocket_url"] == "ws://fake-upstream:4058/ws"))
  end

  test "a perf-seeded Pool serves a gateway turn from a seeded identity through the given fake", %{conn: conn} do
    upstream = start_upstream(stream_success_sse())
    result = Seeds.perf(upstream_base_url: FakeUpstream.url(upstream))
    [raw_key] = for "CODEX_POOLER_PERF_API_KEY=" <> key <- String.split(File.read!("tmp/gateway-perf/bootstrap/perf.env"), "\n"), do: key

    response =
      conn
      |> put_req_header("authorization", "Bearer " <> raw_key)
      |> post("/backend-api/codex/responses", %{"model" => "gpt-6-luna", "input" => [%{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "seed"}]}], "stream" => true})

    assert response.status == 200
    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^result.pool.id))
    assert request.status == "succeeded"
    assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
    assert attempt.upstream_identity_id in Enum.map(result.upstream_identities, & &1.id)
    assert Enum.count(FakeUpstream.requests(upstream), &(&1.path == "/backend-api/codex/responses")) == 1
  end

  test "full seeds every synthetic identity at the local perf fake by default, never at the provider" do
    default = Seeds.full()
    assert Enum.all?(default.upstream_identities, &(&1.metadata["base_url"] == "http://127.0.0.1:4058"))
    assert pool_by_slug("dev-primary") |> resolved_base_urls() == ["http://127.0.0.1:4058"]
  end

  test "full seeds every synthetic identity at the given in-cluster fake" do
    given = Seeds.full(upstream_base_url: @in_cluster)
    assert Enum.all?(given.upstream_identities, &(&1.metadata["base_url"] == @in_cluster))
    assert pool_by_slug("dev-primary") |> resolved_base_urls() == [@in_cluster]
  end

  test "a public upstream host is refused before any seed row is written" do
    assert_raise ArgumentError, ~r/in-cluster service origin/, fn -> Seeds.perf(upstream_base_url: "https://chatgpt.com") end
    refute pool_by_slug("dev-perf-pool")

    assert_raise Mix.Error, ~r/in-cluster service origin/, fn -> SeedTask.run(["full", "--upstream-base-url", "http://chatgpt.com:80"]) end
    assert_raise Mix.Error, ~r/usage: mix dev.seed/, fn -> SeedTask.run(["real_traffic", "--upstream-base-url", @in_cluster]) end
    refute pool_by_slug("dev-primary")
  end

  test "real traffic seed refuses a symlinked output parent without changing its target" do
    root = Path.join(System.tmp_dir!(), "real-traffic-parent-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(Path.join(root, "tmp"))
    target = Path.join(root, "outside")
    File.mkdir!(target)
    File.write!(Path.join(target, "real-traffic.env"), "sentinel")
    File.ln_s!(target, Path.join(root, "tmp/dev-seed"))

    File.cd!(root, fn ->
      assert_raise RuntimeError, ~r/private directory/, fn -> Seeds.real_traffic() end
    end)

    assert File.read!(Path.join(target, "real-traffic.env")) == "sentinel"
    refute Repo.get_by(Pool, slug: RealTraffic.pool_slug())
  end

  test "real traffic seed refuses a final symlink without removing it or changing its target" do
    root = Path.join(System.tmp_dir!(), "real-traffic-file-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(Path.join(root, "tmp/dev-seed"))
    File.chmod!(Path.join(root, "tmp/dev-seed"), 0o700)
    target = Path.join(root, "outside.env")
    File.write!(target, "sentinel")
    File.ln_s!(target, Path.join(root, RealTraffic.env_path()))

    File.cd!(root, fn ->
      assert_raise RuntimeError, ~r/private regular file/, fn -> Seeds.real_traffic() end
      assert File.lstat!(RealTraffic.env_path()).type == :symlink
    end)

    assert File.read!(target) == "sentinel"
    refute Repo.get_by(Pool, slug: RealTraffic.pool_slug())
  end

  test "real traffic rotation holds a database lock while publishing its key" do
    root = Path.join(System.tmp_dir!(), "real-traffic-lock-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(root)
    connection = start_supervised!({Postgrex, Keyword.take(Repo.config(), [:hostname, :port, :username, :password, :database])})
    handler = "real-traffic-lock-#{System.unique_integer([:positive])}"
    owner = self()
    owner_record = Seeds.compact().owner
    on_exit(fn -> :telemetry.detach(handler) end)

    :ok =
      :telemetry.attach(
        handler,
        [:codex_pooler, :repo, :query],
        fn _, _, metadata, _ ->
          if self() == owner and String.starts_with?(metadata.query, "INSERT INTO \"api_keys\"") do
            %{rows: [[acquired]]} = Postgrex.query!(connection, "SELECT pg_try_advisory_lock($1)", [7_391_204_018])
            if acquired, do: Postgrex.query!(connection, "SELECT pg_advisory_unlock($1)", [7_391_204_018])
            send(owner, {:rotation_lock_available, acquired})

            second_writer =
              try do
                RealTraffic.run(%{owner: owner_record})
                :unexpected_success
              rescue
                error in RuntimeError -> Exception.message(error)
              end

            send(owner, {:second_writer, second_writer})
          end
        end,
        nil
      )

    File.cd!(root, fn -> Seeds.real_traffic() end)
    :telemetry.detach(handler)
    assert_receive {:rotation_lock_available, false}
    assert_receive {:second_writer, "seed output is locked; verify that its previous publisher stopped before removing the lock"}
  end

  test "failed real traffic publication rolls back key rotation and preserves the prior env file" do
    root = Path.join(System.tmp_dir!(), "real-traffic-rollback-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(root)
    handler = "real-traffic-rollback-#{System.unique_integer([:positive])}"
    owner = self()
    on_exit(fn -> :telemetry.detach(handler) end)

    File.cd!(root, fn ->
      first = Seeds.real_traffic()
      original_digest = :crypto.hash(:sha256, File.read!(RealTraffic.env_path()))

      :ok =
        :telemetry.attach(
          handler,
          [:codex_pooler, :repo, :query],
          fn _, _, metadata, _ ->
            if self() == owner and String.starts_with?(metadata.query, "INSERT INTO \"api_keys\"") do
              File.chmod!(RealTraffic.env_path(), 0o644)
            end
          end,
          nil
        )

      assert_raise RuntimeError, ~r/private regular file/, fn -> Seeds.real_traffic() end
      :telemetry.detach(handler)
      assert :crypto.hash(:sha256, File.read!(RealTraffic.env_path())) == original_digest
      assert File.ls!(Path.dirname(RealTraffic.env_path())) == ["real-traffic.env"]
      assert Repo.get!(APIKey, first.api_key.id).status == "active"
      assert Repo.aggregate(from(key in APIKey, where: key.pool_id == ^first.pool.id), :count) == 1
    end)
  end

  test "real_traffic seeds an empty dedicated Pool and keeps exactly one active seed key across reruns" do
    on_exit(fn -> File.rm(RealTraffic.env_path()) end)

    first = Seeds.real_traffic()
    second = Seeds.real_traffic()

    assert first.pool.id == second.pool.id
    assert %Pool{slug: "dev-real-traffic", status: "active"} = second.pool
    assert second.revoked_api_keys == 1
    refute Repo.exists?(from(a in PoolUpstreamAssignment, where: a.pool_id == ^second.pool.id))
    assert [active] = Repo.all(from(k in APIKey, where: k.pool_id == ^second.pool.id and k.status == "active"))
    assert active.id == second.api_key.id

    assert File.stat!(Path.dirname(RealTraffic.env_path())).mode |> Bitwise.band(0o777) == 0o700
    assert File.stat!(RealTraffic.env_path()).mode |> Bitwise.band(0o777) == 0o600
    assert File.read!(RealTraffic.env_path()) =~ "CODEX_POOLER_REAL_TRAFFIC_POOL_SLUG=dev-real-traffic\n"
  end

  test "the import task accepts the real-traffic Pool and refuses the seeded synthetic Pool" do
    on_exit(fn -> File.rm(RealTraffic.env_path()) end)

    perf = Seeds.perf(upstream_base_url: @in_cluster)
    real = Seeds.real_traffic()
    scope = Scope.for_user(Repo.get!(User, real.pool.created_by_user_id), ["instance_owner"])
    {bundle, account_id} = bundle!()
    {:ok, %{import_options: options}} = UpstreamAccountBundle.parse_import_args(["b.bin", "--pool", "x"])

    assert {:error, %{code: :target_pool_has_synthetic_sources}} = UpstreamAccountBundle.import_bundle(bundle, perf.pool, scope, @password, options)
    assert {:ok, %{imported: 1}} = UpstreamAccountBundle.import_bundle(bundle, real.pool, scope, @password, options)

    identity = Upstreams.get_upstream_identity_by_chatgpt_account(account_id)
    assert [%{pool_id: pool_id}] = Repo.all(from(a in PoolUpstreamAssignment, where: a.upstream_identity_id == ^identity.id))
    assert pool_id == real.pool.id
  end

  defp pool_by_slug(slug), do: Repo.get_by(Pool, slug: slug)

  # The base URL the gateway would dispatch to for each active assignment.
  defp resolved_base_urls(%Pool{id: pool_id}) do
    Repo.all(
      from assignment in PoolUpstreamAssignment,
        join: identity in UpstreamIdentity,
        on: identity.id == assignment.upstream_identity_id,
        where: assignment.pool_id == ^pool_id,
        select: {identity, assignment}
    )
    |> Enum.map(fn {identity, assignment} -> EndpointMetadata.base_url(identity, assignment) end)
    |> Enum.uniq()
  end

  defp bundle! do
    source_pool = pool_fixture()
    unique = System.unique_integer([:positive])
    account_id = "acct_seed_target_#{unique}"

    fixture =
      active_upstream_assignment_fixture(source_pool, %{
        chatgpt_account_id: account_id,
        account_email: "seed-target-#{unique}@example.com",
        account_label: "Synthetic seed target #{unique}",
        access_token: "synthetic-access-token-#{unique}"
      })

    fixture.identity |> Ecto.Changeset.change() |> UpstreamIdentity.put_credential_provenance(:codex_chatgpt) |> Repo.update!()
    assert {:ok, bundle, %{exported: 1}} = UpstreamAccountBundle.export_bundle(source_pool, @password, refresh_tokens: :omit)

    Repo.delete!(Repo.reload!(fixture.assignment))
    Repo.delete_all(from secret in EncryptedSecret, where: secret.upstream_identity_id == ^fixture.identity.id)
    Repo.delete!(Repo.reload!(fixture.identity))

    {bundle, account_id}
  end
end
