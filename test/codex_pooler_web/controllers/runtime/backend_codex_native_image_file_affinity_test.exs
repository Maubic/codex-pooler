defmodule CodexPoolerWeb.Runtime.BackendCodexNativeImageFileAffinityTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @path "/backend-api/codex/images/edits"

  for mode <- ["full", "lite"] do
    @mode mode
    test "#{mode} native edits route bridged images to their holding assignment", %{conn: conn} do
      held_upstream = start_upstream(FakeUpstream.json_response(%{"created" => 1, "data" => [], "fixture" => "held"}))
      other_upstream = start_upstream(FakeUpstream.json_response(%{"created" => 1, "data" => [], "fixture" => "other"}))
      setup = gateway_setup(other_upstream)
      held = gateway_upstream(setup.pool, held_upstream, "synthetic-held-token", compact?: false)
      prime_routing_quota!(held.identity)
      file = response_affinity_file_fixture(setup, held.assignment, held.identity, file_id: "file-native-image-#{System.unique_integer([:positive])}", filename: "sample.png", status: "uploaded", finalize_status: "succeeded")
      setup = %{setup | model: put_model_source_assignments!(setup.model, [setup.assignment, held.assignment])}
      set_model_serving_mode!(model_serving_scope(), setup, @mode)
      use_routing_strategy!(setup.pool, "bridge_ring", 2)
      seed = seed_preferring_assignment([setup.assignment.id, held.assignment.id], setup.assignment.id)
      images = [%{"file_id" => file.file_id}, %{"image_url" => "data:image/png;base64,c3ludGhldGlj"}]
      payload = %{"model" => "gpt-image-2", "prompt" => "synthetic", "images" => images, "background" => "transparent", "n" => 1}
      response = conn |> put_req_header("x-request-id", seed) |> auth(setup) |> post(@path, payload)
      assert %{"fixture" => "held"} = json_response(response, 200)
      assert FakeUpstream.count(other_upstream) == 0
      assert [captured] = FakeUpstream.requests(held_upstream)
      assert captured.path == @path
      assert captured.json["images"] == images
      assert captured.json["background"] == "transparent"
      assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
      assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
      assert attempt.pool_upstream_assignment_id == held.assignment.id
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
    end
  end

  for {status, finalize_status, expired?, http_status, code} <- [
        {"pending_upload", "pending", false, 409, "file_not_ready"},
        {"pending_upload", "failed", false, 409, "file_not_ready"},
        {"uploaded", "succeeded", true, 404, "file_not_found"}
      ] do
    @status status
    @finalize_status finalize_status
    @expired expired?
    @http_status http_status
    @code code
    test "native edits refuse #{@status}/#{@finalize_status} expired=#{@expired} bridge files before reservation", %{conn: conn} do
      upstream = start_upstream(FakeUpstream.json_response(%{"unexpected" => true}))
      setup = gateway_setup(upstream)
      expires_at = DateTime.add(DateTime.utc_now(), if(@expired, do: -60, else: 3600), :second)
      file = response_affinity_file_fixture(setup, setup.assignment, setup.identity, file_id: "file-state-#{System.unique_integer([:positive])}", status: @status, finalize_status: @finalize_status, expires_at: expires_at)
      response = conn |> auth(setup) |> post(@path, edit_payload([%{"file_id" => file.file_id}]))
      assert %{"error" => %{"code" => @code, "param" => "file_id"}} = json_response(response, @http_status)
      assert_no_dispatch(setup, upstream)
    end
  end

  test "native edits refuse bridge files held by different assignments before reservation", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.json_response(%{"unexpected" => true}))
    setup = gateway_setup(upstream)
    other = gateway_upstream(setup.pool, upstream, "synthetic-other-token", compact?: false)
    prime_routing_quota!(other.identity)

    images =
      for {assignment, identity} <- [{setup.assignment, setup.identity}, {other.assignment, other.identity}] do
        file = response_affinity_file_fixture(setup, assignment, identity, file_id: "file-conflict-#{System.unique_integer([:positive])}", status: "uploaded", finalize_status: "succeeded")
        %{"file_id" => file.file_id}
      end

    response = conn |> auth(setup) |> post(@path, edit_payload(images))
    assert %{"error" => %{"code" => "file_assignment_conflict"}} = json_response(response, 409)
    assert_no_dispatch(setup, upstream)
  end

  for scope <- [:same_pool_other_key, :other_pool, :opaque, :whitespace, :non_list] do
    @scope scope
    test "native edits keep #{@scope} image references outside caller bridge affinity", %{conn: conn} do
      held_upstream = start_upstream(FakeUpstream.json_response(%{"fixture" => "held"}))
      other_upstream = start_upstream(FakeUpstream.json_response(%{"fixture" => "other"}))
      setup = gateway_setup(other_upstream)
      held = gateway_upstream(setup.pool, held_upstream, "synthetic-held-token", compact?: false)
      prime_routing_quota!(held.identity)

      owner =
        case @scope do
          :same_pool_other_key -> Map.put(setup, :api_key, CodexPooler.PoolerFixtures.active_api_key_fixture(setup.pool).api_key)
          :other_pool -> gateway_setup(held_upstream)
          _ -> setup
        end

      file = response_affinity_file_fixture(owner, held.assignment, held.identity, file_id: "file-scope-#{System.unique_integer([:positive])}", status: "uploaded", finalize_status: "succeeded")
      setup = %{setup | model: put_model_source_assignments!(setup.model, [setup.assignment, held.assignment])}
      use_routing_strategy!(setup.pool, "bridge_ring", 2)
      seed = seed_preferring_assignment([setup.assignment.id, held.assignment.id], setup.assignment.id)

      file_id =
        case @scope do
          :opaque -> "file-host-unbridged"
          :whitespace -> " " <> file.file_id <> " "
          _ -> file.file_id
        end

      images = if @scope == :non_list, do: %{"file_id" => file_id}, else: [%{"file_id" => file_id}]
      response = conn |> put_req_header("x-request-id", seed) |> auth(setup) |> post(@path, edit_payload(images))
      assert %{"fixture" => "other"} = json_response(response, 200)
      assert FakeUpstream.count(held_upstream) == 0
      assert [captured] = FakeUpstream.requests(other_upstream)
      assert captured.json["images"] == images
    end
  end

  defp edit_payload(images), do: %{"model" => "gpt-image-2", "prompt" => "synthetic", "images" => images, "background" => "opaque"}

  defp assert_no_dispatch(setup, upstream) do
    assert FakeUpstream.count(upstream) == 0
    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert request.status == "rejected"
    assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 0
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id), :count) == 0
  end
end
