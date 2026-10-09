defmodule CodexPoolerWeb.Runtime.BackendFileSettingsTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPooler.PoolerFixtures
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [auth: 2, start_upstream: 1]

  alias CodexPooler.Accounting.Request
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.AccountsFixtures
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Files
  alias CodexPooler.Files.FileRecord
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.InstanceSettings
  alias CodexPooler.Repo
  alias CodexPooler.TestAppEnv

  setup do
    TestAppEnv.restore_on_exit(OperationalSettings)
    Application.put_env(:codex_pooler, OperationalSettings, use_instance_settings?: true)

    %{user: user} = AccountsFixtures.bootstrap_owner_fixture()
    scope = Scope.for_user(user, ["instance_owner"])
    access = active_api_key_fixture()
    upstream = start_upstream(FakeUpstream.file_protocol_success())

    active_upstream_assignment_fixture(access.pool, %{
      metadata: %{"base_url" => FakeUpstream.url(upstream)},
      access_token: "synthetic-file-settings-token"
    })

    %{scope: scope, access: access, upstream: upstream}
  end

  test "saved file limit accepts its exact boundary and rejects larger metadata before dispatch", context do
    save_limit!(context.scope, 64)
    assert_create(context, 64, 200)
    assert_create(context, 65, 400)
  end

  test "saving a larger file limit permits metadata above the default without changing its default", context do
    assert InstanceSettings.get!().files.max_size_bytes == 25 * 1024 * 1024
    assert Files.max_file_size_bytes() == 25 * 1024 * 1024

    save_limit!(context.scope, 32 * 1024 * 1024)
    assert_create(context, 25 * 1024 * 1024 + 1, 200)
    assert_create(context, 32 * 1024 * 1024, 200)
    assert_create(context, 32 * 1024 * 1024 + 1, 400)
  end

  test "successive owner saves update new file decisions through the live settings cache", context do
    save_limit!(context.scope, 64)
    assert_create(context, 65, 400)

    save_limit!(context.scope, 128)
    assert_create(context, 65, 200)

    save_limit!(context.scope, 32)
    assert_create(context, 65, 400)
    assert_create(context, 32, 200)
  end

  test "explicit file overrides retain precedence and invalid overrides use the saved limit", context do
    config = TestAppEnv.restore_on_exit(Files)
    save_limit!(context.scope, 64)

    for {override, expected_status} <- [{32, 400}, {128, 200}] do
      Application.put_env(:codex_pooler, Files, Keyword.put(config, :max_file_size_bytes, override))
      assert_create(context, 65, expected_status)
    end

    for invalid <- [nil, 0, -1, "128"] do
      Application.put_env(:codex_pooler, Files, Keyword.put(config, :max_file_size_bytes, invalid))
      assert_create(context, 64, 200)
      assert_create(context, 65, 400)
    end
  end

  test "saved file limits preserve authentication before parsing on both file routes", context do
    save_limit!(context.scope, 1)
    requests_before = Repo.aggregate(Request, :count)
    files_before = Repo.aggregate(FileRecord, :count)

    for {path, content_type, body} <- [
          {"/backend-api/files", "application/json", "{"},
          {"/v1/files", "multipart/form-data; boundary=example", "invalid multipart fixture"}
        ] do
      conn =
        build_conn()
        |> put_req_header("content-type", content_type)
        |> post(path, body)

      assert conn.status == 401
      assert json_response(conn, 401)["error"]["code"] == "api_key_missing"
      assert %Plug.Conn.Unfetched{} = conn.body_params
    end

    assert FakeUpstream.requests(context.upstream) == []
    assert Repo.aggregate(FileRecord, :count) == files_before
    assert Repo.aggregate(Request, :count) == requests_before
  end

  defp save_limit!(scope, bytes) do
    settings = InstanceSettings.get!()
    assert {:ok, updated} = InstanceSettings.update(scope, settings, %{"files" => %{"max_size_bytes" => bytes}})
    assert updated.files.max_size_bytes == bytes
    assert Repo.get!(InstanceSettings.Settings, true).files.max_size_bytes == bytes
    assert InstanceSettings.current().files.max_size_bytes == bytes
    assert OperationalSettings.current().file_max_size_bytes == bytes
  end

  defp assert_create(context, size, expected_status) do
    file_id = "file_settings_#{System.unique_integer([:positive])}"
    FakeUpstream.set_mode(context.upstream, FakeUpstream.file_protocol_success(file_id: file_id))
    calls_before = length(FakeUpstream.requests(context.upstream))

    conn =
      build_conn()
      |> auth(context.access)
      |> put_req_header("content-type", "application/json")
      |> post(~p"/backend-api/files", %{"file_name" => "sample.txt", "file_size" => size})

    assert conn.status == expected_status

    if expected_status == 200 do
      assert json_response(conn, 200)["file_id"] == file_id
      file = Repo.get_by!(FileRecord, file_id: file_id)
      assert file.byte_size == size
      assert file.pool_id == context.access.pool.id
      assert file.api_key_id == context.access.api_key.id
      assert length(FakeUpstream.requests(context.upstream)) == calls_before + 1
    else
      assert json_response(conn, 400)["error"]["code"] == "invalid_request"
      refute Repo.get_by(FileRecord, file_id: file_id)
      assert length(FakeUpstream.requests(context.upstream)) == calls_before
    end
  end
end
