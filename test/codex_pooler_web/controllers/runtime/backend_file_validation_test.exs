defmodule CodexPoolerWeb.Runtime.BackendFileValidationTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import ExUnit.CaptureLog
  import CodexPooler.PoolerFixtures
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [auth: 2, gateway_setup: 2, start_upstream: 1]

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.Audit.AuditEvent
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Files
  alias CodexPooler.Files.FileRecord
  alias CodexPooler.Gateway
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Transports.FileBridge
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Plugs.BackendFilesMultipartGuard
  alias CodexPoolerWeb.Plugs.RuntimeIngress.CompressedBody
  alias CodexPoolerWeb.Plugs.RuntimeIngress.Path, as: IngressPath

  setup do
    old_config = Application.get_env(:codex_pooler, Files, [])
    CodexPooler.TestAppEnv.restore_on_exit(FileBridge)

    Application.put_env(:codex_pooler, Files,
      max_file_size_bytes: 64,
      file_ttl_seconds: 60
    )

    Application.put_env(:codex_pooler, FileBridge,
      finalize_retry_timeout_ms: 1_000,
      finalize_retry_interval_ms: 0
    )

    on_exit(fn -> Application.put_env(:codex_pooler, Files, old_config) end)

    :ok
  end

  test "authenticated backend file create rejects malformed JSON before upstream dispatch", %{
    conn: _conn
  } do
    upstream = start_upstream(FakeUpstream.file_protocol_success(file_id: "file_malformed_json"))
    setup = active_api_key_fixture()

    active_upstream_assignment_fixture(setup.pool, %{
      chatgpt_account_id: "acct_malformed_json_create",
      metadata: %{"base_url" => FakeUpstream.url(upstream)},
      access_token: "malformed-json-create-token"
    })

    request_count_before = Repo.aggregate(Request, :count)

    conn =
      Plug.Test.conn("POST", "/backend-api/files", ~s({"file_name":))
      |> put_req_header("content-type", "application/json")
      |> auth(setup)
      |> @endpoint.call(@endpoint.init([]))

    assert %{
             "error" => %{
               "code" => "invalid_request",
               "message" => "request body must be valid JSON"
             }
           } = json_response(conn, 400)

    assert FakeUpstream.requests(upstream) == []
    assert Repo.aggregate(Request, :count) == request_count_before
    assert Repo.aggregate(FileRecord, :count) == 0
  end

  test "rejects JSON two-step create when no upstream assignment is available", %{conn: conn} do
    setup = active_api_key_fixture()
    sensitive_filename = "private-json-name.txt"
    file_count_before = Repo.aggregate(FileRecord, :count)

    conn =
      conn
      |> auth(setup)
      |> put_req_header("content-type", "application/json")
      |> post(~p"/backend-api/files", %{
        "file_name" => sensitive_filename,
        "file_size" => 12,
        "use_case" => "codex"
      })

    response = json_response(conn, 503)
    assert response["error"]["code"] == "no_eligible_backend"
    refute Map.has_key?(response, "upload_url")
    refute Map.has_key?(response, "download_url")
    refute inspect(response) =~ sensitive_filename
    assert Repo.aggregate(FileRecord, :count) == file_count_before

    failed_request = Repo.one!(from request in Request, where: request.pool_id == ^setup.pool.id)
    assert failed_request.status == "failed"
    assert failed_request.transport == "http_json"
    assert failed_request.request_metadata["error_code"] == "no_eligible_backend"
    refute inspect(failed_request.request_metadata) =~ sensitive_filename
    refute inspect(failed_request.request_metadata) =~ "upload_url"
    refute inspect(failed_request.request_metadata) =~ "download_url"
  end

  test "logs sanitized file bridge transport exceptions", %{conn: conn} do
    setup = active_api_key_fixture()
    sensitive_filename = "private-transport-name.txt"
    sensitive_token = "sensitive-file-transport-token"

    %{assignment: assignment, identity: identity} =
      active_upstream_assignment_fixture(setup.pool, %{
        access_token: sensitive_token,
        metadata: %{"base_url" => "http://127.0.0.1:1"}
      })

    logs =
      capture_log(fn ->
        conn =
          conn
          |> auth(setup)
          |> put_req_header("content-type", "application/json")
          |> post(~p"/backend-api/files", %{
            "file_name" => sensitive_filename,
            "file_size" => 12,
            "use_case" => "codex"
          })

        assert json_response(conn, 502)["error"]["code"] == "upstream_request_failed"
      end)

    assert logs =~ "file bridge transport failed"
    assert logs =~ "operation=create"
    assert logs =~ "endpoint=/backend-api/files"
    assert logs =~ "pool_upstream_assignment_id=#{assignment.id}"
    assert logs =~ "upstream_identity_id=#{identity.id}"
    assert logs =~ "exception="
    refute logs =~ sensitive_filename
    refute logs =~ sensitive_token
    refute logs =~ "authorization"
  end

  @tag :file_bridge_auth_before_upstream
  test "unauthenticated create fails before file side effects", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.file_protocol_success(file_id: "file_unauth_create"))

    setup = active_api_key_fixture()

    active_upstream_assignment_fixture(setup.pool, %{
      chatgpt_account_id: "acct_unauthenticated_create",
      metadata: %{"base_url" => FakeUpstream.url(upstream)},
      access_token: "unauthenticated-create-token"
    })

    file_count_before = Repo.aggregate(FileRecord, :count)
    request_count_before = Repo.aggregate(Request, :count)

    conn =
      conn
      |> put_req_header("content-type", "application/json")
      |> post(~p"/backend-api/files", %{
        "file_name" => "unauthenticated.txt",
        "file_size" => 20,
        "use_case" => "codex"
      })

    assert json_response(conn, 401)["error"]["code"] == "api_key_missing"
    assert Repo.aggregate(FileRecord, :count) == file_count_before
    assert Repo.aggregate(Request, :count) == request_count_before
    assert FakeUpstream.requests(upstream) == []
  end

  @tag :file_bridge_auth_before_upstream
  test "unauthenticated finalize fails before upstream side effects", %{conn: conn} do
    setup = active_api_key_fixture()

    upstream =
      start_upstream(
        FakeUpstream.file_protocol_success(
          file_id: "file_unauth_finalize",
          file_name: "unauth-finalize.txt",
          mime_type: "text/plain"
        )
      )

    active_upstream_assignment_fixture(setup.pool, %{
      chatgpt_account_id: "acct_unauthenticated_finalize",
      metadata: %{"base_url" => FakeUpstream.url(upstream)},
      access_token: "unauthenticated-finalize-token"
    })

    create_conn =
      conn
      |> auth(setup)
      |> put_req_header("content-type", "application/json")
      |> post(~p"/backend-api/files", %{
        "file_name" => "unauth-finalize.txt",
        "file_size" => 20,
        "use_case" => "codex"
      })

    file_id = json_response(create_conn, 200)["file_id"]
    request_count_before = Repo.aggregate(Request, :count)

    finalize_conn = post(build_conn(), ~p"/backend-api/files/#{file_id}/uploaded", %{})

    assert json_response(finalize_conn, 401)["error"]["code"] == "api_key_missing"
    assert Repo.aggregate(Request, :count) == request_count_before
    assert [%{path: "/backend-api/files"}] = FakeUpstream.requests(upstream)
    assert Repo.get_by!(FileRecord, file_id: file_id).status == "pending_upload"
  end

  test "rejects expired file finalize", %{conn: conn} do
    setup = active_api_key_fixture()

    upstream =
      start_upstream(
        FakeUpstream.file_protocol_success(
          file_id: "file_expired_bridge",
          file_name: "expired.txt",
          mime_type: "text/plain"
        )
      )

    active_upstream_assignment_fixture(setup.pool, %{
      chatgpt_account_id: "acct_file_expired_bridge",
      metadata: %{"base_url" => FakeUpstream.url(upstream)},
      access_token: "file-expired-bridge-token"
    })

    conn =
      conn
      |> auth(setup)
      |> put_req_header("content-type", "application/json")
      |> post(~p"/backend-api/files", %{
        "file_name" => "expired.txt",
        "file_size" => 13,
        "use_case" => "codex"
      })

    file_id = json_response(conn, 200)["file_id"]

    expired_at =
      DateTime.utc_now() |> DateTime.add(-1, :second) |> DateTime.truncate(:microsecond)

    Repo.get_by!(FileRecord, file_id: file_id)
    |> Ecto.Changeset.change(%{expires_at: expired_at})
    |> Repo.update!()

    conn =
      build_conn()
      |> auth(setup)
      |> post(~p"/backend-api/files/#{file_id}/uploaded", %{})

    assert json_response(conn, 410)["error"]["code"] == "file_expired"
    assert Repo.get_by!(FileRecord, file_id: file_id).status == "expired"

    failed_request =
      Repo.one!(
        from request in Request,
          where:
            request.pool_id == ^setup.pool.id and
              request.endpoint == "/backend-api/files/uploaded",
          order_by: [desc: request.admitted_at],
          limit: 1
      )

    assert failed_request.status == "failed"
    assert failed_request.response_status_code == 410
    assert failed_request.request_metadata["error_code"] == "file_expired"
  end

  test "rejects oversized files before persistence", %{conn: conn} do
    setup = active_api_key_fixture()
    file_count_before = Repo.aggregate(FileRecord, :count)
    request_count_before = Repo.aggregate(Request, :count)

    conn =
      conn
      |> auth(setup)
      |> put_req_header("content-type", "application/json")
      |> post(~p"/backend-api/files", %{
        "file_name" => "large.txt",
        "file_size" => 65,
        "use_case" => "codex"
      })

    assert json_response(conn, 400)["error"]["code"] == "invalid_request"
    assert Repo.aggregate(FileRecord, :count) == file_count_before
    assert Repo.aggregate(Request, :count) == request_count_before
  end

  test "rejects unsupported use_case", %{conn: conn} do
    setup = active_api_key_fixture()
    file_count_before = Repo.aggregate(FileRecord, :count)
    request_count_before = Repo.aggregate(Request, :count)

    purpose_conn =
      conn
      |> auth(setup)
      |> put_req_header("content-type", "application/json")
      |> post(~p"/backend-api/files", %{
        "file_name" => "purpose.txt",
        "file_size" => 4,
        "use_case" => "user_data"
      })

    assert json_response(purpose_conn, 400)["error"]["code"] == "invalid_request"
    assert Repo.aggregate(FileRecord, :count) == file_count_before
    assert Repo.aggregate(Request, :count) == request_count_before
  end

  for {subtype, encoding, budget} <- [
        {"form-data", nil, 1_024},
        {"mixed", nil, 1_024},
        {"form-data", "gzip", 1_024},
        {"mixed", "gzip", 1_024},
        {"form-data", "gzip", 16},
        {"mixed", "gzip", 16}
      ] do
    @tag :multipart_preparse
    test "multipart #{subtype} encoding=#{inspect(encoding)} budget=#{budget} is rejected before reading or storage" do
      settings = CodexPooler.TestAppEnv.restore_on_exit(OperationalSettings)
      Application.put_env(:codex_pooler, OperationalSettings, Keyword.merge(settings, use_instance_settings?: false, settings: %OperationalSettings{max_decompressed_body_bytes: unquote(budget)}))
      setup = active_api_key_fixture()
      upstream = start_upstream(FakeUpstream.file_protocol_success(file_id: "file_multipart_guard"))
      active_upstream_assignment_fixture(setup.pool, %{metadata: %{"base_url" => FakeUpstream.url(upstream)}})

      with_isolated_plug_tmpdir(fn tmp_root ->
        before_counts = file_side_effect_counts()
        body = multipart_body("sample.txt", "synthetic upload")
        encoding = unquote(encoding)
        encoded_body = encode_multipart(body, encoding)

        conn =
          Plug.Test.conn("POST", "/backend-api/files", encoded_body)
          |> put_req_header("content-type", "multipart/#{unquote(subtype)}; boundary=#{multipart_boundary()}")
          |> auth(setup)

        conn = maybe_put_encoding(conn, encoding)
        response = @endpoint.call(conn, @endpoint.init([]))
        {Plug.Adapters.Test.Conn, adapter} = response.adapter

        observed = %{
          status: response.status,
          code: CodexPooler.JSON.decode!(response.resp_body)["error"]["code"],
          body_unfetched: match?(%Plug.Conn.Unfetched{}, response.body_params),
          unread_bytes: byte_size(adapter.req_body),
          upload_entries: length(tmpdir_paths(tmp_root))
        }

        assert observed == %{status: 400, code: "unsupported_multipart_file_create", body_unfetched: true, unread_bytes: byte_size(encoded_body), upload_entries: 0}
        assert file_side_effect_counts() == before_counts
        assert FakeUpstream.requests(upstream) == []
      end)
    end
  end

  @tag :multipart_live_preparse
  test "live multipart file-create refusals skip reads inflate and uploads while JSON and transcription remain usable" do
    previous = CodexPooler.TestAppEnv.restore_on_exit(OperationalSettings)
    Application.put_env(:codex_pooler, OperationalSettings, Keyword.merge(previous, use_instance_settings?: false, settings: %OperationalSettings{max_decompressed_body_bytes: 16}))
    setup = active_api_key_fixture()
    upstream = start_upstream(FakeUpstream.file_protocol_success(file_id: "file_live_guard"))
    active_upstream_assignment_fixture(setup.pool, %{metadata: %{"base_url" => FakeUpstream.url(upstream)}})

    with_isolated_plug_tmpdir(fn tmp_root ->
      {listener, port} = start_traced_file_endpoint!(tmp_root)
      before_counts = file_side_effect_counts()

      for subtype <- ["mixed", "form-data", "RELATED"],
          encoding <- [nil, "gzip"],
          authorization <- [setup.authorization, "Bearer invalid-key"] do
        body = multipart_body("sample.wav", "synthetic upload")
        body = if encoding, do: :zlib.gzip(body), else: body
        headers = [{"authorization", authorization}, {"content-type", "multipart/#{subtype}; boundary=#{multipart_boundary()}"}]
        headers = if encoding, do: [{"content-encoding", encoding} | headers], else: headers
        response = Req.post!("http://127.0.0.1:#{port}/backend-api/%66iles", headers: headers, body: body, decode_body: false, retry: false)
        observed = file_http_observation!()
        code = CodexPooler.JSON.decode!(response.body)["error"]["code"]
        expected_status = if authorization == setup.authorization, do: 400, else: 401
        expected_code = if authorization == setup.authorization, do: "unsupported_multipart_file_create", else: "api_key_missing"

        assert Map.merge(observed, %{status: response.status, code: code}) == %{
                 status: expected_status,
                 code: expected_code,
                 body_unfetched: true,
                 body_reads: 0,
                 multipart_reads: 0,
                 compressed_decodes: 0,
                 inflates: 0,
                 upload_files: 0
               }
      end

      assert file_side_effect_counts() == before_counts
      assert FakeUpstream.requests(upstream) == []

      Application.put_env(:codex_pooler, OperationalSettings, Keyword.merge(previous, use_instance_settings?: false, settings: %OperationalSettings{max_decompressed_body_bytes: 1_024}))
      json_body = CodexPooler.JSON.encode!(%{"file_name" => "sample.txt", "file_size" => 12})
      json_response = Req.post!("http://127.0.0.1:#{port}/backend-api/files", headers: [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"content-encoding", "gzip"}], body: :zlib.gzip(json_body), decode_body: false, retry: false)
      assert json_response.status == 200
      json_observed = file_http_observation!()
      assert json_observed.compressed_decodes == 1
      assert json_observed.inflates == 1
      assert json_observed.body_reads > 0
      assert json_observed.upload_files == 0
      refute json_observed.body_unfetched
      assert Repo.aggregate(FileRecord, :count) == before_counts[FileRecord] + 1
      assert length(FakeUpstream.requests(upstream)) == 1

      media_upstream = start_upstream(FakeUpstream.json_response(%{"text" => "synthetic transcript"}))
      media = gateway_setup(media_upstream, exposed_model_id: Gateway.backend_transcription_model(), upstream_model_id: "sample-transcription", model_metadata: %{"input_modalities" => ["audio"], "modes" => ["transcription"]})
      media_response = Req.post!("http://127.0.0.1:#{port}/backend-api/transcribe", headers: [{"authorization", media.authorization}, {"content-type", "multipart/form-data; boundary=#{multipart_boundary()}"}], body: multipart_body("sample.wav", "synthetic audio"), decode_body: false, retry: false)
      assert media_response.status == 200
      media_observed = file_http_observation!()
      assert media_observed.multipart_reads > 0
      assert media_observed.upload_files > 0
      assert media_observed.compressed_decodes == 0
      assert media_observed.inflates == 0
      refute media_observed.body_unfetched
      assert length(FakeUpstream.requests(media_upstream)) == 1

      stop_supervised!(:file_ingress_listener)
      refute Process.alive?(listener)
    end)
  end

  @tag :rejects_multipart_without_storage
  test "rejects multipart without local storage side effects" do
    setup = active_api_key_fixture()

    with_isolated_plug_tmpdir(fn tmp_root ->
      file_count_before = Repo.aggregate(FileRecord, :count)
      request_count_before = Repo.aggregate(Request, :count)
      audit_count_before = Repo.aggregate(AuditEvent, :count)

      conn =
        Plug.Test.conn(
          "POST",
          "/backend-api/files",
          multipart_body("private-upload.txt", "private multipart bytes")
        )
        |> put_req_header("content-type", "multipart/form-data; boundary=#{multipart_boundary()}")
        |> auth(setup)
        |> @endpoint.call(@endpoint.init([]))

      response = json_response(conn, 400)
      assert response["error"]["code"] == "unsupported_multipart_file_create"
      refute inspect(response) =~ "private-upload.txt"
      assert Repo.aggregate(FileRecord, :count) == file_count_before
      assert Repo.aggregate(Request, :count) == request_count_before
      assert Repo.aggregate(AuditEvent, :count) == audit_count_before
      assert tmpdir_paths(tmp_root) == []
    end)
  end

  test "unauthenticated multipart create fails with api_key_missing before parser side effects" do
    with_isolated_plug_tmpdir(fn tmp_root ->
      file_count_before = Repo.aggregate(FileRecord, :count)
      request_count_before = Repo.aggregate(Request, :count)

      conn =
        Plug.Test.conn(
          "POST",
          "/backend-api/files",
          multipart_body("unauthenticated.txt", "unauthenticated multipart body")
        )
        |> put_req_header("content-type", "multipart/form-data; boundary=#{multipart_boundary()}")
        |> @endpoint.call(@endpoint.init([]))

      assert json_response(conn, 401)["error"]["code"] == "api_key_missing"
      assert Repo.aggregate(FileRecord, :count) == file_count_before
      assert Repo.aggregate(Request, :count) == request_count_before
      assert tmpdir_paths(tmp_root) == []
    end)
  end

  test "encoded multipart file create reaches the same guard without parser side effects" do
    setup = active_api_key_fixture()

    with_isolated_plug_tmpdir(fn tmp_root ->
      file_count_before = Repo.aggregate(FileRecord, :count)
      request_count_before = Repo.aggregate(Request, :count)

      conn =
        Plug.Test.conn("POST", "/backend-api/%66iles", "invalid multipart fixture")
        |> put_req_header("content-type", "multipart/form-data; boundary=example")
        |> auth(setup)
        |> @endpoint.call(@endpoint.init([]))

      assert json_response(conn, 400)["error"]["code"] ==
               "unsupported_multipart_file_create"

      assert Repo.aggregate(FileRecord, :count) == file_count_before
      assert Repo.aggregate(Request, :count) == request_count_before
      assert tmpdir_paths(tmp_root) == []
    end)
  end

  test "multipart guard consumes the populated canonical path view" do
    setup = active_api_key_fixture()

    conn =
      Plug.Test.conn("POST", "/backend-api/%66iles", "invalid multipart fixture")
      |> put_req_header("content-type", "multipart/form-data; boundary=example")
      |> auth(setup)
      |> IngressPath.populate()
      |> Map.put(:path_info, ["unrelated"])
      |> BackendFilesMultipartGuard.call([])

    assert json_response(conn, 400)["error"]["code"] == "unsupported_multipart_file_create"
  end

  test "multipart guard reconstructs the canonical path when called directly" do
    setup = active_api_key_fixture()

    conn =
      Plug.Test.conn("POST", "/backend-api/%66iles", "invalid multipart fixture")
      |> put_req_header("content-type", "multipart/form-data; boundary=example")
      |> auth(setup)
      |> BackendFilesMultipartGuard.call([])

    assert json_response(conn, 400)["error"]["code"] == "unsupported_multipart_file_create"
  end

  test "cleanup marks expired file metadata rows", %{conn: conn} do
    setup = active_api_key_fixture()

    upstream =
      start_upstream(
        FakeUpstream.file_protocol_success(
          file_id: "file_cleanup_bridge",
          file_name: "cleanup.txt",
          mime_type: "text/plain"
        )
      )

    active_upstream_assignment_fixture(setup.pool, %{
      chatgpt_account_id: "acct_file_cleanup_bridge",
      metadata: %{"base_url" => FakeUpstream.url(upstream)},
      access_token: "file-cleanup-bridge-token"
    })

    conn =
      conn
      |> auth(setup)
      |> put_req_header("content-type", "application/json")
      |> post(~p"/backend-api/files", %{
        "file_name" => "cleanup.txt",
        "file_size" => 7,
        "use_case" => "codex"
      })

    file_id = json_response(conn, 200)["file_id"]
    file = Repo.get_by!(FileRecord, file_id: file_id)

    expired_at =
      DateTime.utc_now() |> DateTime.add(-1, :second) |> DateTime.truncate(:microsecond)

    file
    |> Ecto.Changeset.change(%{expires_at: expired_at})
    |> Repo.update!()

    assert {:ok, %{abandoned_files: 1, expired_files: 0}} =
             Files.cleanup_expired(DateTime.utc_now())

    assert Repo.get!(FileRecord, file.id).status == "abandoned"
  end

  defp encode_multipart(body, nil), do: body
  defp encode_multipart(body, "gzip"), do: :zlib.gzip(body)

  defp maybe_put_encoding(conn, nil), do: conn
  defp maybe_put_encoding(conn, encoding), do: put_req_header(conn, "content-encoding", encoding)

  defp start_traced_file_endpoint!(tmp_root) do
    observer = self()
    traced_calls = [{Plug.Conn, :read_body, 2}, {Plug.Conn, :read_part_headers, 2}, {CompressedBody, :decode, 2}, {CompressedBody, :decompress_with_timeout, 3}]

    on_exit(fn ->
      for mfa <- traced_calls, do: :erlang.trace_pattern(mfa, false, [:local])
    end)

    for {module, _function, _arity} = mfa <- traced_calls do
      Code.ensure_loaded!(module)
      assert :erlang.trace_pattern(mfa, true, [:local]) == 1
    end

    probe = fn conn, _opts ->
      :erlang.trace(self(), true, [:call, :arity, {:tracer, observer}])

      try do
        result = @endpoint.call(conn, @endpoint.init([]))
        :erlang.trace(self(), false, [:call])
        delivered = :erlang.trace_delivered(self())

        receive do
          {:trace_delivered, _tracee, ^delivered} -> :ok
        after
          5_000 -> raise "file ingress trace barrier timed out"
        end

        upload_files = tmp_root |> Path.join("**/*") |> Path.wildcard() |> Enum.count(&File.regular?/1)
        send(observer, {:file_body_observed, self(), match?(%Plug.Conn.Unfetched{}, result.body_params), upload_files})
        result
      after
        :erlang.trace(self(), false, [:call])
      end
    end

    refute @endpoint.config(:debug_errors, false)
    listener = start_supervised!({Bandit, plug: probe, port: 0, ip: {127, 0, 0, 1}, startup_log: false}, id: :file_ingress_listener)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(listener)

    on_exit(fn ->
      refute Process.alive?(listener)
      assert {:error, :econnrefused} = :gen_tcp.connect({127, 0, 0, 1}, port, [], 1_000)
    end)

    {listener, port}
  end

  defp file_http_observation! do
    assert_receive {:file_body_observed, request_pid, body_unfetched, upload_files}, 5_000
    calls = drain_file_calls(request_pid, %{})

    %{
      body_unfetched: body_unfetched,
      upload_files: upload_files,
      body_reads: Map.get(calls, {Plug.Conn, :read_body, 2}, 0),
      multipart_reads: Map.get(calls, {Plug.Conn, :read_part_headers, 2}, 0),
      compressed_decodes: Map.get(calls, {CompressedBody, :decode, 2}, 0),
      inflates: Map.get(calls, {CompressedBody, :decompress_with_timeout, 3}, 0)
    }
  end

  defp drain_file_calls(request_pid, counts) do
    receive do
      {:trace, ^request_pid, :call, mfa} ->
        drain_file_calls(request_pid, Map.update(counts, mfa, 1, &(&1 + 1)))
    after
      0 -> counts
    end
  end

  defp file_side_effect_counts do
    for schema <- [FileRecord, Request, Attempt, AuditEvent], into: %{}, do: {schema, Repo.aggregate(schema, :count)}
  end

  defp multipart_boundary, do: "codex-pooler-multipart-boundary"

  defp multipart_body(filename, contents) do
    [
      "--#{multipart_boundary()}\r\n",
      "Content-Disposition: form-data; name=\"purpose\"\r\n\r\n",
      "user_data\r\n",
      "--#{multipart_boundary()}\r\n",
      "Content-Disposition: form-data; name=\"file\"; filename=\"#{filename}\"\r\n",
      "Content-Type: text/plain\r\n\r\n",
      contents,
      "\r\n--#{multipart_boundary()}--\r\n"
    ]
    |> IO.iodata_to_binary()
  end

  defp with_isolated_plug_tmpdir(fun) do
    tmp_root =
      Path.join(System.tmp_dir!(), "codex-pooler-plug-tmp-#{Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)}")

    sibling = tmp_root <> "-control"
    previous_upload_term = :persistent_term.get(Plug.Upload)

    # Also on_exit: the ExUnit timeout or a linked crash kills the test before `after` runs, and
    # every later upload in the run would target the directory this test removes.
    on_exit(fn ->
      :persistent_term.put(Plug.Upload, previous_upload_term)
      File.rm_rf!(tmp_root)
      File.rm(sibling)
      refute File.exists?(tmp_root)
      refute File.exists?(sibling)
    end)

    File.mkdir!(tmp_root)
    File.write!(sibling, "preserved", [:exclusive])
    :persistent_term.put(Plug.Upload, {[tmp_root], "test-upload-suffix"})
    :ets.delete(Plug.Upload.Dir, self())
    :ets.delete(Plug.Upload.Path, self())

    try do
      fun.(tmp_root)
    after
      :ets.delete(Plug.Upload.Dir, self())
      :ets.delete(Plug.Upload.Path, self())
      :persistent_term.put(Plug.Upload, previous_upload_term)
      File.rm_rf!(tmp_root)
      refute File.exists?(tmp_root)
      assert File.read!(sibling) == "preserved"
      File.rm!(sibling)
    end
  end

  defp tmpdir_paths(tmp_root) do
    case File.ls(tmp_root) do
      {:ok, entries} -> Enum.sort(entries)
      {:error, :enoent} -> []
    end
  end
end
