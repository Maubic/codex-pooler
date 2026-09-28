defmodule CodexPoolerWeb.V1.AudioControllerTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import ExUnit.CaptureLog

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, gateway_setup: 1, half_open_circuit!: 2, start_upstream: 1]

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway
  alias CodexPooler.Repo

  @tag :transcription_success
  test "POST /v1/audio/transcriptions canonicalizes gpt-transcribe across multipart and accounting",
       %{conn: conn} do
    alias_sentinel = "gpt-transcribe"
    prompt_sentinel = "audio prompt sentinel"
    keyword_sentinels = ["keyword alpha sentinel", "keyword alpha sentinel"]
    language_sentinels = ["language beta sentinel", "language gamma sentinel"]
    audio_sentinel = "audio bytes sentinel"
    filename_sentinel = "private-audio-sentinel.wav"
    transcript_sentinel = "transcript sentinel"
    response_language_sentinel = "response language sentinel"

    upstream =
      start_upstream(
        FakeUpstream.json_response(%{
          "text" => transcript_sentinel,
          "languages" => [response_language_sentinel],
          "duration" => 1.25,
          "segments" => [%{"id" => 0}]
        })
      )

    setup = upstream |> gateway_setup() |> use_transcription_model!()

    {conn, log} =
      with_log(fn ->
        conn
        |> auth(setup)
        |> post("/v1/audio/transcriptions", %{
          "model" => alias_sentinel,
          "file" => upload_fixture(filename_sentinel, "audio/wav", audio_sentinel),
          "prompt" => prompt_sentinel,
          "keywords" => keyword_sentinels,
          "languages" => language_sentinels
        })
      end)

    assert %{
             "text" => ^transcript_sentinel,
             "duration" => 1.25,
             "segments" => [%{"id" => 0}]
           } = response = json_response(conn, 200)

    refute Map.has_key?(response, "languages")

    assert [captured] = FakeUpstream.requests(upstream)
    assert captured.path == "/backend-api/transcribe"

    assert multipart_parts(captured) == [
             {:file, "file", "audio.wav"},
             {:text, "prompt", prompt_sentinel},
             {:text, "keywords[]", Enum.at(keyword_sentinels, 0)},
             {:text, "keywords[]", Enum.at(keyword_sentinels, 1)},
             {:text, "languages[]", Enum.at(language_sentinels, 0)},
             {:text, "languages[]", Enum.at(language_sentinels, 1)}
           ]

    refute Enum.any?(multipart_parts(captured), &match?({:text, "model", _value}, &1))

    assert [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
    assert request.endpoint == "/backend-api/transcribe"
    assert request.status == "succeeded"
    assert request.requested_model == Gateway.backend_transcription_model()
    assert request.model_id == setup.model.id
    assert request.request_metadata["requested_model"] == Gateway.backend_transcription_model()
    assert request.request_metadata["effective_model"] == Gateway.backend_transcription_model()

    assert [attempt] = Repo.all(from a in Attempt, where: a.request_id == ^request.id)
    assert attempt.status == "succeeded"
    assert attempt.model_id == setup.model.id

    assert Repo.aggregate(
             from(entry in LedgerEntry,
               where: entry.request_id == ^request.id and entry.entry_kind == "settlement"
             ),
             :count
           ) == 1

    ledger_entries = Repo.all(from entry in LedgerEntry, where: entry.request_id == ^request.id)

    assert Enum.sort(Enum.map(ledger_entries, & &1.entry_kind)) == [
             "release",
             "reservation",
             "settlement"
           ]

    assert Enum.all?(ledger_entries, &(&1.model_id == setup.model.id))

    persisted =
      inspect(%{
        request_metadata: request.request_metadata,
        attempt_metadata: attempt.response_metadata,
        attempt_error: attempt.error_message,
        ledger_details: Enum.map(ledger_entries, & &1.details),
        request_logs: Accounting.list_request_logs(setup.pool.id)
      })

    for sentinel <-
          [
            alias_sentinel,
            prompt_sentinel,
            audio_sentinel,
            filename_sentinel,
            transcript_sentinel,
            response_language_sentinel
          ] ++ keyword_sentinels ++ language_sentinels do
      refute persisted =~ sentinel
      refute log =~ sentinel
    end
  end

  @tag :transcription_success
  test "POST /v1/audio/transcriptions reuses transcription gateway and returns text", %{
    conn: conn
  } do
    transcript = "synthetic transcription"
    prompt = "synthetic glossary"
    audio_bytes = "synthetic audio bytes"
    upstream = start_upstream(FakeUpstream.json_response(%{"text" => transcript}))
    setup = upstream |> gateway_setup() |> use_transcription_model!()

    conn =
      conn
      |> auth(setup)
      |> post("/v1/audio/transcriptions", %{
        "model" => Gateway.backend_transcription_model(),
        "file" => upload_fixture("audio-secret.wav", "audio/wav", audio_bytes),
        "prompt" => prompt,
        "response_format" => "json"
      })

    assert %{"text" => ^transcript} = json_response(conn, 200)

    assert [captured] = FakeUpstream.requests(upstream)
    assert captured.path == "/backend-api/transcribe"
    assert captured.body =~ audio_bytes
    assert captured.body =~ prompt
    refute captured.body =~ setup.model.upstream_model_id
    refute captured.body =~ Gateway.backend_transcription_model()
    refute captured.body =~ "audio-secret.wav"
    assert captured.body =~ ~s(filename="audio.wav")

    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert request.endpoint == "/backend-api/transcribe"
    assert request.status == "succeeded"
    assert request.request_metadata["upload_bytes"] == byte_size(audio_bytes)

    assert get_in(request.request_metadata, ["openai_compatibility", "source_endpoint"]) ==
             "/v1/audio/transcriptions"

    assert get_in(request.request_metadata, ["openai_compatibility", "translated_endpoint"]) ==
             "/backend-api/transcribe"

    refute inspect(request.request_metadata) =~ transcript
    refute inspect(request.request_metadata) =~ prompt
    refute inspect(request.request_metadata) =~ audio_bytes
  end

  for caller_model <- ["gpt-4o-transcribe", "gpt-transcribe"],
      {kind, transcript} <- [
        {"nonempty", "  synthetic \"quoted\" transcript\nsecond line\tcaf\u00E9  "},
        {"silence", ""}
      ] do
    @caller_model caller_model
    @transcript transcript
    @tag :transcription_plain_text
    test "transcription returns exact #{kind} plain text for #{caller_model}", %{conn: conn} do
      upstream = start_upstream(FakeUpstream.json_response(%{"text" => @transcript, "languages" => ["synthetic-language"]}))
      setup = upstream |> gateway_setup() |> use_transcription_model!()

      response =
        conn
        |> auth(setup)
        |> post("/v1/audio/transcriptions", %{
          "model" => @caller_model,
          "file" => upload_fixture("audio.wav", "audio/wav", "synthetic bytes"),
          "response_format" => "text"
        })

      assert response(response, 200) == @transcript
      assert get_resp_header(response, "content-type") == ["text/plain; charset=utf-8"]
      assert [captured] = FakeUpstream.requests(upstream)
      assert multipart_parts(captured) == [{:file, "file", "audio.wav"}]

      assert [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
      assert request.status == "succeeded"
      assert request.requested_model == Gateway.backend_transcription_model()
      assert [attempt] = Repo.all(from a in Attempt, where: a.request_id == ^request.id)
      assert attempt.status == "succeeded"
      assert Repo.aggregate(from(entry in LedgerEntry, where: entry.request_id == ^request.id and entry.entry_kind == "settlement"), :count) == 1
      if @transcript != "", do: refute(inspect(request.request_metadata) =~ @transcript)
      refute inspect(attempt.response_metadata) =~ "synthetic-language"
    end
  end

  test "POST /v1/audio/transcriptions routes when the public audio model is not listed", %{
    conn: conn
  } do
    transcript = "hidden transcription"
    audio_bytes = "hidden audio bytes"
    upstream = start_upstream(FakeUpstream.json_response(%{"text" => transcript}))

    setup =
      upstream
      |> gateway_setup()
      |> allow_models!([Gateway.backend_transcription_model()])

    conn =
      conn
      |> auth(setup)
      |> post("/v1/audio/transcriptions", %{
        "model" => Gateway.backend_transcription_model(),
        "file" => upload_fixture("hidden-audio.wav", "audio/wav", audio_bytes)
      })

    assert %{"text" => ^transcript} = json_response(conn, 200)

    assert [captured] = FakeUpstream.requests(upstream)
    assert captured.path == "/backend-api/transcribe"
    refute captured.body =~ Gateway.backend_transcription_model()
    refute captured.body =~ setup.model.upstream_model_id
    refute captured.body =~ "language"
    refute captured.body =~ "response_format"
    refute captured.body =~ "temperature"

    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert request.endpoint == "/backend-api/transcribe"
    assert request.status == "succeeded"
    assert request.request_metadata["requested_model"] == Gateway.backend_transcription_model()
    assert request.request_metadata["effective_model"] == Gateway.backend_transcription_model()
    assert request.request_metadata["upload_bytes"] == byte_size(audio_bytes)
    refute inspect(request.request_metadata) =~ audio_bytes
  end

  for {field, value} <- [
        {"language", "en"},
        {"language", nil},
        {"temperature", 0},
        {"temperature", nil},
        {"response_format", "srt"},
        {"response_format", "verbose_json"},
        {"response_format", "vtt"},
        {"response_format", nil},
        {"response_format", %{}},
        {"prompt", %{"unexpected" => "shape"}},
        {"prompt", 42},
        {"prompt", []}
      ] do
    @field field
    @value value
    test "transcription rejects unsupported #{@field}=#{inspect(value)} before effects", %{
      conn: conn
    } do
      upstream = start_upstream(FakeUpstream.json_response(%{"text" => ""}))
      setup = upstream |> gateway_setup() |> use_transcription_model!()

      response =
        conn
        |> auth(setup)
        |> post("/v1/audio/transcriptions", %{
          "model" => "gpt-transcribe",
          "file" => upload_fixture("audio.wav", "audio/wav", "synthetic bytes"),
          @field => @value
        })

      assert %{"error" => %{"param" => field}} = json_response(response, 400)
      assert field == @field
      assert FakeUpstream.count(upstream) == 0
      assert Repo.aggregate(Request, :count) == 0
      assert Repo.aggregate(Attempt, :count) == 0
      assert Repo.aggregate(LedgerEntry, :count) == 0
    end
  end

  test "transcription treats a null prompt as absent", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.json_response(%{"text" => ""}))
    setup = upstream |> gateway_setup() |> use_transcription_model!()

    response =
      conn
      |> auth(setup)
      |> post("/v1/audio/transcriptions", %{
        "model" => "gpt-transcribe",
        "file" => upload_fixture("audio.wav", "audio/wav", "synthetic bytes"),
        "prompt" => nil
      })

    assert %{"text" => ""} = json_response(response, 200)
    assert [captured] = FakeUpstream.requests(upstream)
    assert multipart_parts(captured) == [{:file, "file", "audio.wav"}]
  end

  test "POST /v1/audio/transcriptions rejects invalid model before dispatch", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.json_response(%{"text" => "should not dispatch"}))
    setup = upstream |> gateway_setup() |> use_transcription_model!()

    conn =
      conn
      |> auth(setup)
      |> post("/v1/audio/transcriptions", %{
        "model" => "whisper-1",
        "file" => upload_fixture("invalid.wav", "audio/wav", "invalid audio")
      })

    assert %{"error" => %{"code" => "invalid_model", "param" => "model"}} =
             json_response(conn, 400)

    assert FakeUpstream.requests(upstream) == []
    assert Repo.aggregate(Request, :count) == 0
  end

  test "transcription host catalog labels remain accounting anchors without changing multipart model",
       %{conn: conn} do
    upstream = start_upstream(FakeUpstream.json_response(%{"text" => ""}))
    setup = upstream |> gateway_setup() |> allow_models!([Gateway.backend_transcription_model()])

    hidden_source =
      setup.model.metadata["source_assignment_models"][setup.assignment.id]
      |> Map.merge(%{"slug" => "aaa-hidden-review", "visibility" => "hide", "priority" => -100})

    hidden =
      CodexPooler.PoolerFixtures.model_fixture(setup.pool, %{
        exposed_model_id: "aaa-hidden-review",
        upstream_model_id: "provider-hidden-review",
        metadata: %{
          "source_assignment_ids" => [setup.assignment.id],
          "source_assignment_models" => %{setup.assignment.id => hidden_source}
        }
      })

    refute Repo.exists?(
             from model in CodexPooler.Catalog.Model,
               where: model.pool_id == ^setup.pool.id and model.exposed_model_id == "gpt-4o-transcribe"
           )

    response =
      conn
      |> auth(setup)
      |> post("/v1/audio/transcriptions", %{
        "model" => "gpt-transcribe",
        "file" => upload_fixture("audio.wav", "audio/wav", "synthetic bytes")
      })

    assert %{"text" => ""} = json_response(response, 200)
    assert [captured] = FakeUpstream.requests(upstream)
    assert captured.path == "/backend-api/transcribe"
    assert Map.new(captured.headers)["authorization"] == "Bearer upstream-token"
    assert multipart_parts(captured) == [{:file, "file", "audio.wav"}]
    assert [request] = Repo.all(from request in Request, where: request.pool_id == ^setup.pool.id)
    assert request.model_id == hidden.id
    assert request.requested_model == Gateway.backend_transcription_model()
    assert request.request_metadata["effective_model"] == Gateway.backend_transcription_model()
    assert [attempt] = Repo.all(from attempt in Attempt, where: attempt.request_id == ^request.id)
    assert attempt.pool_upstream_assignment_id == setup.assignment.id
    assert attempt.model_id == hidden.id
  end

  test "POST /v1/audio/transcriptions rejects malformed decoded lists without effects", %{
    conn: conn
  } do
    malformed_lists = [
      {"keywords", "not-a-list"},
      {"languages", %{"unexpected" => "shape"}}
    ]

    for {field, malformed_value} <- malformed_lists do
      upstream = start_upstream(FakeUpstream.json_response(%{"text" => "must not dispatch"}))
      setup = upstream |> gateway_setup() |> use_transcription_model!()

      response =
        conn
        |> recycle()
        |> auth(setup)
        |> post("/v1/audio/transcriptions", %{
          "model" => "gpt-transcribe",
          "file" => upload_fixture("malformed-list.wav", "audio/wav", "malformed audio"),
          field => malformed_value
        })

      assert %{
               "error" => %{
                 "code" => "invalid_request",
                 "param" => ^field
               }
             } = json_response(response, 400)

      assert FakeUpstream.count(upstream) == 0
      assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 0
      assert Repo.aggregate(Attempt, :count) == 0
      assert Repo.aggregate(LedgerEntry, :count) == 0
    end
  end

  for {endpoint, response_format} <- [
        {"/v1/audio/transcriptions", "json"},
        {"/v1/audio/transcriptions", "text"},
        {"/backend-api/transcribe", "json"}
      ],
      malformed <- [
        nil,
        [],
        %{},
        %{"text" => 42},
        %{"error" => %{"code" => "fixture"}},
        %{"text" => "", "error" => %{}}
      ] do
    @endpoint_path endpoint
    @response_format response_format
    @malformed malformed
    test "#{endpoint} #{response_format} rejects malformed successful body #{inspect(malformed)} before settlement",
         %{conn: conn} do
      upstream = start_upstream(FakeUpstream.json_response(@malformed))
      setup = upstream |> gateway_setup() |> use_transcription_model!()

      response =
        conn
        |> auth(setup)
        |> post(@endpoint_path, %{
          "model" => "gpt-transcribe",
          "response_format" => @response_format,
          "file" => upload_fixture("audio.wav", "audio/wav", "synthetic bytes")
        })

      assert %{"error" => %{}} = json_response(response, 502)
      assert FakeUpstream.count(upstream) == 1
      assert [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
      assert request.status == "failed"
      assert request.last_error_code == "invalid_transcription_response"
      assert [attempt] = Repo.all(from a in Attempt, where: a.request_id == ^request.id)
      assert attempt.status == "failed"
      assert attempt.upstream_status_code == 200
      assert request.response_status_code == 502
      assert request.retry_count == 0

      assert [settlement] =
               Repo.all(
                 from entry in LedgerEntry,
                   where: entry.request_id == ^request.id and entry.entry_kind == "settlement"
               )

      assert settlement.amount_status == "recorded"
      assert Repo.all(CodexPooler.Gateway.Persistence.BridgeDemotion) == []
      assert Repo.all(CodexPooler.Gateway.Persistence.RoutingCircuitState) == []
    end
  end

  test "transcription permits an empty text result for silence", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.json_response(%{"text" => ""}))
    setup = upstream |> gateway_setup() |> use_transcription_model!()

    response =
      conn
      |> auth(setup)
      |> post("/v1/audio/transcriptions", %{
        "model" => "gpt-transcribe",
        "file" => upload_fixture("audio.wav", "audio/wav", "synthetic bytes")
      })

    assert %{"text" => ""} = json_response(response, 200)
    assert [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
    assert request.status == "succeeded"
  end

  test "malformed transcription releases an existing half-open probe without changing health", %{
    conn: conn
  } do
    upstream = start_upstream(FakeUpstream.json_response(%{}))
    setup = upstream |> gateway_setup() |> use_transcription_model!()

    state =
      half_open_circuit!(setup, setup.assignment)
      |> Ecto.Changeset.change(route_class: "audio_transcription")
      |> Repo.update!()

    for _request <- 1..2 do
      response =
        conn
        |> recycle()
        |> auth(setup)
        |> post("/v1/audio/transcriptions", %{
          "model" => "gpt-transcribe",
          "file" => upload_fixture("audio.wav", "audio/wav", "synthetic bytes")
        })

      assert response.status == 502
      current = Repo.reload!(state)
      assert current.metadata["probe_in_flight_count"] == 0
      assert {current.status, current.failure_count, current.success_count} == {"half_open", 1, 0}
    end

    assert FakeUpstream.count(upstream) == 2
  end

  test "transcription rejects a non-JSON successful body without echoing or persisting it", %{
    conn: conn
  } do
    sentinel = "synthetic unexpected upstream body"

    upstream =
      start_upstream(FakeUpstream.raw_response(sentinel, headers: [{"content-type", "text/plain"}]))

    setup = gateway_setup(upstream)

    response =
      conn
      |> auth(setup)
      |> post("/v1/audio/transcriptions", %{
        "model" => "gpt-transcribe",
        "file" => upload_fixture("audio.wav", "audio/wav", "synthetic bytes")
      })

    assert %{"error" => %{"code" => "invalid_transcription_response"}} =
             json_response(response, 502)

    refute response.resp_body =~ sentinel
    assert [request] = Repo.all(from request in Request, where: request.pool_id == ^setup.pool.id)
    assert request.status == "failed"
    assert [attempt] = Repo.all(from attempt in Attempt, where: attempt.request_id == ^request.id)

    refute inspect({request.request_metadata, attempt.response_metadata, attempt.error_message}) =~
             sentinel
  end

  defp use_transcription_model!(setup) do
    model =
      setup.model
      |> Ecto.Changeset.change(%{
        exposed_model_id: Gateway.backend_transcription_model(),
        upstream_model_id: "provider-gpt-4o-transcribe",
        supports_responses: false,
        supports_streaming: false,
        metadata: %{
          "source_assignment_ids" => [setup.assignment.id],
          "source_assignment_models" => %{
            setup.assignment.id => %{
              "slug" => Gateway.backend_transcription_model(),
              "input_modalities" => ["audio"],
              "modes" => ["transcription"]
            }
          },
          "input_modalities" => ["audio"],
          "modes" => ["transcription"]
        }
      })
      |> Repo.update!()

    %{setup | model: model}
  end

  defp allow_models!(setup, allowed_model_identifiers) do
    api_key =
      setup.api_key
      |> Ecto.Changeset.change(%{allowed_model_identifiers: allowed_model_identifiers})
      |> Repo.update!()

    %{setup | api_key: api_key}
  end

  defp upload_fixture(filename, content_type, contents) do
    path =
      Path.join(System.tmp_dir!(), "codex-pooler-v1-audio-#{System.unique_integer([:positive])}")

    File.write!(path, contents)
    on_exit(fn -> File.rm(path) end)
    %Plug.Upload{path: path, filename: filename, content_type: content_type}
  end

  defp multipart_parts(captured) do
    boundary =
      captured.headers
      |> Enum.find_value(fn
        {"content-type", "multipart/form-data; boundary=" <> boundary} -> boundary
        _header -> nil
      end)

    captured.body
    |> String.split("--#{boundary}")
    |> Enum.flat_map(&multipart_part/1)
  end

  defp multipart_part(part) do
    case String.split(part, "\r\n\r\n", parts: 2) do
      [headers, contents] ->
        name = Regex.run(~r/name="([^"]+)"/, headers, capture: :all_but_first)
        filename = Regex.run(~r/filename="([^"]+)"/, headers, capture: :all_but_first)

        case {name, filename} do
          {[name], [filename]} -> [{:file, name, filename}]
          {[name], nil} -> [{:text, name, String.trim_trailing(contents, "\r\n")}]
          _other -> []
        end

      _other ->
        []
    end
  end
end
