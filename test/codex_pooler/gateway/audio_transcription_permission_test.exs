defmodule CodexPooler.Gateway.AudioTranscriptionPermissionTest do
  use CodexPooler.DataCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 1, start_upstream: 1]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Pools
  alias CodexPooler.Repo

  for executor <- [:execute, :execute_multipart] do
    @executor executor
    test "#{executor} reads the current Pool permission before validation and effects" do
      upstream = start_upstream(FakeUpstream.json_response(%{"text" => "synthetic transcript"}))
      setup = gateway_setup(upstream)
      {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
      endpoint = "/backend-api/transcribe"
      opts = RequestOptions.build(%{}, endpoint, %{})

      assert {:error, %{status: 400}} = apply(Gateway, @executor, [auth, endpoint, %{}, opts])

      settings =
        setup.pool
        |> Pools.ensure_routing_settings()
        |> Ecto.Changeset.change(allow_audio_transcription: false)
        |> Repo.update!()

      assert {:error, %{status: 403, code: "audio_transcription_disabled", pooler_policy: true}} =
               apply(Gateway, @executor, [auth, endpoint, %{}, opts])

      assert FakeUpstream.requests(upstream) == []
      assert Repo.aggregate(Request, :count) == 0
      assert Repo.aggregate(Attempt, :count) == 0
      assert Repo.aggregate(LedgerEntry, :count) == 0

      settings |> Ecto.Changeset.change(allow_audio_transcription: true) |> Repo.update!()
      assert {:error, %{status: 400}} = apply(Gateway, @executor, [auth, endpoint, %{}, opts])
    end
  end
end
