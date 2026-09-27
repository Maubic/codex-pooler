defmodule CodexPoolerWeb.Runtime.BackendCodexMailboxMetadataTest do
  # Reachability probe: a native POST /backend-api/codex/responses whose body
  # carries a non-map `client_metadata` and whose canonical turn document
  # arrives in the `x-codex-turn-metadata` header, with a post-compaction
  # resume input ending in mailbox messages, should answer a 4xx/2xx, not blow
  # up inside NativeMailboxContinuation.prefix_witnesses/2.
  use CodexPoolerWeb.ConnCase, async: false

  @moduletag capture_log: true

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, gateway_setup: 1, start_upstream: 1]

  alias CodexPooler.FakeUpstream

  test "native HTTP post-compaction resume with a hostile client_metadata", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.json_response(%{"id" => "resp_synthetic"}))
    setup = gateway_setup(upstream)

    body = %{
      "model" => setup.model.exposed_model_id,
      "input" => [
        %{"type" => "message", "role" => "user", "content" => "synthetic"},
        %{"type" => "compaction", "encrypted_content" => "synthetic-pivot"},
        %{"type" => "reasoning", "id" => "rs_synthetic", "encrypted_content" => "synthetic-reasoning"},
        %{
          "type" => "agent_message",
          "author" => "/root/worker",
          "recipient" => "/root",
          "content" => [%{"type" => "input_text", "text" => "synthetic update"}]
        }
      ],
      "client_metadata" => "not-a-map"
    }

    metadata =
      CodexPooler.JSON.encode!(%{
        "turn_id" => "turn_synthetic_mailbox",
        "request_kind" => "turn",
        "agent_name" => "/root"
      })

    response =
      conn
      |> auth(setup)
      |> put_req_header("session-id", "codex-session-synthetic-mailbox")
      |> put_req_header("x-codex-turn-metadata", metadata)
      |> post("/backend-api/codex/responses", body)

    assert response.status == 200
  end

  test "control: the same request with a map client_metadata" do
    upstream = start_upstream(FakeUpstream.json_response(%{"id" => "resp_synthetic"}))
    setup = gateway_setup(upstream)

    body = %{
      "model" => setup.model.exposed_model_id,
      "input" => [
        %{"type" => "message", "role" => "user", "content" => "synthetic"},
        %{"type" => "compaction", "encrypted_content" => "synthetic-pivot"},
        %{"type" => "reasoning", "id" => "rs_synthetic", "encrypted_content" => "synthetic-reasoning"},
        %{
          "type" => "agent_message",
          "author" => "/root/worker",
          "recipient" => "/root",
          "content" => [%{"type" => "input_text", "text" => "synthetic update"}]
        }
      ],
      "client_metadata" => %{
        "x-codex-turn-metadata" => %{
          "turn_id" => "turn_synthetic_mailbox_map",
          "request_kind" => "turn",
          "agent_name" => "/root"
        }
      }
    }

    response =
      build_conn()
      |> auth(setup)
      |> put_req_header("session-id", "codex-session-synthetic-mailbox-map")
      |> post("/backend-api/codex/responses", body)

    assert response.status == 200
  end
end
