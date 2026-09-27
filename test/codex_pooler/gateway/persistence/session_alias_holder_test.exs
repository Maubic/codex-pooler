defmodule CodexPooler.Gateway.Persistence.SessionAliasHolderTest do
  use CodexPooler.DataCase, async: false
  import CodexPooler.PoolerFixtures
  import Ecto.Query
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeSessionAlias, CodexTurn, SessionContinuity}
  alias CodexPooler.Gateway.Persistence.SessionContinuity.Aliases
  alias CodexPooler.Gateway.Websocket, as: Gateway
  alias CodexPooler.Repo

  @window_source "x-codex-window-id"

  test "previous response attachment preserves another session's in-progress window holder" do
    key = active_api_key_fixture()
    auth = %{pool: key.pool, api_key: key.api_key}
    window = "probe-window-#{System.unique_integer([:positive])}"
    hash = :crypto.hash(:sha256, window)
    opts = RequestOptions.for_websocket(%{session_header: window, session_header_source: @window_source})

    assert {:ok, holder} = Gateway.start_codex_session(auth, opts)
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    assert window_session_id(auth, hash) == holder.id

    # Control: the ordinary start path with the same window header resolves the
    # holder and reuses it, so it cannot move the alias.
    assert {:ok, reused} = Gateway.start_codex_session(auth, opts)
    assert reused.id == holder.id
    assert window_session_id(auth, hash) == holder.id

    turn = in_progress_turn!(key, holder, now)
    assert turn.status == "in_progress"

    assert {:ok, other} =
             Gateway.start_codex_session(auth, RequestOptions.for_websocket(%{accepted_turn_state: "probe-other-session"}))

    refute other.id == holder.id

    # The guarded writer refuses to move the window while the holder's turn runs.
    assert :kept = Aliases.point_frame_window_hash(other, auth, hash, now)
    assert window_session_id(auth, hash) == holder.id

    response_id = "resp_sample_alias_anchor"
    assert :ok = Aliases.register!(other, auth, RequestOptions.put_continuity(opts, session_header: nil, response_id: response_id), now)
    assert {:ok, resumed} = SessionContinuity.start_codex_session_from_previous_response_id(auth, RequestOptions.put_continuity(opts, previous_response_id: response_id))
    assert resumed.id == other.id
    assert window_session_id(auth, hash) == holder.id

    # Same-holder refresh is permitted while its turn remains in progress.
    assert :ok = Aliases.register!(holder, auth, opts, now)
    assert window_session_id(auth, hash) == holder.id

    # The holder's turn is untouched and still in progress.
    assert Repo.get!(CodexTurn, turn.id).status == "in_progress"
  end

  defp window_session_id(auth, hash) do
    Repo.one!(
      from(row in BridgeSessionAlias,
        where:
          row.pool_id == ^auth.pool.id and row.api_key_id == ^auth.api_key.id and
            row.alias_kind == "session_header" and row.alias_hash == ^hash and row.status == "active",
        select: row.codex_session_id
      )
    )
  end

  defp in_progress_turn!(key, session, now) do
    request = request_fixture(key, %{status: "in_progress", transport: "websocket", completed_at: nil})

    Repo.insert!(%CodexTurn{
      codex_session_id: session.id,
      request_id: request.id,
      turn_sequence: 1,
      transport_kind: "websocket",
      semantic_turn_digest: :crypto.strong_rand_bytes(32),
      status: "in_progress",
      started_at: now,
      created_at: now,
      updated_at: now
    })
  end
end
