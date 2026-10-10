defmodule CodexPooler.Accounting.RequestOutcomeTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting.{Attempt, Request, RequestOutcome}
  alias CodexPooler.Repo

  # Failed-request codes that stay failures. The first six name a cut on the
  # Pooler's side, and several of them share the 499 a client disconnect
  # carries; the replay codes close a replay no resend claimed, armed for a
  # loss the row cannot attribute; the last two are provider-side stream ends.
  @kept_out_codes ~w(
    owner_drained owner_unavailable owner_crashed dead_execution_recovered absent_instance_recovered stale_reservation_recovered
    websocket_replay_expired websocket_replay_abandoned websocket_replay_owner_unavailable websocket_replay_revoked websocket_replay_superseded
    upstream_stream_error stream_incomplete
  )

  describe "client_cancelled?/2 and display_status/2" do
    test "a failed request whose last error is client_disconnected is a client cancellation" do
      assert RequestOutcome.client_cancellation_error_codes() == ["client_disconnected"]
      assert RequestOutcome.client_cancelled?("failed", "client_disconnected")
      assert RequestOutcome.client_cancelled?(%{status: "failed", last_error_code: "client_disconnected"})
      assert RequestOutcome.client_cancelled?(%Request{status: "failed", last_error_code: "client_disconnected"})
      assert RequestOutcome.display_status("failed", "client_disconnected") == RequestOutcome.client_cancelled()
      assert RequestOutcome.client_cancelled() == "client_cancelled"
    end

    test "every Pooler-side cut, the 499 ones included, and every replay closure stays a failure" do
      for code <- @kept_out_codes do
        refute RequestOutcome.client_cancelled?("failed", code), "#{code} must stay a failure"
        assert RequestOutcome.display_status("failed", code) == "failed"
      end

      refute RequestOutcome.client_cancelled?("failed", nil)
      refute RequestOutcome.client_cancelled?(%{status: "failed"})
    end

    test "only a failed row qualifies: a late answer that corrected the interrupt is a success" do
      for status <- ~w(succeeded rejected cancelled in_progress accepted) do
        refute RequestOutcome.client_cancelled?(status, "client_disconnected"), status
        assert RequestOutcome.display_status(status, "client_disconnected") == status
      end
    end
  end

  test "server idle cutoff is a failure without changing the stored disconnect code" do
    row = %Request{status: "failed", last_error_code: "client_disconnected", request_metadata: %{"downstream_interruption" => %{"origin" => "server", "cause" => "idle_timeout", "client_activity" => "pong_observed"}}}
    refute RequestOutcome.client_cancelled?(row)
    assert RequestOutcome.display_status(row) == "failed"
    assert RequestOutcome.display_status(%{row | status: "succeeded"}) == "succeeded"
    assert RequestOutcome.client_cancelled?(%{row | request_metadata: %{}})
    refute RequestOutcome.client_cancelled?(%{status: "succeeded", last_error_code: "client_disconnected", client_cancelled?: true})
    refute RequestOutcome.client_cancelled?(%{status: "failed", last_error_code: "client_disconnected", client_cancelled?: false})
  end

  describe "query conditions" do
    setup do
      pool = pool_fixture()
      %{api_key: api_key} = active_api_key_fixture(pool)
      context = %{pool: pool, api_key: api_key}

      # A websocket cut answers 499; an HTTP stream the client left keeps the 200
      # it had already sent, so the class never reads the status code.
      cancelled = [
        request_fixture(context, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 499, transport: "websocket"}),
        request_fixture(context, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 200, transport: "http_sse"})
      ]

      others =
        Enum.map(@kept_out_codes, &request_fixture(context, %{status: "failed", last_error_code: &1, response_status_code: 499})) ++
          [
            request_fixture(context, %{status: "failed", last_error_code: nil, response_status_code: 502}),
            request_fixture(context, %{status: "failed", last_error_code: "client_disconnected", request_metadata: %{"downstream_interruption" => %{"origin" => "server", "cause" => "idle_timeout", "client_activity" => "unknown"}}}),
            request_fixture(context, %{status: "succeeded", last_error_code: "client_disconnected"}),
            request_fixture(context, %{status: "rejected", last_error_code: "client_disconnected", response_status_code: 499}),
            request_fixture(context)
          ]

      %{pool: pool, cancelled: cancelled, others: others}
    end

    test "the condition selects exactly the class and its negation keeps every other row, a failed row without a code included",
         %{pool: pool, cancelled: cancelled, others: others} do
      scoped = from(request in Request, as: :request, where: request.pool_id == ^pool.id, select: request.id)

      assert ids(where(scoped, ^RequestOutcome.client_cancelled_condition(:request))) == ids(cancelled)
      assert ids(where(scoped, ^RequestOutcome.not_client_cancelled_condition(:request))) == ids(others)
    end

    test "the query condition and client_cancelled?/1 classify every row alike", %{pool: pool} do
      rows = Repo.all(from(request in Request, as: :request, where: request.pool_id == ^pool.id))

      selected =
        Request
        |> from(as: :request)
        |> where([request], request.pool_id == ^pool.id)
        |> where(^RequestOutcome.client_cancelled_condition(:request))
        |> select([request], request.id)
        |> Repo.all()
        |> MapSet.new()

      for row <- rows do
        assert RequestOutcome.client_cancelled?(row) == MapSet.member?(selected, row.id), "#{row.status} #{inspect(row.last_error_code)}"
      end
    end

    test "the condition applies to the request under the binding it is given", %{pool: pool, cancelled: [cancelled | _rest]} do
      %{assignment: assignment} = upstream_assignment_fixture(pool)
      attempt_fixture(cancelled, assignment, %{status: "failed", network_error_code: "client_disconnected"})

      attempt_request_ids =
        from(attempt in Attempt, join: request in Request, as: :cut, on: request.id == attempt.request_id, select: request.id)
        |> where(^RequestOutcome.client_cancelled_condition(:cut))
        |> Repo.all()

      assert attempt_request_ids == [cancelled.id]
    end
  end

  defp ids(%Ecto.Query{} = query), do: query |> Repo.all() |> MapSet.new()
  defp ids(requests) when is_list(requests), do: requests |> Enum.map(& &1.id) |> MapSet.new()
end
