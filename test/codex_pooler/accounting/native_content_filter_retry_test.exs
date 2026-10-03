defmodule CodexPooler.Accounting.NativeContentFilterRetryTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport
  import CodexPooler.PoolerFixtures, only: [request_fixture: 2, attempt_fixture: 3]

  alias CodexPooler.Accounting.{ClientRetry, NativeContentFilterRetry, Request}
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Repo

  test "strict metadata rejects old, partial, unknown and extra-field authority" do
    terminal = %{"version" => 1, "event_type" => "response.incomplete", "reason" => "content_filter"}
    assert CodexPooler.Accounting.sanitize_metadata(%{"native_content_filter_terminal" => terminal}) == %{"native_content_filter_terminal" => terminal}

    for invalid <- [nil, "synthetic", [], Map.put(terminal, "version", 2), Map.put(terminal, "extra", "synthetic"), Map.delete(terminal, "reason")] do
      assert CodexPooler.Accounting.sanitize_metadata(%{"native_content_filter_terminal" => invalid}) == %{"native_content_filter_terminal" => %{}}
    end

    for key <- ["native_content_filter_source", "native_content_filter_binding"], invalid <- [nil, "synthetic", [], %{"version" => 1}, %{"version" => 2}] do
      assert CodexPooler.Accounting.sanitize_metadata(%{key => invalid}) == %{key => %{}}
    end

    original = %{"version" => 1, "digest" => Base.url_encode64(:crypto.hash(:sha256, "synthetic original"), padding: false)}
    assert CodexPooler.Accounting.sanitize_metadata(%{"native_content_filter_original" => original}) == %{"native_content_filter_original" => original}

    for invalid <- [nil, "synthetic", [], Map.put(original, "version", 2), Map.put(original, "extra", true), Map.put(original, "digest", "invalid")] do
      assert CodexPooler.Accounting.sanitize_metadata(%{"native_content_filter_original" => invalid}) == %{"native_content_filter_original" => %{}}
    end
  end

  test "PostgreSQL binding is mandatory after reservation and checked again at attempt insertion" do
    setup = accounting_setup()
    now = db_now()
    predecessor = request_fixture(setup, %{model_id: setup.model.id, requested_model: setup.model.exposed_model_id, transport: "websocket"})
    attempt = attempt_fixture(predecessor, setup.assignment, %{upstream_model_id: setup.model.upstream_model_id})
    source = %{"version" => 1, "attempt_id" => attempt.id, "assignment_id" => setup.assignment.id, "identity_id" => setup.identity.id, "credential_epoch" => 1, "serving_mode" => "full", "requested_model" => setup.model.exposed_model_id, "effective_model" => setup.model.exposed_model_id, "upstream_model" => setup.model.upstream_model_id}
    terminal = %{"version" => 1, "event_type" => "response.incomplete", "reason" => "content_filter"}
    attempt |> Ecto.Changeset.change(response_metadata: %{"native_content_filter_source" => source, "native_content_filter_terminal" => terminal}) |> Repo.update!()
    session = Repo.insert!(%CodexSession{pool_id: setup.pool.id, api_key_id: setup.api_key.id, session_key: Ecto.UUID.generate(), status: "active", created_at: now, updated_at: now})
    turn = ClientRetry.insert_successor_turn!(session, predecessor, :crypto.hash(:sha256, "synthetic"), now)
    turn |> Ecto.Changeset.change(status: "succeeded", completed_at: now, final_attempt_id: attempt.id) |> Repo.update!()
    successor = request_fixture(setup, %{model_id: setup.model.id, requested_model: setup.model.exposed_model_id, transport: "websocket", status: "in_progress", completed_at: nil, request_metadata: %{"effective_model" => setup.model.exposed_model_id, "client_resend" => %{"predecessor_shape" => "content_filter_retry"}, "native_content_filter_binding" => source}})
    ClientRetry.insert_link!(predecessor, successor, now)
    scope = %{assignment_id: setup.assignment.id, identity_id: setup.identity.id, credential_epoch: 1, serving_mode: "full", effective_model: setup.model.exposed_model_id, upstream_model: setup.model.upstream_model_id}
    assert NativeContentFilterRetry.dispatch_allowed?(successor, scope)

    for metadata <- [Map.delete(successor.request_metadata, "native_content_filter_binding"), put_in(successor.request_metadata, ["native_content_filter_binding", "version"], 2), Map.drop(successor.request_metadata, ["native_content_filter_binding", "client_resend"])] do
      updated = Repo.get!(Request, successor.id) |> Ecto.Changeset.change(request_metadata: metadata) |> Repo.update!()
      refute NativeContentFilterRetry.dispatch_allowed?(updated, scope)
      assert {:error, %{code: :invalid_content_filter_retry_binding}} = CodexPooler.Accounting.create_attempt(updated, setup.assignment, %{model: setup.model, response_metadata: %{"routing" => %{"model_serving_mode" => "full"}}})
      refute Repo.get_by(CodexPooler.Accounting.Attempt, request_id: successor.id)
    end

    Repo.get!(Request, successor.id) |> Ecto.Changeset.change(request_metadata: successor.request_metadata) |> Repo.update!()
    changed_model = setup.model |> Ecto.Changeset.change(upstream_model_id: "synthetic-changed-mapping") |> Repo.update!()
    assert {:error, %{code: :invalid_content_filter_retry_binding}} = CodexPooler.Accounting.create_attempt(successor, setup.assignment, %{model: changed_model, response_metadata: %{"routing" => %{"model_serving_mode" => "full"}}})
    refute Repo.get_by(CodexPooler.Accounting.Attempt, request_id: successor.id)
    Repo.get!(CodexPooler.Catalog.Model, setup.model.id) |> Ecto.Changeset.change(upstream_model_id: setup.model.upstream_model_id) |> Repo.update!()
    assert {:ok, accepted} = CodexPooler.Accounting.create_attempt(successor, setup.assignment, %{model: setup.model, response_metadata: %{"routing" => %{"model_serving_mode" => "full"}}})
    assert accepted.pool_upstream_assignment_id == setup.assignment.id
    assert accepted.upstream_model_id == setup.model.upstream_model_id
  end

  defp db_now do
    %{rows: [[timestamp]]} = Repo.query!("SELECT clock_timestamp()")
    DateTime.from_naive!(timestamp, "Etc/UTC")
  end
end
