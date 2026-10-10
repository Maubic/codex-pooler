defmodule CodexPooler.Alerts.Delivery.WebhookRedirectDeliveryTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures
  import ExUnit.CaptureLog

  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Alerts
  alias CodexPooler.Alerts.Delivery.{WebhookDelivery, WebhookSigning}
  alias CodexPooler.Alerts.Schemas.{AlertChannel, AlertDeliveryAttempt, AlertRuleChannel}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Platform.OutboundHTTP
  alias CodexPooler.Repo
  alias CodexPooler.TestAppEnv

  setup do
    TestAppEnv.restore_on_exit(OutboundHTTP)
    Application.put_env(:codex_pooler, OutboundHTTP, proxy_config: %{http: [], https: [], no_proxy: []})
    %{user: user} = bootstrap_owner_fixture()
    ip = CodexPooler.WebhookDestinationFixture.private_ip!()
    certificate = certificate([{:dNSName, ~c"localhost"}, {:iPAddress, :erlang.list_to_binary(Tuple.to_list(ip))}])
    trust_test_ca!(certificate[:cacerts])
    %{scope: Scope.for_user(user, ["instance_owner"]), certificate: certificate}
  end

  for status <- [200, 204] do
    @status status
    test "canonical private HTTPS #{@status} sends one valid signed notification", context do
      receiver = receiver!(FakeUpstream.raw_response("", status: @status), context.certificate)
      {incident, channel, secret} = delivery_fixture!(context.scope, https_url(receiver) <> "/hooks")

      assert {:ok, attempt} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1)
      assert attempt.status == "sent"
      assert attempt.response_status_code == @status
      assert FakeUpstream.count(receiver) == 1
      request = hd(FakeUpstream.requests(receiver))
      assert request.method == "POST"
      assert request.path == "/hooks"
      assert signature_valid?(request, secret)
      assert Repo.get!(AlertDeliveryAttempt, attempt.id).status == "sent"
    end
  end

  for {status, destination} <- [{307, :other_port}, {308, :other_port}, {301, :other_host}, {302, :other_host}, {303, :other_host}, {307, :same_origin}, {308, :same_origin}, {307, :http_downgrade}] do
    @status status
    @destination destination
    test "HTTPS #{@status} to #{@destination} never replays a signed notification", context do
      sink = receiver!(FakeUpstream.raw_response("", status: 200), if(@destination == :http_downgrade, do: nil, else: context.certificate))
      sink_url = if @destination == :http_downgrade, do: FakeUpstream.url(sink), else: https_url(sink, if(@destination == :other_host, do: "localhost", else: nil))
      # This real unsigned preflight proves the sink is reachable. The completed
      # delivery orders Req's redirect loop before we read the receiver ledger.
      assert {:ok, %{status: 200}} = OutboundHTTP.get(if(@destination == :http_downgrade, do: FakeUpstream.url(sink), else: https_url(sink)) <> "/health", retry: false)
      assert FakeUpstream.count(sink) == 1
      location = if @destination == :same_origin, do: "/hop", else: sink_url <> "/hop"

      origin = receiver!(FakeUpstream.repeat_last([FakeUpstream.raw_response("", status: @status, headers: [{"location", location}]), FakeUpstream.raw_response("", status: 200)]), context.certificate)
      {incident, channel, secret} = delivery_fixture!(context.scope, https_url(origin) <> "/hooks")
      assert {:ok, attempt} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1)
      origin_requests = FakeUpstream.requests(origin)
      first = hd(origin_requests)
      hops = if @destination == :same_origin, do: tl(origin_requests), else: Enum.reject(FakeUpstream.requests(sink), &(&1.path == "/health"))
      observation = replay_observation(first, hops, secret, attempt)

      assert observation.original_signature_valid
      assert observation.hop_count == 0, inspect(observation)
      assert length(origin_requests) == 1
      assert FakeUpstream.count(sink) == 1
      assert_redirect_attempt!(attempt, @status)
    end
  end

  for status <- [301, 307, 308] do
    @status status
    test "HTTPS #{@status} without Location records a nonretryable redirect message", context do
      origin = receiver!(FakeUpstream.raw_response("", status: @status), context.certificate)
      {incident, channel, secret} = delivery_fixture!(context.scope, https_url(origin) <> "/hooks")
      assert {:ok, attempt} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1)
      assert FakeUpstream.count(origin) == 1
      assert signature_valid?(hd(FakeUpstream.requests(origin)), secret)
      assert_redirect_attempt!(attempt, @status)
    end
  end

  test "real TLS hostname rejection cannot deliver an application request", context do
    wrong_certificate = certificate([{:dNSName, ~c"wrong.example"}])
    trust_test_ca!(wrong_certificate[:cacerts])
    receiver = receiver!(FakeUpstream.raw_response("", status: 200), wrong_certificate)
    {incident, channel, _secret} = delivery_fixture!(context.scope, https_url(receiver) <> "/hooks")

    log =
      capture_log(fn ->
        assert {:error, %{retryable: true}} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1)
      end)

    assert FakeUpstream.count(receiver) == 0
    [attempt] = Repo.all(from attempt in AlertDeliveryAttempt, where: attempt.incident_id == ^incident.id)
    assert attempt.status == "retryable"
    assert String.starts_with?(attempt.failure_code, "alert_webhook_transport_")
    assert attempt.completed_at
    refute log =~ "sha256="
  end

  defp replay_observation(first, hops, secret, attempt) do
    hop = List.first(hops)

    %{
      original_signature_valid: signature_valid?(first, secret),
      hop_count: length(hops),
      hop_method: if(hop, do: hop.method),
      hop_bytes: if(hop, do: byte_size(hop.body)),
      identical_body: hop != nil and hop.body == first.body,
      signature_replayed: hop != nil and header(hop, "x-codex-pooler-signature") == header(first, "x-codex-pooler-signature"),
      event_header_present: hop != nil and is_binary(header(hop, "x-codex-pooler-event-id")),
      attempt_header_present: hop != nil and is_binary(header(hop, "x-codex-pooler-attempt-id")),
      attempt_status: attempt.status,
      response_status: attempt.response_status_code
    }
  end

  defp assert_redirect_attempt!(attempt, status) do
    persisted = Repo.get!(AlertDeliveryAttempt, attempt.id)
    assert persisted.status == "failed"
    assert persisted.response_status_code == status
    assert persisted.failure_code == "alert_webhook_http_#{status}"
    assert persisted.failure_message == "webhook endpoint returned a redirect"
    refute persisted.retryable
    assert persisted.next_retry_at == nil
    assert persisted.completed_at
    refute Map.has_key?(persisted.response_metadata, "location")
  end

  defp signature_valid?(request, secret) do
    WebhookSigning.verify?(header(request, "x-codex-pooler-event-id"), header(request, "x-codex-pooler-attempt-id"), request.body, secret, header(request, "x-codex-pooler-signature"))
  end

  defp header(request, key), do: request.headers |> Enum.find_value(fn {name, value} -> if name == key, do: value end)

  defp delivery_fixture!(scope, endpoint) do
    suffix = System.unique_integer([:positive])
    secret = "synthetic-signing-value-#{suffix}"
    {:ok, projection} = Alerts.create_channel(scope, %{channel_type: "webhook", display_name: "redirect contract #{suffix}", endpoint_url: endpoint, webhook_signing_secret: secret, webhook_signing_secret_action: "preserve"})
    channel = Repo.get!(AlertChannel, projection.id)
    pool = pool_fixture(%{slug: "redirect-contract-#{suffix}"})
    rule = alert_rule_fixture(pool, cooldown_minutes: 30)
    %AlertRuleChannel{} |> AlertRuleChannel.changeset(%{alert_rule_id: rule.id, alert_channel_id: channel.id, created_at: DateTime.utc_now()}) |> Repo.insert!()
    incident = alert_incident_fixture(pool: pool, dedupe_key: "alert:redirect:#{suffix}", safe_evidence_snapshot: %{"reason_code" => "no_usable_assignments", "assignment_count" => 0})
    alert_incident_target_fixture(incident, rule, pool)
    {incident, channel, secret}
  end

  defp receiver!(mode, certificate) do
    supervisor_name = Module.concat(__MODULE__, "Receiver#{System.unique_integer([:positive])}")

    on_exit(fn ->
      if pid = Process.whereis(supervisor_name) do
        monitor = Process.monitor(pid)
        FakeUpstream.stop(%FakeUpstream{supervisor: pid})
        assert_receive {:DOWN, ^monitor, :process, ^pid, _reason}, 15_000
      end

      assert Process.whereis(supervisor_name) == nil
    end)

    opts = if certificate, do: [scheme: :https, thousand_island_options: [transport_options: [cert: certificate[:cert], key: certificate[:key]]]], else: []
    {:ok, receiver} = FakeUpstream.start_link(mode, opts |> Keyword.put(:supervisor_name, supervisor_name) |> Keyword.put(:ip, CodexPooler.WebhookDestinationFixture.private_ip!()))
    %{receiver | url: "http://#{CodexPooler.WebhookDestinationFixture.private_ip!() |> :inet.ntoa() |> List.to_string()}:#{URI.parse(receiver.url).port}"}
  end

  defp https_url(receiver, host \\ nil), do: "https://#{host || CodexPooler.WebhookDestinationFixture.private_ip!() |> :inet.ntoa() |> List.to_string()}:#{URI.parse(FakeUpstream.url(receiver)).port}"

  defp certificate(names) do
    :public_key.pkix_test_data(%{root: [digest: :sha256, key: {:rsa, 2048, 65_537}], peer: [digest: :sha256, key: {:rsa, 2048, 65_537}, extensions: [{:Extension, {2, 5, 29, 17}, false, names}]]})
  end

  defp trust_test_ca!(certificates) do
    previous = :persistent_term.get(:pubkey_os_cacerts, :not_loaded)
    directory = Path.join(System.tmp_dir!(), "webhook-redirect-ca-#{Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)}")

    on_exit(fn ->
      if previous == :not_loaded, do: :public_key.cacerts_clear(), else: :persistent_term.put(:pubkey_os_cacerts, previous)
      File.rm_rf!(directory)
      refute File.exists?(directory)
      restored? = :persistent_term.get(:pubkey_os_cacerts, :not_loaded) == previous
      assert restored?
    end)

    File.mkdir!(directory)
    File.chmod!(directory, 0o700)
    File.write!(Path.join(directory, "ca.pem"), :public_key.pem_encode(Enum.map(certificates, &{:Certificate, &1, :not_encrypted})))
    :ok = :public_key.cacerts_load(String.to_charlist(Path.join(directory, "ca.pem")))
  end
end
