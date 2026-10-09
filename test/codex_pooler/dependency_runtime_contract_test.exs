defmodule CodexPooler.DependencyRuntimeContractTest do
  use CodexPooler.DataCase, async: false

  import ExUnit.CaptureLog

  alias Phoenix.HTML.FormData
  alias TelemetryMetricsPrometheus.Core
  alias TelemetryMetricsPrometheus.Core.Registry

  @timeout 15_000
  @smtp_callback :codex_pooler_dependency_smtp_server

  setup_all do
    fixture = Path.expand("../fixtures/dependency_contract/smtp_server.erl", __DIR__)

    on_exit(fn ->
      :code.purge(@smtp_callback)
      :code.delete(@smtp_callback)
    end)

    assert {:ok, @smtp_callback, bytecode, []} = :compile.file(String.to_charlist(fixture), [:binary, :return_errors, :return_warnings, :warnings_as_errors])
    assert {:module, @smtp_callback} = :code.load_binary(@smtp_callback, String.to_charlist(fixture), bytecode)
    :ok
  end

  test "Gettext parallel locale/domain compilation agrees with serial and unified generation" do
    directory = Path.join(System.tmp_dir!(), "dependency-gettext-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(directory) end)
    File.mkdir_p!(directory)
    priv = Path.join(directory, "gettext")

    for locale <- ["en", "fr"], domain <- ["default", "other"] do
      target = Path.join([priv, locale, "LC_MESSAGES"])
      File.mkdir_p!(target)
      File.write!(Path.join(target, domain <> ".po"), po(locale, domain))
    end

    modules =
      for {suffix, options} <- [parallel: [split_module_by: [:locale, :domain]], serial: [split_module_by: [:locale, :domain], split_module_compilation: :serial], unified: [split_module_by: []]] do
        module = Module.concat(__MODULE__, "Backend#{suffix}#{System.unique_integer([:positive])}")
        source = Path.join(directory, "#{suffix}.ex")
        File.write!(source, "defmodule #{inspect(module)} do\nuse Gettext.Backend, otp_app: :codex_pooler, priv: #{inspect(priv)}, #{Enum.map_join(options, ", ", fn {key, value} -> "#{key}: #{inspect(value)}" end)}\nend\n")
        # pmap needs a real compiler coordinator, not Code.compile_string/1.
        assert {:ok, compiled, %{compile_warnings: [], runtime_warnings: []}} = Kernel.ParallelCompiler.compile_to_path([source], directory, return_diagnostics: true)

        on_exit(fn ->
          for compiled_module <- compiled do
            :code.purge(compiled_module)
            :code.delete(compiled_module)
          end
        end)

        module
      end

    for module <- modules do
      assert Enum.sort(Gettext.known_locales(module)) == ["en", "fr"]
      assert Gettext.put_locale!(module, "fr") == nil
      assert Gettext.get_locale(module) == "fr"
      assert Gettext.put_locale!(module, "en") == "fr"
      assert_raise ArgumentError, fn -> Gettext.put_locale!(module, "unknown") end
      assert_raise FunctionClauseError, fn -> Gettext.put_locale!(module, :erlang.binary_to_term(:erlang.term_to_binary(:invalid))) end

      for locale <- ["en", "fr"], domain <- ["default", "other"] do
        Gettext.with_locale(module, locale, fn ->
          assert Gettext.dgettext(module, domain, "hello") == "#{locale} #{domain} hello"
          assert Gettext.dngettext(module, domain, "one item", "%{count} items", 1) == "#{locale} #{domain} one"
          assert Gettext.dngettext(module, domain, "one item", "%{count} items", 3) == "#{locale} #{domain} 3"
        end)
      end
    end
  end

  test "Gettext template merge accepts blank translations and rejects translated templates" do
    for source <- [~s(msgid "hello"\nmsgstr ""\n), ~s(msgid "one"\nmsgid_plural "many"\nmsgstr[0] ""\nmsgstr[1] ""\n)] do
      template = Expo.PO.parse_string!(source)
      assert %Expo.Messages{messages: [_message]} = Gettext.Extractor.merge_template(template, template, [])
      translated = Expo.PO.parse_string!(String.replace(source, ~s(msgstr ""), ~s(msgstr "translated")) |> String.replace(~s(msgstr[0] ""), ~s(msgstr[0] "translated")))
      assert_raise Gettext.Error, ~r/non-empty msgstr/, fn -> Gettext.Extractor.merge_template(translated, template, []) end
      assert_raise Gettext.Error, ~r/non-empty msgstr/, fn -> Gettext.Extractor.merge_template(template, translated, []) end
    end
  end

  test "range validator and separately the Core list histogram retain their contracts" do
    assert Registry.validate_distribution_buckets!({1..5, 2}) == [1, 3, 5]
    assert Registry.validate_distribution_buckets!({1..5//2, 2}) == [1, 3, 5]
    assert Registry.validate_distribution_buckets!([1, 3, 5]) == [1, 3, 5]
    assert_raise ArgumentError, fn -> Registry.validate_distribution_buckets!({5..1//-1, 2}) end
    assert_raise ArgumentError, fn -> Registry.validate_distribution_buckets!({1..4, 2}) end

    name = unique_name("metrics")
    event = [:dependency_contract, name]
    metric = Telemetry.Metrics.distribution("dependency_contract.value", event_name: event, measurement: :value, reporter_options: [buckets: [1, 3, 5]])
    start_supervised!({Core, name: name, metrics: [metric], start_async: false})
    for value <- [1, 2, 6], do: :telemetry.execute(event, %{value: value}, %{})
    scrape = Core.scrape(name)
    assert scrape =~ ~s(dependency_contract_value_bucket{le="1"} 1)
    assert scrape =~ ~s(dependency_contract_value_bucket{le="3"} 2)
    assert scrape =~ ~s(dependency_contract_value_bucket{le="+Inf"} 3)
    assert scrape =~ "dependency_contract_value_count 3"
    assert scrape =~ "dependency_contract_value_sum 9"
  end

  test "PostgreSQL savepoint rollback and Phoenix Ecto forms retain their contracts" do
    Repo.query!("CREATE TEMP TABLE dependency_contract_values (value jsonb) ON COMMIT DROP")

    assert {:error, :synthetic_rollback} =
             Repo.transaction(
               fn ->
                 Repo.query!("INSERT INTO dependency_contract_values VALUES ($1)", [%{"nested" => [true, nil, "value"]}])
                 Repo.rollback(:synthetic_rollback)
               end,
               mode: :savepoint
             )

    assert %{rows: [[0]]} = Repo.query!("SELECT count(*) FROM dependency_contract_values")
    assert is_list(Ecto.Migrator.migrations(Repo))

    changeset =
      {%{label: "before"}, %{label: :string}}
      |> Ecto.Changeset.cast(%{"label" => "after"}, [:label])
      |> Ecto.Changeset.add_error(:label, "synthetic error")
      |> Map.put(:action, :validate)

    form = FormData.to_form(changeset, as: :sample)
    assert form.errors == [label: {"synthetic error", []}]
    assert FormData.input_value(changeset, form, :label) == "after"
  end

  test "SMTP delivers locally with plaintext and real STARTTLS" do
    port = smtp_server!()

    for {tls, encrypted?} <- [never: false, always: true] do
      assert :gen_smtp_client.send_blocking(email(), smtp_options(port, tls: tls)) == "accepted\r\n"
      assert_receive {:smtp_session, session}, @timeout
      if encrypted?, do: assert_receive({:smtp_tls, ^session}, @timeout)
      assert_receive {:smtp_delivered, ^encrypted?, 1, bytes}, @timeout
      assert bytes > 0
    end
  end

  test "SMTP STARTTLS upgrade exceptions retain temporary-failure classification" do
    # A real ssl option conversion raises before the handshake; gen_smtp must
    # convert that exception to the same temporary failure as legacy catch.
    port = smtp_server!()

    log =
      capture_log(fn ->
        assert {:error, :retries_exceeded, {:temporary_failure, _host, :tls_failed}} =
                 :gen_smtp_client.send_blocking(email(), smtp_options(port, tls: :always, tls_options: :invalid))
      end)

    assert log =~ "Error in ssl upgrade"
    assert log =~ "function_clause"
    assert log =~ ~r/smtp_socket.*proplist_merge/
    refute_received {:smtp_delivered, _, _, _}
  end

  test "SMTP live session code changes preserve return, throw, exit and error handling" do
    port = smtp_server!()
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :line], @timeout)
    on_exit(fn -> :gen_tcp.close(socket) end)
    assert {:ok, <<"220", _::binary>>} = :gen_tcp.recv(socket, 0, @timeout)
    assert_receive {:smtp_session, session}, @timeout
    original = :sys.get_state(session)
    assert :ok = :sys.suspend(session)

    try do
      for action <- [:same, :throw_same, :throw, :exit, :error, :other] do
        assert :ok = :sys.change_code(session, :gen_smtp_server_session, :old, action)
        assert :sys.get_state(session) == original
      end

      # The released server rematches CallbackState; retain its existing
      # rejection of a different callback state instead of fixing it here.
      assert {:error, {:EXIT, {{:badmatch, _}, stack}}} = :sys.change_code(session, :gen_smtp_server_session, :old, :changed)
      assert [{:gen_smtp_server_session, :code_change, 3, _} | _] = stack
      assert :sys.get_state(session) == original
    after
      :sys.resume(session)
    end
  end

  defp smtp_server! do
    name = unique_name("smtp")
    on_exit(fn -> :gen_smtp_server.stop(name) end)
    certificate = :public_key.pkix_test_data(%{root: [digest: :sha256, key: {:rsa, 2048, 65_537}], peer: [digest: :sha256, key: {:rsa, 2048, 65_537}]})
    assert {:ok, _pid} = :gen_smtp_server.start(name, @smtp_callback, port: 0, address: {127, 0, 0, 1}, domain: ~c"example.com", sessionoptions: [callbackoptions: [owner: self()], tls_options: certificate])
    :ranch.get_port(name)
  end

  defp smtp_options(port, options) do
    Keyword.merge([relay: ~c"127.0.0.1", hostname: ~c"example.com", port: port, no_mx_lookups: true, auth: :never, retries: 0, tls_options: [verify: :verify_none]], options)
  end

  defp email, do: {"sender@example.com", ["recipient@example.com"], "Subject: synthetic\r\n\r\nsynthetic\r\n"}
  defp unique_name(prefix), do: String.to_atom("dependency_#{prefix}_#{System.unique_integer([:positive])}")

  defp po(locale, domain) do
    """
    msgid ""
    msgstr ""
    "Language: #{locale}\\n"
    "Plural-Forms: nplurals=2; plural=(n != 1);\\n"

    msgid "hello"
    msgstr "#{locale} #{domain} hello"

    msgid "one item"
    msgid_plural "%{count} items"
    msgstr[0] "#{locale} #{domain} one"
    msgstr[1] "#{locale} #{domain} %{count}"
    """
  end
end
