defmodule CodexPooler.WebhookDestinationFixture do
  @moduledoc false
  import ExUnit.Assertions
  import ExUnit.Callbacks

  @spec setup!() :: map()
  def setup! do
    ip = private_ip!()
    host = ip |> :inet.ntoa() |> List.to_string()
    names = [{:iPAddress, :erlang.list_to_binary(Tuple.to_list(ip))}, {:dNSName, ~c"webhook.example.test"}, {:dNSName, ~c"alias.example.test"}]
    cert = :public_key.pkix_test_data(%{root: [digest: :sha256, key: {:rsa, 2048, 65_537}], peer: [digest: :sha256, key: {:rsa, 2048, 65_537}, extensions: [{:Extension, {2, 5, 29, 17}, false, names}]]})
    trust!(cert[:cacerts])
    %{ip: ip, host: host, cert: cert}
  end

  @spec private_ip!() :: :inet.ip4_address()
  def private_ip! do
    {:ok, interfaces} = :inet.getifaddrs()
    addresses = for {_name, opts} <- interfaces, {:addr, address} <- opts, private_v4?(address), do: address

    case addresses do
      [address | _] -> address
      [] -> flunk("webhook wire tests require an owned RFC1918 interface; loopback is not an allowed substitute")
    end
  end

  defp private_v4?({10, _, _, _}), do: true
  defp private_v4?({172, second, _, _}) when second in 16..31, do: true
  defp private_v4?({192, 168, _, _}), do: true
  defp private_v4?(_), do: false

  @spec trust!([binary()]) :: :ok
  def trust!(certificates) do
    previous = :persistent_term.get(:pubkey_os_cacerts, :absent)
    directory = Path.join(System.tmp_dir!(), "webhook-private-ca-#{Base.encode16(:crypto.strong_rand_bytes(8))}")

    on_exit(fn ->
      if previous == :absent, do: :public_key.cacerts_clear(), else: :persistent_term.put(:pubkey_os_cacerts, previous)
      assert :persistent_term.get(:pubkey_os_cacerts, :absent) == previous
      File.rm_rf!(directory)
      refute File.exists?(directory)
    end)

    File.mkdir!(directory)
    File.chmod!(directory, 0o700)
    File.write!(Path.join(directory, "ca.pem"), :public_key.pem_encode(Enum.map(certificates, &{:Certificate, &1, :not_encrypted})))
    :ok = :public_key.cacerts_load(String.to_charlist(Path.join(directory, "ca.pem")))
  end

  @spec start_fake!(term(), map()) :: CodexPooler.FakeUpstream.t()
  def start_fake!(mode, fixture) do
    {:ok, fake} = CodexPooler.FakeUpstream.start_link(mode, ip: fixture.ip, scheme: :https, thousand_island_options: [transport_options: [cert: fixture.cert[:cert], key: fixture.cert[:key]]])
    on_exit(fn -> CodexPooler.FakeUpstream.stop(fake) end)
    %{fake | url: "https://#{fixture.host}:#{URI.parse(fake.url).port}"}
  end
end
