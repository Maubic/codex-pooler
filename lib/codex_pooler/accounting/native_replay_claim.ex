defmodule CodexPooler.Accounting.NativeReplayClaim do
  @moduledoc """
  The replay claim an anchored native websocket request runs under, recorded
  on its request row (findings#323).

  A session's owner matches the reattach of a turn whose socket was lost on the
  exact replay claim of the request it runs, and an anchored request's claim
  binds its `previous_response_id`. After any reconnect the released client
  resends that request as full history without the anchor, so the resend
  cannot derive the claim, and the request's stored client-retry witness is
  the anchor-free digest of its items, not its claim. Recording the claim lets
  the socket node's replay preflight name it once the resend's alternates
  matched that witness. An unanchored request's witness is its claim, so
  nothing is recorded for it.

  The value is an opaque HMAC digest, recorded as a versioned base64url map
  like the request's other digests, never read from a client.
  """

  alias CodexPooler.Accounting.Request

  @key "native_replay_claim"
  @encoded_bytes 43

  @doc "The metadata key the claim is recorded under."
  @spec key() :: String.t()
  def key, do: @key

  @doc "The recorded form of `claim`."
  @spec metadata(<<_::256>>) :: map()
  def metadata(<<_::256>> = claim), do: %{"version" => 1, "digest" => Base.url_encode64(claim, padding: false)}

  @doc "Keeps exactly the recorded shape of `metadata/1`; anything else is dropped whole."
  @spec sanitize(term()) :: map()
  def sanitize(%{"version" => 1, "digest" => digest} = value) when map_size(value) == 2 and is_binary(digest) and byte_size(digest) == @encoded_bytes do
    case Base.url_decode64(digest, padding: false) do
      {:ok, <<_::256>>} -> value
      _invalid -> %{}
    end
  end

  def sanitize(_value), do: %{}

  @doc "The claim recorded on a websocket request row, or nil."
  @spec recorded(Request.t()) :: <<_::256>> | nil
  def recorded(%Request{transport: "websocket", request_metadata: %{@key => %{"version" => 1, "digest" => encoded} = value}}) when map_size(value) == 2 and is_binary(encoded) and byte_size(encoded) == @encoded_bytes do
    case Base.url_decode64(encoded, padding: false) do
      {:ok, <<_::256>> = claim} -> claim
      _invalid -> nil
    end
  end

  def recorded(_request), do: nil
end
