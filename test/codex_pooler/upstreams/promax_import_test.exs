defmodule CodexPooler.Upstreams.PromaxImportTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Auth.{CodexAuth, CodexAuthJson}
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity

  test "promax auth claims survive parsing and encrypted identity import" do
    %{user: user} = bootstrap_owner_fixture()
    scope = Scope.for_user(user, ["instance_owner"])
    pool = pool_fixture()
    account_id = "acct_promax_#{System.unique_integer([:positive])}"
    id_token = jwt(%{"email" => "sample@example.com", "https://api.openai.com/auth" => %{"chatgpt_account_id" => account_id, "chatgpt_user_id" => "sample-user", "chatgpt_plan_type" => "promax"}})
    access_token = jwt(%{"exp" => DateTime.to_unix(DateTime.add(DateTime.utc_now(), 3600, :second))})
    auth_json = CodexPooler.JSON.encode!(%{"auth_mode" => "chatgpt", "tokens" => %{"account_id" => account_id, "id_token" => id_token, "access_token" => access_token, "refresh_token" => "synthetic-refresh"}})

    assert {:ok, %{plan_family: "promax", plan_label: "promax"}} = CodexAuth.token_info(id_token)
    assert {:ok, %{plan_label: "promax"}} = CodexAuthJson.parse(auth_json)
    assert {:ok, %{identity: identity}} = Upstreams.import_codex_auth_json(scope, pool, auth_json)
    persisted = Repo.get!(UpstreamIdentity, identity.id)
    assert persisted.plan_label == "promax"
    assert persisted.plan_family == "promax"
    refute inspect(persisted) =~ access_token
    refute inspect(persisted) =~ "synthetic-refresh"
  end

  defp jwt(payload) do
    encode = &Base.url_encode64(CodexPooler.JSON.encode!(&1), padding: false)
    Enum.join([encode.(%{"alg" => "none", "typ" => "JWT"}), encode.(payload), "synthetic"], ".")
  end
end
