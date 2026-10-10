defmodule CodexPooler.Accounts.UserInspectTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Accounts.{Scope, User}

  for shape <- [:user, :scope, :changeset] do
    test "#{shape} inspection hides credentials while retaining useful state" do
      password = "synthetic-password-" <> Ecto.UUID.generate()
      hash = "synthetic-hash-" <> Ecto.UUID.generate()
      user = %User{id: Ecto.UUID.generate(), status: "active", password: password, password_hash: hash}

      value =
        case unquote(shape) do
          :user -> user
          :scope -> %Scope{user: user, roles: ["instance_owner"]}
          :changeset -> Ecto.Changeset.change(user, password_hash: hash <> "-new")
        end

      rendered = inspect(value, limit: :infinity, printable_limit: :infinity)
      password_visible? = String.contains?(rendered, password)
      hash_visible? = String.contains?(rendered, hash)
      refute password_visible?
      refute hash_visible?
      assert String.contains?(rendered, "CodexPooler.Accounts.User")
      if unquote(shape) != :changeset, do: assert(String.contains?(rendered, ~s(status: "active")))
      retained? = user.password_hash == hash
      assert retained?
    end
  end
end
