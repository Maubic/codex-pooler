defmodule CodexPoolerWeb.Admin.FlashGroupPlacementTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias CodexPoolerWeb.CoreComponents

  # The connection toast used to sit top-right at every width. At 375 px it covered the header controls and the top of the
  # page connection notices (https://github.com/icoretech/codex-pooler-findings/issues/326, row 326-2). The real
  # placement is measured in a browser; these pin the responsive contract on the rendered markup.
  describe "on an admin page" do
    setup :register_and_log_in_user

    test "the flash stack sits at the bottom below sm, returns to the top while a dialog is open, and sits at the top from sm", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/admin/upstreams")

      assert has_element?(view, "#flash-group.toast.toast-end[class~='z-50'][class~='sm:toast-top']")
      # `toast` alone is bottom-end; no unconditional top placement remains.
      refute has_element?(view, "#flash-group[class~='toast-top']")
      # A bottom-sheet dialog (z 999) would hide a bottom toast, so below sm it goes back to the top while one is open.
      assert has_element?(view, ~s|#flash-group[class~="max-sm:[body:has(dialog[open])_&]:top-4"][class~="max-sm:[body:has(dialog[open])_&]:bottom-auto"]|)
    end
  end

  test "a flash is capped to the content column below sm and keeps its width from sm up" do
    alert = render_component(&CoreComponents.flash/1, kind: :error, flash: %{"error" => "Sample"}) |> LazyHTML.from_fragment() |> LazyHTML.query("#flash-error .alert")

    classes = alert |> LazyHTML.attribute("class") |> List.first() |> String.split()

    # 20rem, or less where the viewport minus the admin rail and gutters is narrower.
    assert "max-w-[min(20rem,calc(100vw_-_6rem))]" in classes
    assert "w-80" in classes
    assert "sm:w-96" in classes
    assert "sm:max-w-96" in classes
    refute "max-w-80" in classes
  end
end
