defmodule CodexPoolerWeb.Admin.LensFilterFormTest do
  use ExUnit.Case, async: true

  alias CodexPoolerWeb.Admin.LensFilterForm

  test "selectors fall back to their first available option and retain known choices" do
    [first | _] = options = LensFilterForm.window_options()
    assert LensFilterForm.selected(options, "unknown") == first
    assert LensFilterForm.selected(options, "7d").value == "7d"
  end
end
