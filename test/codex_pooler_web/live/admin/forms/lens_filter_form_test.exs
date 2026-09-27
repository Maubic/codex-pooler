defmodule CodexPoolerWeb.Admin.LensFilterFormTest do
  use ExUnit.Case, async: true

  alias CodexPoolerWeb.Admin.LensFilterForm

  test "a selector without available options preserves its value with a renderable fallback" do
    assert %{value: "unavailable-model", label: label, icon: icon, icon_class: icon_class} = LensFilterForm.selected([], "unavailable-model")
    assert is_binary(label) and byte_size(label) > 0
    assert is_binary(icon) and byte_size(icon) > 0
    assert is_binary(icon_class)
    [first | _] = options = LensFilterForm.window_options()
    assert LensFilterForm.selected(options, "unknown") == first
    assert LensFilterForm.selected(options, "7d").value == "7d"
  end
end
