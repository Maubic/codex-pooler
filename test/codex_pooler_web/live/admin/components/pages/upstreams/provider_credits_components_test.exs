defmodule CodexPoolerWeb.Admin.ProviderCreditsComponentsTest do
  use ExUnit.Case, async: true

  import Phoenix.Component
  import Phoenix.LiveViewTest

  alias CodexPoolerWeb.Admin.ProviderCreditsPolicyForm
  alias CodexPoolerWeb.Admin.UpstreamPageComponents.AccountCard.QuotaLimitRow
  alias CodexPoolerWeb.Admin.UpstreamPageComponents.ProviderCreditsComponents
  alias Phoenix.LiveView.JS

  test "finite observations keep included exhaustion separate from the precise credit baseline" do
    document =
      render_component(&summary_with_included_window/1, %{summary: finite_summary()})
      |> LazyHTML.from_fragment()

    assert text(document, "#credits-balance") == "12,497"
    assert text(document, "#credits-baseline") == "12,500"
    assert text(document, "#credits-percent") == "99.976%"
    assert has?(document, "#credits-title + #credits-percent.text-success")
    assert has?(document, "#credits-progress[value='99.976'][max='100'][aria-valuetext='99.976% of observed baseline'].progress-success.progress-striped")
    assert has?(document, "#credits-policy + #credits-balance")
    assert has?(document, "#credits-progress[aria-label*='Observed'][aria-describedby='credits-baseline-description']")
    assert has?(document, "#credits-included-windows #included-weekly-progress[value='0']")
    assert has?(document, "#credits-included-windows[aria-labelledby='credits-included-title']")
    assert has?(document, "#included-weekly-progress[aria-label*='Weekly remaining 0%']")
    refute has?(document, "#credits-included-windows [data-role='provider-credits-observed-progress']")
  end

  @tag credits_negative: true
  test "disabling credit admission leaves finite provider observations and included quota visible" do
    summary = %{
      finite_summary()
      | allow_provider_credits: false,
        availability: :disabled,
        availability_label: "Provider credits disabled",
        availability_detail: "Independently valid included capacity and permitted banked-reset recovery remain available."
    }

    document =
      render_component(&summary_with_included_window/1, %{summary: summary})
      |> LazyHTML.from_fragment()

    assert has?(document, "#credits[data-policy-enabled='false'][data-availability='disabled']")
    assert text(document, "#credits-policy") == "Disabled"
    assert text(document, "#credits-balance") == "12,497"
    assert has?(document, "#credits-progress[value='99.976']")
    assert has?(document, "#included-weekly-progress[value='0']")
  end

  @tag credits_negative: true
  test "an observed positive balance does not create an available routing decision" do
    document =
      finite_summary()
      |> render_summary()

    assert has?(document, "#credits[data-policy-enabled='true'][data-availability='unknown']")
    assert text(document, "#credits-policy") == "Enabled"
    assert text(document, "#credits-balance") == "12,497"
    assert has?(document, "#credits-progress[value='99.976']")
    assert has?(document, "#credits[title^='Provider credit capacity unverified;']")
    refute has?(document, "#credits-availability")
    refute has?(document, "#credits[data-availability='available'], #credits[data-availability='conditional']")
  end

  test "the compact row exposes its existing policy dialog only when an authorized action is supplied" do
    command = JS.push("open_provider_credits_policy", value: %{id: "sample-upstream"})

    document =
      render_component(&ProviderCreditsComponents.provider_credits_summary/1, %{id: "credits", summary: finite_summary(), open_policy: command})
      |> LazyHTML.from_fragment()

    assert has?(document, "#credits-policy-open[type='button'][aria-haspopup='dialog'][aria-controls='provider-credits-policy-dialog'][phx-click]")
    assert text(document, "#credits-balance") == "12,497"
    refute has?(render_summary(finite_summary()), "#credits-policy-open")
  end

  @tag credits_negative: true
  test "standalone finite balances need no invented included window or denominator" do
    summary = %{finite_summary() | observed_baseline_label: nil}
    document = render_summary(summary)

    assert text(document, "#credits-balance") == "12,497"
    assert has?(document, "#credits[data-balance-state='finite']")
    refute has?(document, "#credits-baseline, #credits-progress, #credits-percent")
    refute has?(document, "#credits-included-windows, [data-role='upstream-limit-chart']")
  end

  @tag credits_negative: true
  test "unlimited credit observations have no percentage even if a retained finite baseline is present" do
    summary = %{finite_summary() | balance_state: :unlimited, balance_label: nil}
    document = render_summary(summary)

    assert text(document, "#credits-balance") == "Unlimited"
    assert has?(document, "#credits[data-balance-state='unlimited'][data-availability='unknown']")
    refute has?(document, "#credits-baseline, #credits-progress, #credits-percent")
    refute has?(document, "#credits-included-windows")
  end

  @tag credits_negative: true
  test "unknown and zero balances hide the row without fabricating a quota meter" do
    for {state, label} <- [{:unknown, nil}, {:finite, "0"}] do
      summary = %{finite_summary() | balance_state: state, balance_label: label, display_row?: false}
      document = render_summary(summary)
      refute has?(document, "#credits, #credits-progress, [data-role='upstream-limit-chart']")
    end
  end

  test "fractional balances are displayed without rounding them into absent credits" do
    summary = %{
      finite_summary()
      | balance_label: "<1",
        observed_baseline_label: "12",
        observed_percent: Decimal.new("0.024")
    }

    document = render_summary(summary)

    assert text(document, "#credits-balance") == "<1"
    assert text(document, "#credits-percent") == "0.024%"
    assert has?(document, "#credits-progress[value='0.024'][aria-valuetext='0.024% of observed baseline'].progress-error")
  end

  test "credit reference percentages truncate and use the existing quota threshold colors" do
    for {percent, displayed, tone} <- [
          {"70.99999", "70.999", "success"},
          {"30.99999", "30.999", "warning"},
          {"29.99999", "29.999", "error"}
        ] do
      document = render_summary(%{finite_summary() | observed_percent: Decimal.new(percent)})

      assert text(document, "#credits-percent") == "#{displayed}%"
      assert has?(document, "#credits-percent.text-#{tone}")
      assert has?(document, "#credits-progress[value='#{displayed}'].progress-#{tone}.progress-striped")
      assert has?(document, "#credits[title*='of observed baseline']")
    end
  end

  test "finite baselines retain three decimal places for whole percentages" do
    document = render_summary(%{finite_summary() | observed_percent: Decimal.new(100)})

    assert text(document, "#credits-percent") == "100.000%"
    assert has?(document, "#credits-progress[value='100.000'][aria-valuetext='100.000% of observed baseline']")
  end

  test "detail values remain integer numbers while exact decimals are optional hover metadata" do
    summary = finite_summary() |> Map.put(:balance_label, "62,485") |> Map.put(:balance_exact_label, "62,485.24098765")
    document = render_component(&ProviderCreditsComponents.provider_credits_details/1, %{summary: summary}) |> LazyHTML.from_fragment()

    assert text(document, "#provider-credits-details > div:first-child dd") == "62,485"
    assert has?(document, "#provider-credits-details > div:first-child dd[title='62,485.24098765']")
    assert text(document, "#provider-credits-details > div:nth-child(2) dd") == "12,500"
  end

  test "the accessible checked form serializes one canonical typed policy field" do
    document = render_policy_form(true)
    name = "provider_credits_policy[allow_provider_credits]"

    assert has?(document, "#provider-credits-policy-form[phx-change='validate_provider_credits_policy'][phx-submit='save_provider_credits_policy']")
    assert has?(document, "label[for='provider-credits-enabled']")
    assert has?(document, "#provider-credits-enabled[type='checkbox'][checked][aria-describedby='provider-credits-policy-help']")
    refute has?(document, "#provider-credits-save")
    assert attribute(document, "#provider-credits-policy-form input[name='#{name}']", "value") == ["false", "true"]
    assert serialize_policy(document) == %{"allow_provider_credits" => "true"}
    assert text(document, "#provider-credits-policy-help") =~ "before blocked-request banked-reset recovery"
    assert text(document, "#provider-credits-policy-help") =~ "Proactive threshold and expiry reset policies remain separate"
    assert ProviderCreditsPolicyForm.parse(serialize_policy(document)) == {:ok, %{allow_provider_credits: true}}
    refute has?(document, "#provider-credits-policy-form input[name*='identity'], #provider-credits-policy-form input[name*='pool']")
  end

  @tag credits_negative: true
  test "the unchecked checkbox submits hidden false instead of reverting to default on" do
    document = render_policy_form(false)

    assert has?(document, "#provider-credits-enabled[type='checkbox']:not([checked])")
    assert has?(document, "#provider-credits-policy-form input[type='hidden'][name='provider_credits_policy[allow_provider_credits]'][value='false']")
    assert serialize_policy(document) == %{"allow_provider_credits" => "false"}
    assert ProviderCreditsPolicyForm.parse(serialize_policy(document)) == {:ok, %{allow_provider_credits: false}}
  end

  @tag credits_negative: true
  test "disabled controls omit both checkbox and hidden fallback from submitted data" do
    document = render_policy_form(true, disabled: true)

    assert has?(document, "#provider-credits-enabled[disabled]")
    assert has?(document, "#provider-credits-policy-form input[type='hidden'][disabled]")
    refute has?(document, "#provider-credits-save")
    assert serialize_policy(document) == %{}
    assert {:error, %Ecto.Changeset{valid?: false}} = ProviderCreditsPolicyForm.parse(serialize_policy(document))
  end

  test "LiveView unused-field metadata never reaches the shaped domain attributes" do
    assert ProviderCreditsPolicyForm.parse(%{
             "allow_provider_credits" => "false",
             "_unused_allow_provider_credits" => ""
           }) == {:ok, %{allow_provider_credits: false}}
  end

  @tag credits_negative: true
  test "missing policy submission is invalid and its error is linked to the checkbox" do
    assert {:error, changeset} = ProviderCreditsPolicyForm.parse(%{})
    document = render_changeset(changeset)

    refute changeset.valid?
    assert has?(document, "#provider-credits-enabled[aria-invalid='true'][aria-describedby='provider-credits-policy-help provider-credits-policy-errors']")
    assert has?(document, "#provider-credits-policy-errors[role='alert']")
    refute has?(document, "#provider-credits-enabled[checked]")
  end

  @tag credits_negative: true
  test "malformed checkbox values cannot become enabled policy" do
    for value <- [nil, "", "on", "off", "junk", 1, 0, ["false", "true"], %{"enabled" => "true"}] do
      assert {:error, %Ecto.Changeset{valid?: false} = changeset} =
               ProviderCreditsPolicyForm.parse(%{"allow_provider_credits" => value})

      assert Keyword.has_key?(changeset.errors, :allow_provider_credits)
    end

    assert {:error, changeset} = ProviderCreditsPolicyForm.parse(%{"allow_provider_credits" => "on"})
    document = render_changeset(changeset)

    assert has?(document, "#provider-credits-enabled[aria-invalid='true']:not([checked])")
    assert has?(document, "#provider-credits-policy-errors[role='alert']")
  end

  @tag credits_negative: true
  test "forged hidden identity or provider-fact fields cannot pass form normalization" do
    for key <- ["upstream_identity_id", "pool_id", "credit_permission", "balance", "auto_redeem_enabled"] do
      assert {:error, %Ecto.Changeset{valid?: false}} =
               ProviderCreditsPolicyForm.parse(%{"allow_provider_credits" => "true", key => "synthetic"})
    end
  end

  @tag credits_negative: true
  test "mixed atom and string controls are rejected rather than selecting either value" do
    assert {:error, %Ecto.Changeset{valid?: false}} =
             ProviderCreditsPolicyForm.parse(%{"allow_provider_credits" => "true", :allow_provider_credits => false})
  end

  @tag credits_negative: true
  test "malformed form envelopes produce changeset errors rather than raising" do
    for params <- [nil, "true", [allow_provider_credits: true], %URI{}] do
      assert {:error, %Ecto.Changeset{valid?: false} = changeset} = ProviderCreditsPolicyForm.parse(params)
      document = render_changeset(changeset)

      assert has?(document, "#provider-credits-enabled[aria-invalid='true']")
      assert has?(document, "#provider-credits-policy-errors[role='alert']")
    end
  end

  defp summary_with_included_window(assigns) do
    assigns =
      assign(assigns, :limit, %{
        label: "Weekly",
        percent: Decimal.new(0),
        percent_value: 0,
        percent_label: "0%",
        count_label: nil,
        count_title: nil,
        reset_label: nil,
        reset_title: nil,
        reset_semantics: :unknown,
        reset_at: nil
      })

    ~H"""
    <ProviderCreditsComponents.provider_credits_summary id="credits" summary={@summary}>
      <:included_windows>
        <QuotaLimitRow.quota_limit_row id="included-weekly" limit={@limit} />
      </:included_windows>
    </ProviderCreditsComponents.provider_credits_summary>
    """
  end

  defp finite_summary do
    %{
      balance_state: :finite,
      display_row?: true,
      balance_label: "12,497",
      observed_baseline_label: "12,500",
      observed_percent: Decimal.new("99.976"),
      allow_provider_credits: true,
      availability: :unknown,
      availability_label: "Provider credit capacity unverified",
      availability_detail: "Observed balance does not establish permission for this request."
    }
  end

  defp render_summary(summary) do
    render_component(&ProviderCreditsComponents.provider_credits_summary/1, %{id: "credits", summary: summary})
    |> LazyHTML.from_fragment()
  end

  defp render_policy_form(enabled, options \\ []) do
    assigns =
      options
      |> Map.new()
      |> Map.put(:form, ProviderCreditsPolicyForm.form(%{allow_provider_credits: enabled}))

    render_component(&ProviderCreditsComponents.provider_credits_policy_form/1, assigns)
    |> LazyHTML.from_fragment()
  end

  defp render_changeset(changeset) do
    render_component(&ProviderCreditsComponents.provider_credits_policy_form/1, %{form: ProviderCreditsPolicyForm.form_for_changeset(changeset)})
    |> LazyHTML.from_fragment()
  end

  defp serialize_policy(document) do
    name = "provider_credits_policy[allow_provider_credits]"

    document
    |> LazyHTML.query("#provider-credits-policy-form input[name='#{name}']:not([disabled])")
    |> Enum.reduce(%{}, fn input, params ->
      [type] = LazyHTML.attribute(input, "type")
      [value] = LazyHTML.attribute(input, "value")

      if type == "hidden" or LazyHTML.attribute(input, "checked") != [] do
        Map.put(params, "allow_provider_credits", value)
      else
        params
      end
    end)
  end

  defp has?(document, selector), do: not Enum.empty?(LazyHTML.query(document, selector))
  defp text(document, selector), do: document |> LazyHTML.query(selector) |> LazyHTML.text() |> String.trim()
  defp attribute(document, selector, name), do: document |> LazyHTML.query(selector) |> LazyHTML.attribute(name)
end
