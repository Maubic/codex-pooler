defmodule CodexPoolerWeb.Admin.RequestLogsPresentation.Issues do
  @moduledoc false

  use CodexPoolerWeb, :html

  import CodexPoolerWeb.Admin.RequestLogsDisplay, only: [format_errors: 2, format_served_model_detail: 1]

  attr :request_log, :map, required: true
  attr :datetime_preferences, :map, required: true
  attr :prefix, :string, required: true

  def request_log_issues_cell(assigns) do
    errors = assigns.request_log |> format_errors(assigns.datetime_preferences) |> Enum.reject(&(&1 == "—"))
    model_mismatch? = not is_nil(format_served_model_detail(assigns.request_log))
    conflict_attempts = Map.get(assigns.request_log, :model_conflict_attempts, [])

    assigns =
      assigns
      |> assign(:errors, errors)
      |> assign(:model_mismatch?, model_mismatch?)
      |> assign(:conflict_attempts, conflict_attempts)
      |> assign(:has_issues?, errors != [] or model_mismatch? or conflict_attempts != [])

    ~H"""
    <td data-role="request-issues-cell" class={["min-w-0 align-middle max-lg:col-span-2 max-lg:col-start-1 max-lg:row-start-5 max-lg:sm:col-span-3 max-lg:sm:row-start-3", !@has_issues? && "max-lg:hidden"]}>
      <div :if={@has_issues?} id={"#{@prefix}-#{@request_log.id}-issues"} data-role="request-issues" class="grid min-w-0 gap-1 text-[11px] leading-4">
        <span class="text-[10px] uppercase tracking-wide text-base-content/50 lg:hidden">Errors · Warnings</span>
        <ul :if={@errors != []} id={"#{@prefix}-#{@request_log.id}-errors"} data-role="errors" class={["grid min-w-0 gap-1", error_text_class(@request_log.status)]}>
          <li :for={error <- @errors} data-role="error-line" class="flex min-w-0 items-start gap-1">
            <span aria-hidden="true" class="inline-flex h-4 shrink-0 items-center"><.icon name="hero-exclamation-triangle" class="size-3.5 text-error" /></span>
            <span class="min-w-0 whitespace-normal break-words">{error}</span>
          </li>
        </ul>
        <span
          :if={@model_mismatch?}
          id={"#{@prefix}-#{@request_log.id}-served-model"}
          data-role="served-model"
          class="flex min-w-0 items-start gap-1 text-warning"
          title={"Upstream declared model: #{@request_log.served_model}; differs from the model sent upstream"}
          aria-label={"Upstream declared model: #{@request_log.served_model}; differs from the model sent upstream"}
        >
          <span aria-hidden="true" class="inline-flex h-4 shrink-0 items-center"><.icon name="hero-exclamation-triangle" class="size-3.5" /></span>
          <span class="min-w-0 whitespace-normal break-words">Model mismatch: {@request_log.served_model}</span>
        </span>
        <span :if={@conflict_attempts != []} data-role="model-declaration-conflict" class="flex min-w-0 items-start gap-1 text-warning" title={"The provider reported different model names during the same response on attempts #{Enum.join(@conflict_attempts, ", ")}"}>
          <span aria-hidden="true" class="inline-flex h-4 shrink-0 items-center"><.icon name="hero-exclamation-triangle" class="size-3.5" /></span>
          <span class="min-w-0 whitespace-normal break-words">model name changed · attempts {Enum.join(@conflict_attempts, ", ")}</span>
        </span>
      </div>
      <span :if={!@has_issues?} data-role="no-request-issues" class="text-base-content/45">—</span>
    </td>
    """
  end

  defp error_text_class(status) when status in ["failed", "rejected"], do: "text-error"
  defp error_text_class("cancelled"), do: "text-warning"
  defp error_text_class(_status), do: "text-base-content/65"
end
