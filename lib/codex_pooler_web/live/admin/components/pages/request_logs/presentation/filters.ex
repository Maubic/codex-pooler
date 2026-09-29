defmodule CodexPoolerWeb.Admin.RequestLogsPresentation.Filters do
  @moduledoc false

  use CodexPoolerWeb, :html

  alias CodexPoolerWeb.Admin.RequestLogsDisplay
  alias Phoenix.LiveView.JS

  attr :id, :string, required: true
  attr :label, :string, required: true
  attr :field_name, :string, required: true
  attr :hidden_id, :string, required: true
  attr :role, :string, required: true
  attr :event, :string, required: true
  attr :value_attr, :atom, required: true
  attr :selected_value, :string, required: true
  attr :selected, :map, required: true
  attr :options, :list, required: true

  def request_log_filter_dropdown(assigns) do
    ~H"""
    <div class="grid gap-2">
      <input
        type="hidden"
        id={@hidden_id}
        name={"filters[#{@field_name}]"}
        value={@selected_value}
      />
      <details
        id={@id}
        class="dropdown w-full"
        phx-click-away={JS.remove_attribute("open", to: "##{@id}")}
      >
        <summary
          data-role={"#{@role}-trigger"}
          aria-label={@label}
          title={Map.get(@selected, :title)}
          class="select flex min-h-10 w-full cursor-pointer items-center gap-2 pr-8 text-left text-sm font-normal"
        >
          <.icon name={@selected.icon} class={["size-4 shrink-0", option_icon_class(@selected)]} />
          <span class="truncate">{@selected.label}</span>
          <span :if={Map.get(@selected, :detail)} data-role={"#{@role}-detail"} class="min-w-0 truncate text-xs text-base-content/50">{@selected.detail}</span>
        </summary>
        <ul
          data-role={"#{@role}-menu"}
          class="menu dropdown-content z-[60] mt-1 max-h-80 w-full flex-nowrap overflow-y-auto rounded-box border border-base-300 bg-base-100 p-1 !transition-none ![scale:100%] shadow-xl"
        >
          <li :for={option <- @options}>
            <button
              type="button"
              phx-click={@event}
              phx-value-pool-id={filter_option_value(@value_attr, :pool_id, option)}
              phx-value-status={filter_option_value(@value_attr, :status, option)}
              phx-value-upstream-id={filter_option_value(@value_attr, :upstream_id, option)}
              phx-value-model={filter_option_value(@value_attr, :model, option)}
              data-role={"#{@role}-option"}
              data-pool-id={filter_option_value(@value_attr, :pool_id, option)}
              data-status={filter_option_value(@value_attr, :status, option)}
              data-upstream-id={filter_option_value(@value_attr, :upstream_id, option)}
              data-model={filter_option_value(@value_attr, :model, option)}
              class={[
                "flex items-center gap-2 text-sm",
                filter_option_active?(option, @selected_value) && "active"
              ]}
              aria-current={filter_option_active?(option, @selected_value) && "true"}
              title={Map.get(option, :title)}
            >
              <span data-role={"#{@role}-icon"} class="shrink-0">
                <.icon name={option.icon} class={["size-4", option_icon_class(option)]} />
              </span>
              <span class="truncate">{option.label}</span>
              <span
                :if={Map.get(option, :strategy_label)}
                class="ml-auto shrink-0 text-[0.68rem] text-base-content/50"
              >
                {option.strategy_label}
              </span>
              <span
                :if={Map.get(option, :detail)}
                data-role={"#{@role}-option-detail"}
                class="ml-auto shrink-0 text-[0.68rem] text-base-content/50"
              >
                {option.detail}
              </span>
            </button>
          </li>
        </ul>
      </details>
    </div>
    """
  end

  defp filter_option_value(current_attr, target_attr, option),
    do: RequestLogsDisplay.filter_option_value(current_attr, target_attr, option)

  defp filter_option_active?(option, selected_value),
    do: RequestLogsDisplay.filter_option_active?(option, selected_value)

  defp option_icon_class(option), do: RequestLogsDisplay.option_icon_class(option)
end
