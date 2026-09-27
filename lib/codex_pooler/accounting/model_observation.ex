defmodule CodexPooler.Accounting.ModelObservation do
  @moduledoc "Bounded, versioned facts about provider model declarations on one attempt."

  alias CodexPooler.Accounting.{Attempt, Metadata}

  import Ecto.Query

  @doc "Projects known public error placeholders as unavailable without rewriting stored attempts."
  @spec provider_models(Ecto.Queryable.t()) :: Ecto.Query.t()
  def provider_models(query) do
    fields = Attempt.__schema__(:fields) -- [:served_model]

    from(a in query,
      select: struct(a, ^fields)
    )
    |> select_merge([a], %{
      served_model:
        fragment(
          """
          CASE WHEN ? = 'unknown'
            AND ? IN ('failed', 'retryable_failed') AND ? = 'websocket'
            AND ? @> '{"upstream_websocket_bridge":true,"public_openai_responses_stream":{"mode":"normalized","created_seen":false,"visible_seen":false,"delta_count":0,"terminal_seen":true,"terminal_kind":"failed"}}'::jsonb
            AND (? IS NULL OR (?->>'version' = '1' AND ?->>'coverage' IN ('full', 'partial') AND ?->>'conflict' IS NULL AND ?->>'terminal_model' IS NULL AND ?->>'first_conflicting_model' IS NULL))
          THEN NULL ELSE ? END
          """,
          a.served_model,
          a.status,
          a.transport,
          a.response_metadata,
          a.model_observation,
          a.model_observation,
          a.model_observation,
          a.model_observation,
          a.model_observation,
          a.model_observation,
          a.served_model
        )
    })
  end

  @spec normalize(term(), String.t() | nil) :: map() | nil
  def normalize(%{"version" => 1, "coverage" => coverage} = observation, served_model)
      when coverage in ["full", "partial"] do
    conflict = if is_binary(served_model), do: observation["conflict"]
    first_conflicting = Metadata.bounded_model_identifier(observation["first_conflicting_model"])

    %{
      "version" => 1,
      "coverage" => coverage,
      "terminal_status" => terminal_status(observation["terminal_status"]),
      "terminal_model" => Metadata.bounded_model_identifier(observation["terminal_model"]),
      "first_conflicting_model" => if(conflict == true, do: first_conflicting),
      "conflict" => if(is_boolean(conflict), do: conflict)
    }
  end

  def normalize(_observation, _served_model), do: nil

  @spec conflict?(term()) :: boolean()
  def conflict?(%{"version" => 1, "conflict" => true}), do: true
  def conflict?(_observation), do: false

  defp terminal_status(status) when status in ~w(completed failed incomplete cancelled json), do: status
  defp terminal_status(_status), do: nil
end
