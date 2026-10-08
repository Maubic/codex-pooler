defmodule CodexPooler.Gateway.OpenAICompatibility.Validation do
  @moduledoc false

  alias CodexPooler.Gateway.OpenAICompatibility.{
    Audio,
    Chat,
    Error,
    Files,
    Images,
    Matrix,
    Responses
  }

  @type validation_result :: :ok | {:error, Error.reason()}

  @spec validate_indexed([term()], String.t(), (term(), String.t() -> validation_result())) :: validation_result()
  def validate_indexed(items, param, validator) when is_list(items) and is_function(validator, 2) do
    items
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {item, index}, :ok ->
      case validator.(item, "#{param}[#{index}]") do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @spec validate_optional_boolean(map(), String.t(), String.t()) :: validation_result()
  def validate_optional_boolean(map, key, param) do
    case Map.fetch(map, key) do
      :error -> :ok
      {:ok, value} when is_boolean(value) -> :ok
      {:ok, _value} -> {:error, Error.reason(400, "invalid_type", "invalid type for parameter #{param} (invalid_type); expected a boolean", param)}
    end
  end

  @spec validate_tool_async(term(), String.t()) :: validation_result()
  def validate_tool_async(%{"type" => "namespace", "async" => _async}, param) do
    param = param <> ".async"
    {:error, Error.reason(400, "unknown_parameter", "unknown parameter #{param} (unknown_parameter)", param)}
  end

  def validate_tool_async(%{"type" => "namespace", "tools" => tools}, param) when is_list(tools),
    do: validate_indexed(tools, param <> ".tools", &validate_tool_async/2)

  def validate_tool_async(%{"type" => type} = tool, param) when type in ["function", "custom"],
    do: validate_optional_boolean(tool, "async", param <> ".async")

  def validate_tool_async(_tool, _param), do: :ok

  @spec validate_shell(atom(), term()) :: :ok | {:error, Error.reason()}
  def validate_shell(adapter, payload) when is_atom(adapter) and is_map(payload) do
    case adapter do
      :responses -> validate_result(Responses.validate(payload))
      :chat -> validate_result(Chat.validate(payload))
      :files -> validate_result(Files.validate_create(payload))
      :audio -> validate_result(Audio.validate_transcription(payload))
      :image_generations -> validate_result(Images.validate_generation(payload))
      :image_edits -> validate_result(Images.validate_edit(payload))
      _adapter -> :ok
    end
  end

  def validate_shell(_adapter, _payload),
    do: {:error, Error.invalid_request("request body must be an object")}

  @spec normalize_payload(term()) :: {:ok, map()} | {:error, Error.reason()}
  def normalize_payload(payload) when is_map(payload) do
    {:ok, normalize_value(payload)}
  end

  def normalize_payload(_payload),
    do: {:error, Error.invalid_request("request body must be an object")}

  @spec reject_high_impact_fields(map()) :: :ok | {:error, Error.reason()}
  def reject_high_impact_fields(payload) when is_map(payload) do
    if Map.has_key?(payload, "logprobs"),
      do: {:error, Error.unsupported_parameter("logprobs")},
      else: :ok
  end

  @spec reject_unsupported_fields(map(), atom()) :: :ok | {:error, Error.reason()}
  def reject_unsupported_fields(payload, adapter) when is_map(payload) do
    supported = MapSet.new(Matrix.supported_fields(adapter))

    payload
    |> Map.keys()
    |> Enum.reject(&MapSet.member?(supported, &1))
    |> case do
      [] -> :ok
      [field | _rest] -> {:error, Error.unsupported_parameter(field)}
    end
  end

  @spec require_model(map()) :: :ok | {:error, Error.reason()}
  def require_model(payload) when is_map(payload) do
    case clean_string(Map.get(payload, "model")) do
      nil -> {:error, Error.invalid_request("model is required", "model")}
      _model -> :ok
    end
  end

  @spec clean_string(term()) :: String.t() | nil
  def clean_string(value) when is_binary(value) do
    value = String.trim(value)
    if value == "", do: nil, else: value
  end

  def clean_string(_value), do: nil

  @reasoning_effort_token_pattern ~r/^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$/

  @spec validate_reasoning_effort_token(term(), String.t()) :: :ok | {:error, Error.reason()}
  def validate_reasoning_effort_token(value, param) when is_binary(value) do
    value = String.trim(value)

    if Regex.match?(@reasoning_effort_token_pattern, value) and not repeated_separator?(value) do
      :ok
    else
      {:error, Error.invalid_request("reasoning effort is not supported", param)}
    end
  end

  def validate_reasoning_effort_token(_value, param),
    do: {:error, Error.invalid_request("reasoning effort is not supported", param)}

  @spec normalize_value(term()) :: term()
  def normalize_value(%Plug.Upload{} = upload), do: upload

  def normalize_value(%{} = value) do
    Map.new(value, fn {key, item_value} -> {to_string(key), normalize_value(item_value)} end)
  end

  def normalize_value(values) when is_list(values), do: Enum.map(values, &normalize_value/1)
  def normalize_value(value), do: value

  @spec upload_metadata(term()) :: {:ok, map()} | {:error, Error.reason()}
  def upload_metadata(%Plug.Upload{} = upload) do
    case File.stat(upload.path || "") do
      {:ok, stat} ->
        {:ok,
         %{
           "filename" => safe_upload_filename(upload.filename),
           "content_type" => upload.content_type || "application/octet-stream",
           "bytes" => stat.size,
           "path" => upload.path
         }}

      {:error, _reason} ->
        {:error, Error.invalid_request("file upload is not readable", "file")}
    end
  end

  def upload_metadata(%{"filename" => filename, "bytes" => bytes} = metadata)
      when is_binary(filename) and is_integer(bytes) and bytes >= 0 do
    {:ok,
     %{
       "filename" => safe_upload_filename(filename),
       "content_type" => Map.get(metadata, "content_type") || "application/octet-stream",
       "bytes" => bytes
     }}
  end

  def upload_metadata(_value),
    do: {:error, Error.invalid_request("file metadata is invalid", "file")}

  defp repeated_separator?(value) do
    String.contains?(value, ["__", "--", "_-", "-_"])
  end

  defp safe_upload_filename(filename) when is_binary(filename) do
    filename |> Path.basename() |> String.slice(0, 255) |> clean_string() || "upload"
  end

  defp safe_upload_filename(_filename), do: "upload"

  defp validate_result({:ok, _result}), do: :ok
  defp validate_result({:error, reason}), do: {:error, reason}
end
