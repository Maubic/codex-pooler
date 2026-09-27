defmodule CodexPoolerWeb.DateTimeInput do
  @moduledoc """
  Converts operator wall-clock inputs and calendar days to absolute UTC instants.
  """

  @type input_error :: :invalid | :ambiguous | :gap | :unknown_timezone
  @type boundary :: :date_from | :date_to

  @spec local_value(DateTime.t() | nil, String.t()) :: String.t()
  def local_value(nil, _timezone), do: ""

  def local_value(%DateTime{} = datetime, timezone) do
    datetime
    |> DateTime.shift_zone!(timezone, Tz.TimeZoneDatabase)
    |> Calendar.strftime("%Y-%m-%dT%H:%M")
  end

  @spec parse(term(), String.t()) :: {:ok, DateTime.t() | nil} | {:error, input_error()}
  def parse(nil, _timezone), do: {:ok, nil}

  def parse(value, timezone) when is_binary(value) do
    case String.trim(value) do
      "" -> {:ok, nil}
      value -> parse_datetime(value, timezone)
    end
  end

  def parse(_value, _timezone), do: {:error, :invalid}

  @spec describe(DateTime.t(), String.t()) :: String.t()
  def describe(%DateTime{} = datetime, timezone) do
    local = DateTime.shift_zone!(datetime, timezone, Tz.TimeZoneDatabase)
    offset = Calendar.strftime(local, "%z")
    offset = String.slice(offset, 0, 3) <> ":" <> String.slice(offset, 3, 2)
    Calendar.strftime(local, "%Y-%m-%d %H:%M") <> " #{timezone} (UTC#{offset})"
  end

  @spec date_boundary(String.t(), boundary(), String.t()) :: {:ok, DateTime.t()} | {:error, input_error()}
  def date_boundary(value, boundary, timezone) do
    with {:ok, date} <- Date.from_iso8601(value),
         {:ok, start} <- day_start(date, timezone, false) do
      finish_boundary(date, start, boundary, timezone)
    else
      {:error, reason} when reason in [:gap, :unknown_timezone] -> {:error, reason}
      {:error, _reason} -> {:error, :invalid}
    end
  end

  defp finish_boundary(date, _start, :date_to, timezone) do
    case day_start(Date.add(date, 1), timezone, true) do
      {:ok, next_start} -> {:ok, DateTime.add(next_start, -1, :microsecond)}
      {:error, _reason} = error -> error
    end
  end

  defp finish_boundary(_date, start, :date_from, _timezone), do: {:ok, start}

  defp parse_datetime(value, timezone) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, _reason} -> parse_local(value, timezone)
    end
  end

  defp parse_local(value, timezone) do
    value = if String.length(value) == 16, do: value <> ":00", else: value

    case NaiveDateTime.from_iso8601(value) do
      {:ok, naive} -> local_datetime(naive, timezone)
      {:error, _reason} -> {:error, :invalid}
    end
  end

  defp local_datetime(naive, timezone) do
    case DateTime.from_naive(naive, timezone, Tz.TimeZoneDatabase) do
      {:ok, datetime} -> {:ok, utc(datetime)}
      {:ambiguous, _first, _second} -> {:error, :ambiguous}
      {:gap, _before, _after} -> {:error, :gap}
      {:error, _reason} -> {:error, :unknown_timezone}
    end
  end

  defp day_start(date, timezone, allow_skipped_date?) do
    case DateTime.new(date, ~T[00:00:00], timezone, Tz.TimeZoneDatabase) do
      {:ok, datetime} ->
        {:ok, utc(datetime)}

      {:ambiguous, first, _second} ->
        {:ok, utc(first)}

      {:gap, _before, first} ->
        if allow_skipped_date? or DateTime.to_date(first) == date,
          do: {:ok, utc(first)},
          else: {:error, :gap}

      {:error, _reason} ->
        {:error, :unknown_timezone}
    end
  end

  defp utc(datetime), do: DateTime.shift_zone!(datetime, "Etc/UTC", Calendar.UTCOnlyTimeZoneDatabase)
end
