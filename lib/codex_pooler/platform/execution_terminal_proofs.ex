defmodule CodexPooler.Platform.ExecutionTerminalProofs do
  @moduledoc false
  import Ecto.Query

  alias CodexPooler.Platform.ExecutionTerminalProof
  alias CodexPooler.Repo

  @retention_seconds 6 * 60 * 60
  @type terminal :: %{
          required(:owner_execution_id) => Ecto.UUID.t(),
          required(:owner_instance_id) => String.t(),
          required(:owner_instance_boot_id) => String.t(),
          required(:owner_process_id) => String.t(),
          required(:end_kind) => String.t(),
          required(:ended_at) => DateTime.t(),
          optional(:interruption_code) => String.t() | nil
        }

  @spec retention_seconds() :: pos_integer()
  def retention_seconds, do: @retention_seconds

  @spec terminal?(map(), keyword()) :: boolean()
  def terminal?(identity, opts \\ []) do
    if valid_identity?(identity) do
      query =
        from proof in ExecutionTerminalProof,
          where:
            proof.execution_id == ^identity.owner_execution_id and
              proof.owner_instance_id == ^identity.owner_instance_id and
              proof.owner_instance_boot_id == ^identity.owner_instance_boot_id and
              proof.owner_process_id == ^identity.owner_process_id

      query =
        if Keyword.get(opts, :include_interrupted, true),
          do: query,
          else: where(query, [proof], is_nil(proof.interruption_code))

      Repo.exists?(query, Keyword.take(opts, [:timeout, :deadline, :checkout_retries]))
    else
      false
    end
  end

  @spec publish([terminal()], keyword()) :: {:ok, non_neg_integer()} | {:error, atom()}
  def publish(proofs, opts \\ [])

  def publish(proofs, opts) when is_list(proofs) and length(proofs) <= 100 do
    if Enum.all?(proofs, &valid_terminal?/1) do
      rows =
        Enum.map(proofs, fn proof ->
          proof
          |> Map.take([
            :owner_instance_id,
            :owner_instance_boot_id,
            :owner_process_id,
            :end_kind,
            :interruption_code,
            :ended_at
          ])
          |> Map.put(:execution_id, proof.owner_execution_id)
          |> Map.put(:interruption_code, Map.get(proof, :interruption_code))
        end)

      publish_rows(rows, opts)
    else
      {:error, :invalid_execution_terminal_proof}
    end
  end

  def publish(_proofs, _opts), do: {:error, :invalid_execution_terminal_proof}

  defp publish_rows(rows, opts) do
    Repo.transact(
      fn ->
        Repo.insert_all(ExecutionTerminalProof, rows,
          on_conflict: :nothing,
          conflict_target: :execution_id
        )

        ids = Enum.map(rows, & &1.execution_id)
        persisted = Repo.all(from p in ExecutionTerminalProof, where: p.execution_id in ^ids)

        exact = Enum.all?(rows, &exact_row?(&1, persisted))

        if exact, do: {:ok, length(rows)}, else: {:error, :execution_terminal_proof_conflict}
      end,
      Keyword.take(opts, [:timeout, :deadline, :checkout_retries])
    )
  end

  defp exact_row?(row, persisted),
    do: Enum.any?(persisted, &(Map.take(&1, Map.keys(row)) == row))

  @spec prune(DateTime.t()) :: {:ok, %{execution_terminal_proofs_pruned: non_neg_integer()}}
  def prune(_now) do
    expired =
      from p in ExecutionTerminalProof,
        where:
          p.published_at <
            fragment("(statement_timestamp() AT TIME ZONE 'UTC') - interval '6 hours'"),
        order_by: [asc: p.published_at, asc: p.execution_id],
        limit: 1_000,
        select: p.execution_id

    {count, _} =
      Repo.delete_all(from p in ExecutionTerminalProof, where: p.execution_id in subquery(expired))

    {:ok, %{execution_terminal_proofs_pruned: count}}
  end

  @spec valid_identity?(map()) :: boolean()
  def valid_identity?(identity) do
    valid_uuid?(Map.get(identity, :owner_execution_id)) and
      bounded?(Map.get(identity, :owner_instance_id), 255) and
      bounded?(Map.get(identity, :owner_instance_boot_id), 64) and
      bounded?(Map.get(identity, :owner_process_id), 64) and
      Regex.match?(~r/\A<0\.[0-9]+\.[0-9]+>\z/, identity.owner_process_id)
  end

  defp valid_terminal?(proof),
    do:
      valid_identity?(proof) and
        Map.get(proof, :end_kind) in ["completed", "process_down"] and
        Map.get(proof, :interruption_code) in [nil, "client_disconnected", "owner_drained", "owner_task_exception", "unobserved_exit"] and
        match?(%DateTime{}, Map.get(proof, :ended_at))

  defp valid_uuid?(id) when is_binary(id), do: match?({:ok, ^id}, Ecto.UUID.cast(id))
  defp valid_uuid?(_), do: false
  defp bounded?(value, max), do: is_binary(value) and byte_size(value) in 1..max
end
