defmodule Shardex.Error do
  @moduledoc "Raised by the bang routing functions (`ref!/3`, `run!/4`)."

  defexception [:reason, :key, :instance]

  @impl true
  def message(%__MODULE__{reason: reason, key: key, instance: instance}) do
    "#{inspect(instance)} could not route key #{inspect(key)}: #{describe(reason)}"
  end

  @doc false
  @spec describe(Shardex.reason()) :: String.t()
  def describe(:maintenance), do: "the shard (or requested role) is in maintenance"
  def describe(:unassigned), do: "the strategy has no shard assigned to this key"
  def describe(:no_routable_shards), do: "no shard is currently active"

  def describe({:unknown_shard, name}), do: "the strategy returned #{inspect(name)}, which is not a configured shard"

  def describe({:unknown_role, role}), do: "the shard has no usable #{inspect(role)} role"

  def describe({:strategy_error, value}), do: "the strategy returned an invalid value: #{inspect(value)}"

  def describe(other), do: inspect(other)
end

defmodule Shardex.BatchError do
  @moduledoc "Raised by `Shardex.run_batch/5` with `on_error: :raise` when items cannot be routed."

  defexception [:errors, :instance]

  @impl true
  def message(%__MODULE__{errors: errors, instance: instance}) do
    details =
      errors
      |> Enum.take(5)
      |> Enum.map_join("; ", fn {item, reason} -> "#{inspect(item)}: #{Shardex.Error.describe(reason)}" end)

    "#{inspect(instance)} could not route #{length(errors)} item(s): #{details}"
  end
end
