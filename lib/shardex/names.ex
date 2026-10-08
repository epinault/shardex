defmodule Shardex.Names do
  @moduledoc false

  @spec coordinator(atom()) :: atom()
  def coordinator(instance), do: Module.concat(instance, Coordinator)

  @spec pool_sup(atom()) :: atom()
  def pool_sup(instance), do: Module.concat(instance, PoolSup)

  @spec table(atom()) :: atom()
  def table(instance), do: instance

  @spec pool(atom(), atom(), atom()) :: atom()
  def pool(instance, shard, role), do: Module.concat([instance, shard, role])
end
