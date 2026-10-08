defmodule Shardex.State do
  # ETS layout (table named after the instance, owned by the Coordinator):
  #   {:topology, %Shardex.Topology{}}
  #   {{:shard, name}, %Shardex.Shard{}}
  @moduledoc false

  alias Shardex.Names
  alias Shardex.Shard
  alias Shardex.Topology

  @spec create_table(atom()) :: atom()
  def create_table(instance) do
    :ets.new(Names.table(instance), [:named_table, :set, :protected, read_concurrency: true])
  end

  @spec topology!(atom()) :: Topology.t()
  def topology!(instance) do
    case lookup(instance, :topology) do
      [{:topology, topology}] -> topology
      [] -> raise ArgumentError, not_started_message(instance)
    end
  end

  @spec fetch_shard(atom(), term()) :: {:ok, Shard.t()} | :error
  def fetch_shard(instance, name) do
    case lookup(instance, {:shard, name}) do
      [{_key, shard}] -> {:ok, shard}
      [] -> :error
    end
  end

  @spec put_topology(atom(), Topology.t()) :: :ok
  def put_topology(instance, %Topology{} = topology) do
    true = :ets.insert(Names.table(instance), {:topology, topology})
    :ok
  end

  @spec put_shard(atom(), Shard.t()) :: :ok
  def put_shard(instance, %Shard{} = shard) do
    true = :ets.insert(Names.table(instance), {{:shard, shard.name}, shard})
    :ok
  end

  @spec delete_shard(atom(), atom()) :: :ok
  def delete_shard(instance, name) do
    true = :ets.delete(Names.table(instance), {:shard, name})
    :ok
  end

  @spec not_started_message(atom()) :: String.t()
  def not_started_message(instance), do: "Shardex instance #{inspect(instance)} is not started"

  defp lookup(instance, key) do
    :ets.lookup(Names.table(instance), key)
  rescue
    ArgumentError -> reraise ArgumentError, [message: not_started_message(instance)], __STACKTRACE__
  end
end
