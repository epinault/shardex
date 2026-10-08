defmodule Shardex.Router do
  # Read path. Runs in the caller's process; only reads ETS.
  @moduledoc false

  alias Shardex.Adapter, as: Adapter
  alias Shardex.Error
  alias Shardex.Pool
  alias Shardex.Shard
  alias Shardex.State
  alias Shardex.Strategy
  alias Shardex.Telemetry
  alias Shardex.Topology

  @spec lookup(atom(), term(), keyword()) :: {:ok, Shard.t()} | {:error, Shardex.reason()}
  def lookup(instance, key, _opts) do
    case State.topology!(instance) do
      %Topology{active: []} -> {:error, :no_routable_shards}
      topology -> topology |> route_one(instance, key) |> resolve_shard(instance)
    end
  end

  @spec ref(atom(), term(), keyword()) :: {:ok, term()} | {:error, Shardex.reason()}
  def ref(instance, key, opts) do
    with {:ok, shard} <- lookup(instance, key, opts),
         {:ok, pool} <- select_pool(shard, opts) do
      {:ok, pool.ref}
    end
  end

  @spec ref!(atom(), term(), keyword()) :: term()
  def ref!(instance, key, opts) do
    case ref(instance, key, opts) do
      {:ok, ref} -> ref
      {:error, reason} -> raise Error, reason: reason, key: key, instance: instance
    end
  end

  @spec run(atom(), term(), (term() -> result), keyword()) :: {:ok, result} | {:error, Shardex.reason()}
        when result: term()
  def run(instance, key, fun, opts) do
    with {:ok, shard} <- lookup(instance, key, opts),
         {:ok, pool} <- select_pool(shard, opts) do
      {:ok, execute(instance, shard, pool, fun)}
    end
  end

  @spec run!(atom(), term(), (term() -> result), keyword()) :: result when result: term()
  def run!(instance, key, fun, opts) do
    case run(instance, key, fun, opts) do
      {:ok, result} -> result
      {:error, reason} -> raise Error, reason: reason, key: key, instance: instance
    end
  end

  @doc false
  @spec select_pool(Shard.t(), keyword()) :: {:ok, Pool.t()} | {:error, Shardex.reason()}
  def select_pool(%Shard{roles: roles}, opts) do
    role = Keyword.get(opts, :role, :primary)
    candidates = [role | List.wrap(Keyword.get(opts, :fallback, []))]

    case Enum.find_value(candidates, &active_pool(roles, &1)) do
      %Pool{} = pool ->
        {:ok, pool}

      nil ->
        if Enum.any?(candidates, &Map.has_key?(roles, &1)),
          do: {:error, :maintenance},
          else: {:error, {:unknown_role, role}}
    end
  end

  defp active_pool(roles, role) do
    case Map.fetch(roles, role) do
      {:ok, %Pool{status: :active} = pool} -> pool
      _other -> nil
    end
  end

  defp route_one(topology, instance, key) do
    ctx = ctx(instance, topology)

    if Strategy.trace?(topology.strategy) do
      meta = %{instance: instance, strategy: topology.strategy, batch?: false}

      Telemetry.span_measured([:route], meta, fn ->
        {Strategy.route(topology.strategy, key, ctx), %{key_count: 1}}
      end)
    else
      Strategy.route(topology.strategy, key, ctx)
    end
  end

  defp resolve_shard({:ok, name}, instance), do: fetch_routable(instance, name)
  defp resolve_shard({:error, _reason} = error, _instance), do: error
  defp resolve_shard(other, _instance), do: {:error, {:strategy_error, other}}

  defp fetch_routable(instance, name) do
    case State.fetch_shard(instance, name) do
      {:ok, %Shard{status: :active} = shard} -> {:ok, shard}
      {:ok, %Shard{}} -> {:error, :maintenance}
      :error -> {:error, {:unknown_shard, name}}
    end
  end

  defp execute(instance, shard, pool, fun) do
    Telemetry.span([:run], %{instance: instance, shard: shard.name, role: pool.role}, fn -> Adapter.run(pool, fun) end)
  end

  defp ctx(instance, topology), do: %{shards: topology.shards, state: topology.strategy_state, instance: instance}
end
