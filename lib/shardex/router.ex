defmodule Shardex.Router do
  # Read path. Runs in the caller's process; only reads ETS.
  @moduledoc false

  alias Shardex.Adapter
  alias Shardex.Batch
  alias Shardex.BatchError
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

  @spec group(atom(), Enumerable.t(), (term() -> term()), keyword()) :: Batch.t()
  def group(instance, items, key_fun, opts) do
    started = System.monotonic_time()
    topology = State.topology!(instance)
    keyed = Enum.map(items, &{key_fun.(&1), &1})
    keys = keyed |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
    results = route_keys(instance, topology, keys)
    batch = build_batch(keyed, results, opts)

    Telemetry.execute(
      [:batch, :stop],
      %{
        duration: System.monotonic_time() - started,
        items: length(keyed),
        groups: map_size(batch.groups),
        errors: length(batch.errors)
      },
      %{instance: instance}
    )

    batch
  end

  @spec run_batch(atom(), Enumerable.t(), (term() -> term()), (term(), [term()] -> result), keyword()) ::
          {:ok, %{atom() => result}, [{term(), Shardex.reason()}]}
        when result: term()
  def run_batch(instance, items, key_fun, fun, opts) do
    {on_error, max_concurrency, timeout} = batch_opts!(opts)
    batch = group(instance, items, key_fun, opts)

    if on_error == :raise and batch.errors != [] do
      raise BatchError, errors: batch.errors, instance: instance
    end

    run_group = fn {name, %{shard: shard, pool: pool, items: group_items}} ->
      {name, execute(instance, shard, pool, &fun.(&1, group_items))}
    end

    results =
      if max_concurrency == 1 do
        Enum.map(batch.groups, run_group)
      else
        batch.groups
        |> Task.async_stream(run_group, max_concurrency: max_concurrency, ordered: false, timeout: timeout)
        |> Enum.map(fn {:ok, result} -> result end)
      end

    {:ok, Map.new(results), batch.errors}
  end

  defp batch_opts!(opts) do
    on_error = Keyword.get(opts, :on_error, :collect)
    max_concurrency = Keyword.get(opts, :max_concurrency, 1)
    timeout = Keyword.get(opts, :timeout, :infinity)

    if on_error not in [:collect, :raise] do
      raise ArgumentError, "expected :on_error to be :collect or :raise, got: #{inspect(on_error)}"
    end

    if not (is_integer(max_concurrency) and max_concurrency > 0) do
      raise ArgumentError, "expected :max_concurrency to be a positive integer, got: #{inspect(max_concurrency)}"
    end

    if not ((is_integer(timeout) and timeout > 0) or timeout == :infinity) do
      raise ArgumentError, "expected :timeout to be a positive integer or :infinity, got: #{inspect(timeout)}"
    end

    {on_error, max_concurrency, timeout}
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

  # Returns %{key => {:ok, %Shard{}} | {:error, reason}}.
  defp route_keys(_instance, _topology, []), do: %{}
  defp route_keys(_instance, %Topology{active: []}, keys), do: Map.new(keys, &{&1, {:error, :no_routable_shards}})

  defp route_keys(instance, topology, keys) do
    meta = %{instance: instance, strategy: topology.strategy, batch?: true}

    raw =
      Telemetry.span_measured([:route], meta, fn ->
        {Strategy.route_many(topology.strategy, keys, ctx(instance, topology)), %{key_count: length(keys)}}
      end)

    shards =
      raw
      |> Map.values()
      |> Enum.flat_map(fn
        {:ok, name} -> [name]
        _other -> []
      end)
      |> Enum.uniq()
      |> Map.new(&{&1, fetch_routable(instance, &1)})

    Map.new(keys, fn key -> {key, resolve_batch(Map.get(raw, key, {:error, :unassigned}), shards)} end)
  end

  defp resolve_batch({:ok, name}, shards), do: Map.fetch!(shards, name)
  defp resolve_batch({:error, _reason} = error, _shards), do: error
  defp resolve_batch(other, _shards), do: {:error, {:strategy_error, other}}

  defp build_batch(keyed, results, opts) do
    pools =
      results
      |> Map.values()
      |> Enum.flat_map(fn
        {:ok, shard} -> [shard]
        _error -> []
      end)
      |> Enum.uniq_by(& &1.name)
      |> Map.new(&{&1.name, select_pool(&1, opts)})

    {groups, errors} =
      keyed
      |> Enum.reverse()
      |> Enum.reduce({%{}, []}, fn {key, item}, {groups, errors} ->
        with {:ok, shard} <- Map.fetch!(results, key),
             {:ok, pool} <- Map.fetch!(pools, shard.name) do
          group = Map.get(groups, shard.name, %{shard: shard, pool: pool, items: []})
          {Map.put(groups, shard.name, %{group | items: [item | group.items]}), errors}
        else
          {:error, reason} -> {groups, [{item, reason} | errors]}
        end
      end)

    %Batch{groups: groups, errors: errors}
  end

  defp ctx(instance, topology), do: %{shards: topology.shards, state: topology.strategy_state, instance: instance}
end
