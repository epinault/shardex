defmodule Shardex.Strategy.Cached do
  @moduledoc """
  Caches another strategy's results in ETS. Useful for directory strategies
  that query a database.

      strategy: {Shardex.Strategy.Cached, inner: MyApp.ShardDirectory, ttl: :timer.minutes(5)}

  Options:

    * `:inner` (required) - the wrapped strategy (module, `{module, opts}` or function)
    * `:ttl` - entry lifetime in milliseconds or `:infinity` (default 5 minutes)
    * `:cache_errors` - also cache `{:error, _}` results (default `false`)

  Only cache misses reach the inner strategy, in one `route_many/2` call.
  The cache is cleared on `add_shard`/`remove_shard`; use `Shardex.invalidate/2`
  when an assignment changes.

  Call `Shardex.invalidate/2` only after the new assignment is visible to the
  inner strategy. Invalidation is not atomic with lookups: a lookup that missed
  the cache concurrently can re-cache the old value after `invalidate/2`, where
  it stays until `:ttl` expires. Choose `:ttl` to bound how long a stale route
  is acceptable.

  Expired entries are only replaced when their key is looked up again, so
  memory grows with the number of distinct keys routed. If that matters, use a
  bounded key space, or a finite `:ttl` plus a periodic `Shardex.invalidate(instance, :all)`.
  """

  @behaviour Shardex.Strategy

  alias Shardex.Strategy

  @impl true
  def init(opts, shards) do
    {inner, inner_opts} = inner!(Keyword.fetch!(opts, :inner))
    {:ok, inner_state} = Strategy.init(inner, inner_opts, shards)

    {:ok,
     %{
       inner: inner,
       inner_opts: inner_opts,
       inner_state: inner_state,
       table: :ets.new(__MODULE__, [:set, :public, read_concurrency: true, write_concurrency: true]),
       ttl: ttl!(Keyword.get(opts, :ttl, to_timeout(minute: 5))),
       cache_errors: Keyword.get(opts, :cache_errors, false)
     }}
  end

  @impl true
  def route_many(keys, %{state: state} = ctx) do
    now = System.monotonic_time(:millisecond)

    {hits, misses} =
      Enum.reduce(keys, {%{}, []}, fn key, {hits, misses} ->
        case :ets.lookup(state.table, key) do
          [{^key, result, expires_at}] when expires_at == :infinity or expires_at > now ->
            {Map.put(hits, key, result), misses}

          _miss ->
            {hits, [key | misses]}
        end
      end)

    misses
    |> Enum.reverse()
    |> fetch_misses(%{ctx | state: state.inner_state}, state, now)
    |> Map.merge(hits)
  end

  @impl true
  def on_topology_change(state, shards) do
    true = :ets.delete_all_objects(state.table)
    {:ok, inner_state} = Strategy.on_topology_change(state.inner, state.inner_state, state.inner_opts, shards)
    {:ok, %{state | inner_state: inner_state}}
  end

  @impl true
  def trace?, do: true

  @doc false
  @spec invalidate(map(), term()) :: :ok
  def invalidate(state, :all) do
    true = :ets.delete_all_objects(state.table)
    :ok
  end

  def invalidate(state, key) do
    true = :ets.delete(state.table, key)
    :ok
  end

  defp fetch_misses([], _ctx, _state, _now), do: %{}

  defp fetch_misses(keys, ctx, state, now) do
    results = Strategy.route_many(state.inner, keys, ctx)
    expires_at = if state.ttl == :infinity, do: :infinity, else: now + state.ttl
    entries = for {key, result} <- results, cacheable?(result, state), do: {key, result, expires_at}
    true = :ets.insert(state.table, entries)
    results
  end

  defp cacheable?({:ok, _shard}, _state), do: true
  defp cacheable?(_error, state), do: state.cache_errors

  defp inner!(inner) do
    case Strategy.validate(inner) do
      {:ok, strategy} -> strategy
      {:error, message} -> raise ArgumentError, "invalid :inner strategy for Shardex.Strategy.Cached: " <> message
    end
  end

  defp ttl!(ttl) when (is_integer(ttl) and ttl > 0) or ttl == :infinity, do: ttl
  defp ttl!(ttl), do: raise(ArgumentError, "expected :ttl to be a positive integer or :infinity, got: #{inspect(ttl)}")
end
