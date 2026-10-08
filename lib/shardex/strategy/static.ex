defmodule Shardex.Strategy.Static do
  @moduledoc """
  Routes from a fixed map: `{Shardex.Strategy.Static, map: %{key => shard}, default: shard}`.
  Without `:default`, unmapped keys are `{:error, :unassigned}`. Handy in tests.
  """

  @behaviour Shardex.Strategy

  @impl true
  def init(opts, _shards), do: {:ok, %{map: Keyword.get(opts, :map, %{}), default: Keyword.get(opts, :default)}}

  @impl true
  def route_many(keys, ctx), do: Map.new(keys, &{&1, route(&1, ctx)})

  @impl true
  def route(key, %{state: %{map: map, default: default}}) do
    case Map.fetch(map, key) do
      {:ok, shard} -> {:ok, shard}
      :error when is_nil(default) -> {:error, :unassigned}
      :error -> {:ok, default}
    end
  end

  @impl true
  def trace?, do: false
end
