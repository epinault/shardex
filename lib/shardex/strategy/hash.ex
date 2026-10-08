defmodule Shardex.Strategy.Hash do
  @moduledoc """
  Default strategy: `:erlang.phash2(key, shard_count)` over the configured
  shard list. Adding or removing a shard remaps most keys; use
  `Shardex.Strategy.JumpHash` if the shard count will grow.
  """

  @behaviour Shardex.Strategy

  @impl true
  def init(_opts, shards), do: {:ok, List.to_tuple(shards)}

  @impl true
  def route_many(keys, ctx), do: Map.new(keys, &{&1, route(&1, ctx)})

  @impl true
  def route(key, %{state: shards}), do: {:ok, elem(shards, :erlang.phash2(key, tuple_size(shards)))}

  @impl true
  def trace?, do: false
end
