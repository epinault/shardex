defmodule Shardex.Strategy.JumpHash do
  @moduledoc """
  Jump consistent hash (Lamping & Veach, 2014). Appending a shard to the end of
  the shard list moves only ~1/n of the keys, all of them to the new shard.
  Always append new shards; reordering or removing shards remaps keys.
  """

  @behaviour Shardex.Strategy

  import Bitwise

  @two_pow_32 4_294_967_296
  @two_pow_64 18_446_744_073_709_551_616

  @impl true
  def init(_opts, shards), do: {:ok, List.to_tuple(shards)}

  @impl true
  def route_many(keys, ctx), do: Map.new(keys, &{&1, route(&1, ctx)})

  @impl true
  def route(key, %{state: shards}) do
    {:ok, elem(shards, jump(:erlang.phash2(key, @two_pow_32), tuple_size(shards)))}
  end

  @impl true
  def trace?, do: false

  @doc false
  @spec jump(non_neg_integer(), pos_integer()) :: non_neg_integer()
  def jump(key, buckets), do: do_jump(key, buckets, -1, 0)

  defp do_jump(_key, buckets, bucket, next) when next >= buckets, do: bucket

  defp do_jump(key, buckets, _bucket, next) do
    key = rem(key * 2_862_933_555_777_941_757 + 1, @two_pow_64)
    do_jump(key, buckets, next, trunc((next + 1) * (2_147_483_648 / ((key >>> 33) + 1))))
  end
end
