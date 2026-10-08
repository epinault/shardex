defmodule Shardex.Test.RecordingStrategy do
  @moduledoc false
  # Sends {:route_many, keys} to opts[:pid]. Keys in opts[:routes] return the
  # mapped result; other keys hash over ctx.shards.
  @behaviour Shardex.Strategy

  @impl true
  def init(opts, _shards), do: {:ok, %{pid: Keyword.fetch!(opts, :pid), routes: Keyword.get(opts, :routes, %{})}}

  @impl true
  def route_many(keys, %{state: state, shards: shards}) do
    send(state.pid, {:route_many, keys})

    Map.new(keys, fn key ->
      {key, Map.get(state.routes, key, {:ok, Enum.at(shards, :erlang.phash2(key, length(shards)))})}
    end)
  end
end
