defmodule Shardex.Strategy.Function do
  @moduledoc false
  # Wraps a `(key, ctx -> result)` function given as `strategy:`.

  @behaviour Shardex.Strategy

  @impl true
  def init(opts, _shards), do: {:ok, Keyword.fetch!(opts, :fun)}

  @impl true
  def route_many(keys, %{state: fun} = ctx) do
    user_ctx = %{ctx | state: nil}
    Map.new(keys, &{&1, fun.(&1, user_ctx)})
  end
end
