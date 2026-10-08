defmodule Shardex.Telemetry do
  @moduledoc """
  Telemetry events emitted by Shardex. Every event's metadata includes `:instance`.

    * `[:shardex, :route, :start | :stop | :exception]` - span around strategy routing.
      Always emitted for batches; for single keys only when the strategy's `trace?/0`
      is `true`. Stop measurements include `:key_count`. Metadata: `:strategy`, `:batch?`.
    * `[:shardex, :run, :start | :stop | :exception]` - span around the function run
      against a pool. Metadata: `:shard`, `:role`.
    * `[:shardex, :batch, :stop]` - after grouping. Measurements: `:duration`, `:items`,
      `:groups`, `:errors`.
    * `[:shardex, :shard, :status_changed]` - Metadata: `:shard`, `:role` (`nil` for the
      whole shard), `:from`, `:to`.
    * `[:shardex, :topology, :changed]` - Metadata: `:action` (`:add | :remove`), `:shard`,
      `:version`.
  """

  @doc false
  @spec span([atom()], map(), (-> result)) :: result when result: term()
  def span(event, meta, fun), do: :telemetry.span([:shardex | event], meta, fn -> {fun.(), meta} end)

  @doc false
  @spec span_measured([atom()], map(), (-> {result, map()})) :: result when result: term()
  def span_measured(event, meta, fun) do
    :telemetry.span([:shardex | event], meta, fn ->
      {result, measurements} = fun.()
      {result, measurements, meta}
    end)
  end

  @doc false
  @spec execute([atom()], map(), map()) :: :ok
  def execute(event, measurements, meta), do: :telemetry.execute([:shardex | event], measurements, meta)
end
