defmodule Shardex.Strategy do
  @moduledoc """
  Behaviour for routing keys to shard names.

  `route_many/2` is the only required callback: routing is batch-first and
  single-key routing is derived from it unless `route/2` is implemented.

  `ctx.shards` is the full configured shard list (in configuration order),
  including shards in maintenance, so maintenance never remaps keys. Shardex
  validates every result: unknown shard names become
  `{:error, {:unknown_shard, name}}`, shards in maintenance become
  `{:error, :maintenance}`, keys missing from the returned map become
  `{:error, :unassigned}`.

  A strategy may also be given as a function `(key, ctx -> result)`.
  """

  @type ctx :: %{shards: [atom()], state: term(), instance: atom()}
  @type result :: {:ok, atom()} | {:error, term()}

  @doc "Builds the strategy state. Runs inside the Coordinator. Default: `{:ok, opts}`."
  @callback init(opts :: keyword(), shards :: [atom()]) :: {:ok, term()}

  @doc "Routes many keys at once. Keys missing from the result are `:unassigned`."
  @callback route_many(keys :: [term()], ctx()) :: %{optional(term()) => result()}

  @doc "Routes one key. Default: `route_many([key], ctx)`."
  @callback route(key :: term(), ctx()) :: result()

  @doc "Called after `add_shard`/`remove_shard`. Default: `init(original_opts, shards)`."
  @callback on_topology_change(state :: term(), shards :: [atom()]) :: {:ok, term()}

  @doc "Whether single-key routes emit `[:shardex, :route, ...]` spans. Default: `true`."
  @callback trace?() :: boolean()

  @optional_callbacks init: 2, route: 2, on_topology_change: 2, trace?: 0

  @doc false
  @spec validate(term()) :: {:ok, {module(), keyword()}} | {:error, String.t()}
  def validate(fun) when is_function(fun, 2), do: {:ok, {Shardex.Strategy.Function, [fun: fun]}}
  def validate({mod, opts}) when is_atom(mod) and is_list(opts), do: validate_module(mod, opts)
  def validate(mod) when is_atom(mod), do: validate_module(mod, [])

  def validate(other),
    do: {:error, "expected a strategy module, {module, opts} or a 2-arity function, got: #{inspect(other)}"}

  defp validate_module(mod, opts) do
    if exports?(mod, :route_many, 2) do
      {:ok, {mod, opts}}
    else
      {:error, "#{inspect(mod)} does not implement the Shardex.Strategy behaviour (missing route_many/2)"}
    end
  end

  @doc false
  @spec init(module(), keyword(), [atom()]) :: {:ok, term()}
  def init(mod, opts, shards) do
    if exports?(mod, :init, 2), do: mod.init(opts, shards), else: {:ok, opts}
  end

  @doc false
  @spec route(module(), term(), ctx()) :: term()
  def route(mod, key, ctx) do
    if exports?(mod, :route, 2) do
      mod.route(key, ctx)
    else
      [key] |> mod.route_many(ctx) |> Map.get(key, {:error, :unassigned})
    end
  end

  @doc false
  @spec route_many(module(), [term()], ctx()) :: map()
  def route_many(mod, keys, ctx), do: mod.route_many(keys, ctx)

  @doc false
  @spec on_topology_change(module(), term(), keyword(), [atom()]) :: {:ok, term()}
  def on_topology_change(mod, state, opts, shards) do
    if exports?(mod, :on_topology_change, 2),
      do: mod.on_topology_change(state, shards),
      else: init(mod, opts, shards)
  end

  @doc false
  @spec trace?(module()) :: boolean()
  def trace?(mod), do: if(exports?(mod, :trace?, 0), do: mod.trace?(), else: true)

  # Modules are loaded lazily in interactive mode, so make sure the module is
  # loaded before checking its exports.
  defp exports?(mod, fun, arity) do
    Code.ensure_loaded?(mod) and function_exported?(mod, fun, arity)
  end
end
