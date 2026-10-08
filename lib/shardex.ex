defmodule Shardex do
  @moduledoc """
  Route keys and batches of items to sharded pools.

  See the "Getting started" guide for an overview.
  """

  alias Shardex.Router

  @typedoc "Reason returned in `{:error, reason}` by the routing functions."
  @type reason ::
          :maintenance
          | :unassigned
          | :no_routable_shards
          | {:unknown_shard, term()}
          | {:unknown_role, atom()}
          | {:strategy_error, term()}
          | term()

  @typedoc "An instance: the `use Shardex` module or the `:name` given to `start_link/1`."
  @type instance :: atom()

  @doc """
  Starts an instance. Usually you `use Shardex` instead and add your module to
  your supervision tree.

  ## Options

  #{NimbleOptions.docs(Shardex.Config.schema())}
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts), do: Shardex.Supervisor.start_link(opts)

  @doc """
  Returns the shard a key routes to.

  Errors: `:unassigned`, `:maintenance`, `:no_routable_shards`,
  `{:unknown_shard, name}`, `{:strategy_error, value}` or any `{:error, term}`
  returned by your strategy.
  """
  @spec lookup(instance(), term(), keyword()) :: {:ok, Shardex.Shard.t()} | {:error, reason()}
  def lookup(instance, key, opts \\ []), do: Router.lookup(instance, key, opts)

  @doc """
  Returns the ref of the pool a key routes to.

  ## Options

    * `:role` - pool role (default `:primary`)
    * `:fallback` - role or list of roles to try, in order, when `:role` is missing
      or in maintenance
  """
  @spec ref(instance(), term(), keyword()) :: {:ok, term()} | {:error, reason()}
  def ref(instance, key, opts \\ []), do: Router.ref(instance, key, opts)

  @doc "Like `ref/3` but raises `Shardex.Error`."
  @spec ref!(instance(), term(), keyword()) :: term()
  def ref!(instance, key, opts \\ []), do: Router.ref!(instance, key, opts)

  @doc """
  Runs `fun` against the pool a key routes to, inside the adapter's context
  (for Ecto dynamic repos, `put_dynamic_repo/1` is set and restored).

  Accepts the same options as `ref/3`. Exceptions raised by `fun` propagate;
  nothing is retried.
  """
  @spec run(instance(), term(), (term() -> result), keyword()) :: {:ok, result} | {:error, reason()}
        when result: term()
  def run(instance, key, fun, opts \\ []), do: Router.run(instance, key, fun, opts)

  @doc "Like `run/4` but returns the result directly and raises `Shardex.Error` on routing errors."
  @spec run!(instance(), term(), (term() -> result), keyword()) :: result when result: term()
  def run!(instance, key, fun, opts \\ []), do: Router.run!(instance, key, fun, opts)

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{id: Keyword.get(opts, :name, __MODULE__), start: {__MODULE__, :start_link, [opts]}, type: :supervisor}
  end
end
