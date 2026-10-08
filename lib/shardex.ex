defmodule Shardex do
  @moduledoc """
  Route keys and batches of items to sharded pools.

  See the "Getting started" guide for an overview.
  """

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

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{id: Keyword.get(opts, :name, __MODULE__), start: {__MODULE__, :start_link, [opts]}, type: :supervisor}
  end
end
