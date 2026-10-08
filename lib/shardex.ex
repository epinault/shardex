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
end
