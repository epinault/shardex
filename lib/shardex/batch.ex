defmodule Shardex.Batch do
  @moduledoc """
  Result of `Shardex.group/4`: items grouped by shard, plus every item that
  could not be routed with its reason. Items keep their input order.
  """

  @type group :: %{shard: Shardex.Shard.t(), pool: Shardex.Pool.t(), items: [term()]}

  @type t :: %__MODULE__{
          groups: %{atom() => group()},
          errors: [{term(), Shardex.reason()}]
        }

  defstruct groups: %{}, errors: []
end
