defmodule Shardex.Shard do
  @moduledoc """
  A shard: a routing target with one pool per role (`:primary`, `:replica`, ...).

  `status` is `:active` (routable), `:drain` (not routable, pools running) or
  `:stopped` (not routable, pools stopped).
  """

  @type status :: :active | :drain | :stopped

  @type t :: %__MODULE__{
          name: atom(),
          status: status(),
          roles: %{atom() => Shardex.Pool.t()},
          meta: map()
        }

  @enforce_keys [:name]
  defstruct [:name, status: :active, roles: %{}, meta: %{}]
end
