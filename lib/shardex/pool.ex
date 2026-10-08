defmodule Shardex.Pool do
  @moduledoc """
  One pool of a shard for a given role. `ref` is what callers receive
  (a repo module, a registered name, ...). `meta` is adapter-specific.
  """

  @type t :: %__MODULE__{
          role: atom(),
          adapter: module(),
          adapter_state: term(),
          ref: term(),
          child_spec: Supervisor.child_spec() | nil,
          status: Shardex.Shard.status(),
          meta: map()
        }

  @enforce_keys [:role, :adapter]
  defstruct [:role, :adapter, :adapter_state, :ref, :child_spec, status: :active, meta: %{}]
end
