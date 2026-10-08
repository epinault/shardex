defmodule Shardex.Topology do
  @moduledoc false

  @type t :: %__MODULE__{
          shards: [atom()],
          active: [atom()],
          strategy: module() | nil,
          strategy_opts: keyword(),
          strategy_state: term(),
          version: pos_integer()
        }

  defstruct shards: [], active: [], strategy: nil, strategy_opts: [], strategy_state: nil, version: 1
end
