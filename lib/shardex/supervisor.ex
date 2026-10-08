defmodule Shardex.Supervisor do
  @moduledoc false
  use Supervisor

  alias Shardex.Config
  alias Shardex.Names

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    config = Config.validate!(opts)
    Supervisor.start_link(__MODULE__, config, name: config.name)
  end

  @impl true
  def init(config) do
    pool_sup = %{
      id: :pool_sup,
      type: :supervisor,
      start: {Supervisor, :start_link, [[], [strategy: :one_for_one, name: Names.pool_sup(config.name)]]}
    }

    Supervisor.init([pool_sup, {Shardex.Coordinator, config}], strategy: :rest_for_one)
  end
end
