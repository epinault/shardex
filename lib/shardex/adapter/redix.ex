if Code.ensure_loaded?(Redix) do
  defmodule Shardex.Adapter.Redix do
    @moduledoc """
    Adapter for Redis via `Redix`. Requires the optional `:redix` dependency.

        cache_1: {Shardex.Adapter.Redix, host: "redis-1", port: 6379, pool_size: 4}

    All options except `:pool_size` are passed to `Redix.start_link/1`.
    `run/2` calls `fun` with a connection name, picked at random when
    `pool_size > 1`:

        MyApp.Shards.run(key, &Redix.command(&1, ["GET", key]))
    """

    @behaviour Shardex.Adapter

    defmodule Pool do
      @moduledoc "Ref of a Redix shard pool with `pool_size > 1`: the connection names."
      @type t :: %__MODULE__{names: tuple()}
      defstruct [:names]
    end

    @impl true
    def init(opts, %{instance: instance, shard: shard, role: role}) do
      {pool_size, redix_opts} = Keyword.pop(opts, :pool_size, 1)
      init_pool(pool_size, Shardex.Names.pool(instance, shard, role), redix_opts)
    end

    @impl true
    def run(%Pool{names: names}, fun), do: fun.(elem(names, :rand.uniform(tuple_size(names)) - 1))
    def run(name, fun), do: fun.(name)

    defp init_pool(1, name, opts) do
      {:ok, %{state: name, ref: name, child_spec: {Redix, Keyword.put(opts, :name, name)}, meta: %{}}}
    end

    defp init_pool(size, base, opts) when is_integer(size) and size > 1 do
      names = for n <- 1..size, do: Module.concat(base, "C#{n}")

      children =
        for {name, n} <- Enum.with_index(names, 1) do
          Supervisor.child_spec({Redix, Keyword.put(opts, :name, name)}, id: {:conn, n})
        end

      spec = %{id: base, type: :supervisor, start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]}}
      pool = %Pool{names: List.to_tuple(names)}
      {:ok, %{state: pool, ref: pool, child_spec: spec, meta: %{}}}
    end

    defp init_pool(size, _base, _opts), do: {:error, {:invalid_option, :pool_size, size}}
  end
end
