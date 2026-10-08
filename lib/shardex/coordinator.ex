defmodule Shardex.Coordinator do
  @moduledoc false
  # Single writer for an instance's ETS table. Owns the table (and any ETS
  # tables strategies create in init/2). Starts/stops pools under PoolSup.
  use GenServer

  alias Shardex.Adapter
  alias Shardex.Names
  alias Shardex.Pool
  alias Shardex.Shard
  alias Shardex.State
  alias Shardex.Strategy
  alias Shardex.Telemetry
  alias Shardex.Topology

  require Logger

  @spec start_link(map()) :: GenServer.on_start()
  def start_link(config), do: GenServer.start_link(__MODULE__, config, name: Names.coordinator(config.name))

  @spec call(atom(), term()) :: term()
  def call(instance, message) do
    case GenServer.whereis(Names.coordinator(instance)) do
      nil -> raise ArgumentError, State.not_started_message(instance)
      pid -> GenServer.call(pid, message)
    end
  end

  @impl true
  def init(config) do
    instance = config.name
    State.create_table(instance)
    state = %{instance: instance, start_pools: config.start_pools, pool_sup: Names.pool_sup(instance)}
    {strategy, strategy_opts} = config.strategy
    names = Enum.map(config.shards, &elem(&1, 0))

    with {:ok, shards} <- map_ok(config.shards, fn {name, roles} -> build_shard(instance, name, roles) end),
         {:ok, strategy_state} <- Strategy.init(strategy, strategy_opts, names),
         :ok <- remove_orphans(state, shards),
         {:ok, shards} <- map_ok(shards, &boot_shard(&1, state, config.start_failure)) do
      Enum.each(shards, &State.put_shard(instance, &1))

      State.put_topology(instance, %Topology{
        shards: names,
        active: for(shard <- shards, shard.status == :active, do: shard.name),
        strategy: strategy,
        strategy_opts: strategy_opts,
        strategy_state: strategy_state
      })

      {:ok, state}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:set_status, name, role, target}, _from, state) do
    reply =
      with {:ok, shard} <- fetch(state.instance, name),
           {:ok, updated} <- apply_status(shard, role, target) do
        transition(state, shard, updated, target == :active)
      end

    {:reply, reply, state}
  end

  ## Status transitions

  defp fetch(instance, name) do
    case State.fetch_shard(instance, name) do
      {:ok, shard} -> {:ok, shard}
      :error -> {:error, {:unknown_shard, name}}
    end
  end

  defp apply_status(shard, nil, :active) do
    roles = Map.new(shard.roles, fn {role, pool} -> {role, %{pool | status: :active}} end)
    {:ok, %{shard | status: :active, roles: roles}}
  end

  defp apply_status(shard, nil, target), do: {:ok, %{shard | status: target}}

  defp apply_status(shard, role, target) do
    case Map.fetch(shard.roles, role) do
      {:ok, pool} -> {:ok, %{shard | roles: Map.put(shard.roles, role, %{pool | status: target})}}
      :error -> {:error, {:unknown_role, role}}
    end
  end

  # Becoming available: start pools first, then publish.
  # Becoming unavailable: publish first, then stop pools.
  defp transition(_state, unchanged, unchanged, _up?), do: :ok

  defp transition(state, old, new, true = _up?) do
    case sync_pools(new, state) do
      :ok ->
        commit(state, new)
        emit_changes(state.instance, old, new)

      {:error, _reason} = error ->
        # Roll back pools that did start before the failure.
        _ = sync_pools(old, state)
        error
    end
  end

  defp transition(state, old, new, false = _up?) do
    commit(state, new)
    emit_changes(state.instance, old, new)
    sync_pools(new, state)
  end

  defp commit(state, shard) do
    State.put_shard(state.instance, shard)
    topology = State.topology!(state.instance)

    State.put_topology(state.instance, %{
      topology
      | active: active_names(state.instance, topology.shards),
        version: topology.version + 1
    })
  end

  defp active_names(instance, names) do
    Enum.filter(names, &match?({:ok, %Shard{status: :active}}, State.fetch_shard(instance, &1)))
  end

  defp emit_changes(instance, old, new) do
    if old.status != new.status, do: emit_status(instance, new.name, nil, old.status, new.status)

    Enum.each(new.roles, fn {role, pool} ->
      old_status = Map.fetch!(old.roles, role).status
      if old_status != pool.status, do: emit_status(instance, new.name, role, old_status, pool.status)
    end)
  end

  ## Boot helpers

  defp build_shard(instance, name, roles) do
    Enum.reduce_while(roles, {:ok, %Shard{name: name}}, fn {role, spec}, {:ok, shard} ->
      case Adapter.build_pool(instance, name, role, spec) do
        {:ok, pool} -> {:cont, {:ok, %{shard | roles: Map.put(shard.roles, role, pool)}}}
        {:error, reason} -> {:halt, {:error, {:adapter_init_failed, name, role, reason}}}
      end
    end)
  end

  defp boot_shard(shard, state, start_failure) do
    case sync_pools(shard, state) do
      :ok ->
        {:ok, shard}

      {:error, reason} when start_failure == :stop_shard ->
        Logger.warning(
          "[Shardex] #{inspect(state.instance)} could not start shard #{inspect(shard.name)} " <>
            "(#{inspect(reason)}); it boots as :stopped"
        )

        stopped = %{shard | status: :stopped}
        :ok = sync_pools(stopped, state)
        emit_status(state.instance, shard.name, nil, :active, :stopped)
        {:ok, stopped}

      {:error, reason} ->
        {:error, {:pool_start_failed, shard.name, reason}}
    end
  end

  # Pools started for {shard, role} ids that are not in the boot config
  # (e.g. added at runtime before a Coordinator restart) are stopped.
  defp remove_orphans(state, shards) do
    wanted = for shard <- shards, {role, _pool} <- shard.roles, into: MapSet.new(), do: {shard.name, role}

    for {id, _pid, _type, _mods} <- Supervisor.which_children(state.pool_sup), not MapSet.member?(wanted, id) do
      stop_child(state.pool_sup, id)
    end

    :ok
  end

  ## Pool lifecycle
  # Invariant: a pool runs iff shard.status != :stopped and pool.status != :stopped.

  defp sync_pools(_shard, %{start_pools: false}), do: :ok

  defp sync_pools(shard, state) do
    Enum.reduce_while(shard.roles, :ok, fn {_role, pool}, :ok ->
      case ensure_pool(shard, pool, state.pool_sup) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp ensure_pool(_shard, %Pool{child_spec: nil}, _sup), do: :ok

  defp ensure_pool(shard, pool, sup) do
    if shard.status != :stopped and pool.status != :stopped,
      do: start_child(sup, pool.child_spec),
      else: stop_child(sup, pool.child_spec.id)
  end

  ## Shared helpers

  defp start_child(sup, spec) do
    case Supervisor.start_child(sup, spec) do
      {:ok, _pid} -> :ok
      {:ok, _pid, _info} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, :already_present} -> restart_child(sup, spec.id)
      {:error, reason} -> {:error, reason}
    end
  end

  defp restart_child(sup, id) do
    case Supervisor.restart_child(sup, id) do
      {:ok, _pid} -> :ok
      {:ok, _pid, _info} -> :ok
      {:error, :running} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp stop_child(sup, id) do
    _ = Supervisor.terminate_child(sup, id)
    _ = Supervisor.delete_child(sup, id)
    :ok
  end

  defp emit_status(instance, shard, role, from, to) do
    Telemetry.execute([:shard, :status_changed], %{}, %{instance: instance, shard: shard, role: role, from: from, to: to})
  end

  defp map_ok(enum, fun) do
    enum
    |> Enum.reduce_while({:ok, []}, fn item, {:ok, acc} ->
      case fun.(item) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end
end
