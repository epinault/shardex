defmodule Shardex.BootTest do
  use ExUnit.Case, async: true

  import Shardex.TestHelpers

  alias Shardex.Names
  alias Shardex.State

  setup do
    {:ok, instance: unique_instance()}
  end

  defp running_pool_ids(i) do
    for {id, pid, _type, _mods} <- Supervisor.which_children(Names.pool_sup(i)), is_pid(pid), do: id
  end

  test "boots, starts one pool per {shard, role} and writes the topology", %{instance: i} do
    start_supervised!(
      {Shardex,
       name: i, shards: [s1: [primary: agent_pool(i, :s1), replica: agent_pool(i, :s1, :replica)], s2: agent_pool(i, :s2)]}
    )

    topology = State.topology!(i)
    assert topology.shards == [:s1, :s2]
    assert topology.active == [:s1, :s2]
    assert topology.strategy == Shardex.Strategy.Hash
    assert topology.version == 1
    assert Enum.sort(running_pool_ids(i)) == [{:s1, :primary}, {:s1, :replica}, {:s2, :primary}]

    {:ok, s1} = State.fetch_shard(i, :s1)
    assert s1.status == :active
    assert Agent.get(s1.roles.replica.ref, & &1) == {:s1, :replica}
  end

  test "start_pools: false registers shards without starting pools", %{instance: i} do
    start_supervised!({Shardex, name: i, start_pools: false, shards: [s1: agent_pool(i, :s1)]})
    assert running_pool_ids(i) == []
    assert {:ok, %{status: :active}} = State.fetch_shard(i, :s1)
  end

  @tag :capture_log
  test "a pool that fails to start fails boot by default", %{instance: i} do
    assert {:error, _reason} = start_supervised({Shardex, name: i, shards: [s1: failing_pool(Module.concat(i, F))]})
  end

  @tag :capture_log
  test "an adapter init error fails boot", %{instance: i} do
    assert {:error, _reason} = start_supervised({Shardex, name: i, shards: [s1: {Shardex.Adapter.Generic, []}]})
  end

  @tag :capture_log
  test "start_failure: :stop_shard boots the failing shard as :stopped", %{instance: i} do
    attach_telemetry([[:shardex, :shard, :status_changed]])

    start_supervised!(
      {Shardex,
       name: i, start_failure: :stop_shard, shards: [s1: agent_pool(i, :s1), s2: failing_pool(Module.concat(i, F))]}
    )

    assert {:ok, %{status: :stopped}} = State.fetch_shard(i, :s2)
    assert State.topology!(i).active == [:s1]
    assert_receive {:telemetry, _event, _m, %{instance: ^i, shard: :s2, role: nil, from: :active, to: :stopped}}
  end

  test "invalid config raises a validation error", %{instance: i} do
    assert_raise NimbleOptions.ValidationError, fn -> Shardex.start_link(name: i, shards: []) end
  end

  test "reading an instance that is not started raises ArgumentError" do
    assert_raise ArgumentError, ~r/is not started/, fn -> State.topology!(:shardex_never_started) end
  end

  test "a coordinator crash rebuilds state without restarting running pools", %{instance: i} do
    start_supervised!({Shardex, name: i, shards: [s1: agent_pool(i, :s1)]})
    pool_pid = Process.whereis(agent_name(i, :s1))
    coordinator = Process.whereis(Names.coordinator(i))
    monitor = Process.monitor(coordinator)

    Process.exit(coordinator, :kill)
    assert_receive {:DOWN, ^monitor, :process, _pid, :killed}

    eventually(fn ->
      pid = Process.whereis(Names.coordinator(i))
      pid != nil and pid != coordinator
    end)

    # Blocks until the restarted Coordinator has finished init/1.
    :sys.get_state(Names.coordinator(i))
    assert State.topology!(i).shards == [:s1]
    assert Process.whereis(agent_name(i, :s1)) == pool_pid
  end
end
