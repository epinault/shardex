defmodule Shardex.TopologyTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import Shardex.TestHelpers

  alias Shardex.Adapter.Generic
  alias Shardex.Names
  alias Shardex.State

  setup do
    {:ok, instance: unique_instance()}
  end

  defp start(i, opts \\ []) do
    defaults = [
      name: i,
      shards: [s1: agent_pool(i, :s1), s2: agent_pool(i, :s2)],
      strategy: {Shardex.Strategy.Static, map: %{"a" => :s1, "b" => :s2, "c" => :s3}}
    ]

    start_supervised!({Shardex, Keyword.merge(defaults, opts)})
    i
  end

  defp shard_name(i, key) do
    {:ok, shard} = Shardex.lookup(i, key)
    shard.name
  end

  test "add_shard starts pools and makes the shard routable", %{instance: i} do
    attach_telemetry([[:shardex, :topology, :changed]])
    start(i)
    assert Shardex.lookup(i, "c") == {:error, {:unknown_shard, :s3}}

    assert :ok = Shardex.add_shard(i, :s3, agent_pool(i, :s3))
    assert Shardex.run(i, "c", &Agent.get(&1, fn s -> s end)) == {:ok, {:s3, :primary}}
    assert State.topology!(i).shards == [:s1, :s2, :s3]

    assert_receive {:telemetry, [:shardex, :topology, :changed], _m,
                    %{instance: ^i, action: :add, shard: :s3, version: v}}

    assert v > 1
  end

  test "add_shard with status: :drain starts pools without routing", %{instance: i} do
    start(i)
    assert :ok = Shardex.add_shard(i, :s3, agent_pool(i, :s3), status: :drain)
    assert Shardex.lookup(i, "c") == {:error, :maintenance}
    assert is_pid(Process.whereis(agent_name(i, :s3)))
    assert_raise ArgumentError, fn -> Shardex.add_shard(i, :s4, agent_pool(i, :s4), status: :stopped) end
  end

  @tag :capture_log
  test "add_shard errors leave nothing behind", %{instance: i} do
    start(i)
    assert Shardex.add_shard(i, :s1, agent_pool(i, :s1)) == {:error, :already_exists}
    assert {:error, {:invalid_shard_spec, _message}} = Shardex.add_shard(i, :s3, :nope)

    assert Shardex.add_shard(i, :s3, {Generic, []}) ==
             {:error, {:adapter_init_failed, :s3, :primary, {:missing_option, :ref}}}

    # :primary starts, then :replica fails: the primary must be stopped again.
    assert {:error, _reason} =
             Shardex.add_shard(i, :s3, primary: agent_pool(i, :s3), replica: failing_pool(Module.concat(i, F)))

    assert Shardex.status(i, :s3) == {:error, {:unknown_shard, :s3}}
    refute Process.whereis(agent_name(i, :s3))
    assert State.topology!(i).shards == [:s1, :s2]
  end

  test "remove_shard stops pools and unroutes the shard", %{instance: i} do
    start(i)
    assert :ok = Shardex.remove_shard(i, :s2)
    assert Shardex.lookup(i, "b") == {:error, {:unknown_shard, :s2}}
    refute Process.whereis(agent_name(i, :s2))
    assert State.topology!(i).shards == [:s1]
    assert Shardex.remove_shard(i, :s2) == {:error, {:unknown_shard, :s2}}
    assert Shardex.remove_shard(i, :s1) == {:error, :last_shard}
  end

  test "hash strategies log that keys remap", %{instance: i} do
    start_supervised!({Shardex, name: i, shards: [s1: agent_pool(i, :s1)]})
    log = capture_log(fn -> assert :ok = Shardex.add_shard(i, :s2, agent_pool(i, :s2)) end)
    assert log =~ "remap"
  end

  test "with jump hash, adding a shard only moves keys to that shard", %{instance: i} do
    shards = for s <- [:s1, :s2, :s3, :s4], do: {s, agent_pool(i, s)}
    start_supervised!({Shardex, name: i, shards: shards, strategy: Shardex.Strategy.JumpHash})
    keys = Enum.map(1..2000, &"org-#{&1}")
    before = Map.new(keys, &{&1, shard_name(i, &1)})

    capture_log(fn -> assert :ok = Shardex.add_shard(i, :s5, agent_pool(i, :s5)) end)

    moved = Enum.filter(keys, &(shard_name(i, &1) != before[&1]))
    assert moved != []
    assert Enum.all?(moved, &(shard_name(i, &1) == :s5))
  end

  test "a coordinator restart drops runtime-added shards and stops their pools", %{instance: i} do
    start(i)
    assert :ok = Shardex.add_shard(i, :s3, agent_pool(i, :s3))
    coordinator = Process.whereis(Names.coordinator(i))
    Process.exit(coordinator, :kill)

    eventually(fn ->
      pid = Process.whereis(Names.coordinator(i))
      pid != nil and pid != coordinator
    end)

    :sys.get_state(Names.coordinator(i))
    assert Shardex.status(i, :s3) == {:error, {:unknown_shard, :s3}}
    refute Process.whereis(agent_name(i, :s3))
  end
end
