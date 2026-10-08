defmodule Shardex.MaintenanceTest do
  use ExUnit.Case, async: true

  import Shardex.TestHelpers

  alias Shardex.Adapter.Generic

  setup do
    {:ok, instance: unique_instance()}
  end

  defp start(i, opts \\ []) do
    defaults = [
      name: i,
      shards: [s1: [primary: agent_pool(i, :s1), replica: agent_pool(i, :s1, :replica)], s2: agent_pool(i, :s2)],
      strategy: {Shardex.Strategy.Static, map: %{"a" => :s1, "b" => :s2}}
    ]

    start_supervised!({Shardex, Keyword.merge(defaults, opts)})
    i
  end

  defp alive?(name), do: is_pid(Process.whereis(name))

  test "drain stops routing but keeps pools running", %{instance: i} do
    start(i)
    assert :ok = Shardex.maintenance(i, :s1, :drain)
    assert Shardex.lookup(i, "a") == {:error, :maintenance}
    assert alive?(agent_name(i, :s1))
    assert Shardex.status(i, :s1) == {:ok, %{status: :drain, roles: %{primary: :active, replica: :active}}}

    assert :ok = Shardex.activate(i, :s1)
    assert {:ok, _shard} = Shardex.lookup(i, "a")
  end

  test "stop terminates pools and activate restarts them", %{instance: i} do
    start(i)
    assert :ok = Shardex.maintenance(i, :s1, :stop)
    refute alive?(agent_name(i, :s1))
    refute alive?(agent_name(i, :s1, :replica))
    assert {:ok, %{status: :stopped}} = Shardex.status(i, :s1)

    assert :ok = Shardex.activate(i, :s1)
    assert Shardex.run(i, "a", &Agent.get(&1, fn s -> s end)) == {:ok, {:s1, :primary}}
  end

  test "transitions are idempotent and emit one event per change", %{instance: i} do
    attach_telemetry([[:shardex, :shard, :status_changed]])
    start(i)

    assert :ok = Shardex.maintenance(i, :s1, :drain)
    assert :ok = Shardex.maintenance(i, :s1, :drain)
    assert_receive {:telemetry, _e, _m, %{instance: ^i, shard: :s1, role: nil, from: :active, to: :drain}}
    refute_receive {:telemetry, _e, _m, %{instance: ^i, shard: :s1, to: :drain}}

    assert :ok = Shardex.activate(i, :s1)
    assert :ok = Shardex.activate(i, :s1)
    assert_receive {:telemetry, _e, _m, %{instance: ^i, shard: :s1, role: nil, from: :drain, to: :active}}
    refute_receive {:telemetry, _e, _m, %{instance: ^i, shard: :s1, to: :active}}
  end

  test "role-scoped drain only affects that role", %{instance: i} do
    start(i)
    assert :ok = Shardex.maintenance(i, :s1, :drain, role: :replica)
    assert {:ok, _shard} = Shardex.lookup(i, "a")
    assert Shardex.ref(i, "a", role: :replica) == {:error, :maintenance}
    assert {:ok, ref} = Shardex.ref(i, "a", role: :replica, fallback: :primary)
    assert Agent.get(ref, & &1) == {:s1, :primary}
    assert Shardex.status(i, :s1) == {:ok, %{status: :active, roles: %{primary: :active, replica: :drain}}}
  end

  test "role-scoped stop terminates only that pool", %{instance: i} do
    start(i)
    assert :ok = Shardex.maintenance(i, :s1, :stop, role: :replica)
    refute alive?(agent_name(i, :s1, :replica))
    assert alive?(agent_name(i, :s1))

    assert :ok = Shardex.activate(i, :s1, role: :replica)
    assert alive?(agent_name(i, :s1, :replica))
  end

  test "activate without a role resets every role", %{instance: i} do
    start(i)
    assert :ok = Shardex.maintenance(i, :s1, :drain, role: :replica)
    assert :ok = Shardex.activate(i, :s1)
    assert Shardex.status(i, :s1) == {:ok, %{status: :active, roles: %{primary: :active, replica: :active}}}
  end

  test "unknown shards and roles are errors", %{instance: i} do
    start(i)
    assert Shardex.maintenance(i, :nope, :drain) == {:error, {:unknown_shard, :nope}}
    assert Shardex.activate(i, :s1, role: :nope) == {:error, {:unknown_role, :nope}}
    assert Shardex.status(i, :nope) == {:error, {:unknown_shard, :nope}}
  end

  test "shards/1 lists shards in order with live pool pids", %{instance: i} do
    start(i)
    assert [%{name: :s1} = s1, %{name: :s2}] = Shardex.shards(i)
    assert is_pid(s1.meta.pids.primary)
    assert is_pid(s1.meta.pids.replica)

    assert :ok = Shardex.maintenance(i, :s2, :stop)
    assert [_s1, s2] = Shardex.shards(i)
    assert s2.meta.pids == %{primary: nil}
  end

  test "mutations on an instance that is not started raise ArgumentError" do
    assert_raise ArgumentError, ~r/is not started/, fn -> Shardex.maintenance(:shardex_never_started, :s1, :drain) end
  end

  # Review focus: the shard must never become routable before its pool exists.
  test "activating a slow pool never exposes the shard before the pool is ready", %{instance: i} do
    slow = Module.concat(i, Slow)
    test_pid = self()

    slow_pool =
      {Generic,
       child_spec: %{
         id: slow,
         start: {Agent, :start_link, [fn -> Process.sleep(200) && send(test_pid, :pool_ready) && :slow end, [name: slow]]}
       },
       ref: slow}

    start(i, shards: [s1: slow_pool])
    assert :ok = Shardex.maintenance(i, :s1, :stop)

    spawn_link(fn -> poll_until_routable(i, test_pid) end)
    assert :ok = Shardex.activate(i, :s1)

    first =
      receive do
        :pool_ready -> :pool_ready
        {:routable, _} -> :routable
      after
        2000 -> :timeout
      end

    assert first == :pool_ready
    assert_receive {:routable, _}, 2000
  end

  @tag :capture_log
  test "a failed activation rolls back pools it already started", %{instance: i} do
    start(i,
      start_failure: :stop_shard,
      shards: [s1: [primary: agent_pool(i, :s1), replica: failing_pool(Module.concat(i, F))]]
    )

    assert {:ok, %{status: :stopped}} = Shardex.status(i, :s1)
    refute alive?(agent_name(i, :s1))

    assert {:error, _reason} = Shardex.activate(i, :s1)
    assert {:ok, %{status: :stopped}} = Shardex.status(i, :s1)
    refute alive?(agent_name(i, :s1))
  end

  defp poll_until_routable(i, parent) do
    case Shardex.lookup(i, "a") do
      {:ok, _shard} -> send(parent, {:routable, true})
      {:error, _reason} -> poll_until_routable(i, parent)
    end
  end
end
