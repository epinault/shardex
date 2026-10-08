defmodule Shardex.RouterTest do
  use ExUnit.Case, async: true

  import Shardex.TestHelpers

  alias Shardex.Shard

  @routes %{"a" => :s1, "b" => :s2, "ghost" => :s9, "str" => "s1"}

  setup do
    {:ok, instance: unique_instance()}
  end

  defp start(i, opts \\ []) do
    defaults = [
      name: i,
      shards: [s1: [primary: agent_pool(i, :s1), replica: agent_pool(i, :s1, :replica)], s2: agent_pool(i, :s2)],
      strategy: {Shardex.Strategy.Static, map: @routes}
    ]

    start_supervised!({Shardex, Keyword.merge(defaults, opts)})
    i
  end

  defp whoami(ref), do: Agent.get(ref, & &1)

  test "lookup returns the shard for a key", %{instance: i} do
    start(i)
    assert {:ok, %Shard{name: :s1, status: :active}} = Shardex.lookup(i, "a")
  end

  test "ref returns the primary by default and other roles on request", %{instance: i} do
    start(i)
    assert {:ok, ref} = Shardex.ref(i, "a")
    assert whoami(ref) == {:s1, :primary}
    assert {:ok, ref} = Shardex.ref(i, "a", role: :replica)
    assert whoami(ref) == {:s1, :replica}
  end

  test "run calls the function with the pool ref and wraps the result", %{instance: i} do
    start(i)
    assert Shardex.run(i, "b", &whoami/1) == {:ok, {:s2, :primary}}
    assert Shardex.run!(i, "b", &whoami/1) == {:s2, :primary}
  end

  test "unassigned and unknown shards are errors, including string shard names", %{instance: i} do
    start(i)
    assert Shardex.lookup(i, "zzz") == {:error, :unassigned}
    assert Shardex.lookup(i, "ghost") == {:error, {:unknown_shard, :s9}}
    # Review focus: DB-backed strategies often return strings; never crash or create atoms.
    assert Shardex.lookup(i, "str") == {:error, {:unknown_shard, "s1"}}
  end

  test "missing roles are errors unless a fallback is usable", %{instance: i} do
    start(i)
    assert Shardex.ref(i, "b", role: :replica) == {:error, {:unknown_role, :replica}}
    assert {:ok, ref} = Shardex.ref(i, "b", role: :replica, fallback: :primary)
    assert whoami(ref) == {:s2, :primary}
    assert {:ok, _ref} = Shardex.ref(i, "b", role: :replica, fallback: [:other, :primary])
  end

  @tag :capture_log
  test "a stopped shard is in maintenance", %{instance: i} do
    start(i, start_failure: :stop_shard, shards: [s1: agent_pool(i, :s1), s2: failing_pool(Module.concat(i, F))])
    assert Shardex.lookup(i, "b") == {:error, :maintenance}
    assert {:ok, _shard} = Shardex.lookup(i, "a")
  end

  @tag :capture_log
  test "no routable shards short-circuits the strategy", %{instance: i} do
    start(i, start_failure: :stop_shard, shards: [s1: failing_pool(Module.concat(i, F))])
    assert Shardex.lookup(i, "a") == {:error, :no_routable_shards}
  end

  test "function strategies get ctx and invalid returns are strategy errors", %{instance: i} do
    start(i, strategy: fn key, ctx -> if key == "bad", do: :s1, else: {:ok, List.last(ctx.shards)} end)
    assert {:ok, %Shard{name: :s2}} = Shardex.lookup(i, "anything")
    assert Shardex.lookup(i, "bad") == {:error, {:strategy_error, :s1}}
  end

  test "bang functions raise Shardex.Error with a readable message", %{instance: i} do
    start(i)
    error = assert_raise Shardex.Error, fn -> Shardex.ref!(i, "zzz") end
    assert error.reason == :unassigned
    assert error.key == "zzz"
    assert Exception.message(error) =~ "no shard assigned"
    assert_raise Shardex.Error, fn -> Shardex.run!(i, "ghost", &whoami/1) end
  end

  test "exceptions raised by the function propagate", %{instance: i} do
    start(i)
    assert_raise RuntimeError, "boom", fn -> Shardex.run(i, "a", fn _ref -> raise "boom" end) end
  end

  test "run emits a run span with shard and role", %{instance: i} do
    attach_telemetry([[:shardex, :run, :stop], [:shardex, :run, :exception]])
    start(i)
    Shardex.run(i, "a", &whoami/1, role: :replica)
    assert_receive {:telemetry, [:shardex, :run, :stop], %{duration: _}, %{instance: ^i, shard: :s1, role: :replica}}

    catch_error(Shardex.run(i, "a", fn _ref -> raise "boom" end))
    assert_receive {:telemetry, [:shardex, :run, :exception], _m, %{instance: ^i, shard: :s1}}
  end

  test "single-key route spans are emitted only for traced strategies", %{instance: i} do
    attach_telemetry([[:shardex, :route, :stop]])
    start(i)
    Shardex.lookup(i, "a")
    refute_receive {:telemetry, [:shardex, :route, :stop], _m, %{instance: ^i}}

    other = unique_instance()
    start(other, strategy: fn _key, _ctx -> {:ok, :s1} end)
    Shardex.lookup(other, "a")
    assert_receive {:telemetry, [:shardex, :route, :stop], %{key_count: 1}, %{instance: ^other, batch?: false}}
  end

  test "routing on an instance that is not started raises ArgumentError" do
    assert_raise ArgumentError, ~r/is not started/, fn -> Shardex.lookup(:shardex_never_started, "a") end
  end
end
