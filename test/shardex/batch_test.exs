defmodule Shardex.BatchTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Shardex.TestHelpers

  alias Shardex.Batch
  alias Shardex.Test.RecordingStrategy

  @routes %{"a" => :s1, "b" => :s2, "ghost" => :s9}
  @items [%{k: "a", n: 1}, %{k: "b", n: 2}, %{k: "a", n: 3}, %{k: "zzz", n: 4}, %{k: "ghost", n: 5}]

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

  test "group returns items by shard in input order and lists every failure", %{instance: i} do
    start(i)
    assert %Batch{groups: groups, errors: errors} = Shardex.group(i, @items, & &1.k)

    assert groups.s1.items == [%{k: "a", n: 1}, %{k: "a", n: 3}]
    assert groups.s1.shard.name == :s1
    assert groups.s1.pool.role == :primary
    assert groups.s2.items == [%{k: "b", n: 2}]
    assert errors == [{%{k: "zzz", n: 4}, :unassigned}, {%{k: "ghost", n: 5}, {:unknown_shard, :s9}}]
  end

  test "group calls the strategy once with deduplicated keys", %{instance: i} do
    start(i, strategy: {RecordingStrategy, pid: self()})
    Shardex.group(i, [%{k: 1}, %{k: 1}, %{k: 2}], & &1.k)
    assert_received {:route_many, [1, 2]}
    refute_received {:route_many, _keys}
  end

  # Review focus: empty input.
  test "empty items return an empty batch without calling the strategy", %{instance: i} do
    start(i, strategy: {RecordingStrategy, pid: self()})
    assert Shardex.group(i, [], & &1) == %Batch{}
    assert Shardex.run_batch(i, [], & &1, fn _ref, _items -> :never end) == {:ok, %{}, []}
    refute_received {:route_many, _keys}
  end

  # Review focus: mixed key types.
  test "mixed key types dedupe and route", %{instance: i} do
    start(i, strategy: {RecordingStrategy, pid: self()})
    keys = [1, "1", :one, {1}, %{id: 1}, 1]
    batch = Shardex.group(i, keys, & &1)
    assert batch.errors == []
    assert batch.groups |> Map.values() |> Enum.flat_map(& &1.items) |> Enum.sort() == Enum.sort(keys)
    assert_received {:route_many, [1, "1", :one, {1}, %{id: 1}]}
  end

  test "role resolution applies per shard, with fallback", %{instance: i} do
    start(i)
    batch = Shardex.group(i, [%{k: "a"}, %{k: "b"}], & &1.k, role: :replica)
    assert Map.keys(batch.groups) == [:s1]
    assert batch.groups.s1.pool.role == :replica
    assert batch.errors == [{%{k: "b"}, {:unknown_role, :replica}}]

    batch = Shardex.group(i, [%{k: "a"}, %{k: "b"}], & &1.k, role: :replica, fallback: :primary)
    assert batch.errors == []
    assert batch.groups.s2.pool.role == :primary
  end

  test "run_batch calls fun once per shard with the group's ref and items", %{instance: i} do
    start(i)

    assert {:ok, results, errors} =
             Shardex.run_batch(i, @items, & &1.k, fn ref, items -> {Agent.get(ref, & &1), Enum.map(items, & &1.n)} end)

    assert results == %{s1: {{:s1, :primary}, [1, 3]}, s2: {{:s2, :primary}, [2]}}
    assert Enum.map(errors, fn {item, _reason} -> item.n end) == [4, 5]
  end

  test "on_error: :raise raises before running any group", %{instance: i} do
    start(i)
    test_pid = self()

    assert_raise Shardex.BatchError, ~r/2 item\(s\)/, fn ->
      Shardex.run_batch(i, @items, & &1.k, fn _ref, _items -> send(test_pid, :ran) end, on_error: :raise)
    end

    refute_received :ran
  end

  test "run_batch validates its options before routing", %{instance: i} do
    start(i, strategy: {RecordingStrategy, pid: self()})
    attach_telemetry([[:shardex, :batch, :stop]])

    for {opts, message} <- [
          {[on_error: :ignore], ~r/:on_error/},
          {[max_concurrency: 0], ~r/:max_concurrency/},
          {[max_concurrency: :many], ~r/:max_concurrency/},
          {[timeout: 0], ~r/:timeout/},
          {[timeout: "5s"], ~r/:timeout/}
        ] do
      assert_raise ArgumentError, message, fn ->
        Shardex.run_batch(i, @items, & &1.k, fn _ref, items -> items end, opts)
      end
    end

    refute_received {:route_many, _keys}
    refute_received {:telemetry, [:shardex, :batch, :stop], _measurements, _meta}
  end

  test "max_concurrency runs groups in separate processes", %{instance: i} do
    start(i)
    test_pid = self()

    assert {:ok, results, []} =
             Shardex.run_batch(i, [%{k: "a"}, %{k: "b"}], & &1.k, fn _ref, _items -> self() end, max_concurrency: 2)

    assert map_size(results) == 2
    assert Enum.all?(Map.values(results), &(&1 != test_pid))
  end

  test "emits batch and route telemetry", %{instance: i} do
    attach_telemetry([[:shardex, :batch, :stop], [:shardex, :route, :stop]])
    start(i)
    Shardex.group(i, @items, & &1.k)
    assert_receive {:telemetry, [:shardex, :batch, :stop], %{items: 5, groups: 2, errors: 2}, %{instance: ^i}}
    assert_receive {:telemetry, [:shardex, :route, :stop], %{key_count: 4}, %{instance: ^i, batch?: true}}
  end

  property "every item appears exactly once, in input order within its group", %{instance: i} do
    start(i)

    check all(items <- list_of(tuple({member_of(["a", "b", "zzz", "ghost"]), integer()}))) do
      batch = Shardex.group(i, items, &elem(&1, 0))
      grouped = batch.groups |> Map.values() |> Enum.flat_map(& &1.items)
      errored = Enum.map(batch.errors, &elem(&1, 0))

      assert Enum.sort(grouped ++ errored) == Enum.sort(items)

      for {_name, group} <- batch.groups do
        assert group.items == Enum.filter(items, &(&1 in group.items))
      end
    end
  end
end
