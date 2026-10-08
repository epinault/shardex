defmodule Shardex.StrategyTest do
  use ExUnit.Case, async: true

  alias Shardex.Strategy
  alias Shardex.Strategy.Function
  alias Shardex.Strategy.Hash
  alias Shardex.Strategy.JumpHash
  alias Shardex.Strategy.Static

  defp ctx(mod, opts, shards) do
    {:ok, state} = Strategy.init(mod, opts, shards)
    %{shards: shards, state: state, instance: :test}
  end

  describe "validate/1" do
    test "accepts modules, {module, opts} and 2-arity functions" do
      assert Strategy.validate(Hash) == {:ok, {Hash, []}}
      assert Strategy.validate({Static, map: %{}}) == {:ok, {Static, [map: %{}]}}
      fun = fn _key, _ctx -> {:ok, :s1} end
      assert Strategy.validate(fun) == {:ok, {Function, [fun: fun]}}
    end

    test "rejects modules without route_many/2 and other terms" do
      assert {:error, message} = Strategy.validate(String)
      assert message =~ "route_many/2"
      assert {:error, _} = Strategy.validate("nope")
      assert {:error, _} = Strategy.validate(fn _ -> :s1 end)
    end
  end

  describe "Hash" do
    test "is deterministic and only returns configured shards" do
      ctx = ctx(Hash, [], [:s1, :s2, :s3])

      for key <- 1..500 do
        assert {:ok, shard} = Strategy.route(Hash, key, ctx)
        assert shard in [:s1, :s2, :s3]
        assert Strategy.route(Hash, key, ctx) == {:ok, shard}
      end
    end

    test "distributes keys roughly evenly" do
      ctx = ctx(Hash, [], [:s1, :s2, :s3])
      counts = 1..3000 |> Enum.map(&Strategy.route(Hash, "key-#{&1}", ctx)) |> Enum.frequencies()
      for {_shard, count} <- counts, do: assert(count in 800..1200)
    end

    test "route_many matches route" do
      ctx = ctx(Hash, [], [:s1, :s2])
      keys = Enum.to_list(1..50)
      assert Strategy.route_many(Hash, keys, ctx) == Map.new(keys, &{&1, Strategy.route(Hash, &1, ctx)})
    end

    test "does not trace single routes" do
      refute Strategy.trace?(Hash)
    end
  end

  describe "JumpHash" do
    test "jump/2 returns a bucket in range" do
      for key <- 0..1000, buckets <- [1, 2, 7, 64] do
        assert JumpHash.jump(key, buckets) in 0..(buckets - 1)
      end
    end

    test "appending a shard only moves keys to the new shard, about 1/n of them" do
      before = ctx(JumpHash, [], [:s1, :s2, :s3, :s4])
      after_ = ctx(JumpHash, [], [:s1, :s2, :s3, :s4, :s5])
      keys = Enum.map(1..10_000, &"org-#{&1}")

      moved =
        Enum.filter(keys, fn key -> Strategy.route(JumpHash, key, before) != Strategy.route(JumpHash, key, after_) end)

      assert Enum.all?(moved, &(Strategy.route(JumpHash, &1, after_) == {:ok, :s5}))
      assert length(moved) / length(keys) > 0.15
      assert length(moved) / length(keys) < 0.25
    end
  end

  describe "Static" do
    test "routes mapped keys, falls back to default, otherwise :unassigned" do
      ctx = ctx(Static, [map: %{"a" => :s1}], [:s1, :s2])
      assert Strategy.route(Static, "a", ctx) == {:ok, :s1}
      assert Strategy.route(Static, "b", ctx) == {:error, :unassigned}

      ctx = ctx(Static, [map: %{"a" => :s1}, default: :s2], [:s1, :s2])
      assert Strategy.route(Static, "b", ctx) == {:ok, :s2}
    end
  end

  describe "Function" do
    test "calls the function per key with a ctx whose state is nil" do
      fun = fn key, ctx -> {:ok, {key, ctx.shards, ctx.state}} end
      ctx = ctx(Function, [fun: fun], [:s1])

      assert Strategy.route_many(Function, [1, 2], ctx) == %{
               1 => {:ok, {1, [:s1], nil}},
               2 => {:ok, {2, [:s1], nil}}
             }
    end

    test "route/3 falls back to route_many/2 when route/2 is not implemented" do
      ctx = ctx(Function, [fun: fn _key, _ctx -> {:ok, :s1} end], [:s1])
      assert Strategy.route(Function, "k", ctx) == {:ok, :s1}
      assert Strategy.trace?(Function)
    end
  end

  test "on_topology_change/4 re-runs init/2 when the strategy does not implement it" do
    {:ok, state} = Strategy.on_topology_change(Hash, {:s1}, [], [:s1, :s2])
    assert state == {:s1, :s2}
  end
end
