defmodule Shardex.ConfigTest do
  use ExUnit.Case, async: true

  alias Shardex.Adapter.Generic
  alias Shardex.Config
  alias Shardex.Strategy.Static

  @pool {Generic, ref: :r}

  test "normalizes a bare pool spec to a :primary role and applies defaults" do
    config = Config.validate!(name: :inst, shards: [s1: @pool])
    assert config.name == :inst
    assert config.shards == [s1: [primary: @pool]]
    assert config.strategy == {Shardex.Strategy.Hash, []}
    assert config.start_pools == true
    assert config.start_failure == :raise
  end

  test "keeps explicit roles in order" do
    config = Config.validate!(name: :inst, shards: [s1: [primary: @pool, replica: @pool]])
    assert config.shards == [s1: [primary: @pool, replica: @pool]]
  end

  test "normalizes strategies" do
    fun = fn _k, _c -> {:ok, :s1} end

    assert Config.validate!(name: :i, shards: [s1: @pool], strategy: fun).strategy ==
             {Shardex.Strategy.Function, [fun: fun]}

    assert Config.validate!(name: :i, shards: [s1: @pool], strategy: {Static, map: %{}}).strategy ==
             {Static, [map: %{}]}
  end

  test "rejects invalid configurations" do
    for opts <- [
          [shards: [s1: @pool]],
          [name: :i, shards: []],
          [name: :i, shards: :nope],
          [name: :i, shards: [s1: :not_a_spec]],
          [name: :i, shards: [s1: [primary: :not_a_spec]]],
          [name: :i, shards: [s1: @pool, s1: @pool]],
          [name: :i, shards: [s1: @pool], strategy: String],
          [name: :i, shards: [s1: @pool], start_failure: :explode],
          [name: :i, shards: [s1: @pool], unknown: true]
        ] do
      assert_raise NimbleOptions.ValidationError, fn -> Config.validate!(opts) end
    end
  end

  test "normalize_roles/2 is usable on its own" do
    assert Config.normalize_roles(:s1, @pool) == {:ok, [primary: @pool]}
    assert {:error, message} = Config.normalize_roles(:s1, [])
    assert message =~ ":s1"
  end
end
