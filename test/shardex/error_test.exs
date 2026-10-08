defmodule Shardex.ErrorTest do
  use ExUnit.Case, async: true

  test "Shardex.Error has a readable message for each reason" do
    for {reason, fragment} <- [
          {:maintenance, "maintenance"},
          {:unassigned, "no shard assigned"},
          {:no_routable_shards, "no shard is currently active"},
          {{:unknown_shard, "s1"}, ~s("s1")},
          {{:unknown_role, :replica}, ":replica"},
          {{:strategy_error, :oops}, ":oops"},
          {:custom, ":custom"}
        ] do
      message = Exception.message(%Shardex.Error{reason: reason, key: "org_1", instance: MyShards})
      assert message =~ "MyShards"
      assert message =~ ~s("org_1")
      assert message =~ fragment
    end
  end

  test "Shardex.BatchError summarizes failed items" do
    error = %Shardex.BatchError{errors: [{%{id: 1}, :unassigned}, {%{id: 2}, :maintenance}], instance: MyShards}
    message = Exception.message(error)
    assert message =~ "2 item(s)"
    assert message =~ "%{id: 1}"
  end
end
