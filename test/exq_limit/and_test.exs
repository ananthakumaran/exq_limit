defmodule ExqLimit.AndTest do
  use ExUnit.Case
  use ExUnitProperties

  setup do
    start_supervised!({Redix, name: TestRedis})
    {:ok, "OK"} = Redix.command(TestRedis, ["FLUSHALL"])
    :ok
  end

  test "simulate" do
    limit = 6
    times = 1000
    nodes = 3
    local_limit = 3
    all_nodes = Enum.map(1..nodes, &"node_#{&1}")

    invariant = fn state ->
      assert length(state.available_nodes) <= limit

      Enum.each(state.running, fn {_node_id, limit} ->
        assert limit <= local_limit
      end)

      total_running =
        Map.values(state.running)
        |> Enum.sum()

      assert total_running <= limit
    end

    NodeSimulator.Fuzzy.run(
      ExqLimit.And,
      fn common ->
        [
          {ExqLimit.Local, limit: local_limit},
          {ExqLimit.Global,
           Keyword.merge(common,
             redis: TestRedis,
             limit: limit,
             interval: 50,
             missed_heartbeats_allowed: 20
           )}
        ]
      end,
      %{all_nodes: all_nodes, max_nodes: limit, times: times},
      invariant,
      invariant
    )
  end
end
