defmodule ExqLimit.GlobalTest do
  alias ExqLimit.Global
  use ExUnit.Case
  use ExUnitProperties

  setup do
    {:ok, _redis} = Redix.start_link(name: TestRedis)
    :ok
  end

  @tag timeout: 120_000
  property "preserves invariant" do
    check all limit <- unshrinkable(integer(0..50)),
              times <- unshrinkable(integer(100..1000)),
              nodes <- unshrinkable(integer(1..50)),
              max_run_time: 60_000 do
      all_nodes = Enum.map(1..nodes, &"node_#{&1}")

      # For simplicity, make sure none of the node misses heartbeat
      # if any node misses heartbeat, most of the assertions made below
      # won't hold

      NodeSimulator.run(
        ExqLimit.Global,
        %{all_nodes: all_nodes, limit: limit, times: times},
        fn state ->
          assert length(state.available_nodes) <= state.limit

          Enum.each(state.nodes, fn {_node_id,
                                     %Global.State{
                                       current: current,
                                       allowed: allowed,
                                       running: running
                                     }} ->
            assert current >= 0
            assert allowed >= 0
            assert running <= current
          end)

          total_running =
            Map.values(state.running)
            |> Enum.sum()

          assert total_running <= limit, inspect(state, label: "total running")

          total_current =
            Enum.map(state.nodes, fn {_, s} -> s.current end)
            |> Enum.sum()

          assert total_current <= limit, inspect(state, label: "total current")
        end
      )
    end
  end
end
