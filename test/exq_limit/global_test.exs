defmodule ExqLimit.GlobalTest do
  alias ExqLimit.Global
  use ExUnit.Case

  test "fuzz" do
    NodeSimulator.run(ExqLimit.Global, fn state ->
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

      assert total_running <= state.limit, inspect(state)
    end)
  end
end
