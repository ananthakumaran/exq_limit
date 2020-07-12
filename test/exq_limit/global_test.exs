defmodule ExqLimit.GlobalTest do
  use ExUnit.Case, async: false
  use ExUnitProperties
  alias ExqLimit.Global

  setup do
    start_supervised!({Redix, name: TestRedis})
    {:ok, "OK"} = Redix.command(TestRedis, ["FLUSHALL"])
    :ok
  end

  test "life cycle" do
    queue_info = %{queue: "hard"}
    options = [limit: 10, interval: 50, missed_heartbeats_allowed: 5, redis: TestRedis]
    {:ok, n1} = Global.init(queue_info, options ++ [node_id: "n1"])
    assert n1.current == 10
    assert n1.allowed == 10

    {:ok, n2} = Global.init(queue_info, options ++ [node_id: "n2"])
    assert n2.current == 0
    assert n2.allowed == 5

    Process.sleep(55)
    assert {:ok, true, n1} = Global.available?(n1)
    assert n1.mode == :drain
    assert n1.current == 10
    assert n1.allowed == 5

    Process.sleep(55)
    assert {:ok, true, n1} = Global.available?(n1)
    assert n1.mode == :heartbeat
    assert n1.current == 5
    assert n1.allowed == 5

    assert {:ok, true, n2} = Global.available?(n2)
    assert n2.mode == :heartbeat
    assert n2.current == 5
    assert n2.allowed == 5

    n1 =
      Enum.reduce(1..5, n1, fn _, n1 ->
        assert {:ok, true, n1} = Global.available?(n1)
        assert {:ok, n1} = Global.dispatched(n1)
        n1
      end)

    assert {:ok, false, n1} = Global.available?(n1)

    assert :ok = Global.stop(n2)

    Process.sleep(55)
    assert {:ok, true, n1} = Global.available?(n1)
    assert n1.mode == :heartbeat
    assert n1.current == 10
    assert n1.allowed == 10
    assert n1.running == 5

    n1 =
      Enum.reduce(1..5, n1, fn _, n1 ->
        assert {:ok, n1} = Global.processed(n1)
        n1
      end)

    assert {:ok, true, n1} = Global.available?(n1)
    assert n1.current == 10
    assert n1.allowed == 10
    assert n1.running == 0

    {:ok, n2} = Global.init(queue_info, options ++ [node_id: "n2"])
    assert n2.current == 0
    assert n2.allowed == 5

    Process.sleep(100)
    assert {:ok, false, n2} = Global.available?(n2)

    Process.sleep(100)
    assert {:ok, false, n2} = Global.available?(n2)

    Process.sleep(150)
    # n1 times out
    assert {:ok, true, n2} = Global.available?(n2)
    assert n2.current == 10
    assert n2.allowed == 10

    assert {:ok, false, n1} = Global.available?(n1)
    assert n1.current == 0
    assert n1.allowed == 5

    Process.sleep(60)
    assert {:ok, true, n2} = Global.available?(n2)
    assert n2.current == 10
    assert n2.allowed == 5

    Process.sleep(50)
    assert {:ok, true, n2} = Global.available?(n2)
    assert n2.current == 5
    assert n2.allowed == 5

    assert {:ok, true, n1} = Global.available?(n1)
    assert n1.current == 5
    assert n1.allowed == 5
  end

  @tag timeout: 120_000, integration: true
  property "preserves invariant" do
    check all limit <- unshrinkable(integer(0..50)),
              times <- unshrinkable(integer(100..1000)),
              nodes <- unshrinkable(integer(1..50)),
              max_run_time: 60_000 do
      all_nodes = Enum.map(1..nodes, &"node_#{&1}")

      # For simplicity, make sure none of the node misses heartbeat
      # if any node misses heartbeat, most of the assertions made below
      # won't hold

      invariant = fn state ->
        assert length(state.available_nodes) <= state.limit

        Enum.each(state.nodes, fn {_node_id, s} ->
          assert s.current >= 0
          assert s.allowed >= 0
          assert s.running <= s.current
        end)

        total_running =
          Map.values(state.running)
          |> Enum.sum()

        assert total_running <= limit

        total_current =
          Enum.map(state.nodes, fn {_, s} -> s.current end)
          |> Enum.sum()

        assert total_current <= limit
      end

      stable_invariant = fn state ->
        assert length(state.available_nodes) <= state.limit

        Enum.each(state.nodes, fn {_node_id, s} ->
          assert s.current >= 0
          assert s.allowed >= 0
          assert s.running <= s.current
        end)

        total_running =
          Map.values(state.running)
          |> Enum.sum()

        assert total_running <= limit

        total_current =
          Enum.map(state.nodes, fn {_, s} -> s.current end)
          |> Enum.sum()

        assert total_current == limit

        total_allowed =
          Enum.map(state.nodes, fn {_, s} -> s.allowed end)
          |> Enum.sum()

        assert total_allowed == limit
      end

      NodeSimulator.run(
        ExqLimit.Global,
        %{all_nodes: all_nodes, limit: limit, times: times},
        invariant,
        stable_invariant
      )
    end
  end
end
