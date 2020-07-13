defmodule ExqLimit.GCRATest do
  use ExUnit.Case, async: false
  alias ExqLimit.GCRA

  setup do
    start_supervised!({Redix, name: TestRedis})
    {:ok, "OK"} = Redix.command(TestRedis, ["FLUSHALL"])
    :ok
  end

  test "rate" do
    jobs = run_for(5000, 3, rate: 10, period: 1, redis: TestRedis)
    assert jobs >= 50 && jobs <= 53

    jobs = run_for(5000, 5, rate: 10, period: 1, burst: 10, redis: TestRedis)
    assert jobs >= 60 && jobs <= 75

    jobs = run_for(3000, 1, rate: 10, period: 1, burst: 20, redis: TestRedis)
    assert jobs >= 50 && jobs <= 51

    jobs = run_for(3000, 4, rate: 5, period: 1, redis: TestRedis, local: true)
    assert jobs >= 60 && jobs <= 64
  end

  defp run_for(milliseconds, node_count, options) do
    {:ok, "OK"} = Redix.command(TestRedis, ["FLUSHALL"])
    queue_info = %{queue: "hard"}

    nodes =
      Enum.map(1..node_count, fn id ->
        {:ok, state} = GCRA.init(queue_info, options ++ [node_id: to_string(id)])
        state
      end)

    parent = self()

    pid =
      spawn_link(fn ->
        {nodes, jobs_count} = get_rate(nodes, 0)
        send(parent, {:count, jobs_count, nodes})
      end)

    Process.sleep(milliseconds)
    send(pid, :stop)

    receive do
      {:count, jobs_count, nodes} ->
        Enum.map(nodes, fn state -> :ok = GCRA.stop(state) end)
        jobs_count
    end
  end

  defp get_rate(nodes, acc) do
    {nodes, acc} =
      Enum.reduce(nodes, {[], acc}, fn state, {nodes, acc} ->
        {:ok, available, state} = GCRA.available?(state)

        if available do
          {:ok, state} = GCRA.dispatched(state)
          {:ok, state} = GCRA.processed(state)
          {[state | nodes], acc + 1}
        else
          {[state | nodes], acc}
        end
      end)

    receive do
      :stop -> {nodes, acc}
    after
      0 -> get_rate(nodes, acc)
    end
  end
end
