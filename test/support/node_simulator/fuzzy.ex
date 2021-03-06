defmodule NodeSimulator.Fuzzy do
  require Logger

  defmodule State do
    defstruct all_nodes: [],
              nodes: %{},
              max_nodes: 0,
              available_nodes: [],
              running: %{},
              limit_options: nil
  end

  defp unknown_node(%{nodes: nodes, all_nodes: all_nodes}),
    do: Enum.random(all_nodes -- Map.keys(nodes))

  defp known_node(%{nodes: nodes}), do: Enum.random(Map.keys(nodes))
  defp running_node(%{running: nodes}), do: Enum.random(Map.keys(nodes))

  defp trace_apply(module, function, args) do
    result = apply(module, function, args)
    Logger.debug("#{module}:#{function}(#{inspect(args)}) --> #{inspect(result)}")
    result
  end

  def start_node(state, module) do
    node_id = unknown_node(state)

    {:ok, ns} =
      trace_apply(module, :init, [
        %{queue: "hard"},
        state.limit_options.(node_id: node_id)
      ])

    %{state | nodes: Map.put(state.nodes, node_id, ns)}
  end

  def stop_node(state, module) do
    node_id = known_node(state)
    :ok = trace_apply(module, :stop, [Map.fetch!(state.nodes, node_id)])

    %{
      state
      | nodes: Map.delete(state.nodes, node_id),
        running: Map.delete(state.running, node_id),
        available_nodes: List.delete(state.available_nodes, node_id)
    }
  end

  def get_available(state, module) do
    Enum.reduce(state.nodes, %{state | available_nodes: []}, fn {node_id, ns}, state ->
      {:ok, available, ns} = trace_apply(module, :available?, [ns])

      available_nodes =
        if available do
          [node_id | state.available_nodes]
        else
          state.available_nodes
        end

      nodes = Map.put(state.nodes, node_id, ns)
      %{state | nodes: nodes, available_nodes: available_nodes}
    end)
  end

  defp try_stablize_all_nodes(state, module) do
    Process.sleep(50)

    Enum.reduce(state.nodes, %{state | available_nodes: []}, fn {node_id, ns}, state ->
      {:ok, _, ns} = trace_apply(module, :available?, [ns])
      nodes = Map.put(state.nodes, node_id, ns)
      %{state | nodes: nodes}
    end)
  end

  def stabilize(state, module) do
    try_stablize_all_nodes(state, module)
    |> try_stablize_all_nodes(module)
    |> try_stablize_all_nodes(module)
  end

  def start_job(state, module) do
    state =
      Enum.reduce(state.nodes, %{state | available_nodes: []}, fn {node_id, ns}, state ->
        {:ok, available, ns} = trace_apply(module, :available?, [ns])

        available_nodes =
          if available do
            [node_id | state.available_nodes]
          else
            state.available_nodes
          end

        nodes = Map.put(state.nodes, node_id, ns)
        %{state | nodes: nodes, available_nodes: available_nodes}
      end)

    if length(state.available_nodes) > 0 do
      node_id = Enum.random(state.available_nodes)
      ns = Map.fetch!(state.nodes, node_id)
      {:ok, ns} = trace_apply(module, :dispatched, [ns])

      %{
        state
        | nodes: Map.put(state.nodes, node_id, ns),
          running: Map.update(state.running, node_id, 1, &(&1 + 1))
      }
    else
      state
    end
  end

  defp stop_job(state, module, reason) do
    node_id = running_node(state)
    current = Map.fetch!(state.running, node_id)
    current = current - 1

    state =
      if current > 1 do
        %{state | running: Map.update!(state.running, node_id, &(&1 - 1))}
      else
        %{state | running: Map.delete(state.running, node_id)}
      end

    ns = Map.fetch!(state.nodes, node_id)
    {:ok, ns} = trace_apply(module, reason, [ns])
    %{state | nodes: Map.put(state.nodes, node_id, ns)}
  end

  def finish_job(state, module) do
    stop_job(state, module, :processed)
  end

  def fail_job(state, module) do
    stop_job(state, module, :failed)
  end

  def sleep(state, _module) do
    Process.sleep(Enum.random(1..50))
    state
  end

  defp select_random_command(weights) do
    case Process.get(weights) do
      nil ->
        list =
          Enum.map(weights, fn {command, times} ->
            Stream.cycle([command])
            |> Enum.take(times)
          end)
          |> Enum.concat()

        Process.put(weights, list)
        list

      list ->
        list
    end
    |> Enum.random()
  end

  def run(
        module,
        limit_options,
        %{max_nodes: max_nodes, times: times, all_nodes: all_nodes},
        invariant,
        stable_invariant
      ) do
    {:ok, "OK"} = Redix.command(TestRedis, ["FLUSHALL"])

    Enum.reduce(
      1..times,
      %State{all_nodes: all_nodes, max_nodes: max_nodes, limit_options: limit_options},
      fn _, state ->
        command = select_random_command(next_command(state))
        state = apply(__MODULE__, command, [state, module])

        if command == :stabilize do
          stable_invariant.(state)
        else
          invariant.(state)
        end

        state
      end
    )
  end

  def next_command(%State{nodes: nodes}) when map_size(nodes) == 0 do
    %{start_node: 1}
  end

  def next_command(%State{
        nodes: nodes,
        max_nodes: max_nodes,
        all_nodes: all_nodes,
        running: running
      }) do
    start_node =
      if length(all_nodes) == map_size(nodes) || max_nodes == map_size(nodes), do: 0, else: 3

    stop_node = if map_size(nodes) > 0, do: 2, else: 0
    finish_job = if map_size(running) > 0, do: 3, else: 0
    fail_job = if map_size(running) > 0, do: 3, else: 0

    %{
      start_node: start_node,
      stop_node: stop_node,
      start_job: 10,
      finish_job: finish_job,
      fail_job: fail_job,
      get_available: 5,
      sleep: 5,
      stabilize: 1
    }
  end
end
