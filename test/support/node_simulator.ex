defmodule NodeSimulator.Helper do
  defmacro weight(spec) do
    {spec, _} = Code.eval_quoted(spec)

    list =
      Enum.map(spec, fn {command, times} ->
        Stream.cycle([command])
        |> Enum.take(times)
      end)
      |> Enum.concat()

    quote do
      unquote(list)
    end
  end

  @max_nodes 20
  @nodes 1..@max_nodes |> Enum.map(&"node_#{&1}")

  def unknown_node(%{nodes: nodes}), do: Enum.random(@nodes -- Map.keys(nodes))
  def known_node(%{nodes: nodes}), do: Enum.random(Map.keys(nodes))
  def available_node(%{available_nodes: nodes}), do: Enum.random(Map.keys(nodes))
  def running_node(%{running: nodes}), do: Enum.random(Map.keys(nodes))
end

defmodule NodeSimulator do
  require Logger
  require NodeSimulator.Helper
  import NodeSimulator.Helper

  defmodule State do
    defstruct nodes: %{}, limit: 10, available_nodes: [], running: %{}
  end

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
        [
          node_id: node_id,
          redis: TestRedis,
          limit: state.limit,
          interval: 50,
          missed_heartbeats_allowed: 20
        ]
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
    Process.sleep(60)
    state
  end

  def run(module, invariant) do
    {:ok, _redis} = Redix.start_link(name: TestRedis)

    Enum.reduce(1..1000, %State{}, fn _, state ->
      command = Enum.random(next_command(state))
      state = apply(__MODULE__, command, [state, module])
      invariant.(state)
      state
    end)
  end

  def next_command(%State{nodes: nodes}) when map_size(nodes) == 0 do
    weight(%{start_node: 1})
  end

  def next_command(%State{nodes: nodes}) when map_size(nodes) == 10 do
    weight(%{
      stop_node: 3,
      get_available: 10,
      sleep: 5
    })
  end

  def next_command(%State{running: running}) when map_size(running) > 0 do
    weight(%{
      start_node: 3,
      start_job: 5,
      finish_job: 5,
      fail_job: 5,
      stop_node: 2,
      get_available: 5,
      sleep: 5
    })
  end

  def next_command(_) do
    weight(%{
      start_node: 3,
      start_job: 5,
      stop_node: 2,
      get_available: 5,
      sleep: 5
    })
  end
end
