defmodule ExqLimit.All do
  @behaviour Exq.Dequeue.Behaviour

  @impl true
  def init(queue_info, limits) when is_list(limits) and length(limits) > 0 do
    state =
      Enum.map(limits, fn {module, options} ->
        {:ok, limit_state} = apply(module, :init, [queue_info, options])
        {module, limit_state}
      end)

    {:ok, state}
  end

  @impl true
  def stop(state) do
    Enum.each(state, fn {module, limit_state} ->
      :ok = apply(module, :stop, [limit_state])
    end)

    :ok
  end

  @impl true
  def available?(state) do
    {state, available?} =
      Enum.reduce(state, {[], true}, fn {module, limit_state}, {state, acc} ->
        {:ok, available?, limit_state} = apply(module, :available?, [limit_state])
        {[{module, limit_state} | state], available? && acc}
      end)

    {:ok, available?, Enum.reverse(state)}
  end

  @impl true
  def dispatched(state) do
    state =
      Enum.map(state, fn {module, limit_state} ->
        {:ok, limit_state} = apply(module, :dispatched, [limit_state])
        {module, limit_state}
      end)

    {:ok, state}
  end

  @impl true
  def processed(state) do
    state =
      Enum.map(state, fn {module, limit_state} ->
        {:ok, limit_state} = apply(module, :processed, [limit_state])
        {module, limit_state}
      end)

    {:ok, state}
  end

  @impl true
  def failed(state) do
    state =
      Enum.map(state, fn {module, limit_state} ->
        {:ok, limit_state} = apply(module, :failed, [limit_state])
        {module, limit_state}
      end)

    {:ok, state}
  end
end
