defmodule ExqLimit.All do
  @behaviour Exq.Dequeue.Behaviour

  @impl true
  def init(queue_info, specs) when is_list(specs) and length(specs) > 0 do
    state =
      Enum.map(specs, fn spec ->
        {module, limit_options, options} = normalize_spec(spec)
        short_circuit? = Keyword.get(options, :short_circuit, false)
        {:ok, limit_state} = apply(module, :init, [queue_info, limit_options])
        {module, limit_state, short_circuit?}
      end)

    {:ok, state}
  end

  @impl true
  def stop(state) do
    Enum.each(state, fn {module, limit_state, _} ->
      :ok = apply(module, :stop, [limit_state])
    end)

    :ok
  end

  @impl true
  def available?(state) do
    {state, available?} =
      Enum.reduce(state, {[], true}, fn {module, limit_state, short_circuit?}, {state, acc} ->
        if short_circuit? && !acc do
          {[{module, limit_state, short_circuit?} | state], acc}
        else
          {:ok, available?, limit_state} = apply(module, :available?, [limit_state])
          {[{module, limit_state, short_circuit?} | state], available? && acc}
        end
      end)

    {:ok, available?, Enum.reverse(state)}
  end

  @impl true
  def dispatched(state) do
    state =
      Enum.map(state, fn {module, limit_state, short_circuit?} ->
        {:ok, limit_state} = apply(module, :dispatched, [limit_state])
        {module, limit_state, short_circuit?}
      end)

    {:ok, state}
  end

  @impl true
  def processed(state) do
    state =
      Enum.map(state, fn {module, limit_state, short_circuit?} ->
        {:ok, limit_state} = apply(module, :processed, [limit_state])
        {module, limit_state, short_circuit?}
      end)

    {:ok, state}
  end

  @impl true
  def failed(state) do
    state =
      Enum.map(state, fn {module, limit_state, short_circuit?} ->
        {:ok, limit_state} = apply(module, :failed, [limit_state])
        {module, limit_state, short_circuit?}
      end)

    {:ok, state}
  end

  defp normalize_spec({module, limit_options}), do: {module, limit_options, []}
  defp normalize_spec({module, limit_options, options}), do: {module, limit_options, options}
end
