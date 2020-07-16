defmodule ExqLimit.And do
  @moduledoc """
  This module provides the ability to combiner multiple rate limiters
  together.

      {ExqLimit.And,
        [
          {ExqLimit.Local, limit: 20},
          {ExqLimit.Global, limit: 100},
          {ExqLimit.GCRA, [period: :second, rate: 60, burst: 0], short_circuit: true}
        ]
      }

  The above example creates a rate limiter which dequeues new jobs
  only if all the rate limiter returns true. This can be used to
  create interesting combinations and also supports custom rate
  limiters as long as it implements the `Exq.Dequeue.Behaviour`
  behaviour

  ### Options

  - short_circuit (boolean) - whether to short circuit the `c:Exq.Dequeue.Behaviour.available?/1` call when any one of the previous rate limiters returned `false`. Defaults to `false`.

  Some of the modules in ExqLimit expect specific value to be set for `short_circuit` option, otherwise the behaviour is undefined when used with `ExqLimit.And`

  | module          | short_circuit   |
  |-----------------|-----------------|
  |`ExqLimit.Local` | `true or false` |
  |`ExqLimit.Global`| `false`         |
  |`ExqLimit.GCRA`  | `true`          |

  """

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
