defmodule ExqLimit.Local do
  @behaviour Exq.Dequeue.Behaviour

  defmodule State do
    @moduledoc false

    defstruct limit: nil, current: 0
  end

  @impl true
  def init(_, options) do
    {:ok, %State{limit: Keyword.fetch!(options, :limit)}}
  end

  @impl true
  def stop(_), do: :ok

  @impl true
  def available?(state), do: {:ok, state.current < state.limit, state}

  @impl true
  def dispatched(state), do: {:ok, %{state | current: state.current + 1}}

  @impl true
  def processed(state), do: {:ok, %{state | current: state.current - 1}}

  @impl true
  def failed(state), do: {:ok, %{state | current: state.current - 1}}
end
