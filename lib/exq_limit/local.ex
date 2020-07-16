defmodule ExqLimit.Local do
  @moduledoc """
  It limits the total number of concurrent jobs on a single node. same
  as the Exq default rate limiter.

  In isolation, this limiter is not that much useful, but it can be
  combined with other rate limiters to get intersting results. For
  example, it can be combined with `ExqLimit.Global` rate limiter to
  make sure a single node doesn't get overloaded when total the number
  of live worker nodes go down due to unexpected reason. Check the
  documentation of `ExqLimit.And` on how to combine rate limiters

  ### Options
  - `limit` (integer) - max concurrency allowed. Required field
  """
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
