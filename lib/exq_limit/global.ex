defmodule ExqLimit.Global do
  require Logger
  alias ExqLimit.Redis.Script
  require ExqLimit.Redis.Script

  @behaviour Exq.Dequeue.Behaviour

  Script.compile(:rebalance)
  Script.compile(:drain)
  Script.compile(:fill)
  Script.compile(:heartbeat)

  defmodule State do
    defstruct limit: nil,
              running: 0,
              allowed: 0,
              current: 0,
              redis: nil,
              node_id: nil,
              version: nil,
              queue: nil,
              interval: nil,
              cutoff_threshold: nil,
              last_synced: nil,
              mode: :rebalance
  end

  @impl true
  def init(%{queue: queue}, options) do
    interval = Keyword.get(options, :interval, 20_000)
    missed_heartbeats_allowed = Keyword.get(options, :missed_heartbeats_allowed, 5)

    state = %State{
      interval: interval,
      cutoff_threshold: interval / 1000 * (missed_heartbeats_allowed + 1),
      limit: Keyword.fetch!(options, :limit),
      redis:
        Keyword.get_lazy(options, :redis, fn ->
          Exq.Support.Config.get(:name)
          |> Exq.Support.Opts.redis_client_name()
        end),
      node_id:
        Keyword.get_lazy(options, :node_id, fn ->
          Exq.Support.Config.node_identifier().node_id()
        end),
      queue: queue
    }

    state = sync(state)

    {:ok, state}
  end

  @impl true
  def stop(_), do: :ok

  @impl true
  def available?(state) do
    state = sync(state)
    {:ok, state.running < Enum.min([state.allowed, state.current]), state}
  end

  @impl true
  def dispatched(state), do: {:ok, %{state | running: state.running + 1}}

  @impl true
  def processed(state), do: {:ok, %{state | running: state.running - 1}}

  @impl true
  def failed(state), do: {:ok, %{state | running: state.running - 1}}

  defp sync(state) do
    now = System.system_time(:millisecond)

    if state.last_synced && now - state.last_synced < state.interval do
      state
    else
      prefix = "exq:exq_limit:#{state.queue}:"
      time = now / 1000
      state = %{state | last_synced: now}

      case state.mode do
        :rebalance ->
          rebalance(state, prefix, time)

        :drain ->
          drain(state, prefix, time)

        :fill ->
          fill(state, prefix, time)

        :heartbeat ->
          heartbeat(state, prefix, time)
      end
    end
  end

  defp heartbeat(state, prefix, time) do
    case Script.eval!(
           state.redis,
           @heartbeat,
           [
             prefix <> "version",
             prefix <> "heartbeat"
           ],
           [state.node_id, state.version, time, time - state.cutoff_threshold]
         ) do
      {:ok, 0} ->
        rebalance(state, prefix, time)

      {:ok, 1} ->
        state

      error ->
        Logger.error(
          "Failed to run hearbeat script. Unexpected error from redis: #{inspect(error)}"
        )

        state
    end
  end

  defp fill(state, prefix, time) do
    case Script.eval!(
           state.redis,
           @fill,
           [
             prefix <> "version",
             prefix <> "allocation",
             prefix <> "heartbeat"
           ],
           [state.node_id, state.version, time, time - state.cutoff_threshold]
         ) do
      {:ok, 0} ->
        rebalance(state, prefix, time)

      {:ok, [allowed, current]} ->
        %{state | allowed: allowed, current: current, mode: next_mode(allowed, current)}

      error ->
        Logger.error("Failed to run fill script. Unexpected error from redis: #{inspect(error)}")
        state
    end
  end

  defp drain(state, prefix, time) do
    amount = Enum.min([state.current - state.allowed, state.current - state.running])

    if amount == 0 do
      heartbeat(state, prefix, time)
    else
      case Script.eval!(
             state.redis,
             @drain,
             [
               prefix <> "version",
               prefix <> "allocation",
               prefix <> "heartbeat"
             ],
             [state.node_id, state.version, time, time - state.cutoff_threshold, amount]
           ) do
        {:ok, 0} ->
          rebalance(state, prefix, time)

        {:ok, 1} ->
          current = state.current - amount
          %{state | current: current, mode: next_mode(state.allowed, current)}

        error ->
          Logger.error(
            "Failed to run drain script. Unexpected error from redis: #{inspect(error)}"
          )

          state
      end
    end
  end

  defp rebalance(state, prefix, time) do
    case Script.eval!(
           state.redis,
           @rebalance,
           [
             prefix <> "version",
             prefix <> "limit",
             prefix <> "allocation",
             prefix <> "heartbeat"
           ],
           [state.node_id, state.limit, time, time - state.cutoff_threshold]
         ) do
      {:ok, [version, allowed, current]} ->
        %{
          state
          | version: version,
            allowed: allowed,
            current: current,
            mode: next_mode(allowed, current)
        }

      error ->
        Logger.error(
          "Failed to run rebalance script. Unexpected error from redis: #{inspect(error)}"
        )

        state
    end
  end

  defp next_mode(allowed, current) do
    cond do
      current > allowed -> :drain
      current < allowed -> :fill
      current == allowed -> :heartbeat
    end
  end
end
