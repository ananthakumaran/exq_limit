defmodule ExqLimit.Global do
  @moduledoc """
  Exq comes with concurrency control support per queue, but it's
  limited to a single worker node. This module on the other hand
  limits the concurrency globally across all the worker nodes. For
  example, with a limit of 10, if there are two active worker nodes,
  each will be allowed to work on 5 concurrent jobs. The limits per
  worker node will get auto adjusted when new worker nodes get added
  or existing ones get removed.

  ### Options

  - `limit` (integer) - Global max concurrency across all the worker nodes. Required field
  - `node_id` (string) - Unique id of the worker node. Defaults to Exq node identifier.
  - `interval` (integer - milliseconds) -  The availability of each node is determined by the last hearbeat. The interval controls how often the node registers the hearbeat. Defaults to 20_000.
  - `missed_heartbeats_allowed` (integer) - Number of hearbeats a node is allowed to miss. After which, the node will be considered dead and its capacity will be redistributed to remaining worker nodes. Defaults to 5.

  ### NOTES

  This implementations tries to never run more than the configured
  limit. But the limit could be crossed if a node could not
  communicate with redis server, but able to continue with current
  running jobs. In those cases, after certain time, the node will be
  considered dead and its capacity will be shared across remaining
  nodes.
  """

  require Logger
  alias ExqLimit.Redis.Script
  require ExqLimit.Redis.Script

  @behaviour Exq.Dequeue.Behaviour

  Script.compile(:rebalance)
  Script.compile(:drain)
  Script.compile(:fill)
  Script.compile(:heartbeat)
  Script.compile(:clear)

  defmodule State do
    @moduledoc false

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
              mode: :rebalance,
              version_key: nil,
              limit_key: nil,
              allocation_key: nil,
              heartbeat_key: nil
  end

  @version "limit_global_v1"

  @impl true
  def init(%{queue: queue}, options) do
    interval = Keyword.get(options, :interval, 20_000)
    missed_heartbeats_allowed = Keyword.get(options, :missed_heartbeats_allowed, 5)

    namespace =
      Keyword.get_lazy(options, :namespace, fn ->
        Exq.Support.Config.get(:namespace)
      end)

    prefix = "#{namespace}:#{@version}:#{queue}:"

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
      queue: queue,
      version_key: prefix <> "version",
      limit_key: prefix <> "limit",
      allocation_key: prefix <> "allocation",
      heartbeat_key: prefix <> "heartbeat"
    }

    state = sync(state)

    {:ok, state}
  end

  @impl true
  def stop(state) do
    sync(%{state | mode: :clear})
    :ok
  end

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

    if state.mode != :clear && state.last_synced && now - state.last_synced < state.interval do
      state
    else
      time = now / 1000
      state = %{state | last_synced: now}

      case state.mode do
        :clear ->
          clear(state, time)

        :rebalance ->
          rebalance(state, time)

        :drain ->
          drain(state, time)

        :fill ->
          fill(state, time)

        :heartbeat ->
          heartbeat(state, time)
      end
    end
  end

  defp heartbeat(state, time) do
    case Script.eval!(
           state.redis,
           @heartbeat,
           [
             state.version_key,
             state.heartbeat_key
           ],
           [state.node_id, state.version, time, time - state.cutoff_threshold]
         ) do
      {:ok, 0} ->
        rebalance(state, time)

      {:ok, 1} ->
        state

      error ->
        Logger.error(
          "Failed to run hearbeat script. Unexpected error from redis: #{inspect(error)}"
        )

        state
    end
  end

  defp fill(state, time) do
    case Script.eval!(
           state.redis,
           @fill,
           [
             state.version_key,
             state.allocation_key,
             state.heartbeat_key
           ],
           [state.node_id, state.version, time, time - state.cutoff_threshold]
         ) do
      {:ok, 0} ->
        rebalance(state, time)

      {:ok, [allowed, current]} ->
        %{state | allowed: allowed, current: current, mode: next_mode(allowed, current)}

      error ->
        Logger.error("Failed to run fill script. Unexpected error from redis: #{inspect(error)}")
        state
    end
  end

  defp drain(state, time) do
    amount = Enum.min([state.current - state.allowed, state.current - state.running])

    if amount == 0 do
      heartbeat(state, time)
    else
      case Script.eval!(
             state.redis,
             @drain,
             [
               state.version_key,
               state.allocation_key,
               state.heartbeat_key
             ],
             [state.node_id, state.version, time, time - state.cutoff_threshold, amount]
           ) do
        {:ok, 0} ->
          rebalance(state, time)

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

  defp rebalance(state, time) do
    case Script.eval!(
           state.redis,
           @rebalance,
           [
             state.version_key,
             state.limit_key,
             state.allocation_key,
             state.heartbeat_key
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

  defp clear(state, _time) do
    case Script.eval!(
           state.redis,
           @clear,
           [
             state.version_key,
             state.limit_key,
             state.allocation_key,
             state.heartbeat_key
           ],
           [state.node_id]
         ) do
      {:ok, 1} ->
        state

      error ->
        Logger.error("Failed to run clear script. Unexpected error from redis: #{inspect(error)}")

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
