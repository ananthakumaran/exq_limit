defmodule ExqLimit.GCRA do
  @moduledoc """
  This module implements the
  [GCRA](https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm)
  algorithm. Check the [blog post](https://brandur.org/rate-limiting)
  by brandur for an excellent introduction.

  ### Example

      {ExqLimit.GCRA, period: :hour, rate: 60}

  The above config can be used to run 60 jobs per hour. The algorithm
  only allows jobs at regular interval, so if 10 jobs get enqueued at
  once, each one will get processed at 1 minute interval.

      {ExqLimit.GCRA, period: :hour, rate: 60, burst: 5}

  `burst` option provides the ability to overshoot the limit for a
  short period of time. It can be compared to a battery. With the
  above config, when it's fully charged, it can process 5 jobs
  immediatly, but from 6th job onwards, it will process jobs at 1
  minute interval. The battery will get recharged 1 point every minute
  and get maxed at 5.



  ### Options
  - `period` (atom | integer) - Can be either `:second`, `:minute`, `:hour`, `:day`. In case of integer, it will be consider as seconds. Required field.
  - `burst` (integer) - Number of jobs allowed over the limit. Defaults to 0
  - `local` (boolean) - if set to true, the rate limiting will apply to the local worker node. Otherwise, the rate limiting will apply to all worker nodes. Defaults to `false`.
  - `node_id` (string) - Unique id of the worker node. Defaults to Exq node identifier. This will be used only if local option is set to true.


  ### Credits

  The lua script used in this module is a slightly modified version of [redis-gcra](https://github.com/rwz/redis-gcra)
  """
  require Logger
  alias ExqLimit.Redis.Script
  require ExqLimit.Redis.Script

  @behaviour Exq.Dequeue.Behaviour

  Script.compile(:lease)

  defmodule State do
    @moduledoc false

    defstruct redis: nil,
              running: 0,
              emission_interval: nil,
              burst: 0,
              queue: nil,
              available?: false,
              tat_key: nil,
              last_synced: nil,
              reset_after: -1,
              retry_after: 0
  end

  @version "limit_gcra_v1"

  @impl true
  def init(%{queue: queue}, options) do
    namespace =
      Keyword.get_lazy(options, :namespace, fn ->
        Exq.Support.Config.get(:namespace)
      end)

    period =
      case Keyword.fetch!(options, :period) do
        :second -> 1
        :minute -> 60
        :hour -> 60 * 60
        :day -> 60 * 60 * 24
        seconds when is_integer(seconds) and seconds > 0 -> seconds
      end

    burst =
      case Keyword.get(options, :burst) do
        nil -> 1
        burst when is_integer(burst) and burst >= 0 -> burst + 1
      end

    prefix =
      if Keyword.get(options, :local) do
        node_id =
          Keyword.get_lazy(options, :node_id, fn ->
            Exq.Support.Config.node_identifier().node_id()
          end)

        "#{namespace}:#{@version}:#{queue}:#{node_id}:"
      else
        "#{namespace}:#{@version}:#{queue}:"
      end

    state = %State{
      redis:
        Keyword.get_lazy(options, :redis, fn ->
          Exq.Support.Config.get(:name)
          |> Exq.Support.Opts.redis_client_name()
        end),
      queue: queue,
      emission_interval: period / Keyword.fetch!(options, :rate),
      burst: burst,
      tat_key: prefix <> "tat",
      last_synced: System.monotonic_time(:millisecond) / 1000
    }

    {:ok, state}
  end

  @impl true
  def stop(_state) do
    :ok
  end

  @impl true
  def available?(state) do
    state = sync(state)
    {:ok, state.available?, state}
  end

  @impl true
  def dispatched(state), do: {:ok, %{state | running: state.running + 1, available?: false}}

  @impl true
  def processed(state), do: {:ok, %{state | running: state.running - 1}}

  @impl true
  def failed(state), do: {:ok, %{state | running: state.running - 1}}

  defp sync(state) do
    diff = System.monotonic_time(:millisecond) / 1000 - state.last_synced

    state =
      if (state.available? && diff < state.reset_after) ||
           (!state.available? && diff < state.retry_after) do
        state
      else
        lease(state)
      end

    :telemetry.execute(
      [:exq_limit, :gcra],
      %{running: state.running},
      %{queue: state.queue}
    )

    state
  end

  defp lease(state) do
    case Script.eval!(
           state.redis,
           @lease,
           [
             state.tat_key
           ],
           [state.emission_interval, state.burst]
         ) do
      {:ok, [limited, retry_after, reset_after]} ->
        {retry_after, ""} = Float.parse(retry_after)
        {reset_after, ""} = Float.parse(reset_after)
        available? = limited != 1

        %{
          state
          | available?: available?,
            retry_after: retry_after,
            reset_after: reset_after,
            last_synced: System.monotonic_time(:millisecond) / 1000
        }

      error ->
        Logger.error(
          "Failed to run rebalance script. Unexpected error from redis: #{inspect(error)}"
        )

        %{
          state
          | available?: false,
            retry_after: 5,
            reset_after: -1,
            last_synced: System.monotonic_time(:millisecond) / 1000
        }
    end
  end
end
