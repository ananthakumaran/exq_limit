for app <- [:redix, :telemetry, :stream_data] do
  Application.ensure_all_started(app)
end

ExUnit.start(capture_log: true, exclude: [integration: true])
