defmodule ExqLimit.MixProject do
  use Mix.Project

  def project do
    [
      app: :exq_limit,
      version: "0.1.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger],
      mod: {ExqLimit.Application, []}
    ]
  end

  defp deps do
    [
      {:exq, path: "../exq"},
      {:redix, ">= 0.9.0"},
      {:stream_data, "~> 0.5", only: [:test, :dev]}
    ]
  end
end
