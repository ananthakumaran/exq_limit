defmodule ExqLimit.MixProject do
  use Mix.Project

  def project do
    [
      app: :exq_limit,
      version: "0.1.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {ExqLimit.Application, []}
    ]
  end

  defp deps do
    [
      {:exq, path: "../exq"},
      {:redix, ">= 0.9.0"}
    ]
  end
end
