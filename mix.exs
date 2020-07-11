defmodule ExqLimit.MixProject do
  use Mix.Project

  def project do
    [
      app: :exq_limit,
      version: "0.1.0",
      elixir: "~> 1.8",
      description: "Exq Rate Limiter",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      name: "ExqLimit",
      source_url: "https://github.com/ananthakumaran/exq_limit",
      docs: [
        main: "readme",
        extras: ["README.md"]
      ],
      package: package()
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

  defp package do
    %{
      licenses: ["MIT"],
      links: %{"Github" => "https://github.com/ananthakumaran/exq_limit"},
      maintainers: ["ananthakumaran@gmail.com"]
    }
  end

  defp deps do
    [
      {:exq, github: "akira/exq", branch: "dequeue_controller"},
      {:ex_doc, "~> 0.22", only: :dev, runtime: false},
      {:redix, ">= 0.9.0"},
      {:stream_data, "~> 0.5", only: [:test, :dev]}
    ]
  end
end
