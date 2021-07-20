# ExqLimit

[![CI](https://github.com/ananthakumaran/exq_limit/actions/workflows/ci.yml/badge.svg)](<https://github.com/ananthakumaran/exq_limit/actions/workflows/ci.yml/badge.svg>)
[![Hex.pm](https://img.shields.io/hexpm/v/exq_limit.svg)](<https://hex.pm/packages/exq_limit>)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](<https://hexdocs.pm/exq_limit/ExqLimit.Global.html>)
[![License](https://img.shields.io/hexpm/l/exq_limit.svg)](<https://github.com/ananthakumaran/exq_limit/blob/master/LICENSE>)

ExqLimit implements different types of rate limiting for
[Exq](https://github.com/akira/exq) queue.

## Example

```elixir
config :exq,
  queues: [{"default", {ExqLimit.Global, limit: 10}}]
```

## Types

[ExqLimit.Global](https://hexdocs.pm/exq_limit/ExqLimit.Global.html) - Global concurrency limit across all worker nodes.

[ExqLimit.Local](https://hexdocs.pm/exq_limit/ExqLimit.Local.html) - Local concurrency limit for a worker node.

[ExqLimit.GCRA](https://hexdocs.pm/exq_limit/ExqLimit.GCRA.html) - An implementation of GCRA algorithm.

[ExqLimit.And](https://hexdocs.pm/exq_limit/ExqLimit.And.html) - A limiter which allows to combine other limiters.
