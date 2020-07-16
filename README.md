# ExqLimit

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
