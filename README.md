# ExqLimit

ExqLimit plans to implement different types of rate limiting for
[Exq](https://github.com/akira/exq) queue.

## Example

```elixir
config :exq,
  queues: [{"default", {ExqLimit.Global, limit: 10}}]
```

## Types

[ExqLimit.Global](https://hexdocs.pm/exq_limit/ExqLimit.Global.html) - Global concurrency limit across all worker nodes.
