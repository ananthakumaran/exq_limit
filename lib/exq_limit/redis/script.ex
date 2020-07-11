defmodule ExqLimit.Redis.Script do
  @moduledoc false

  require Logger

  defmacro compile(name) do
    path =
      Path.expand(
        "#{name}.lua",
        Path.join(Path.dirname(__CALLER__.file), Path.basename(__CALLER__.file, ".ex"))
      )

    source = File.read!(path)

    hash =
      :crypto.hash(:sha, source)
      |> Base.encode16(case: :lower)

    quote do
      Module.put_attribute(__MODULE__, :external_resource, unquote(path))
      Module.put_attribute(__MODULE__, unquote(name), unquote(Macro.escape({name, hash, source})))
    end
  end

  def eval!(redis, {name, hash, source}, keys, args) do
    Logger.debug(fn -> "eval script " <> inspect({name, keys, args}) end)

    case Redix.command(redis, ["EVALSHA", hash, length(keys)] ++ keys ++ args) do
      {:error, %Redix.Error{message: "NOSCRIPT" <> _}} ->
        Redix.command(redis, ["EVAL", source, length(keys)] ++ keys ++ args)

      result ->
        result
    end
  end
end
