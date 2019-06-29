defmodule MssqlEcto.Helper do
  def get_source(query, sources, ix, source) do
    {expr, name, _schema} = elem(sources, ix)
    {expr || expr.(source, sources, query), name}
  end

  defp quote_qualified_name(name, sources, ix) do
    {_, source, _} = elem(sources, ix)
    [source, ?. | quote_name(name)]
  end

  defp quote_name(name) when is_atom(name) do
    quote_name(Atom.to_string(name))
  end

  defp quote_name(name) do
    if String.contains?(name, "\"") do
      error!(nil, "bad field name #{inspect(name)}")
    end

    [?", name, ?"]
  end

  defp quote_table(nil, name), do: quote_table(name)
  defp quote_table(prefix, name), do: [quote_table(prefix), ?., quote_table(name)]

  defp quote_table(name) when is_atom(name),
    do: quote_table(Atom.to_string(name))

  defp quote_table(name) do
    if String.contains?(name, "\"") do
      error!(nil, "bad table name #{inspect(name)}")
    end

    [?", name, ?"]
  end

  defp single_quote(value), do: [?', escape_string(value), ?']

  defp intersperse_map(list, separator, mapper, acc \\ [])

  defp intersperse_map([], _separator, _mapper, acc),
    do: acc

  defp intersperse_map([elem], _separator, mapper, acc),
    do: [acc | mapper.(elem)]

  defp intersperse_map([elem | rest], separator, mapper, acc),
    do: intersperse_map(rest, separator, mapper, [acc, mapper.(elem), separator])

  defp intersperse_reduce(list, separator, user_acc, reducer, acc \\ [])

  defp intersperse_reduce([], _separator, user_acc, _reducer, acc),
    do: {acc, user_acc}

  defp intersperse_reduce([elem], _separator, user_acc, reducer, acc) do
    {elem, user_acc} = reducer.(elem, user_acc)
    {[acc | elem], user_acc}
  end

  defp intersperse_reduce([elem | rest], separator, user_acc, reducer, acc) do
    {elem, user_acc} = reducer.(elem, user_acc)
    intersperse_reduce(rest, separator, user_acc, reducer, [acc, elem, separator])
  end

  defp if_do(condition, value) do
    if condition, do: value, else: []
  end

  defp escape_string(value) when is_binary(value) do
    :binary.replace(value, "'", "''", [:global])
  end

  defp ecto_to_db({:array, t}), do: [ecto_to_db(t), ?[, ?]]
  defp ecto_to_db(:id), do: "integer"
  defp ecto_to_db(:serial), do: "serial"
  defp ecto_to_db(:bigserial), do: "bigserial"
  defp ecto_to_db(:binary_id), do: "uuid"
  defp ecto_to_db(:string), do: "varchar"
  defp ecto_to_db(:binary), do: "bytea"
  defp ecto_to_db(:map), do: Application.fetch_env!(:ecto_sql, :postgres_map_type)
  defp ecto_to_db({:map, _}), do: Application.fetch_env!(:ecto_sql, :postgres_map_type)
  defp ecto_to_db(:time_usec), do: "time"
  defp ecto_to_db(:utc_datetime), do: "timestamp"
  defp ecto_to_db(:utc_datetime_usec), do: "timestamp"
  defp ecto_to_db(:naive_datetime), do: "timestamp"
  defp ecto_to_db(:naive_datetime_usec), do: "timestamp"
  defp ecto_to_db(other), do: Atom.to_string(other)

  defp error!(nil, message) do
    raise ArgumentError, message
  end

  defp error!(query, message) do
    raise Ecto.QueryError, query: query, message: message
  end
end
