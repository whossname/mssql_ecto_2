defmodule MssqlEcto.Connection.Expression do
  alias MssqlEcto.Connection
  alias Connection.{Constraints, Expression}
  import MssqlEcto.Connection.Helper

  def expr({:^, [], [ix]}, _sources, _query) do
    [?$ | Integer.to_string(ix + 1)]
  end

  def expr({{:., _, [{:&, _, [idx]}, field]}, _, []}, sources, _query) when is_atom(field) do
    quote_qualified_name(field, sources, idx)
  end

  def expr({:&, _, [idx]}, sources, _query) do
    {_, source, _} = elem(sources, idx)
    source
  end

  def expr({:in, _, [_left, []]}, _sources, _query) do
    "false"
  end

  def expr({:in, _, [left, right]}, sources, query) when is_list(right) do
    args = intersperse_map(right, ?,, &expr(&1, sources, query))
    [expr(left, sources, query), " IN (", args, ?)]
  end

  def expr({:in, _, [left, {:^, _, [ix, _]}]}, sources, query) do
    [expr(left, sources, query), " = ANY($", Integer.to_string(ix + 1), ?)]
  end

  def expr({:in, _, [left, right]}, sources, query) do
    [expr(left, sources, query), " = ANY(", expr(right, sources, query), ?)]
  end

  def expr({:is_nil, _, [arg]}, sources, query) do
    [expr(arg, sources, query) | " IS NULL"]
  end

  def expr({:not, _, [expr]}, sources, query) do
    ["NOT (", expr(expr, sources, query), ?)]
  end

  def expr(%Ecto.SubQuery{query: query}, _sources, _query) do
    [?(, Connection.all(query), ?)]
  end

  def expr({:fragment, _, [kw]}, _sources, query) when is_list(kw) or tuple_size(kw) == 3 do
    error!(query, "PostgreSQL adapter does not support keyword or interpolated fragments")
  end

  def expr({:fragment, _, parts}, sources, query) do
    Enum.map(parts, fn
      {:raw, part} -> part
      {:expr, expr} -> expr(expr, sources, query)
    end)
    |> Connection.parens_for_select()
  end

  def expr({:datetime_add, _, [datetime, count, interval]}, sources, query) do
    [
      expr(datetime, sources, query),
      "::timestamp + ",
      Connection.interval(count, interval, sources, query)
    ]
  end

  def expr({:date_add, _, [date, count, interval]}, sources, query) do
    [
      ?(,
      expr(date, sources, query),
      "::date + ",
      Connection.interval(count, interval, sources, query) | ")::date"
    ]
  end

  def expr({:filter, _, [agg, filter]}, sources, query) do
    aggregate = expr(agg, sources, query)
    [aggregate, " FILTER (WHERE ", expr(filter, sources, query), ?)]
  end

  def expr({:over, _, [agg, name]}, sources, query) when is_atom(name) do
    aggregate = expr(agg, sources, query)
    [aggregate, " OVER " | quote_name(name)]
  end

  def expr({:over, _, [agg, kw]}, sources, query) do
    aggregate = expr(agg, sources, query)
    [aggregate, " OVER ", Connection.window_exprs(kw, sources, query)]
  end

  def expr({:{}, _, elems}, sources, query) do
    [?(, intersperse_map(elems, ?,, &expr(&1, sources, query)), ?)]
  end

  def expr({:count, _, []}, _sources, _query), do: "count(*)"

  def expr({fun, _, args}, sources, query) when is_atom(fun) and is_list(args) do
    {modifier, args} =
      case args do
        [rest, :distinct] -> {"DISTINCT ", [rest]}
        _ -> {[], args}
      end

    case Connection.handle_call(fun, length(args)) do
      {:binary_op, op} ->
        [left, right] = args
        [op_to_binary(left, sources, query), op | op_to_binary(right, sources, query)]

      {:fun, fun} ->
        [fun, ?(, modifier, intersperse_map(args, ", ", &expr(&1, sources, query)), ?)]
    end
  end

  def expr(list, sources, query) when is_list(list) do
    ["ARRAY[", intersperse_map(list, ?,, &expr(&1, sources, query)), ?]]
  end

  def expr(%Decimal{} = decimal, _sources, _query) do
    Decimal.to_string(decimal, :normal)
  end

  def expr(%Ecto.Query.Tagged{value: binary, type: :binary}, _sources, _query)
      when is_binary(binary) do
    ["'\\x", Base.encode16(binary, case: :lower) | "'::bytea"]
  end

  def expr(%Ecto.Query.Tagged{value: other, type: type}, sources, query) do
    [expr(other, sources, query), ?:, ?: | tagged_to_db(type)]
  end

  def expr(nil, _sources, _query), do: "NULL"
  def expr(true, _sources, _query), do: "TRUE"
  def expr(false, _sources, _query), do: "FALSE"

  def expr(literal, _sources, _query) when is_binary(literal) do
    [?\', escape_string(literal), ?\']
  end

  def expr(literal, _sources, _query) when is_integer(literal) do
    Integer.to_string(literal)
  end

  def expr(literal, _sources, _query) when is_float(literal) do
    [Float.to_string(literal) | "::float"]
  end
end
