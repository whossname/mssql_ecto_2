defmodule MssqlEcto.Connection.Query do
  alias Ecto.Query.{BooleanExpr, JoinExpr, QueryExpr}
  import MssqlEcto.Connection.Helper

  def all(query) do
    sources = create_names(query)
    {select_distinct, order_by_distinct} = distinct(query.distinct, sources, query)

    from = from(query, sources)
    select = select(query, select_distinct, sources)
    join = join(query, sources)
    where = where(query, sources)
    group_by = group_by(query, sources)
    having = having(query, sources)
    window = window(query, sources)
    combinations = combinations(query)
    order_by = order_by(query, order_by_distinct, sources)
    limit = limit(query, sources)
    offset = offset(query, sources)
    lock = lock(query.lock)

    [
      select,
      from,
      join,
      where,
      group_by,
      having,
      window,
      combinations,
      order_by,
      limit,
      offset | lock
    ]
  end

  defp insert_as({%{sources: sources}, _, _}) do
    {_expr, name, _schema} = create_name(sources, 0)
    [" AS " | name]
  end

  defp insert_as({_, _, _}) do
    []
  end

  defp on_conflict({:raise, _, []}, _header),
    do: []

  defp on_conflict({:nothing, _, targets}, _header),
    do: [" ON CONFLICT ", conflict_target(targets) | "DO NOTHING"]

  defp on_conflict({fields, _, targets}, _header) when is_list(fields),
    do: [" ON CONFLICT ", conflict_target(targets), "DO " | replace(fields)]

  defp on_conflict({query, _, targets}, _header),
    do: [" ON CONFLICT ", conflict_target(targets), "DO " | update_all(query, "UPDATE SET ")]

  defp conflict_target({:constraint, constraint}),
    do: ["ON CONSTRAINT ", quote_name(constraint), ?\s]

  defp conflict_target({:unsafe_fragment, fragment}),
    do: [fragment, ?\s]

  defp conflict_target([]),
    do: []

  defp conflict_target(targets),
    do: [?(, intersperse_map(targets, ?,, &quote_name/1), ?), ?\s]

  defp replace(fields) do
    [
      "UPDATE SET "
      | intersperse_map(fields, ?,, fn field ->
          quoted = quote_name(field)
          [quoted, " = ", "EXCLUDED." | quoted]
        end)
    ]
  end

  defp insert_all(rows, counter) do
    intersperse_reduce(rows, ?,, counter, fn row, counter ->
      {row, counter} = insert_each(row, counter)
      {[?(, row, ?)], counter}
    end)
    |> elem(0)
  end

  defp insert_each(values, counter) do
    intersperse_reduce(values, ?,, counter, fn
      nil, counter ->
        {"DEFAULT", counter}

      {%Ecto.Query{} = query, params_counter}, counter ->
        {[?(, all(query), ?)], counter + params_counter}

      _, counter ->
        {[?$ | Integer.to_string(counter)], counter + 1}
    end)
  end

  ## Query generation

  binary_ops = [
    ==: " = ",
    !=: " != ",
    <=: " <= ",
    >=: " >= ",
    <: " < ",
    >: " > ",
    +: " + ",
    -: " - ",
    *: " * ",
    /: " / ",
    and: " AND ",
    or: " OR ",
    ilike: " ILIKE ",
    like: " LIKE "
  ]

  @binary_ops Keyword.keys(binary_ops)

  Enum.map(binary_ops, fn {op, str} ->
    defp handle_call(unquote(op), 2), do: {:binary_op, unquote(str)}
  end)

  defp handle_call(fun, _arity), do: {:fun, Atom.to_string(fun)}

  defp select(%{select: %{fields: fields}} = query, select_distinct, sources) do
    ["SELECT", select_distinct, ?\s | select_fields(fields, sources, query)]
  end

  defp select_fields([], _sources, _query),
    do: "TRUE"

  defp select_fields(fields, sources, query) do
    intersperse_map(fields, ", ", fn
      {key, value} ->
        [Expression.expr(value, sources, query), " AS " | quote_name(key)]

      value ->
        Expression.expr(value, sources, query)
    end)
  end

  defp distinct(nil, _, _), do: {[], []}
  defp distinct(%QueryExpr{expr: []}, _, _), do: {[], []}
  defp distinct(%QueryExpr{expr: true}, _, _), do: {" DISTINCT", []}
  defp distinct(%QueryExpr{expr: false}, _, _), do: {[], []}

  defp distinct(%QueryExpr{expr: exprs}, sources, query) do
    {[
       " DISTINCT ON (",
       intersperse_map(exprs, ", ", fn {_, expr} -> expr(expr, sources, query) end),
       ?)
     ], exprs}
  end

  defp from(%{from: %{hints: [_ | _]}} = query, _sources) do
    error!(query, "table hints are not supported by PostgreSQL")
  end

  defp from(%{from: %{source: source}} = query, sources) do
    {from, name} = get_source(query, sources, 0, source)
    [" FROM ", from, " AS " | name]
  end

  defp update_fields(%{updates: updates} = query, sources) do
    for(
      %{expr: expr} <- updates,
      {op, kw} <- expr,
      {key, value} <- kw,
      do: update_op(op, key, value, sources, query)
    )
    |> Enum.intersperse(", ")
  end

  defp update_op(:set, key, value, sources, query) do
    [quote_name(key), " = " | Expression.expr(value, sources, query)]
  end

  defp update_op(:inc, key, value, sources, query) do
    [
      quote_name(key),
      " = ",
      quote_qualified_name(key, sources, 0),
      " + "
      | Expression.expr(value, sources, query)
    ]
  end

  defp update_op(:push, key, value, sources, query) do
    [
      quote_name(key),
      " = array_append(",
      quote_qualified_name(key, sources, 0),
      ", ",
      Expression.expr(value, sources, query),
      ?)
    ]
  end

  defp update_op(:pull, key, value, sources, query) do
    [
      quote_name(key),
      " = array_remove(",
      quote_qualified_name(key, sources, 0),
      ", ",
      Expression.expr(value, sources, query),
      ?)
    ]
  end

  defp update_op(command, _key, _value, _sources, query) do
    error!(query, "unknown update operation #{inspect(command)} for PostgreSQL")
  end

  defp using_join(%{joins: []}, _kind, _prefix, _sources), do: {[], []}

  defp using_join(%{joins: joins} = query, kind, prefix, sources) do
    froms =
      intersperse_map(joins, ", ", fn
        %JoinExpr{qual: :inner, ix: ix, source: source} ->
          {join, name} = get_source(query, sources, ix, source)
          [join, " AS " | name]

        %JoinExpr{qual: qual} ->
          error!(query, "PostgreSQL supports only inner joins on #{kind}, got: `#{qual}`")
      end)

    wheres =
      for %JoinExpr{on: %QueryExpr{expr: value} = expr} <- joins,
          value != true,
          do: expr |> Map.put(:__struct__, BooleanExpr) |> Map.put(:op, :and)

    {[?\s, prefix, ?\s | froms], wheres}
  end

  defp join(%{joins: []}, _sources), do: []

  defp join(%{joins: joins} = query, sources) do
    [
      ?\s
      | intersperse_map(joins, ?\s, fn
          %JoinExpr{
            on: %QueryExpr{expr: expr},
            qual: qual,
            ix: ix,
            source: source,
            hints: hints
          } ->
            if hints != [] do
              error!(query, "table hints are not supported by PostgreSQL")
            end

            {join, name} = get_source(query, sources, ix, source)
            [join_qual(qual), join, " AS ", name | join_on(qual, expr, sources, query)]
        end)
    ]
  end

  defp join_on(:cross, true, _sources, _query), do: []

  defp join_on(_qual, expr, sources, query),
    do: [" ON " | Expression.expr(expr, sources, query)]

  defp join_qual(:inner), do: "INNER JOIN "
  defp join_qual(:inner_lateral), do: "INNER JOIN LATERAL "
  defp join_qual(:left), do: "LEFT OUTER JOIN "
  defp join_qual(:left_lateral), do: "LEFT OUTER JOIN LATERAL "
  defp join_qual(:right), do: "RIGHT OUTER JOIN "
  defp join_qual(:full), do: "FULL OUTER JOIN "
  defp join_qual(:cross), do: "CROSS JOIN "

  defp where(%{wheres: wheres} = query, sources) do
    boolean(" WHERE ", wheres, sources, query)
  end

  defp having(%{havings: havings} = query, sources) do
    boolean(" HAVING ", havings, sources, query)
  end

  defp group_by(%{group_bys: []}, _sources), do: []

  defp group_by(%{group_bys: group_bys} = query, sources) do
    [
      " GROUP BY "
      | intersperse_map(group_bys, ", ", fn
          %QueryExpr{expr: expr} ->
            intersperse_map(expr, ", ", &Expression.expr(&1, sources, query))
        end)
    ]
  end

  defp window(%{windows: []}, _sources), do: []

  defp window(%{windows: windows} = query, sources) do
    [
      " WINDOW "
      | intersperse_map(windows, ", ", fn {name, %{expr: kw}} ->
          [quote_name(name), " AS " | Expression.window_exprs(kw, sources, query)]
        end)
    ]
  end

  defp window_exprs(kw, sources, query) do
    [?(, intersperse_map(kw, ?\s, &window_expr(&1, sources, query)), ?)]
  end

  defp window_expr({:partition_by, fields}, sources, query) do
    ["PARTITION BY " | intersperse_map(fields, ", ", &Expression.expr(&1, sources, query))]
  end

  defp window_expr({:order_by, fields}, sources, query) do
    ["ORDER BY " | intersperse_map(fields, ", ", &order_by_expr(&1, sources, query))]
  end

  defp window_expr({:frame, {:fragment, _, _} = fragment}, sources, query) do
    Expression.expr(fragment, sources, query)
  end

  defp order_by(%{order_bys: []}, _distinct, _sources), do: []

  defp order_by(%{order_bys: order_bys} = query, distinct, sources) do
    order_bys = Enum.flat_map(order_bys, & &1.expr)

    [
      " ORDER BY "
      | intersperse_map(distinct ++ order_bys, ", ", &order_by_expr(&1, sources, query))
    ]
  end

  defp order_by_expr({dir, expr}, sources, query) do
    str = Expression.expr(expr, sources, query)

    case dir do
      :asc -> str
      :asc_nulls_last -> [str | " ASC NULLS LAST"]
      :asc_nulls_first -> [str | " ASC NULLS FIRST"]
      :desc -> [str | " DESC"]
      :desc_nulls_last -> [str | " DESC NULLS LAST"]
      :desc_nulls_first -> [str | " DESC NULLS FIRST"]
    end
  end

  defp limit(%{limit: nil}, _sources), do: []

  defp limit(%{limit: %QueryExpr{expr: expr}} = query, sources) do
    [" LIMIT " | Expression.expr(expr, sources, query)]
  end

  defp offset(%{offset: nil}, _sources), do: []

  defp offset(%{offset: %QueryExpr{expr: expr}} = query, sources) do
    [" OFFSET " | Expression.expr(expr, sources, query)]
  end

  defp combinations(%{combinations: combinations}) do
    Enum.map(combinations, fn
      {:union, query} -> [" UNION (", all(query), ")"]
      {:union_all, query} -> [" UNION ALL (", all(query), ")"]
      {:except, query} -> [" EXCEPT (", all(query), ")"]
      {:except_all, query} -> [" EXCEPT ALL (", all(query), ")"]
      {:intersect, query} -> [" INTERSECT (", all(query), ")"]
      {:intersect_all, query} -> [" INTERSECT ALL (", all(query), ")"]
    end)
  end

  defp lock(nil), do: []
  defp lock(lock_clause), do: [?\s | lock_clause]

  defp boolean(_name, [], _sources, _query), do: []

  defp boolean(name, [%{expr: expr, op: op} | query_exprs], sources, query) do
    [
      name
      | Enum.reduce(query_exprs, {op, paren_expr(expr, sources, query)}, fn
          %BooleanExpr{expr: expr, op: op}, {op, acc} ->
            {op, [acc, operator_to_boolean(op), paren_expr(expr, sources, query)]}

          %BooleanExpr{expr: expr, op: op}, {_, acc} ->
            {op, [?(, acc, ?), operator_to_boolean(op), paren_expr(expr, sources, query)]}
        end)
        |> elem(1)
    ]
  end

  defp operator_to_boolean(:and), do: " AND "
  defp operator_to_boolean(:or), do: " OR "

  defp parens_for_select([first_expr | _] = expr) do
    if is_binary(first_expr) and String.starts_with?(first_expr, ["SELECT", "select"]) do
      [?(, expr, ?)]
    else
      expr
    end
  end

  defp paren_expr(expr, sources, query) do
    [?(, Expression.expr(expr, sources, query), ?)]
  end

  defp tagged_to_db({:array, type}), do: [tagged_to_db(type), ?[, ?]]
  # Always use the largest possible type for integers
  defp tagged_to_db(:id), do: "bigint"
  defp tagged_to_db(:integer), do: "bigint"
  defp tagged_to_db(type), do: ecto_to_db(type)

  defp interval(count, interval, _sources, _query) when is_integer(count) do
    ["interval '", String.Chars.Integer.to_string(count), ?\s, interval, ?\']
  end

  defp interval(count, interval, _sources, _query) when is_float(count) do
    count = :erlang.float_to_binary(count, [:compact, decimals: 16])
    ["interval '", count, ?\s, interval, ?\']
  end

  defp interval(count, interval, sources, query) do
    [
      ?(,
      Expression.expr(count, sources, query),
      "::numeric * ",
      interval(1, interval, sources, query),
      ?)
    ]
  end

  defp op_to_binary({op, _, [_, _]} = expr, sources, query) when op in @binary_ops do
    paren_expr(expr, sources, query)
  end

  defp op_to_binary(expr, sources, query) do
    Expression.expr(expr, sources, query)
  end

  defp returning(%{select: nil}, _sources),
    do: []

  defp returning(%{select: %{fields: fields}} = query, sources),
    do: [" RETURNING " | select_fields(fields, sources, query)]

  defp returning([]),
    do: []

  defp returning(returning),
    do: [" RETURNING " | intersperse_map(returning, ", ", &quote_name/1)]

  defp create_names(%{sources: sources}) do
    create_names(sources, 0, tuple_size(sources)) |> List.to_tuple()
  end

  defp create_names(sources, pos, limit) when pos < limit do
    [create_name(sources, pos) | create_names(sources, pos + 1, limit)]
  end

  defp create_names(_sources, pos, pos) do
    []
  end

  defp create_name(sources, pos) do
    case elem(sources, pos) do
      {:fragment, _, _} ->
        {nil, [?f | Integer.to_string(pos)], nil}

      {table, schema, prefix} ->
        name = [create_alias(table) | Integer.to_string(pos)]
        {quote_table(prefix, table), name, schema}

      %Ecto.SubQuery{} ->
        {nil, [?s | Integer.to_string(pos)], nil}
    end
  end

  defp create_alias(<<first, _rest::binary>>) when first in ?a..?z when first in ?A..?Z do
    <<first>>
  end

  defp create_alias(_) do
    "t"
  end
end
