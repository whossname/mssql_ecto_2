if Code.ensure_loaded?(Mssqlex) do
  defmodule MssqlEcto.Connection do
    @moduledoc false

    @default_port 1433
    @behaviour Ecto.Adapters.SQL.Connection

    alias MssqlEcto.Connection.{Constraints, Expression, DDL}

    import MssqlEcto.Connection.Helper
    require Logger

    ## Module and Options

    @impl true
    def child_spec(opts) do
      opts
      |> Keyword.put_new(:port, @default_port)
      |> Mssqlex.child_spec()
    end

    @impl true
    def to_constraints(
          %Mssqlex.Error{
            odbc_code: odbc_code,
            message: message,
            constraint_violations: constraint_violations
          } = error
        ) do
      Logger.debug(error)
      Constraints.to_constraints(odbc_code, message, constraint_violations)
    end

    ## Query
    @impl true
    def prepare_execute(conn, name, sql, params, opts) do
      Mssqlex.prepare_execute(conn, name, sql, params, opts)
    end

    @impl true
    def query(conn, sql, params, opts) do
      Mssqlex.query(conn, sql, params, opts)
    end

    @impl true
    def execute(conn, %{ref: ref} = query, params, opts) do
      case Mssqlex.execute(conn, query, params, opts) do
        {:ok, %{ref: ^ref}, result} ->
          {:ok, result}

        {:ok, _, _} = ok ->
          ok

        {:error, %Mssqlex.QueryError{} = err} ->
          {:reset, err}

        {:error, %Mssqlex.Error{odbc_code: :feature_not_supported} = err} ->
          {:reset, err}

        {:error, _} = error ->
          error
      end
    end

    @impl true
    def stream(conn, sql, params, opts) do
      Mssqlex.stream(conn, sql, params, opts)
    end

    @impl true
    def execute_ddl(args) do
      DDL.execute(args)
    end

    # query
    @impl true
    def all(query), do: Query.all(query)

    @impl true
    def update_all(%{from: %{source: source}} = query, prefix \\ nil) do
      sources = create_names(query)
      {from, name} = get_source(query, sources, 0, source)

      prefix = prefix || ["UPDATE ", from, " AS ", name | " SET "]
      fields = update_fields(query, sources)
      {join, wheres} = using_join(query, :update_all, "FROM", sources)
      where = where(%{query | wheres: wheres ++ query.wheres}, sources)

      [prefix, fields, join, where | returning(query, sources)]
    end

    @impl true
    def delete_all(%{from: from} = query) do
      sources = create_names(query)
      {from, name} = get_source(query, sources, 0, from)

      {join, wheres} = using_join(query, :delete_all, "USING", sources)
      where = where(%{query | wheres: wheres ++ query.wheres}, sources)

      ["DELETE FROM ", from, " AS ", name, join, where | returning(query, sources)]
    end

    @impl true
    def insert(prefix, table, header, rows, on_conflict, returning) do
      values =
        if header == [] do
          [" VALUES " | intersperse_map(rows, ?,, fn _ -> "(DEFAULT)" end)]
        else
          [?\s, ?(, intersperse_map(header, ?,, &quote_name/1), ") VALUES " | insert_all(rows, 1)]
        end

      [
        "INSERT INTO ",
        quote_table(prefix, table),
        insert_as(on_conflict),
        values,
        on_conflict(on_conflict, header) | returning(returning)
      ]
    end

    @impl true
    def update(prefix, table, fields, filters, returning) do
      {fields, count} =
        intersperse_reduce(fields, ", ", 1, fn field, acc ->
          {[quote_name(field), " = $" | Integer.to_string(acc)], acc + 1}
        end)

      {filters, _count} =
        intersperse_reduce(filters, " AND ", count, fn
          {field, nil}, acc ->
            {[quote_name(field), " IS NULL"], acc}

          {field, _value}, acc ->
            {[quote_name(field), " = $" | Integer.to_string(acc)], acc + 1}
        end)

      [
        "UPDATE ",
        quote_table(prefix, table),
        " SET ",
        fields,
        " WHERE ",
        filters | returning(returning)
      ]
    end

    @impl true
    def delete(prefix, table, filters, returning) do
      {filters, _} =
        intersperse_reduce(filters, " AND ", 1, fn
          {field, nil}, acc ->
            {[quote_name(field), " IS NULL"], acc}

          {field, _value}, acc ->
            {[quote_name(field), " = $" | Integer.to_string(acc)], acc + 1}
        end)

      ["DELETE FROM ", quote_table(prefix, table), " WHERE ", filters | returning(returning)]
    end
  end
end
