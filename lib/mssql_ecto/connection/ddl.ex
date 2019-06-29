defmodule MssqlEcto.Connection.DDL do
  alias Ecto.Migration.{Table, Index, Reference, Constraint}
  import MssqlEcto.Connection.Helper

  @creates [:create, :create_if_not_exists]
  @drops [:drop, :drop_if_exists]

  def execute({command, %Table{} = table, columns}) when command in @creates do
    table_name = quote_table(table.prefix, table.name)

    query = [
      "CREATE TABLE ",
      if_do(command == :create_if_not_exists, "IF NOT EXISTS "),
      table_name,
      ?\s,
      ?(,
      column_definitions(table, columns),
      pk_definition(columns, ", "),
      ?),
      options_expr(table.options)
    ]

    [query] ++
      comments_on("TABLE", table_name, table.comment) ++
      comments_for_columns(table_name, columns)
  end

  def execute({command, %Table{} = table}) when command in @drops do
    [
      [
        "DROP TABLE ",
        if_do(command == :drop_if_exists, "IF EXISTS "),
        quote_table(table.prefix, table.name)
      ]
    ]
  end

  def execute({:alter, %Table{} = table, changes}) do
    table_name = quote_table(table.prefix, table.name)

    query = [
      "ALTER TABLE ",
      table_name,
      ?\s,
      column_changes(table, changes),
      pk_definition(changes, ", ADD ")
    ]

    [query] ++
      comments_on("TABLE", table_name, table.comment) ++
      comments_for_columns(table_name, changes)
  end

  def execute({:create, %Index{} = index}) do
    fields = intersperse_map(index.columns, ", ", &index_expr/1)

    queries = [
      [
        "CREATE ",
        if_do(index.unique, "UNIQUE "),
        "INDEX ",
        if_do(index.concurrently, "CONCURRENTLY "),
        quote_name(index.name),
        " ON ",
        quote_table(index.prefix, index.table),
        if_do(index.using, [" USING ", to_string(index.using)]),
        ?\s,
        ?(,
        fields,
        ?),
        if_do(index.where, [" WHERE ", to_string(index.where)])
      ]
    ]

    queries ++ comments_on("INDEX", quote_name(index.name), index.comment)
  end

  def execute({:create_if_not_exists, %Index{} = index}) do
    if index.concurrently do
      raise ArgumentError,
            "concurrent index and create_if_not_exists is not supported by the Mssql adapter"
    end

    [
      [
        "DO $$ BEGIN ",
        execute({:create, index}),
        ";",
        "EXCEPTION WHEN duplicate_table THEN END; $$;"
      ]
    ]
  end

  def execute({command, %Index{} = index}) when command in @drops do
    [
      [
        "DROP INDEX ",
        if_do(index.concurrently, "CONCURRENTLY "),
        if_do(command == :drop_if_exists, "IF EXISTS "),
        quote_table(index.prefix, index.name)
      ]
    ]
  end

  def execute({:rename, %Table{} = current_table, %Table{} = new_table}) do
    [
      [
        "ALTER TABLE ",
        quote_table(current_table.prefix, current_table.name),
        " RENAME TO ",
        quote_table(nil, new_table.name)
      ]
    ]
  end

  def execute({:rename, %Table{} = table, current_column, new_column}) do
    [
      [
        "ALTER TABLE ",
        quote_table(table.prefix, table.name),
        " RENAME ",
        quote_name(current_column),
        " TO ",
        quote_name(new_column)
      ]
    ]
  end

  def execute({:create, %Constraint{} = constraint}) do
    table_name = quote_table(constraint.prefix, constraint.table)
    queries = [["ALTER TABLE ", table_name, " ADD ", new_constraint_expr(constraint)]]

    queries ++ comments_on("CONSTRAINT", constraint.name, constraint.comment, table_name)
  end

  def execute({:drop, %Constraint{} = constraint}) do
    [
      [
        "ALTER TABLE ",
        quote_table(constraint.prefix, constraint.table),
        " DROP CONSTRAINT ",
        quote_name(constraint.name)
      ]
    ]
  end

  def execute({:drop_if_exists, %Constraint{} = constraint}) do
    [
      [
        "ALTER TABLE ",
        quote_table(constraint.prefix, constraint.table),
        " DROP CONSTRAINT IF EXISTS ",
        quote_name(constraint.name)
      ]
    ]
  end

  def execute(string) when is_binary(string), do: [string]

  def execute(keyword) when is_list(keyword),
    do: error!(nil, "MSSQL adapter does not support keyword lists in execute")

  def logs(%Mssqlex.Result{} = result) do
    %{messages: messages} = result

    for message <- messages do
      %{message: message, severity: severity} = message

      {ddl_log_level(severity), message, []}
    end
  end

  def table_exists_query(table) do
    {"SELECT true FROM information_schema.tables WHERE table_name = $1 AND table_schema = current_schema() LIMIT 1",
     [table]}
  end

  # From https://www.postgresql.org/docs/9.3/static/protocol-error-fields.html.
  defp ddl_log_level("DEBUG"), do: :debug
  defp ddl_log_level("LOG"), do: :info
  defp ddl_log_level("INFO"), do: :info
  defp ddl_log_level("NOTICE"), do: :info
  defp ddl_log_level("WARNING"), do: :warn
  defp ddl_log_level("ERROR"), do: :error
  defp ddl_log_level("FATAL"), do: :error
  defp ddl_log_level("PANIC"), do: :error
  defp ddl_log_level(_severity), do: :info

  defp pk_definition(columns, prefix) do
    pks =
      for {_, name, _, opts} <- columns,
          opts[:primary_key],
          do: name

    case pks do
      [] -> []
      _ -> [prefix, "PRIMARY KEY (", intersperse_map(pks, ", ", &quote_name/1), ")"]
    end
  end

  defp comments_on(_object, _name, nil), do: []

  defp comments_on(object, name, comment) do
    [["COMMENT ON ", object, ?\s, name, " IS ", single_quote(comment)]]
  end

  defp comments_on(_object, _name, nil, _table_name), do: []

  defp comments_on(object, name, comment, table_name) do
    [
      [
        "COMMENT ON ",
        object,
        ?\s,
        quote_name(name),
        " ON ",
        table_name,
        " IS ",
        single_quote(comment)
      ]
    ]
  end

  defp comments_for_columns(table_name, columns) do
    Enum.flat_map(columns, fn
      {_operation, column_name, _column_type, opts} ->
        column_name = [table_name, ?. | quote_name(column_name)]
        comments_on("COLUMN", column_name, opts[:comment])

      _ ->
        []
    end)
  end

  defp column_definitions(table, columns) do
    intersperse_map(columns, ", ", &column_definition(table, &1))
  end

  defp column_definition(table, {:add, name, %Reference{} = ref, opts}) do
    [
      quote_name(name),
      ?\s,
      reference_column_type(ref.type, opts),
      column_options(ref.type, opts),
      reference_expr(ref, table, name)
    ]
  end

  defp column_definition(_table, {:add, name, type, opts}) do
    [quote_name(name), ?\s, column_type(type, opts), column_options(type, opts)]
  end

  defp column_changes(table, columns) do
    intersperse_map(columns, ", ", &column_change(table, &1))
  end

  defp column_change(table, {:add, name, %Reference{} = ref, opts}) do
    [
      "ADD COLUMN ",
      quote_name(name),
      ?\s,
      reference_column_type(ref.type, opts),
      column_options(ref.type, opts),
      reference_expr(ref, table, name)
    ]
  end

  defp column_change(_table, {:add, name, type, opts}) do
    ["ADD COLUMN ", quote_name(name), ?\s, column_type(type, opts), column_options(type, opts)]
  end

  defp column_change(table, {:add_if_not_exists, name, %Reference{} = ref, opts}) do
    [
      "ADD COLUMN IF NOT EXISTS ",
      quote_name(name),
      ?\s,
      reference_column_type(ref.type, opts),
      column_options(ref.type, opts),
      reference_expr(ref, table, name)
    ]
  end

  defp column_change(_table, {:add_if_not_exists, name, type, opts}) do
    [
      "ADD COLUMN IF NOT EXISTS ",
      quote_name(name),
      ?\s,
      column_type(type, opts),
      column_options(type, opts)
    ]
  end

  defp column_change(table, {:modify, name, %Reference{} = ref, opts}) do
    [
      drop_constraint_expr(opts[:from], table, name),
      "ALTER COLUMN ",
      quote_name(name),
      " TYPE ",
      reference_column_type(ref.type, opts),
      constraint_expr(ref, table, name),
      modify_null(name, opts),
      modify_default(name, ref.type, opts)
    ]
  end

  defp column_change(table, {:modify, name, type, opts}) do
    [
      drop_constraint_expr(opts[:from], table, name),
      "ALTER COLUMN ",
      quote_name(name),
      " TYPE ",
      column_type(type, opts),
      modify_null(name, opts),
      modify_default(name, type, opts)
    ]
  end

  defp column_change(_table, {:remove, name}), do: ["DROP COLUMN ", quote_name(name)]

  defp column_change(table, {:remove, name, %Reference{} = ref, _opts}) do
    [drop_constraint_expr(ref, table, name), "DROP COLUMN ", quote_name(name)]
  end

  defp column_change(_table, {:remove, name, _type, _opts}),
    do: ["DROP COLUMN ", quote_name(name)]

  defp column_change(table, {:remove_if_exists, name, %Reference{} = ref}) do
    [
      drop_constraint_if_exists_expr(ref, table, name),
      "DROP COLUMN IF EXISTS ",
      quote_name(name)
    ]
  end

  defp column_change(_table, {:remove_if_exists, name, _type}),
    do: ["DROP COLUMN IF EXISTS ", quote_name(name)]

  defp modify_null(name, opts) do
    case Keyword.get(opts, :null) do
      true -> [", ALTER COLUMN ", quote_name(name), " DROP NOT NULL"]
      false -> [", ALTER COLUMN ", quote_name(name), " SET NOT NULL"]
      nil -> []
    end
  end

  defp modify_default(name, type, opts) do
    case Keyword.fetch(opts, :default) do
      {:ok, val} ->
        [", ALTER COLUMN ", quote_name(name), " SET", default_expr({:ok, val}, type)]

      :error ->
        []
    end
  end

  defp column_options(type, opts) do
    default = Keyword.fetch(opts, :default)
    null = Keyword.get(opts, :null)
    [default_expr(default, type), null_expr(null)]
  end

  defp null_expr(false), do: " NOT NULL"
  defp null_expr(true), do: " NULL"
  defp null_expr(_), do: []

  defp new_constraint_expr(%Constraint{check: check} = constraint) when is_binary(check) do
    ["CONSTRAINT ", quote_name(constraint.name), " CHECK (", check, ")"]
  end

  defp new_constraint_expr(%Constraint{exclude: exclude} = constraint)
       when is_binary(exclude) do
    ["CONSTRAINT ", quote_name(constraint.name), " EXCLUDE USING ", exclude]
  end

  defp default_expr({:ok, nil}, _type), do: " DEFAULT NULL"
  defp default_expr({:ok, literal}, type), do: [" DEFAULT ", default_type(literal, type)]
  defp default_expr(:error, _), do: []

  defp default_type(list, {:array, inner} = type) when is_list(list) do
    [
      "ARRAY[",
      Enum.map(list, &default_type(&1, inner)) |> Enum.intersperse(?,),
      "]::",
      ecto_to_db(type)
    ]
  end

  defp default_type(literal, _type) when is_binary(literal) do
    if :binary.match(literal, <<0>>) == :nomatch and String.valid?(literal) do
      [?', escape_string(literal), ?']
    else
      encoded = "\\x" <> Base.encode16(literal, case: :lower)

      raise ArgumentError,
            "default values are interpolated as UTF-8 strings and cannot contain null bytes. " <>
              "`#{inspect(literal)}` is invalid. If you want to write it as a binary, use \"#{
                encoded
              }\", " <>
              "otherwise refer to MSSQL documentation for instructions on how to escape this SQL type"
    end
  end

  defp default_type(literal, _type) when is_number(literal), do: to_string(literal)
  defp default_type(literal, _type) when is_boolean(literal), do: to_string(literal)

  defp default_type(%{} = map, :map) do
    library = Application.get_env(:postgrex, :json_library, Jason)
    default = IO.iodata_to_binary(library.encode_to_iodata!(map))
    [single_quote(default)]
  end

  defp default_type({:fragment, expr}, _type),
    do: [expr]

  defp default_type(expr, type),
    do:
      raise(
        ArgumentError,
        "unknown default `#{inspect(expr)}` for type `#{inspect(type)}`. " <>
          ":default may be a string, number, boolean, list of strings, list of integers, map (when type is Map), or a fragment(...)"
      )

  defp index_expr(literal) when is_binary(literal),
    do: literal

  defp index_expr(literal),
    do: quote_name(literal)

  defp options_expr(nil),
    do: []

  defp options_expr(keyword) when is_list(keyword),
    do: error!(nil, "MSSQL adapter does not support keyword lists in :options")

  defp options_expr(options),
    do: [?\s, options]

  defp column_type({:array, type}, opts),
    do: [column_type(type, opts), "[]"]

  defp column_type(type, _opts) when type in ~w(time utc_datetime naive_datetime)a,
    do: [ecto_to_db(type), "(0)"]

  defp column_type(type, opts)
       when type in ~w(time_usec utc_datetime_usec naive_datetime_usec)a do
    precision = Keyword.get(opts, :precision)
    type_name = ecto_to_db(type)

    if precision do
      [type_name, ?(, to_string(precision), ?)]
    else
      type_name
    end
  end

  defp column_type(type, opts) do
    size = Keyword.get(opts, :size)
    precision = Keyword.get(opts, :precision)
    scale = Keyword.get(opts, :scale)
    type_name = ecto_to_db(type)

    cond do
      size -> [type_name, ?(, to_string(size), ?)]
      precision -> [type_name, ?(, to_string(precision), ?,, to_string(scale || 0), ?)]
      type == :string -> [type_name, "(255)"]
      true -> type_name
    end
  end

  defp reference_expr(%Reference{} = ref, table, name),
    do: [
      " CONSTRAINT ",
      reference_name(ref, table, name),
      " REFERENCES ",
      quote_table(ref.prefix || table.prefix, ref.table),
      ?(,
      quote_name(ref.column),
      ?),
      reference_on_delete(ref.on_delete),
      reference_on_update(ref.on_update)
    ]

  defp constraint_expr(%Reference{} = ref, table, name),
    do: [
      ", ADD CONSTRAINT ",
      reference_name(ref, table, name),
      ?\s,
      "FOREIGN KEY (",
      quote_name(name),
      ") REFERENCES ",
      quote_table(ref.prefix || table.prefix, ref.table),
      ?(,
      quote_name(ref.column),
      ?),
      reference_on_delete(ref.on_delete),
      reference_on_update(ref.on_update)
    ]

  defp drop_constraint_expr(%Reference{} = ref, table, name),
    do: ["DROP CONSTRAINT ", reference_name(ref, table, name), ", "]

  defp drop_constraint_expr(_, _, _),
    do: []

  defp drop_constraint_if_exists_expr(%Reference{} = ref, table, name),
    do: ["DROP CONSTRAINT IF EXISTS ", reference_name(ref, table, name), ", "]

  defp drop_constraint_if_exists_expr(_, _, _),
    do: []

  defp reference_name(%Reference{name: nil}, table, column),
    do: quote_name("#{table.name}_#{column}_fkey")

  defp reference_name(%Reference{name: name}, _table, _column),
    do: quote_name(name)

  defp reference_column_type(:serial, _opts), do: "integer"
  defp reference_column_type(:bigserial, _opts), do: "bigint"
  defp reference_column_type(type, opts), do: column_type(type, opts)

  defp reference_on_delete(:nilify_all), do: " ON DELETE SET NULL"
  defp reference_on_delete(:delete_all), do: " ON DELETE CASCADE"
  defp reference_on_delete(:restrict), do: " ON DELETE RESTRICT"
  defp reference_on_delete(_), do: []

  defp reference_on_update(:nilify_all), do: " ON UPDATE SET NULL"
  defp reference_on_update(:update_all), do: " ON UPDATE CASCADE"
  defp reference_on_update(:restrict), do: " ON UPDATE RESTRICT"
  defp reference_on_update(_), do: []
end
