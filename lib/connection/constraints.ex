defmodule MssqlEcto.Connection.Constraints do
  def to_constraints(_code, _msg, constraint),
    do: [unique: constraint]

  def to_constraints(_code, _msg, constraint),
    do: [foreign_key: constraint]

  def to_constraints(_code, _msg, constraint),
      do: [exclusion: constraint]

  def to_constraints(_code, _msg, constraint),
    do: [check: constraint]

    """
  # Postgres 9.2 and earlier does not provide the constraint field
  @impl true
  def to_constraints(%Mssqlex.Error{postgres: %{code: :unique_violation, message: message}}) do
    case :binary.split(message, " unique constraint ") do
      [_, quoted] -> [unique: strip_quotes(quoted)]
      _ -> []
    end
  end

  def to_constraints(%Mssqlex.Error{postgres: %{code: :foreign_key_violation, message: message}}) do
    case :binary.split(message, " foreign key constraint ") do
      [_, quoted] ->
        [quoted | _] = :binary.split(quoted, " on table ")
        [foreign_key: strip_quotes(quoted)]

      _ ->
        []
    end
  end

  def to_constraints(%Mssqlex.Error{postgres: %{code: :exclusion_violation, message: message}}) do
    case :binary.split(message, " exclusion constraint ") do
      [_, quoted] -> [exclusion: strip_quotes(quoted)]
      _ -> []
    end
  end

  def to_constraints(%Mssqlex.Error{postgres: %{code: :check_violation, message: message}}) do
    case :binary.split(message, " check constraint ") do
      [_, quoted] -> [check: strip_quotes(quoted)]
      _ -> []
    end
  end

  def to_constraints(_),
    do: []

  defp strip_quotes(quoted) do
    size = byte_size(quoted) - 2
    <<_, unquoted::binary-size(size), _>> = quoted
    unquoted
  end

  """
end
