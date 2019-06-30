defmodule MssqlEcto.DeleteTest do
  use MssqlEcto.Case, async: true

  test "delete" do
    query =
      SQL.delete(nil, "schema", [:x, :y], [])
      |> IO.iodata_to_binary()

    assert query == ~s{DELETE FROM "schema" WHERE "x" = ?1 AND "y" = ?2}

    query =
      SQL.delete(nil, "schema", [:x, :y], [:z])
      |> IO.iodata_to_binary()

    assert query ==
             ~s{DELETE FROM "schema" OUTPUT DELETED."z" WHERE "x" = ?1 AND "y" = ?2}

    query =
      SQL.delete("prefix", "schema", [:x, :y], [])
      |> IO.iodata_to_binary()

    assert query ==
             ~s{DELETE FROM "prefix"."schema" WHERE "x" = ?1 AND "y" = ?2}
  end
end
