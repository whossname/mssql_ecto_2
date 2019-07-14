ecto = Mix.Project.deps_paths[:ecto]
ecto = "#{ecto}/integration_test/cases"

Code.require_file "#{ecto}/assoc.exs", __DIR__
Code.require_file "#{ecto}/interval.exs", __DIR__
Code.require_file "#{ecto}/joins.exs", __DIR__
Code.require_file "#{ecto}/preload.exs", __DIR__
Code.require_file "#{ecto}/repo.exs", __DIR__
Code.require_file "#{ecto}/type.exs", __DIR__
Code.require_file "#{ecto}/windows.exs", __DIR__

ecto_sql = Mix.Project.deps_paths[:ecto_sql]
ecto_sql = "#{ecto_sql}/integration_test/sql"

Code.require_file "#{ecto_sql}/alter.exs", __DIR__
Code.require_file "#{ecto_sql}/lock.exs", __DIR__
Code.require_file "#{ecto_sql}/logging.exs", __DIR__
Code.require_file "#{ecto_sql}/migration.exs", __DIR__
Code.require_file "#{ecto_sql}/migrator.exs", __DIR__
Code.require_file "#{ecto_sql}/sandbox.exs", __DIR__
Code.require_file "#{ecto_sql}/sql.exs", __DIR__
Code.require_file "#{ecto_sql}/stream.exs", __DIR__
Code.require_file "#{ecto_sql}/subquery.exs", __DIR__
Code.require_file "#{ecto_sql}/transaction.exs", __DIR__
