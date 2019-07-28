ecto_sql = Mix.Project.deps_paths[:ecto_sql]
ecto_sql = "#{ecto_sql}/integration_test/sql"

Code.require_file "#{ecto_sql}/alter.exs", __DIR__
Code.require_file "#{ecto_sql}/logging.exs", __DIR__
Code.require_file "#{ecto_sql}/migration.exs", __DIR__
Code.require_file "#{ecto_sql}/migrator.exs", __DIR__
Code.require_file "#{ecto_sql}/sql.exs", __DIR__
Code.require_file "#{ecto_sql}/subquery.exs", __DIR__


# should support
#Code.require_file "#{ecto_sql}/sandbox.exs", __DIR__

# Partial / No Support
#Code.require_file "#{ecto_sql}/stream.exs", __DIR__
#Code.require_file "#{ecto_sql}/transaction.exs", __DIR__
#Code.require_file "#{ecto_sql}/lock.exs", __DIR__
