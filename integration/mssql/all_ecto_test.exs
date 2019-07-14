ecto = Mix.Project.deps_paths[:ecto]
ecto = "#{ecto}/integration_test/cases"

Code.require_file "#{ecto}/assoc.exs", __DIR__
Code.require_file "#{ecto}/interval.exs", __DIR__
Code.require_file "#{ecto}/joins.exs", __DIR__
Code.require_file "#{ecto}/preload.exs", __DIR__
Code.require_file "#{ecto}/repo.exs", __DIR__
Code.require_file "#{ecto}/type.exs", __DIR__
Code.require_file "#{ecto}/windows.exs", __DIR__
