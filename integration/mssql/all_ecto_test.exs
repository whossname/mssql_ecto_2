ecto = Mix.Project.deps_paths()[:ecto]
ecto = "#{ecto}/integration_test/cases"

Ecto.Integration.TestRepo.__info__(:compile)

Code.require_file("assoc.exs", ecto)
Code.require_file("interval.exs", ecto)
Code.require_file("joins.exs", ecto)
Code.require_file("preload.exs", ecto)
Code.require_file("repo.exs", ecto)
Code.require_file("type.exs", ecto)
Code.require_file("windows.exs", ecto)
