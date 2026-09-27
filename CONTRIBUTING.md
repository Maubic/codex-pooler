# Contributing

## Tests

Use the Elixir and Erlang versions in `mise.toml` and an isolated PostgreSQL test database. Never point tests at a development or production database.

Run application tests with:

```sh
mix test.product
```

Run a focused file with `mix test path/to/file_test.exs`. On Unix, `make test-fast N=4` runs only the application suite in four isolated database partitions and reports each partition's duration and test count. `mix test` remains the complete compatibility entry point, with Unix tests excluded unless explicitly included.

A run with `CODEX_POOLER_TEST_RUN_NAMESPACE` and `MIX_TEST_PARTITION` set uses a database of its own and drops it when it finishes, whether it passes or fails. A run that is killed or crashes leaves that database behind; `make test-db-prune` drops every such database that no session is connected to and no running test holds, and never touches the shared `codex_pooler_test` databases.

Development tools, Mix tasks and test-infrastructure contracts run separately from the application tests. This profile also includes every `unix_integration` test: Bash lifecycle scripts, Makefile behavior, process signals, resource cleanup, source manifests, POSIX file operations and Docker Compose configuration merging. CI runs both product and tooling profiles:

```sh
mix test.tooling
```

To partition the tooling profile with the same scheduler and database isolation as product tests, run `TEST_FAST_COMMAND="mix test.tooling --warnings-as-errors" make test-fast N=4`. CI runs the two four-partition profiles sequentially, so it never doubles the configured CPU budget.

Use `mix test.unix` to run just the Unix subset. These commands select files before loading them; `mix test --only unix_integration` remains valid but loads the entire test tree before applying tags. `CodexPooler.TestProfiles` owns the profile inventory. The normal suite rejects a Unix-tagged test in an application file not covered by the tooling inventory, so newly added Unix cases cannot silently disappear from CI.

`mix test.tooling` owns its profile filters; for custom tag/name filters, pass explicit file paths or use the ordinary `mix test` command. Execution options such as `--seed` and `--warnings-as-errors` remain available.

Use Linux, macOS, or WSL2 for tooling/Unix profiles, with the same isolated PostgreSQL test setup. The tests check prerequisites before starting their fixtures and report missing commands. The Compose test needs the Docker CLI and Compose plugin to render configuration; it does not need a Docker daemon. Ordinary application tests do not run these shell harness checks. Native Windows execution of the full application suite has not been certified.

## Diagnostic output

Successful tests keep measurement output quiet. Query and frame budget failures include their measured values in the assertion. To print additional replay-cleanup measurements explicitly:

```sh
CODEX_POOLER_TEST_DIAGNOSTICS=1 mix test test/codex_pooler/accounting/request_replay_cleanup_test.exs
```

Expected error scenarios assert their diagnostics locally. Do not suppress unexpected application warnings or lower logging globally to make the suite pass.
