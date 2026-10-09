# dependency compatibility sources

These packages retain the exact runtime source and dependency requirements of the listed Hex releases, with bounded compiler compatibility changes for the application's declared Elixir and Erlang toolchain. They are normal path dependencies, copied into the Docker dependency layer before compilation. No download-time patching or compiler warning filter is required. Rebar generates the SMTP RFC822 parser from the archived grammar and writes compiler graphs; vendor/.gitignore keeps those build outputs out of source control.

| package | release | local changes |
| --- | --- | --- |
| gettext | 1.0.2 | remove two unreachable clauses; use `Kernel.ParallelCompiler.pmap/2`; declare its actual Elixir minimum of 1.16 |
| postgrex | 0.22.4 | migrate the existing optional Jason reference allowance from `xref.exclude` to `elixirc_options.no_warn_undefined` |
| phoenix_ecto | 4.7.0 | migrate the same three existing optional Ecto.Migrator reference allowances to the supported compiler option |
| telemetry_metrics_prometheus_core | 1.2.1 | remove unused `require Logger`; explicitly ignore the Range step in the existing tuple bucket pattern |
| gen_smtp | 1.3.0 | replace two deprecated Erlang `catch` expressions with `try/catch`, preserving normal results, thrown values, exit reasons and error stack traces; install the returned state after a successful server code-change callback |

The upstream release versions remain unchanged. Gettext's honest minimum is the only changed language requirement. The metrics package already requires Elixir 1.12, which supports stepped ranges. No new undefined-reference allowances are introduced. SMTP's exception handling is preserved. Successful server code-change callbacks now install their returned state, including a thrown `{ok, State}` result; other outcomes retain the previous callback state.

## provenance and licenses

`provenance.json` records the original release archive URL, outer SHA-256 (verified against the official Hex release API), inner package checksum, original per-file SHA-256 values, changed-file SHA-256 values and patch origins. Original archive files are retained without modification except the listed patches and the prominent change notices required in modified Apache-licensed files. Package development dependencies and upstream documentation remain part of the original payload.

- Gettext and Postgrex contain their copyright and Apache-2.0 notices in their original `README.md` files. Neither the release archive nor the matching upstream tag contains a separate LICENSE file. `APACHE-2.0.txt` supplies the full canonical license text without replacing those notices.
- telemetry_metrics_prometheus_core includes its original Apache-2.0 `LICENSE`.
- phoenix_ecto includes its original MIT `LICENSE`.
- gen_smtp includes its original BSD license and individual source-file notices, including the separate notice in `smtp_socket.erl`.

Gettext's three code edits follow upstream commit `3163e3cbf6c015d9e37efa08adf42dc3e907f58b`; Postgrex's option migration follows `4fb0e42d722853c61706a949021f6727fadcfc0b`; Phoenix Ecto's follows `d0b02063159762791982c0d44beff411b61cc5f7`. The metrics range pattern is present in upstream commit `fe82cc5457fb7a769e195ddd3586c492e72072ff`; the unused require removal and semantically equivalent SMTP syntax replacements are local compatibility changes.

## replacing these copies

Recheck official releases before updating. Replace each package with a pinned Hex release only after its actual archive includes the required fixes and its transitive/runtime changes have been reviewed. Run a real cold compile with isolated dependency caches, the dependency runtime contract tests and relevant adjacent PostgreSQL, metrics and mailer tests. Verify the Docker dependency layer too. Remove that package's directory and provenance entry when its path dependency is replaced; remove the Docker vendor COPY only after the last vendored dependency is gone.

Do not edit downloaded `deps/` copies or add warning filters. Keep future patches bounded and update the original-to-patched file identities. Do not format unchanged upstream files with the application's formatting settings.
