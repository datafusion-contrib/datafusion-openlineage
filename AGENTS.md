# Agent notes

Read [CONTRIBUTING.md](CONTRIBUTING.md) first. It covers setup, conventions and
the release flow. The essentials:

- **Pre-push check:** `just check` (fmt, clippy `-D warnings` on all targets and
  features, `cargo nextest run --workspace --all-features`, doctests via
  `cargo test --workspace --all-features --doc`, typos, cargo-machete).
- **Crates:** `crates/open-lineage` is the `datafusion-openlineage` package (the
  directory and package names differ; use `-p datafusion-openlineage`).
  `crates/openlineage-client` is `openlineage-client`.
- **PR titles** are Conventional Commits and become the squash-merged commit
  and changelog entry. Scope is the package name, e.g.
  `fix(openlineage-client): …`. Don't bump versions or edit `CHANGELOG.md`.
  release-plz does that.
- **Facets:** emitted events must validate against the vendored schemas in
  `crates/open-lineage/tests/schemas/openlineage/`. Update them per that
  directory's README.
- **Workflows:** pin actions to full commit SHAs with a `# vX.Y.Z` comment,
  start from `permissions: {}` and grant per job, and keep `zizmor .github`
  clean.
