# Contributing

Thanks for helping improve datafusion-openlineage. Bug reports, feature requests
and pull requests are all welcome. For anything larger than a small fix, open an
issue first so we can agree on the approach before you write the code.

## Development setup

- **Rust.** CI builds with stable 1.96 and checks the MSRV (`rust-version` in
  the workspace `Cargo.toml`, currently 1.95) separately. Coverage needs a
  nightly toolchain with `llvm-tools-preview`.
- **[just](https://github.com/casey/just)** runs the common tasks (`just` lists
  them).
- **[cargo-nextest](https://nexte.st)** runs the tests.
- **[typos](https://github.com/crate-ci/typos)** and
  **[cargo-machete](https://github.com/bnjbvr/cargo-machete)** are used by
  `just check`.
- **Optional:** [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov) for
  `just coverage`, [cargo-deny](https://github.com/EmbarkStudios/cargo-deny) for
  the dependency policy, Docker for the Marquez acceptance test, and
  [pre-commit](https://pre-commit.com) (`pre-commit install`) to run fmt,
  typos and machete on commit.

## Before you push

```sh
just check
```

This mirrors the required CI checks: `cargo fmt --check`, clippy with
`-D warnings` on all targets and features, the nextest suite, doctests (nextest
doesn't run them), typos, and cargo-machete. CI also runs `cargo deny check`,
builds on the MSRV, and runs zizmor on the workflows.

Other useful recipes:

| Recipe | What it does |
|---|---|
| `just test` | Unit, integration and OpenLineage conformance tests. No services needed. |
| `just marquez-it` | Acceptance test against a live [Marquez](https://marquezproject.ai) (needs Docker). |
| `just demo` | End-to-end pipeline example. `OPENLINEAGE_URL=console` prints the events. |
| `just coverage` | The coverage report CI publishes to Codecov, as HTML (`just coverage-open`). |

## Tests

- Emitted events are validated against the vendored OpenLineage JSON Schemas in
  `crates/open-lineage/tests/conformance.rs`. When you add or change a facet,
  update the schemas as described in
  [`tests/schemas/openlineage/README.md`](crates/open-lineage/tests/schemas/openlineage/README.md).
- Tests that need external services are `#[ignore]`d and feature-gated, so
  `just test` stays offline.
- Public API changes should come with rustdoc, and with a doc example where it
  helps.

## Pull requests

- **PR titles must be [Conventional Commits](https://www.conventionalcommits.org)**:
  `type(scope): subject`, e.g. `feat: add column lineage for MERGE INTO`. Types
  are `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `chore`, `ci`, `build`.
  Mark breaking changes with `!` (`feat!: …`). CI checks the title.
- PRs are **squash-merged**, so the PR title becomes the commit on `main` and
  the changelog entry. Commits inside the PR are only for review.
- Keep each PR to one coherent change.
- The **API compatibility** check runs `cargo semver-checks` against `main` and
  comments on the PR. It's advisory. If it reports a break, make sure the title
  has `!`.
- Coverage is reported by Codecov on the PR and doesn't block merging.

## Dependencies

Dependabot opens weekly update PRs. It waits 7 days before proposing a new
release, so fresh releases get time to settle. `deny.toml` restricts
dependencies to permissive licenses and crates.io sources. A new license needs
a review before it goes on the allowlist.

## Releases

Releases are automated with [release-plz](https://release-plz.dev). Every push
to `main` updates a release PR with version bumps and changelogs derived from
the conventional commits. Merging that PR publishes the crates to crates.io via
trusted publishing (no API token) and creates per-crate tags and GitHub
releases. You don't need to bump versions or edit changelogs by hand.

## License

By contributing, you agree that your contributions are licensed under the
[Apache License, Version 2.0](LICENSE), the same as the project.
