<!--
The PR title becomes the squash-merged commit and changelog entry, so it must be
a Conventional Commit: `type(scope): subject`, e.g. `fix: handle empty batches`.
Add `!` for breaking changes (`feat!: ...`). See CONTRIBUTING.md.
-->

## What and why

<!-- What does this change, and why? Link the issue it addresses (Fixes #123). -->

## How it was tested

<!-- Tests added or updated; anything you checked by hand. -->

## Checklist

- [ ] `just check` passes locally
- [ ] Public API changes are documented (rustdoc), and the title has `!` if they break compatibility
- [ ] New or changed facets are covered by the conformance tests
