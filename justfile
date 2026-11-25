# list all commands by default
_default:
    just --list

# build the whole workspace with all features
build:
    cargo build --workspace --all-features

# check formatting and run clippy with warnings denied (what CI gates on)
lint:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# The always-on OpenLineage conformance suite needs no services; the Marquez
# acceptance test is #[ignore]d and gated behind the `marquez-it` feature.
#
# run the test suite
test:
    cargo nextest run --workspace --all-features

# run the doctests (nextest does not run them)
doctest:
    cargo test --workspace --all-features --doc

# the full pre-push check: lints, tests, doctests, spelling, unused dependencies
check: lint test doctest
    typos
    cargo machete crates

# Instruments a real DataFusion session, runs a bronze→silver→gold pipeline,
# and emits the resulting lineage. Point it at any OpenLineage HTTP endpoint
# with OPENLINEAGE_URL=…, or use OPENLINEAGE_URL=console for a service-free dry
# run that logs each event.
#
# run the live end-to-end DataFusion demo
demo:
    cargo run -p datafusion-openlineage --example e2e_pipeline

# Pulls marquezproject/marquez + postgres via testcontainers.
#
# run the live Marquez acceptance test (needs Docker)
marquez-it:
    cargo test -p datafusion-openlineage --features marquez-it --test marquez_acceptance -- --ignored --nocapture

# Doctest coverage needs the nightly compiler, so everything runs on nightly
# (stable and nightly profile formats can differ). Writes an HTML report under
# target/llvm-cov/html.
#
# collect the same coverage CI publishes to Codecov (nextest + doctests)
coverage:
    cargo +nightly llvm-cov clean --workspace
    cargo +nightly llvm-cov --no-report nextest --workspace --all-features
    cargo +nightly llvm-cov --no-report --doc --workspace --all-features
    cargo +nightly llvm-cov report --doctests --html

# open the HTML coverage report produced by `just coverage`
coverage-open:
    open target/llvm-cov/html/index.html
