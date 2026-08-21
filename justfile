# --- local dev environment knobs (override on the command line, e.g.
# `just PG_PORT=55432 pg-up`) ---

# Docker container + volume names for the local dev Postgres.
PG_CONTAINER := "headwaters-postgres"
PG_VOLUME := "headwaters-pgdata"
# Matches the testcontainers image/creds used by the integration tests.
PG_IMAGE := "postgres:16-alpine"
PG_PORT := "5432"
PG_USER := "postgres"
PG_PASSWORD := "postgres"
PG_DB := "lineage"
# DSN headwaters reads from DATABASE_URL.
DATABASE_URL := "postgres://" + PG_USER + ":" + PG_PASSWORD + "@localhost:" + PG_PORT + "/" + PG_DB

# Port headwaters serves on (the read API the UI talks to).
HEADWATERS_PORT := "8091"

# list all commands by default
_default:
    just --list

# build the whole workspace with all features
build:
    cargo build --workspace --all-features

# run the test suite (the always-on OpenLineage conformance suite needs no
# services; live-integration tests are #[ignore]d and the Marquez acceptance
# test is gated behind the `marquez-it` feature)
test:
    cargo nextest run --workspace --all-features

# run the live end-to-end DataFusion demo against a running headwaters instance: it
# instruments a real DataFusion session, runs a bronze→silver→gold pipeline, and
# emits the resulting lineage to the service (exercises the full instrumentation
# path, unlike the static `seed`). Target a non-default host with OPENLINEAGE_URL=…
# (defaults to http://localhost:8091/api/v1/lineage), or OPENLINEAGE_URL=console
# for a service-free dry run. Start the server first (`just dev`), then `just ui-dev`.
demo:
    cargo run -p datafusion-open-lineage --example e2e_pipeline

# run the black-box DuckDB lineage journey against a running headwaters: installs
# the published duck_lineage community extension into DuckDB, runs the same
# bronze→silver→gold pipeline through it, then asserts headwaters reconstructed the
# graph via the read API. Validates the live wire path for an external engine we
# don't control (unlike the in-process `demo`). Start the server first (`just dev`);
# then `just ui-dev` to see the `duckdb` graph next to the DataFusion `datafusion`
# one. Run via `uv` — deps are declared inline in each script (PEP 723), so no venv
# to manage. Needs `uv` (https://docs.astral.sh/uv/).
duck-journey:
    #!/usr/bin/env bash
    set -euo pipefail
    cd examples/journeys/duckdb
    if ! curl -fsS "http://localhost:{{ HEADWATERS_PORT }}/health" >/dev/null 2>&1; then
        echo "error: no headwaters on :{{ HEADWATERS_PORT }} — start it with \`just dev\`" >&2
        exit 1
    fi
    uv run journey.py
    uv run assert_lineage.py

# --- local dev Postgres (Docker) ---

# Idempotent: reuses (and starts, if stopped) an existing `{{ PG_CONTAINER }}`
# container, so re-running is safe. Data persists in the named volume
# `{{ PG_VOLUME }}` across restarts; use `pg-down` to wipe it. Override
# PG_PORT/PG_DB/etc. on the command line.
#
# start a local Postgres in Docker and wait until it accepts connections
pg-up:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -n "$(docker ps -aq -f name='^{{ PG_CONTAINER }}$')" ]; then
        echo "→ container {{ PG_CONTAINER }} exists; (re)starting"
        docker start {{ PG_CONTAINER }} >/dev/null
    else
        echo "→ creating {{ PG_CONTAINER }} ({{ PG_IMAGE }}) on :{{ PG_PORT }}"
        docker run -d \
            --name {{ PG_CONTAINER }} \
            -e POSTGRES_USER={{ PG_USER }} \
            -e POSTGRES_PASSWORD={{ PG_PASSWORD }} \
            -e POSTGRES_DB={{ PG_DB }} \
            -p {{ PG_PORT }}:5432 \
            -v {{ PG_VOLUME }}:/var/lib/postgresql/data \
            {{ PG_IMAGE }} >/dev/null
    fi
    echo -n "→ waiting for Postgres "
    for _ in $(seq 1 30); do
        if docker exec {{ PG_CONTAINER }} pg_isready -U {{ PG_USER }} -d {{ PG_DB }} >/dev/null 2>&1; then
            echo "ready"
            echo "  DATABASE_URL={{ DATABASE_URL }}"
            exit 0
        fi
        echo -n "."
        sleep 1
    done
    echo; echo "error: Postgres did not become ready in time" >&2; exit 1

# (To keep the data, `docker stop {{ PG_CONTAINER }}` instead.)
#
# remove the local dev Postgres container AND its data volume (a clean slate)
pg-down:
    -docker rm -f {{ PG_CONTAINER }} 2>/dev/null
    -docker volume rm {{ PG_VOLUME }} 2>/dev/null
    @echo "✓ removed {{ PG_CONTAINER }} and volume {{ PG_VOLUME }}"

# open a psql shell against the local dev Postgres.
pg-shell:
    docker exec -it {{ PG_CONTAINER }} psql -U {{ PG_USER }} -d {{ PG_DB }}

# Brings up Postgres, then runs the service against it (auto-migrates on boot,
# serves on :8091). Ctrl-C stops the service; the Postgres container keeps
# running — `just dev-down` tears it down. Extra args pass through to the service
# (e.g. `just dev -- --port 9000`).
#
# clean start of the whole local environment (Postgres + headwaters)
dev *args: pg-up
    DATABASE_URL="{{ DATABASE_URL }}" \
    RUST_LOG="${RUST_LOG:-headwaters=debug}" \
    cargo run -p headwaters -- serve {{ args }}

# the Postgres-backed read/projection acceptance tests (needs Docker; spins up
# a postgres container per test via testcontainers). On colima/Docker Desktop
# you may need to point DOCKER_HOST at the right socket first.
postgres-it:
    cargo nextest run -p headwaters --features postgres-it --test read_test

# the live Marquez reference-backend acceptance test (needs Docker; pulls
# marquezproject/marquez + postgres via testcontainers)
marquez-it:
    cargo test -p datafusion-open-lineage --features marquez-it --test marquez_acceptance -- --ignored --nocapture

# the same coverage CI publishes to Codecov: nextest + doctests, all features, the
# committed codegen / entry points filtered out (keep the regex in sync with the
# `coverage` job in .github/workflows/ci.yml and the `ignore` list in codecov.yml).
# Writes an HTML report under target/llvm-cov/html (open with `just coverage-open`).
# Doctest coverage needs the nightly compiler. Because `--all-features` enables
# `postgres-it`/`conformance-it`, this brings up Docker containers — on
# colima/Docker Desktop, point DOCKER_HOST at the right socket first (e.g.
# `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock just coverage`).
coverage:
    cargo llvm-cov --no-report nextest --workspace --all-features
    cargo +nightly llvm-cov --no-report --doc --workspace --all-features
    cargo +nightly llvm-cov report --doctests --html \
        --ignore-filename-regex 'headwaters-proto/src/(proto/[^/]+\.v1\.rs|connect_gen/)|headwaters/src/(main\.rs|projection/applier\.rs)'

# open the HTML coverage report produced by `just coverage`
coverage-open:
    open target/llvm-cov/html/index.html
