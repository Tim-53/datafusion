# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Apache DataFusion is a fast, extensible query engine built on Apache Arrow, written in Rust. It is organized as a Cargo workspace with ~51 crates under `datafusion/` plus top-level crates (`datafusion-cli`, `datafusion-examples`, `benchmarks`, `test-utils`).

**Version:** 52.x | **MSRV:** 1.88.0 | **Rust Edition:** 2024

## Common Commands

```bash
# Build
cargo build
cargo build --profile ci          # faster CI-optimized build

# Test all (matches CI)
cargo test --profile ci --workspace --lib --tests --bins \
  --features serde,avro,json,backtrace,integration-tests,parquet_encryption

# Test a single crate
cargo test -p datafusion-expr

# Test a specific test by name
cargo test -p datafusion-expr test_name -- --nocapture

# Format
cargo fmt --all

# Lint (full suite including TOML, license headers, typos)
./dev/rust_lint.sh
./dev/rust_lint.sh --write           # auto-fix
./dev/rust_lint.sh --write --allow-dirty

# Clippy only
cargo clippy --all-targets --workspace \
  --features avro,integration-tests,extended_tests -- -D warnings

# Pre-commit hook (runs fmt + clippy)
./pre-commit.sh
# Install: ln -s ../../pre-commit.sh .git/hooks/pre-commit
```

## Architecture

The codebase is aggressively modularized. The dependency flow runs roughly:

```
common / common-runtime / expr-common / physical-expr-common
    ↓
expr → optimizer → physical-expr → physical-plan → physical-optimizer
    ↓
functions* crates (functions, functions-aggregate, functions-nested, functions-window)
    ↓
datasource → datasource-{arrow,csv,json,parquet,avro}
    ↓
session → catalog → catalog-listing
    ↓
core  (main user-facing API: SessionContext, DataFrame)
```

**Key crates:**
- `datafusion/core` — Main entry point; `SessionContext`, `DataFrame`, `ListingTable`. Default features enable most expression types and parquet.
- `datafusion/expr` — Logical plan and expression IR (`LogicalPlan`, `Expr`)
- `datafusion/physical-plan` — Physical execution operators; `ExecutionPlan` trait
- `datafusion/optimizer` — Logical optimizer rules
- `datafusion/physical-optimizer` — Physical optimizer rules
- `datafusion/functions*` — Built-in scalar, aggregate, window, nested functions
- `datafusion/datasource-parquet` — Parquet reader/writer with predicate pushdown
- `datafusion/sql` — SQL parsing (`sqlparser`) and logical planning
- `datafusion/proto` — Protobuf serialization of plans
- `datafusion/substrait` — Substrait plan interchange
- `datafusion/ffi` — C ABI for embedding DataFusion

**Extending DataFusion** (common tasks):
- New scalar function → `datafusion/functions`
- New aggregate function → `datafusion/functions-aggregate`
- New optimizer rule → `datafusion/optimizer`
- New physical operator → `datafusion/physical-plan`
- New data source → `datafusion/datasource`

## Clippy Rules

`clippy.toml` enforces several project-specific rules:
- Use `SpawnedTask::spawn` / `SpawnedTask::spawn_blocking` instead of `tokio::task::spawn*`
- Use `datafusion_common::instant::Instant` instead of `std::time::Instant` (WASM compatibility)
- `future-size-threshold: 10000`, `large-error-threshold: 70`

## SQLLogicTests

Many tests are written as `.slt` files (SQL Logic Tests) in `datafusion/sqllogictest/tests/`. Run them via:
```bash
cargo test -p datafusion-sqllogictest
```

## CI Profiles

The `ci` Cargo profile (defined in workspace `Cargo.toml`) is used heavily in CI — it disables incremental compilation and strips debug info from dependencies. Prefer it for running the full test suite locally to match CI behavior.
