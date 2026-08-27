# RFC 001: Steady-State Backend Comparison

**Status:** Draft
**Date:** 2026-08-25
**Author:** crud-bench contributors

## 1. Summary

Add a steady-state comparison suite to `crud-bench` for embedded backends such
as ToyKV, RocksDB, Fjall, Redb, and SurrealKV.

The suite complements the existing fixed-count CRUD rows with time-windowed
workloads that run against a prepared dataset, measure throughput and p95/p99
latency, and validate that each backend actually executed the configured
operation mix.

The first target is a ToyKV vs RocksDB gate under the same adapter semantics
already used by existing `crud-bench` rows. That means the comparison measures
the configured adapters as they are today, including any backend-specific
transaction wrapper or durability behavior. ToyKV currently wins or ties the
existing durable CRUD rows, so the next comparison should answer a harder
question: does ToyKV remain competitive under hot-key read/update churn,
bounded scans, sustained ingest, and warm cache pressure after setup cost has
been removed from the measured window?

## 2. Motivation

The current `crud-bench` rows are useful for broad backend comparison:

1. create, read, update, and delete phases;
2. batch create/read/update/delete rows;
3. scan rows from `config/bench.toml`;
4. CSV/JSON/HTML artifacts that downstream reports and gates already consume.

Those rows are intentionally short and phase-oriented. They are weaker for
storage-engine work where the important behavior happens after the database is
already loaded:

1. hot-key read/update churn;
2. p95/p99 latency under concurrent writers;
3. cache behavior after warmup;
4. flush and compaction interference during a measurement window;
5. durable write behavior after setup has been excluded;
6. correctness validation for mixed workloads.

ToyKV already has an in-repository `write-perf --suite steady-state` contract
covering these shapes. `crud-bench` should adopt the same benchmark semantics
for cross-backend comparison because it owns the backend adapters and the
comparison artifact schema.

## 3. Goals

1. Add steady-state workloads to `crud-bench` without changing existing CRUD
   row names or result semantics.
2. Support a prepared dataset lifecycle shared by all participating backends.
3. Measure warmup, measurement, and drain as distinct phases.
4. Emit throughput windows plus p50/p95/p99 operation latency.
5. Validate operation mix, completed operation count, read hit/miss
   expectations, and scan result counts.
6. Keep result artifacts parseable by existing comparison tooling.
7. Make `balanced_zipfian` the primary ToyKV vs RocksDB watch row.
8. Keep long steady-state runs out of normal CI.

## 4. Non-Goals

1. Replacing the existing fixed-count CRUD, batch, scan, filter, or index rows.
2. Replacing ToyKV's `write-perf` harness.
3. Adding a production-grade statistical framework in the first slice.
4. Requiring every backend to support every steady-state workload.
5. Automatically failing CI on performance regressions.
6. Adding engine-specific optimization in this RFC.

## 5. Workloads

The MVP should implement these rows first:

| Row | Operation mix | Key selection | Dataset | Purpose |
|---|---:|---|---|---|
| `balanced_zipfian` | 50% read, 50% update | scrambled Zipfian, exponent 0.99 | prepared | Primary mixed read/write gate |
| `read_heavy_zipfian` | 95% read, 5% update | scrambled Zipfian, exponent 0.99 | prepared | Cache-heavy churn |
| `update_heavy_zipfian` | 5% read, 95% update | scrambled Zipfian, exponent 0.99 | prepared | Write-heavy churn |
| `point_read_zipfian` | 100% read | scrambled Zipfian, exponent 0.99 | prepared | Hot-key read throughput and latency |
| `point_read_uniform` | 100% read | uniform positional key selection | prepared | Uniform point-read throughput and latency |
| `point_read_missing_in_range` | 100% missing read | scrambled Zipfian missing-range | prepared | Negative lookup behavior |
| `range_scan_uniform` | 100% scan | uniform positional start | prepared | Bounded scan throughput and count validation |
| `sustained_ingest` | 100% create | unique sequential | fresh empty | Steady durable write path |

Later rows:

| Row | Operation mix | Purpose |
|---|---:|---|
| `idle` | no client operations | Background phase and drain timing |
| `transaction_contention` | parameterized | Serializable transaction contention over a deterministic hot set |

Backends that cannot support a row should report the row as skipped with a
structured unsupported reason, not as a benchmark failure.

The canonical operation names for steady-state rows are `create`, `read`,
`update`, `scan`, and `transaction`. Adapter methods such as `read_*` and
`update_*` map to those names directly. CLI `--operation-mix`, JSON
`task.operation_mix`, and `validation.observed_mix` must use only canonical
names.

`transaction_contention` is a single logical operation. Each attempt reads
`--transaction-reads` keys and updates `--transaction-updates` keys selected
uniformly from `--transaction-hot-set`. Optimistic commit conflicts are
expected outcomes, counted separately, and retried up to
`--transaction-retries`; non-conflict errors remain validation failures.

## 6. Dataset Contract

Steady-state rows use a prepared logical keyspace:

```text
records:       preset-controlled
key type:      integer for MVP
value shape:   existing [value] TOML template
durability:    follows --sync
load phase:    excluded from timed measurement
warmup phase:  included in setup metadata, excluded from result OPS
```

The first slice should use `crud-bench`'s existing value providers and adapter
methods so all embedded backends keep the same datastore surface. It still
needs a new steady-state key-selection layer above the existing `KeyProvider`:
the current providers can generate ordered or Feistel-scrambled keys, but they
do not express Zipfian sampling, the Zipfian exponent, per-run seeds, or the
recorded sampler contract. The runner must expose `--seed <u64>` with default
`1`, serialize the effective seed in every steady-state row, and derive worker
streams as `splitmix64(seed ^ worker_index)`. ToyKV and RocksDB rows in the
same comparison must use the same effective seed. A later compatibility slice
may add ToyKV-compatible fixed-width byte keys if cross-harness parity with
`write-perf` requires it.

Prepared datasets must be backend-local and disposable. The runner may create a
fresh database per backend, bulk-load it, quiesce, and then run one or more
steady-state rows against that prepared state. Copying or checkpointing a
prepared database is optional and backend-specific.

Each measured row must start from a defined dataset state:

1. prepared rows use a freshly loaded or freshly cloned dataset containing keys
   `0..records`;
2. mixed update rows update only keys inside `0..records`;
3. `sustained_ingest` starts from a fresh empty dataset and creates unique keys
   from `0` upward for the duration of the measured window;
4. if multiple rows run in one invocation, the runner must reset, reload, or
   clone the required starting state before each row unless a row explicitly
   opts into reuse.

## 7. Timing Model

Each steady-state row has these phases:

1. `prepare`: create or open the prepared dataset.
2. `warmup`: run the configured workload without recording result OPS.
3. `measure`: run closed-loop workers until `measurement_secs` expires.
4. `drain`: stop workers, wait for requested backend durability or background
   drain hooks, and record elapsed drain time.
5. `cleanup`: remove temporary backend state unless persistence was requested.

The measured row duration is time-based, not fixed-count. Workers run
closed-loop: each worker starts the next operation only after the previous
operation returns. This keeps latency meaningful and avoids hiding backend
queueing behind an unbounded client-side backlog.

When `measurement_secs` expires in the middle of an operation-mix period,
workers finish only the in-flight operation and stop before starting another.
The measurement window may therefore contain a prefix of the deterministic
period rather than an exact number of whole periods. Operation-mix validation
must compare observed counts against the deterministic prefix implied by the
actual completed operation count, not against a full-period ratio.

## 8. CLI

Add a new suite selector while preserving the current default behavior:

```bash
cargo run --release --bin crud-bench -- \
  --suite steady-state \
  --database <toykv|rocksdb|fjall|redb|surrealkv> \
  --bench balanced_zipfian \
  --samples 1000000 \
  --clients 4 \
  --threads 4 \
  --sync \
  --warmup-secs 30 \
  --measurement-secs 120 \
  --latency-sample-every 100 \
  --color never
```

New options:

```text
--suite <crud|steady-state>        default: crud
--bench <name[,name...]>           steady-state workload selection
--preset <smoke|default|large>     steady-state scale preset
--warmup-secs <n>
--measurement-secs <n>
--latency-sample-every <n>
--seed <u64>                         default: 1
--zipfian-exponent <f>             default: 0.99
--operation-mix <spec>             e.g. read=0.5,update=0.5
--operation-mix-period <n>         default: 1000
```

Preset defaults:

| Preset | Records | Clients | Warmup | Measurement | Latency sample every |
|---|---:|---:|---:|---:|---:|
| `smoke` | 10,000 | 1 | 0s | 1s | 1 |
| `default` | 1,000,000 | CLI/default clients | 30s | 120s | 100 |
| `large` | CLI-controlled | CLI/default clients | 60s | 300s | 100 |

Explicit CLI values override preset values.

## 9. Result Schema

Existing CSV/JSON rows stay unchanged for the current CRUD suite. Steady-state
rows add structured fields to the JSON artifact. CSV output must preserve the
existing `result.rs` header contract (`Test`, `Total time`, `Mean`, `Max`,
`99th`, `95th`, `OPS`, and the other current columns) so `perf-gate` and
existing compare tools can continue to parse `Test`, `OPS`, `99th`, and `95th`.
Any extra steady-state fields should be emitted either in JSON only or in a
separate sidecar CSV, not by replacing the legacy CSV columns.

Required JSON fields:

```json
{
  "name": "balanced_zipfian",
  "suite": "steady-state",
  "database": "toykv",
  "status": "completed",
  "unsupported_reason": null,
  "sync": true,
  "task": {
    "records": 1000000,
    "clients": 4,
    "threads": 4,
    "warmup_secs": 30,
    "measurement_secs": 120,
    "latency_sample_every": 100,
    "seed": 1,
    "worker_seed_derivation": "splitmix64(seed ^ worker_index)",
    "operation_mix": "read=0.5,update=0.5",
    "operation_mix_period": 1000,
    "key_selection": "scrambled_zipfian",
    "zipfian_exponent": 0.99
  },
  "phases": {
    "prepare": { "elapsed_ms": 1000.0, "status": "completed" },
    "warmup": { "elapsed_ms": 30000.0, "status": "completed" },
    "measure": { "elapsed_ms": 120000.0, "status": "completed" },
    "drain": { "elapsed_ms": 25.0, "status": "completed" },
    "cleanup": { "elapsed_ms": 10.0, "status": "completed" }
  },
  "throughput": {
    "completed_operations": 123456,
    "ops_per_sec": 1028.8,
    "per_second_windows": [1001.0, 1030.0]
  },
  "latency": {
    "sample_count": 1234,
    "p50_ms": 0.5,
    "p95_ms": 2.0,
    "p99_ms": 5.0
  },
  "validation": {
    "errors": 0,
    "read_hits": 61728,
    "read_misses": 0,
    "updates": 61728,
    "scan_count_errors": 0,
    "observed_mix": "read=0.500000,update=0.500000",
    "expected_mix_prefix": "read=61728,update=61728",
    "transaction_attempts": 0,
    "transaction_commits": 0,
    "transaction_conflicts": 0
  },
  "drain": {
    "elapsed_ms": 25.0,
    "timed_out": false
  }
}
```

Required legacy CSV mapping for completed rows:

```text
Test = [T]steady-state::<row>
OPS = throughput.ops_per_sec
50th = latency.p50_ms
95th = latency.p95_ms
99th = latency.p99_ms
```

Unsupported and failed steady-state rows must use the legacy CSV skip marker
columns, even when the JSON row keeps diagnostic measurement data.

Optional sidecar CSV columns:

```text
suite,row,database,status,unsupported_reason,failure_reason,sync,
completed_operations,validation_errors,latency_sample_count,
latency_sample_every,drain_elapsed_ms,drain_timed_out,operation_mix,
key_selection
```

Steady-state gate tools must read the JSON artifact for throughput, latency,
validation, drain, and unsupported-row state. The sidecar CSV is a diagnostic
artifact for status dashboards and quick inspection; it intentionally does not
replace JSON for gate decisions. The legacy CSV is retained for existing
throughput and latency consumers only; it is not sufficient by itself to decide
a steady-state gate.

Allowed row statuses are `completed`, `unsupported`, and `failed`.
Unsupported rows must set `status = "unsupported"` and a non-empty
`unsupported_reason`. For unsupported rows, throughput, latency, validation,
and drain fields may be absent or null, and gate tools must treat the row as
skipped only when the gate marks that row optional. Failed rows must not be
converted to unsupported rows. `failure_reason` is optional for completed and
unsupported JSON rows, and required for failed rows.

Gate tools must reject rows with:

1. `validation.errors > 0`;
2. zero completed operations;
3. missing latency samples when latency sampling was requested;
4. timed-out drain when drain was requested;
5. unsupported rows unless the gate explicitly marks them optional.

## 10. Adapter API

The first slice can build on the existing `BenchmarkClient` methods:

1. `read_*` for point reads;
2. `update_*` for updates;
3. `create_*` for sustained ingest;
4. existing scan methods for bounded range scans.

The runner should add an internal steady-state execution layer rather than
forcing the existing phase-oriented benchmark path to handle time-windowed
workers.

Optional future adapter hooks:

```rust
async fn prepare_steady_state_dataset(...);
async fn quiesce_steady_state(...);
async fn drain_steady_state(...);
async fn clone_prepared_dataset(...);
```

Backends without custom hooks use the generic create/read/update/scan path.
ToyKV can later map clone preparation to its checkpoint API, but that is not
required for the MVP.

## 11. Validation Rules

`balanced_zipfian`, `read_heavy_zipfian`, and `update_heavy_zipfian` must use a
deterministic operation-mix scheduler. For a period of 1000:

```text
balanced_zipfian:     500 read, 500 update
read_heavy_zipfian:   950 read, 50 update
update_heavy_zipfian: 50 read, 950 update
```

The runner must reject mixes that cannot be represented exactly by the selected
period.

`point_read_zipfian` must validate that every measured read hits.

`point_read_missing_in_range` must validate that every measured read misses,
once that row is added.

`range_scan_uniform` must validate:

1. expected row count per scan in the MVP, using the existing scan API;
2. ordered keys when a later adapter API returns scan evidence;
3. duplicate keys inside one scan result when a later adapter API returns scan
   evidence.

`sustained_ingest` must validate that completed write count matches the
reported operation count.

`range_scan_uniform` uses a prepared keyspace with `records` keys and a fixed
MVP scan width of `100`. For each operation, the selector chooses a positional
start in `0..=(records - scan_width)`, then creates a `Scan` with
`start = selected_start`, `limit = scan_width`, and `expect = scan_width`.
This intentionally matches the current ToyKV and RocksDB adapter behavior,
where `scan_bytes` and `do_scan` iterate in key order and apply `Scan::start`
and `Scan::limit` positionally. The MVP must include a cross-adapter test with
keys `0..255` and identical `Scan` inputs on ToyKV and RocksDB.

For mixed rows, `observed_mix` is valid when it matches the deterministic
prefix schedule for `completed_operations`. For example, a 50/50 mix with a
1000-operation period may stop after 1234 operations; validation checks the
first 1234 scheduled slots, not a rounded 617/617 split.

## 12. Gates

The current default ToyKV vs RocksDB steady-state gate compares:

1. `balanced_zipfian`;
2. `read_heavy_zipfian`;
3. `update_heavy_zipfian`;
4. `point_read_zipfian`;
5. `point_read_uniform`;
6. `point_read_missing_in_range`;
7. `range_scan_uniform`;
8. `sustained_ingest`.

Acceptance for a ToyKV storage-engine performance PR:

1. no required ToyKV steady-state row regresses by more than 5% OPS versus the
   previous ToyKV baseline;
2. p95 and p99 latency do not regress by more than 5% on required rows;
3. validation errors remain zero;
4. if RocksDB beats ToyKV by more than 10% on a row, profile that exact row
   before choosing an engine optimization.

The existing fixed-count CRUD gates remain active. A patch should not trade a
steady-state win for a durable CRUD regression unless the PR explicitly changes
the accepted benchmark priority.

## 13. Implementation Plan

### Phase 1: Schema And CLI

1. Add `--suite steady-state` while keeping `crud` as default.
2. Add steady-state CLI options and preset resolution.
3. Resolve `--samples` as the steady-state record count for the MVP. A later
   `--records` alias can be added only if it does not weaken existing CRUD CLI
   behavior.
4. Add JSON result fields while preserving the existing CSV columns.
5. Add parse/schema tests.

### Phase 2: Runner

1. Add the time-windowed closed-loop worker runner.
2. Add latency sampling and per-second throughput windows.
3. Add warmup, measurement, drain, and cleanup phase records.
4. Add a steady-state key-selection layer with seeded scrambled-Zipfian,
   uniform, and unique-sequential selectors.
5. Add deterministic operation-mix scheduling.

### Phase 3: MVP Workloads

1. Implement `point_read_zipfian`. Done in this implementation.
2. Implement `balanced_zipfian`.
3. Implement `range_scan_uniform`.
4. Implement `sustained_ingest`.
5. Implement `point_read_uniform`. Done in this implementation.
6. Add validation for each row.

### Phase 4: Comparison And Gate Tooling

1. Run ToyKV and RocksDB smoke rows.
2. Add `perf-gate` support for steady-state JSON rows. Keep legacy CSV parsing
   for existing OPS/p95/p99 comparisons only.
3. Document the ToyKV vs RocksDB command shape, including the explicit
   `--allow-database-mismatch` gate option.
4. Defer publishing resulting artifact names in the ToyKV benchmark report to
   the first ToyKV-side report update that consumes these crud-bench artifacts.

### Phase 5: Follow-Up Rows

1. Add `read_heavy_zipfian`. Done in this implementation.
2. Add `update_heavy_zipfian`. Done in this implementation.
3. Add `point_read_missing_in_range`. Done in this implementation.
4. `transaction_contention` is implemented in the CRUD harness, with
   serializable adapter support, deterministic tests, and bounded
   ToyKV/RocksDB smoke comparisons.

## 14. Open Questions

1. Should generic prepared datasets be rebuilt per row, or reused across rows
   in one benchmark invocation?
2. Should latency samples be exact by default for short smoke rows and sampled
   by default for long rows?
3. Should ToyKV checkpoint cloning become a generic adapter hook, or stay an
   engine-specific optimization?
4. Should a future scan-evidence API return keys for ordered/duplicate
   validation, or should deeper scan correctness stay in backend-specific tests?

## 15. First Acceptance Target

The first useful PR does not need to make ToyKV faster. It should make this
command produce valid, comparable ToyKV and RocksDB artifacts:

```bash
cargo run --release --bin crud-bench -- \
  --name steady_state_smoke_toykv_balanced_zipfian \
  --suite steady-state \
  --preset smoke \
  --bench balanced_zipfian \
  --database toykv \
  --samples 10000 \
  --seed 1 \
  --latency-sample-every 1 \
  --sync \
  --color never

cargo run --release --bin crud-bench -- \
  --name steady_state_smoke_rocksdb_balanced_zipfian \
  --suite steady-state \
  --preset smoke \
  --bench balanced_zipfian \
  --database rocksdb \
  --samples 10000 \
  --seed 1 \
  --latency-sample-every 1 \
  --sync \
  --color never
```

Both rows must have:

1. `validation.errors = 0`;
2. non-zero completed operations;
3. p95 and p99 latency values;
4. a measured throughput window;
5. clear skip/error behavior for unsupported backends.
