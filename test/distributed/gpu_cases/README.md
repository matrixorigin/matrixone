# GPU vector-index BVT cases

These cases exercise the GPU-backed vector index plugins (CAGRA, IVF-PQ)
and **require a CUDA-capable build / runtime** (`MO_CL_CUDA` enabled). They
are kept out of the main `test/distributed/cases` tree so the standard
CPU-only BVT run is not gated on a GPU.

| File | Algorithm | Path | Drives |
|---|---|---|---|
| `vector_cagra.sql`  | CAGRA  | `gpu_cases/vector/` | sync CREATE INDEX, DDL surface, exact-match search, drop/recreate lifecycle |
| `vector_ivfpq.sql`  | IVF-PQ | `gpu_cases/vector/` | sync CREATE INDEX, DDL surface, exact-match search, drop/recreate lifecycle |
| `vector_cagra_quantization.sql` | CAGRA | `gpu_cases/vector/` | `QUANTIZATION 'float16'` and `'int8'` — option round-trips through the catalog + exact-match search |
| `vector_ivfpq_quantization.sql` | IVF-PQ | `gpu_cases/vector/` | `QUANTIZATION 'float16'` and `'int8'` — option round-trips through the catalog + exact-match search |
| `vector_cagra_async.sql` | CAGRA | `gpu_cases/pessimistic_transaction/vector/` | ASYNC build via InitSQL + ISCP CDC INSERT/DELETE/UPDATE into the tag=1 overflow |
| `vector_ivfpq_async.sql` | IVF-PQ | `gpu_cases/pessimistic_transaction/vector/` | ASYNC build via InitSQL + ISCP CDC INSERT/DELETE/UPDATE into the tag=1 overflow |
| `vector_cagra_load.sql` | CAGRA | `gpu_cases/pessimistic_transaction/vector/` | real 128-dim SIFT data: build over 10k rows, append another 10k via CDC, search both layers |
| `vector_ivfpq_load.sql` | IVF-PQ | `gpu_cases/pessimistic_transaction/vector/` | real 128-dim SIFT data: build over 10k rows, append another 10k via CDC, search both layers |
| `vector_cagra_sharded.sql` | CAGRA | `gpu_cases/vector/` | `distribution_mode 'sharded'` (2-way + 3-way) via `gpu_multi_simulation` — shard split + top-k merge, exact-match search |
| `vector_ivfpq_sharded.sql` | IVF-PQ | `gpu_cases/vector/` | `distribution_mode 'sharded'` (2-way) via `gpu_multi_simulation` — per-shard codebook + merge, exact-match search |
| `vector_cagra_replicated.sql` | CAGRA | `gpu_cases/vector/` | `distribution_mode 'replicated'` via `gpu_multi_simulation` — full-copy replicas, load-balanced search |
| `vector_ivfpq_replicated.sql` | IVF-PQ | `gpu_cases/vector/` | `distribution_mode 'replicated'` via `gpu_multi_simulation` — full-copy replicas, load-balanced search |

## Distribution modes on a single GPU (`gpu_multi_simulation`)

The `*_sharded.sql` / `*_replicated.sql` cases exercise the multi-GPU dispatch
paths (`distribution_mode 'sharded'` / `'replicated'`) on a one-GPU host. The
test-only session variable `gpu_multi_simulation = N` makes the index present
**N logical GPUs all mapped to physical device 0** (`[0,0,…]`), so the shard /
replica fan-out, merge, and per-rank locking run for real — see
`pkg/vectorindex.SimulateDevices`. The same value must be set for the
`CREATE INDEX` and the `SELECT` so build and search agree on the topology; each
case resets it to `0` at the end. This validates the **orchestration** logic,
not true multi-device behavior (separate VRAM / NVLink / real parallelism).

SHARDED needs enough rows: the splitter rounds each shard down to a multiple of
**32 rows** (word-aligned deleted bitset), so a shard with < 32 rows is empty.
The sharded cases therefore use **128 rows** (64/64 for 2-way, 32/32/64 for
3-way); the replicated cases keep the 20-row set since each replica is a full
copy. Per-rank index/dataset state is keyed by logical **rank** (not device id)
so the N copies coexist on device 0 instead of colliding.

## Layout convention

Cases that depend on **ISCP / async-index CDC** (they `CREATE INDEX ... ASYNC`
and `SELECT SLEEP(...)` to let the CDC consumer catch up) live under
`pessimistic_transaction/vector/`. The pure synchronous-build cases live in
`gpu_cases/vector/`.

## Sync vs async

Without the `ASYNC` keyword, `cagra_create` / `ivfpq_create` runs inline in
the user's CREATE INDEX txn (the session blocks until the cuVS model is
built) and the CDC task is registered `startFromNow=true`. With `ASYNC`, the
build SQL is stashed as `ConsumerInfo.InitSQL` and executed at the first CDC
iteration; later DML flows through the same CDC stream into the storage
table's tag=1 brute-force overflow.

## Determinism

### Quantization

The `*_quantization.sql` cases exercise the `QUANTIZATION` clause (vectors
stay `vecf32`; only the GPU index's internal storage type changes):
`'float16'` is a near-lossless bit-level f32→f16 conversion, while `'int8'`
is a **learned scalar quantizer** that samples the data for min/max and maps
the range to 256 levels (lossy). Because int8 resolution depends on the data
range, these cases use a tight, well-separated integer set (1..20) so each
value maps to a distinct level and the exact-match probe stays the unique
top-1. Do **not** reuse the wide-range sentinel data (100…800) from the
async/load cases under int8 — adjacent levels would collapse and the result
would not be reproducible.

CAGRA and IVF-PQ are **approximate** indexes — the graph / PQ build is
thread- and floating-point-order dependent, so only the **top-1 exact-match
neighbor** is guaranteed stable across runs. Every search in these cases
probes a vector that either exactly matches an indexed row, or is
overwhelmingly nearest to a single row living in the exact brute-force
overflow. Do not add `LIMIT > 1` assertions over approximate neighbors — the
lower ranks are not reproducible.

## Data

The `*_load.sql` cases use the real SIFT dataset shipped under
`test/distributed/resources/vector/`:
`sift128_base_10k.csv.gz` (10k rows, built into the main index) and
`sift128_base_10k_2.csv.gz` (10k more rows, appended via CDC into the
overflow). Each query vector is itself a dataset member, so its zero-distance
exact match is the deterministic top-1. The remaining cases use small
synthetic `vecf32(8)` data inline.

## Generating `.result`

The `.result` files are produced with mo-tester against a GPU-enabled MO. The
async cases include `SELECT SLEEP(30)` between each DML and its verifying
search to absorb the 10s ISCP sync interval.
