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
