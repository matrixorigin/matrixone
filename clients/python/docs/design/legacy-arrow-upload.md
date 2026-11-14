# Arrow input uploads on releases without native Arrow LOAD

Revision: `PR32671-ARROW-COMPAT-2026-11-13-r1`.
Implementation: [PR #32671](https://github.com/matrixorigin/matrixone/pull/32671).
This extends [the native upload design](analytics-arrow-upload.md); its original
approval does not approve this extension.

## Evidence and contract

MatrixOne v4.2.8 rejects the Arrow LOAD format with error 20300. The SDK currently
rejects uploads and 84 release analytics cases cannot exercise their positive
write paths. Parquet LOAD already exists on both supported server lines. A real
4.2 source control preserves uint64's maximum, positive/negative Decimal(38,12),
negative zero, Inf/NaN, NULL, microsecond time/timestamps, date, Boolean, UTF-8 and
binary bytes through uncompressed Parquet. That control is feasibility evidence;
the complete upload acceptance suite remains required.

`upload_arrow` and `upload_dataframe` must preserve their accepted scalar types,
row counts, precision checks, transaction/commit evidence and cleanup behavior.
The input API need not imply the staging file format. Native `arrow_load`
capability observations continue to describe native Arrow, independently of this
compatibility path. Explicit caller `unsupported`/`disabled` overrides keep their
existing rejection before staging or SQL.

This is a compatibility change across capability discovery, staging and the
async connection owner. It requires design review before implementation.

## Decision and alternatives

Choose the file format before consuming the input, using one short read-only
EXPLAIN checkout. Only the existing exact 20300/"format arrow is not supported"
classifier selects Parquet. A successful or otherwise inconclusive observation
keeps Arrow. A lost connection propagates its error. Permission, catalog,
missing-file and unrelated syntax errors never select a different format.

The alternatives are:

| Choice | Correctness and cost |
|---|---|
| Keep rejection | Preserves the current gap and cannot exercise supported release write semantics. |
| Backport native Arrow LOAD | Crosses extensive vector/import/storage ownership and requires a separate server feature/upgrade qualification. |
| Always stage Parquet | Avoids negotiation but changes the established main native path and its performance without necessity. |
| Stage IPC, then transcode after rejection | Replays owned input safely but adds a second file, combined disk reservation, another CPU admission and cleanup transitions. |
| Short probe before one staging file | Adds one read-only checkout/EXPLAIN; preserves the existing one-file/one-worker ownership model and main native path. Selected. |

## Owners and transitions

The capability module owns the exact rejection classifier. Extend its existing
checkout-local check with an explicit internal compatibility option; default
native-only callers retain their current behavior. Do not introduce a capability
cache, version allowlist, retry or fallback after a write error.

The upload manager owns this sequence:

1. Validate options and explicit overrides; validate the optional dependency and
   existing staging directory before the probe.
2. For async, acquire the existing bounded staging-operation permit. Acquire a
   short DB checkout, determine the format, and close it before CPU admission.
3. The existing producer creates one reserved file and consumes the reader once.
   IPC uses the current writer. Parquet uses version 2.6, no compression,
   dictionary encoding or statistics, and the same safely converted wire schema
   and sliced batches. Both use the existing byte-checking sink, including footer
   writes. Keep the current `mo_py_arrow_` prefix and select the matching suffix.
4. Acquire the final write checkout through the existing transaction owner and
   repeat the native format check on that checkout. A staged Arrow file cannot
   be sent to a checkout that now definitively rejects Arrow: reject before DDL.
   A Parquet file remains Parquet when the second checkout also accepts Arrow.
5. CREATE/append and exactly one LOAD use that transaction. Existing strict mode,
   UTC, primary-key admission, commit classification, rollback and session reset
   remain authoritative. There is no automatic retry of any write.
6. Existing sync `finally` and async `StagingOperation` cleanup reclaim the one
   file on every terminal path. Reader ownership remains unchanged.

Async probing must abort an acquired driver synchronously on cancellation/error,
then use the manager's existing tracked, bounded cleanup for connection close.
Capture the existing engine/generation and reject a late checkout or result after
disconnect/reconnect. Cleanup failure preserves the primary error. No await may
leave an untracked connection cleanup, and cleanup must never revalidate a
connection. Reuse `_abort` and `_cleanup`; add no executor or cleanup lane.

Content conversion failures may now follow a read-only probe. Every validation
still precedes DDL/LOAD; the API does not promise zero read-only SQL for an invalid
producer. This error-precedence/cost change is intentional.

## Bounds and cost

There is one file per upload, bounded by `max_staging_bytes` before every write,
including the footer. Conversion retains the current batch-size checks; caller
Tables/DataFrames may already materialize their input. IPC and Parquet footer
metadata scale with batches/row groups and columns; no global RSS cap is promised.

Async staging permits and CPU workers keep their existing limits of two; unlink
keeps its independent existing lane. The probe retains no CPU lease. A producer
releases CPU before waiting for the write checkout, so a single-connection stream
cannot create a CPU/DB wait cycle. Cancellation of blocked foreign input retains
its existing charged permit until the producer really exits.

Main gains one short read-only checkout/EXPLAIN per upload. Release gains the same
probe plus Parquet encoding, without a second input pass or file. Measure this
fixed cost separately from file/LOAD work; do not claim CI wall-time savings from
formatter or upload microbenchmarks.

## Validation and coverage map

| Scenario | Required proof |
|---|---|
| Main/native Arrow | Retain positive native format and complete existing scalar, transaction and cleanup oracles. |
| 4.2/Parquet | Run the former 84 gated positive cases with the same value, isolation, late-error, cancellation, commit and cleanup assertions. Record actual transport. |
| Capability facts | Exact native rejection remains independently observable; explicit overrides still reject before SQL/input. Unknown failures never choose Parquet. |
| Pool routing | Reject Arrow-to-unsupported checkout drift before CREATE; preserve Parquet on a later Arrow-capable checkout. |
| Staging | Read back both file formats; cover empty input, schema drift, footer budget/close failure, NULL and precision rejection, caller-reader ownership. |
| Async probing | Cancel/fail during checkout and EXPLAIN; prove transport abort, checked-out count zero, primary error preservation and subsequent capacity reuse. |
| Concurrency/lifecycle | Retain bounded workers/uploads, blocked reader cancellation, cleanup retry and disconnect/reconnect tests. No new worker or queue. |

Remove a release skip only after the corresponding positive path actually passes.
Keep the native-rejection control; it must not be reclassified as native support.
Run focused contracts first, then owning offline/analytics suites and the real
4.2/latest-main positive suites. Validate supported SQLAlchemy profiles, package
formatting/static checks and distributions after the implementation stabilizes.

## Review status

Design review and exact document digest are recorded separately before code is
changed. Implementation delivery requires all mapped proof and a final
`mo-self-review` with no unresolved blockers.
