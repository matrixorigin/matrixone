# MongoDB connector validation and operational examples

Run package tests with `make test-mongodb-unit` and the complete local smoke with `make test-mongodb-e2e-local`. The E2E fixture is a single-node ReplicaSet using SCRAM-SHA-256, majority reads, a read-only source user, deterministic documents, a two-row cursor batch, and a random published port. Reports are written under `test/mongodb/reports/` and redacted before upload.

## Security defaults

The MongoDB SQL surface is enabled by default for the system account and every tenant. This does not grant network access: `allow-loopback` defaults to `false`, and both `allowed-host-suffixes` and `allowed-cidrs` default to empty. `CREATE MONGODB CONNECTION` therefore fails until the cluster operator supplies the appropriate hostname suffix or CIDR allowlist. The same policy is rechecked for every seed, SRV result, and ReplicaSet member before a driver socket is opened. Credentials and TLS material remain account-scoped `secret://` references.

Kubernetes is not an egress security boundary by itself. `NetworkPolicy`, CNI policy, routing rules, and cloud firewalls are defense-in-depth controls and may restrict a CN Pod, but they do not replace the MatrixOne endpoint allowlists. Without those controls a Pod may be able to reach cluster, VPC, metadata, or public endpoints; MatrixOne allowlists are still required even when the deployment supplies network-level restrictions.

## Frozen MVP contracts

- MongoDB 8.0.12 and official Go driver v2.8.0 are the PR baseline. The product implementation accepts normal ReplicaSet seeds and SRV subject to endpoint policy; TLS/SRV and minimum supported production versions remain release gates.
- BSON values mapped to an MO `JSON` column use MongoDB Relaxed Extended JSON. BSON int32, int64, finite double, strings, booleans, arrays and documents become their ordinary JSON equivalents, so MO JSON predicates behave like predicates over locally stored JSON. BSON-only or non-JSON values such as Decimal128, Binary, ObjectID and non-finite doubles retain their standard Extended JSON wrappers. BSON DateTime uses the relaxed `$date` string form for years 1970 through 9999, and the canonical `$numberLong` form otherwise.
- Numeric contract is `mongodb-aggregate-v1`: BSON double, `AVG(DOUBLE)`, one cursor, `max_parallelism=1`, then the existing target-compatible `FLOAT` cast. Every floating column uses the named numeric comparison contract and is excluded from `mongodb-aggregate-v1-exact`; that hash covers the exact key/count/source-batch subset with delimiter-safe hex encoding and an explicit NULL marker. This implementation enforces `max_parallelism=1` for every MongoDB external table. Local split remains a post-MVP gated task and stays disabled for this ingestion.
- Missing and BSON null both map to SQL NULL. There is no presence column in MVP.
- `try_null` is bounded per statement by `max-conversion-errors` and `max-conversion-error-rate` (the rate gate starts after 100 non-null try-conversion attempts); threshold violations fail the statement instead of silently degrading an unbounded scan.
- The indexed incremental timestamp must be declared `MONGODB_CONVERT 'try_null'`. Its BSON DateTime range is then pushed as an outward-rounded candidate and retained as an MO residual, so bounded ingestion does not pull the whole collection. A strict timestamp deliberately stays residual-only because pushdown could hide malformed source values outside the requested range.
- Incremental bounds are `[low, high)`. `low` includes the configured overlap and `high` includes a safety lag. A cursor failure restarts from the last committed watermark.
- MO does not yet expose an external-connection `USAGE` privilege object. Until it does, creating a MongoDB connection or a table mapping requires the account-admin role; ordinary users consume an existing mapping through normal table `SELECT` grants. Execution revalidates the tenant-scoped mapping ID/version, namespace, schema and connection generation before opening a cursor.

## Explicit MongoDB read operations

`ENGINE = MONGODB` tables expose a synthetic `__mo_query` column for an explicit, constant read selector. It is omitted from `SELECT *` and catalog table definitions, but can be named directly. The value must be strict MongoDB Extended JSON with exactly one top-level operation:

| Envelope | MongoDB path | Semantics |
|---|---|---|
| `{"filter": {...}}` | `Collection.Find` | Intersects the explicit filter with safe automatic MO predicate candidates; MO still evaluates ordinary SQL predicates as residuals. |
| `{"pipeline": [...]}` | `Collection.Aggregate` | Runs the pipeline as written, then adds a connector-owned mapped-field projection; ordinary SQL predicates run over the pipeline output in MO. |

```sql
-- A MongoDB filter outside the automatic SQL-pushdown subset.
SELECT device_id, ts, measurement
FROM mongo_events
WHERE __mo_query = '{
  "filter": {
    "ts": {"$gte": {"$date": "2026-07-27T10:00:00Z"}},
    "device_id": "pump-1"
  }
}';

-- The aggregate output paths must match this external table's explicit mapping.
SELECT device_id, event_count, avg_measurement
FROM mongo_events_aggregate
WHERE __mo_query = '{
  "pipeline": [
    {"$match": {"measurement": {"$type": "number"}}},
    {"$group": {
      "_id": "$device_id",
      "event_count": {"$sum": 1},
      "avg_measurement": {"$avg": "$measurement"}
    }},
    {"$project": {
      "_id": 0,
      "device_id": "$_id",
      "event_count": 1,
      "avg_measurement": 1
    }}
  ]
}';
```

The first version accepts one equality selector per scan. Prepared parameters remain gated on #27411. The query is bounded to 1 MiB, 100 nesting levels, and 100 stages. Pipelines use `allowDiskUse=false`; the statement context and driver operation timeout bound the initial command and every `getMore`.

The stage allowlist is `$match`, `$project`, `$set`, `$addFields`, `$unset`, `$group`, `$sort`, `$limit`, `$skip`, `$unwind`, and `$count`. Unknown stages/operators fail closed. Write stages (`$out`, `$merge`), cross-collection stages (`$lookup`, `$graphLookup`, `$unionWith`), metadata stages, and server-side JavaScript (`$where`, `$function`, `$accumulator`) are rejected before a MongoDB operation is sent. The external table mapping remains the authorization boundary; this is not an arbitrary command interface.

When explicitly selected, `__mo_query` is populated on every returned row as canonical relaxed Extended JSON (insignificant input whitespace is not preserved). Diagnostics expose only the operation kind and a digest prefix; errors, metrics labels, E2E reports, and EXPLAIN output do not include the raw query body.

## Backfill, cutover and rollback

1. Run `sql/key_conflict_audit.js` against live data and every archive segment for the whole old-key period. Record gaps and the earliest recoverable timestamp; the old MO target cannot prove that no site was overwritten.
2. Create the `(device_id, site_id, window_start)` target. Install `sql/incremental_ingest.sql` for live ingestion and use `sql/bounded_backfill.sql` for bounded site/day history slices; each stored-procedure call commits its target rows and checkpoint together. Pause/retry only from a committed checkpoint. The SQL task body contains one `CALL`, because a multi-statement task body is not transactional while a top-level stored procedure is.
3. Export the same versioned columns from both shadows, sort by the row key, and run `compare_results.py`. Do not present the legacy Python `_row_hash` as the new `exact_row_hash`: omit it from both exports, or include its expected change as an exact per-row rule. Exact columns must match; all eight v1 floating columns use the explicit tolerance list (override it only with a reviewed contract change). Expected mismatches require a JSON rule containing the exact `key` array, `column`, `expected`, `actual`, and a named `rule`; unused or stale rules fail the comparison, so there is no broad ignore path. Duplicate result keys also fail instead of being silently overwritten.
4. Measure MongoDB CPU/lag/connections, scanned rows/bytes, CN memory/CPU and end-to-end p50/p95. Do not cut over until a 3-million-row bounded run completes inside the agreed five-minute budget without exceeding source limits.
5. Enable the account feature gate for shadow reads, then move only the scheduler. Old and new writers must use separate targets and watermarks. Stop the legacy ingestion workers only after the required connector gate, history repair, differential, failure recovery and owner sign-offs pass.
6. Roll back by pausing the new scheduler, waiting for its transaction to finish, restoring the old scheduler from its saved watermark, and validating that only one generation advances. Retain both targets through the rollback window.

Production sign-offs, 30-run CI stability, source/archive backfill, performance evidence and a cutover rehearsal are external operational gates; repository code cannot mark them complete on behalf of their owners.
