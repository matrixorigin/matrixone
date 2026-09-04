# Arrow LOAD observability and rollback

Arrow LOAD uses the existing MatrixOne metric registry and operational views.
Do not create a dedicated Arrow dashboard. Put storage/range panels in the
existing FileService view and conversion/publication panels in the existing
Pipeline view when dashboard integration is deployed.

## Rollout controls

The zero-value CN frontend settings are fail-closed and default to disabled.
A release or deployment profile should begin with:

```toml
[cn.frontend.arrow-load]
enabled = false
s3-enabled = false
distributed-enabled = false
force-materialize = false
```

Repository development/BVT launch profiles may explicitly enable local and
distributed Arrow LOAD so the SQL case is runnable. That test opt-in does not
change the zero-value production default or enable the separate S3 gate.

The current implementation charges every raw range, cache pin, and decoded
Arrow allocation to the shared statement account and limits each cache pin to
4x amplification. The design's additional cross-worker aggregate pin quota is
still a release blocker. `mo_arrow_load_pinned_bytes` observes process usage; it
is not an admission controller and must not be treated as that missing quota.

Enable local File/Stream LOAD first, then object storage, then distributed
execution. During rollback, stop admitting new Arrow statements by disabling
`enabled`; allow already admitted statements to drain or cancel them through
the normal query lifecycle. Do not switch an executing statement to a
different object generation or conversion policy.

`force-materialize=true` leaves Arrow LOAD enabled but disables borrowed Arrow
backing for statements compiled after the setting is applied. Use it to isolate
ownership/pinning incidents or as a temporary rollback from the borrow path.
The compile snapshot is carried to remote scopes, so a statement never mixes
borrow and forced-materialize policy. Restart or otherwise reload every CN with
the same setting before using this control in a distributed cluster.

## Metrics

All labels are bounded and contain no object path, credential, account name,
query text, or user data.

| Signal | Meaning |
| --- | --- |
| `mo_arrow_load_objects_total{outcome}` | object or object-shard open attempts |
| `mo_arrow_load_shards_total` | successfully opened record shards |
| `mo_arrow_load_records_total` | accepted non-empty IPC record batches |
| `mo_arrow_load_batches_total`, `mo_arrow_load_rows_total` | published MO batches and rows |
| `mo_arrow_load_payload_bytes_total{kind}` | eligible, borrowed, and retained-capacity bytes |
| `mo_arrow_load_copy_bytes_total{layer}` | materialized Arrow-to-MO payload bytes |
| `mo_arrow_load_conversion_columns_total{mode}` | borrowed/materialized column decisions |
| `mo_arrow_load_fallbacks_total{reason}` | pin-amplification or alignment fallback |
| `mo_arrow_load_errors_total{category}` | stable error category |
| `mo_arrow_load_phase_duration_seconds{phase,outcome}` | `open`, `next_record`, `convert`, `wire_budget`, and `publish` latency |
| `mo_arrow_load_pinned_bytes` | current live range/decoded capacity |
| `mo_arrow_load_pinned_bytes_high_water` | process-lifetime pinned high-water mark |

Useful PromQL:

```promql
sum(rate(mo_arrow_load_errors_total[5m])) by (category)

sum(rate(mo_arrow_load_payload_bytes_total{kind="borrowed"}[5m]))
/
clamp_min(sum(rate(mo_arrow_load_payload_bytes_total{kind="eligible"}[5m])), 1)

sum(rate(mo_arrow_load_copy_bytes_total{layer="arrow_to_mo"}[5m]))
/
clamp_min(sum(rate(mo_arrow_load_payload_bytes_total{kind="eligible"}[5m])), 1)

histogram_quantile(
  0.99,
  sum(rate(mo_arrow_load_phase_duration_seconds_bucket[5m])) by (le, phase)
)
```

## Alert candidates

- page on a sustained `resource_exhausted`, `internal`, or `object_changed`
  error rate above the environment's LOAD baseline;
- warn when pinned bytes remain high while published rows do not increase;
- warn when pin-amplification fallback or Arrow-to-MO copy ratio changes
  materially after a rollout;
- compare open/convert/publish p99 with ordinary LOAD latency before enabling
  the next rollout stage.

Thresholds belong to deployment owners and must be derived from representative
workloads. This repository does not hard-code cluster-specific alert values.

For a local canary/release rehearsal, use these reference gates until deployment
owners replace them with workload-specific values:

- zero partial commits, successful statements with internal errors, leaked
  allocation-account bytes, or pinned bytes that fail to return to baseline;
- `internal` errors page on the first occurrence; `resource_exhausted` and
  `object_changed` warn on any new rate and page when sustained for 5 minutes;
- Arrow p99 may not regress more than 20% and throughput may not fall below 90%
  of the accepted control for the same data, topology, cache state, and binary;
- after the last Arrow statement terminates, pinned bytes must return to the
  pre-run baseline within 60 seconds.

These are rollout acceptance gates, not universal production SLOs. Compare
against ordinary LOAD/INSERT and the forced-materialize control before advancing
local file, S3/stage, and distributed stages.

## Triage

1. Check the error category and the failing phase; retain the query/trace ID
   from normal logs, not a source path metric label.
2. For `object_changed`, verify object-store versioning and whether a producer
   overwrote or deleted a key, or deleted the planned version, between planning
   and execution. Conditional `404`/`NoSuchVersion` and precondition failures
   are both classified here. The statement must retry as a whole against a
   newly planned object set.
3. For `resource_exhausted`, compare retained capacity with borrowed payload,
   copy ratio, statement memory, and FileService cache pressure.
4. For stalled cancellation, verify no new rows are published and pinned bytes
   return to the pre-query baseline after all retained consumers release.
5. If correctness, lease accounting, or mixed-version behavior is uncertain,
   disable Arrow admission and use the existing Parquet/INSERT path while the
   incident is investigated.
