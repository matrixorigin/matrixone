# Analytics integration and adversarial validation

These tests exercise the synchronous optional SDK APIs against MatrixOne. DuckDB is an independent file-result oracle, never the implementation backend. Install the SDK's analytics extra plus `pytest duckdb pandas polars 'moto[server]' boto3` in a suitable isolated Python environment. Use a Python version supported by the selected PyArrow release.

## Service prerequisites

Use an isolated **local test** deployment that supports native Arrow IPC LOAD. Record the server revision and binary identity with each run; SDK checks do not certify a released server binary. The client and both CNs must see local source/staging paths. Configure an S3-compatible stage as created by the tests. Account/permission tests require the local test administrator.

Defaults are local synthetic credentials `root` / `111`, database `mo_analytics_test`, CN1 port 6001, CN2 port 6002, metrics ports 7001/7002 and local S3 port 5000. Do not point these administrator/crash harnesses at a production deployment. Main pytest fixtures use `MO_HOST`, `MO_PORT`, `MO_USER`, `MO_PASSWORD` and `MO_DATABASE`; the explicitly local soak, chaos and second-CN fixtures retain their documented local defaults.

Create `mo_analytics_test` in the test deployment before running. For the full matrix, start a second CN process with a distinct UUID, frontend port, local file-service directory and metrics endpoint, sharing the same test TN/log/shared file service. This records same-host multi-CN behavior, not cross-host failover.

```bash
python -m moto.server -H 127.0.0.1 -p 5000
# From the repository root, after the two CNs and database are ready:
MO_ANALYTICS_ONLINE=1 MO_SECOND_CN_PORT=6002 \
MO_ANALYTICS_S3_ENDPOINT=http://127.0.0.1:5000 PYTHONPATH=clients/python \
python -m pytest clients/python/tests/analytics -o addopts='' -q --tb=short
```

`MO_ANALYTICS_ONLINE=1` is required for real-service cases. Missing the second-CN/S3 variables skips those cases; such a run must not be described as the complete matrix. The GitHub workflow may not reproduce the online cases automatically; local evidence is recorded separately from CI status.

Include existing regression cases:

```bash
MO_ANALYTICS_ONLINE=1 MO_SECOND_CN_PORT=6002 \
MO_ANALYTICS_S3_ENDPOINT=http://127.0.0.1:5000 PYTHONPATH=clients/python \
python -m pytest clients/python/tests/analytics \
clients/python/tests/offline/test_load_data.py \
clients/python/tests/offline/test_connection_hooks_offline.py \
clients/python/tests/offline/test_sql_escaping_edge_cases.py -o addopts='' -q
```

Compatibility checks should cover the minimum supported PyArrow and SQLAlchemy versions as well as current versions in separate environments. Record the actual dependency versions with each result.

## Real network faults

`mysql_fault_proxy.py` owns a local proxy and alters actual MySQL packets: drop a COMMIT success acknowledgement, drop before COMMIT or stall a result. It never records authentication payloads. Assertions reconnect directly to the server to distinguish a lost acknowledgement from a rolled-back transaction. Other cases deliberately corrupt only their owned staging files or inject cleanup failures.

## Concurrent/resource soak

```bash
PYTHONPATH=clients/python python clients/python/tests/analytics/run_soak.py \
  --staging-dir /tmp/mo-analytics-soak \
  --output /tmp/mo-analytics-soak.json \
  --warmup-seconds 30 --seconds 90 --workers 8 --rows 1000000
```

The harness owns UUID-named tables, limits its own FD soft limit to 64 and measures a mixed workload after warmup. It asserts no unexpected errors, no owned catalog leaks, zero checked-out connections/live Arrow allocations/pinned server buffers, and a 64 MiB RSS-growth threshold after warmup. Thread-phase virtual address headroom is 1 GiB because native allocators reserve virtual arenas; this is distinct from resident memory.

The large-query child has an additional 128 MiB address-space limit. It verifies count/checksum over 1M rows with 256-byte payload. The same-limit buffered control must fail by MemoryError or a 30-second hard deadline. Record which failure occurred and retain failed calibration results separately.

## SIGKILL and restart harness

Stop the local CN/TN/log test deployment and second CN first. The harness launches and kills only service/child PIDs it owns. Supply a launch configuration and runtime directory for an isolated local deployment; do not supply a production configuration.

```bash
PYTHONPATH=clients/python python clients/python/tests/analytics/run_chaos.py \
  --binary /path/to/test/mo-service \
  --config /path/to/test/launch.toml \
  --cwd /path/to/test/runtime \
  --work /tmp/mo-analytics-chaos
```

The six barriers cover server precommit/postcommit/active LOAD/streaming and client precommit/external-relation crashes. Active LOAD requires observing the server's live Arrow pinned-byte metric. After restart, assert exact persistent counts/sums. Record SIGKILL orphan files/catalog entries before explicitly cleaning only known-owned objects; an automatic reaper is not implemented.

## Evidence and limits

Keep run logs, JUnit XML, dependency/server identities and measured resource results outside the source tree. Record the tested revision, exact command, selected cases, terminal result and any skips. Historical reports are not current acceptance evidence. Design documents under `../../docs/design/analytics-*.md` map test cases to requirements.

The ownership regressions cover database changes before/after creation, same-named foreign objects, ambiguous CREATE/DROP outcomes, retained engines and cleanup/logger failures. Native cases require the real service; model/offline cases do not certify its protocol or catalog behavior.

The performance and crash harnesses are separate from ordinary functional tests. Report their actual parameters and observed outcomes when run; there is no fixed RSS or elapsed-time promise for all workloads. Scope remains same-host CNs, caller-managed S3 credentials/stages, initial-block JSON inference, non-atomic source preflight, shared staging and explicit reconciliation of unknown commits/SIGKILL artifacts.
