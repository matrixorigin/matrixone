# Analytics integration and adversarial validation

These tests exercise the synchronous optional SDK APIs against MatrixOne. DuckDB is an independent file-result oracle, never the implementation backend. Install the SDK's analytics extra plus `pytest duckdb pandas polars 'moto[server]' boto3` in a suitable isolated Python environment. PyArrow 18 requires a supported Python version; the recorded matrix uses Python 3.12.

## Service prerequisites

Use an isolated **local test** deployment that supports native Arrow IPC LOAD. The source build tested here is identified in `evidence/local-validation.json`; a released server binary is not certified by these results. The client and both CNs must see local source/staging paths. Configure an S3-compatible stage as created by the tests. Account/permission tests require the local test administrator.

Defaults are local synthetic credentials `root` / `111`, database `mo_analytics_test`, CN1 port 6001, CN2 port 6002, metrics ports 7001/7002 and local S3 port 5000. Do not point these administrator/crash harnesses at a production deployment. Main pytest fixtures use `MO_HOST`, `MO_PORT`, `MO_USER`, `MO_PASSWORD` and `MO_DATABASE`; the explicitly local soak, chaos and second-CN fixtures retain their documented local defaults.

Create `mo_analytics_test` in the test deployment before running. For the full matrix, start a second CN process with a distinct UUID, frontend port, local file-service directory and metrics endpoint, sharing the same test TN/log/shared file service. This records same-host multi-CN behavior, not cross-host failover.

```bash
python -m moto.server -H 127.0.0.1 -p 5000
# From the repository root, after the two CNs and database are ready:
MO_ANALYTICS_ONLINE=1 MO_SECOND_CN_PORT=6002 \
MO_ANALYTICS_S3_ENDPOINT=http://127.0.0.1:5000 PYTHONPATH=clients/python \
python -m pytest clients/python/tests/analytics -o addopts='' -q --tb=short
```

`MO_ANALYTICS_ONLINE=1` is required for real-service cases. Missing the second-CN/S3 variables skips those cases; such a run must not be described as the complete recorded matrix. With all prerequisites, the recorded final 192-case runs had zero skips. The GitHub workflow may not reproduce the online cases automatically; local evidence is recorded separately from CI status.

Include existing regression cases:

```bash
MO_ANALYTICS_ONLINE=1 MO_SECOND_CN_PORT=6002 \
MO_ANALYTICS_S3_ENDPOINT=http://127.0.0.1:5000 PYTHONPATH=clients/python \
python -m pytest clients/python/tests/analytics \
clients/python/tests/offline/test_load_data.py \
clients/python/tests/offline/test_connection_hooks_offline.py \
clients/python/tests/offline/test_sql_escaping_edge_cases.py -o addopts='' -q
```

The full compatibility matrix uses separate environments for PyArrow 18.0.0 / 18.1.0 with SQLAlchemy 1.4.54, and PyArrow 21.0.0 with SQLAlchemy 2.1.4. Existing SDK code was unchanged between the tested source commit and the submission's refreshed upstream base; the feature patch was applied without modification.

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

The large-query child has an additional 128 MiB address-space limit. It verifies count/checksum over 1M rows with 256-byte payload. The same-limit buffered control must fail by MemoryError or a 30-second hard deadline. The recorded control hit the deadline. Initial failed calibration runs were retained and investigated; they were not silently excluded from the report.

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

## Recorded evidence and limits

`evidence/local-validation.json` contains dependency counts, source provenance, measured soak/memory results, crash outcomes, calibration findings and the final cleanup audit. It is a historical local run, not a claim that CI has passed or production qualification. Design documents under `../../docs/design/analytics-*.md` map concrete test cases to requirements.

- Full tests: 192 new cases, plus 49 selected existing regressions.
- Measured soak: 4960 operations after 1474 warmup operations; 8 workers / 90 seconds.
- Constrained streaming: 1M rows, checksum 499999500000, peak RSS 161.68 MiB, 3.63 seconds.
- Crash recovery: six scenarios passed; orphans observed where expected and explicitly reclaimed.
- Final audit: both CNs report zero pinned bytes; no test tables/accounts/staging files remain.
- Scope limits: same-host CNs, local S3 emulator, initial-block JSON inference, non-atomic source preflight, shared staging, unknown COMMIT reconciliation and SIGKILL artifacts. No cross-host/cloud-provider or multi-day endurance certification.
