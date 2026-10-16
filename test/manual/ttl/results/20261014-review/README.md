# PR #30072 discovery review amendment

Tested implementation: `17df01f0a9` on the reviewed `b65fa0d` head/base `a53390c`. The following evidence-only commit does not change tested production/tests/scripts. `summary.json` binds the full implementation SHA, exact source hashes, Go/native artifact identity, selections, terminal events and raw-log hashes. `test-events.jsonl` contains scoped compact test evidence; verbose logs remain in the execution workspace.

The old-code negative control fails both `TestTTLDiscoveryCanceledHandoff/32` and `/64` because cancellation is ignored and the returned error is nil. The amended implementation returns cancellation with no candidates and zero query-result pool allocation. No production OOM or process RSS result is claimed by that probe.

- Normal validation: 16,847 parent/subtest entries across TTL, full parser/planner packages, CN registration and nine real-storage scenarios (34 integration entries).
- Full TTL package under race: 78 parent/subtest entries. Eight named cancellation/continuation/admission/revalidation tests each pass 100 repeats under race; counts are derived from terminal events rather than shell command success alone.
- Native boundary scenario: an ordinary non-TTL table and TTL table with metadata just over 16 KiB yield capped/NULL SQL output; a following small TTL table still expires the expected row, while the oversized table retains its row. One-row pages exercise all-NULL CASE projection. Existing cron/restart, stale discovery, rollback, tenant and concurrent SQL cases pass.
- 15-second checked-constraint fuzzing: 59,535 executions, pass.
- golangci-lint 2.14.0 on both changed packages: zero issues. Consumer-closure vet, `make err-check`, diff whitespace and verification-script syntax pass. Cleanup is preserved on query failure; no lint suppression, case skip or assertion weakening was added.

The design-first checkpoints `3f029bdbdc` and `92a3bce30f` precede the implementation commit. The design review record is explicitly an author self-review, tied to exact English/Chinese blob/SHA256 revisions; it does not impersonate a maintainer approval or resolve their review state. External design/implementation re-review remains required for merge.

Ownership and compatibility review: one result page closes before the next query/DELETE; copied candidates require count/byte credit; diagnostics retain bounded text, not original error graphs; one task closure owns a fenced cursor and non-waiting admission token. Completed attempts publish progress; canceled or unattempted candidates remain reachable. Existing policy wire encoding, DELETE/MVCC/tenant checks, scheduling concurrency and retry bounds stay compatible. The locked constraint check also caps its current payload to the credited candidate's exact byte length.

Limits are explicit in the design: TTL logical output/candidate/diagnostic bounds do not create a process-RSS/storage-I/O limit. Cursor restart/migration resets are accepted; no cross-restart completion SLA, multi-CN fault or scale throughput claim is made. The new native case reuses the package's existing one-CN fixture; a single minimally oversized value and a two-row control distinguish the SQL contract without a new fixture topology or workload.

Reproduction: prepare the repository's Go/native dependencies and run `bash test/manual/ttl/verify.sh`; it now uses the repository CGo wrapper and includes full TTL race validation. Exact stress selectors and repetition counts are in `summary.json`.
