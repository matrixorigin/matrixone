# PR #30072 CI repair

The failed run checked `7dc0daf091ad43ee870470da14d2f1ea478c6745` on Go 1.27.1. Ubuntu UT and coverage used amd64; SCA used arm64.

- SCA stopped in `make err-check`: TTL production code constructed errors with `fmt.Errorf`. Both TTL production files now use `moerr`; the repository gate passes without changing or suppressing the rule.
- UT and coverage could not compile `pkg/logservice`: `gossip_fixture_test.go:130` still used the old `allocatedPorts.allocate(probe)` signature. The same compile failure was reproduced at clean base `3e50e1b32d8da61bbbf70c89989fb08b671809de`. Current main already repairs this caller and preserves the listener range outside ephemeral ports.
- UT also failed `TestTTLRandomizedRowsRollbackAndConcurrentWorkers` at line 235 with `ErrLockWaitTimeout`. A one-second wait limit deliberately interrupts a competing transaction; TTL now retries local and remote lock-wait timeouts by reopening the entire transaction and revalidating metadata. The original eight-attempt bound, backoff, caller cancellation and per-table/run budgets remain effective.
- `TestTTLRetryLockWaitTimeout` injects joined local/remote errors and proves recovery, exhaustion at eight attempts, cancellation after one attempt, and non-retryable controls. The full TTL package passed normal and race modes; the exact regression passed 100 times under race. All eight real-storage TTL scenarios (33 parent/subtest entries) passed.

`summary.json` records the local base, named selections, terminal events and verbose-log hashes. `test-events.jsonl` retains compact execution evidence. These are local scoped results, not a claim that the complete remote CI passed. The original failed run remains linked for attribution; the repaired PR must complete its newly triggered checks.

To reproduce with the repository Go/native dependencies:

```bash
make err-check
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=120s ./pkg/sql/ttl
.agents/skills/mo-dev/scripts/mo-cgo-test -race -count=1 -timeout=120s ./pkg/sql/ttl
.agents/skills/mo-dev/scripts/mo-cgo-test -race -count=100 -timeout=120s -run '^TestTTLRetryLockWaitTimeout$' ./pkg/sql/ttl
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=240s -run '^TestTTL' ./pkg/tests/sqlintegration
```

The integration command requires local socket access. A sandbox `socket: operation not permitted` is an environment failure and was not counted as passing evidence.
