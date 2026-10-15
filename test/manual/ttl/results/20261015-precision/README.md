# TIMESTAMP cutoff review correction

Addresses review 5521258184 and inline 4273999274 on PR #30072 at
`4455ed39154ba46ff4f04339da191e22460e798e`.

Invariant: cleanup must not delete a row until `event_time + retention <= now`.
Mixed TIMESTAMP/DATETIME comparison rounds the cutoff to the column scale.
The fix widens the column to DATETIME(6), under the existing UTC transaction
session, and compares two DATETIME(6) values. The cast implementation does not
round at target scale 6. Formatting floors sub-microsecond invocation fractions.
DATE, DATETIME, NULL, quoting, transaction locking and identity checks retain
their existing contracts. This may change predicate pushdown opportunities;
no throughput improvement is claimed. Discovery and DELETE quotas are unchanged.

## Regression and validation

`TestTTLTemporalMatrix/fractional_timestamp_{0,3,6}` uses the existing embedded
cluster: nine four-row tables, dropped after each successful case. Each precision
checks invocation just before, exactly at and after the central row's expiry;
the neighboring stored values and NULL independently check survivor IDs and
affected-row count. Long retention prevents the real-time cron from expiring
these future rows. No sleeps or additional service fixture were introduced.
The old production source fails precisions 0 and 3 (two deletions instead of one);
precision 6 is a control, including a sub-microsecond invocation fraction.
`before.txt` retains the terminal failure excerpt. The final test adds successful
per-case table cleanup to avoid exceeding the discovery candidate window when
running the original temporal matrix in the same database.

Reproduce from the repository root with the configured Go/native toolchain:

```sh
.agents/skills/mo-dev/scripts/mo-cgo-test ./pkg/sql/ttl ./pkg/tests/sqlintegration -run '^TestTTL' -count=1
```

`after.txt` is the complete successful command output. `manifest.json` binds
source and log SHA256 hashes. Both changed packages pass configured golangci-lint
and go vet; `git diff --check` passes. Existing verification script already selects
these tests. No new shared state, lifecycle or concurrent protocol was introduced;
prior race evidence remains applicable to unchanged worker ownership and retry
behavior. The self-review scope is the predicate, its SQL consumers, updated
expected SQL, regression cases and corresponding design/evidence text. Both
language designs now describe the actual predicate. No broad design change or
independent approval is claimed.
