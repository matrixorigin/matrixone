# Preflight for issue #28140

This read-only check finds existing TIMESTAMP-dependent VIRTUAL generated columns whose read expression would exceed the planner's 65,536-node limit. Run it against the **system account** before rolling out the fix. It checks every account's table and column catalog rows, including hidden functional-index columns. Missing or undecodable metadata stops the check.

Build the repository's native CGo dependencies once, then run through its CGo test wrapper:

```bash
export MO_ISSUE28140_DSN='root:password@tcp(host:6001)/?tls=false'
make cgo
.agents/skills/mo-dev/scripts/mo-cgo-test -tags manual -run '^TestPreflightLive$' -v -count=1 ./test/manual/issue28140
```

Protect the DSN as a credential. The tool does not print it. A zero exit status and `PASS ... oversized=0` mean the catalog snapshot passed. `OVERSIZE ...` or `BLOCK: ...` means the corrected read contract must not be activated yet.

Freeze relevant DDL and rerun immediately before the **all-CN** cutover. Keep the receipt. If an oversized table is found, defer the upgrade until its owner approves a semantics-preserving schema/application rewrite. Converting a virtual intermediate to STORED without analyzing its time-zone semantics is unsafe. Recreate old prepared statements when the last CN is upgraded. After a rollback, run preflight again before retrying the cutover. The tool inspects a consistent catalog transaction; it does not keep DDL frozen for the operator.
