# DML diff fixture ownership and cost boundaries

`TestDataBranchDiffAsFile` in the [DML owner](../../pkg/tests/dml/dml_test.go)
shares one compatible fixture across independent diff scenarios. Its two-slot
admission bounds active scenarios without changing the package's cluster budget.

## Scenario lifetime

The parent admits scenarios in declaration order before launching goroutines,
placing the longer independent cases first. It releases a slot only after
`t.Run` returns, including the child's `FailNow` cleanup, and joins every admitted
scenario before releasing the shared fixture or dropping its database.

Each scenario owns its SQL connection and a fresh bounded context after
admission; waiting for a slot does not spend its SQL budget or inherit a sibling's
expired deadline. Scenario names, table namespaces, session isolation, nested
special-column cases and SQL/result assertions retain their own identities.
This is fixture-local admission, not a replacement suite scheduler.

## Independent data and result witnesses

The mixed composite-key round trip preserves updated, deleted and untouched
rows from both the original data and newly inserted data, with repeated key
components. Explicit preconditions distinguish all six cells; arbitrary seed
volume is not a substitute for these oracles.

Physical-block, PICK/key-chunk, output-limit and CSV fixtures have different
boundary witnesses. Reducing the mixed-mutation population does not authorize
shrinking those fixtures, deleting their sentinels or replacing their assertions
with a successful round trip. Sharing setup preserves independent failure
mechanisms rather than multiplying every scenario across every representation.

## Historical cost evidence

The [original cost record](https://github.com/matrixorigin/matrixone/blob/6cbe375b49696021d72778baa292a66287f080bd/docs/design/DML_TEST_COST_REDUCTION.md)
preserves the matched serial race measurements, complete subtest identity/result
comparison, local allocation context and limits. Those measurements concern the
recorded fixture, revision and host; overlapping child elapsed times cannot be
summed into a whole-CI saving. This document changes no tests and reports no new
runtime or performance validation.
