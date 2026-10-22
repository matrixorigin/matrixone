# Default identity projection for #23728

Issue: [matrixorigin/matrixone#23728](https://github.com/matrixorigin/matrixone/issues/23728).
Implementation PR: the PR carrying this document.
Revision: v2, 2026-10-22; publication of the accepted v1 design without a scope change.
Design review: **GO / PASS**, `gpt-6.1-sol` with `xhigh` reasoning; no blocking findings.

## Problem and scope

`Operator.ExecProjection` is required by the execution interface, but 58 of its 65 production implementations in `pkg/sql/colexec` only return `input, nil`.
All 58 receiver types already embed `vm.OperatorBase`; the other seven perform real projection work.
The clean-code goal is to remove repeated identity implementations while preserving execution behavior.
The design gate applies because the refactor spans at least five production packages.

## Design and invariants

Add `ExecProjection(_ *process.Process, input *batch.Batch) (*batch.Batch, error)` to `*vm.OperatorBase`, returning `input, nil`, and remove exactly the 58 identity overrides, including `MockOperator`.
Use Go's existing embedded-method promotion; keep the `Operator` interface and `vm.Exec` dispatch unchanged.
Keep explicit implementations on `External`, `Fill`, `Group`, `MergeGroup`, `MongoScan`, `TableScan`, and `ValueScan` unchanged.

- The default returns the exact input pointer and nil error for nil, empty and nonempty batches, without mutation, copying, allocation, retention or cleanup.
- Every removed-method receiver continues to satisfy `vm.Operator`; an explicit override continues to win interface dispatch.
- Cancellation precedes `Call`; Call errors and `ExecYield` bypass projection; other results retain existing projection and analyzer behavior.
- `External` retains hidden Iceberg read-column masking, including its no-ProjectList path.
- Prepare, Reset, Free, expression executors, allocation accounting and projected-batch ownership remain with their current owners.

## Ownership, compatibility and cost

The default owns no state or resources and introduces no waits, concurrency, lifecycle transitions or accumulation.
The flow remains `vm.Exec -> Call -> ExecProjection -> analyzer.Output`, with existing early returns.
Concrete operator pointers retain their method signature through promotion; compile all affected consumers to detect ambiguity or missing methods.
Execution already requires a nonnil operator; invoking a promoted method on a nil outer operator pointer can differ from a local identity method and must not be a relied-on caller contract.
There are no changes to operator layouts, wire formats, persistent state, SQL behavior, configuration, security or topology; no migration or rollout flag is needed.
Rollback is a source revert and rebuild.
The default has O(1) work and zero explicit allocation, copying, I/O or synchronization; no runtime performance improvement is claimed.

## Alternatives and accepted tradeoffs

- Keeping the copies preserves behavior but leaves the duplication and future operator boilerplate.
- Moving all projection state/evaluation into the base expands ownership and dependency changes: the shared evaluator already lives in `colexec`, which imports `vm`; special masking still needs an override.
- Extracting optimizer projection nodes or adding optional runtime interface detection expands plan or dispatch behavior beyond this cleanup.
- Default promotion removes the copies at their existing common owner while retaining seven specialized wrappers and existing projection storage.

## Validation and acceptance

1. Compare the complete deletion inventory and final diff: exactly 58 identity bodies removed, all receivers embed the base, all seven real implementations unchanged, and import cleanup is limited to unused imports.
2. Reuse the existing VM test fixture: let the ordinary operator inherit the default and isolate the custom hook in an embedding projection fixture. Check nil/empty/nonempty pointer identity, row counts/analyzer output, explicit override dispatch, cancellation, Call errors and Yield suppression.
3. Prove nonempty focused test selection and run affected existing and owning-package tests in normal and race modes; record any short-mode/tag exclusions accurately.
4. Successfully compile every removed-method owning package and its tests, the seven specialization packages, and the real construction consumer `pkg/sql/compile`; reuse successful test compilation where equivalent.
5. Retain existing specialized-path proof, including `TestIcebergDeleteApplyMasksHiddenReadColumns` and `TestInlineFilter`, without adding service fixtures.
6. Run gofmt, incremental `go vet` and configured lint over changed packages and directly affected consumers, using the reviewed base and supported Go/CGo mode with provenance-verified native artifacts.

BVT, multi-CN, restart, GPU, scale and benchmark work add no distinct proof because those contracts are unchanged.
Acceptance requires terminal validation results, a complete consumer-compilation inventory, no new runtime state, no unresolved final-review blocker, and alignment with these invariants.
Moving state, changing dispatch, removing a non-identity override or discovering a relied-on nil-receiver caller requires design re-review.
