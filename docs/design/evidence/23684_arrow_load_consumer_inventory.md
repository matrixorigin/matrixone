# #23684 Arrow LOAD consumer inventory

This inventory closes the LOAD-reachable borrowed-vector surface. It does not
claim that arbitrary external-table SELECT or every stateful operator can
consume borrowed backing.

| Reachable boundary | Borrowed-input behavior | Ownership or copy boundary | Evidence |
| --- | --- | --- | --- |
| External Arrow reader | Publishes a generation-scoped batch; fixed and long-varlen payloads may be borrowed | each output vector retains its Arrow ArrayData/range backing; the reader may advance after publication | `pkg/sql/colexec/external/reader_arrow.go`, Arrow reader lifecycle tests |
| PreInsert | Reads source columns synchronously; generated, auto, const, row-id, and transformed columns are copied with `UnionBatch` | destination vectors are MPool-owned; no source lease is detached | `pkg/sql/colexec/preinsert/preinsert.go`, vector `UnionBatch` borrowed-source tests |
| Constraint, dedup, and lock operators | Read input during the call and retain no untracked source pointer | mutation routes through vector materialization; terminal batch owner keeps the generation alive | vector borrowed/COW tests and affected operator package tests |
| Conditional local dispatch / pSpool | Async fan-out is reachable when one source feeds multiple writers | `RetainedReadonlyViewWithMP` copies descriptors where required and retains immutable payload owners per slot; abort and late release are idempotent | `pkg/container/pSpool/copy.go`, `pkg/container/pSpool/sender_test.go` |
| Insert / MultiUpdate | Builds owned write batches with `UnionBatch`, or synchronously delegates a unique batch | copied destinations own MPool data; the direct S3 handoff is accepted only after an owned check | `pkg/sql/colexec/multi_update/insert.go`, multi-update tests |
| Sinker direct stage | Rejects borrowed or `NeedDup` vectors from `WriteOwned` | callers use copying `Write` when ownership cannot move | `pkg/objectio/ioutil/sinker.go`, `pkg/objectio/ioutil/sinker_test.go` |
| Vector mutation | A borrowed vector is readonly until materialized | mutators call `MaterializeOwned`; reserve/allocate/convert/swap is transactional | `pkg/container/vector/buffer_lease.go`, buffer-lease and allocation-account tests |
| Remote marshal / object encoding | Borrowed process pointers never cross the wire or storage boundary | canonical marshal copies fixed data and compacts/rebases only referenced varlen area | `pkg/container/vector/vector.go`, borrowed varlen marshal tests |

## Required invariants

- a next `External.Call` may release the reader's record only after every async
  consumer has retained a readonly view or made an owned copy;
- `NeedDup` is not a lease carrier and cannot authorize ownership transfer;
- `WriteOwned` is move-only and rejects borrowed backing;
- every materialization is charged to the statement allocation account;
- every retained backing remains charged by physical capacity until its final
  release, including cancellation and late consumer cleanup;
- unsupported or unknown mutators materialize before write.

## Deliberate exclusions

General external-table SELECT, arbitrary joins/aggregations, spill paths not
reachable from LOAD, and future Python UDF operators are outside this closure.
They must perform their own ownership audit before accepting borrowed Arrow
backing.
