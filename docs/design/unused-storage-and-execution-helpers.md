# Retire unused storage and execution helpers

Version 2, 2026-10-22. Owner: engineering-quality issue #29249.
Implementation: `cleanup/29249-local-batch-20261022`.
Base: `75ae9a79d626622e6ccec0bcb28412f5fcf84c7f`.

## Boundaries and evidence

Repository-wide source and import checks identify these disconnected components:

- `pkg/fileservice/interval` has no importers, including tests outside the package.
  Its interval tree and overlap checker can be removed with their own tests.
- `pkg/common/spool` and `pkg/common/async` also have no importers. Remove their
  disconnected queue/future implementations and self-tests. The active pipeline
  spool in `pkg/container/pSpool` remains unchanged. Example programs are entry
  points and are excluded from this no-importer retirement rule.
- Multipart upload's `getParallelUploadPool` is called only by its singleton test.
  AWS/COS upload implementations acquire the shared channel semaphore instead.
  Remove the unused pool, its initialization and the singleton test. Change the
  two SDK tests that inject the unused pool to limit the actual semaphore to one
  slot while uploading two parts. Preserve completion and part-count assertions.
- `fscache.CachingCapacity` is used only by its own test. Keep the live capacity
  function abstraction and constant-capacity constructor; remove the unused timed
  cache, its atomics and its test.
- `mpool.PtrLenFromSlice`, `mpool.MakeSliceArgs`, and
  `colexec.ExpressionExecutorsRetainedBytes` have no callers. Remove these entry
  points while retaining their live allocation and accounting implementations.

These are internal implementation utilities, without a documented SDK, protocol,
configuration or persistence contract. Preserve error codes and live APIs.
Keeping stubs or moving the unused implementations into tests would preserve
maintenance obligations without a consumer. Deleting the disconnected paths
introduces no replacement state or abstraction. The interval-package deletion
triggers the production-line size gate.

## Ownership and validation

Production multipart methods already join their workers before returning. SDK
tests call them synchronously with a bounded context and restore the shared
semaphore afterward; the package test timeout bounds an actual deadlock. These
tests are sequential and must remain so while overriding shared state.

Require exact reference/import checks, successful owning-package and consumer
compilation, relevant normal/race tests, and incremental vet/configured lint.
Exercise upload success, cancellation, read/upload failure, abort and buffer
admission using existing SDK tests. Delete tests only for removed components;
preserve distinct live-path oracles. No production execution path changes, new
allocations or runtime performance gains are claimed for this retirement.
