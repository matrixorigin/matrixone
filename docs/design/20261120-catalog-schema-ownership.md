# Catalog schema ownership in CN

Revision 1 for #32948. Related issues #32426 and #32963.

CN catalog replay builds a schema for every visible table. Keeping both the planner and engine attribute graphs increased the live heap in the replay fixture described in the PR. Keep one immutable planner graph per completed catalog version. Retain original constraint bytes and the sparse engine fields that the planner cannot reconstruct exactly, including physical row ID, compression and unusual type encodings. Keep the persisted catalog format and existing table visibility rules.

## Publication and reads

- Table rows carry identity and version. Column replay completes a schema under the catalog write guard. Column-only replay uses the existing equivalence check. Before CN admission, catalog replay completes and queued logtail callbacks apply in order.
- A canonical relation and the statistics collector may borrow the completed planner graph read-only. Stats captures the graph under the catalog read guard and releases the guard before scanning objects. Automatic refresh and explicit refresh without observations are fenced by the subscription generation. ANALYZE and explicit refresh with observations also check the schema version. The graph stays valid while a reader holds a reference, including across a later DDL, catalog GC or reconnect.
- `CloneTableDefForPlan` gives a caller a private header and optionally a private column pointer slice. Nested columns, defaults, maps, and keys remain shared. Copy the specific column before changing fields such as those used for legacy tiny-text recovery. Use an owned planner projection or a deep copy when writing nested metadata.
- Private engine projections rebuild mutable attributes and decode the original constraint bytes. A primary-key read selects the columns marked primary in catalog order, including a hidden physical key when marked primary. It reconstructs only those attributes and their mutable children. Legacy or transaction-local items keep the existing validating fallback until they have a canonical source.

## ALTER, replay failures and costs

ALTER materializes transaction-owned engine definitions and constraint bytes before detaching its relation from the borrowed source. Statement rollback restores the prior planner, source and version. Committed metadata reaches other readers through logtail. A failed replay uses the existing cancellation and retry path. Reset drops queued callbacks from the discarded attempt, and successful confirmation releases them after applying them under the queue mutex.

Borrowing allocates no schema copy. Shallow planning copies can allocate a column pointer slice. Fully owned projections still scale with schema width and nested metadata. Primary-key projection scans the canonical columns and sparse residuals, with allocations proportional to selected keys and their metadata. Empty stats collection skips per-column accumulators. Replay copies only groups that cross batch boundaries, and still materializes the executor result and retains the published schema.

Use projection oracles and mutation-isolation tests for schema fidelity, ALTER rollback and versioned stats tests for publication, and reconnect, durable restart and batch-boundary tests for replay.
