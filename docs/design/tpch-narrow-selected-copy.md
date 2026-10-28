# Narrow selected-copy specialization

Owning issue: #30845. This is one independently measured execution candidate.

## Contract and scope

Preserve selected-row values bit for bit, order, duplicates, NULL/grouping state,
provenance, destination prefixes and allocation/error ownership. Change only
flat, non-NULL four/eight-byte payload copying. Keep the common union owner and
its metadata admission, allocation, length publication and cleanup authoritative.
No SQL, public API, format, configuration or retained-lifetime change is involved.

Current scalar loops form byte offsets and bounds checks for every row. The
candidate uses the existing typed shuffle to express destination bounds once.
The complete owner, including setup and admission, is the performance boundary.

## Implementation

Keep a direct scalar store for exactly one selected row in the existing
four/eight-byte cases. A single selection is one store and needs no typed views
or gather loop. For multiple selections, use a dedicated private narrow helper.
Construct bounded source/destination views there and copy using
uint32/uint64 representatives; these are physical bit copies for every logical
type of that width, including floating NaNs and signed zero. Keep the existing
wide helper untouched instead of extending its width dispatch, which exposed
wide-control costs in the earlier prototype. Do not introduce per-row dispatch,
allocation, a new eligibility pre-scan or view locals in the common stack frame.

Source and target views exist only during the existing call; ownership never
moves into a cache or registry. Existing preflight remains before destination
publication. Existing caller cleanup remains responsible after an allocation
failure; the helper neither allocates nor changes metadata.

## Alternatives and cost gates

Keep current scalar copies if they win complete-owner controls. Extending the
wide dispatcher changes unaffected wide work, while gathering batches through a
new generic fast-path preflight adds fallback cost. Both are excluded here.
Inspect generated common-owner frame/code before expensive measurements. Abort
this candidate if setup spills into common paths or a proven changed-path
slowdown cannot be removed within this scope; do not add a framework or tuning
threshold just to rescue a prototype.

## Single-row correction

The first prototype incurred about two nanoseconds of additional complete-owner
cost for one row because it called the bulk helper and admitted two typed views
for one scalar store. An independent longer run reproduced the cost after
removing dynamic width multiplication. Preserve the zero/one/multiple operation
distinction instead: the existing early return owns zero, a direct scalar copy
owns one, and the bounded typed gather owns multiple selections. There is no
empirically selected batch-size crossover or configurable threshold. Keep these
stores in the existing width dispatch so they add no common typed-view locals.

## Evidence and acceptance

First attribute four/eight-byte selected-copy exposure on clean current main,
including shuffle versus join callers. Correctness extends existing vector tests
for both selector widths, bits/types, windows, repeats, self-append, prefixes,
growth, NULL/grouping and provenance. Reuse unchanged consumer tests; run the
owning package once on final code. A public SQL semantic change is a defect,
not a result-file update.

Benchmark complete-owner tiny/empty, constant/NULL, narrow and wide controls.
Use unchanged local SF10 Q1-Q22 and local SF100 mixed-I/O Q9/Q10/Q19 when SF10
lacks exposure. Exclude cold runs, profile separately and execute/drain without
input/schema/row/result/checksum verification. Require attributable target gain;
block proven PR-caused slowdowns, including indirect ones. Confirm relevant
signals once; disclose unattributed shifts in unchanged paths as limitations.
Publish only if scoped acceptance passes, using the repository PR template and
exact measured pins. The broader issue remains separate from this slice.
