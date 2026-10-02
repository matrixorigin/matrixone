# Expression quality consolidation — revision 1

Continuation of #29249 and PR #29574; baseline9078a8bd, temporal closure commit95f58ac1d37. Broader audit covers342 recently changed production files. Candidate counts are discovery evidence, never deletion authority.

## Existing responsibilities and planned changes

TIMESTAMPADD DATE currently multiplies constant/dynamic units, output kind and initial wrapper type into eight row loops. Keep constant-unit parsing and dynamic metadata discovery exactly as supported today; resolve vector type once, execute one shared row loop through doCalendarInterval, and store DATE or DATETIME in the correctly typed slice. Preserve constantNULL/invalid-unit error timing, selected-row rules, dynamic per-row NULLs, warning counts, precision and result-wrapper reuse. Check SetTypeAndFixData errors before interpreting backing memory: denied DATE-to-DATETIME growth leaves the original vector intact. A denied-growth oracle must prove error return without writes or leaked allocation. Preserve dynamic unit discovery before date/count NULL suppression. No public metadata policy change, no retirement of dynamic units merely because ordinary SQL grammar rejects them. Avoid caching parsed dynamic units in a new per-batch array; memory cost would add state for an uncommon internal path.

Expression constants must validate literal source metadata once before allocating, then materialize through existing allocation-aware helpers and apply source metadata once. NULL and non-NULL branches currently duplicate validation/application and have different cleanup. Keep IsBin/runtime-domain application exclusive to non-NULL values as today; share only source validation/application and failure cleanup. Preserve string domain, charset, binary subtype and prepared source identity; preserve allocation-account selection. Do not combine SQL semantic type and transport type.

Zonemap pruning owns temporary vectors passed to EvaluateFilterByZoneMap; verify ownership and panic paths before changing cleanup. Consolidate test fixtures and membership/endpoint cases using existing binders and real execution oracles; do not replace actual residual evaluation with assertions on helper internals. Preserve per-iteration leak checks, varying denominators, NULL/unknown proofs, signed bounds, scale and DST controls.

For unused function/state candidates, trace every reference including function values, registration, interfaces, generated/remote callers and tests before removal. Remove exclusively retired tests together. Any independent public contract or required recovery path blocks removal until separately designed.

## Validation

Record named coverage mappings and pre/post checks for each retired capability. Run full changed owning packages with native CGo; mutations must detect metadata, mask/NULL/diagnostic and arithmetic failures after fixture consolidation. Race checks only where lifecycle/shared state changes. Report implementation/test/docs deltas separately and preserve initial closure evidence. Final reviewer must assess entire expanded PR; no performance claim without measurement.
