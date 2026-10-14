# Temporal execution consolidation — revision 1

Scope: engineering-quality issue #29249, baseline 9078a8bd87af49ea06f52e09d64e7740122aea61. Refactor existing temporal arithmetic; no new feature or compatibility path.

## Existing owners and contracts

The binder owns SQL return metadata; registered functions own vector adaptation, selection, NULL propagation and per-evaluated-row warnings. doCalendarInterval and doTimeInterval own checked arithmetic. String parsing/formatting remains distinct from typed values. DATE, DATETIME, TIMESTAMP, TIME and integer-date adapters have different conversions and must retain those conversions.

## Planned reduction

Consolidate each typed ADD/SUB vector pair into one local directional implementation, preserving registered entrypoints. Route scalar directional adapters through existing checked arithmetic owners; retire redundant scalar wrappers where callers can use the common owner directly. Preserve timestamp timezone conversion, integer-date unit restrictions, precision and warning counts. Do not introduce a generic executor or new state.

Consolidate scalar arithmetic tests around canonical owners and retain adapter-specific conversion, metadata, selection, NULL and warning assertions. Replace repeated manual vector fixtures with existing function test utilities and cleanup. Consolidate binder TIMESTAMPADD metadata cases while retaining result-column metadata validation.

TIMESTAMPADD DATE currently duplicates eight execution loops and independently narrows output types. Investigate production callers before removal: parser probes accept keyword constant units but reject ordinary and quoted dynamic units. This alone does not prove internal dynamic calls unreachable. Any simplification must retain supported internal callers or prove their absence. No metadata behavior change is approved by this revision.

## Alternatives and limits

The existing binary NULL-on-error template is unsuitable: it swallows fatal errors and executes constant callbacks once, conflicting with per-row warning counts. Reuse arithmetic owners, not that template. Do not merge TIMESTAMPADD string formatting with DATE_ADD string formatting: their fractional/date parsing policies differ.

## Validation and acceptance

Map retired tests to retained independent literal oracles before deletion. Cover add/sub direction, MinInt64, boundary overflow, selected-out invalid rows, NULL inputs, exact warning counts, timestamp timezone and integer-date restrictions. Run the owning function and planner packages through the CGo wrapper. Use targeted mutations for direction, selection and metadata sensitivity. Report implementation/test/document additions and deletions separately; no performance claims without measurements. Existing first closure already passes the full function package and three targeted mutation checks.
