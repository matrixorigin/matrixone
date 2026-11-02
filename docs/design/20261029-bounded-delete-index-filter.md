# Bounded DELETE index-maintenance runtime filters

Issue: https://github.com/matrixorigin/matrixone/issues/31618

Design baseline: `fc6882279b3b55e4dab8caef19b712b663c5eb66`.
This is a conservative, partial optimization;
opposite join orientations and independently distributed execution retain their
existing behavior. This change does not close the whole issue.

## Problem and choice

The reproduced DELETE selects 1,000 base rows but its outer secondary-index
maintenance join scans 97,000 entries. An inflated intermediate join estimate
causes the ordinary runtime-filter selectivity test to reject the filter.
The bounded source still supplies at most 1,000 distinct values of its stable
index-key expression: intermediate joins can duplicate these values without
creating new ones when the source side is preserved.

Global cardinality corrections previously changed unrelated query behavior.
Generic candidate analysis added ownership and execution-placement complexity.
A new streaming lookup algorithm also has material costs: on the same million-row
fixture, 1,000 exact keys read 28 blocks together but 404 blocks when shuffled
and split into batches of 64. Reopening readers between batches additionally
requires a stable statement snapshot that retains earlier transaction writes.
These alternatives are not part of this change.

Use the existing DELETE binder, runtime-filter generator, exact-key codec,
HashBuild, scans, Lock and MultiUpdate. Add no storage or execution algorithm,
wire fields, configuration, materialization, retry or lifecycle mechanism.

## Ownership and admission

1. The DELETE binder records its generated regular-index scan binding tags and
   resolved table IDs in a query-builder-local map. This is construction
   provenance, not an assertion that an arbitrary name identifies an index.
   Optimization must revalidate the actual tag, table and physical column.
   The map also records whether the original RF generator actually rejected
   this probe at one of its selectivity thresholds. Only that original pass
   writes this decision provenance, binding it to the exact JOIN and probe
   node identities as well as tag/table identity. The late pass rejects
   replaced or reshaped candidates. Later statistics can change without
   changing the historical reason for the missing pair. The map is not
   serialized or retained by cached execution plans.
2. Complete normal RF generation, projection cleanup and the existing
   `forceJoinOnOneCN` pass first. Only then consider additional filters.
3. Admit only a single-step DELETE whose complete live root is MultiUpdate
   with its existing optional Lock wrapper and a simple index-maintenance
   input chain. All other query shapes fail closed. Require an independently
   existing, live `Stats.ForceOneCN` flag and a final TP/AP_ONECN execution
   classification. Do not introduce a new localization constraint. Reject
   forced distributed execution and special distributed index regions.
4. Only an INNER join already using a registered direct index scan as probe
   and the bounded source as build is eligible. Require non-shuffle and
   `HashOnPK=false`: only the distinct-collecting build path can use a distinct
   bound. Preserve hints and predeclared filters. Any existing filter on the
   JOIN's build/probe lists or target TABLE_SCAN's probe list excludes the
   candidate. The probe has no SQL filters, block filters, projection,
   pagination or effects that a new filter could suppress.
5. Require one resolved equality ON condition. Its probe operand is the bare,
   complete actual index primary-key column, with no cast. Reuse
   `sqlEqualityCompatiblePrimaryKeyColumnPositions` and
   `sqlEqualityJoinUsesOneIdentityDomain`; RF encoding eligibility alone is
   not a uniqueness proof. The source key consists only of direct source
   columns and the binder's pure `serial`/`serial_full` composition. All columns
   must trace to the same unprojected base scan and match its declared types.
6. The source scan has a positive uint64 literal LIMIT. Through INNER and
   source-preserving LEFT/RIGHT maintenance joins, that scan's values can be
   copied or removed, not invented. Every sibling is a distinct, registered
   direct index scan; no source projection, user join, shared reader, step,
   recursive input or union is accepted. Verify a single incoming edge for
   every scan/join on this chain. Existing RF dependencies on chain scans must
   be produced by their own direct parent join, not another subtree. The LIMIT
   bounds distinct key values, not build rows, retained bytes or physical I/O.

`SetForceScanOnMultiCN` has no non-test call site in the complete baseline
repository. Its existing tests must keep that diagnostic hook fixed throughout
plan generation and compilation/reuse. Reject it when enabled at planning.
Introducing a production caller would invalidate this locality premise and
require compile-time admission or an execution-time delivery/fallback design.
Ordinary cost/execType hints preserve ForceOneCN and must not remove the premise.

## Generator and execution contract

Separate the existing recursive traversal from its single-node generator. The
normal traversal calls that generator with no bound and retains its behavior.
The late maintenance pass invokes the same single-node generator only for the
proven candidates, with the source-key bound.

Only retry a selectivity rejection recorded from the original generator. Do
not infer its earlier decision from late statistics: the existing intervening
recalculation can make the original cost pass without regenerating the filter.
Keep all other guards, including the five-million build-row guard, exact
encoding checks, hints and actual runtime IN budget. The proven bound must fit
that budget and itself pass both selectivity conditions. Use the bound itself,
not the minimum of that bound and a possibly underestimated late HashmapSize.
Do not admit a known above-budget collection merely to return PASS.
An additional pair must not apply `recalcStatsByRuntimeFilter`: preserve the
baseline access-path, cardinality, placement and earlier filter decisions.
Run message preparation, final column liveness/remapping and plan publication
with the resulting complete pair through their existing owners.

The normal HashBuild still consumes and evaluates exactly the same build rows
and join keys. The new filter can skip only the pure index probe. It adds
optional unique-key collection, encoding and message CPU/memory; it is not a
zero-cost optimization. Existing query-budget admission, optional collection
fallback/PASS and operator cleanup remain authoritative. Do not change their
state machines. Physical block reads may remain necessary for scattered keys.

Cached plans contain the complete original local-placement premise and filter
pair. Each normal compile/retry uses the existing statement-scoped message
board; there is no new cross-CN transport or batch generation. No extra side
effects, locking, transaction snapshot advancement or write path is introduced.

## Validation

* Unit tests must show that the original generator rejects and the late pass
  actually adds the target pair, preserves all baseline Stats/filters, and is
  idempotent. Include source fanout, nullable values, PK identity/type/collation
  rejection, literal/dynamic/zero LIMIT, budget boundary, hints, shared inputs,
  foreign RF dependencies, wrong direction and nonlocal placement.
* Retain existing HashBuild tests for deduplication before the key limit and
  exact/PASS behavior. Challenge wide keys and optional-allocation failure;
  validate output and mandatory join-map ownership after fallback.
* Public SQL must execute the actual newly admitted pair. Check deletion count,
  rollback, committed base/index consistency and mixed NULL/duplicate user
  keys. Error controls must demonstrate the intended candidate boundary,
  rather than merely execute an unrelated unchanged plan.
* Compare the original issue at approximately 100k and 1m rows, small and
  large deletes, and ordinary SELECT/full UPDATE controls on the same data.
  Report scan rows, blocks/bytes and resource use separately. Timing must use
  controlled paired samples; no benefit/no-regression claim from noisy runs.
* A two-CN deployment must show the eligible plan's actual scopes stay on its
  original single CN and the new filter works with multiple local lanes.
  A truly distributed plan must retain the baseline filters and direction;
  do not infer this from service count or scope address metadata alone.
* Run final incremental formatting/vet/lint and affected normal/race tests on
  the final diff.

Do not claim all bounded DELETE plans are fixed or close #31618.
