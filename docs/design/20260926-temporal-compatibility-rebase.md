# Temporal compatibility repair after MORPC 97

Status: product target decided by GPT-6-astra/xhigh on 2026-09-26 at the
user's request and implemented in PR #28851 on main
`b1b68925d7f6fb32153e788b5285f424222e85ca`. The
user directly accepted C03, C13, and C12/C34.  Implementation PR:
[#28851](https://github.com/matrixorigin/matrixone/pull/28851).  This document
records the target for integrating that PR with main after #29241.  The PR's
C01–C42 matrix is frozen in the accompanying
[contract appendix](20260926-temporal-compatibility-contract.md); this document
defines the decisions and ownership boundaries that those examples must satisfy.
The decision record in the appendix governs the remaining expect behavior.
The PR records implementation evidence and final review separately from this
target decision.

## Problem and evidence

Temporal SQL currently depends on the spelling and physical path of equivalent
inputs.  The PR's earlier reviews reproduced typed SUBTIME emitting a malformed
negative second or year 10000, an inactive CASE publishing an overflow warning,
TIME and DATETIME interpreting the same no-digit interval differently, and a
new coordinator routing changed temporal semantics to released 4.2 workers.
Those are violations of result-domain, evaluation, grammar, and mixed-version
ownership, respectively. An earlier remote BVT run had 67 proxy and 5
pessimistic failures and no coverage verdict. Its logs exposed outdated
temporal goldens, an un-restored `sql_mode`, and duplicate decimal-comparison
warnings. The current implementation restores mode, updates reviewed goldens,
and defers a constant JOIN-key diagnostic until both logical inputs have rows;
local BVT and focused tests exercise those repairs. A fresh remote CI verdict
must be read from the final pushed head.

The PR was based on `3f0a68bd8034d37e098765920f328985c308c9ff`.
Its integration base `b457977980137e5056be18f03b22da6fc31908e0` adds decimal
division semantics under MORPC 97.  The PR independently assigned temporal
semantics to MORPC 97.  A worker advertising 97 after #29241 is not thereby
capable of the PR's temporal behavior.  Textual conflict resolution alone would
break mixed-version admission.

## Decided target and explicit differences from MySQL

1. Equivalent temporal arithmetic has one exact-result policy.  A valid
   evaluated DATE_ADD/SUB, ADDTIME/SUBTIME, or TIMEDIFF result outside its SQL
   result domain is row NULL plus warning 1441.  This includes typed/string,
   constant/column/parameter, both signs, and calendar/TIME boundaries.  It
   deliberately differs from MySQL TIME saturation.  Explicit CAST and
   assignment retain their separate strict/permissive adjustment contracts.
   NULL inputs propagate without overflow warning; malformed syntax is invalid
   without being misclassified as arithmetic overflow.  Inactive CASE arms and
   masked rows publish no evaluation diagnostic.
2. A text interval with no numeric field (`''`, whitespace, `bad`) is invalid
   and yields row NULL for every supported receiver and scalar/compound unit.
   Explicit numeric/text zero remains valid.  This deliberately departs from
   the main/MySQL no-op behavior of some compound units.  Parsing validity
   cannot depend on whether a caller requests an overflow-status output.
3. DATE_ADD/SUB numeric scalar intervals retain their unit, scale with checked
   arithmetic, then round once half away from zero; exact DECIMAL values remain
   exact.  FLOAT uses its binary multiplication/rounding path.  TIMESTAMPADD
   retains its declared integer-amount coercion.
   Compound numeric values use the documented field normalization, while
   VARCHAR keeps its released field grammar.  These source-type differences
   are explicit; transport or constant folding is not a semantic distinction.
4. Preserve released 4.2 exceptions: ordinary empty text assigned/cast to TIME
   produces NULL, but `TIME('')` / `TIME ''` retains zero under the TIME-function
   grammar.  Whitespace-only produces zero, the numeric versus VARCHAR
   compound grammar remains distinct, and the established STR_TO_DATE `%%`
   extension remains.  These are not claims of MySQL parity.  New binding may
   change result family/FSP/width where specified in C01–C42; released stored
   expression identities keep their physical ABI.  Already normalized stored
   constants are not reparsed using the new interval grammar.
5. Ordinary calendar years are 1–9999 with real date validation.  Typed
   all-zero sentinel fields stay zero; all-zero *text* calendar fields depend on
   `NO_ZERO_DATE`, while accepted partial-zero raw fields and valid zero-calendar
   clocks remain inspectable.  The appendix freezes the exact truth table.
   TIME remains a signed duration bounded by ±838:59:59 with FSP
   0–6.  Unsupported SQL types/units fail at binding, not as a row-value
   exception.  Explicit strict numeric PERIOD APIs keep their own error policy.

The acceptance tuple is value, NULL, SQL result family, FSP/width, error stage
and code, warning count/content, and persisted/remote behavior.  For the same
declared inputs and session snapshot it must be invariant across literal,
expression, column, prepared marker, batch shape, direct SELECT, consuming SQL,
local and remote execution.  A genuine type or operation distinction may differ
only when this document or the PR matrix declares it.

## Alternatives and decision rationale

Full MySQL 8.0 behavior would saturate some TIME arithmetic and preserve some
no-digit compound no-ops.  It reduces one migration difference but makes
equivalent MatrixOne arithmetic disagree and accepts malformed text as success.
Retaining the current branch without one owner leaves typed/string and
TIME/DATETIME path dependence.  The decided contract centralizes parse,
checked arithmetic, final-domain validation, and diagnostic publication while
limiting compatibility exceptions to documented release behavior.  The MySQL
8.0.46 observations in the PR are comparison data, not an automatic oracle for
all versions or MatrixOne operations.

## Owner and consumer boundaries

| Owner | Required closure |
| --- | --- |
| `pkg/container/types` | Classify complete temporal syntax; distinguish invalid, internal numeric overflow, and valid zero; bound parser field storage and work per input byte. |
| Function evaluators | Perform checked intermediate arithmetic; validate the final SQL domain once for typed/string and ADD/SUB siblings; preserve adjacent valid rows and NULL masks.  Shared SQL HEX/BIT numeric casts and numeric-to-TIME conversion retain their separate source/operation rules. |
| Binder and constant folder | Keep family/FSP/width and source provenance; fold without publishing an EXECUTE-only diagnostic from an inactive expression.  Repeated prepared execution observes current session state. |
| Session/process codec | Carry sql_mode, time zone, and week mode to remote consumers; unavailable required state fails explicitly. |
| Function registry and persistence | Preserve released 4.2 physical overload ABI; new SQL binds new identities where needed; view/default/CTAS/Substrait consumers see the declared result type. |
| Compile, wire, and catalog admission | Decimal division requires MORPC 97.  The changed temporal/conversion contract requires one *new* final epoch, MORPC 98 while main remains at 97.  Detect all changed identities, including SQL HEX/BIT numeric casts, numeric-to-TIME, string-to-temporal casts, and stable temporal arithmetic IDs.  A 97 worker remains semantically eligible for decimal-only plans, subject to the deployment's admission floor.  Apply this at feature detection, placement, send, receive, and persisted-expression admission.  Do not change the meaning of decimal 97. |
| Bootstrap | Commit prerequisite tables first.  Before ingress, reconcile missing derived views only after an authoritative enabled, non-preparing, admitted and catalog-fenced temporal-98 snapshot.  Wait outside SQL transactions; routing Ready is an output of completion, not an input to catalog authoring.  Keep existing release view definitions; wrong-kind objects and permanent/rollback errors fail startup. |

MORPC 98 is a mainline integration decision, not another epoch for an
intermediate revision of this PR.  If main advances again before integration,
choose the next free epoch and repeat the capability proof.  Worker eligibility
depends on the capability introduced by that epoch, not on a coincidentally
equal integer in a branch that assigned the same number a different meaning.

| Worker capability | Newly bound decimal division | Newly bound temporal/conversion contract |
| --- | --- | --- |
| Released 4.2, 9/10 | Reject or place elsewhere | Reject or place elsewhere |
| Main after #29241, 97 | Execute | Reject or place elsewhere |
| Integrated candidate, 98 | Execute | Execute |

A plan containing both requires `max(97, 98) = 98`.  Feature collection keeps
the two flags independently through nested expressions and casts, including
SQL HEX/BIT numeric conversions and numeric-to-TIME conversions.  Binding
checks source expressions before constant folding removes their provenance.
Placement, scope encoding, receiving, and persisted-expression authoring apply
the same requirement; a released physical identity gets only its enumerated
compatibility path, never a blanket exemption for newly authored SQL.  The
bootstrap authority requires temporal 98 while public routing Ready remains
false.  A worker may still be excluded by a later deployment admission floor
even when the query's minimum required version is lower.  This matrix is based
on actual capability semantics, not merely the
numeric version reported by the PR's old temporary branch.

## Failure, lifecycle, cost, and rollback

Malformed input and arithmetic overflow remain distinct, including constant
folding and prepared reuse.  Session diagnostics obey the existing retention
limit.  Failed binding/evaluation leaves subsequent rows and executions usable.
No parser path may allocate an unbounded token list or add per-row CAST/TRIM
chains.  No new background worker/cache is proposed.  Missing-view recovery
uses bounded catalog reads and one transaction per attempt; normal startup does not rewrite
existing views.  Admission waits are cancellable and cannot hold a SQL
transaction or public ingress open.  Retry applies only to known transient
transaction failures; a nonretryable rollback error is reported.

Rolling upgrade rejects or localizes a new temporal query while any potential
executor lacks 98; decimal-only queries may still use 97.  Old stored
expressions remain readable by the upgraded binary under their released ABI.
In-place rollback to a pre-98 binary is supported only before any irreversible
catalog or protocol activation, and only after proving that binary accepts the
current disk and coordinator state.  Advancing the admission floor or catalog
fence already crosses that boundary, even if no temporal-98 expression has
been authored.  After activation or any 98 metadata write, roll forward or
restore a complete pre-upgrade snapshot, including coordinator/HAKeeper
persistent state; never lower the floor to force an older reader through.  An
older binary is not presumed to understand the new admission marker or overload
identity.  No claim of safe in-place downgrade follows from the new binary's
admission checks.  Exact upgrade,
restart, and mixed-version restrictions must be tested before delivery; no
live rolling upgrade is claimed without running it.

The startup state transition is: (a) commit prerequisite tables; (b) read all
bounded derived-view identities and reject wrong-kind objects; (c) if any view
is absent, wait outside a SQL transaction for a fresh enabled, non-preparing,
admitted, catalog-fenced 98 snapshot.  A stale epoch, unsupported required
protocol, or disabled authority is rejected; a not-yet-ready authority is
awaited only while the owning startup context remains live.  None authorizes
DDL.  Cancellation terminates the wait and startup without public ingress.
(d) Within one bounded transaction per attempt, reread the
actual catalog identities, create only still-missing views, then commit; the
commit is the publication point.  A concurrent creator is resolved by reread
and idempotent completion, not by overwriting an existing definition.  Only
known transient transaction conflicts retry from the catalog reread under a
fresh authority snapshot, with the startup context owning termination; no
unbounded background retry remains after cancellation.  Rollback failure is
reported even when another error occurred.  (e) Verify the complete set, then open public
ingress.  A restart repeats this observation from catalog state; no separate
marker or background reconciler is introduced.

## Repeated acceptance plan

1. Owner-level invariant and nearest counterexample tests, `-count=3`, for
   parse, final domain, diagnostic masking, FSP/width, and prepared reuse.
   Include valid→NULL→invalid→overflow→valid on one actual prepared handle.
2. A protocol cross-product of release 9/10, main decimal-only 97, and new 98:
   97 accepts decimal semantics but rejects new temporal semantics; 98 accepts
   both.  Exercise placement, send, receive, persisted admission, and bootstrap.
3. Relevant owning packages, direct consumers, race for admission/lifecycle,
   and configured incremental SCA on the rebased diff.  Recheck decimal
   division, defaults, and DUMP replay touched by #29241.
4. Existing temporal BVT with metadata/warning oracles, normal comparison on
   the same test-owned instance twice, including cleanup and residual catalog
   checks.  Classify every current 67/5 mismatch before changing a golden.
   Save and restore `sql_mode` in every test file that changes it; verify that
   a subsequent unrelated case sees its own declared/default mode.  Assert
   the appendix's zero-calendar truth table under both empty and default modes,
   the CAST-versus-TIME empty-input distinction, and two diagnostics for the
   specified conversion-then-arithmetic example.  Keep the decimal-comparison
   warning count at one rather than approving a duplicate-warning golden.
5. Two independent fresh 4.2.4 seed→candidate→same-disk restart cycles, plus
   empty-catalog and competing multi-CN missing-view startup.  Real binary
   prepared protocol checks use server preparation, not client emulation.
6. Paired benchmark samples for changed hot paths with exact-result and bounded
   warning assertions.  Helper-only numbers do not establish whole-query CPU,
   throughput, or peak-memory behavior.

Product target: the user accepted C03, C13, and C12/C34 and delegated the
remaining expectation decisions to GPT-6-astra/xhigh. Its 2026-09-26 decision
accepted C01–C42 with the precise amendments recorded in the appendix,
including C10 binary FLOAT behavior, C39 persisted ABI, and the C40/C42
integration at temporal MORPC 98. The implementation has been reviewed and
locally validated on the rebased branch; remote CI remains an independent gate.
