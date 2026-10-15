- Status: draft (pending independent approval)
- Start Date: 2026-10-16
- Revision: 1
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/30184
- Issue: https://github.com/matrixorigin/matrixone/issues/30155

# Signed-zero set membership and protocol admission

## Contract and scope

FLOAT and DOUBLE set operations compare their physical membership keys while
returning the original visible values. Positive and negative zero remain distinct;
NULL, NaN normalization, ALL multiplicities, and the shared GROUP BY/DISTINCT
contracts retain their existing owners. UNION ALL needs no membership key.
The internal `set_operation_float_key` registry identity is 599, with overloads
0 (FLOAT) and 1 (DOUBLE). No wire field or user-visible column is added.

Identical, nonprepared, effect-free UNION arms may reuse one evaluation when each
arm has an ungrouped aggregate or a literal LIMIT of zero or one, through
cardinality-preserving project/filter/sort wrappers. Such an arm needs no DISTINCT.
This preserves signed-zero representatives and leaves aggregate empty-input rows,
COUNT zero reconstruction, and per-correlation pagination with the existing
single-arm decorrelation owner. An unordered LIMIT proves a cardinality bound,
not ordering: identical arms retain the established single-evaluation choice.
Grouped or multirow arms, volatile expressions, parameters, and extra producer
steps retain the typed set route and its scalar cardinality checks.

## Compatibility and rollback

Official main currently ends at MORPC 115. This capability uses MORPC **116**.
The feature walker recognizes encoded function identity 599 without depending on
its display name. It sees hidden physical equality lists, grouping keys and
serialized SetOp.KeyExprs as well as ordinary expressions.

Placement requires workers advertising 116 or newer; missing or old capabilities
fall back to one CN on the capable coordinator. Sending rechecks the actual
pipeline destinations, including destination changes after placement. The receiver
rejects a pipeline containing these expressions before operator decoding when its
local floor is missing or below 116. Operator expression construction checks the
same floor before allocating an executor. Persisted-expression read and authoring
admission also require 116. These checks add no per-row work or new resource owner.

Prepared specialization regenerates membership keys from the resolved output
domain before capability detection: integer -> float -> integer must acquire and
then release the 116 requirement. A prepare-time ANY type is not capability proof.
A new binary configured at the immediate predecessor floor 115 rejects these
expressions before execution. An actual old binary lacks the new local guard;
new senders exclude it through destination admission, and persisted authoring
cannot publish the expression under a durable floor below 116. There is no reservation for unmerged PRs;
a rebase advances this candidate to fresh official-main latest + 1 when needed.

## Acceptance evidence

- Public SQL: identical correlated MAX and LIMIT arms, empty input and real NULL,
  signed-zero representatives, ordered/unordered LIMIT, OFFSET, zero LIMIT, COUNT,
  plus existing multirow signed-zero and scalar-cardinality controls.
- Typed planning: singleton boundaries and their negative controls; prepared
  integer/float transitions and hidden key regeneration.
- Compatibility: both registry overloads and nameless protobuf roundtrip; actual
  compiled INTERSECT ALL and UNION grouping stages; placement fallback; old,
  missing and new destination responses; changed destination; balanced releases;
  receive-before-decode rejection; executor and persisted admission at 115/116.

## Alternatives and bounds

Ordinary DISTINCT cannot represent signed-zero set equality. Unconditionally
collapsing identical floating arms loses multirow membership. Sending a new
registry identity under the unchanged 115 advertisement permits old workers to
attempt unknown-function execution. Explicit capability admission closes that
boundary without changing the wire schema or implementing another set operator.
Independent RFC approval remains external; no author approval is asserted here.
