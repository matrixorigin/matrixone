# PR #28830 expression and spatial compatibility supplement

Status: design supplement for PR #28830

Related PR: https://github.com/matrixorigin/matrixone/pull/28830

Related issues: #28194, #28200, #28892

## Scope and problem

The PR contains three coupled public compatibility closures:

1. SRID 4326 discrete Fréchet/Hausdorff distance must use the repository's
   established spherical geodetic model, while legacy planar overloads retain
   their old execution identities.
2. The spatial distance family accepts the authoritative length-unit names and
   factors, without confusing a string unit with the legacy numeric SRID form.
3. Prepared execution and plan persistence must preserve the runtime source
   domain, result metadata, and exact DECIMAL256 literal spelling across plan
   copies, folding, serialization, remote execution, and view/default reload.

The current implementation also contains prerequisite prepared-expression and
IP metadata fixes. This supplement freezes that stacked scope; it does not add
new planner-wide type inference or a new compatibility framework.

## Invariants and non-goals

- A newly bound SRID-4326 geodetic expression returns spherical meters before
  unit conversion; the two legacy planar identities remain planar.
- A supported unit selects exactly its registered factor. Case and accent
  differences follow the existing MySQL-compatible lookup contract; spaces,
  punctuation, and unsupported aliases remain significant.
- Each PREPARE/EXECUTE uses the current parameter's source domain and cannot
  mutate the cached template or leak a previous execution's overload choice.
- Every changed expression identity or result/metadata contract is rejected or
  kept local until the selected remote CN is known to support its protocol
  epoch. The actual destination is checked again at serialization time.
- A persisted expression's maximum required protocol survives constant folding,
  deep copy, vector folding, protobuf round-trip, view creation, and reload.
- Empty unit-overload geometry returns SQL NULL through the existing empty
  payload path. The newly introduced two-argument geodetic identities retain
  the existing kernel error behavior for empty geometry; changing legacy
  identities is out of scope.

Non-goals are an ellipsoidal geodesic rewrite, a cancellation redesign for
long-running geometry kernels, new aliases outside the authoritative unit
registry, and a generic planner metadata framework.

## Ownership and compatibility epochs

The first owner is the planner/registry identity. Its consumers are the vector
executor, remote pipeline sender/receiver, and persisted-expression admission
path. The epoch assignments are monotonic and cumulative:

| Contract | Epoch |
|---|---:|
| existing integer-parameter coercion | 85 |
| extended IP overload/result and expression metadata | 86 |
| exact decimal literal and coercion semantics (complete D closure) | 87 |
| geodetic distance semantics and length-unit overloads | 88 |

C must include the complete finalized D87 closure before validation or rollout;
copying only the common-coercion delta is insufficient. Main86 -> D87 -> C88
ensures that advertising the latest version also supports every prior contract.
Both new epochs are unmerged; earlier branch-local assignments are superseded.

Integration baseline: the complete D87 branch ending at
`19269e68b2b96f8ee0354603bf09783e8ef7233a` is merged, including its arithmetic,
common coercion decisions, scalar/vector fold provenance, interval/fulltext
prerequisites, and regressions. C retains spatial88 and maximum-feature
admission alongside that closure. Post-integration validation and actual
mixed-binary acceptance remain separate pending gates.

The protobuf additions are optional fields. Old readers can decode the message,
but must not execute a marked expression under a different semantic contract.
Therefore placement, send-time destination checks, receiver checks, and
persisted-expression admission use the same feature identity and epoch.

For mixed-version rollout, old spatial/IP/decimal identities are never
reinterpreted. New identities are kept on a compatible worker or rejected.
Persisted definitions requiring a higher floor cannot be authored or rebound
below that floor. A binary rollback does not silently make a higher-floor
definition readable; the existing deployment admission policy remains the
rollback owner.

## Resource and performance decision

Discrete Fréchet recurrence needs only the previous row and the current-row
predecessor. Replace the full `n*m` matrix with two rows of length `m`:

- time remains O(n*m);
- workspace changes from O(n*m) float64 cells to O(m) cells;
- empty-input and vertex order semantics remain unchanged;
- no wire, catalog, or API shape changes.

Do not precompute S2 points or add cancellation checks in this change without a
measured CPU or liveness requirement. The rolling rows address the bounded
workspace invariant without changing the distance recurrence.

## Alternatives

1. Spatial-only extraction: smallest review surface, but unsafe unless every
   prepared/protocol prerequisite is proven independent and removed without
   regressing the linked public paths.
2. Keep the current stacked scope and add only the missing compatibility and
   workspace closures: preserves the validated fixes and avoids history
   reconstruction; this is the selected option.
3. Split prerequisite PRs and rebase the spatial change afterward: clearer
   ownership, but requires rebuilding the existing 43-commit dependency chain
   and repeating all public-path validation.

The selected option is the smallest safe update to the PR currently under
review. It does not broaden the feature set.

## Failure and cleanup paths

- Invalid SRID, unit, malformed geometry, coordinate range, empty payload, row
  mask, SQL NULL, and result-append errors retain their current ordering.
- The rolling workspace is call-local and released with the function return;
  input vectors remain borrowed and result ownership remains with the executor.
- Remote placement failure is fail-closed. A worker replacement or downgrade
  between compile and send is caught by the destination recheck.
- Prepared-plan specialization always operates on a copied plan and preserves
  the original template on success, error, and repeated execution.
- View/default admission records the strongest requirement before optimizer
  folding and checks it again when the definition is read or rebound.

## Validation and acceptance

Focused evidence must include:

- `pkg/geo`: planar/geodetic controls, empty input, asymmetric directed
  Hausdorff, antimeridian/high-latitude boundary cases, and a small independent
  full-matrix Fréchet oracle against the rolling-row implementation.
- `pkg/sql/plan/function`: registered spatial executors, unit validation,
  float32/float64, masks/NULLs, malformed payloads, and prepared reuse.
- `pkg/pb/plan` and `pkg/sql/plan`: feature identity, copy/fold/protobuf
  provenance, persisted floor, and view/default lifecycle.
- `pkg/sql/compile`: remote placement and send-time checks for v86/v87/v88,
  including the DECIMAL256 marker; old spatial/IP identities are negative
  controls.
- Distributed geo and decimal cases: normal result comparison through the
  public SQL path. Upgrade/compatibility jobs are an external mixed-binary
  acceptance gate and must not be described as passed when skipped.

The design is falsified by any result change for a retained legacy identity,
loss of a feature marker across a rewrite or protobuf round-trip, successful
remote delivery to an older worker, or workspace that still grows with `n*m`.

### Runtime acceptance boundary: scheduling versus wire compatibility

The existing `disttae.Engine.QueryCandidates` policy admits only workers whose
`CommitID` equals the producer's build commit. Actual D87 and C88 builds therefore
cannot form a cross-commit public SQL worker set, independently of the expression
epoch checks. Keeping only the other build Working must fail closed; changing
reported commit IDs or protocol versions is not an acceptable test workaround.
This change does not relax that scheduling policy.

Consequently the mixed-binary acceptance evidence has separate obligations:

- Public SQL: retain compatible local execution, reject unavailable placement,
  and prove prepared reuse and persisted authoring/reload/restart/rollback gates.
- Old-produced wire compatibility: a probe built from the actual D87 source must
  use D's scope encoder and transport to execute a non-NULL legacy expression on
  the actual C88 receiver, with an independently known exact result. This is not
  an old-to-new public SQL scheduling claim.
- New sender and receiver boundaries: probe real destination UUID/address/version
  tuples at serialization, replace and restore the destination, and exercise
  valid positive envelopes before interpreting old-receiver rejection.

All three require recorded actual binary/source identities. Unit mocks, skipped
upgrade jobs and malformed-envelope errors cannot substitute for these results.
