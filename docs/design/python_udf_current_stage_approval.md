# Python UDF current-stage design approval record

| Field | Value |
| --- | --- |
| Record | `python-udf-current-stage-r1-2026-09-24-approval` |
| Issue | [MatrixOne #28132](https://github.com/matrixorigin/matrixone/issues/28132) |
| Implementation PR | [MatrixOne #29152](https://github.com/matrixorigin/matrixone/pull/29152) |
| Current-stage proposal | [`python-udf-current-stage-r14-2026-10-15`](python_udf_current_stage.md); historical r1 approval retained below |
| Scope | Explicitly enabled test/development adapter; no production rollout or production-readiness claim |

This record separates the immutable current-stage design artifact, the feature
owner's scope authorization, and independent technical approval facts. It is a
traceability record; it does not create an approval that is not present in the
PR or issue history.

## Immutable artifacts

The design content accepted for the current test/development stage was published
in the rebased PR history at commit [`c6f9c7682279816872fc3b3f7d5cf41ffe940acc`](https://github.com/matrixorigin/matrixone/commit/c6f9c7682279816872fc3b3f7d5cf41ffe940acc).
The corresponding pre-rebase review commit was
[`610d1e4dcfe2bad2ed1060babe0acc60c89b1c60`](https://github.com/matrixorigin/matrixone/commit/610d1e4dcfe2bad2ed1060babe0acc60c89b1c60).
The exact Git blob for
[`docs/design/python_udf_current_stage.md`](https://github.com/matrixorigin/matrixone/blob/c6f9c7682279816872fc3b3f7d5cf41ffe940acc/docs/design/python_udf_current_stage.md)
at both design commits is
`2adcb1ec63ced2dfbdedf192509b07d5b7c9db31`.

The design was written against implementation baseline
[`9847da80d6c7096cb0460e46a5cf710e9714af3a`](https://github.com/matrixorigin/matrixone/commit/9847da80d6c7096cb0460e46a5cf710e9714af3a).
The rebase-equivalent implementation baseline in the final PR history is
[`1a26a2f9046643a284df488f57470b811cca747a`](https://github.com/matrixorigin/matrixone/commit/1a26a2f9046643a284df488f57470b811cca747a).
The pre-rebase exact review head was
[`92a89d81588159a918458b1a930bccc9d0e75d4b`](https://github.com/matrixorigin/matrixone/commit/92a89d81588159a918458b1a930bccc9d0e75d4b).
The repair commit carrying this record is the final documentation commit on
top of the rebased PR history; its exact remote head is verified at delivery.
The historical r1 design blob delivered with the approval-record link was
`994b8016729d2e87a6a706a2833eee7faf50c89b`; its only difference from the
approved design blob is the link to this traceability record.

The namespace-validation companion is the implementation contract from commit
[`284a1d971a1d2c763c6aed425d52f751735c409f`](https://github.com/matrixorigin/matrixone/commit/284a1d971a1d2c763c6aed425d52f751735c409f),
with historical recorded blob `51ccfb771db19321d03b292cd3d7219f698a11b8`.

## Approval ledger

| Decision area | Evidence and scope | Status |
| --- | --- | --- |
| Feature-owner acceptance | [PR comment](https://github.com/matrixorigin/matrixone/pull/29152#issuecomment-5809313638) approves the exact current-stage contract for testing/development use. | Approved for this limited scope |
| Architecture review | No independent approval record naming the reviewer, exact design artifact, date, scope, and decision is present in the PR/issue history examined at pre-rebase head `92a89d81588159a918458b1a930bccc9d0e75d4b`. | Pending; not inferred |
| Security review | The contract records the unisolated, opt-in boundary and explicitly excludes sandboxing and authenticated/TLS Flight. That scope record is not a production security approval. | Pending for any production/security enablement; not inferred |
| SQL/Planner review | The current-stage document contains the shared Catalog, exact revision, overload, evaluation, migration, restore, and compatibility contract. No independent approval record for that exact artifact is present in the PR/issue history examined at the recorded pre-rebase head. | Pending; not inferred |
| Cloud/Operator review | The current-stage document records launch, init, endpoint, and rollout limits, and excludes Operator rollout acceptance. No independent approval record for production/operator rollout is present. | Pending; not inferred |

The pending rows are external approval facts and are intentionally not replaced
by feature-owner authorization, passing CI, or implementation review. They must
remain separate from the test/development acceptance boundary. This PR does not
authorize production tenant isolation, sandboxing, authenticated transport,
Operator rollout, artifact garbage collection, total artifact quota, or
cross-version downgrade/restore.

## Evidence boundary

The design document's evidence table identifies the focused unit, process, Flight,
SQL/BVT, restore, and exact-head CI evidence. Upgrade-compatibility checks were
skipped at the recorded head; cross-version rollback/restore is unsupported in
this stage. Those gaps are recorded as limits and are not represented as passing
approval evidence. Full implementation/lifecycle review remains a separate PR
review step after the design record is accepted by the relevant reviewers.

## r3 maintenance addendum (2026-10-05)

The current document proposes protocol 107 and repair catalog 4.0.12 against
upstream `c65991043b10fb39988d45b72af1485560ac5433`. It adds explicit sender,
receiver, prepared-reuse and physical execution admission, and schedules repair
for a tenant already at upstream-final 4.0.11. These are conservative corrections
within the opt-in development scope. The immutable r1 blobs above describe that
historical approval only; they are not the current proposal's content identity.
Architecture and SQL/Planner approval remain pending, as recorded in reviews
5302243148 and the exact-head review on 4ca229411166b9da09a702dbe1b799de43d8d694.
Code review and passing tests do not substitute for those independent decisions.

## r4 rebase maintenance addendum (2026-10-09)

Against upstream `9df2c4006142a0593f9bb4c40019ee1a8cd2cde7`, the current repair catalog is 4.0.13 with minimum direct source 4.0.12. Upstream 4.0.12 owns the charset metadata refresh and remains unchanged. The shared-function repair replays its nine entries only in the new handler, so already-upgraded 4.0.12 tenants are scheduled. Protocol admission remains 107, immediate predecessor 106. This mechanical rebase correction and the historical-schema lookup correction for snapshot/TS/PITR restore remain within the existing development scope; independent approval remains pending.

## r5 rebase maintenance addendum (2026-10-09)

Official main `8cc9167cb0a04479cd4c2f083aece5130f8c1946` has allocated
MORPC 109 to the native Unicode COLUMNS migration in catalog 4.0.14. This
proposal uses the next MORPC 110 and preserves that upstream migration alongside
the Python revision repair (minimum direct source 4.0.13). The nine idempotent
shared-function entries remain replayed by the combined handler; already-upgraded
4.0.13 tenants remain eligible. Sender, receiver, prepared reuse, physical
execution and status RPC admission move together; protocol 109 is the immediate
predecessor rejection case. This is a mechanical compatibility
correction within the existing test/development scope. Independent Architecture
and SQL/Planner approval remains pending; no approval is inferred from this
addendum or validation.

## r6 maintenance addendum (2026-10-11)

Against official main `4022729da24d71b082c22b52aaed3faba2b77ea5`,
MORPC remains 109, so this proposal retains capability 110 and immediate
predecessor 109. The r5 combined 4.0.14 handler above is superseded: a tenant
already at the official final 4.0.14 would not be scheduled by that version.
The r6 proposal preserves the upstream 4.0.14 handler exactly and registers
Python repair 4.0.15 with minimum direct source 4.0.14. It replays only the nine
idempotent shared-function entries, gates catalog access at protocol 110, and
covers real first-login repair from 4.0.12, 4.0.13 and 4.0.14. Revision-table
snapshot/PITR restore also uses the same trusted catalog DDL context as the
head-table restore; ordinary user DDL does not acquire that context.
This maintenance stays within the authorized test/development scope.
Independent Architecture and SQL/Planner approval remains pending; this
addendum records implementation changes and does not grant approval.

## r7 rebase maintenance addendum (2026-10-12)

Official main `11914526ba0cd40d471299d8e5e2929909b9e09c` has allocated
MORPC 110 to MEDIUMINT assignment bounds. The Python capability therefore moves
to the next version, 111, with 110 as its immediate predecessor rejection case.
The published 4.0.14 handler remains unchanged; the separate 4.0.15 repair still
replays the same nine idempotent shared-function entries. Catalog publication,
reads, prepared reuse, physical execution, remote sender/receiver admission,
repair scheduling and direct replay, and the status RPC share the new floor.
Tests reject predecessor-only and mixed 111/110 CNs before catalog mutation,
and retain MEDIUMINT's independent version-110 admission.

The new upstream MEDIUMINT uses a 24-bit logical domain in INT32 storage. The
current Python ABI rejects signed/unsigned MEDIUMINT declarations and inputs
before normalization could widen that domain; ordinary 32-bit INT is unchanged.
Typed descriptor and parsed CREATE FUNCTION cases cover this fail-closed boundary.

This supersedes r6's candidate protocol allocation and records the new upstream
type boundary. It preserves the existing opt-in, unisolated test/development
scope and catalog repair contract.
Independent Architecture and SQL/Planner approval remains pending. Neither this
mechanical rebase nor its validation grants approval or extends production scope.

## r8 rebase maintenance addendum (2026-10-12)

Official main is now b09f2c6fc54eb159f549f69db2636fd5fb6e5125. Its MORPC
latest remains 110, so Python capability 111 and immediate predecessor 110
remain unchanged. The published catalog 4.0.14 handler and separate 4.0.15
Python revision repair retain the r7 contract, including signed/unsigned
MEDIUMINT rejection before descriptor normalization. This rebase incorporates
upstream decimal-tail validation, the lock-timeout compatibility alias, and
batched catalog ID lookup for table statistics without adding Python ABI or
rollout scope. The immutable r7 maintenance record above remains historical;
this addendum records the new implementation baseline only. Independent
Architecture and SQL/Planner approval remains pending, and validation does not
supply that approval.

## r9 rebase maintenance addendum (2026-10-13)

Official main `0a4db0e6a6517e489a333ad9073dfcdc5d26aff4` allocates
MORPC 111 to IndexSearchScan. Python's single new capability moves to 112,
with 111 as its immediate predecessor rejection case. The main IndexSearchScan
plan and pipeline contract remains at 111; MEDIUMINT remains at 110. The merged
plan protobuf retains both IndexSearchScan and typed routine metadata, including
routine dependency envelopes and deep-copy ownership. Python catalog reads,
publication, Prepare reuse, physical/remote execution, repair scheduling/direct
replay, and status RPC admission all use the new Python floor. Tests reject
predecessor-only and mixed 112/111 CNs before catalog mutation.

The published catalog 4.0.14 handler remains unchanged, and the separate 4.0.15
repair retains the nine idempotent shared-function entries. The current Python
ABI and opt-in, unisolated test/development scope are unchanged. Historical r8
records remain immutable. Independent Architecture and SQL/Planner approval of
this current revision remains pending; rebase and validation do not supply it.

## r10 documentation errata (2026-10-13)

This revision corrects two stale r9 prose references: shared revision
publication/Python reads and `GetPythonUdfStatus` both require MORPC 112,
as implemented at `5daca2160a95495ef343a4c13ebdc8337309dab5`. Protocol 111
remains the official IndexSearchScan capability and Python's immediate
predecessor rejection case. The implementation, ABI, catalog repair, and
test/development scope are unchanged. The r9 maintenance record above is
retained. Independent Architecture and SQL/Planner approval of the current
revision remains pending; this documentation correction does not grant it.

## r11 rebase maintenance addendum (2026-10-13)

Official main is now `fa7cc1d9b38e3324572a4efc5617033522de0cbf`.
Its MORPC latest remains 111, so Python capability 112 and immediate
predecessor 111 remain unchanged. The published catalog 4.0.14 handler and
separate 4.0.15 Python revision repair retain the r10 contract. The rebase
incorporates upstream prepared COALESCE typing, integer common-type precision,
OR normalization, Unicode collation metadata, performance-schema formatters,
buffered TLS handshakes, and bounded gossip fixtures without changing Python
ABI or rollout scope. The embedded admission test now releases concurrent
light packages through distinct FIFO channels and checks each package's token;
its admission, exclusion, failure, and cleanup assertions remain in force.

Historical r10 records remain immutable. Independent Architecture and
SQL/Planner approval remains pending; rebase and validation do not supply it.

## r12 protocol allocation addendum (2026-10-14)

Official main `1b66ff135c64ef1c642a297105bf599cd3173dee` assigns
MORPC 112 to signed INT WEEK/DAYOFYEAR result vectors. The current
`python-udf-current-stage-r12-2026-10-14` contract moves Python's single new
capability to 113, with 112 as the immediate predecessor rejection case.
The signed date result capability remains at 112 and IndexSearchScan remains
at 111. Shared catalog publication and reads, prepared reuse, physical
execution, remote placement/send/receive admission, the 4.0.15 repair, and
`GetPythonUdfStatus` all use the Python 113 floor. Predecessor-only and mixed
113/112 CNs are rejected before catalog mutation; legacy SQL fallback is
retained. Catalog versions, Python ABI, and the test/development scope are
unchanged.

Historical r11 and earlier artifacts above remain immutable. Independent
Architecture and SQL/Planner approval of r12 remains pending; mechanical
rebase and validation do not supply an approval decision.

## r13 protocol allocation addendum (2026-10-15)

Official main `8c8f829f9d3e9576d3b7371333e0eb1147a008e4` assigns
MORPC 113 to DECIMAL variance/stddev DOUBLE result contracts. The governing
`python-udf-current-stage-r13-2026-10-15` revision moves Python's single new
capability to 114, with 113 as the immediate predecessor rejection case.
The upstream statistical result contract remains at 113; signed date results
remain at 112 and IndexSearchScan remains at 111.

Shared revision publication and reads, cached/prepared/evaluator checks,
actual remote-destination admission, receiver validation, catalog repair
metadata and login compensation, and `GetPythonUdfStatus` all require Python
capability 114. Predecessor-only and mixed 114/113 CNs are rejected before
catalog mutation. Ordinary SQL UDFs retain their pre-floor legacy fallback.
The nine idempotent shared-function entries and catalog repair 4.0.15 are
unchanged; this revision adds no execution, production or security scope.

Independent Architecture and SQL/Planner approval of r13 remains pending.
Mechanical rebase, protocol allocation and validation do not supply it.

## r14 protocol allocation addendum (2026-10-15)

Official main `493d8b8181b7d74bed91e360874a3dd35223f276` assigns
MORPC 114 to native Unicode UNIQUE key format and duplicate-admission semantics.
The governing `python-udf-current-stage-r14-2026-10-15` revision moves Python's
single new capability to 115, with 114 as the immediate predecessor rejection
case. The upstream Unicode contract remains at 114, DECIMAL statistical results
at 113, signed date results at 112 and IndexSearchScan at 111.

Shared revision publication and reads, cached/prepared/evaluator checks,
actual remote-destination admission, receiver validation, catalog repair
metadata and login compensation, and `GetPythonUdfStatus` all require Python
capability 115. Predecessor-only and mixed 115/114 CNs are rejected before
catalog mutation. Ordinary SQL UDFs retain their pre-floor legacy fallback.
The nine idempotent shared-function entries and catalog repair 4.0.15 are
unchanged; this revision adds no execution, production or security scope.

Independent Architecture and SQL/Planner approval of r14 remains pending.
Mechanical rebase, protocol allocation and validation do not supply it.
