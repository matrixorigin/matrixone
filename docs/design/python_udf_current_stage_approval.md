# Python UDF current-stage design approval record

| Field | Value |
| --- | --- |
| Record | `python-udf-current-stage-r1-2026-09-24-approval` |
| Issue | [MatrixOne #28132](https://github.com/matrixorigin/matrixone/issues/28132) |
| Implementation PR | [MatrixOne #29152](https://github.com/matrixorigin/matrixone/pull/29152) |
| Current-stage proposal | [`python-udf-current-stage-r39-2026-10-31`](python_udf_current_stage.md); historical r1 approval retained below |
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
Operator rollout, production artifact garbage collection/quota management, or
cross-version downgrade/restore.

## Evidence boundary

The design document's evidence table identifies the focused unit, process, Flight,
SQL/BVT, restore, and exact-head CI evidence. Upgrade-compatibility checks were
skipped at the recorded head; cross-version rollback/restore is unsupported in
this stage. Those gaps are recorded as limits and are not represented as passing
approval evidence. Full implementation/lifecycle review remains a separate PR
review step after the design record is accepted by the relevant reviewers.

Historical protocol-allocation addenda below describe prior proposals. Their
per-feature rollout floors are superseded by r32; they are not current admission
requirements or independent approvals.

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

## r15 protocol allocation addendum (2026-10-16)

Official main `99f52528308289e08ab6f07f87b0a54719b877a3` assigns MORPC 115
to type-sensitive calendar DAY and truncating SECOND intervals. The governing
`python-udf-current-stage-r15-2026-10-16` revision moves Python's single new
capability to 116, with 115 as the immediate predecessor rejection case.
Calendar normalization remains at 115 and native Unicode UNIQUE semantics at 114.
Both remote-expression feature detectors and their distinct admission floors
are retained after rebase.

Shared revision publication and reads, cached/prepared/evaluator checks,
actual remote-destination admission, receiver validation, catalog repair
metadata and login compensation, and `GetPythonUdfStatus` all require Python
capability 116. Predecessor-only and mixed 116/115 CNs are rejected before
catalog mutation. Ordinary SQL UDFs retain their pre-floor legacy fallback.
The nine idempotent shared-function entries and catalog repair 4.0.15 are
unchanged; this revision adds no execution, production or security scope.

Independent Architecture and SQL/Planner approval of r15 remains pending.
Mechanical rebase, protocol allocation and validation do not supply it.

## r16 protocol allocation addendum (2026-10-16)

Official main `76deafc8062f9a7e33a31a4df858ebcdb9477cb5` assigns MORPC 116
to SELECT scalar division warning diagnostics. The governing
`python-udf-current-stage-r16-2026-10-16` revision moves Python's single new
capability to 117, with 116 as the immediate predecessor rejection case.
Division diagnostics retain their execution-only 116 floor and statement-policy
check; calendar normalization remains at 115. The receiver and actual remote
endpoint retain both independent admission checks after rebase.

Shared revision publication and reads, cached/prepared/evaluator checks,
actual remote-destination admission, receiver validation, catalog repair
metadata and login compensation, and `GetPythonUdfStatus` all require Python
capability 117. Predecessor-only and mixed 117/116 CNs are rejected before
catalog mutation. Ordinary SQL UDFs retain their pre-floor legacy fallback.
The nine idempotent shared-function entries and catalog repair 4.0.15 are
unchanged; this revision adds no execution, production or security scope.

Independent Architecture and SQL/Planner approval of r16 remains pending.
Mechanical rebase, protocol allocation and validation do not supply it.

## r17 protocol allocation addendum (2026-10-16)

Official main `14a8d854b0f9346065ff8432eb979e35f32f50d8` assigns MORPC 117
to collation-aware LEAST/GREATEST ordering. The governing
`python-udf-current-stage-r17-2026-10-16` revision moves Python's single new
capability to 118, with 117 as the immediate predecessor rejection case.
LEAST/GREATEST retains its independent 117 floor; SELECT division diagnostics
remain at the execution-only 116 floor and calendar normalization at 115.
The sender, actual endpoint and receiver preserve all independent checks.

Shared revision publication and reads, cached/prepared/evaluator checks,
remote-destination and receiver admission, catalog repair metadata and login
compensation, and `GetPythonUdfStatus` all require Python capability 118.
Predecessor-only and mixed 118/117 CNs are rejected before catalog mutation.
Ordinary SQL UDFs retain their pre-floor legacy fallback. The nine idempotent
shared-function entries and catalog repair 4.0.15 are unchanged. The rebase also
preserves upstream SQL UDF subquery admission and COLLATE expression binding;
it does not restore the removed Sirius Flight transport or add execution,
production or security scope to Python UDFs.

Independent Architecture and SQL/Planner approval of r17 remains pending.
Mechanical rebase, protocol allocation and validation do not supply it.

## r18 protocol and catalog allocation addendum (2026-10-17)

Official main `b111224b0c624773389b390cd3cf1223126b6ee2` owns MORPC 118
for transactional database defaults and table charset alteration, including
its catalog 4.0.15 migration. The governing
`python-udf-current-stage-r18-2026-10-17` revision preserves that handler and
all earlier published handlers unchanged. Python's single new capability is
119; its nine idempotent shared-function entries move to catalog 4.0.16,
whose minimum direct source is 4.0.15. The historical 4.0.7 entries keep their
original provenance and are not treated as current Python admission.

Shared revision publication/read, cached/prepared/evaluator checks,
remote destination/send/receive admission, repair metadata, login compensation
and `GetPythonUdfStatus` require capability 119. Immediate predecessor 118
and mixed 119/118 CNs are rejected before shared-function mutation. The
independent table-collation/defaults contract remains at 118; LEAST/GREATEST
remains at 117. Ordinary SQL UDFs retain their pre-floor legacy fallback.
The real login-repair regression covers tenant versions 4.0.12 through
4.0.15, rejection at 118, preserved legacy SQL results, and idempotent replay.

Generated plan bindings combine the upstream database-defaults message with
Python routine identities; the merged parser retains both charset DDL and
Python function syntax. This mechanical rebase adds no Python execution,
production or security scope. The earlier addenda describe their historical
allocations, not the governing r18 admission floor.

Independent Architecture and SQL/Planner approval of r18 remains pending.
Rebase, allocation and validation do not provide independent approval.

## r19 catalog allocation addendum (2026-10-18)

Official main `a8be04735bcf6d2a7aa48d464f76c0b13c691ef5` publishes
catalog 4.0.16 to refresh persisted COLLATIONS metadata. The governing
`python-udf-current-stage-r19-2026-10-18` proposal preserves that handler,
its protocol-118 admission and all earlier published handlers unchanged.
Python's nine idempotent shared-function entries move to catalog 4.0.17,
whose minimum direct source is 4.0.16. Official MORPC latest remains 118,
so the single Python capability remains 119; immediate predecessor 118 and
mixed 119/118 admission checks are unchanged.

The upgrade registry and scheduling oracle retain the explicit 4.0.15,
4.0.16 and 4.0.17 hops. Real first-login repair covers tenant versions
4.0.12 through 4.0.16, rejection at protocol 118 before mutation, preserved
legacy SQL results and two mutation-free replays. Upstream row-scoped scalar
warning diagnostics and test-harness cleanup ownership remain intact.

This allocation update adds no Python execution, production or security
scope. Earlier addenda record historical allocations. Independent Architecture
and SQL/Planner approval of r19 remains pending; rebase and validation do not
provide that approval.


## r20 protocol and shared-plan allocation addendum (2026-10-19)

Official main `75d16a2b875905ffa4121783dcda4f538d3c05f7` owns protocol 119
for coordinator-owned parameterized subquery programs and `Query.correlation`
field 19. The current `python-udf-current-stage-r20-2026-10-19` proposal preserves
both official allocations. Its single Python capability moves to 120, and
`Query.routine_dependencies` uses field 20. The merged protobuf descriptor,
marshal/unmarshal and deep-copy must preserve both contracts independently.

Catalog versions and entries are unchanged: all published handlers through
4.0.16 remain intact; the separate Python repair is 4.0.17 with minimum direct
source 4.0.16. Catalog repair, SQL revision read/write, prepared-plan reuse,
Python evaluator Prepare, remote sender/receiver and `GetPythonUdfStatus` use
capability 120. Immediate predecessor 119 and mixed 120/119 workers fail closed
before shared catalog mutation or Python execution. The main dependent-program
contract stays at 119 and the independent table/defaults contract stays at 118.

The acceptance evidence includes merged descriptor and mixed Query round-trip /
deep-copy checks, both compiler gates, predecessor rejection, and real login
repair from tenant versions 4.0.12 through 4.0.16 with legacy SQL UDF results and
idempotent replay. These tests do not establish cross-binary downgrade or
independent architecture approval. No new Python production, security or runtime
scope is added. Independent Architecture and SQL/Planner approval of r20 remains
pending; this allocation and rebase do not provide that approval.


## r21 catalog allocation addendum (2026-10-19)

Official main `def799e1260dd547cae2e0e54323ea20ec1b7685` publishes catalog
4.0.17 for the persisted COLUMNS MEDIUMINT metadata refresh. The governing
`python-udf-current-stage-r21-2026-10-19` proposal preserves that handler and
its entries byte-for-byte, including its protocol-118 admission metadata.
The shared COLUMNS entry used by 4.0.14 also retains the official implementation.
Python's nine idempotent shared-function entries move to catalog 4.0.18,
whose minimum direct source is 4.0.17. The registry retains both distinct hops;
no published handler is replaced by the Python repair.

Official MORPC latest remains 119, so the single Python capability remains 120.
Catalog repair, revision publication/read, Prepare, prepared reuse, remote
sender/receiver and status admission continue to reject predecessor 119 and
mixed 120/119 workers before Python catalog mutation or execution. Query fields
19 (correlation) and 20 (routine dependencies) remain unchanged.

Scheduling covers the 4.0.17 to 4.0.18 transition. The existing real first-login
fixture now includes tenant versions 4.0.12 through 4.0.17, predecessor rejection,
legacy SQL UDF and nested correlated results, and two mutation-free repair
replays. The COLUMNS fixture independently retains its published 4.0.17 boundary.
These tests do not establish cross-binary downgrade or independent approval.
No new Python runtime, production or security scope is added. Independent
Architecture and SQL/Planner approval of r21 remains pending; rebase and
validation do not provide that approval.


## r22 protocol and query allocation addendum (2026-10-19)

Official main `8e315d1fe5cdf6661bbf2802515695190dc1c221` owns MORPC 120
for current-session KILL authorization and proxy disconnect cleanup. The governing
`python-udf-current-stage-r22-2026-10-19` proposal preserves that behavior and
allocates the single Python capability 121. Catalog repair, revision publication
and reads, Prepare, prepared reuse, remote sender/receiver and status admission
reject predecessor 120 and mixed 121/120 CNs before Python mutation or execution.
The status client now explicitly enforces the same floor; its old mapping at 119
did not enforce the previously documented Python floor.

Official query methods 45/46, request fields 47/48 and response fields 48/49
remain unchanged. Python status uses method 47, request field 49 and response
field 50. Regenerated bindings, status wire fixtures and the existing combined
client fixture cover coexistence, immediate predecessor rejection, acceptance and
rollback. Independent dependent-program capability 119, table/defaults capability
118, plan correlation field 19 and routine-dependency field 20 remain unchanged.
Published catalog handlers through 4.0.17 and the separate Python 4.0.18 repair
(minimum source 4.0.17) retain their allocation.

Validation must refresh overlapping query, frontend, CN/proxy, admission and
login-repair evidence after this rebase. Unchanged worker, transport and storage
evidence retains only its original demonstrated scope. This maintenance adds no
Python runtime, production or security scope. Independent Architecture and
SQL/Planner approval of r22 remains pending; these repairs do not provide it.


## r23 protocol allocation addendum (2026-10-19)

Official main `873282742714436bff407dfcdbbe879147162bd3` owns MORPC 121
for exact unsigned DECIMAL DIV result domains. The governing
`python-udf-current-stage-r23-2026-10-19` proposal preserves that capability and
allocates the single Python capability 122 (`latest + 1`). Catalog repair,
revision publication and reads, Prepare, prepared reuse, evaluator, remote
sender/receiver and status admission reject predecessor 121 and mixed 122/121
CNs before Python mutation or execution. Rolling back the active floor from
122 to 121 disables Python again while retaining ordinary SQL and DIV 121.

Shared expression feature detection preserves both capabilities: DIV alone
requires 121, while a pipeline containing Python and DIV requires 122. The
coordinator-only dependent-program execution owner remains mandatory regardless
of capability. Upstream DIV overloads, precision, prepared inference, CTAS/view
schema boundaries and the legacy DIV rollback at 120 are unchanged.

Query methods 45/46/47 and their wire fields retain the r22 allocations. Published
catalog handlers through 4.0.17 and the separate Python 4.0.18 repair (minimum
source 4.0.17) also retain their allocation. This is a protocol admission and
integration repair, with no new Python runtime, production or security scope.
Independent Architecture and SQL/Planner approval of r23 remains pending; this
allocation repair and its validation do not provide it.


## r24 temporal/Python integration addendum (2026-10-19)

Official main `d82e828b0a4df9ce635fe573c1e90c5cde978bf9` owns MORPC 122
for unary TIMESTAMP and dynamic TIME result contracts. The governing
`python-udf-current-stage-r24-2026-10-19` proposal preserves that capability and
allocates the single Python capability 123 (`latest + 1`). Catalog repair,
revision publication and reads, Prepare, prepared reuse, evaluator, remote
sender/receiver and status admission reject predecessor 122 and mixed 123/122
CNs before Python mutation or execution. Rolling back the active floor from
123 to 122 disables Python while preserving temporal 122 and DECIMAL DIV 121.

Shared expression feature detection retains Python, temporal and DIV contracts.
Their combined pipeline requires 123; temporal-only requires 122 and DIV-only
requires 121. The existing execution-stream handshake carries the maximum
required by the encoded Python/temporal scope and vector partition contract;
a replacement receiver at 122 cannot admit a Python scope after a 123 probe.
No second handshake or protocol round trip is introduced. Coordinator-owned
dependent-program execution remains mandatory regardless of capability.

The merged plan schema preserves routine identities and query dependencies as
well as the upstream execution-only CTAS population scalar at CreateTable field
19. Temporal prepared binding, released overload identities, pre-fold CTAS/view
observation, borrowed-plan admission and schema-only clone cleanup retain their
upstream owners and protocol 122 gates. Query status wire fields and catalog
4.0.18 allocations are unchanged. This repair adds no Python runtime, production
or security scope. Independent Architecture and SQL/Planner approval of r24
remains pending; rebase and validation do not supply that approval.


## r25 nonblocking-lock/Python allocation addendum (2026-10-20)

Official main `c7bd2e7c1d9ca53daf80d640b3b307902d54161c` owns MORPC 123
for nonblocking row-lock admission. The governing
`python-udf-current-stage-r25-2026-10-20` proposal preserves that capability and
allocates the single Python capability 124 (`latest + 1`). Catalog repair,
revision publication and reads, Prepare, prepared reuse, evaluator, remote
sender/receiver and status admission reject predecessor 123 and mixed 124/123
CNs before Python mutation or execution. Rolling back the active floor from
124 to 123 disables Python while preserving nonblocking locks at 123, temporal
contracts at 122, and DECIMAL DIV at 121.

The merged plan schema retains upstream LockTarget no_wait/skip_locked fields
18/19 together with routine identities, query dependencies and CTAS admission.
Canonical generation and both plan admission postprocessors preserve the union.
Nonblocking locks and dependent expression programs retain their coordinator
execution owners; Python capability 124 does not authorize remote lock policy
execution. The existing stream handshake carries the maximum required feature
floor, with no new protocol round trip or lifecycle mechanism.

Catalog 4.0.18, its minimum source 4.0.17, query method/field allocations and
Python runtime scope are unchanged. The upstream SQL/planner and cancellation
harness changes require fresh integration evidence rather than unchanged-base
reuse. This maintenance adds no production or security scope. Independent
Architecture and SQL/Planner approval of r25 remains pending; rebase and
validation do not supply that approval.


## r26 fractional-TIMESTAMPADD/Python allocation addendum (2026-10-20)

Official main 743e749a1e33d6c845bb73999e8c82d297a2b657 owns MORPC 124
for exact fractional TIMESTAMPADD SECOND intervals. The governing
python-udf-current-stage-r26-2026-10-20 proposal preserves that capability and
allocates the single Python capability 125 (latest + 1). Catalog repair,
revision publication and reads, Prepare, prepared reuse, evaluator, remote
sender/receiver and status admission reject predecessor 124 and mixed 125/124
CNs before Python mutation or execution. Rolling the active floor from 125
to 124 disables Python while retaining fractional TIMESTAMPADD at 124,
nonblocking locks at 123, prior temporal contracts at 122 and DECIMAL DIV at 121.

The combined Python/fractional-temporal pipeline requires 125. Fractional-only
pipelines require 124, and rollback to 123 rejects them. The existing stream
handshake carries the maximum required feature floor; no new round trip or
resource-lifecycle owner is introduced. Coordinator-dependent programs keep
their independent ownership restriction.

The upstream function identity 601, literal/source normalization, format-3 dump
reader/writer contract and legacy epoch-specific bindings remain upstream-owned.
Fresh protocol composition, dump roundtrip and real SQL/prepared tests are
required after this merge. Catalog 4.0.18 and its minimum source 4.0.17, query
method/field allocations and Python runtime scope are unchanged. Independent
Architecture and SQL/Planner approval of r26 remains pending; rebase and
validation do not supply that approval. No production or security scope is added.


## r27 LOAD FK/Python allocation addendum (2026-10-21)

Official main 63a964a82fbaae4759cfabb45655e0185bc0979c owns MORPC 125
for LOAD IGNORE foreign-key row rejection diagnostics and builtin identity 602.
The governing python-udf-current-stage-r27-2026-10-21 proposal preserves this
capability and allocates Python 126 (latest + 1). Catalog repair, revision
publication and reads, Prepare, prepared reuse, evaluator, remote sender/receiver
and status admission reject predecessor 125 and mixed 126/125 CNs before Python
mutation or execution. Rollback from126 to125 disables Python while retaining
LOAD FK125, fractional TIMESTAMPADD124, nonblocking locks123 and temporal122.

Combined LOAD FK/Python pipelines require126; LOAD-only requires125 and rejects
124; fractional-only remains124. The existing execution stream carries the
maximum required feature floor, with no new RPC or lifecycle owner. Upstream
FK filtering barriers, diagnostic-expression traversal and function identity602
remain unchanged. The prepared FIELD history integration also resets retained
comparison history when a captured routine declaration changes and revalidates
bounded immutable routine facts before compile retry applies old history.

Catalog4.0.18, minimum source4.0.17, query method/field allocations, immutable
historical approval artifacts and Python runtime scope remain unchanged.
Independent Architecture and SQL/Planner approval of r27 remains pending;
maintenance validation does not supply that approval. No production or security
scope is added.


## r28 VIEWS/Python allocation and historical upgrade identity addendum (2026-10-21)

Official main 7038c8520e5fc1ba456914b3c389ccedb69e22f7 owns MORPC126
for parser-derived information_schema.VIEWS metadata and builtin IDs603/604.
The governing python-udf-current-stage-r28-2026-10-21 proposal preserves this
capability and assigns Python127 (official latest + 1). Catalog repair,
revision publication and reads, Prepare, prepared reuse, evaluator, remote
sender/receiver and status admission reject predecessor126 and mixed127/126
CNs before Python mutation or execution. Rollback127 to126 disables Python
while retaining VIEWS126, LOAD FK125, fractional TIMESTAMPADD124 and prior
contracts. Combined VIEWS/Python pipelines negotiate127; VIEWS-only negotiates126
and rejects125 through the existing stream, sender and receiver gates.

The historical4.0.7 handler keeps its released zero entry offset and protocol61.
Its Python schema entries are a separate helper list replayed only by the
protocol127-gated4.0.18 repair; the upstream VIEWS maintenance keeps its own126
fence. The old-worker regression uses the actual retained4.0.7 handler rather
than a synthetic replacement. Catalog4.0.18 and minimum source4.0.17, query
method/field allocations and Python runtime scope remain unchanged.

The prior r27 text described the intended historical-handler preservation but
its implementation still attached the Python list to4.0.7; this addendum and
its offset/worker/upgrade tests correct that discrepancy. Historical approval
records above remain immutable. Independent Architecture and SQL/Planner
approval of r28 remains pending; maintenance validation does not supply it.
No production or security scope is added.


## r29-r30 official-main rebase allocation addendum (2026-10-24)

The current official main retains MORPC 127 for IndexSearchScan internal_topk/max_internal_topk search knobs, MORPC 128 for category-preserving mixed JSON/numeric comparisons, and MORPC 129 for selected JSON ELT and opaque-text expression execution. This rebase preserves those sender, receiver and Prepare gates and allocates the single Python UDF capability at MORPC 130 (latest + 1).

Catalog revision publication and reads, prepared-plan reuse, evaluator Prepare, remote sender/receiver, and GetPythonUdfStatus reject predecessor 129 and mixed 130/129 CNs before Python mutation or execution. Rolling the active floor from 130 to 129 disables Python while retaining the three official main capabilities. Combined pipelines negotiate the maximum required floor through the existing stream protocol; no new round trip or lifecycle owner is introduced.

The merged plan descriptor and parser artifacts preserve both the official main schema additions and the Python routine fields. Focused protocol, queryservice, UDF and generated-plan tests pass; full planner/frontend execution remains dependent on the repository native CGo facility. Independent Architecture and SQL/Planner approval remains pending and is not substituted by this maintenance rebase.


## r29 — 2026-10-25 official-main protocol preservation

Governing artifact: `python_udf_current_stage.md` r29. Official main
`6fb6531432aaec8c1fbd98a30364941559e0e1d6` allocates MORPC 130 to incomplete
temporal strings. Python uses MORPC 131, exactly main latest + 1. Catalog
4.0.18 scheduling and repair, revision publication/read, prepared reuse, physical
Prepare, destination admission, execution-stream negotiation and receiver checks,
and GetPythonUdfStatus use this floor. Immediate predecessor 130 rejects Python;
rolling the active floor from 131 to 130 disables Python before side effects.
Main's JSON 128/129 gates, temporal 130 gates and vector session-knob propagation
are preserved. Combined payloads negotiate the maximum capability on the bound
execution stream. This allocation changes no lifecycle owner or round trip.

Independent Architecture and SQL/Planner approval remains pending. This addendum
records the rebase and mechanical allocation; it does not supply independent
approval or extend the accepted development scope.


## r30 — 2026-10-25 official-main weighted aggregate protocol preservation

Governing artifact: `python_udf_current_stage.md` r30. Official main
`9a7e12a93328a69bac7261beedc88930b7984a10` assigns MORPC 131 to exact
weighted percentile aggregate dispatch. That published capability and its sender,
placement and stream gates remain at 131. Python and shared routine revision
repair use MORPC 132, exactly main latest + 1. Catalog 4.0.18 scheduling,
Prepare and prepared reuse, publication/read, destination admission, receiver
checks, execution-stream negotiation and GetPythonUdfStatus share this floor.
The immediate predecessor 131 rejects Python before side effects while retaining
weighted aggregate dispatch; rolling the active floor from 132 to 131 disables
Python. Combined pipelines require the maximum capability of their contents.

The maintenance rebase also retains upstream temporary-table ownership repairs,
SHOW metadata typing and quoting, session timestamp precision, and weighted
prepared percentile handling. It introduces no new runtime owner or round trip.
Independent Architecture and SQL/Planner approval of r30 remains pending; this
mechanical allocation does not supply that approval or extend development scope.


## r31 — 2026-10-26 official-main float conversion protocol preservation

Governing artifact: `python_udf_current_stage.md` r31. Official main
`861a2c4cb68961d8b6b01f18e0aa6ac52a3a505f` assigns MORPC 132 to decimal
rounding and strict textual float conversions. That published capability retains
132 across detection, destination admission, execution streams and receivers.
Python execution and shared routine revision repair move to MORPC 133, exactly
main latest + 1. Catalog 4.0.18, Prepare and prepared reuse, revision publication
and reads, remote sender/receiver admission and GetPythonUdfStatus share 133.
Query method 47 and protobuf field identities are unchanged.

The immediate predecessor 132 rejects Python and shared revision publication,
including rollback from 133, while retaining upstream float conversions and
weighted aggregates. Combined pipelines negotiate the maximum capability.
The rebase preserves upstream statement DML classification, diagnostic-bearing
float folding, view restore creation context and Prepare hooks, outfile audit
receipts and ISCP lifecycle/timing changes. No new runtime owner is introduced.
Independent Architecture and SQL/Planner approval of r31 remains pending; this
mechanical allocation does not supply approval or expand the development scope.


## r32 — 2026-10-27 shutdown-only upgrade contract

Governing artifact: `python_udf_current_stage.md` r32. The repository upgrade
contract requires full-cluster shutdown; concurrent old/new in-cluster binaries
and downgrade are unsupported. This revision removes the PR-introduced MORPC
133, Python/shared-revision local and remote admission, destination probes,
protocol-driven SQL fallback, and upgrade protocol prechecks. MORPC latest
returns to the unchanged official baseline 132. No unrelated existing gate is
removed or extended.

Catalog 4.0.18, its minimum source 4.0.17 and nine idempotent repair entries
remain, including first-login repair and historical schema/snapshot reads.
Immutable revision, account, namespace, descriptor and corruption checks remain.
External Flight wire/ABI/schema/environment negotiation remains exact. The
status query retains its method/field identities without feature admission.
Historical per-feature predecessor/rollback evidence is superseded; retained
round-trip, catalog, evaluator and external-contract oracles test the supported
behavior. This revision does not grant independent Architecture/SQL Planner
approval or expand the authorized test/development scope.

## r33 — 2026-10-28 bounded group retirement

Governing artifact: `python_udf_current_stage.md` r33. Completion previously
retained each distinct statement/evaluator group for the Gateway and worker
lifetimes, exhausting their bounded fence maps permanently. Expiring immutable
group IDs now provide the rejection authority required to retire inactive
fences safely. Both admission owners reject new Opens after the deadline,
including changed invocation IDs and higher epochs; already admitted work keeps
its cleanup ownership. Nondecreasing owner clocks prevent resurrection after
wall-clock rollback, and existing terminal-record TTL and account isolation
remain enforced. The evaluator renews transport identity before expiry and the
external capability handshake requires `expiring-v1`. No persisted catalog,
MORPC, production isolation or deployment scope changes. Existing upstream
protocols through 134 remain unchanged. Independent Architecture and SQL/Planner
approval of r33 remains pending; this maintenance fix does not supply approval.

## r34 — 2026-10-28 encoded-size and frozen-type correctness

Governing artifact: `python_udf_current_stage.md` r34. Encoded catalog TEXT and
external Open bounds are checked before accepting definitions. TEXT/BLOB widths
preserve SQL byte capacities, while CHAR/VARCHAR keep character semantics.
String-domain analysis uses frozen RoutineCall return metadata instead of
passing user-only arguments to the legacy Python overload checker. No persisted
format, MORPC or deployment scope is added. Existing width-zero descriptors are
not rewritten. [Review 5673875427](https://github.com/matrixorigin/matrixone/pull/29152#pullrequestreview-5673875427)
accepted the development-stage design and identified these concrete defects;
that assessment does not assert implementation approval of this correction.
Independent Architecture/SQL approval facts remain as recorded above.


## r35 — 2026-10-28 merged Query field allocation

Governing artifact: `python_udf_current_stage.md` r35, based on official
`66c9e7624e3374897acc649195fde5b2836343a5`. Official Query field 20 now carries
integer precision. The unmerged routine dependency envelope moves to field 21;
correlation remains field 19. Generated descriptors and executable-plan
serialization preserve both metadata contracts. Catalog revision storage and
full-cluster shutdown upgrade remain unchanged; previously emitted unmerged PR
plan/cache blobs are not a supported persisted format. No Python MORPC feature
gate, old-tag fallback or independent approval is introduced. Historical
feature-owner scope approval is retained; independent Architecture/SQL approval
of this revision remains pending.


## r36 — consolidated catalog predecessor (2026-10-28)

Current artifact: `python-udf-current-stage-r36-2026-10-28` in
[python_udf_current_stage.md](python_udf_current_stage.md).
The prior exact-parent [review 5678129549](https://github.com/matrixorigin/matrixone/pull/29152#pullrequestreview-5678129549)
accepted the development-stage design and confirmed the earlier correctness,
MORPC and group-lifetime findings resolved. That review remains evidence for its
reviewed revision, rather than an approval inferred for this addendum.

Official main consolidated the released-source catalog route into frozen
`4.0.7`. This revision retains that handler/list/offset and moves the routine
repair helpers into their own `4.0.18` owner with minimum source `4.0.7`.
It preserves predecessor CDC, offset/recovery and snapshot preconditions when
that later repair is the final target. Unregistered unpublished intermediate
source catalogs fail before cluster/account mutation, including locked rereads,
recovery routes and scheduled batches. Login/manual paging continues to the
final committed repair before completion is cached. Restore combines upstream
source SQL-mode inspection and procedure projection with the existing immutable
UDF heads/descriptors and revision-row validation.

The Query allocation, catalog row formats, external worker protocol and
shutdown-only upgrade policy are unchanged. No feature MORPC gate, production
isolation or Operator scope is introduced. Independent review of this revision
is tracked separately; no owner decision is fabricated by this record.


## r37 bounded artifact publication repair (2026-10-29)

The current proposal validates worker definitions before persistent publication
and adds a backing-store-wide lifetime admission budget of 1,024 account/digest
identities (at most 1 MiB each). Immutable exclusive-create slots survive failed
transactions, process restart and account/database changes. They are not
reclaimed on REPLACE, DROP or restore, so no failed publisher can delete source
shared by another revision or snapshot. Admission exhaustion fails closed for
new objects; existing objects remain readable and republishable. Pre-r37 objects
are preserved as a finite baseline; this bounds additional growth rather than
performing retrospective garbage collection. The exact state/size/compatibility
contract is in the governing r37 proposal. This is a resource-correctness repair
within the existing development scope, not a production quota/GC service.
Independent Architecture and SQL/Planner approval of r37 remains pending;
implementation review and tests do not grant that approval.


## r38: preserve the published 4.0.18 predecessor (2026-10-30)

The governing proposal is `python-udf-current-stage-r38-2026-10-30`. Official
main `bbfe55669a1440424914d45a283a1d0e36f722fa` now registers 4.0.18 for
the attributed-DECIMAL information_schema.COLUMNS repair. Preserve that
handler, entry list and offset exactly. The nine shared-routine entries move
to 4.0.19, with minimum direct predecessor 4.0.18; earlier released sources
continue through both official predecessors before routine repair. Real first
login and idempotence controls include an already-4.0.18 tenant. Frozen
4.0.7/retired-source/CDC admission safeguards and upstream iterative paging
continuation remain intact. Regenerate the plan descriptor from merged proto
so official numeric attributes coexist with RoutineCall and dependencies.
Reject attributed DECIMAL declarations at the external-routine type boundary
instead of silently dropping their plan-side attributes; no ABI extension is added.

This is a mechanical namespace/conflict repair, without new Python protocol,
artifact-budget, sandbox or production scope. MORPCLatestVersion remains the
official 134; no per-feature mixed-version gate is introduced. Historical r1
acceptance and subsequent records are retained. Independent Architecture and
SQL/Planner approval of r38 remains pending; this maintenance does not assert
or supply that decision.


## r39: preserve the published 4.0.19 metadata repair (2026-10-31)

The governing proposal is `python-udf-current-stage-r39-2026-10-31`. Official
main published the independent declared COLUMNS/STATISTICS metadata repair
in 4.0.19. Its handler, two entries and offset remain byte-for-byte official,
as do the frozen 4.0.7 and official 4.0.18 handlers. The nine unchanged routine
repair entries move from the unpublished candidate 4.0.19 to 4.0.20, whose
minimum direct predecessor is 4.0.19. Earlier released sources complete the
registered predecessor chain; an already-4.0.19 account receives the routine
repair on scheduled upgrade or first login. Existing unfinished-route CDC
admission, retired-source rejection and commit-only completion caching remain
in force. The real login/replay controls include official 4.0.19 as a source.

The merged plan descriptor retains both the official Boolean EXISTS marker
and this proposal's typed routine/numeric metadata. No new Python MORPC gate
is introduced; official latest remains 134 and upgrade is full-cluster shutdown.
The opt-in development scope and permanent artifact admission limit are unchanged.
Independent Architecture and SQL/Planner approval of r39 remains pending; this
maintenance does not assert independent approval or broaden production scope.
