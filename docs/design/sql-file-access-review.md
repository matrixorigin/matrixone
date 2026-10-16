# SQL file access design review record

Issue: #30185. Implementation PR: #30212.

This records the distinct design-first decisions for the complete change. The
current consolidated design is `sql-file-access.md`, blob
`96fd5ecab87c549cf59751d81bb72161fb074b7c`, delivered with this record. These approvals permit implementation;
they do not turn missing runtime evidence into a pass.

## Recorded decisions

The model and effort below were checked against each local CLI rollout's
`turn_context`; no collaboration subagents were used. Original records and
command receipts are retained in the issue30185 task ledger.

| Phase | Model / session / turn | Decision and scope |
| --- | --- | --- |
| Owned artifact persistence | gpt-6.1-sol / xhigh; `01a13c78-a53d-77e2-847a-a3d28785ac57` | DESIGN ACCEPT before replacement implementation. Existing artifact owners pass owned ReaderAt ranges through immutable binary parameters and the existing transaction. No generated SQL identity elevation. The initial 32 MiB ceiling was subsequently reduced to 8 MiB using concurrent measurements; chunk/format/row contracts remain unchanged. |
| Async Datalink lifecycle | gpt-6.1-sol / xhigh; session `01a13d9a-1166-7523-bb54-1a7853a59a9d`; turn `01a13da8-36d8-7b81-8fd4-7144c6f024bb` | DESIGN ACCEPT for stateless remote-only async source admission at CREATE/rebuild and final stored DML values, plus acquisition. Foreground local-to-TEXT remains supported. External invalidation uses existing failure/retry behavior; no history scanner or locking protocol. |
| Provider dependencies | Same session; turn `01a13dd7-1b58-7dc0-9afd-c5b5f4c683b1` | Corrective design approved before implementation: shared argument/backend ownership, SQL caller-only SDK construction, explicit upstream role credentials, error propagation and refresh consistency. Existing HDFS simple and explicit MinIO anonymous modes are compatible. |
| Mandatory acquisition | gpt-6.1-sol / xhigh; session `01a13de3-bded-7bf2-a270-ff201e5eff53`; turn `01a13de3-be66-7212-bdf7-d487d0092bbe` | Missing policy rejects all generic acquisitions. SQL dynamic certificate/keytab/profile/instance selectors are rejected for all roles. This supersedes the earlier privileged dynamic-provider allowance. |
| Acquisition-only policy | gpt-6.1-sol / xhigh; session `01a13e1d-15a0-7d32-abd2-23e15f0f79c3`; turn `01a13e1d-163e-73a1-b0ee-dc6849022238` | DESIGN APPROVED. Derive scoped immutable policy only in file operations, using existing current identity propagation. Remove global policy/query/pipeline hooks and missing-context trusted fallback. External and CRT owners must bind their actual acquisition contexts. |

The final acquisition-only review explicitly withheld WIP delivery approval.
The implementation then removed those hooks, bound both External contexts after
remote reconstruction, bound CRT's temporary parameters, and deleted the
unnecessary backup adapter. No policy is stored in catalog or wire metadata.

## Local first-principles correction

The active implementation review rechecked compatibility against the actual
constructors, rather than preserving unnecessarily broad restrictions:

- HDFS URL/stage encoders provide endpoint-only paths; the constructor uses a
  literal `hadoop` default without credential/configuration discovery. Preserve
  this simple mode while rejecting a local keytab dependency.
- MinIO already supports anonymous access with an explicitly empty credential
  chain. Preserve it only with explicit `no-default-credentials=true` and no
  credential/upstream-role input; missing or partial object-store credentials
  otherwise fail. No new anonymous mode was added to another SDK.
- AWS credential caching suppresses request cancellation, and MinIO STS creates
  context-free requests. SQL-scoped initial/refresh requests borrow their
  operation lifetime. AWS refresh does not retain the constructor's canceled
  timeout context; each refresh has its own bounded timeout.
- Early cancellation preserves MatrixOne's existing cancellation error identity.
  Existing Parquet sibling-cancellation and allocation-release oracles remain.
- Preserve backup's existing `is-hdfs` selector while retaining ETL's historical
  service-name selection. One parser takes the owning factory mode; admission
  and construction consume the same result. A shared parser must not silently
  change an existing factory's provider choice.
- Remove unused context-free file-reader wrappers. Keep existing native storage
  owners separate, including the explicitly guarded local table-dump owner.

These corrections add no worker, shared lock, grant cache, persistent state or
normal-query policy lifecycle. They align the earlier provider compatibility
approval with the final acquisition owner. Actual source/parser, factory,
current-role, public SQL, cancellation and refresh controls verify the contract.

## Delivery criteria

The final validation record must identify its tested source revision, normal
and race results, actual public SQL and two-CN execution, ordinary-workload A/B,
artifact persistence measurements and reused evidence. All failed attempts and
baseline failures remain visible. CPU evidence does not validate GPU-native
code. The user approved PR update with GPU native verification explicitly
pending because no suitable machine is available. Remote CI remains a separate
result after publication.

## Bounded classic async admission amendment

For review #5531071132, gpt-6.1-sol / xhigh accepted the focused correction at
`0cf0c8b2478a9dc96481178b9079338d40f4a1f1`, base `83222fa8371`.
Session `01a13eff-93e6-7e10-a1b2-435e225e8373`, turn `01a13eff-9450-7663-a473-7c4971292f11`; actual rollout
`turn_context` verifies model and effort. The retained design artifact has
SHA256 `ce527e154a0f58bcc00fd5bf3dbe4b42c541d7a2b49f4be492b114aaed8d6970`.

Aggregate each existing checker expression with SUM(CAST(... AS UNSIGNED)).
The complete scan and existing failure/CDC ordering remain; the discarded
nonstreaming result is one fixed-width aggregate row rather than one row per
input. Existing owners, transaction, identity and cleanup are unchanged. This
introduces no generic executor API, worker or ordinary-query path. Regression
uses the existing cluster, the production plugin hook and real internal executor.

Final overall review: **PASS**, no unresolved material blocker for PR update,
gpt-6.1-sol / xhigh session `01a13f10-39ff-7c02-be5c-27701acebe74`, turn
`01a13f10-3ab3-7e63-a0f3-c29fd8c8bf89`. The review reconciles the complete
change map, unchanged earlier reviews, four amended Go fingerprints, normal/race
terminal success and all 22 CPU static results. Review artifact SHA256:
`30a5071965f3453e41841f068c3351d61fe3028c8568461a2812537e973a0237`.
GPU-native validation and historical performance limits remain as stated.

## CSV cleanup amendment

For review #5531899835, gpt-6.1-sol / xhigh accepted the focused correction at
`23bef8dc17d`, base `83222fa8371`: retain the CSV operation policy, derive
uncancelled cleanup with the existing 30-second context budget, close only
owned reacquired ETL services and leave borrowed SHARED with its owner.
Session `01a13f10-39ff-7c02-be5c-27701acebe74`, turn `01a13f3b-7969-7803-aada-96df9aea6405`; actual
rollout metadata verifies model/effort. Design artifact SHA256:
`5ba6c388bbdd7344370da736c05e6174d81ef314f25b455b1309e5deb34d5fb3`.

Final CSV overall review: **PASS**, gpt-6.1-sol / xhigh session
`01a13f10-39ff-7c02-be5c-27701acebe74`, turn
`01a13f46-d15a-7232-a912-3d0f0f9c258d`. It verified normal/race terminal
success, all 22 CPU static checks, the real CSV owner regression and borrowed
service cleanup. Review artifact SHA256:
`0f6bc395a76f97b7491ef2e8b6fd6532d25f0401f9e10cb4ed7750f894eda652`.

After rebase to `eb18dbfa030`, the same gpt-6.1-sol / xhigh session
reconciled the 12 identical patches, unchanged 132 Go fingerprints and fresh
focused normal/race plus 22 CPU static receipts: **PASS**, prior approval valid.
Turn `01a13f4c-b0e7-70e3-bbcc-ea61a8b02e76`; reconciliation artifact SHA256:
`d849e93e98ead0efa754d8b2a1fb67069acbab97cce93c5c50dee5c6fa3fdb27`.

## Changed-column admission and parsed URL reuse

Design accepted before this amendment by gpt-6.1-sol / xhigh, session
`01a14284-3523-71e1-a83b-bda47043296f`, turn
`01a14284-35aa-70a1-813e-44d98e3110e4`; actual rollout metadata verifies
model and effort. Reviewed source `a2541cfeef5` on main `c46b893f616`,
including the previously recorded reader-only candidate.

The final-row owner selects assigned async Datalink columns from its existing
projection map. Unchanged sources are not revalidated by unrelated UPDATE;
recomputed values and repeated writable physical aliases retain admission.
Datalink and stage resolution share already parsed operation-local URLs without
changing CSV generation, stage-cache lifetime or acquisition policy. There is
no new runtime state or execution owner. Earlier rejected optimization and
performance records remain valid historical outcomes. Final readiness requires
new functional, static and complete-candidate performance evidence.


Rebase reconciliation: all 12 existing patches remain identical after rebase to
main `ac55588b8d4` (head `380740aac56`). Current-base functional and static
receipts are recorded separately from the prior source review. The complete
Darwin performance cohort did not meet acceptance; two independent Linux
preparations were invalidated by other compilation. Source acceptance does not
waive the incomplete performance gate. The validation record retains these
limits and the user-approved GPU-native exception.

Final amendment review: **SOURCE ACCEPT; OVERALL NOT READY**, actual
gpt-6.1-sol / xhigh, session `01a14284-3523-71e1-a83b-bda47043296f`,
turn `01a1436d-7a01-7e80-bed0-1f7a26ba7203`. Artifact SHA256:
`5d5e12f25781fa8298f5565e7011aae05ffbf6d6b3e92fb9186890f499553af8`.
It reconciles all 12 unchanged rebased patches, 135 Go fingerprints, final
normal/race/public/static receipts and the retained failed performance cohort.
No new material source blocker remains. End-to-end performance is still a
mandatory unresolved gate; this source decision does not authorize a push or
waive that gate.
