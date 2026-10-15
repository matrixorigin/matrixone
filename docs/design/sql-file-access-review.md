# SQL file access design review record

Issue: #30185. Implementation PR: #30212.

This records the distinct design-first decisions for the complete change. The
current consolidated design is `sql-file-access.md`, blob
`012635892883493679c682a7ecf36bb3ca50fb1a`, delivered with this record. These approvals permit implementation;
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
