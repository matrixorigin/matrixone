# 4.2 branch identity design review

Decision: **accepted for implementation** for revision
`PR32723-BRANCH-IDENTITY-42-2026-11-13-r1` at commit
`423793425c333b0a5e3acd1ab2039ef0d264f192`.
SHA-256 of `data_branch_identity_42.md`:
`b7e90a5db10fbe066615d2d0cca5ec37ee96cb5a35055b5770b7166fce37cdb1`.

This is the implementing root agent's distinct design-first review, not an
independent external review or an implementation approval. No implementation of
this adaptation preceded the reviewed document. The review follows mo-dev's
feature/major-refactor contract because persisted identity, authorization,
catalog locking and logical restore are affected.

The invariant is owned by the existing database catalog row. Reusing dat_type
avoids a new schema or synthetic table lineage. The design keeps ordinary-table
receipt validation, fail-closed handling of old empty databases, tenant/account
resolution, and existing privileges. It rejects name inference and fallback DROP.

Producer/consumer closure includes internal CREATE, DELETE under the existing
catalog-key lock, publication CREATE/ALTER, statement context restoration and all
three 4.2 logical restore readers (snapshot, timestamp and PITR). Account/cluster
entry preflight and table-only destination identity need explicit validation.
The 4.2 adaptation must not copy main's historical v75 mixed-version gate: the
current repository upgrade contract supports shutdown upgrade only.

Resource and wait review: reuse the transaction's background executor/process;
release result sets, vectors and the temporary process lexically; hold the lock
only until commit/rollback. Reject an optimistic transaction rather than silently
proceeding when LockRows would be a no-op. RC lock refresh/retry must use the
existing owner. No new worker, cache, retry loop or persistent table is accepted.

There are no unresolved design decisions. Implementation remains blocked from
delivery until the document's scenario map, source-release compatibility,
owning normal/race tests, static checks and real SDK/BVT lifecycle/restore proof
are complete. Any material deviation requires a new reviewed document revision.
