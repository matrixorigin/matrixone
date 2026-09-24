# Python UDF current-stage design approval record

| Field | Value |
| --- | --- |
| Record | `python-udf-current-stage-r1-2026-09-24-approval` |
| Issue | [MatrixOne #28132](https://github.com/matrixorigin/matrixone/issues/28132) |
| Implementation PR | [MatrixOne #29152](https://github.com/matrixorigin/matrixone/pull/29152) |
| Current-stage contract | [`python-udf-current-stage-r1-2026-09-24`](python_udf_current_stage.md) |
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
The final current-stage design blob is
`994b8016729d2e87a6a706a2833eee7faf50c89b`; its only difference from the
approved design blob is the link to this traceability record.

The namespace-validation companion is the implementation contract from commit
[`284a1d971a1d2c763c6aed425d52f751735c409f`](https://github.com/matrixorigin/matrixone/commit/284a1d971a1d2c763c6aed425d52f751735c409f),
with current-head blob `51ccfb771db19321d03b292cd3d7219f698a11b8`.

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
