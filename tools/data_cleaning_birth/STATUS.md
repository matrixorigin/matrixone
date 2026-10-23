# Restricted DEFAULT false candidate: Draft publication

This is an independent follow-up to [#30816](https://github.com/matrixorigin/matrixone/pull/30816),
related to [#30518](https://github.com/matrixorigin/matrixone/issues/30518).
Keep this change Draft / do-not-merge until the dependency, compatibility,
current-head CI, review and acceptance gates are resolved.
This document describes the DEFAULT false extension; inherited M0 status notes
describe the prerequisite NULL-suffix candidate and are not this extension's
validation record.

## Identity and dependency

- Integrated source foundation: `e1d1ea829170cec09ed681eb1defce38135eb155`.
- Frozen COPY control D3: `64aa85d35f926136bdb249ece3839307fedef1ca`.
- Frozen metadata-only candidate C3: `0343d680a7bce6763e6aea4c513601212bf95f3a`.
- Shared ordinal / derived-View correction from #30816:
  `b3c86e3f3375f6441d3326d08ac23ac6598d4d1d`, applied equivalently to D3/C3.
- D3-to-C3 binary diff SHA-256:
  `41364e814f9ac0091ad5c12a65a6db375a30733a7181a812a57e3e96c5746aa5`.

The publication commit adds this document and corrects gofmt indentation in
`m0_default_copy_test.go`, without changing test logic. It does not rebuild or
change the measured product. The independent branch preserves candidate history and
does not update #30816. Until that dependency merges, the PR against main also
contains prerequisite M0 changes; they must not be attributed to this extension.
Main has advanced independently. Any later integration requires delta review
and revalidation of affected evidence, not relabelling these binaries.

## Supported behavior

Under `mo_m0` and a valid isolated deployment admission, allow one trailing
nullable BOOLEAN column with literal `DEFAULT false` in an independent
pessimistic RC autocommit DDL, subject to the existing M0 table/layout restrictions.
Ordinary builds do not enable this optimization. Legal default-algorithm
operations outside eligibility select COPY before catalog mutation; an explicitly
requested unsupported INPLACE operation is rejected.

The immutable historical missing-column value is distinct from the current
SQL INSERT default. `SchemaExtra.append_missing_state` is authoritative through
`mo_tables.extra_info`; projected schema and captured logtail write metadata carry
the required identity/version information. Existing physical values, including
NULL, win over synthesized values. Only a proved complete old physical prefix
may receive a typed missing suffix; malformed layouts fail closed.

Readers, writers, clone, merge, rollback and recovery preserve the state. Later
unsupported legal DDL uses a standard COPY exit in the same user transaction:
read with the old fill semantics, materialize a complete ordinary target and
clear the target append state, while preserving old snapshots and child clones.
No general expression/type expansion or new concurrency/merge policy is included.

## Compatibility limits

This extension adds durable schema/protobuf metadata. It is not safe to infer
old-reader compatibility from protobuf unknown-field handling.
Only a controlled, static, same-executable LOG/TN/CN deployment with the guarded
launcher and bound data roots is qualified. An older binary cannot retroactively
recognize the new guard. The exclusive owner must enforce launcher use; this is
not a security sandbox or universal downgrade protection.

Mixed versions, rolling upgrades, downgrade, arbitrary recovery tools and
production enablement are not qualified. Do not open candidate data with the
COPY control or another binary. Admission failures must not be bypassed.
See [the launcher documentation](README.md) for the isolated prototype contract.

## Reused correctness and review evidence

- Independent review of the feature and generated-field contracts found no new
  blocking defect within the controlled prototype scope. Its known public-ordinal
  blocker was subsequently repaired; shared corrective review and focused
  Linux regression evidence close that blocker without rewriting the old failure.
- Local targeted unit/SQL/planner checks and relevant race evidence are reused
  by unchanged content. The latest corrective run covered four SQL roots,
  five planner roots and eight wrapper checks; this is not a full-suite claim.
- Latest Linux D3/C3 qualification covered seven focused roots, stock-release
  schema/content/NULL/parent-child isolation, ordinary catalog compatibility,
  same-binary restart and COPY exit. Each arm executed 48 direct SQL statements
  with one expected constraint error and no unexpected SQL error.
- Large fixtures subsequently validated sorted full contents, schema, isolation
  and same-binary restart for zero-match and real 60,000-row UPDATE/DELETE paths.
  This is not full SF100 branch-content acceptance or crash/power-loss testing.
- The canonical BVT extension is
  `test/distributed/cases/ddl/alter_table_algorithm_lock.sql` and `.result`.
  It covers fallback/default/NULL behavior and unsupported explicit INPLACE.
  Official mo-tester execution of that file is not claimed. Ordinary CI alone
  does not validate the guarded `mo_m0` optimization.

Relevant real-SQL tests include `TestM0BirthFalseSQL`,
`TestM0BirthFalseFallbackSQL`, `TestM0BirthFalseTransactionFallback`,
`TestM0BirthFalsePublicOrdinalsSQL`, the private/TN-applied rollback and
cancellation tests, and `TestM0DefaultFalseCopySQL`. They require the corresponding
tags, native setup and isolated admission; selecting zero tests is not validation.

## Sealed exploratory performance, not full-workflow acceptance

Same frozen D3/C3 products, resource/cache contract and independently copied
closed control inputs; one pair per fixture in each exploration. The timed chain
is ADD, first DML, main evaluation and a fixed cross-branch read. It excludes
branch creation, full validation, restart, DROP and shutdown. A separate 60-second
observation is included only in the cumulative CPU/write figures below.

| Fixture | Chain D -> C (seconds) | Chain speedup | Cumulative MO CPU change |
| --- | --- | --- | --- |
| Previously cleaned UPDATE input | 0.488186 -> 0.142227 | 3.432x | -29.08% |
| Previously cleaned DELETE input | 0.523958 -> 0.145952 | 3.590x | -29.17% |
| Actual UPDATE of 60,000 NULL rows | 0.859562 -> 0.501963 | 1.712x | -28.53% |
| Actual DELETE of 60,000 NULL rows | 0.756028 -> 0.364174 | 2.076x | -26.64% |

The real-DML inputs contain 3,000,000 rows / 60,000 NULLs; full-content oracles
verify the exact changes and unchanged parents. They differ from the cleaned
2,940,000-row fixtures and are not extra repetitions of the same comparison.
Successful LocalFS write-call stream bytes fell from 58.17 to 1.44 MB for UPDATE
and from 57.02 to 1.01 MB for DELETE. This is a process-level successful-call
counter, not total device I/O, WAL, object-level write amplification or all failed
write cost. Host swap warnings remain part of the sample limitations.

First-read differences remain: the real UPDATE main evaluation was 4.594 ms
slower; DELETE's fixed cross-read was 2.988 ms slower in its single pair.
Earlier same-arm repeats narrowed some gaps but had different cache/merge
histories and do not prove the cause or stable neutrality.
Unadmitted merge work and long-term debt remain unmeasured; 60 seconds is not
natural drain. No statistical significance or paper-performance recovery is claimed.

Evidence archive SHA-256:

- D3/C3 corrective Linux qualification (212 verified files):
  `f5904fe7514c82943c836ab19f31907222f04a89cf93f0ceb105634760db4b10`.
- Read-path exploration (330 verified files):
  `27a2091a3bbb85a05afc77f53d14b2ff1372e8bbb36f42e4ba2faf8e3d3c13d0`.
- Matched-DML exploration (331 verified files):
  `8841f1424d94797a9499b4835425ef3be4f35d5f5c7a36eb3a156a5fafa4acdf`.

## Remaining gates at publication preparation

The formal two-fixture / three-pair screen is in progress in a separate execution
session and is not counted as completed acceptance here. Its failed resource-
attribution pair is retained and excluded as a pair; it is not spliced into
valid samples. A full 10-worker x 20-step first/warm pilot is not yet accepted.
No new experiment or compilation was started for this Draft publication.

Before Ready: resolve prerequisite integration; review any resulting semantic
delta; complete applicable ordinary and guarded-mode CI/BVT; close the formal
performance decision and deployment/compatibility contract with maintainers.
QA is required for persistent metadata, clone and recovery semantics. This Draft
is not QA acceptance, production enablement or permission to merge.
