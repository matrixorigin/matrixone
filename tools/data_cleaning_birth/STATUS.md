# Restricted DEFAULT false candidate: integration and acceptance

This is an independent follow-up to [#30816](https://github.com/matrixorigin/matrixone/pull/30816),
related to [#30518](https://github.com/matrixorigin/matrixone/issues/30518).
Ready-for-review requires the final integration and validation gates below.
Merge remains blocked on the prerequisite, maintainer review and compatibility
acceptance; Ready does not remove `do-not-merge/wip` or grant production use.
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

The original publication preserved C3 product identity. The 2026-10-26 integration
merges prerequisite `4c9092203a49f5bba5fcd8e1d2f2a8c6bd0c4982` and pinned main
`5fe08a3307e246e1ab9951ab690b50557af0f509`, retaining both cluster-key replay and
captured write-schema semantics. Scan proof errors and standalone CREATE INDEX
COPY exit are repaired. Ordinary COPY avoids unnecessary target schema reads;
nil catalog extra metadata and standard admission error handling are corrected.
The independent branch does not update #30816. Its unmerged changes remain in
the main-based diff and must not be attributed to the false extension. Integrated
source validation is recorded separately from the measured D3/C3 binaries.
See [the versioned design](DESIGN.md) and [distinct design decision](DESIGN_REVIEW.md).

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
unsupported legal DDL on false-history tables uses a standard COPY exit in the same user transaction:
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

## Completed formal screening and full pilot (frozen D3/C3)

The sealed formal screen has twelve valid arms, three pairs for each cleaned
2,940,000-row UPDATE/DELETE fixture. Median original-chain speedups are 3.626x
and 3.631x; frozen content, mechanism, read, continuation, cumulative CPU/write
and lifecycle gates pass. An invalid first DELETE pair was retained and replaced
as a whole under the one-retry environment rule. These zero-match fixtures do
not represent actual DML writes; the separate 60,000-row checks above do.

One full SF100 data_cleaning pair (10 workers x 20 steps) completed:

| Phase | D3 seconds | C3 seconds | Speedup | Business MO CPU D3 -> C3 seconds |
| --- | ---: | ---: | ---: | ---: |
| process-first-run | 112.61 | 24.51 | 4.59x | 876.50 -> 169.38 |
| warm | 113.44 | 23.82 | 4.76x | 882.85 -> 168.47 |

Each round completed all 200 steps, zero unexpected SQL errors, evaluation
checks and bounded sanity. Actual branch graphs, inherited widths and cross-read
counts differ, limiting strict causal attribution. These are one-pair results,
not stability or significance evidence. The fixed 60-second tails are not
natural drain; successful LocalFS-call bytes are not object payload/device I/O.
Long-term maintenance debt, every-branch full contents, mixed-version/old-binary
access and paper-performance recovery are not established.

The formal protocol/analysis and full pilot are preserved independently; the
current integrated source is not the measured C3 binary. No new SF100 run is
required for documentation or Ready status alone. Actual path-changing integration
deltas require proportionate correctness/performance qualification.

QA required: yes (persistent state, historical views, clone and recovery).
Integrated-head Linux qualification, official BVT and CI have separate bound
receipts; none is implied by these historical results or by a local test.
Consult the current PR checks and validation record for their exact identities.


## Integration repair and local qualification — 2026-10-26

Product repair `109332183a97a3f89bcf9328bac9efd376364a6c` follows the pinned
integration above. An independent no-context design/review loop found and closed
public metadata gaps: absent historical false was counted as NULL by
`metadata_scan`, and `mo_table_col_min/max` omitted its logical bounds. Both
now use the existing proved-object/immutable-fill rule; physically present NULLs
win, object metadata remains borrowed, and aggregate zone maps own their bytes.
No new history state, cache or execution path is introduced.

The existing embedded SQL fixture covers old persistent false plus new physical
true/NULL before COPY exit, unchanged relation/version identity, COPY controls,
and subsequent NULL ADD exit. COPY may publish a conservative boolean upper
bound; metadata min/max are compared with the persisted bounds rather than
assuming exact row extrema. The canonical customer BVT extends those public
metadata entry points. Temporary diagnostic logging is absent from the product.

Local ordinary admission/compile/catalog packages passed 1,619 root tests.
Focused tagged tests cover the merged cluster-key/captured-write and COMMENT
contracts, plus proof-error and CREATE INDEX exit repairs. The final tagged
SQL qualification covers the false history, mixed objects, public ordinals,
clone/merge, cancellation, rollback, restart/checkpoint and ordinary COPY
controls. These local results do not certify external launcher deployment,
current-head CI, production QA or mixed versions. Those remain separate gates;
the source integration has no newly measured full-workflow performance claim.


## Dependency safety and default-build coverage closure — 2026-10-26

Merge `25d1e5b76d9fbf481f8e0ab145a712adaed32130` integrates prerequisite
`a329de964e8b7738790c98d26bdf24c0d446a589`. Legacy catalog missing-column
proof now requires the real catalog database/table identity, the canonical logical
ID column seqnum/type and a concrete object. Same-name user layouts cannot obtain
that authority. The owning tables package and positive/negative identity tests
pass locally; this does not qualify mixed binaries or old-tool access.

The former default-build coverage result was 480/835 (57.49%). Most manifest
validation tests previously returned at the compile-capability gate, so negative
inputs could all pass for the same unrelated rejection. Initialize still gates
ordinary binaries before reading a manifest. Its existing verification/publication
body is shared through one private helper; tests now check the real guard reason,
failed reinitialization, lock/root/config identity and strict marker decoding.
Tagged exec/startup tests retain the real public entry point. No capability flag,
second state machine, admission bypass or new deployment mode is introduced.

Mode-independent COPY exit and deep-copy tests now run in both builds. Existing
hook seams separately test bound ADD dispatch and typed-fill consumption; those
injected consumer tests are not described as proof-producer or Linux integration
qualification. Catalog projection tests verify immutable history is copied and
restored from its owner. Default-disabled startup tests reject marked data,
fileservice and disk-cache paths. No service or performance workload is added.

The original CI parser and ignore rules reproduce the archived 480/835 exactly.
A local projection combining unchanged archived Linux blocks with current passing
ordinary component profiles reaches 630/836 (75.36%). This is a hybrid estimate,
not a final-head CI PASS; the unchanged >75% gate must still pass in CI.
No coverage exclusion, threshold relaxation or CI-repository modification was made.

Only the dependency/coverage closure is authorized in this phase. Ubuntu, the
new-head Linux window, SF100 and Ready remain untouched. Previously prepared
5ef8889c Linux inputs are superseded for final-head qualification; their absence
of execution must not be confused with a pass at the newer source.
