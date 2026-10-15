# COPY ALTER design decision record

- Design: [publication protocol](20260911-copy-alter-publication.md)
- Issue: [#28319](https://github.com/matrixorigin/matrixone/issues/28319)
- PR: [#28418](https://github.com/matrixorigin/matrixone/pull/28418)
- Current proposed revision: **4 (2026-10-15)**.
- Maintainer design decision: **PENDING**.
- Requested decision owners: @cpegeric and @LeftHandCold.
- Production repair, conflict integration and exact-new-head acceptance: **NOT_RUN**.

## Decision required before implementation

This is the distinct design-first phase in the existing Draft PR. Revision 4
reuses main's scalar replacement admission for ordinary ADD/DROP PRIMARY KEY
COPY, retaining shared C/G/D and exclusive source K/T throughout the transaction.
It proposes no late global lock upgrade and no post-copy snapshot refresh.
The exact protocol, restricted SNAPSHOT-write-barrier omission, complex/View
fallback and pre-copy-only retry require explicit maintainer acceptance.
Full 1/2/4/8-worker, <=45.55s per-test race, three-nightly and QA gates remain.

Record the approving maintainer, immutable reviewed document revision, decision
URL, timestamp and any conditions here only after an actual decision. Conflicting
conditions must be reconciled before implementation. Author review, a user
request to obtain approval, an existing review request or a successful historical
run is not that maintainer decision.

## Existing blocking threads

Each remains independently open; a single decision request links the related
threads without resolving them or requesting implementation approval prematurely.

| Reviewer | Thread | Disposition and next evidence |
| --- | --- | --- |
| aptend | [r4001780128](https://github.com/matrixorigin/matrixone/pull/28418#discussion_r4001780128) | Design/acceptance blocker: revision-4 decision, exact-head workload and QA |
| XuPeng-SH | [r4008474122](https://github.com/matrixorigin/matrixone/pull/28418#discussion_r4008474122) | Same design/acceptance obligation; retain separate response |
| XuPeng-SH | [r4009643127](https://github.com/matrixorigin/matrixone/pull/28418#discussion_r4009643127) | Same design/acceptance obligation; retain separate response |
| XuPeng-SH | [r4008474132](https://github.com/matrixorigin/matrixone/pull/28418#discussion_r4008474132) | Ubuntu race blocker: actual per-test <=45.55s and CI report on repaired head |
| XuPeng-SH | [r4009643130](https://github.com/matrixorigin/matrixone/pull/28418#discussion_r4009643130) | Same race obligation; retain separate response |

## Superseded decisions and evidence

The [revision-1 conditional handoff](https://github.com/matrixorigin/matrixone/blob/f7903a01aa528bb05b5662d6b2bdd859cf4212f5/docs/design/20260911-copy-alter-publication-review.md)
was not maintainer approval. Revisions 2/3 also have no recorded maintainer
approval. Revision 3's proposed move of 4/8 workers to follow-up is withdrawn;
it never waived the original acceptance contract. Old one-copy late-publication
oracles must not be transferred to revision 4's shared-admission protocol.

Historical Ubuntu run `c105-20261015T081332Z-a9617d` compared base
`8a4c84f4516d5098de9eef50b8afecc7524f37a2` with head
`5be67e3694712bc208e42c8db4c6032a2d896f65`, not the present implementation.
Its complete workload, live metrics and cleanup establish a reusable runner
pattern, not revision-4 optimization, current race/CI, nightly or QA acceptance.
