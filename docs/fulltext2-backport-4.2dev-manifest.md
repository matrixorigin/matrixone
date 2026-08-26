# FULLTEXT2 4.2-dev backport manifest

This branch is a bounded backport from the refs captured on 2026-08-26. The
source and target refs are intentionally fixed for this run; later commits on
`upstream/main` are not followed.

| source | scope | local commit |
| --- | --- | --- |
| `1d7b6311ce91df0968da435bb405f3d68137c595` (#25904) | FULLTEXT2 engine, parser/catalog contracts, plugin/CDC, planner/DDL and tests | `8330936547`, `7ce6f41954`, `cb2c363023`, `fb4eb5b499` |
| `ec591dc9d4ec6b3beb81b804dac11aceb9b66441` (#27165) | delayed phrase-position decode after document alignment | `1a8274faf5` |
| `3da66f2ca28d62f5e657c46d90b7cde9475486d0` (#27171), selective FT2 portion | wrapped FT2 score expressions, argument-aware score resolution, AND-reachable score ranges, and conservative float32 bounds; classic/view/vector changes from the source commit are excluded | `64515ad29e` |
| `6dccc954a1a32c12a2114328c752f15cf97ef2ba` (#27461) | cold-load reason/latency observation and cache-load lifecycle | `e3f25f079c` |
| `b8aa3f7f7351aacaaa37da27891d8f8d85e2e44d` (#27462) | immutable base/tail reuse across cache generations | `e3f25f079c` |
| `e07a7a792749db4228c1acf96d72827161747e60` (#27598) | FULLTEXT2 BVT readiness polling in place of fixed sleeps | `30e026c0b2` |

## Ref closure

- Source reference: `upstream/main` = `8e616a693458652cc26b3c4be45c6602dff298d4`
- Target reference: `upstream/4.2-dev` = `d2393868a7aaa6343518d80849fe4696aa3577e9`
- Branch: `backport/fulltext2-main-8e616a6-4.2dev-d239386`
- The target branch's existing parser/PERFORM and pipe changes were preserved;
  the generated MySQL parser was regenerated from the merged 4.2 grammar.

The diff does not use classic FULLTEXT as validation evidence. No local image,
direct `4.2-dev` push, Kubernetes mutation, or GitOps file mutation is part of
this backport.
