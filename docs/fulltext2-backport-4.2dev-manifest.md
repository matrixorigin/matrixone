# FULLTEXT2 4.2-dev backport manifest

This branch is a bounded backport from the refs captured on 2026-08-26. The
source and target refs are intentionally fixed for this run; later commits on
`upstream/main` are not followed.

| source | scope | local commit |
| --- | --- | --- |
| `1d7b6311ce91df0968da435bb405f3d68137c595` (#25904) | FULLTEXT2 engine, parser/catalog contracts, plugin/CDC, planner/DDL and tests | `966acb2a92bb740e57b0b7a181df2d21956e1d3`, `0eb9a921ece274c956447640b41ad2a255079e8d`, `403a99fb65304e75b82e75793448f0690a871d0e`, `bfa83f42737ca6304c0af3f6913e70ccc961464` |
| `ec591dc9d4ec6b3beb81b804dac11aceb9b66441` (#27165) | delayed phrase-position decode after document alignment | `2b142144c3c24a2100d7d313e952717025d56aeb` |
| `3da66f2ca28d62f5e657c46d90b7cde9475486d0` (#27171), selective FT2 portion | wrapped FT2 score expressions, argument-aware score resolution, AND-reachable score ranges, and conservative float32 bounds; classic/view/vector changes from the source commit are excluded | `835e046faa9242dbe288bd72364a475e2132bbed` |
| `6dccc954a1a32c12a2114328c752f15cf97ef2ba` (#27461) | cold-load reason/latency observation and cache-load lifecycle | `e4abf5e90d0b5e09590571ca78447e50806527c` |
| `b8aa3f7f7351aacaaa37da27891d8f8d85e2e44d` (#27462) | immutable base/tail reuse across cache generations | `e4abf5e90d0b5e09590571ca78447e50806527c` |
| `e07a7a792749db4228c1acf96d72827161747e60` (#27598) | FULLTEXT2 BVT readiness polling in place of fixed sleeps | `e3a7440e2ac32bac780669f33c656960e22cff22` |
| current validation and review repairs | restore execution-level wrapped-score coverage, repair reverse score-bound membership and ISCP error propagation, and remove fixed-sleep/whitespace gate failures | `35f910dd543e1d3a952e4bd1a1b9852558ee2e27`, `fbea073c3b55836317c7aeb49d090ed2167f004c`, `06ab54d812ffcae1b4910e74f2f42a85ae9dfe0b`, `4cce042c8d3ad2cbada59ddd24e287785c297c1a` |

## Ref closure

- Source reference: `upstream/main` = `8e616a693458652cc26b3c4be45c6602dff298d4`
- Target reference: `upstream/4.2-dev` = `d2393868a7aaa6343518d80849fe4696aa3577e9`
- Branch: `backport/fulltext2-main-8e616a6-4.2dev-d239386`
- The repair/BVT commits above are ancestors of the manifest update; the exact
  post-update branch head is recorded in the PR and the final remote reread.
- The target branch's existing parser/PERFORM and pipe changes were preserved;
  the generated MySQL parser was regenerated from the merged 4.2 grammar.

The diff does not use classic FULLTEXT as validation evidence. No local image,
direct `4.2-dev` push, Kubernetes mutation, or GitOps file mutation is part of
this backport.
