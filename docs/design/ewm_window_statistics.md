# Exponentially Weighted Window Statistics (ewmMean / ewmVar / ewmStd / ewmCov / ewmCorr) — Design

- **Status:** rev 22 — revised after the twenty-first (PASS) design
  review's two P3 advisories: the §8 pandas comparison correction and
  the §9 cost-model rewrite. Rev 21
  (SHA256 `b5dedbe344b2cf8c345d951356b2a31bac07d109c20dd11bd5d0015a98bcad28`)
  was APPROVED as a design; this revision records the advisory
  resolutions and the implementation-phase adaptations, supersedes rev 21
  which superseded rev 20
  (SHA256 `f94e298aad005d783cb9905fcfb7005a0522dd9d23c3a9498158a80f77199ed1`),
  which superseded rev 19
  (SHA256 `45274f4c98dba12b9ea5f846c5f3ad0df654ee79ac58db964c53460626c15141`),
  which superseded rev 18
  (SHA256 `2c23529b5f3af096f6717a15a683b6e174ba678f0dac3a1eadf789d9a7a2fe9e`),
  which superseded rev 17
  (SHA256 `3dbe1a8df47a77ecb35e3a9a55c275639d7719dd604f548ffb8807d5b620e700`),
  which superseded rev 16
  (SHA256 `17d1db7d6f6b46250345f988ed5c105a660a87a6a2ff7ad30c57023ff82cfa3b`),
  which superseded rev 15
  (SHA256 `2ab16728582b9745d8008536ba120b169131ff3be44f207d065c18dd233da181`),
  which superseded rev 14
  (SHA256 `6dd4ec2affdc3495c7414665986b64e1fbf0fe3ce8a73ed94c75b8129ba3397a`),
  which superseded rev 13
  (SHA256 `94aaea95919320efa2a2a6b107f70a0fd8f46f3c644706dde724128c970c8128`),
  which superseded rev 12
  (SHA256 `02c6f089f87bbc8e0c707e7fb5d36783f5d1d8df87330bd69dde6565e8739413`),
  which superseded rev 11
  (SHA256 `cf10d7cd4e2b7caece9326b3ed416d984ee56a92e5800fcf45c35ce4cb4e84ad`),
  which superseded rev 10
  (SHA256 `a857cbf2dd306afda34498972a6f9461dcac2fa6dec8e1afe48f684389fda3e9`),
  which superseded rev 9
  (SHA256 `d9e206f6598d2f08082003be831c2d5372df87b4905fb1c49cc9e193408cf0b0`),
  which superseded rev 8
  (SHA256 `abc36eac9be303a6447b6331db4150369b4b676af2105d1db2f333f0a08a5c57`),
  which superseded rev 7
  (SHA256 `37b81fbcf3bf45dd8f80643ac4d1f5173a79beccd1597c18e13901b790dc55e5`),
  which superseded rev 6
  (SHA256 `e34b32e650895d91722d7eb0f711dd1ffa01856ca18a1675d566f854021c11dc`),
  which superseded rev 5
  (SHA256 `d3d9a3af6b1d2c5412854b1ed5dd2c105d900ed106f69d35092df3ead5c1983b`),
  which superseded rev 4
  (SHA256 `5b10f929ff4e95a70d8184c9e2a3dc55bcd8bf26cd02d88f96553b8a3a704a01`),
  which superseded rev 3
  (SHA256 `cd7bb8501a6204e56522d1f515ecb0d7cd49f4c663d671480625d791831ec21d`),
  which superseded rev 2
  (SHA256 `3fe50a217557fb63e1445bab0083232dbc82303a2ce6d6b16ea0e2a2b1479f9a`)
  and rev 1
  (SHA256 `c2590c4eff168a03c12d42b10af580c0c55dcd0081b5996cbb5ae3b4ee414985`).
- **Owning issue:** [#29945 — exponential-weighted statistics with explicit decay and NULL policies](https://github.com/matrixorigin/matrixone/issues/29945)
- **Implementation PR / PR series:** TBD (implementation is blocked on
  approval of this revision; the PR must link this revision)
- **Base:** `f8c872001603fed3d23784e1ada26f9a2d8f4fba` (origin/main)
- **Design owner:** issue-29945 handler (this agent session)


## Revision record (rev 21 → rev 22)

- **Finding (twenty-first review, PASS with advisories).** Two P3
  advisories, recorded here with no engine-code change: (a) the §8
  pandas comparison mistakenly claimed pandas loses constant-signal
  mass through gaps — pandas computes normalized weighted averages, so
  the meaningful distinction is the continuation form after a gap: for
  `[1,NULL,3,4]`, alpha=0.5, adjust=false, ignoreNA=false this design
  emits `23/7` (`3.2857142857142856`) at the final row against the
  per-observation recursive continuation's `19/6`
  (`3.1666666666666665`); (b) the §9 performance model still claimed
  "~2× naive adds" and "~20 flops" although the specified arithmetic
  performs error-free deviation and product formation, compensated
  mean tails and normalization with route-dependent exact counts.
- **Fix.** §8 now states the accurate comparison and the four-row
  continuation expectation (`1, 1, 7/3` (`2.3333333333333335`), `23/7`
  (`3.2857142857142856`) — corpus `p3_continuation_adjF`, its
  adjust=true counterpart `1, 1, 2.6, 3.4615384615384617`); §10
  gained the "Gap continuation profile" verification row and this
  implementation round's operator tests and BVT carry those vectors.
  §9 drops the unsupported constant-factor estimates, describes the
  route-dependent arithmetic and pins the post-implementation
  measurement (focused singleton and tied-batch benchmarks plus the
  state-size assert) as the arbiter of cost.
- **Implementation-phase adaptations (recorded for review).**
  (1) The base revision carries no `explain_expr.go`; the default
  function-expr explain printer already renders the committed call
  structure and markers, so §9's EXPLAIN contract is met by the
  existing printer. (2) A decimal α or h literal is fenced behind the
  base's decimal-literal protocol and binds as a source expression
  rather than a folded constant, so such literals surface their §7
  range errors at the per-execution configuration boundary (the
  "resolved value at execution" form) while integral and float
  literals keep the bind-time family; the configuration boundary
  implements decimal64/128/256 evaluation for this. (3) A prepared
  marker resolved through the text form of a numeric decay or h is
  parsed strictly at that boundary, while the binder keeps quoted
  string literals on the declared-decay path. No decision, invariant
  or oracle literal changed this round; the §5/§10 vectors are
  frozen.

## Revision record (rev 20 → rev 21)

- **Finding (twentieth review).** One P2 finding, independently
  reproduced FIRST on the frozen rev-20 archive (probe
  `repro_review21.py`, SHA256 `31a817961f94dd00c6a1c9c216fa49ef2794dfc90d4f9e4d1c9ad983461c6a90`; log
  `repro_review21_rev20.log`, SHA256 `32098412569da41f403b54d7c4f90946996a1176578bd2067ab3e74a57797c00`, exit 0 — 26 fault
  observations against 26 green controls, every committed account
  read directly inside the frozen engine). **P2 — nonzero subnormal
  tails are still discarded when COMMITTING MERGED STATE:** the
  complete-mean contract (§3 Invariant 29, :2659–2705) requires the
  merged residual to stay retained at every merge boundary, but
  rev 20's ONE shared retention rule was specified at the batch
  commit lanes only (:3302–3314) while the merged-state tail lane
  still narrows its packed tail (:3673–3676), and the archived
  `ewm_rev20_oracle_simulator.py` consequently retains a merged
  tail only when its narrowed value is zero (:2210–2213, mirrored
  on the y store at :2223–2226). With u = 2^-1074, B = 2^-1021
  and the singleton groups [B], [B+2u], [B+4u] under α=0.5
  adjust=true, the first merge narrows its genuine −2u/3 merged
  tail to the scalar −u and commits the complete mean B+u although
  the exact 4u/3 is representable, the final commits B+3u against
  the exact 20u/7 (weights (1,2,4)/7) and the final std reads 2u
  although the exact variance 104u²/49 rounds to u; the
  fourth-singleton continuation commits B+5u against the exact
  68u/15 (weights (1,2,4,8)/15). All eight row/time × ignoreNA ×
  sign combinations fail identically (both signs observed), while
  the translated [0], [2u], [4u] controls stay green below the
  lattice — the merged-tail narrowing rule, not the scatter, is
  the defect (the review's own in-memory ablation applying
  lane_retains at both merged-tail stores heals all eight).

- **Root cause.** Rev 20 built `lane_retains` as the tail-retention
  test of the BATCH commit lanes — Decision 61 binds the rule to
  "every batch commit lane on BOTH streams and BOTH routes", and
  the §5 commit-lane paragraph says the same. The merged-state
  path had gained the complete-mean packer (Invariant 38) but its
  STORE still committed the packed merged tail through fl64 with
  the rev-9 finding-3 semantics — a narrow-once store whose only
  retention trigger was the superseded rev-19 zero-only test — so
  every nonzero merged tail quantized onto the subnormal lattice
  was silently discarded at a merge boundary, exactly the loss
  class Invariant 29 forbids.

- **Fix — the retention rule owns every tail-storage boundary
  (Invariant 40; Decision 64).** `lane_retains` is now the ONE
  shared retention test at EVERY tail-storage boundary: every
  batch commit lane (rev 20, Invariant 37) AND both merged-state
  tail stores, x and y, AFTER `pack_remainder` packs the merged
  tail — the (C) absolute-mass layer is the only merge route that
  can commit subnormal-lattice merged content (the (P)/(F) gates
  rebind those merges to (C)), so the two (C) stores complete the
  boundary surface. The store narrows once (xval) and keeps the
  extended pair whenever re-normalizing the narrowed value does
  NOT reproduce the pair — preserving any content lost through
  nonzero subnormal narrowing and the below-lattice flush — while
  every exact shape (zero, an exact subnormal word like 3u, every
  normal lead, the honest out-of-range ±Inf) keeps its unchanged
  narrowed scalar lane byte-for-byte. The committed merged
  (word, tail) is an exact split of the computed complete mean,
  so continuation merges consume true pair semantics.

- **Witnesses pinned (§10 r21F).** The r21F suite row drives: the
  singleton witness [B], [B+2u], [B+4u] at B = 2^-1021,
  u = 2^-1074 under α=0.5 adjust=true over all 8 {row, time} ×
  {ignoreNA} × {sign} combinations — committed (hi, tail)
  snapshots after every merge asserted as the GENUINE retained
  pair (first merge hi B+2u with the kept −2u/3 pair, complete =
  the exact 4u/3; rev 20 committed the narrowed scalar B+u), the
  final complete the exact 20u/7 (rev 20: B+3u) and std EXACTLY
  u — the exact 104u²/49 rounding (rev 20: 2u) — and the
  fourth-singleton continuation the exact 68u/15 (rev 20: B+5u)
  in both signs; the translated [0], [2u], [4u] controls (signed,
  with the continuation variant) keeping their genuine completes
  and std; and the merged-scale retention boundary vectors (the
  lossy −2u/3 pair, the exact 3u word, the below-lattice class) —
  20/20 suite assertions. Planned operator UT replicates every
  vector; the minimal SQL BVT covers the subnormal singleton
  witness via `SELECT ewmStd(x, 0.5, <adjust>, <ignoreNA>, 1)
  OVER (ORDER BY id)`.

- **Preservation.** The rebuilt core's pre-row full run is
  BYTE-IDENTICAL to the frozen rev-20 run log (`suite_rev21_pre.log`
  SHA256 `fd158c0a715fa0d019a6adec945d00b3b3150f36dbee5906adaee09d10358f9a` EQUALS the frozen `ewm_rev20_run.log` SHA256
  `fd158c0a715fa0d019a6adec945d00b3b3150f36dbee5906adaee09d10358f9a`); all 174 preserved
  suite rows stayed byte-identical — NO preservation literal was
  reassessed this round — and the appended r21F row makes 175/175
  PASS (`ewm_rev21_run.log`, SHA256 `747704a8c0c38655fbc3251e97a95c98fd042faeaa83ac7fa46ec8f07263c291`,
  SUITE_EXIT=0); the archive rerun reproduces the run log
  byte-for-byte (`rev21_rerun_archive.log`, SHA256
  `e0c3898247e0bf2b8a9fd85e42642f518c3c55b4246ab1d2d7710d26671e3687`); the preservation log records the
  174/174 identity with the frozen rev-20 parent pins
  (`rev21_preservation_diff.log`, SHA256
  `dd07416700bf06d95a56980792d3b41dec3c0d4d333301c9ae2658272d4e3e23`); the phased build probe is 29/29
  (`probe_rev21a.py` SHA256
  `87ea03857d7b85812f92ea553231de30fc2dd900a835874e46bf997c9d450a88`; log SHA256 `385bea252fec6debdadb793e59c96488ba6f0ce33c2a046499e3654b8e8c0a84`) and the final
  cross-module probe over the ARCHIVED rev-21 simulator with the
  frozen rev-20 module loaded is 15/15 green
  (`probe_rev21_final.py` SHA256 `ce3e1daac3ab27ad4ef3f53a4911923685b2306b971ced13227815f051685328`; log
  SHA256 `4df28e153e143c45463edf6c2a4619645b5e2e4d92e9db54f67a8677470d23ba`) — including the frozen
  rev-20 fault re-observations (the scalar −u merged-tail slot on
  the store, the committed B+u/B+3u/B+5u completes and std 2u in
  all eight combinations), the healed merged lanes on both
  streams and both signs, the retention boundary vectors, the
  exact-Fraction model reproduction of the pinned literals
  (4u/3, 20u/7, 68u/15, 104u²/49), and FULL bit-identity of the
  r16F/r17F families, the r13F lattice row, the r20F family and
  the profile through the changed sites. Verified builders:
  `build_rev21.py` SHA256 `fecd945a1811ebc0443a81c6224f2d8e6593417231c4388abfe7df33b1dac6f7` (lane_retains applied at both
  merged-tail stores, derived from the frozen rev-20 archive
  with reciprocal inversion byte-equal to the parent),
  `build_rev21_b.py` SHA256 `6c90b014167efe0971944eb5c0580046bd7dfc4b0a85bcaada9762ba9abca70b` (the r21F row insertion with
  its inverse reproducing the core). Archived rev-21 simulator
  SHA256 `087286c259caee021ef810fc0354f9e6a400cd9f0f13adcfef78435aecf44af3` over the frozen parent
  `ewm_rev20_oracle_simulator.py` SHA256
  `01db300b68415064a0978979241e3e2acf63e10d0e421a5bde8bccde76a70594` and run log SHA256
  `fd158c0a715fa0d019a6adec945d00b3b3150f36dbee5906adaee09d10358f9a`.

## Revision record (rev 19 → rev 20)

- **Finding (nineteenth review).** Three findings, each independently
  reproduced FIRST on the frozen rev-19 archive (probe
  `repro_review20.py`, SHA256 `e130ed1b384498bc8ce4678c2e2b1f7a0a08d76d2a568e76d28557142ddf4c53`; log
  `repro_review20_rev19.log`, SHA256 `4f7202e1f5dfbfdca72db8765d459c0ccff82ad8ec0a5f387e21212458e27479`, exit 0 — 72 fault
  observations against 59 green controls, every committed account
  read directly inside the frozen engine). **P2 — nonzero subnormal
  tails are discarded at the commit lane:** §5:3151–3157 retains the
  extended tail only when narrowing produces zero, and the archived
  `ewm_rev19_oracle_simulator.py` (:1298–1302) consequently discards
  `genuine_tail` whenever the narrowed scalar stays nonzero — with
  u = 2^-1074, B = 2^-1021 and the tied batch [B, B+2u, B+8u] the
  genuine tail fl(4/3)·u narrows to u, the account commits (B+2u, u)
  in all six peer orders and both streams, and every one of the 48
  permutation × decay-mode × adjust × ignoreNA executions emitted std
  4u although the exact variance is 104u²/9 whose std rounds to 3u
  (the translated [0, 2u, 8u] control emits 3u — the narrowing rule,
  not the scatter, is the defect). **P2 — the extended lead's narrowing
  remainder is discarded during mean packing:** Invariant 29 (:2529)
  requires the complete mean after normalization, but the archived
  simulator's packing (:2146, mirrored by `mean_pair_norm` at :539)
  keeps only the FOLD LOSS of xfold(extended_lead, −narrowed_mean),
  discarding its primary result — with the singleton sequence [0],
  [2u], [4u] under α=0.5, adjust=true the first merge loses ≈u/3 of
  its committed mean (exact 4u/3, committed u plus a tiny fold
  residue), the final lands on 3u against the exact 20u/7 (drift
  u/7), and the final std reads 2u although the exact variance
  104u²/49 rounds to u; both row and event-time decay reproduce this,
  and adjust=false keeps its green u first merge but commits the
  fl(2.5u) = 2u tie at the final. **P2 — the public non-finite-sample
  contract is unresolved:** §4:2726 accepts numeric samples without a
  finiteness restriction while §7 rejects non-finite keys and
  policies but defines no sample policy, and :3455–3457 promises
  pass-through for infinities from non-finite data; supplying a
  singleton ±Inf or NaN sample (x or y, initial, later or tied)
  instead leaks the internal `AssertionError("non-finite value
  reached the extended normalization")` to the caller — reachable too
  by composing the design's own permitted infinite variance/covariance
  outputs through a subquery.

- **Root cause.** The rev-19 retention lane tested "quantizes to 0.0
  with a nonzero mantissa" — a below-lattice test written for the
  rev-13 [0,2u,6u] witness and blind to content lost by SUBNORMAL
  quantization whenever the narrowed lead stays nonzero. The packing
  sites kept xfold's addition loss but let its PRIMARY result fall on
  the floor, so every benign-looking fold silently dropped the real
  remainder that makes (hi, tail) the complete mean. And the design
  never decided what a non-finite SAMPLE does: the §5 pass-through
  clause was written for carried internal infinities, the §7 catalog
  covered only keys and policies, and the internal normalization
  assertion became the de-facto public behavior.

- **Fix — one shared rule per channel (Invariants 37/38/39;
  Decisions 61/62/63).** (1) `lane_retains`: the ONE shared
  tail-retention test at every batch commit lane on both streams and
  both routes — narrowing loses content iff re-normalizing the
  narrowed lead does NOT reproduce the pair exactly, which covers the
  nonzero subnormal lead, the below-lattice flush and keeps every
  exact shape (zero, an exact subnormal word like 3u, every normal
  lead, the out-of-range honest ±Inf) on its unchanged byte lane.
  (2) `pack_remainder`: the ONE shared complete-mean packing
  procedure at every normalization and merge boundary — the full
  extended-minus-narrowed difference enters the tail accumulator
  together with its exact fold loss and the caller's existing low
  word; benign exact-narrowing folds append nothing. (3) the public finite-sample gate
  `EwmNonFiniteSample("invalid input: ewm sample values must be finite")`: raised by the
  engine's row pre-scan
  BEFORE any group is processed and any state published (initial,
  tied or later samples, either stream), one deterministic execution
  error, clean reuse after rejection; the internal
  extended-normalization assertion remains a true internal invariant,
  and the §5 pass-through clause is superseded to internal overflow
  products of finite samples only.

- **Witnesses pinned (§10 r20F).** The r20F suite row drives: the
  witness [B, B+2u, B+8u] over all 6 peer orders × both decay modes
  × both adjust policies × both ignoreNA policies — the committed
  account asserted as the GENUINE PAIR on every order (hi B+2u, pair
  fl(4/3)·u with xval u), all 48 emissions EXACTLY 3u = the exact
  104u²/9 rounding, the translated [0,2u,8u] controls 3u, and the
  five retention boundary vectors (zero/exact-subnormal 3u/normal/
  lossy fl(4/3)·u/out-of-range); the singleton sequence [0], [2u],
  [4u] with committed (hi, tail) snapshots after every merge —
  4u/3, 20u/7 and std u under adjust=true; u EXACT then 5u/2 (the
  healed fl(2.5u) tie) under adjust=false, both decay modes; and the
  defined rejection on every non-finite initial/tied/later x or y
  sample with a clean finite reuse (mean 2.0). The minimal SQL BVT
  witness covers the profile, the subnormal witness and the
  invalid-input rejection.

- **Preservation.** The rebuilt core's pre-row full run is
  BYTE-IDENTICAL to the frozen rev-19 run log (`suite_rev20_pre.log`
  SHA256 `7aa1d2d3c74b3a55c95d2325937d8b8a1df905285cdb59a65789742ca4044778` EQUALS the frozen `ewm_rev19_run.log` SHA256
  `7aa1d2d3c74b3a55c95d2325937d8b8a1df905285cdb59a65789742ca4044778`); all 173 preserved
  suite rows stayed byte-identical — NO preservation literal was
  reassessed this round — and the appended r20F row makes 174/174
  PASS (`ewm_rev20_run.log`, SHA256 `fd158c0a715fa0d019a6adec945d00b3b3150f36dbee5906adaee09d10358f9a`,
  SUITE_EXIT=0); the archive rerun reproduces the run log
  byte-for-byte (`rev20_rerun_archive.log`, SHA256
  `1c796bf2b43152b1eda401ed16783ed60257b550a3e621cc7b903cc8ca1613a5`); the preservation log records the
  173/173 identity with the frozen rev-19 parent pins
  (`rev20_preservation_diff.log`, SHA256
  `b609ce7771f363d1e422ce2f0cdd47b135859427303f2a16ddd1600a64483352`); the phased build probe is 37/37
  (`probe_rev20a.py` SHA256
  `92c00698201579702bed4f12561425ae1752ae5391b987360ee0af3710d1d857`; log SHA256 `4295440cf2b7f82429b740789e126b2d7cf426e30da2ae7fc79b45a4df18526b`) and the final
  cross-module probe over the ARCHIVED rev-20 simulator with the
  frozen rev-19 module loaded is 15/15 green
  (`probe_rev20_final.py` SHA256 `1c77669caf08c13ebec4d4529378d4b5ebb34eb90e6fa0df3caaaf9dddaffdd6`; log
  SHA256 `69a5a2821e9fbae7d22581fd0e589bb001366b7f18e53372a7521aab47e3b14a`) — including the frozen
  rev-19 fault re-observations (the narrowed u account on every
  order, the lost u/3 first merge landing ON u far below the exact
  4u/3, the 2u-tie, and the leaked internal assertion), the healed
  channels on both streams, the retention boundaries, the
  y-stream/account mirrors, exact-Fraction model reproduction of the
  pinned literals, and FULL bit-identity of the r16F/r17F families,
  the fifteenth K-vector, the r13F lattice row and the profile
  through the changed sites. Verified builders: `build_rev20.py`
  SHA256 `50c54b81da20d42b4871b89b69bba56c0154bb16705725b094d6d290a1387501` (the one-shared-rule core derived from
  the frozen rev-19 archive with reciprocal inversion byte-equal to
  the parent), `build_rev20_b.py` SHA256
  `159f610e8bf19ab3977bc2c37c37292e7fed646916ce3d7191232163e109a6f6` (the row insertion with its inverse
  reproducing the core). Archived rev-20 simulator SHA256
  `01db300b68415064a0978979241e3e2acf63e10d0e421a5bde8bccde76a70594` over the frozen parent
  `ewm_rev19_oracle_simulator.py` SHA256
  `76749013a0186b4774f63d05bd33a8d612b7d91294c3b47acea0bb68a63c1410` and run log SHA256
  `7aa1d2d3c74b3a55c95d2325937d8b8a1df905285cdb59a65789742ca4044778`.

## Revision record (rev 18 → rev 19)

- **Finding (eighteenth review).** Two findings. **P2 — the genuine
  mean residual is lost when the batch sum overflows:** §5:2838
  divides each anchored observation before accumulation while the
  genuine-tail contract (:2930) requires τx := (Σx − k·m̄x)/k, and
  the archived `ewm_rev18_oracle_simulator.py` (:1201/:1212) rebuilt
  the anchored tail from the ALREADY-ROUNDED per-observation
  quotients of the rev-9 mean shift, losing every quotient's
  division residual. Independently reproduced FIRST on the frozen
  rev-18 archive (probe `repro_review19.py`, SHA256 `3b1833dd8fdeb8b7ba6bbd14fe31620b51ed38e6d1f32d64a96e9b7c0634b85f`;
  log `repro_review19_rev18.log`, SHA256 `db82bf11fce81305309e754d4828bc4c329c43b11df2fd9819ccdf488d42367e`, exit 0): the tied
  batch [H,H,−H] at H = 2^1023 routes to the anchored lane (the raw
  partial sum H+H overflows float64) and commits m̄x = b = fl(H/3) =
  `2.9961552247705263e+307` with tail EXACTLY `0.0`, although the
  genuine complete residual is `1.6632002579455998e+291`; both
  reordered peers [H,−H,H] and [−H,H,H] keep finite partial sums,
  take the ordinary route and commit the genuine tail. Appending −b
  at the next key under α=0.5, adjust=false emits mean EXACTLY `0.0`
  instead of `8.316001289727999e+290` = fl((H−3b)/6); under
  adjust=true appending −H/2 emits `−9.9792015476736e+290` =
  −(H−3b)/5 where EXACTLY `0.0` is required; both row and event-time
  decay reproduce the defect — 16 failing combinations (initial and
  incoming batches × both adjust policies × both ignoreNA policies ×
  both decay modes) against 32 correct reordered controls, every
  committed ACCOUNT checked directly inside the frozen engine.
  **P3 — the decision log is inconsistent:** §5 cites Decision 58
  (the rev-18 xq_cascade) while the §11 log ends at Decision 57 and
  still prescribes the superseded ddsum/dddiv two-word quotient for
  every route.

- **Root cause.** The anchored (overflowing-sum) route rebuilt its
  carried tail from the SAME pre-divided quotients that produce its
  mean — each (x_i − a)/k is one-rounded, so the division residuals
  are destroyed before any compensation can see them, and the tied
  witness's quotients cancel to EXACTLY zero. The ordinary route
  kept a SECOND, register-sourced variant of the same channel. And
  the rev-18 revision recorded its Decision 58 in §5/§10/§12 but
  never appended it to the §11 decision log.

- **Fix — ONE shared, range-safe reconstruction of the complete
  batch residual from the ORIGINAL observations (Invariants 35/36;
  Decisions 59/60).** The new `genuine_tail(obs, k, mean)` forms
  Σx in the xsum4 register cascade over the RAW xfrexp(obs) items,
  enters the k·m̄x subtrahend as its TWO exact xtwoprod words (the
  subtraction residuals chase through the cascade registers), and
  performs ONE final xdiv — nothing pre-divided, no per-route
  variant (Invariant 35). Both routes commit the SAME channel with
  the SAME lane (Invariant 36): the anchored route commits either
  the narrowed xval lead — bit-for-bit the ordinary route's
  committed pair — or the retained (pair) shape when the lead
  quantizes to 0.0 with a nonzero mantissa (the rev-13 below-lattice
  retention); the ordinary route keeps its rev-13/15 float64 fast
  lane UNCHANGED with the shared channel as its retained fallback
  (the rev-13 triggers untouched; the former register-sourced
  fallback is superseded). The scatter, cross, routing, retention
  and merge layers are untouched.

- **Witnesses pinned (§10 r19F).** The r19F suite row drives the
  full 16-combination matrix (the witness order [H,H,−H] plus both
  reordered controls × {initial, incoming} × {row, time} ×
  {adjust T/F} × {ignoreNA T/F}): every committed account asserted
  EXACTLY (b, `1.6632002579455998e+291`) on every order and route,
  and every merged mean emission BIT-IDENTICAL across all three
  peer orders and equal to the independently derived targets —
  fl((H−3b)/6) = `8.316001289727999e+290` initial adjF, EXACTLY
  `0.0` adjT, `4.993592041284211e+306` = nextafter(fl(H/18)) at
  incoming row decay (ONE documented merge ulp above the strict
  fl(H/18) = `4.9935920412842106e+306` — the value the corrupt
  account emitted) and fl(−H/12) = `−7.490388061926316e+306` at
  incoming time decay; plus the genuine_tail unit vectors: the
  rev-13 first-tied [M,0,0] residual exact and [0,2u,6u]
  (u = 2^-1074) retaining the below-lattice pair. The minimal SQL
  BVT witness shape is recorded in the r19F row.

- **Preservation.** The rebuilt core's pre-row full run is
  BYTE-IDENTICAL to the frozen rev-18 run log
  (`suite_rev19_pre.log` SHA256 `f8740fe1e3c952d497583878641e97cea99af2194d23931efa22f3389bc91bd7` EQUALS the frozen
  `f8740fe1e3c952d497583878641e97cea99af2194d23931efa22f3389bc91bd7`); all 172 preserved suite rows stayed byte-identical — NO
  preservation literal was reassessed this round — and the appended
  r19F row makes 173/173 PASS (`ewm_rev19_run.log`, SHA256
  `7aa1d2d3c74b3a55c95d2325937d8b8a1df905285cdb59a65789742ca4044778`, SUITE_EXIT=0); the archive rerun reproduces the
  run log byte-for-byte (`rev19_rerun_archive.log`, SHA256
  `ce341645de74741afa6491790751315adaac4e44a6010622647f22b068727294`); the preservation log records the 172/172 identity with
  the frozen rev-18 parent pins (`rev19_preservation_diff.log`,
  SHA256 `a2d645558a1a6e73cb6c109878b37ed3ecc85b4ea69814249800a25fa7669f6d`); the final probe over the ARCHIVED rev-19
  simulator with the frozen rev-18 module loaded is 17/17 green
  (`probe_rev19_final.py` SHA256 `4921503de3492d365423b6d6cf4eaa912343d71d16f500df14730b18b8ba1b72`; log SHA256
  `940e6640fa1be2f2d9d80a3144d3d08216b2c93fcd31cba137124ac5fde8ca3e`) — including the frozen rev-18 fault re-observation
  (stated order tail `0.0`, reversed peers genuine), the healed
  matrix, the exact-Fraction model reproduction of every pinned
  literal, the y-stream mirror, and FULL bit-identity of the
  r16F/r17F families, the fifteenth K-vector, the anchored-memory
  batch, the row-44 batch and the profile through the changed
  sites. Verified builders: `build_rev19.py` SHA256 `6eead9446619c8c32d9fbaa7f643c8708f6e79cb1709b3628dcbbcbcc469ab80`;
  `build_rev19_b.py` SHA256 `e61df5fc8f60b8557289098ea564630027055d62a36e6b36669f1e2ae717caed`; phased probe
  `probe_rev19a.py` SHA256 `a0d81a525cdb99bff19dc89519a0bd4f8470480799b7888888bbf1d3613856ba` (log SHA256 `91f9f3732b23716f34b5439642e8b1cbfa9a1d5c48303f15e3fed868eb40db00`,
  88/88). Archived rev-19 simulator SHA256 `76749013a0186b4774f63d05bd33a8d612b7d91294c3b47acea0bb68a63c1410`
  over the frozen parent `ewm_rev18_oracle_simulator.py` SHA256
  `ad3e3fb14736b7d5724073f55f37bcccf422968569ea304f6f0d3b70fb8d1aa9` and run log `f8740fe1e3c952d497583878641e97cea99af2194d23931efa22f3389bc91bd7`.

- **Decision-log realignment (P3).** §11 now carries Decision 58
  (recorded with the rev-18 provenance it was cited under), Decision
  59 (this realignment and the superseding of Decision 57's
  signed-cross quotient prescription — its scatter-quotient and
  routing contracts are retained) and Decision 60 (Invariants 35/36,
  the shared genuine reconstruction). §12 gains the rev-19 increment.

## Revision record (rev 17 → rev 18)

- **Finding (seventeenth review):** the routed formations still
  compressed the UNCANCELLED signed cross pile into two words
  BEFORE the quotient. §3/§5's batch-moment contract already
  required the full register cascade for a cross pile (full
  cancellation admitted), but Invariant 33's tail prescribed
  `ddsum → dddiv → xadd` for the routes, §5's routed lattice line
  carried the same `the dd cascade + division and ONE final fold`
  prescription over `sxx, syy, sxy` together, and the archived
  rev-17 simulator (`:1517`/`:1564`) implemented it.
  Independently reproduced FIRST on the frozen rev-17 archive
  chain (doc `design-ewm_window_statistics-rev17.md` SHA256
  `3dbe1a8df47a77ecb35e3a9a55c275639d7719dd604f548ffb8807d5b620e700`; simulator
  `ewm_rev17_oracle_simulator.py` SHA256 `95193b4e8a9f9f4f705707f4283e63b63bcd68860f854623bc867a2a9bb5fa90`;
  run log `ewm_rev17_run.log` SHA256 `e74160cfbb92e1ddf2c2cabbce559810cf3a30bd4184ca5c9fb696dadfbfa070`)
  with exact Fraction/Decimal expectations (probe
  `repro_review18.py`, SHA256 `56c9acda710e0ff8a70586d919b773200b1547c3fc00927333bfac7b42028fa5`; log
  `repro_review18_rev17.log`, SHA256 `84ef1c221260e1390c090207aa0d616bdebb815e612a61266057d5688e6bedf4`,
  exit 0 — 20 fault observations, 10 green controls): (a) the
  ANCHORED route emits cov EXACTLY `0.0` — both decay modes,
  both adjust policies, both stream orientations — for the tied
  batch x = [B,B,B,B,B,−B], B=1.7e308, y = [−H,M,1,H,−M,−1]
  (H=2^400, M=2^200), where the complete covariance
  fl(B/3) = `5.666666666666667e+307` is exact: the cross pile
  holds ±BH-scale and ±BM-scale pair products whose exact
  cancellation leaves the surviving +2B content hundreds of
  binary orders below the running BH-scale lead, and the ddsum
  two-word fold (~106 bits) erases it before dddiv ever divides;
  the REVERSED peer order already cancels the big pairs inside
  the register word and returns the exact value — the
  order-dependence proving the two-word compression; (b) the
  LATTICE route has the same defect: the ten-pair batch
  [(H,H),(−H,−H),(M,M),(−M,−M),(1,1),(−1,−1),(H,−H),(−H,H),
  (M,−M),(−M,M)] with x scaled by 2^−512 emits cov AND corr
  EXACTLY `0.0` in the stated order, where
  fl(2^−512/5) = `1.4916681462400414e-155` and the nonzero
  correlation `7.498484069478155e-242` are exact — the ±2^288-scale
  H-pair products tower ~800 binary orders above the surviving
  2^−511-scale unit content — while the reversed order succeeds
  and scaling x by 2^−511 keeps the normal route exact in both
  orders (the frexp fence untouched); (c) all EIGHT
  initial/incoming continuations stay poisoned at EXACTLY `0.0`
  for both witnesses (the zero batch covariance propagates
  through every merge). Green controls: 10 — the reversed and
  swapped-reversed anchored orders, the reversed lattice order
  and the normal-route 2^−511 boundary batches in both orders.

- **Root cause:** rev 17's route revival ran the error-free
  FORMATIONS but kept the normal SCATTER's dd quotient pipeline
  on the crosses. `ddsum(items)` folds the whole pile into a
  double-double BEFORE `dddiv` divides: safe for a
  positive-dominant scatter pile (every discarded piece sits
  ≈106+ binary orders below the lead, under its half-ulp),
  destructive for a SIGNED cross pile where full cancellation is
  admitted and the pile's true content can sit hundreds of binary
  orders below the running lead — exactly the property §5's
  normal-route cross contract already routes to the full
  register cascade for.

- **Fix — one cancellation-preserving accumulation contract for
  the signed cross pile on every route (Invariant 34 /
  Decision 58).** The two routed cross sites now call the shared
  `xq_cascade(items, d)`: the FULL `xsum4` register cascade
  feeds the first quotient word, the EXACT −q1·d residual
  (an xtwoprod) re-cascades WITH the pile, and the two-word
  quotient keeps its true remainder — dddiv's structure
  generalized to a multi-item numerator, so NO uncancelled
  cross pile is ever compressed before the division and the
  pile's cancelable bits survive in every peer order. The
  scatter quotients keep the dd pipeline. The first-cut concern
  — losing the dd pipeline's pinned remainder fidelity (the
  rev-8 low-weight-arrival row) — is refuted by the rebuilt
  core's pre-row full run, which is byte-identical to the
  archived rev-17 run log (`suite_rev18_pre.log`, SHA256
  `e74160cfbb92e1ddf2c2cabbce559810cf3a30bd4184ca5c9fb696dadfbfa070`; all 171 rows, diff exit 0);
  no formation, routing, retention or merge layer was touched.

- **Witnesses pinned (§10 r18F).** Both witnesses across the
  stated, reversed, swapped and swapped-reversed peer orders,
  both decay modes and both adjust policies, plus five rotated
  orders (witness A) and four rotated orders (witness B) at row
  decay; the routing-boundary pair — the lattice side 2^−512
  (witness B) and the normal side 2^−511 → EXACTLY
  `2.983336292480083e-155` in both orders, both policies and
  both modes; the witness-B correlation pinned NONZERO and
  bit-equal across the stated and reversed orders; and all
  eight initial/incoming continuations at both adjust policies
  against the exact 900-digit Decimal decay-mixture ideals
  (the model first reproduces the four r17F (a)-family
  continuation literals bit-for-bit): witness B lands EXACTLY
  at fl(5c/6) = `1.2430567885333677e-155`,
  fl(c/2) = `7.458340731200207e-156`,
  fl(20c/21) = `1.4206363297524203e-155` and
  fl(10c/11) = `1.3560619511273104e-155`
  (c = fl(2^−512/5)), and witness A lands within the merge's
  documented ONE-ulp bound of fl(B/4) = `4.25e+307`,
  fl(B/6) = `2.8333333333333334e+307`,
  fl(4B/13) = `5.23076923076923e+307` and
  fl(2B/7) = `4.857142857142857e+307` (observed EXACT in the
  pinned order; the bound documents the merge layer's own
  B-scale single-pair assembly — rev-8 finding 3 — not a
  formation defect).

- **Preservation.** All 171 preserved suite rows stay
  byte-identical — NO preservation literal was reassessed this
  round; the appended r18F row makes 172/172 PASS
  (`ewm_rev18_run.log`, exit 0); the archive rerun reproduces
  the run log byte-for-byte (`rev18_rerun_archive.log`, exit
  0); the preservation log records the 171/171 identity with
  the frozen rev-17 parent pins (`rev18_preservation_diff.log`);
  the final probe over the archived rev-18 simulator with the
  frozen rev-17 module is 17/17 green (`probe_rev18_final.log`)
  — including the archived rev-17 fault re-observation (stated
  orders `0.0`, reversed controls exact), the routing boundary,
  the healed continuations against independently re-derived
  Fraction ideals, and full bit-identity of the r15F/r16F/r17F
  families through the changed sites. Verified artifacts:
  archived `ewm_rev18_oracle_simulator.py` SHA256
  `ad3e3fb14736b7d5724073f55f37bcccf422968569ea304f6f0d3b70fb8d1aa9`; run log SHA256
  `f8740fe1e3c952d497583878641e97cea99af2194d23931efa22f3389bc91bd7`; rerun log SHA256
  `68fe58bd42c2022157fa851ca386d1301e663a8e3b906e5fac2533059800927d`; preservation log SHA256
  `f225f5895803de735cb312827091eae3306e5f72b04d5a911ecb5c4b86e90bf6`; final probe `probe_rev18_final.py`
  SHA256 `764c2d04ca05ffe33f375715f3c2148839fb997566849a232b98a442ddd84324, log SHA256
  `62c99ee4922406fb67055fc6d1be08167ac1981007f2eb00343ab8766a51cb48`; builders `build_rev18.py` SHA256
  `05de3f2fbaad71ba001ec2ce4cfb0d05e3856a386c1390f29ba9fcf782e95561` and `build_rev18_b.py` SHA256
  `c6ec82cf22c0e26d93fb905d948b863689065ed09ed827a2fa128355bcd03cec`.

## Revision record (rev 16 → rev 17)

- **Finding (sixteenth review):** the error-free batch-moment contract
  did not reach the lattice and anchored routes. §5's Step-1 fallback
  text (the anchored and lattice blocks below), the rev-16 §12
  bullet “keep their established shapes” and the archived rev-16
  simulator (`:1416–1436`) all retained older one-rounded arithmetic:
  deviation lists `x − m̄x` / `x − a` and one-rounded `xmul`
  products. Independently reproduced FIRST on the frozen rev-16
  archive chain (doc `design-ewm_window_statistics-rev16.md`
  SHA256 `17d1db7d6f6b46250345f988ed5c105a660a87a6a2ff7ad30c57023ff82cfa3b`; simulator
  `ewm_rev16_oracle_simulator.py` SHA256 `c70967df9a4e7e706789480d31e443503274afdecff45f5545bcf742ef251362`;
  run log `ewm_rev16_run.log` SHA256 `f6b6f155cae2a3e4d8b5ac9ce000cd9df5c754b179e21431a13aa7fe7b8d930f`)
  with exact Fraction/Decimal expectations (probe
  `repro_review17.py`, SHA256
  `6d4d63ed000e68bfa3f12ee66c7ed41697a9b0bcef40329dd90523a821424401`; log
  `repro_review17_rev16.log`, SHA256
  `0b3a8f9071adfbd35cb6ff83d4ca8b494e5a10c36d11c6c96c7db94d20b483ab`, exit 0
  — 488 fault observations, 192 green controls): (a) the lattice route
  emits cov EXACTLY `0.0` in 96/96 executions (24 permutations ×
  both decay modes × both adjust policies) for x = 2^−600·[K+1,K,−K−1,−K]
  y = [K−1,1−K,K,−K] (K=2^27) where the complete covariance
  fl(−2^−602) = `−6.02479966275721e−182` is exact
  — every one-rounded `xmul(dx, dy)` discards the row's −2^−600-scale
  residual 54 binades below the lead (fl(2^−546 − 2^−600) = 2^−546) —
  and the y-scaled one-sided control shows the same loss; (b) the
  anchored route emits `4.25e307` in 96/96 executions AND on the
  swapped streams for x = [B,B,B,−B], B=1.7e308, y = [2^52,1,−2^52,0],
  where the exact covariance is B/8 = `2.125e307` — the one-rounded
  deviation list reads [2^52, 0.75, −2^52, −0.25] against the exact
  [2^52−1/4, 3/4, −2^52−1/4, −1/4] and both ±1/4 residuals multiply
  against the B-rows; the routing scale 2^−538 (normal route) stays
  green, and the fault spread includes the poisoned continuations
  (`0.0` for all four (a) scenarios; `3.3055555555555557e+307` /
  `2.65625e+307` / `3.9876543209876543e+307` / `3.74e+307` for (b)).

- **Root cause:** three formations kept pre-rev-16 arithmetic while
  Invariant 32 promised every finite batch: the lattice scatter/cross
  consumed one-rounded deviations and one-rounded extended
  products, and the anchored scatter/cross consumed one-rounded anchor
  deviations, one-rounded products and a post-division one-rounded
  `xmul(deltx, delty)` anchor correction — the mixed anchored/
  mean-anchored pairing also dropped the −k·δx·τy cross term of an
  overflowing side merged with a mean-anchored side carrying a
  tail.

- **Fix — one contract across normal, lattice and anchored routes
  (Invariant 33 / Decision 57).** The lattice and anchored scatters
  and crosses now form through the SAME error-free helpers as the
  normal route: per-row twosum deviations (against the narrowed mean
  on lattice sides; against the EXACT range midpoint on overflowing
  sides — range safety untouched: |x_i − a| ≤ half the batch range
  in the twosum pair, no raw deviation and no infinite operand ever
  formed), error-free xtwoprod product pieces per row, and the new
  shared `xanchor_k_items` anchor/mean-tail correction: −k·δx·δy
  with δ = mu − anchor supplied as its EXACT additive parts (the
  twosum(m̄x, −a) words plus the tail; tails may be retained pairs),
  every sub-product error-free in the extended channel, the k-scaling
  and negation folding each piece once, zero parts contributing
  nothing by VALUE — so the mixed pairing keeps its −k·δx·τy
  term. The quotient keeps the exact remainder (ddsum → dddiv → xadd,
  the normal scatter's pipeline): the first cut's restored-fold
  `xdiv` committed mantissa `0.7949031179498357` for the rev-8
  low-weight-arrival fixture [B,B,−B], one ulp above the correctly
  rounded complete moment `0.7949031179498356` (independently
  re-derived with exact rationals, centered on the engine's own
  committed complete mean); the dd quotient restored the exact
  pinned bytes and the full preserved corpus.

- **Witnesses pinned (§10 r17F).** Both witnesses across all 24
  permutations × both decay modes × both adjust policies
  (exact: `−2^-602` both one-sided scalings; `B/8 = 2.125e307` both
  stream orders), the routing-boundary pair at the frexp fence
  (scale 2^−538, max-deviation exponent −510 → normal route);
  scale 2^−539, exponents −511/−512 → lattice route; both exact),
  and the initial/incoming continuations at both policies with
  the exact decay-mixture ideals derived from the 900-digit Decimal
  model (which first reproduces the four r16F continuation literals
  bit-for-bit): the (a) family and the (b) initial adjF land
  EXACTLY; the (b) initial/incoming adjT and incoming adjF are
  asserted within ONE ulp of the ideal with the ideals recorded —
  the merge's own moment assembly (rev-8 finding 3: the dd_mul-
  folded single-pair term feeds, the one-rounded W·vxx / A·sxx
  assembly products and the single-pair numerator restore feeding
  the quotient) cannot carry the 55-bit numerator of the B-scale
  mixture — a documented bound of the merge layer, not a formation
  defect (the batch commits feeding the merge are exact).

- **Preservation.** All 170 preserved suite rows stay byte-identical
  — NO preservation literal required reassessment this round; the
  appended r17F row makes 171/171 PASS (`ewm_rev17_run.log`, exit
  0); the archive rerun reproduces the run log byte-for-byte
  (`rev17_rerun_archive.log`, exit 0); the final probe over the
  archived simulator is 18/18 green (`probe_rev17_final.log`).
  Verified artifacts: archived `ewm_rev17_oracle_simulator.py`
  SHA256
  `95193b4e8a9f9f4f705707f4283e63b63bcd68860f854623bc867a2a9bb5fa90`;
  run log SHA256
  `e74160cfbb92e1ddf2c2cabbce559810cf3a30bd4184ca5c9fb696dadfbfa070`;
  rerun log SHA256
  `0ea525b06c3c27c5dca43bb8b48f5eb971971bda878bcf9026b4fb7726e3bd64`;
  preservation log SHA256
  `f7ba550152a73f2313953bad76ca2e665036c51a5c74b558e9cadf2c58d0b887`;
  final probe `probe_rev17_final.py` SHA256
  `8acf250b528308af3fefc721a86a3d7deead621feb6eb8fe05c436d1e827d8d7`, log
  SHA256
  `9650cc1729d6d1e03332c848ad8fa5b71727f3eaf684971681708892fb060c24`.

## Revision record (rev 15 → rev 16)

The fifteenth review (`pre-pr-review-1792941712991.json`, rev-15
document/archive SHA256
`2ab16728582b9745d8008536ba120b169131ff3be44f207d065c18dd233da181`)
returned one P2 finding against rev 15; it was first reproduced on
the frozen rev-15 archive simulator with independently derived
exact expectations (`repro_review16_rev15.log`, SHA256
`ec048f2e6ef3352f44c7adea69fe9006ff26845af2ad8b1b1cc846b129d157fd`,
exit 0 — all 20 fault observations: the (a) H=2^52 tied batch
[H,1,−H]×[1,−2,1] emitted −0.6111111111111112 in all six peer
orders, both decay modes and both adjust policies where the
complete covariance fl(−2/3) is exact, while its translated
(+H,+H)-streams controls stayed green at the correct value; the
(b) K=2^27 zero-mean batch [K+1,K,−K−1,−K]×[K−1,1−K,K,−K]
emitted EXACTLY 0.0 in 96/96 executions (24 permutations × both
decay modes × both adjust policies) where the complete covariance
is exactly −1/4; and the continuations merged the corrupt
committed covariance into −0.3666666666666667,
−0.3055555555555556, −0.5238095238095238 and
−0.45833333333333337 where fl(−2/5), fl(−1/3), fl(−4/7) and
fl(−1/2) are exact — probe `repro_review16.py`, SHA256
`ba526112877d531247bc8e53b5e8840033cd5f4db3860f0e8520810383c85fff`),
then fixed, and every demanded vector is pinned in §10 (the r16F
row; Invariant 32 added, Decision 56 recorded, the r14F2 sentinel
literal reassessed):

1. **Finding — the finite batch moments rounded their deviations
   and products before compensation.** The spec's Step-1 formation
   built one-rounded deviation lists `x − m̄x` and accumulated
   one-rounded products `d·d` / `dx·dy`, with the exact
   reconstruction gated to retained-tail mixes only (rev 14,
   Invariant 30's scope). Two counterexamples, unrecoverable by
   correct mean tails or compensated summation: fl(K²−1) = K²
   discards the −1 BEFORE any compensation could see it (the
   zero-mean K=2^27 batch emitted cov exactly 0.0 in every peer
   order, policy and mode where −1/4 is exact); and the
   one-rounded deviations [H−0.5, 2/3, −H] measure about
   fl(1/3) instead of the exact deviations (the tied H=2^52 batch
   emitted −0.6111111111111112 where fl(−2/3) is exact). The
   committed corrupt covariance then poisoned every subsequent
   merge (the four continuation faults above).

2. **Fix — the common batch-moment contract (Invariant 32 /
   Decision 56).** Every finite batch-moment term now carries its
   subtraction and multiplication residuals into the compensated
   accumulation, for scalar AND retained inputs, zero-tail cases
   included: per-row deviations form exactly (twosum), every
   product contributes error-free pieces (xtwoprod over the four
   sub-product parts), the corrections −k·τx² / −k·τx·τy
   join the numerator error-free and gated by VALUE, reusing the
   existing error-free arithmetic with no fixture-specific
   branches. The SCATTER piles keep the dd pipeline (a
   deviation-square pile is positive-dominant: every discarded
   piece is ≈106+ binary orders below the lead sum, under its
   half-ulp). The CROSS pile cascades through the full register
   accumulator (xsum4) and divides once — full cancellation is
   admitted, and the complete term can sit hundreds of binary
   orders below every product's own rounding residual, beyond any
   two-word (acc, lo) window: the first-cut dd-piled cross
   implementation FOLDED the five preserved cancellation families
   to 0.0 (traced before shipping: the ±H² product leads cancel
   at register level, the leading word ends at the product-residual
   scale and the true content sits ~279 binary orders below,
   outside the double-double window) — the shipped xsum4 cascade
   cancels the residual mirrors pairwise at register level and
   holds every scale content in its own register.

3. **Preservation and the one reassessed literal.** Every
   preserved suite row stays byte-identical EXCEPT the r14F2
   [M/2,0,0]×[1+2^−52,1,1] all-scalar control, reassessed per the
   review's own "Reassess preservation literals that conflict with
   the mathematics": the exact complete covariance, re-derived
   independently from the fixture's exact values with rational
   arithmetic, rounds to 4.4352006878549327e+291; the
   rev-13..15 pin 4.435200687854932e+291 was one ulp BELOW the
   correctly rounded complete moment. Verified artifacts:
   `ewm_rev16_oracle_simulator.py` SHA256
   `c70967df9a4e7e706789480d31e443503274afdecff45f5545bcf742ef251362`;
   run log `ewm_rev16_run.log` SHA256
   `f6b6f155cae2a3e4d8b5ac9ce000cd9df5c754b179e21431a13aa7fe7b8d930f`
   (PASS 170 / FAIL 0, exit 0 — 168 preserved rows byte-identical
   + the reassessed r14F2 row + the new r16F row); preservation
   evidence `rev16_preservation_diff.log` SHA256
   `9d3131ee01aa1c1c37b0a4e689973152851be02d17f073f9a3bbc4fb8f0e3699`;
   the archive rerun reproduces the run log byte-for-byte; the
   final probe `probe_rev16_final.log` SHA256
   `ef612facd03516675118698a12291ec3a309a7357f95ff83c870d0cb77eb5340`
   (17/17 green — review vectors, controls, continuations, frozen
   families and the mechanism table; probe `probe_rev16_final.py`
   SHA256
   `a6b80fad2ab32644141bc140a101f4859fcf330eb290ef93079f9319691d42e3`).

## Revision record (rev 14 → rev 15)

The fourteenth review (`pre-pr-review-1792935938598.json`, rev-14
document/archive SHA256
`6dd4ec2affdc3495c7414665986b64e1fbf0fe3ce8a73ed94c75b8129ba3397a`)
returned one P2 finding against rev 14; it was first reproduced on
the archived rev-14 simulator with independently derived Decimal
expectations (`repro_review15_rev14.log`, SHA256
`d68f419bb580dbe26b972e04faf7fdc63eb55e148fd3d11974755f14873fd69e`,
exit 0 — all four continuation faults observed at the review's
literals, 6/6 control rows green; probe `repro_review15.py`, SHA256
`17be46bc9d2c81fc0aaf903757e05e1afa527c27b801a566f46df30b9808698a`),
then fixed, and every demanded vector AND its reorder control is
pinned in §10 (the r15F rows; Invariant 31 added, Decision 54
rescinded, Decision 55 recorded, Alternative AU rewritten):

1. **Finding 1 — Decision 54's institutionalized tail fold loss was a
   live continuation defect, not a benign pin.** Historical rev 14
   kept the raw `W == 0` state commit's pair at Document-recorded
   (mean 0.2, tail -0.2) for the review-order tied batch
   [H=2^400, M=2^200, 1, -H, -M] and only the single-batch emission
   was pinned (r11F4: the narrowed word 0.2) — but every subsequent
   MERGE consumes both words as the complete mean. Reproduced: keys
   [0,0,0,0,0,1] with x=y=[H, M, 1, -H, -M, 0] at alpha=.5 emit
   final mean **0.0** under BOTH adjust policies where EXACTLY
   fl(1/7) = 0.14285714285714285 (adjT) and EXACTLY 0.1 (adjF) are
   required; keys [0,1,1,1,1,1] (the same batch as the INCOMING
   state after a singleton zero) emit 0.0 where EXACTLY
   fl(2/11) = 0.18181818181818182 (adjT) and EXACTLY
   fl(1/6) = 0.16666666666666666 (adjF) are required; the
   [H, -H, M, -M, 1] reorder produced all four correct values —
   ORDER DEPENDENCE at the committed state, itself breaching the
   order-free batch contract. **Root cause:** the batch mean-tail
   reconstruction folded the register pair (cx + cxl) in float64
   INSIDE its own Neumaier list (`ewm_rev14_oracle_simulator.py:1151`);
   the mirrored y side and both anchored-branch tails held the same
   premature (ca_ + ca_lo) folds — the cross-scale registers
   (sx = -2^200, cx = 2^200, cxl = 1.0) collapsed to
   (-2^200 - 1 + 2^200 - 2^-54)/5 = **-0.2 at the mean word's own
   scale**, so the committed pair's complete content read 0.0 while
   the emission alone stayed 0.2 and masked the corruption.
   **Fix:** tau_x/tau_y rebuild from ALL compensated registers as
   SEPARATE cascade items — the rev-11 register-fold cascade keeps
   every register losslessly, reconstructing the review-order tail
   to EXACTLY fl(-2^-54/5) = -1.1102230246251566e-17 = the reorder's
   value (order-consistent). The raw single-batch emission keeps
   the pinned narrowed word; the retained-pair fallbacks (k*m̄x
   overflow; below-lattice tails) and every merge layer are
   UNTOUCHED — the [M,0,0] and [0,2u,6u] families, the F2a
   cov/std/continuation, the [M/2,0,0] sentinel and the r13F3
   continuation all re-probed byte-identical (`probe_rev15a_v2.log`,
   24/24). Decision 54 is rescinded and replaced by Decision 55
   (the consistent batch/merge mean-pair contract); the rev-14
   "documented rather than repaired" justification is withdrawn.

Validation (artifacts under state/artifacts/issue-29945/): the fault
reproduction above; the full rev-15 suite **168 + 1 = 169 PASS /
0 FAIL** (the r15F row pins the genuine residual and all four
continuation values with their reorder controls and the raw
single-batch emission; `ewm_rev15_oracle_run.log`, SHA256
`12ae8e80541c8559e0557aa7ae06bd05bf6d9c76efd287b3c23071f0d9fa7cc2`,
run_status=0; archive `ewm_rev15_oracle_simulator.py`, SHA256
`6f2990e9c88822d023d907bd1e10a4e96071e9058c45bfedf748b06b67afee62`;
the archive rerun reproduces the log byte-for-byte through the
terminal PASS line); preservation: **all 168 prior rows
BYTE-IDENTICAL — zero shifted rows, zero ulp shifts**
(`rev15_preservation_diff.log`, SHA256
`ba7860e911d8cd3195f617c7fb829045fc16ee774d58159a3d09cfb36dccbef5`).
The worktree diff remains this document alone; no implementation
has started (the runtime UT/BVT ladder of §10 stays a
post-approval task under the G-FEATURE-DESIGN gate).

## Revision record (rev 13 → rev 14)

The thirteenth review (`pre-pr-review-1792921361830.json`, rev-13
document/archive SHA256
`94aaea95919320efa2a2a6b107f70a0fd8f46f3c644706dde724128c970c8128`)
returned two P2 findings against rev 13; both were first reproduced on
the archived rev-13 simulator with independent 900/1200-digit Decimal
expectations (`repro_review13_rev13.log`, SHA256
`e2be69d841e54422e5d5d1fd29a6e5484264c383056887c9a10c799b52af2033`,
exit 0 — 2/2 findings observed, 4/4 non-fault rows green (the
[0,2,2] translation, the α=.25 hi-word-dominant control, the [M/2,0,0]
sentinel and the x=y +Inf-cov control); probe `repro_review13.py`,
SHA256
`b9e9686c83b37760bf278b86d7b2e65cf46d39067f974dd35daa34b87243b5aa`),
every observed fault matching the review's literals), then fixed, and
every demanded vector AND its translated/half-scale/permutation/
swapped-stream/continuation control is pinned in §10 (the r14F rows;
Invariants 29/30 added, Decision 54 recorded, Alternatives AU/AV
withdrawn):

1. **Finding 1 — the complete-mean normalization fired only on an
   exactly-ZERO narrowed high word, and the rev-13 doc claimed no
   other case could matter.** The claim "the tail of a nonzero high
   word is below its half-ulp" is false, and the rev-13 suite already
   carried a counterexample: the in-suite merge
   `[1.6e308, 1.6e308, 1.7e308]` α=.25 adjT printed its dd high word
   `1.643243243243243e+308` — one ulp BELOW its own pinned
   `1.6432432432432432e308`, tolerated by the 1e-12 comparator. The
   review's vectors are sharper: keys [0,0,1],
   x=y=[2^53, 2^53+2, −2^53+2] (α=.5, both adjust) stored the merged
   pair hi 1.0 with the EXACT tail .5 and emitted 1.0 where exactly
   **1.5** is required; and the absolute-mass layer singletons
   x=y=[2^600, −fl(5/3)·2^600 + 2^549] (α=3/8, adjust=false) emitted
   the dd lead 4·2^545 = 4.606887725612233e+164 with the exact +2^545
   sitting in the remainder tail, where exactly **5·2^545 =
   5.758609657015291e+164** is required. **Fix:** the shared
   complete-mean normalization now applies to EVERY high word at
   every merge layer — (P)/(F) commit and emit the exact two-sum fold
   hi′ := fl(hi + tail), tail′ := the exact residual (float tails
   fold through twosum — an exact identity whenever the tail stays
   below the high word's half-ulp boundary; retained pair tails fold
   through the exact pair two-sum whose fold loss stays retained; a
   non-finite fl(hi+tail) keeps the pair raw, so honest propagated
   non-finites are untouched) — and (C) replaces the zero-only swap
   with the same exact pair fold over the dd quotient plus commit-tail
   retention. The review vectors emit exactly 1.5 and 5·2^545 with
   their controls equal (translated [0,2,2] → 1.5; α=.25 control
   == its 900-digit expectation), and the lifted in-suite row now
   emits its own pinned literal exactly.
2. **Finding 2 — cross-centering supported only the both-scalar and
   the already-retained-sxy shapes.** Whenever either mean tail is a
   retained pair while sxy stays a finite scalar, the float64
   `sxy − τx·τy` multiplied the tails as scalars and raised TypeError
   (or worse): a first tied batch x=[M,0,0] at M = maxfloat64 —
   whose x tail is retained by rev-13 finding 2 — meeting
   y=[1+2^-52, 1, 1] (finite scalar sxy, scalar y tail) raised
   TypeError under BOTH adjust policies and on the swapped stream,
   where the finite covariance **8.870401375709865e+291** and finite
   x standard deviation **8.474406707690974e+307** are required
   (1200-digit Decimal). **Fix:** whenever ANY tail is retained while
   sxy is scalar, the complete cross moment re-forms through the
   exact chain — every per-row deviation forms EXACTLY
   (twosum(x, −m̄x): the raw float64 difference rounds an M-scale
   deviation by a half ulp it cannot afford, 9.98e291, more than the
   entire tail correction at the division scale), every product is
   error-free (xtwoprod over the four sub-product pieces per row),
   the tail correction −k·τx·τy enters error-free, and the pile
   cascades through ddsum/dddiv with ONE final fold. The all-scalar
   shapes keep the pinned float64 path and an already-retained sxy
   keeps its xsub route — the [M/2,0,0] sentinel keeps
   4.435200687854932e+291 and the x=y control keeps its honest +Inf
   covariance with std 8.474406707690974e+307.

**Scope correction recorded during validation (Decision 54):** the
first rev-14 build also normalized the RAW W == 0 batch state commit;
the preserved r11F4 row ([H=2^400, M=2^200, 1, −H, −M] — mean exactly
0.2) broke to 0.0. Root cause: the raw batch tail is the rev-9
compensated correction τx, whose reconstruction folds the register
pair `cx + cxl` in float64 — in that cross-scale shape the +1.0 term
is dropped THERE (fl(2^200 + 1) = 2^200) and the correction reads
−0.2 at the mean word's own scale, so folding hi+tail corrupts the
mean instead of normalizing it. The raw commit keeps its pinned
rev-9 contract (narrowed word fl(fl(Σ_registers)/k) + compensated
tail) and the complete-mean normalization lives at the merge layers,
where (hi, tail) IS an exact split of the computed complete mean.

Validation (artifacts under state/artifacts/issue-29945/): the fault
reproduction above; the preservation run — **164/165 preserved rows
BYTE-IDENTICAL plus ONE documented lift**: the
`[1.6e308,1.6e308,1.7e308]` α=.25 adjT merged mean moved from the
tolerated one-ulp-low dd high word `1.643243243243243e+308` to its
own pinned complete mean `1.6432432432432433e+308` (float-equality
proof in `rev14_preservation_diff.log`, SHA256
`a8335430a1f4176f2910a8e42dc4135358b147cf8f0d97dfee200cf6785083ef`)
— the finding-1 semantics applied to a NONZERO high word; the full
rev-14 suite **165 + 3 = 168 PASS / 0 FAIL** (the three r14F rows pin
every demanded vector with its translated/half-scale/permutation/
swapped-stream/continuation controls; `ewm_rev14_oracle_run.log`,
SHA256
`88977607307a377b0c97514c795e7cd4a480830d3741b971edd74d124155d96c`,
run_status=0; archive `ewm_rev14_oracle_simulator.py`, SHA256
`cc5f3bd6b6fdaac9cb9b1991b7043893a3a1a1ec988341208c5d265237d920be`;
the archive rerun reproduces the log byte-for-byte through the
terminal PASS line); the phased probe `probe_rev14a.py`, SHA256
`e3d7d333cb408d380abf95eb645a64fe99264fd18377f5ee6635cd57116fe13b`
(9/9 including the doctrinal continuation values,
`probe_rev14a_noB2.log` SHA256
`2005d07b9637631e2440a703b40a0062528436580edd6bb03b5a5ed267893a6`,
exit 0). The worktree diff remains this document alone; no
implementation has started (the runtime UT/BVT ladder of §10 stays a
post-approval task under the G-FEATURE-DESIGN gate).

## Revision record (rev 12 → rev 13)

The twelfth review (`pre-pr-review-1792915189178.json`, rev-12
archive SHA256
`02c6f089f87bbc8e0c707e7fb5d36783f5d1d8df87330bd69dde6565e8739413`)
returned three P2 findings against rev 12; all three were first
reproduced on the archived rev-12 simulator with independent
900-digit Decimal / 1200-digit expectations (`repro_review12_rev12.log`,
SHA256
`d3e29881ce3ef559fbaf3a6b6b94f020c763af057626f15e74befac03ac04b2a`,
exit 0 — 3/3 findings observed, 5/5 non-fault rows green (the four
translated/half-scale/unit-scale controls plus the already-correct
F3a mean row; probe `repro_review12.py`, SHA256
`e47c5162b3c0f1b64cbe707eda480657a31721f6df33a4df606f2eb052413cd2` —
a rerun after this correction is byte-identical to the first run
except the counter line), every observed
fault matching the review's literals), then fixed, and every
demanded vector AND its translated/permutation/continuation control
is pinned in §10 (the r13F rows; Invariants 27/28 added, Decisions
49–51, Alternatives AR–AT withdrawn/rejected):

1. **Finding 1 — the complete means were committed and emitted from
   the narrowed high word alone, dropping cancelled content.** The
   primary/share merged-mean products round once each (`s1·μ +
   s2·m̄` before the tail formula), and a merged word that cancels to
   ZERO commits 0.0 while the computed tail holds the real content.
   Keys [0,0,1], x=y=[2^53, 2^53+2, −2^53] (α=.5, both adjust)
   emitted final mean **0.0** where exactly 0.5 is required — the
   stored tail already contained the 0.5; and in (C) the singleton
   pair x=y=[2^600, −fl(5/3)·2^600] (α=3/8, adjust=false) has an
   error-free numerator of EXACTLY −2^545: `dddiv` returned hi (0, 0)
   with the exact remainder tail −2^545, and rev 12 emitted
   `xval(hi)` = 0.0. **Fix:** canonical complete-mean normalization
   before every store and emission — when the narrowed high word is
   exactly ZERO and the tail is nonzero, the tail IS the complete
   value and moves into the narrowed word (the residual goes to the
   tail slot); (P), (F) and (C) all commit the normalized pair and
   `out_mean` equals fl of the COMPLETE mean. The bit-identity
   promise is reconciled: a nonzero high word's tail never reaches
   its half-ulp, so every benign commit is a zero-op — all 161
   preserved rows stayed byte-identical.
2. **Finding 2 — the batch residual still materialized k·m̄x as
   float64.** With M = maxfloat64, a first tied batch
   x=y=[M, 0, 0] has finite sum M, finite mean 5.992310449541053e307
   and finite standard deviation 8.474406707690974e307, but fl(3·
   fl(M/3)) OVERFLOWS: rev 12's `twoprod(3, m̄x)` returned
   (inf, 0.0), the tail formula propagated inf and the merge raised
   the non-finite normalization AssertionError, under both adjust
   policies. **Fix:** the complete residual (Σx − k·m̄x)/k is also
   formed in the retained pair channel (xtwoprod/xsum4/xdiv never
   overflow, never touch the subnormal lattice) and commits there
   whenever the float64 tail path cannot carry it: the k·m̄x
   overflow case routes the retained pair through the extended
   centering — finite mean 5.992310449541053e307 and std EXACTLY
   8.474406707690974e307 retained, variance emitting its honest
   +Inf — with both [0,M,0]/[0,0,M] permutations equal and the
   [M/2, 0, 0] control unchanged (4.237203353845487e307).
3. **Finding 3 — the batch mean tail remained float64 below the
   subnormal lattice.** With u = 2^-1074, a first tied batch
   x=y=[0, 2u, 6u] has the exact mean 8u/3: the narrowed mean rounds
   to 3u and the EXACT tail −u/3 is below the lattice, so the
   float64 tail quantized to 0.0, the centering lost it, the
   retained variance became (19/3)u² instead of (56/9)u² and std
   emitted 3u instead of the correctly rounded 2u (1200-digit
   Decimal verification; the unit-scale [0, 2, 6] control
   correct). **Fix:** the below-lattice tail commits as the
   RETAINED pair, centers the extended scatter on it (variance
   (56/9)u², std exactly 2u, honest 0.0 variance emission), stays
   retained through the state commit, and the retained-tail routing
   sends follow-up merges to the pair-native absolute layer — the
   continuation keys [0,1] singleton [1u] merges the retained tail
   into EXACTLY 2u. Narrowing happens only at emission.

Validation (artifacts under state/artifacts/issue-29945/): the fault
reproduction above; the preservation run — **161/161 PASS with every
preserved row BYTE-IDENTICAL** (zero ulp shifts; verified both on the
bare build before any pinned row and on the final archive;
`rev13_preservation_diff.log`, SHA256
`64cefe7056dff80062731cc2a9fdaa693236ade5c8ec25a1b6e0f70e83db0663`);
the full rev-13 suite **161 + 4 = 165 PASS / 0 FAIL** (the four
r13F rows pin every demanded vector with its
translated/permutation/continuation controls; `ewm_rev13_oracle_run.log`,
SHA256
`80973eed3cf91a0ff9a737500a45306c1dbd3ff4a68deeca963f53e48923bc76`,
run_status=0; archive `ewm_rev13_oracle_simulator.py`, SHA256
`2340b37f1e2d5727bb7f58449d830d4b9b73993fd4edacbf7c82494d129d20e6`;
the archive rerun reproduces the log byte-for-byte through the
terminal PASS line). The worktree diff remains this document alone;
no implementation has started (the runtime UT/BVT ladder of §10 stays
a post-approval task under the G-FEATURE-DESIGN gate).

## Revision record (rev 11 → rev 12)

The eleventh review (`pre-pr-review-1792901147724.json`, rev-11 archive
SHA256 `cf10d7cd4e2b7caece9326b3ed416d984ee56a92e5800fcf45c35ce4cb4e84ad`)
returned three P2 findings against rev 11; all three were first
reproduced on the archived rev-11 simulator with independent
900-digit Decimal requirements (`repro_review11_rev11.log`, SHA256
`3c6746d944304b0e086f4ac7c5027d5298fbfe12928b5a0f514bc6ed7e12e8a4`,
exit 0 — 3/3 findings observed, four controls green, every observed
fault value bit-identical to the review's own literals), then fixed,
and every demanded vector AND its translated/reordered control is
pinned in §10 (the r12F rows; Invariants 21/24 extended, 26 added,
Decisions 46–48, Alternatives AO–AQ withdrawn):

1. **Finding 1 — the complete-mean products and quotient remainder
   still rounded before their compensation acted.** `xmul` gave each
   merged-mean numerator and weighted-difference product one rounded
   extended word, and `dddiv` subtracted that same one-rounded
   `q_hi·T` from the numerator — the remainder computed the residual
   of a rounded product, so even `dddiv(1, 3)` returned a zero tail
   word. With x=y=[2^600, 2^600+2^548] the singleton stds emitted
   **5.150657059589176e+164** (alpha=3/8, adjust=false) and
   **5.319575737017211e+164** (alpha=.5, adjust=true) where
   √15/8·2^548 = 4.460599859785886e+164 and √2/3·2^548 =
   4.343415401260641e+164 are exact (both translated [0, 2^548]
   controls already exact). **Fix:** `xtwoprod` — the error-free
   pair product whose leading word is the same pair `xmul` returned
   plus its EXACT residual — feeds every complete-mean weighted
   product through `ddsum_e` (leading words fold through the
   exact-alignment chain with fold losses kept; residuals join the
   lo word), and `dddiv` subtracts the error-free `q_hi·T` so the
   remainder is exact. The carried-moment assembly products stay
   one-rounded; the shared bounded reconstruction owns their
   accumulation losses. Power-of-two masses produce exact products,
   so every preserved row keeps its rev-11 value bit-for-bit.
2. **Finding 2 — both register restorations discarded real content.
   `neumaier_pair` computed its lower registers' tail by a naive
   `comp_lo += r` chain, and the accumulator's restore xadd'd
   `acc + regs[0]` (dropping whatever fell outside the 53-bit
   window) and then discarded its leftover re-folds' own losses.
   The tied batch [-2^54, 2^216, -2^162, 0.75, 2^162, 2^54,
   -2^216] (alpha=.5, adjust=true) cascades to registers
   [2^162, -2^162, 0.75]: the naive tail rounds 0.75 away and the
   mean emits **0.0** where 3/28 is exact (the magnitude-reordered
   control was right); the accumulator over
   [-2^323, 2^215, 2^431, 2^323, 0.75, -2^431, -2^215] returned
   **0.0** where 0.75 is exact — neither failure approaching the
   register-capacity limit. **Fix:** one SHARED bounded
   reconstruction at both sites: the registers fold through the
   same exact-alignment chain as their comp word, and the restore
   keeps the head fold's exact loss, then processes every register
   and fold loss through a DESCENDING-magnitude queue in which
   mirror pairs cancel exactly before small content folds — a fold
   that returns the whole item as its loss is held back and retried
   only after the running total changed; a pass that changes nothing
   can never host a held item, and the remainder flushes through
   xadd, the documented float64 information limit. Termination is
   structural (no countdown loop); single-register/benign walks are
   bit-identical.
3. **Finding 3 — the Dekker splitter overflowed and silently returned
   an UNSPLIT operand beyond ~1.34e300.** `_split`'s native-domain
   `134217729·v` scaling overflows at magnitudes whose mean-tail
   corrections are observable: `twoprod(3, 2^1000+2^948)` returned
   `e = 0.0` losing the exact residual −2^948, so the first tied
   batch [H=2^1000, H+U, H+U] (U=2^948) stored the mean tail
   **−2U/3 instead of −U/3**, drove the retained variance NEGATIVE
   and `out_std` raised a math-domain ValueError at a vector whose
   translated [0, U, U] control was exact at
   1.1215783890386092e+285. **Fix:** the split runs in the
   MANTISSA domain (the 2^27+1 scaling multiplies only the
   normalized mantissa — exact at every representable magnitude —
   with ldexp piece reassembly), and the raw float64 k·m̄x products
   form their residual in the mantissa domain, reassembled at
   ea+eb — bounded by half an ulp of p, in range whenever
   fl(a·b) is finite. A raw-domain split is IMPOSSIBLE exactly at
   the top band [2^1024·(1−2^-27), maxfloat64] (the high word
   would be 2^1024), so no raw-domain caller exists above that
   frontier. NO output clamping: a negative retained variance
   remains the internal error it is (§7 shape).

Validation (artifacts under state/artifacts/issue-29945/): the fault
reproduction above; the preservation run — **153/153 PASS with every
preserved row BYTE-IDENTICAL** (zero ulp shifts;
`rev12_preservation_diff.log`, SHA256
`050307805ecc029a96c0695905fd4c306af7b5cf3bd60e58687df4d0ea08f796`);
the full rev-12 suite **153 + 8 = 161 PASS / 0 FAIL** (the eight
r12F rows pin every demanded vector with its translated/reordered
control, the dddiv(1,3) remainder tail, the extended-sum accumulator
value, the k·mean two-product residual and the splitter boundary;
`ewm_rev12_oracle_run.log`, SHA256
`759bd2e4156f9b7206b1d0560fff9a37d9dfae2966128ad41df26a660af65a34`,
run_status=0; archive `ewm_rev12_oracle_simulator.py`, SHA256
`ccaa827e2cb5dfb4803f0e6ef4dcc1e31f5e8391fc11e84e032755997232e4b1`;
the archive rerun reproduces the log byte-for-byte through the
terminal PASS line). The worktree diff remains this document alone;
no implementation has started (the runtime UT/BVT ladder of §10 stays
a post-approval task under the G-FEATURE-DESIGN gate).

## Revision record (rev 10 → rev 11)

The tenth review (run 1792890629351, rev-10 archive SHA256
`a857cbf2dd306afda34498972a6f9461dcac2fa6dec8e1afe48f684389fda3e9`)
returned four P2 findings against rev 10; all four were first
reproduced on the archived rev-10 simulator with independent
900-digit Decimal expectations (`repro_review10_rev10.log`, SHA256
`7addd387f29025abe5d0480e18121828b84200720ad1ad95ecc9f22fefe3013e`,
exit 0 — 4/4 findings observed, five controls green), then fixed, and
every demanded vector AND its reordered control is pinned in §10 (the
r11F rows; Invariants 21/23 rebuilt, 24/25 added, Decisions 42–45,
Alternatives AK–AN withdrawn):

1. **Finding 1 — the absolute-mass layer still crushed the complete
   means to one 53-bit extended word.** The specified
   `(W·(μ+μ_lo) + A·(m̄+τ))/T` kept one extended carrier: the
   numerator folded through `xadd`, a single rounded 53-bit sum, so a
   54-bit merged center lost its tail. With x=y=[2^600, 2^600+2^548]
   (α=.5, adjF) the arrival deviation measured 2^548 instead of 2^547
   and ewmStd emitted **2^547.5 = 6.515123101890962e+164** where
   2^547 = 4.606887725612233e+164 is exact (the translated [0, 2^548]
   control was right), and the mixed-scale continuation
   x=[2^53, +2, +4] with y = 2^-260·[0, 2, 4] emitted cov
   **1.8891618714269098e-78** / corr **0.9949366763261822** where
   2.75·2^-260 = 1.4843414704068577e-78 and 1 are exact. **Fix:** the
   (C) means are true DOUBLE-DOUBLE pairs at every step — `ddsum`
   (the exact-alignment fold chain with the fold losses kept as the
   second word), `dddiv` (leading quotient, exact hi residual plus the
   second word, tail quotient), `ddsub` sources/targets and `dd_mul`
   terms (the three correction products folded; the extended products
   never overflow, so the representable-raw-difference fast paths and
   the q·q/mass fallback are gone); the stored narrowed mean keeps
   `fl(ν_hi)` and the tail is the narrowed residual plus the dd tail
   word, folded once.
2. **Finding 2 — the retention gates ignored the weighted deviation
   products when both source moments are zero.** The moment gates
   tested weight × moment, the deviation gates tested an unweighted
   threshold; with both moments zero the merged moment's entire
   content is `w·dev²` (cross `w·dx·dy`) and those products
   underflowed unwitnessed: keys [0,1000] h=1 adjF x=y=[2^-100, 0]
   emitted std 0.0 where the engine's own rounded masses make
   **2.4099198651029665e-181** exact, and the incoming-side
   counterpart keys [0,1e-300] x=y=[0, 2^-100] emitted 0.0 where
   **6.567697842037889e-181** is exact (variance honestly 0.0 in
   both). **Fix:** six weighted-deviation tuples per (P)/(F) branch —
   (dx,dx,w1), (dbx,dbx,w2), (dy,dy,w1), (dby,dby,w2), (dx,dy,w1),
   (dbx,dby,w2) against the share weights in the share mirror —
   rebind to (C) whenever the frexp exponent sum reaches −1021,
   before the assembly multiplies.
3. **Finding 3 — nonzero subnormal-lattice alignment losses were
   unrecorded.** The rev-10 exact-residual claim held only when the
   ldexp-aligned operand stayed exact; on the subnormal mantissa
   lattice bits round away BEFORE the two-sum and only the
   post-quantization residue was recorded: the tied batch
   [(1.5,1.5),(−1.5,−1.5),(H,H),(−H,−H),(H,−H),(−H,H)] at H=2^537
   returned cov **0.6666666666666666** small-pairs-first (0.75 exact;
   small-pairs-last already right), and the direct accumulator value
   0.75 + 2^1073 − 2^1073 returned **1.0**. **Fix:** `_align_sum` —
   every fold (both directions, main accumulator and register folds)
   runs its two-sum at a scale where BOTH mantissa views are normal:
   gaps in [1022, 2043] scale both (the big mantissa up by
   gap−1021 ≤ 1022 stays finite/normal/exact; the small into
   [2^-1022, 2^-1021)) and rescale the exact complement; gaps beyond
   2043 cascade the smaller operand INTACT. Capacity stays 128 (the
   ≥53-order descent argument holds a fortiori with exact
   complements).
4. **Finding 4 — the float64 chains' single compensation register
   broke the peer-permutation contract on FINITE vectors.** The plain
   Neumaier chains dropped residuals clobbered INSIDE comp: with
   H=2^400, M=2^200 the ten-row batch [(h,h),(−h,−h) for h∈{H,M,1}]
   + [(h,−h),(−h,h) for h∈{H,M}] returned cov **0.0** where 0.2 is
   exact (grouped control right), and the mean [H, M, 1, −H, −M]
   returned **0.0** (reorder right). **Fix:** the chains cascade —
   (sum, comp, comp_lo) with comp's own fold losses in the next
   bounded register; benign sums bit-identical. A NON-FINITE residual
   (an overflowing partial sum in a branch the gates reject anyway,
   e.g. the rev-6 range-safety mean-tail lists) folds ONCE into the
   first register and never enters the loop — `-inf − (-inf) = NaN`
   would otherwise never terminate; down-stream propagation is
   identical to the single-compensation form (found while re-running
   the preserved [1.6e308, 1.6e308, 1.7e308] rows; the vector
   completes with its pinned values).

Validation (artifacts under state/artifacts/issue-29945/): the fault
reproduction above; the preservation run — **145/145 PASS with every
preserved row BYTE-IDENTICAL** (no ulp shifts at all this revision;
`rev11_preservation_diff.log`, SHA256
`fc3d145f661fded96504bd126fd1fe03df7c188c94dcebc8850e627240df00c5`);
the full rev-11 suite **145 + 8 = 153 PASS / 0 FAIL** (the eight
r11F rows pin every demanded vector with its reordered control;
`ewm_rev11_oracle_run.log`, SHA256
`8061eb19d91193040136f2a0f181dff6261116e23274f28e15524ba9694a3503`,
run_status=0; archive `ewm_rev11_oracle_simulator.py`, SHA256
`a0287e98543ba7fc1d539e1a74a15d2b7e3c803a8e7cdc15f7b3f77b2a8cdeb3`;
the archive rerun reproduces the log byte-for-byte through the
terminal PASS line). The worktree diff remains this document alone;
no implementation has started (the runtime UT/BVT ladder of §10 stays
a post-approval task under the G-FEATURE-DESIGN gate).

## Revision record (rev 9 → rev 10)

The ninth review (run 1792885902092, rev-9 archive SHA256
`d9e206f6598d2f08082003be831c2d5372df87b4905fb1c49cc9e193408cf0b0`)
returned four P2 findings against rev 9; all four were first reproduced
on the archived rev-9 simulator with independent 900-digit Decimal
expectations (`repro_review9_rev9.log`, SHA256
`2449d8720443263d26c4cf7cb3fe089461f1e055f8d1af9d3d5bef17f7a0c09f`,
exit 0 — F1/F2/F3/F4 all observed, four controls green), then fixed,
and the demanded vectors AND their reordered controls are pinned in §10
(rows r10F1–r10F4) with Invariants 21/22/23:

1. **Finding 1 — merge moments used the hi parts although the moments
   now center on hi+lo.** The merge deviations were measured from the
   source HI parts while the moments centered on hi+lo, and the
   hi-only equality shortcut `m̄x == μx` fired for equal hi parts with
   different tails. With x=y=[2^53, 2^53+2, 2^53+4] (α=.5, adjF,
   distinct keys) the rev-9 oracle committed final var/cov **4.25**
   where the complete-mean merge returns exactly **2.75** (the [0,2,4]
   translation already returned 2.75 — an Invariant-5 violation); a
   tied first pair followed by the third value fails identically, and
   [2^53, 2^53+2, 2^53] returns **0.5** instead of **0.75**. The
   rev-9 end-of-merge δ² correction cannot fix this: centered on the
   merged hi with sources carrying their OWN tails, it has no cross
   term ((d−δ)² = d² − 2dδ + δ² — rev 9 kept only the outer pieces).
   **Fix:** the merged mean tails are formed FIRST (same compensated
   mass·mean numerators as rev 9, `fl(ν)` bit-identical) and every
   merge deviation is measured BETWEEN COMPLETE MEANS — state hi+lo,
   arrival hi+lo, merged `fl(ν)`+tail; no end correction exists
   anywhere in a rev-10 merge; the equality fast paths require hi AND
   tail equality on both sides. The equal-hi/different-tails
   continuation (state `2^53` carrying tail 1 vs the tie
   [2^53−2, 2^53+2]) returns 29/9 ≈ 3.222222222222222 where rev 9
   committed 3.0 (§10 r10F1).
2. **Finding 2 — the accumulator's supported domain was not covered.**
   The cascade capped residual depth at six registers AND the
   larger-incoming alignment dropped the old accumulator: when the
   old accumulator's scaled mantissa underflows (shift > 1074) the
   two-sum residuals read zero and the old value silently vanished.
   The tied batch [(1,1),(−1,−1),(H,H),(−H,−H),(H,−H),(−H,H)] at
   H = 2^600 returned a covariance of exactly 0.0 in the
   unit-rows-first order while 1/3 is exact (the unit-rows-last
   control returned 1/3 only because two defects canceled); the
   30-row ten-scale batch ([(h,h),(−h,−h)] for h = 2^600 down to
   2^250, then the cross-negatives except the 2^250 scale) returned
   0.0 where 2^500/15 = 2.1822604052640945e+149 is exact, again
   order-dependent. **Fix:** SYMMETRIC alignment handling — the old
   accumulator cascades intact as the residual, mirroring the first
   branch's rule for an underflowing incoming term — and the capacity
   is 128 registers with a domain justification: every fold residual
   is bounded by half an ulp of its folded operands, so each cascade
   level descends at least 53 binary orders below the previous
   level's scale, and the supported term grammar bounds relevable
   term exponents to [−3222, 3074] (subnormal masses against
   subnormal deviations at the floor; the q·q/mass overflow fallback
   at the top) — a span of at most 6296 exponent orders, at most
   ~120 register levels. Both counterexamples and their reorderings
   are pinned (§10 r10F2).
3. **Finding 3 — retention decisions read unweighted exponents.**
   The lattice gate examined the stored moment/deviation exponents
   and missed normal moments whose WEIGHTED products underflow: keys
   [0,0,1000], half-life 1, adjust=false, x=y=[−2^-50, +2^-50, 0] —
   the state's variance 2^-100 passed the gate, but the decayed mass
   w1 ≈ 2^-1000 multiplied it to ≈2^-1100 and the float64 product
   rounded to zero BEFORE the division and store; rev 9 emitted std
   0.0 where the engine's own rounded masses require exactly
   2.713328551617619e-166 (the variance is honestly 0.0 — below the
   emission lattice while its square root is representable).
   **Fix:** every merge layer additionally rebinds when the WEIGHTED
   PRODUCT of a nonzero moment with its side's weight would land on
   the subnormal lattice (mantissas live in [0.5,1), so the product
   sits at exponent e_val + e_weight, subnormal from −1021 down):
   the (P) layer tests w1/w2 and the share layers test s1/s2; the
   (C) extended mass·moment product, division and store retain
   ≈2^-1100, so ewmVar honestly emits 0.0 and ewmStd emits the
   representable 2.713328551617619e-166. The incoming-side
   counterpart (keys [0, 1e-300], tie arrival, variance 2^-100 under
   arrival mass A ≈ 6.93e-301) emits std 7.394570388520962e-166 where
   rev 9 emitted 0 — both pinned (§10 r10F3).
4. **Finding 4 — correlation normalized narrowed moments.** The
   emission clause narrowed vxy/vxx/vyy before the geometric
   normalization, contradicting the retained-value claim made for
   ewmStd/ewmCorr: on the perfectly correlated streams
   [(±2^-537, ±1.5·2^-537)] (α=.5, adjF, tied batch AND singleton
   merges) subnormal quantization of the numerator and the geometric
   denominator emitted corr **2.0** (−2.0 on the negative pair) where
   every retained bit says exactly ±1.0; the unit-scale control
   returned 1.0. **Fix:** the normalization is computed from the
   RETAINED moments — the extended square root (already specified for
   ewmStd) plus the extended product and quotient — narrowing only
   the final verdict; the float64 view keeps driving the
   NULL/finiteness routing, and on plain float64 states the form is
   bit-identical to the rev-9 expression (the same sqrt/product/
   quotient roundings, power-of-two rescalings only — verified by the
   129/131 byte-identical preservation rows). Pinned in §10 r10F4
   with both signed subnormal paths and the unit-scale controls.

Preservation evidence (`rev10_preservation_diff.log`): the 131-row
rev-9 oracle rerun under the rev-10 simulator is PASS 131 / FAIL 0
with **129 rows byte-identical**; the two shifted rows (the
partial-decay carried variance and the deep-decay subnormal-W carried
variance) move by one ulp of the emitted 17-digit detail and are
audited at 900 digits against the engine's own rounded masses: the
rev-9 details were the nearest float on exactly these two vectors and
rev-10 sits one ulp farther — removing the defective δ² chain (no
cross term) redistributes the final rounding bit; both rows' pinned
tolerance assertions are unchanged PASSes. The full rev-10 suite is
**145/145 PASS** (the fourteen new rows carry the review's exact
expectations: 2.75 both paths with the translation control, 0.75,
29/9, 1/3 both orders, 2^500/15 both orders, var 0.0 with std
2.713328551617619e-166, var 0.0 with std 7.394570388520962e-166,
corr 1.0/1.0/−1.0 with the 1.0 control); the archive rerun reproduces
the log byte-for-byte (`ewm_rev10_oracle_simulator.py`,
`ewm_rev10_oracle_run.log`). The worktree diff remains this document
only; implementation stays blocked until this revision passes review.

## Revision record (rev 8 → rev 9)

The eighth review (run 1792797859202, rev-8 archive SHA256
`abc36eac9be303a6447b6331db4150369b4b676af2105d1db2f333f0a08a5c57`)
returned four P2 findings against rev 8; all four were first
reproduced on the archived rev-8 simulator with independent 900-digit
expectations (`repro_review8_rev8.log`), then fixed, and the demanded
vectors are pinned in §10 and Invariants 19/20:

1. **Finding 1 — the share-reconstruction certificate also rounds.**
   `:1572` accepted subnormal shares when `s·denom == mass`, but
   that multiplication also rounds: with the retained `W = 2^-1074`
   under the fractional arrival `A = 0.75`, the share `W/(W+A)`
   rounds to `2^-1074`, "reconstructs" W, and commits a
   25%-understated merge — mean `4.940656458412465e-124`,
   var/cov `4.940656458412465e+76` where independent arithmetic on
   the same rounded masses requires `6.587541944549954e-124` /
   `6.587541944549954e+76` (keys `[0,1072,1074]`, h=1, adjust=false).
   **Fix:** the float64 share layers NEVER commit a subnormal share on
   either side; every such merge rebinds to (C), whose `mass·value`
   products keep the mass exponent exact. The rev-9 simulator returns
   the independent values with corr exactly 1.
2. **Finding 2 — one residual register loses cross-scale residuals.**
   `:1427` folded every two-sum residual into a single extended
   compensation register by plain `xadd`; the three-scale tied batch
   (H=2^600, M=2^400, L=2^200, means exactly zero) folds its L²
   residuals into an M-scale register that rounds them away — one
   peer order returns a covariance of exactly 0.0 while the true
   L²/5 is representable. **Fix:** cascading registers — the
   level-0 loop and the first register stay bit-identical to rev 8
   (every no-loss vector keeps its byte-identical value, verified by
   log diff) and each fold's exact discarded bits pass to the next
   register; both review orderings now return
   `5.1644997561738175e+119`, means exactly 0, honest +Inf variance,
   finite std, corr NULL. The falsified "~1074 binary orders"
   claim (§12) is replaced by the bounded cascade-depth contract.
3. **Finding 3 — moments centered on the rounded output mean.**
   `:1393/:1539` measured the scatter around the already-narrowed
   float64 mean: `[2^53, 2^53+2]` (adjust=false) returns
   var/cov/std `2/2/√2` — a 100% boundary error — while its `[0,2]`
   translation returns the exact `1/1/1`; the defect exists in both
   the tied-batch and the two-singleton merge paths (Invariant 5).
   **Fix:** the batch carries the compensated mean tail and subtracts
   the exact mean-tail correction; the account carries its mean as a
   hi+lo pair and every merge computes the true merged mean ν from
   compensated mass·mean numerators (Dekker two-products + two-sums,
   including the mass·lo carries), keeps `fl(ν)` bit-identical to
   the rev-8 mean forms, and subtracts `δ² = (ν−fl(ν))²` per unit
   mass (cross `δx·δy`). Both boundary vectors now return
   var/cov/std exactly 1.
4. **Finding 4 — subnormal-lattice moments destroyed before
   emission.** `:1389` triggered the extended recomputation only on
   non-finite results and `:1770` took the sqrt of the stored
   (already destroyed) value: tied `[−1e-200, 1e-200]` emits std 0.0
   although 1e-200 is representable (the true variance 1e-400 is
   honestly below the emission lattice); the singleton merges
   `[−2^-537, +2^-537]` emit var/cov/std `0/0/0` although
   `2^-1074 / 2^-1074 / 2^-537` are exactly representable — the
   primary merge rounds each `0.5·2^-1074` term to zero BEFORE the
   summation and no gate sees the loss. **Fix:** deviations at or
   below 2^-511 (their squares reach the subnormal lattice),
   subnormal nonzero moments, and merges with deviations of that
   scale rebind the affected statistic to the extended channel; the
   store keeps results that would narrow to a subnormal value or to
   zero from a nonzero magnitude, and ewmStd/ewmCorr read the
   retained values. The simulator now emits std `1e-200` (var 0.0
   honest, corr NULL) and var/cov exactly `2^-1074`, std exactly
   `2^-537`, corr exactly 1.

Preservation evidence (`rev9_preservation_diff.log`): the 123-row
rev-8 oracle rerun under the rev-9 simulator is PASS 123 / FAIL 0
with **122 rows byte-identical**; the single shifted row is the
translated FIRST tied batch, whose detail value moves
`1.5555555555555571 → 1.5555555555555556` — the finding-3 recentering
acting on a sub-ulp batch-mean tail, closer to the true 14/9 and
inside the row's pinned 1e-9 tolerance. The full rev-9 suite is
**131/131 PASS** (the eight new rows carry the review's exact
expectations); the archive rerun reproduces the log byte-for-byte.
The worktree diff remains this document only; implementation stays
blocked until this revision passes review.

## Revision record (rev 7 → rev 8)

The seventh review (run `1792793541181`, archived at
`state/artifacts/issue-29945/pre-pr-review-1792793541181.json`) returned
four P2 findings on rev 7. **All four were verified before revision** on
the archived rev-7 simulator
(`ewm_rev7_oracle_simulator.py`, SHA256
`d122deedf69a9fde6eb93f64e71ed40212ad7baa3214457dbc178010526142d5`):
a SIGALRM watchdog gain evidences the two hanging vectors, and every
other fault value was reproduced digit-for-digit with 900-digit
independent expectations (probe `/tmp/mo29945/repro_review7.py`; log
`repro_review7_rev7.log`, exit 0).

1. **Batch raw mean-deviations overflow before their weighting (P2 at
   doc `:1255/:1267`).** For the tied batch
   `[1.7e308, 1.7e308, −1.7e308]` the batch mean is finite
   (`5.666666666666667e307`) but the min-side raw deviation
   `x − m̄x = −2.2667e308` overflows to −Inf *in every peer
   permutation*; rev 7 multiplied that −Inf into the extended layer,
   where the mantissa normalization (`while ≥ 1: halve`) never
   terminates — the archived simulator **never returned** (6/6
   permutations; and keys `[0, 8e-16, 8e-16, 8e-16]`, h=1e308, adjF,
   `x=y=[0, 1.7e308, 1.7e308, −1.7e308]` hangs the same way while its
   final variance/covariance ≈ `1.4278497164812023e293`, corr 1, is
   finite). **Resolution:** §5 gates the raw deviation lists on
   finiteness before any multiplication and routes a non-finite list
   to **anchored deviations** — subtraction from the exact midpoint
   a = min/2 + max/2 first (range-preserving: |x_i − a| ≤ half the
   batch range), δx = m̄x − a likewise bounded, scatter via the exact
   identity `Σ(x−m̄x)² = Σ(x−a)² − k·δx²` (cross:
   `Σ dax·day − k·δx·δy` with per-side anchors, δ = 0 on a
   finite-deviation side, zero dax on a constant side) in the
   extended compensated form; **the extended normalization now
   enforces finite mantissas** — a non-finite operand is a defined
   internal error, never a loop (Invariant 17; Decision 30;
   Alternative Y withdrawn). §10 pins the six permutations (mean
   `5.666666666666667e307`, honest +Inf var/cov, **finite std
   `1.6027753706895077e308`**, corr NULL) and the low-weight arrival
   row (`1.4278497164812023e293`, corr 1).

2. **Inexact nonzero subnormal weight scaling (P2 at doc
   `:1340/:1351`).** The `frexp/ldexp` normalization is exact off the
   subnormal lattice but ties-to-even on it: keys `[0, 2e-15]`, h=1e308,
   adjF, `x=y=[0, 1e150]` — arrival mass exactly three smallest
   subnormals, `A = 1.5e-323` — scaled to `w2 = 1e-323` (two
   subnormals), and rev 7's primary path committed
   `1.976262583364986e-173` / `1.976262583364986e-23` where
   independent arithmetic on the *same rounded arrival mass* gives
   `1.4821969375237395e-173` / `1.4821969375237397e-23` (corr 1) —
   a silent 33% reweight; the rev-7 gate rejected only weights
   rounded to zero. **Resolution:** §5 requires the **bit-exact ldexp
   round-trip** (`ldexp(w, e) == mass`, both sides) before the primary
   form runs; when the scaling is degenerate *or inexact* the shares
   are built from the RAW masses (`nsum = W + A`), and a
   **subnormal share commits only when it reconstructs its mass
   bit-exactly** (`s·denominator == mass`) — a failing side rebinds to
   the absolute-mass layer, where every contribution forms as a
   mass·value product with the mass exponent preserved (Invariant 16;
   Decision 31; Alternative Z withdrawn). Pinned on **both merge
   sides**: incoming (above) and the engine-reachable retained
   odd-subnormal state `W = 3·2^-1074` (three-observation adjT batch,
   one 1074-half-life decay) meeting `A = 2` →
   `7.4109847e-320` / `7.4109847e-316`, corr 1 (rev 7 committed
   `9.8813e-320` / `9.8813129e-316`, 33% high).

3. **Uncompensated extended signed accumulation (P2 at doc `:1267`,
   simulator `:200`).** The extended accumulation folded plain adds,
   losing cancelling signed contributions ~53 binary orders below the
   running partial sum: the tied pair batch
   `[(H,H),(s,s),(H,−H),(−H,H),(−s,−s),(−H,−H)]`, H=1e200, s=1e150,
   returned covariance **0.0** (means exactly 0) while reordering the
   same pairs returned the independent `2s²/6 = 3.333333333333333e299`
   — breaking Invariant 7's peer-permutation contract (doc `:902`)
   even though exponents were retained. **Resolution:** every extended
   moment sum — the batch sxx/syy/sxy fallbacks and the (C) moment
   assemblies alike — is the **compensated extended Neumaier
   accumulation**: a two-sum on the aligned mantissas with the exact
   residual folded into a second extended register. **Defensible error
   bound:** each two-sum residual is exact (standard compensated-
   summation property), and the residual register is ulp-scale, so the
   returned value differs from the exact sum by at most one rounding
   of the result plus `k·u²·max|term|` at the running-sum scale
   (u = 2⁻⁵³) — the Neumaier bound carried to the aligned extended
   form. The claimed information limit is **corrected** (rev 8
   supersedes rev 7's "terms more than ~1020 binary orders below the
   leading partial sum round away"): with compensation a term is lost
   only when its alignment shift exceeds ~1074 binary orders — the
   subnormal lattice of the running partial sum — where it quantizes
   and is carried at its own scale in the residual register; §10 pins
   the covariance **bit-identically across all 720 peer permutations**
   plus the review's two orderings and the finite std
   `8.16496580927726e199` at the honest +Inf variance (Invariant 17;
   Decision 32; Alternative AA withdrawn).

4. **ewmStd computed from the narrowed variance (P2 at doc `:1535`,
   simulator `:714-718`).** The std was `sqrt` of the *emitted
   float64* variance: for the retained extended `4e308` population
   variance (tied `[−2e154, 2e154]` first batch) the emitted variance
   is honestly +Inf and the perfectly representable standard
   deviation `2e154` came out as `sqrt(+Inf) = Inf`. **Resolution:**
   §5 Outputs define `ewm_std` as the square root applied **to the
   retained scaled moment before the float64 narrowing** (extended
   sqrt: exact power-of-two exponent splitting, one float64 mantissa
   sqrt; a plain float64 variance keeps the plain sqrt) — so variance
   overflow does not imply standard-deviation overflow and the
   var/std verdicts may legitimately differ on the same row. §10 adds
   independent ewmStd assertions on the existing recovery vector:
   rows 1–2 std **exactly `2e154`** at the +Inf-variance rows;
   recovery row var `3.90625e305` with std `6.25e152` (Invariant 17;
   Decision 33; Alternative AB withdrawn).

**Post-revision validation:** the archived rev-8 oracle suite
(`ewm_rev8_oracle_simulator.py` / `ewm_rev8_oracle_run.log`, 123/123
PASS, exit 0) keeps **every one of the 116 rev-7 rows byte-identical**
(verified by log diff — zero preserved PASS lines changed; the primary
paths, the finite share commits and the (F)/(C) intermediate results
for preserved vectors are unchanged) and greens the seven new rows with
the review's exact expectations and fault records above. The worktree
diff remains this document only; implementation stays blocked pending
approval of this revision.

## Revision record (rev 6 → rev 7)

The sixth independent design review rejected rev 6 with five P2 and one
P3 finding. Each was verified against the repository code and/or
reproduced numerically on the archived rev-6 simulator
(`state/artifacts/issue-29945/ewm_rev6_oracle_simulator.py`, SHA256
`9b1409368ed8cb50b07ba578a296f37954cfb458b8bb9843de89a784efaa3a4c`)
before being resolved here (reproduction log:
`state/artifacts/issue-29945/repro_review6_rev6.log`, SHA256
`ae13f62b82d08d0a67a3fba46cdc6493c2a7c035f4e04b56eb7ecb25bd30424d`;
independent expectations computed with 900-digit decimal arithmetic on
this revision's own mass semantics; `:NNN` line references below point
into the **rejected rev-6 document**, quoted from the review):

1. **Replace the overflowing batch-mean recurrence.** Verified: the
   rev-6 fallback `m += (x_i − m)/i` (§5 `:1029`) overflows at the
   *subtraction* for opposite-sign extremes — for the tied batch
   `[1e308, 1.1e308, −1e308]` the primary Neumaier sum overflows and
   the recurrence's `−1e308 − 1.05e308` overflows to −Inf, so the
   archived simulator returned **mean −Inf** for the two same-sign-first
   orders and the correct finite
   `3.6666666666666666e307` (independent 900-digit expectation) in only
   four of six permutations — violating the peer-permutation
   contract, and falsifying rev 6's "cannot overflow" claim (:1051).
   Resolution: the fallback mean is now the **midpoint-anchored
   accumulation** — anchor `a = min/2 + max/2` (exact power-of-two
   halves; `|a|` bounded by max float64), deviations `d_i = x_i − a`
   each bounded by the half batch range, and a compensated (Neumaier)
   accumulation of the per-observation `(x_i − a)/k` terms: every
   partial sum is a convex share of the half range and the mean (a
   convex combination of the observations) is always representable,
   so no intermediate can overflow **in any peer permutation** (the
   anchor is permutation-invariant). §10 pins all six permutations
   (verified: all six finite at `3.666666666666666e+307`-±1 ULP — the
   two same-sign-first orders that returned −Inf now agree), with the
   honest out-of-range variance (+Inf by sign at emission) and corr
   NULL unchanged. The primary Neumaier path is untouched.
2. **Preserve deviation range before applying merge weights.**
   Verified: the rev-6 safe-merge moments formed the **raw** difference
   before weighting (§5 `:1096`, `dbxs := m̄x − μx'`) — with event keys
   `[0, 8e−16]`, h=1e308, adjust=false, `x=y=[−1e308, 1e308]` and the
   rounded arrival mass 5e−324, the incoming deviation `2e308`
   overflows *before* its weight enters, so the archived simulator
   returned var/cov **Inf** and corr NULL where independent weighted
   arithmetic gives **≈ 1.9762625833649863e293** and corr 1 —
   falsifying rev 6's weight-inside-product safety claim for the
   difference itself. Resolution: the safe merge's deviation terms are
   now **weighted-difference identities**: form
   `q = mass·S − mass·T` (each product is a true weighted value, finite
   whenever the contribution is) and the term as `q·D` with the raw
   difference `D = S − T` when D is representable, or `q²/mass` when D
   overflows; cross terms pair the two weighted differences the same
   way (`q_a·D_b`, `q_b·D_a`, `q_a·q_b/mass`). No raw difference is
   ever evaluated where it can overflow. §10 pins the vector (verified:
   var/cov 1.9762625833649863e293, corr 1; the convex merged mean
   −1e308 is unchanged).
3. **Avoid materializing an underflowing relative share.** Verified:
   the rev-6 extreme-ratio path materialized the shares
   `s1 = W/(W+A)`, `s2 = A/(W+A)` (§5 `:1089`) and claimed they were
   "exactly representable down to the smallest subnormal" — with keys
   `[0, 1074, 1074]`, h=1, adjust=true, `x=y=[1e4, 0, 0]` the retained
   `W = 2^−1074` meets a tied arrival `A = 2`, and `W/(W+A)` rounds to
   **zero** (half of the smallest subnormal, ties-to-even), silently
   discarding the old state: the archived simulator returned
   mean/var/cov **0** and corr NULL where independent arithmetic gives
   **≈ 2.4703e−320 / ≈ 2.47032823e−316 / 1**. Resolution: the
   extreme-ratio path computes with **absolute masses** only — means
   as `(W·μ + A·m̄)/(W+A)` (each product is the side's true absolute
   mass contribution, representable whenever the contribution is) and
   moment terms through the same weighted-difference identities with
   the absolute mass in place of the share; no share is materialized
   anywhere in the merge. The share layers keep their unchanged
   float64 shapes whenever they produce finite results *without
   dropping a positive-mass side* (a new gate), and drop to the
   absolute-mass layer otherwise, so every preserved value is
   unchanged. The extreme-ratio plan extends **beyond representable
   shares to shares below the smallest subnormal on both
   asymmetries**: the retained side is the engine-reachable vector
   above; the incoming side (share `A/(W+A)` rounding below the
   smallest subnormal because the retained total mass dominates — e.g.
   a merge-level `W = 2` against `A = 5e−324`, unreachable through
   this engine's bounded-mass row/event semantics but in-scope for the
   merged operator's own input contract) is pinned as a merge-layer
   unit vector (§10; verified mean ≈ 2.4703282292062327e−174,
   var/cov ≈ 2.4703282292062325e−24, corr 1 — mirroring the preserved
   W=1 arrival at half scale).
4. **Retain moment information when an intermediate variance exceeds
   float64.** Verified: the float64 normalized state (§5 `:923`) and
   the batch-overflow sign pin (`:1056`) discarded out-of-range
   magnitude *before* later weighting — keys `[1, 2, 2]`, α=.01,
   adjust=false, `x=y=[0, −2e154, 2e154]`: the incoming batch variance
   (true `4e308`) saturates at Inf, and the archived simulator
   returned Inf variance and **NaN covariance** where the merged
   variance **≈ 7.920792079207922e306** is representable; and a first
   tied batch `[−2e154, 2e154]` (true state variance `4e308`) followed
   by a zero ten half-lives later must recover **3.90625e305** but the
   stored Inf remained Inf forever (the sign pin also never applied to
   the signed cross-moment, the NaN source). Resolution: batch and
   state centered moments out of float64 range are **retained in
   extended (mantissa, exponent) form** — exact power-of-two scalings
   (frexp/ldexp), float64 mantissa arithmetic, integer exponents; the
   merges weigh extended operands with the same absolute-mass/
   weighted-difference identities (products add exponents); results
   back in float64 range resume the plain float64 state (bit-compatible
   downstream), and **the output overflow policy (honest sign-pinned
   ±Inf at emission, corr NULL on non-finite variance) applies only
   when a result row is emitted**. §10 pins both vectors (verified:
   merged var/cov 7.920792079207921e306, corr 1; rows 1–2 emit honest
   +Inf with corr NULL while the state keeps 4e308, and the row ten
   half-lives later recovers var/cov 3.90625e305, corr 1).
5. **Poll cancellation during complete-group processing.** Verified in
   source: the cumulative aggregate loops **do** poll cancellation
   inside their row/fill loops through `checkCanceled(proc, iteration)`
   with `cancellationCheckInterval = 1024` (`pkg/sql/colexec/window/
   window.go:57`, polled at `:989` per output row and `:1031` per fill
   row) — rev 6's claim that the operator polls only at the Call/status
   boundary (`§9 :1687`) was wrong, and since a single peer group can
   contain the entire partition, whole-group scans/statistical passes
   could run unbounded between Call iterations. Resolution: §9's
   lifecycle now requires **bounded in-loop cancellation polling**
   through the very same helper and interval (every ≤1024 examined
   rows/scalar steps inside group scans, the statistical passes and
   any recomputation/emit loops of the EWM dispatch), while keeping
   the per-group atomic publication contract intact (the cursor never
   advances past an incomplete group; a cancelled group's in-flight
   values are discarded, the error propagates, and the generation's
   eval-start reset makes reuse clean). §10 adds the deterministic
   planned test: cancel after group processing starts → termination
   within the bounded interval *before* the group finishes (error
   returned; no partial rows emitted) → clean reuse after Reset
   (identical outputs on re-execution).
6. **Correct the partition-reset wiring references** (P3). Verified in
   source: the rev-6 lifecycle cited `window.go:968/:1053/:1072` as
   in-generation partition transitions — they are respectively the
   "cumulative window output is not sequential" error guard, the
   final-output cleanup (`outputEnd == n`), and the "sliding window
   output is not sequential" error guard; the real in-loop cumulative
   partition transition (the `runningAgg` group-state free) happens at
   `:992` under the `j == currentPartitionEnd` check at `:988`
   (with `ctr.runningPartition++` at `:997`). Resolution: §9 now
   specifies the partition-boundary check **directly inside
   `processEwmFuncRange`** (the EWM dispatch owns its row loop, like
   the cumulative path at `:474-1051`): at each partition start the
   dispatch resets the EWM **account statistics and clocks** (state,
   per-account decay clocks, C gates) exactly where the cumulative
   path does; the **generation's absolute output cursor**
   (`ewmCompletedRow`, `ewmPartitionSeen`, the in-flight group cache)
   is *generation-scoped and not reset at partition transitions* —
   it is reset only by the eval-start generation transition
   (`:371`), `Window.Reset` (`types.go:195`) and `Window.Free`
   (`:217`) — and the dispatch mirrors the sequential-order guard
   (`:968`-style) and the final-output cleanup (`:1053`-style) with
   its own `freeEwmState` beside them. All cited owner paths are
   corrected in §9 (finding 6's own demand).

Every preserved oracle row was re-run on the revised machine
(`state/artifacts/issue-29945/ewm_rev7_oracle_simulator.py`; log
`ewm_rev7_oracle_run.log` — **116/116 PASS, exit 0**, the unified
suite of all 109 rev-6 rows plus the 7 new rows above). All 109
preserved rows keep their pinned assertions: 108 of them bit-identical
(the primary and share layers are untouched wherever they commit
finite results without dropping a side), and the one preserved row
that previously committed a non-finite share-layer result
(`[1.6e308, 1.6e308, 1.7e308]`, α=.25 adjT — whose *variance* is
genuinely out of range and honestly Inf at emission) now flows through
the absolute-mass layer: its mean moves by **1 ULP**
(`1.6432432432432433e308` → `1.643243243243243e308`, relative
6e−17, far inside its own pinned 1e−12 tolerance) and its emitted
+Inf variance and NULL correlation are unchanged — the layered cost is
recorded in §12 rather than silently accepted.


## Revision record (rev 5 → rev 6; historical)

The fifth independent design review rejected rev 5 with five blocking
findings. Each was verified against the repository code and/or
reproduced numerically on the archived rev-5 simulator
(`state/artifacts/issue-29945/ewm_rev5_oracle_simulator.py`) before
being resolved here (reproduction log:
`state/artifacts/issue-29945/repro_review5_rev5.log`):

1. **The scaled merge still overflows for representable results.**
   Verified in the rev-5 §5 merge shape (it computes the mean numerator
   as an unbounded sum `w1·μx + w2·m̄x` and squares deviations before
   weighting them) and reproduced on the archived rev-5 simulator:
   singleton values `[1.6e308, 1.6e308, 1.7e308]`, α=.25, adjust=true
   return mean `Inf` where ≈ `1.6432432432432432e308` is representable
   (the third merge's numerator `1.05e308 + 0.85e308` overflows although
   the weights sum to 1); `x=y=[0, 2e154]`, α=.01, adjust=false return
   Inf variance/covariance and NaN correlation where `3.96e306 / 1` is
   correct (the pre-weight square `(1.98e154)²` overflows although the
   weighted contribution `0.005·3.92e308 = 1.96e306` is representable).
   The rev-5 claim "weights ≤ 1 protect the merge" is hereby superseded:
   weights bound individual *products*, never additions of same-sign
   products nor pre-weight squares. Resolution: §5's merge is now a
   **three-path layered computation** whose primary path is the rev-5
   scaled mass form *unchanged* (bit-identity for every preserved oracle
   value is retained — the new paths trigger only when they are needed);
   when finite inputs produce a non-finite result — the exact signature
   of an intermediate addition or pre-weight square overflow — the
   merge is recomputed with **convex shares** `s1 = w1/wtot`,
   `s2 = w2/wtot` (`s1+s2 = 1`; a convex combination of representable
   means cannot overflow while its result is representable) and
   **weight-inside-product moment terms** `d·(d·s)` / `dx·(dy·s1)`
   (each term equals its true weighted share, so no term can overflow
   while the result is representable). Verified: `[1.6e308, 1.6e308,
   1.7e308]` → `1.6432432432432432e308` (first two rows stay EXACT
   `1.6e308` through the equality fast path); `[0, 2e154]` → var/cov
   `3.96e306`, corr 1. §10 pins both vectors; Invariant 16/17 updated.
2. **Mass normalization discards statistically significant
   contributions.** Verified and reproduced: the scaled weight
   `w2 = ldexp(A, −e)` rounds to zero *by itself* while the
   **weight-times-value product stays representable** — with event keys
   `[0, 8e-16]`, h=1e308, adjust=false, x=y=`[0, 1e150]`, the arrival
   mass is the smallest subnormal `A = 5e-324` (its half is a
   ties-to-zero case of the subnormal lattice), the rev-5 merge scales
   it to `w2 = 0` and returns mean/var/cov 0 and corr NULL, while the
   contribution `A/(W+A) = 5e-324` is itself representable and requires
   mean `4.940656458412465e-174`, var/cov `4.940656458412465e-24`,
   corr 1. The rev-5 rule "a weight that still rounds to zero is a
   genuinely negligible relative share" is hereby **withdrawn**
   (Alternative R): negligibility is a property of the weighted
   *product*, never of the weight alone. Resolution: §5 adds the
   **extreme-ratio path**: whenever the power-of-two scaling would
   round one weight to zero while its mass is positive — an asymmetric
   ratio beyond the scaled domain — the merge uses **direct shares**
   `s1 = W/(W+A)`, `s2 = A/(W+A)` and the same weight-inside-product
   term shape; direct shares are exactly representable down to the
   smallest subnormal, so a representable contribution is never
   discarded. Verified *both asymmetries*: the incoming-side vector
   above (mean `4.940656458412465e-174` / var-cov `4.94e-24` / corr 1)
   and the retained side (a `W = 2^−1074` state meeting a fresh mass —
   keys `[0, 1074]` h=1 adjF x=y=`[1e4, 0]` — keeps mean `s1·1e4` ≈
   `4.9407e-320` and var/cov `s1·1e8` ≈ `4.9407e-316`, corr 1; rev 5
   silently dropped the old state's mean to 0). §10 pins both; the
   honest float64 limit (a weighted product below the smallest
   subnormal still rounds to 0) is documented.
3. **Compensated batch accumulation is not range-safe.** Verified and
   reproduced on the archived rev-5 simulator: a first tied batch
   `[1e308, 1.1e308]` returns a **NaN mean** (the Neumaier sum
   overflows to Inf and its compensation computes `Inf − Inf = NaN`)
   where `1.05e308` is representable; a tied batch
   x=y=`[−1e154, 1e154]` returns NaN variance/covariance/correlation
   (the square sum `1e308 + 1e308` overflows) where var/cov `1e308`
   and corr 1 are correct. Resolution: §5's batch statistics keep the
   direct compensated sums as the primary shape (bit-identity for
   every preserved vector) and add **detected fallbacks**: a
   finite-input non-finite mean is recomputed as the shifted running
   mean `m += (x_i − m)/i` (a convex update of representable values —
   no summation happens at all); a non-finite square/cross sum is
   recomputed as the per-term regrouped accumulation
   `Σ d_i·(d_i/k)` / `Σ (dx_i/k)·dy_i` (each term equals its true
   `1/k` share, so it cannot overflow while the total is
   representable). A square sum whose *individual terms* are beyond
   float64 (e.g. `[1e308, 1.1e308]`'s deviations `±5e306`, squares
   `2.5e613`) is pinned to honest **+Inf** — a non-negative sum has an
   unambiguous sign — so its variance is Inf and correlation NULL
   (below), never NaN. Verified: `[1e308,1.1e308]` → mean `1.05e308`,
   var Inf, corr NULL; `[−1e154,1e154]` → mean 0, var/cov `1e308`,
   corr 1. The identity fast path (constant batches) and compensation
   are unchanged; §10 adds both non-constant vectors.
4. **The oracle comparator accepts infinity as a finite expected
   result.** Verified by direct probe of the archived rev-5 helper:
   `close(Inf, 1)`, `close(1, Inf)` and `close(Inf, −Inf)` all
   return true (the relative comparison becomes `Inf <= Inf`). Any
   overflowing engine result could therefore masquerade as a PASS
   against a finite expectation. Resolution: §5/§10 define the
   range-safe comparator contract: **NaN matches nothing; infinity
   matches only the same-signed infinity** (an intentionally expected
   +Inf or −Inf); finite comparisons stay purely relative plus exact
   equality. The archived rev-6 simulator implements it and pins
   meta-rows (`close(inf,1)`, `close(1,inf)`, `close(inf,−inf)`,
   `close(nan,·)` all False; `close(±inf, ±inf)` True); every
   previously passing oracle row was **reassessed under the strict
   comparator** (the full 101-row rev-5 suite re-run: no silently
   accepted Inf remained — all preserved rows remain green with the
   same exact values), and the new Inf/NULL rows assert honest
   infinities by sign. The planned implementation tests adopt the
   same comparator contract.
5. **Complete the EWM execution lifecycle and its validation plan.**
   Verified in source: the window operator emits bounded slices and
   returns to receive between them (`processFuncRange(start, end)` is
   invoked per `DefaultBatchSize` chunk, `window.go:414-436`), a new
   materialized input batch starts a new aggregate generation whose
   reset "also closes reuse after an interrupted or failed generation"
   (`case eval` → `freeRunningAgg()`, `window.go:371` with the source
   comment), `Window.Reset`/`Window.Free` reset exactly the existing
   aggregate fields (`types.go:177/:195/:208/:217`, the freeRunningAgg
   at `:283-292`), and in-generation partition transitions free the
   running aggregate at `window.go:968/:1053/:1072`; cancellation is
   polled per Call/status iteration (`vm.CancelCheck`, `window.go:310-312`).
   Rev 5 named none of these for the EWM state — the new dispatch owns
   its own state, cursors and cleanup. Resolution: §9 adds the **EWM
   execution lifecycle** contract (initialization; the emit-slice
   publication contract with a monotonic exactly-once peer-group
   cursor and in-flight group cache — a peer group straddling a slice
   boundary merges exactly once because the operator's materialized
   `ctr.bat` holds the whole generation and the cursor completes the
   group containing the slice's last row; publication is atomic per
   group); **isolation** (generation reset via a new `freeEwmState`
   beside `freeRunningAgg` at all four sites — eval-start generation
   transition, in-generation partition transitions, `Window.Reset`,
   `Window.Free` — so an interrupted (LIMIT), canceled or failed
   generation leaves no partial state, exactly like the existing
   aggregates); **cancellation/errors** (polling at the operator's
   existing Call boundaries; per-group work is O(group size) with no
   frame re-scans; a failed group does not advance the cursor, so no
   partial backfill; clean reuse follows from the eval-start reset).
   §10 adds the four planned deterministic tests the finding demands:
   split-peer exactly-once; partition/generation isolation; LIMIT
   followed by Reset; cancellation/error followed by clean reuse (the
   window UT harness's `t.Cleanup(ctr.freeRunningAgg)` container-drive
   pattern precedents at `window_test.go:2658` / `interval_test.go:197`).
   Executed implementation tests are not part of this checkpoint.

Every rev-1..5 oracle row whose value survived rev 6 was re-verified on
the new machine under the **strict comparator** (109/109 assertions,
exit 0; the profile `1, 1.5, 2.25, 3.125`, the bit-exact gap matrix
(2.6 / 7-3 / 7-3 / 2.0), the clock rows (12/10), the tiny-ratio
`0.6931471805599453`, all tie/accumulation/stability/underflow/
zero-mass/integer-domain/precision rows included — the layered merge
keeps its primary path bit-identical, so preserved values change by
zero ULP), and every changed row is recorded in §5/§12. The rev-6
oracle simulator is archived
(`state/artifacts/issue-29945/ewm_rev6_oracle_simulator.py`; log
`ewm_rev6_oracle_run.log` — 109/109 PASS, exit 0), cross-checked
against hand-derived exact values and direct absolute-position-weight
closed forms.

## Revision record (rev 4 → rev 5; historical)

The fourth independent design review rejected rev 4 with six blocking
findings. Each was verified against the repository code and reproduced
numerically on the archived rev-4 simulator
(`state/artifacts/issue-29945/ewm_rev4_oracle_simulator.py`) before being
resolved here (reproduction log:
`state/artifacts/issue-29945/repro_review4_rev4.log`):

1. **Retain incoming batch covariance during every merge.**
   Verified in the rev-4 §5 merge formulas themselves: the `vxy'` line
   carried the existing state's centered cross moment and the incoming
   batch's mean shift, but dropped the incoming batch's *own* within-batch
   cross-scatter `sxy` (the `vxx'`/`vyy'` lines had their `sxx`/`syy`
   terms; the `vxy'` line did not). Reproduced on the archived rev-4
   simulator: keys `[1,2,2]`, x `[0,10,20]`, y=−x, α=.5, adjust=true gave
   covariance −36 and correlation −0.6428571428571429 where the design's
   own §5 oracle row (56.0 / cov −56.0 / corr −1, the translation row)
   requires −56 / −1: all previous cov/corr assertions hit only
   singleton batches (`sxy = 0`) or first/degenerate branches, so the
   missing term was invisible. Resolution: §5's merge now carries `sxy`
   in the incoming term (verified: row-mode adjust=true → cov −56 /
   corr −1; adjust=false → cov −200/3 / corr −1; event-mode multi-obs
   batches match direct closed forms for mean/var/cov/corr under both
   adjust modes). §10 adds covariance/correlation assertions for
   non-initial multi-observation batches in **both decay modes and both
   adjust modes**, with closed-form cross-checks (the review's exact
   demand), and the new rev-5 vector table pins each.
2. **Exclude policy markers from both prepared caches.**
   Verified in source: rev 4's §9 extended only `shouldCachePrepareCompile`
   (`computation_wrapper.go:2802`); a **second**, independent cache — the
   runtime-specialization cache — admits plans at
   `computation_wrapper.go:1916`
   (`cacheable := bound.DiagnosticFree && !bound.ValueDependent &&
   !prepareStmt.hasPaginationParams && !prepareStmt.hasPercentileParams`),
   retrieves a cached `runtimePlan`/`runtimeCompile` by
   `runtimeSpecializationKey` at `:1859-1868`, and returns the cached
   compile at `:1969` (`retComp = cachedRuntimeCompile`) — all with
   *unchanged parameter types*, so a diagnostic-free EWM SELECT with a
   policy marker would reuse a stale compile even under rev-4's
   `shouldCachePrepareCompile` exclusion. The percentile precedent gates
   this cache independently (`hasPercentileParams` in the admission
   line), and `prepared_percentile_cache_test.go:53`
   (`TestPreparedPercentileDisablesMixedRuntimeSpecializationCache`,
   mixed `percentile_disc(?) … abs(?)`) covers exactly this distinct
   path. Resolution: §9 extends **both** mechanisms: `PreparedPlanHasEwmParams`
   (detected at PREPARE, recorded as `hasEwmParams` beside
   `hasPercentileParams`) adds its disjunct to `shouldCachePrepareCompile`
   (both call sites — `mysql_cmd_executor.go:3196` prepare-time,
   `computation_wrapper.go:1663` execute-time) **and** adds
   `!prepareStmt.hasEwmParams` to the runtime-cache admission at
   `:1916`, so marker-parameterized ewm statements neither reuse a
   prepare-compile nor a runtime-specialization compile. §10's frontend
   sequence now uses a mixed scalar expression exactly as the review
   demands — `ewmMean(v, ABS(?), ?, ?, ?)` — so EXECUTE `0.5` →
   ABS(0.5)=0.5 valid, EXECUTE `-1.0` → ABS(−1.0)=1.0 (a **different
   valid** α without changing parameter types), EXECUTE `3.0` → invalid
   (error confined to that execution), EXECUTE `0.5` → valid again —
   asserting both the results and the exclusion of both caches
   (replicating **both** percentile cache tests: the `:45-50`
   `shouldCachePrepareCompile` shape and the `:53` mixed runtime-cache
   shape). Rev 4's §10 prepared signature `ewmMean(?, ?, ?, ?` …) was
   itself wrong (the review's note: with no value column the EXECUTE
   values could not occupy the positions they were claimed to) —
   superseded by the `ewmMean(v, ABS(?), ?, ?, ?)` form above.
3. **Define a reachable configuration path into the EWM executor.**
   Verified in source: `aggexec.AggFuncExecExpression` has no struct
   `cfg` field — its aggregate configuration travels as **bytes**
   (`extraConfig []byte` + `configType`, `pkg/sql/colexec/aggexec/types.go:37-50`;
   `MakeAggFunctionExpression(id, isDistinct, args, config []byte,
   configType...)`), with an `EncodeApproxPercentileConfig` precedent
   (`operator.go:2128-2154` returns `(args[:1],
   aggexec.EncodeApproxPercentileConfig(config, descending), nil)` after
   `GetReadonlyResultFromNoColumnExpression`). Worse, the proposed EWM
   dispatch at §9 runs **before/outside** `newAggregateExecutor`, so the
   `SetExtraInformation` delivery inside it (`window.go:562-566`) never
   executes for the EWM branch — rev 4's channel could not configure the
   EWM path at all. Resolution: §9's construction path is redefined:
   `constructEwmConfig` validates resolved policies against this
   compilation's `proc`, encodes the configuration **once** to bytes
   (`aggexec.EncodeEwmConfig`, a new encode/decode pair beside the
   percentile's), passes `(args, bytes, nil)` through
   `MakeAggFunctionExpression`'s `extraConfig` exactly like the
   percentile constructor (no plan-proto change; no struct through the
   channel); the **EWM dispatch decodes its own expression's bytes
   directly** — `GetExtraConfig()` (`types.go:114`) at container setup —
   never through `newAggregateExecutor`/`SetExtraInformation`. A decode
   failure or invalid encoded value is a compile-time construction error
   (§7); the bytes are immutable shared inputs of the copied operator
   exactly as for the percentile (no per-copy re-Encoding, no state in
   the bytes — the execution's *state* remains the operator's own
   `ewmState`). §10 adds the demanded producer-to-consumer test:
   construct → encode → decode → **drive execution** and observe the
   configured decay/policies in the row results (plus a
   valid→different-valid→invalid `constructEwmConfig` UT after the
   `operator_test.go:483` pattern).
4. **Normalize merge weights before multiplying small moments.**
   Reproduced on the archived rev-4 simulator: event keys
   `[−1075, −1074, 0, 5e-324]`, h=1, adjust=false, x=`[.5, 1.5, NULL, 1]`,
   y=−x reach the final merge with `W = A = 5e-324` (both the smallest
   subnormal); the rev-4 merge multiplied the normalized moments by the
   **absolute** masses, the products `W·vxx`/`A·sxx` round to 0.0, and
   the simulator reported variance 0 and corr NULL where the two *equal*
   mass shares require variance `.125`, covariance `−.125`, correlation
   `−1`. The rev-4 claim "products that round to zero are genuinely
   negligible shares" is hereby **superseded**: comparable masses are
   never negligible relative to *each other*, whatever their absolute
   scale. Also verified in the review and fixed in the test plan: the
   expected-value comparison must be scale-sensitive (a `max(1,…)`
   absolute tolerance would accept the wrong zero). Resolution: §5's
   merge computes the mass weights in a **scale-free normalized
   domain**: both masses are divided by the power of two that brings
   `max(W, A)` into `[0.5, 1)` (`frexp`, exact exponent adjustment),
   and the merge then runs the ordinary mass form with `w1, w2 ≤ 1`.
   Two comparable subnormal masses become `0.5/0.5` normal shares
   (verified: the vector above now yields exactly var `.125`, cov
   `−.125`, corr `−1`); a weight that still rounds to zero is a
   *relative* share below float64 resolution of the larger mass — the
   genuinely negligible case. Because the normalization is by an exact
   power of two, **every merge whose intermediates stay in the normal
   range is bit-identical to the unnormalized mass form** (power-of-two
   scalings are exact and cancel in the quotients), so every pinned
   oracle value of rev 2–4 is preserved exactly (§5, Invariant 16) —
   verified by rerunning all 101 vectors. §10 asserts nonzero moments
   with **purely relative** comparisons for this continuation vector.
5. **Preserve constant signals across batch merges.**
   Reproduced on the archived rev-4 simulator: two singleton batches with
   identical `x = y = .1`, α=.5, adjust=true merged to mean
   `0.10000000000000002`, variance `1.925929944387236e-34` and corr 1
   (the identity fast path protected only *batch-local* statistics; the
   merge's mass products broke constancy); identical `1.3e308` batches
   overflowed the merge to `Inf` mean / `Inf` variance / `NaN`
   correlation in Go (the Python simulator raised `OverflowError`)
   although the correct mean is representable and the variance is
   exactly 0. Resolution: §5's merge gains a **merge-level equality
   fast path** — when the incoming batch mean is *bitwise equal* to the
   state mean, the merged mean is that exact value and both mean-deltas
   are exactly 0, so constant streams keep variance exactly 0 / corr
   NULL at any magnitude (verified: `.1` under both adjust modes —
   mean exact, var 0, corr NULL; `1.3e308` — mean exact, var 0, corr
   NULL; a tie batch then a singleton; a real 64-row constant first
   batch; a 4-singleton constant chain — including values at the
   double-precision limit) — and the finding-4 normalization bounds
   every merge product by the scaled masses (`w ≤ 1`), so **no merge
   product can overflow while its result is representable**; a merge
   whose *true* result exceeds the float64 range produces ±Inf mean /
   Inf variance per IEEE (documented), never NaN from intermediate
   products. Invariant 4 now states merge-level constancy explicitly.
6. **Avoid overflow when deriving decay from finite DOUBLE keys.**
   Reproduced on the archived rev-4 simulator: accepted finite keys
   `[−1e308, 1e308]`, h=1e308, adjust=false, x=`[0,10]` overflow the
   float64 subtraction `b − a` to `+inf`, so the rev-4 §5 domain-table
   rule ("float64 subtraction") delivered `g = exp(−inf) = 0`, full
   decay, final mean 10 where the finite decay exponent `c = ln2·2`
   yields `g = .25` and mean 7.5 (arrival mass `−expm1(−2·ln2) = .75`).
   Resolution: §5's domain table for FLOAT/DOUBLE keys computes the
   **ratio directly in the quotient domain**: if `b − a` overflows,
   `r = Δt/h` is computed as `b/h − a/h` (each term finite by the
   finite-key rule, no cancellation; if a term is infinite the exponent
   is saturated to full decay as before — a defined state, §5 zero-mass
   rows); `c = ln2·r`; `A = −expm1(−c)` (cancellation-safe as in rev 4).
   Verified: `[-1e308,1e308]` h=1e308 → `0, 7.5` exactly; identical
   *constant* signals across the same extreme gap stay exactly constant
   (mean `7` exact for x=7); §7 additionally defines **non-finite** key
   values (NaN/±Inf) and **non-finite resolved policy values** as bind /
   execution-boundary rejections (finite keys are validated; ±Inf keys
   and NaN/±Inf α/h are never silently saturated). §10 pins the
   extreme-span oracle and the rejection rows.

Every rev-1..4 oracle row whose value survived rev 5 was re-verified on
the new machine (101/101 assertions; the profile `1, 1.5, 2.25, 3.125`,
the gap matrix (2.6 / 7-3 / 7-3 / 2.0 — bit-exact again under the
normalized merge), the tie rows (12/10 and 15/15/22.5), the precision
rows (tiny-ratio `0.6931471805599453` preserved exactly), and all
event-time/underflow/zero-mass/integer-domain rows included), and every
changed row is recorded in §5. The rev-5 oracle simulator is archived
(`state/artifacts/issue-29945/ewm_rev5_oracle_simulator.py`; log
`ewm_rev5_oracle_run.log` — 101/101 PASS, exit 0), cross-checked against
hand-derived exact values and direct absolute-position-weight closed
forms.

## Revision record (rev 3 → rev 4; historical)
The third independent design review rejected rev 3 with seven blocking
findings. Each was verified against the repository code and reproduced
numerically on the archived rev-3 simulator
(`state/artifacts/issue-29945/ewm_rev3_oracle_simulator.py`) before being
resolved here (reproduction log:
`state/artifacts/issue-29945/repro_review3_rev3.log`):

1. **Resolve policies across cached physical execution generations.**
   Rev 3's operating assumption "constructWindow builds operator arguments
   at every execution" is false for cached prepared statements: a reused
   prepared pipeline runs directly after `Reset` and only retries compile
   again (verified in source, `pkg/sql/compile/compile.go:367-368`); the
   percentile precedent therefore *separately excludes* such statements from
   prepare-compile caching — `shouldCachePrepareCompile`
   (`pkg/frontend/computation_wrapper.go:2802`) returns false when
   `PreparedPlanHasPercentileParams` (`pkg/sql/plan/utils.go:4958-4989`)
   finds a percentile supplied by a prepared marker, "the value is compiled
   into aggregate configuration … reusing that compile would reuse an
   earlier EXECUTE value". Rev 3 cited the precedent without adopting its
   cache exclusion, so a cached ewm statement would freeze policies at one
   execution's values. Resolution: §9 pins the complete ownership chain: a
   new `PreparedPlanHasEwmParams` (beside the percentile helper) detects an
   ewm decay/policy position supplied by a prepared marker;
   `shouldCachePrepareCompile` gains the ewm branch (both call sites —
   PREPARE-time caching `pkg/frontend/mysql_cmd_executor.go:3196` and
   EXECUTE-time reuse `pkg/frontend/computation_wrapper.go:1663` — thereby
   disable the reused pipeline for such statements, and every EXECUTE
   re-attaches a fresh `Compile` that runs `constructEwmConfig` against
   that execution's `proc`); literal-only policies (no markers) keep the
   cache (their frozen configuration is constant anyway). §10 replaces the
   rev-3 helper-level test with (a) a frontend prepared sequence through
   the real cache path with unchanged parameter types — EXECUTE valid
   (α=0.5) → different valid (α=1.0) → invalid (α=3, error confined to
   that execution) → valid again (α=0.5) — asserting per-execution results
   and recovery, (b) unit rows for the cache decision
   (`shouldCachePrepareCompile` false with markers, true with literals —
   `prepared_percentile_cache_test.go:45-50` is the exact precedent shape),
   and (c) the constructEwmConfig unit test.
2. **Make the row-decay clock consistent across peer batches.**
   Reproduced on the rev-3 engine: with no NULL rows at all, keys
   `[1,2,2]`, x `[0,10,20]`, α=.5, adjust=true gave 12 under ignoreNA=false
   but 13.333333333333334 under ignoreNA=true (counting observation *rows*
   of the tied batch); and the absolute path counted a two-row NULL peer
   as one step (`[1,NULL,NULL,3]` keys `[1,2,2,3]` → 2.6 instead of the
   declared absolute-row-weight result 25/9). Resolution: §5 defines two
   precise, per-account clocks: **ignoreNA=false** ages by
   `λ^(first row of this observation batch − last row of the previous
   observation batch)` — every intervening row counts, so multi-row NULL
   peers age fully (25/9 verified) and observed ties age one step;
   **ignoreNA=true** ages exactly one observation-**batch** step between
   consecutive observation batches of the account (12 for the tied
   example — equal to ignoreNA=false; 10 under adjust=false). The two
   policies are *identical whenever every row is observed* (asserted for
   all five statistics), and each account (x-observations; full pairs)
   runs its own clock with the same rule. New oracles: `[1,NULL,NULL,3]`
   keys `[1,2,2,3]` → 25/9 (adjT inaF), 7/3 (adjT inaT), 2.6 (adjF inaF),
   2.0 (adjF inaT); agreement rows `[0,10,20]` keys `[1,2,2]` → 12 (adjT)
   and 10 (adjF) under both policies; mixed full-pair stream rows (§5).
   Superseded value recorded: the rev-3 inaT tie result 13.333333… was an
   artifact of counting the tied batch's rows instead of its one batch.
3. **Specify numerically reproducible batch accumulation.**
   Reproduced on the rev-3 engine: naive float64 batch sums are
   order-dependent (`[1e16, 1, −1e16]` adjust=true → mean 0 in one order,
   1/3 after swapping the last two rows), and a constant huge batch
   (`9007199254740991` ×3) produced mean `9007199254740990`, variance 1
   and correlation 1 instead of exact constancy. Resolution: §5 pins the
   accumulation method and its cost: because per-batch weights are
   *uniform*, batch statistics are **unweighted** — `m̄x = Σx_i/k`,
   `sxx = Σ(x_i−m̄x)²/k` (and y-stream analogues) — computed with
   **Neumaier compensated sums** (~2× the adds of naive summation, two
   float64 registers per accumulator, O(k) per batch, O(1) state; the
   cross-batch chain is sequential in the *deterministic* batch order).
   A **bit-identical-observation fast path** makes constant batches exact
   at any magnitude (deviations are set to 0 without any division):
   `9007199254740991`×3 → the exact mean, variance exactly 0, corr NULL;
   `0.1`×3 and `3.25e300`×64 likewise. Invariant 7 is restated honestly:
   outputs are **exact** under intra-batch permutation when the batch's
   observations are bit-identical, and otherwise agree within the
   compensated-sum bound (≤ a few ULP of the accumulated magnitudes);
   studies/UTs assert **unrounded** permutation equality at 1e-12 relative
   tolerance (the archived rev-3 permutation checks that rounded outputs
   are superseded), plus the determination-heavy vectors above
   (`[1e16,1,−1e16]`: exactly 1/3 in all 6 orders — verified).
4. **Preserve normalized moments before total weight underflows.**
   Reproduced on the rev-3 engine: event times `[0,1,501]`, h=1,
   adjust=true, streams `[1e-100, 2e-100, NULL]` left `W ≈
   4.582404545249564e-151` (nonzero — no zero-mass branch) while the
   *absolute* moment `Mxx ≈ 1.02e-351` underflowed: variance collapsed
   from `2.222222222222222e-201` to 0 and correlation from 1 to NULL.
   Resolution: **the state stores normalized, decay-invariant statistics**
   — `(W; μx, μy; vxx, vyy, vxy)` with `v = M/W` maintained directly.
   A transition scales **only** `W`; the normalized quantities and means
   are ratios and do not change under decay, so they survive any
   partial-underflow depth exactly (the subnormal-W vector now carries
   variance `2.222222222222222e-201` and corr 1 through the 500-half-life
   gap), the state *itself* is the carried estimate (rev-3's separate
   snapshot is removed — superseded simplification), and the next merge
   degenerates to batch-local statistics exactly when `W = 0`. Merges use
   the ratio form of the parallel update: terms whose products round to
   zero correspond to genuinely negligible shares (`W·vxx ≪ n·anything`),
   documented in §5. Continuation vectors pin the post-gap restart
   (single-observation batch → var 0 / corr NULL with the carried mean
   replaced by the fresh observation).
5. **Represent the full supported integer time difference.**
   Reproduced: the ascending pairs `[MinInt64, MaxInt64]` and
   `[0, MaxUint64]` have gap `18446744073709551615`, which overflows
   int64; rev 3's "exact int64 subtraction" wraps in Go to −1, and the
   rev-3 formulas then produce `g = 2`, arrival mass −1 and mean −10 for
   x=`[0,10]` (fault recorded; the Python verifier could not expose the
   boundary because Python integers are arbitrary-precision — the rev-4
   verifier now *demonstrates the wrap by emulation* next to the fixed
   result). Resolution: **integer gaps are computed in the unsigned
   magnitude domain**: signed int64 keys `b−a` (ascending ⇒ in `[0, 2^64−1]`)
   via `uint64(b) − uint64(a)`; unsigned keys directly; the (at most
   64-bit) magnitude then converts to float64 once. Verified:
   `[MinInt64, MaxInt64]` h=1 → `0, 10`; `[0, MaxUint64]` h=1 → `0, 10`
   (the mass `−expm1(−ln2·1.8446744073709552e19)` is exactly 1; the state
   re-anchors through the degenerate branch). DECIMAL keys: exact decimal
   subtraction; the subtraction's overflow (both keys near opposite
   extremes of the widest decimal domain) surfaces as the §7
   execution-boundary invalid-input error rather than wrapping.
   TIMESTAMP/DATETIME/DATE: int64 microseconds (range differences fit
   int64). Extreme-key vectors entered into §5/§10.
6. **Handle zero arrival mass before dividing batch totals.**
   Reproduced on the rev-3 engine: event keys `[0, 1e-20]`, h=1e308 make
   `ln2·Δt/h` round to 0.0, so the arrival mass `A = −expm1(−0.0)` is
   exactly 0.0 — and the rev-3 merge computes `Na/A`, raising
   ZeroDivisionError instead of counting the observation and carrying the
   estimate. Also reproduced: a representable subnormal total
   (`A = 4.94e-324`, the smallest subnormal) disappears when split into
   per-observation `A/k` (`A/2 = 2.47e-324` rounds to 0.0). Resolution:
   order of operations in §5: **(i)** if `A == 0.0` the batch is processed
   as a pure zero-mass arrival *before any division*: `C += k`, state and
   clocks advance, outputs at the group's rows emit the carried state;
   **(ii)** batch statistics are unweighted (finding 2 above), so `A/k`
   per-observation masses never exist and a representable total can never
   be lost to k-division — `A` enters only as one merge weight (a
   subnormal `A` then contributes its genuine, below-resolution share,
   documented). Verified: `[7, 9]` h=1e308 → carried `7, 7` with `C = 2`
   and no error; a pair stream carries cov −1 / corr −1 through a
   zero-mass arrival; a subnormal-A two-observation instant carries
   without error with C counted.
7. **Complete the public signature and decay-default contract.**
   Verified: rev 3 permitted `ewmMean(x [, policy…])` while naming no
   default α and no missing-decay rule, and §7/§9 referenced "the §4
   arity/signature table" that did not exist — leaving `ewmMean(v) OVER
   (ORDER BY id)` undefined. Resolution: §4 now carries the **normative
   signature table** — decay is **required**: numeric α (row mode) **or**
   `'halflife'` + h (event-time mode); a call without a decay argument is
   a bind-time error (`invalid argument: ewm statistics require a
   declared decay (alpha or 'halflife' half-life)`); positions, arity
   bounds (mean/var/std: 2..5 args with α, 3..6 with the tag — cov/corr:
   +1 for y), types and defaults (adjust=true, ignoreNA=false,
   minPeriods=0 effective ≥1) are tabulated; §7's wrong-arity row and §9's
   registry checkFns now reference this table; planned negative tests
   cover every missing/misplaced argument, the missing-decay call, and
   the decay-type rules.

Every rev-3 oracle row whose value survived rev 4 was re-verified on the
new machine (89/89 assertions; the profile `1, 1.5, 2.25, 3.125` and all
preserved event-time/tie/precision rows included), and every changed row
is recorded. The rev-4 oracle simulator is archived
(`state/artifacts/issue-29945/ewm_rev4_oracle_simulator.py`; log
`ewm_rev4_oracle_run.log` — 89/89 PASS, exit 0), cross-checked against
hand-derived exact values and direct absolute-position-weight closed
forms.

## Change scope and design-gate trigger

**Change scope:** one feature — a native family of five exponentially weighted
window statistics for MatrixOne SQL: `ewmMean`, `ewmVar`, `ewmStd`, `ewmCov`,
`ewmCorr`, with declared decay (row-position α *or* event-time half-life),
`adjust`, `ignoreNA` and initialization policies.

**Gate trigger (G-FEATURE-DESIGN):** complexity triggers apply and the default
size trigger is met:

1. *Public SQL API / compatibility contract:* five new user-visible function
   names, a normative signature/arity table, new binding rules (OVER-clause,
   ORDER BY shape and direction, decay axis, constant policies), a new
   function class behavior, new EXPLAIN output, and — per the parser
   findings below — five new lexer keywords and grammar productions.
2. *Crosses subsystem boundaries:* MySQL-dialect lexer/grammar
   (`pkg/sql/parsers/dialect/mysql`) → binder (`pkg/sql/plan`) → function
   registry (`pkg/sql/plan/function`) → plan encoding and prepared-plan
   cache decisions (`pkg/sql/plan/utils.go`, `pkg/frontend`) → per-execution
   operator construction (`pkg/sql/compile`) and window-operator execution
   (`pkg/sql/colexec/window`) → EXPLAIN.
3. *Size:* estimated 650–900 non-generated production lines (keyword/grammar
   entries, registry entries, binder validation, cache-exclusion helper,
   per-execution config construction, window operator processing path with
   two decay modes, explain support), plus focused UT/BVT.
4. New *long-lived execution state* (per-partition running state inside the
   window operator) with order-sensitive update rules — a semantic contract,
   not an ordinary fix.

### Parser findings (reproduced on this base, `f8c8720016`)

MatrixOne's grammar attaches OVER only to function names that are lexer
keywords with dedicated productions:

- `SELECT rank();`, `SELECT row_number();` fail as **syntax errors (1064)**
  — `function_call_window` requires `window_spec` grammatically.
- `SELECT abs(1) OVER (ORDER BY 1);` and `SELECT ewmmean(1) OVER (ORDER BY
  1);` fail as **syntax errors near `OVER`** — the generic
  `function_call_generic: ID '(' expression_list_opt ')'` production cannot
  parse any OVER clause.
- Keyword matching is case-insensitive (`SELECT RoW_nUmBeR();` errors
  exactly like `row_number()`), so registering `ewmmean` covers
  `ewmMean`/`EWMMEAN`.
- Keyword tokens stay usable as identifiers when listed in the grammar's
  `not_keyword` set (verified live: `approx_percentile`/`percentile_cont` are
  accepted as table and column names; today `CREATE TABLE ewmmean(ewmmean
  INT)` also works because the names are plain identifiers — **the design
  must preserve this**, see §7/§10).

This is a feature (`kind/feature`), and it is not exempt from the design gate.

## 1. Problem, evidence, and root cause

### Observed problem

On `main` at `f8c8720016`, the SQL probes from the issue fail at function
resolution:

```sql
SELECT ewmMean(CAST(1 AS DOUBLE),3) result;
```

reports the MatrixOne error

```text
(20105, not supported: function or operator 'ewmmean')
```

and the same rejection happens for `ewmVar`, `ewmStd`, `ewmCov`, `ewmCorr`
(3/3 per the issue's probe matrix; reproduced on this base by the handler
on a clean-build `mo-service` from revision `f8c8720016`, `vcs.modified=false`
— see `state/artifacts/issue-29945/repro-baseline-f8c8720016.log` and
`server-startup-addresses.log`).

### Root cause (owner path)

`mo-service` parses any `identifier(args)` call as a generic function call
(`tree.FuncExpr`) — no grammar change is needed to *see* these names. The
failure is purely at the function registry:

```
baseBinder / window binder → function.GetFunctionByName(ctx, name, args)
  → getFunctionIdByName                       (pkg/sql/plan/function/function.go)
  → name missing from functionIdRegister      (function_id.go)
  → moerr.NewNotSupportedf("function or operator '%s'", name)  → ErrNotSupported (20105)
```

There is no function ID, no registration in `list_window.go`, no window
operator processing path, no binder validation, and no tests for any
exponentially weighted statistic. (Searched: no `ewm*` registration or
implementation exists anywhere in `pkg/sql`.)

### The user problem behind the missing names

Monitoring/anomaly baselines need *gradual forgetting*: recent samples matter
more than old ones, and a hard `ROWS BETWEEN n PRECEDING AND CURRENT ROW`
cutoff is an inadequate substitute. The issue's WHY paragraph demands decay
by **event time** (samples arrive at irregular instants) plus missing-sample
handling. The issue's verified engineering profile — which this design
adopts as the primary acceptance oracle — is:

> values `1, 2, 3, 4`, decay α=0.5, adjust=false, no missing samples,
> first-sample initialization ⇒ means `1, 1.5, 2.25, 3.125`.

A finite historical self-join can emulate that one profile (the issue's control
query does), but it is O(n²) per output row, expresses no native incremental
state, and cannot express the other four statistics, the NULL policies, or
event-time decay.

## 2. Goals and non-goals

### Goals

1. Resolve the five names as *known* window functions (entry-name
   availability), removing the 20105 unknown-function outcome of the issue's
   probe.
2. Provide the five statistics as native window functions with O(r) state
   per row (linear total cost, no per-row re-scan of the partition).
3. Declare — not imply — the complete policy set requested by the issue:
   decay (row-position α **or** event-time half-life), `adjust`, `ignoreNA`,
   and initialization (`minPeriods`); the full signature is normative (§4).
4. Define all NULL, partition-reset, tie, and type semantics precisely enough
   for independent reimplementation and deterministic tests, and satisfy the
   accumulated review invariants: constant-signal preservation, translation-
   invariant moments, scale-safe correlation (rev-1); order-free batch
   initialization, within-instant scatter, single ascending event time,
   native-domain decay, carried estimates (rev-2/3); consistent clocks,
   reproducible accumulation, normalized-state underflow resilience,
   full-domain integer gaps, zero-mass arrivals, completed signature
   (rev-4); incoming cross-scatter retained in every merge, scale-free
   merge weights, merge-level constant preservation, finite-DOUBLE-key
   decay without overflow, and policy markers excluded from both
   prepared caches (rev-5); representable results computed without
   spurious overflow/NaN (merges, batch statistics), extreme-ratio mass
   contributions preserved, a range-safe oracle comparator,
   out-of-range moment magnitude retained through the merge (rev-7),
   and the EWM execution lifecycle (exactly-once publication, generation/
   partition isolation, in-loop bounded cancellation and reuse) fully
   specified (rev-6, wiring corrected and cancellation bound added in
   rev-7).
5. Handle prepared-statement policy parameters the way the repository really
   handles parameterized window aggregates: per-execution resolution that
   survives the physical-plan cache (the percentile exclusion pattern),
   verified through the real cache path.
6. Keep ordinary aggregates (AVG, VAR_POP, STDDEV_SAMP, covariance-free
   windows) untouched.

### Non-goals (explicitly out of scope for v1)

1. **Decay declared by `com= / span= / halflife_rows=` named spellings.** SQL
   named-function arguments do not exist in MatrixOne's MySQL dialect, and
   20 registry aliases are rejected as a disproportionate registry surface.
   Row-position com/span/halflife(row) are document-able constant conversions
   over the α primitive (§4).
2. **Ordinary (non-window) aggregation semantics.** `ewm*(...)` without
   `OVER` has no set-level meaning; the calls are window functions only.
3. **Unbiased/bias-corrected variance** (pandas `bias=False` West-style
   correction): v1 ships population (mass-normalized) variance. The
   correction remains a documented follow-up option.
4. **Explicit window frames other than the cumulative default:** rejected
   with a clear bind-time error (§7) rather than silently ignored.
5. **Multi-key or descending event-time axes:** rejected at bind (§7) —
   see revision-record findings 4/5 of the rev-2/3 rounds. Row-decay mode
   keeps full-tuple peers and any direction.

## 3. Invariants

1. **Resolution invariant:** after the change, the exact probe
   `SELECT ewmMean(CAST(1 AS DOUBLE),3)` no longer fails with
   `not supported: function or operator 'ewmmean'` (20105). It fails — as any
   window function without `OVER` does — with the *framework* error
   `Window function 'ewmMean' requires an OVER clause`, proving entry-name
   availability. The same holds for the other four names.
2. **Prefix/direction invariant:** a row's output depends only on the
   partition prefix at and before that row's peer group (per §6); no present
   or future row can change a past output.
3. **Profile invariant (the issue's oracle):** for rows `1,2,3,4` with
   `ewm_mean(v, 0.5, false)` the outputs are exactly `1, 1.5, 2.25, 3.125`
   (float64). Gap-free, tie-free, row-decay sequences reproduce the plain
   recurrence `Y = λ·Y + α·x` exactly (W ≡ 1 there).
4. **Constant-signal preservation:** a partition whose *observed* (non-null)
   values are all equal to `c` emits `c` (or NULL only per the minPeriods/
   no-observation rules) at every row — in every adjust × ignoreNA ×
   decay-axis combination, including NULL-input gaps, tied ORDER BY keys,
   irregular event-time gaps, α=1, arrival-mass underflow, and decay-mass
   underflow. Constant batches have variance exactly 0 and correlation
   NULL at any magnitude (identity fast path), **and constant signals stay
   exact across batch merges at any magnitude** — including merges of
   singleton batches and values just below the float64 limit (`1.3e308`) —
   via the merge-level equality fast path (§5; rev-5 finding 5).
5. **Translation invariance of centered statistics:** `ewm_var`, `ewm_std`,
   `ewm_cov` are unchanged (float64 round-off aside) when the numerical
   inputs are translated by a constant in both streams, for every batch
   shape (first ties, later ties, singletons): `[100000001,100000002]` and
   its +1e9 translation give exactly `2/9`; the tied first batch
   `[10⁹+1, 10⁹+2, 10⁹+4]` gives `14/9`; `[10⁹, {10⁹+10, 10⁹+20}]` equals
   `[0, {10, 20}]` (var 56.0, cov −56.0, corr −1).
6. **Scale invariance of correlation:** `ewm_corr` is unchanged under
   non-zero scaling of either stream, and never forms the variance product:
   identical streams `[s,2s]` yield correlation `1` for `s` up to `1e100`
   and down to `1e-100`.
7. **Order-free peer processing:** every state update is expressed over
   order-free batch aggregates; permuting rows *inside* any peer group
   leaves all five output vectors unchanged — **exactly** when the batch's
   observations are bit-identical (identity fast path) and otherwise within
   the compensated-accumulation bound (a few ULP of the accumulated
   magnitudes); permutation tests compare raw (unrounded) outputs with
   tight tolerance `1e-12 relative` and additionally require absolute
   equality for constant batches.
8. **Reset invariant:** statistics state resets at every PARTITION BY
   boundary; partitions never share state, and a reset state is
   indistinguishable from a fresh partition.
9. **Linearity invariant:** time is O(r) and state is O(1) per partition for
   every function and decay mode (no history buffering, no ewm spilling).
10. **Regression invariant:** all existing window/aggregate functions,
    including cumulative `AVG(...)/ROW_NUMBER()/RANK()`, keep their existing
    results (no change to shared dispatch paths beyond an inserted branch
    for ewm names).
11. **NULL-policy invariant:** outputs are NULL if and only if the pinned
    rules of §5/§6 say so; NULL propagation is governed by minPeriods and
    the mode-dependent ignoreNA rule — a decayed-to-zero mass or a
    zero-weight arrival never yields NULL by itself.
12. **Prepared-execution stability:** one prepared statement executes with
    per-execution policy values through the *real* cache paths: valid value →
    correct results for that value; different valid value next EXECUTE →
    correct results for the new value (no earlier execution's frozen
    configuration — the statement is excluded from prepare-compile reuse
    **and** from runtime-specialization compile reuse when any ewm
    decay/policy position is a prepared marker, §9); invalid
    value → that execution errors (§7), the bound plan stays clean, and the
    *next* valid execution works. Statements whose ewm policies are all
    literals are cached normally and execute unchanged.
13. **Decay-depth invariance of normalized statistics:** means and
    normalized centered moments survive any partial-decay depth exactly
    (they are ratios; transitions scale only the mass `W`); the subnormal-W
    vector carries variance `2.222222222222222e-201` and corr 1 through a
    500-half-life gap, and the state carries into any full underflow until
    a new batch degenerates to fresh ratios.
14. **Event-time precision invariant:** event-time decay derives `Δt` in
    the ORDER BY key's native comparison domain — unsigned-magnitude for
    integer keys (full `2^64−1` span; Go's int64 wrap is expressly
    forbidden — the fault vector `dt = −1 → mass −1 → mean −10` is
    recorded), exact decimal subtraction (overflow → §7 error), int64
    microseconds for temporal keys — with FLOAT/DOUBLE keys the decay
    exponent is derived in the **quotient domain** (`r = Δt/h` as `b/h −
    a/h` when the subtraction overflows; §5, rev-5 finding 6; fault
    vector: `[−1e308, 1e308]` h=1e308 → raw subtraction `+inf` → full
    decay → mean 10, where mean 7.5 is correct) — and computes arrival
    mass via `−expm1(−ln2·r)`, so adjacent timestamps beyond float64
    integer precision still decay and arbitrarily small `Δt/h` never
    rounds the arrival mass to zero. Zero and subnormal arrival masses
    are defined states (§5), never errors; non-finite key values and
    non-finite resolved policy values are rejections (§7).
15. **Clock-agreement invariant:** for any partition in which every row is
    an observation of the account under evaluation, `ignoreNA=false` and
    `ignoreNA=true` produce identical output vectors (asserted for all five
    statistics); the flags differ only over rows whose inputs are NULL.
16. **Merge correctness and scale-safety invariant (rev-5 findings
    1/4/5, rev-6 layering, rev-7 finding 3):**
    every non-degenerate merge retains **both** centered-moment sources —
    the existing state's moments **and the incoming batch's own scatter,
    including the cross-scatter `sxy`** (rev-4 dropped `sxy` and returned
    cov −36 / corr −0.6428571428571429 where −56 / −1 is correct); merge
    weights are computed in a scale-free mass domain — the
    **power-of-two normalized** primary form
    (`max(W, A) → [0.5, 1)`, run only when the ldexp round-trip is
    bit-exact — rev-8 finding 2: on the subnormal lattice ldexp
    ties-to-even and would silently reweight an odd subnormal mass —
    with comparable subnormal masses keeping their exact relative
    shares: var `.125`, cov `−.125`, corr `−1` at `W = A = 5e-324`)
    with two defined share layers per §5 (extreme-ratio/inexact
    direct shares over RAW masses; overflow-safe convex shares) that
    are now **gated**:
    either one commits only when it produces finite results **without
    dropping a positive-mass side and without a subnormal share that
    fails to reconstruct its mass bit-exactly** (rev-8 finding 2; a
    materialized share that rounds to
    zero — e.g. `W = 2^-1074` against `A = 2` — was itself the rev-7
    finding-3 fault), and otherwise the merge recomputes in the
    **absolute-mass form** (§5: means `(W·μ + A·m̄)/(W+A)`, no share
    anywhere) — so every merge is **bit-identical to the plain mass form
    while its intermediates stay in the normal range** (all pinned
    oracle values preserved exactly) and **no representable contribution
    is ever discarded** (a representable weighted product survives to
    the output; only a product below float64's smallest subnormal
    rounds away, §5); constant signals with bitwise-equal means stay
    exact at any magnitude, including `1.3e308` and inside the
    absolute-mass layer itself).
17. **Range-safety invariant (rev-6 findings, rev-7 findings
    1/2/4, rev-8 findings 1/2/3/4):**
    every mean, variance, covariance, standard deviation and
    correlation whose **true** value is representable in float64 is
    **computed** representable — no intermediate addition,
    *difference*, *raw batch deviation*, deviation square or weighted
    product may overflow/NaN en route (an overflowing raw batch
    deviation routes to anchored deviations before any multiplication
    — rev-8 finding 1 — and the standard deviation takes its sqrt on
    the retained moment so an over-range variance never drags a
    representable std, rev-8 finding 4), and **no transmission
    of the computation may destroy out-of-range magnitude that later
    weighting could bring back into range** (§5's layered batch
    statistics and merge): batch/state moments beyond float64 are
    carried in extended (mantissa, exponent) form through the merge
    (rev-7 finding 4: the stored `4e308` still yields `3.90625e305`
    after ten half-lives of decay; the incoming `4e308` batch yields
    the representable merged `7.92e306` at arrival mass `0.02`), raw
    differences are never evaluated where they can overflow
    (weighted-difference identities, rev-7 finding 2), the batch mean
    fallback never overflows in any peer permutation (midpoint
    anchoring, rev-7 finding 1), and shares are never materialized
    where they can round a positive side away (absolute masses,
    rev-7 finding 3). The extended sums are **compensated**
    (rev-8 finding 3): cancelling signed terms below the running
    sum's half-ULP window are carried in a residual register, and a
    term is lost only when its alignment shift exceeds ~1074 binary
    orders — the subnormal lattice of the running partial sum
    (superseding the rev-7 "~1020 orders" wording; the compensated
    two-sum residual is exact, giving a one-rounding-of-the-result
    plus `k·u²·max|term|` error bound at the running-sum scale).
    Results whose true
    magnitude still exceeds float64 **at emission** are honest IEEE
    values: ±Inf by sign — a non-negative variance never emits
    Inf−Inf NaN; emission is the *only* place the conversion happens
    (a non-finite emitted variance means the value left the merge out
    of range after weighting, never during it) — and correlation NULL
    when either normalized variance is non-finite. The oracle/test
    comparator is itself range-safe: NaN matches nothing; infinity
    matches only the same-signed infinity.
18. **Execution-isolation invariant (rev-6 finding 5, rev-7 findings
    5/6):** a peer group merges **exactly once** even when its rows span
    emit slices; state resets at every PARTITION boundary (the EWM
    dispatch's own partition-boundary check inside its row loop, at
    the same place the cumulative path checks `j == currentPartitionEnd`)
    and every materialized-generation boundary (eval start), so an
    interrupted (LIMIT), canceled or failed generation leaves no
    partial state and a reused container behaves as new; the
    generation's absolute output cursor and in-flight group cache are
    generation-scoped (reset only by the generation/Reset/Free sites,
    never by partition transitions, §9). Cancellation is honored
    **inside** complete-group processing: group scans, statistical
    passes and recomputation/emit loops poll through the shared
    bounded-interval helper (≤1024 rows/steps between checks, the
    cumulative paths' own `checkCanceled` interval at
    `window.go:57`) while each group's publication stays atomic —
    the exactly-once cursor never advances past an incomplete group.
    Verification follows §9's lifecycle contract with its five
    planned deterministic tests.

19. **Never-commit-subnormal and lattice-retention invariants
  (eighth-review findings 1/4; rev-10, ninth-review finding 3):** a
  merge whose share would be subnormal on either side never commits
  in the float64 layers — the absolute-mass layer decides it with
  `mass·value` products that keep the mass exponent exact, so no
  rounded reconstruction ever decides a committed share; and any
  batch deviation, stored moment, or merge deviation whose weighted
  square reaches the subnormal lattice rebinds that statistic to the
  extended channel — the rebind decisions read the WEIGHTED
  PRODUCTS (rev 10): a nonzero float64 moment whose product with its
  side's merge WEIGHT would land on the subnormal lattice rebinds
  BEFORE the assembly multiplies, so every exactly representable
  variance/standard-deviation/correlation still emits even when the
  decayed mass destroys the float64 product (keys `[0,0,1000]` h=1
  adjF: var honestly 0.0 with std ≈ 2^-550 representable; snapshot:
  keys `[0,1072,1074]` h=1 adjF → `6.587541944549954e-124` /
  `6.587541944549954e+76` / corr 1; singletons `[−2^-537, +2^-537]`
  → `2^-1074 / 2^-537 / corr 1`; tied `[−1e-200, 1e-200]` → var
  0.0 honest / std 1e-200 / corr NULL).
20. **Un-narrowed-centering invariant (eighth-review finding 3,
  rewritten in rev 10 for the ninth-review finding 1):** the emitted
  mean stays the narrowed float64 `fl(ν)`, but every centered
  statistic is the moment of the UN-NARROWED mean. The batch keeps
  its compensated tail and applies the exact single-anchor identity
  `Σ(x−μ)²/k = Σ(x−m̄x)²/k − τx²` (cross τx·τy); every merge forms the
  merged tails FIRST and measures its deviations BETWEEN COMPLETE
  MEANS (state hi+lo, arrival m̄+τ, merged `fl(ν)` plus the carried
  tail ν−fl(ν)) — no δ² end correction exists (the rev-9 chain had
  no cross term and committed 4.25 where 2.75 is exact) — and no
  compensated residual ever survives a register fold silently (the
  cascade holds every discarded bit, symmetric in both alignment
  directions, capacity 128; order-dependent covariances at ANY
  scale are an internal error, not accepted noise). Together with
  Invariant 5: translating the inputs by any constant leaves
  var/std/cov/corr unchanged for every batch shape and arrival
  order — `x = [2^53, 2^53+2, 2^53+4]` and its `[0,2,4]` translation
  both give var/cov exactly 2.75, tied or singleton or mixed.
21. **Complete-mean merge algebra (ninth-review finding 1; rebuilt
  in rev 11 for the tenth-review finding 1):** every merge deviation
  is measured between complete means on all three anchors, the merged
  tails are formed before the deviations, and the equality fast paths
  require hi AND tail equality on both sides; through the
  absolute-mass layer the complete means are carried as DOUBLE-DOUBLE
  pairs — two extended words, ~106 significand bits — through the
  numerator (`ddsum`, the exact-alignment fold chain whose fold
  losses form the second word), the division (`dddiv`, remainder
  kept), the source subtraction (`ddsub`) and the weighted products
  (`dd_mul`, the correction products folded; the extended products
  never overflow). A single extended word — for the numerator, for
  the merged mean, or for any source `(hi + lo)` crushed by one
  rounded addition — is an internal error: rev 10 folded the (C)
  numerator by one xadd and lost the 54-bit center ([2^600,
  2^600+2^548] measured the arrival deviation as 2^548 and emitted
  std 2^547.5 where 2^547 is exact, while the translated [0, 2^548]
  control was right; the mixed-scale [2^53,+2,+4] × 2^-260
  continuation emitted cov 1.8891618714269098e-78 / corr
  0.9949366763261822 where 2.75·2^-260 and 1 are exact).
  Continuation after both tied-batch and singleton initialization is
  pinned: `[2^53, 2^53+2, 2^53+4]` → 2.75 exactly, the tied-first-pair
  path likewise, `[2^53, 2^53+2, 2^53]` → 0.75 exactly, and the
  equal-hi/different-tails continuation → 29/9.
  (Rebuilt in rev 12, eleventh-review finding 1:) every weighted
  product inside those sums is formed ERROR-FREE — `xtwoprod`
  produces the one-rounded leading word bit-identical to `xmul` PLUS
  its exact residual pair, `ddsum_e` folds the leading words through
  the exact-alignment chain while the residuals and fold losses
  rebuild the second word, and `dddiv` subtracts the error-free
  `q_hi·T` so the remainder is exact. Rev 11 rounded each mass
  product once before any compensation could act and computed the
  remainder against the same rounded product: even `dddiv(1, 3)`
  returned a zero tail word, and the singleton centers
  `[2^600, 2^600+2^548]` emitted std `5.150657059589176e+164`
  (alpha=3/8, adjust=false) and `5.319575737017211e+164` (alpha=.5,
  adjust=true) where `√15/8·2^548 = 4.460599859785886e+164` and
  `√2/3·2^548 = 4.343415401260641e+164` are exact — the translated
  `[0, 2^548]` controls already held. The carried-moment assembly
  products (W·vxx, A·sxx) stay one-rounded; the shared bounded
  reconstruction (Invariant 24) owns their accumulation losses.
  Power-of-two masses give exact products, so every preserved row
  keeps its pre-rev-12 value bit-for-bit (r12F1).
22. **Retained-moment correlation (ninth-review finding 4):** the
  geometric correlation normalization is computed from the RETAINED
  moments (extended square roots, product and quotient); the float64
  view drives only the NULL/finiteness routing, and plain float64
  states are bit-identical to the narrowed form. Subnormal-quantized
  numerator/denominator pairs can never emit |corr| > 1 on an
  exactly correlated stream — the pinned subnormal paths return
  exactly ±1.0.
23. **Accumulator-domain contract (ninth-review finding 2; exact
  alignments in rev 11 for the tenth-review finding 3):** the
  cascaded accumulator handles BOTH alignment directions
  symmetrically — an operand whose scaled alignment underflows
  cascades intact, old accumulator or incoming term alike — and its
  capacity (128 registers) exceeds the deepest cascade the supported
  term grammar can produce (every fold residual descends ≥ 53 binary
  orders per level; relevable term exponents span ≤ 6296 exponent
  orders, at most ~120 levels). Each fold's two-sum is EXACT at every
  representable gap: an ldexp landing on the subnormal mantissa
  lattice no longer quantizes the smaller operand before the
  addition — for gaps in [1022, 2043] the two-sum runs at a scale
  where BOTH mantissa views are normal (the big mantissa up by
  gap−1021 ≤ 1022, finite/normal/exact; the small into [2^-1022,
  2^-1021)), and the EXACT complement rescales back; gaps beyond 2043
  cascade the smaller operand intact (out = big, loss = sml). Rev 10
  recorded only the post-quantization bits (0.75 aligned against
  2^1073 came back as 1.0; the H=2^537 unit batch returned
  0.6666666666666666 small-pairs-first where 0.75 is exact).
  Residual loss below the reconstruction's 2×53-bit window is
  bounded by the documented double-double rounding contract.
24. **Finite-chain cascade and non-finite fold (tenth-review finding
  4):** the plain float64 compensated chains (batch sums, scatters
  and merged-mean tails) carry a compensation register that is itself
  compensated — each register fold's residual cascades into the next
  bounded register (the same ≥53-order descent as the extended
  accumulator) and the chain exposes (sum, comp, comp_lo) — so finite
  cross-scale sums keep every bit in EVERY peer order
  ([H=2^400, M=2^200, 1, −H, −M] → mean 0.2 in the review order and
  the reorder; the finite cross-scale covariance quartet batch → cov
  0.2 as-written and grouped) while benign sums stay bit-identical to
  the (sum, comp) form. A NON-FINITE residual — an overflowing
  partial sum in a branch the range-safety gates reject anyway —
  folds ONCE into the first register and never enters the cascading
  loop (−inf − (−inf) = NaN would never terminate); its propagation
  to the caller is identical to the single-compensation form.
  (Extended in rev 12, eleventh-review finding 2:) the RESTORATION is
  ONE shared bounded procedure at both cascade sites. The neumaier
  registers fold through the same exact-alignment chain as their
  comp word — rev 11 summed the lower registers naively
  (`comp_lo += r`), so the tied batch
  [−2^54, 2^216, −2^162, 0.75, 2^162, 2^54, −2^216] cascaded to
  registers [2^162, −2^162, 0.75], the naive tail rounded 0.75 away
  and the mean emitted 0.0 where 3/28 is exact (the magnitude
  reorder was right). The accumulator restore keeps the head fold's
  exact loss (rev 11 xadd'd `acc + regs[0]` and dropped whatever fell
  outside the 53-bit window) and processes the head loss, every
  register and every subsequent fold loss through a
  DESCENDING-magnitude queue in which mirror pairs cancel exactly
  before small content folds; a fold returning the WHOLE item as its
  loss is held back and retried only after the running total changed
  — a pass that changes nothing can never host a held item, and the
  remainder flushes through xadd, the documented float64 information
  limit (rev 11's leftover pass discarded its own `_l2` losses: the
  extended sum [−2^323, 2^215, 2^431, 2^323, 0.75, −2^431, −2^215]
  restored to 0.0 where 0.75 is exact). Termination is structural,
  not a countdown: each partial-fit loss descends ≥ 53 binary orders
  and an intact-refeed only follows a total-changing event.
  Neither failure came near the 128-register capacity; benign
  walks stay bit-identical (r12F2).
25. **Weighted-deviation retention (tenth-review finding 2):** the
  lattice rebind decisions read EVERY weighted square and cross
  product, moments AND deviations: a merge whose source moments are
  both zero still rebinds to the extended channel when a nonzero
  deviation product with its side's weight would reach the subnormal
  lattice — the (P) pairs (dx,dx,w1), (dbx,dbx,w2), (dy,dy,w1),
  (dby,dby,w2), (dx,dy,w1), (dbx,dby,w2) rebind at frexp-sum −1021
  and the share layers mirror with s1/s2 (keys [0,1000] h=1 adjF
  x=y=[2^-100,0]: std 2.4099198651029665e-181 pinned against the
  engine's own rounded masses, variance honestly 0.0; the
  incoming-side counterpart keys [0,1e-300] x=y=[0,2^-100]: std
  6.567697842037889e-181).

26. **Range-safe two-products (eleventh-review finding 3):** the
  Dekker split runs in the MANTISSA domain — the 2^27+1 scaling
  multiplies only the normalized mantissa, exact at every
  representable magnitude (subnormal edges included), with the
  pieces reassembled by ldexp — and the raw float64 k·m̄x mean-tail
  products form their residual in the mantissa domain, reassembled
  at ea+eb: the residual is bounded by half an ulp of p, in range
  whenever fl(a·b) is finite, so τx keeps its exact complement across
  the declared range. A raw-domain split is IMPOSSIBLE exactly at the
  top band [2^1024·(1−2^-27), maxfloat64] — the high word would be
  2^1024 — the very frontier where the rev-11 native-domain splitter
  overflowed and silently returned an UNSPLIT operand:
  `twoprod(3, 2^1000+2^948)` lost the residual −2^948, the first
  tied batch [H=2^1000, H+U, H+U] (U=2^948) stored the mean tail
  −2U/3 instead of −U/3 and drove the retained variance negative —
  `out_std` raised a math-domain ValueError while the translated
  [0, U, U] control was exact at `1.1215783890386092e+285`. NO
  output clamping was added: a negative retained variance remains an
  internal error (the §7 shape), and the batch-tail boundaries are
  pinned at 2^1000+2^948, at the band frontier and at the top-band
  operand 1.7976931348623157e308 itself (r12F3).

27. **Canonical complete-mean normalization (twelfth-review finding
  1):** every merge layer normalizes the complete mean before
  committing and emitting it. A narrowed high word that cancels to
  EXACTLY ZERO while the computed tail carries nonzero content is not
  the complete mean — the tail IS it (the high word contributed
  nothing), so the normalized store carries the tail into the
  narrowed word and its residual into the tail slot; the emitted mean
  equals fl of the COMPLETE mean at every layer (share-form keys
  [0,0,1] x=y=[2^53, 2^53+2, −2^53]: the stored tail held the exact
  0.5 and the emission was 0.0; (C) singletons [2^600,
  −fl(5/3)·2^600] under α=3/8 adjust=false: the error-free numerator
  is exactly −2^545, dddiv's high word is (0, 0) and the exact
  remainder tail is −2^545). The rev-14 correction
  (thirteenth-review finding 1): the scoping "the tail of a
  nonzero high word is below its half-ulp, so the normalization
  is a zero-op" was FALSE — a nonzero high word's tail can sit AT
  its half-ulp boundary and flip the correctly rounded complete
  mean one ulp. The review's keys [0,0,1] x=y=[2^53, 2^53+2,
  −2^53+2] stored hi 1.0 with the exact tail .5 and emitted 1.0
  where EXACTLY 1.5 is required; the (C) singletons
  [2^600, −fl(5/3)·2^600+2^549] under α=3/8 adjust=false emitted
  the dd lead 4·2^545 while the exact remainder tail holds
  +2^545 — EXACTLY 5·2^545 = 5.758609657015291e+164 is required;
  and the suite's OWN in-range merge [1.6e308,1.6e308,1.7e308]
  a=.25 adjT carried a dd high word one ulp below its pinned
  complete mean, tolerated by the row's 1e-12 comparator since
  rev 5. The normalization therefore covers EVERY high word
  through the exact two-sum fold, and the bit-identity promise
  covers exactly the benign pairs (tail below the boundary:
  the fold is an exact identity) — 164/165 preserved rows stayed
  byte-identical while the one boundary row was lifted to its
  own pinned complete-mean literal, which is the finding-1
  contract (Invariant 29; Decision 54).
28. **Retained mean tails through the batch-residual boundary
  (twelfth-review findings 2 and 3):** the complete batch residual
  (Σx − k·m̄x)/k is ALSO formed in the retained pair channel and
  commits there whenever the float64 tail path cannot carry it:
  (a) k·m̄x overflows float64 while Σx and m̄x are finite — a first
  tied batch [M, 0, 0] at M = maxfloat64 keeps its finite mean
  5.992310449541053e307 and std EXACTLY 8.474406707690974e307
  (variance honestly +Inf; both permutations and the [M/2, 0, 0]
  control unchanged), and (b) the exact tail falls BELOW the
  subnormal lattice — a first tied batch [0, 2u, 6u] at u = 2^-1074
  whose exact tail −u/3 quantized to 0.0: the retained pair centers
  the extended scatter on the un-narrowed mean ((56/9)u² instead of
  the lost (19/3)u²; std exactly the correctly rounded 2u).
29. **Complete-mean normalization for every high word
  (thirteenth-review finding 1):** the merge layers — (P), (F)
  and (C) — normalize (hi, tail) by the EXACT two-sum fold
  before every commit and emission: hi′ := fl(hi + tail), tail′
  := the exact residual — float tails fold through twosum,
  retained pair tails through the exact pair two-sum whose fold
  loss stays retained in the tail slot, and a non-finite
  fl(hi+tail) keeps the pair raw (honest propagated
  non-finites are §7 behavior). A pair whose tail is below the
  high word's half-ulp boundary is an exact identity — the
  benign zero-ops that keep preserved rows bit-identical; a
  pair at/across the boundary folds the boundary bit into the
  narrowed word, which is then the correctly rounded complete
  mean (hi 1.0/tail .5 → exactly 1.5; dd lead 4·2^545 + exact
  +2^545 → exactly 5·2^545; the suite's one-ulp-low dd word →
  its own pinned literal). Scope (rev 15, Decision 55): the RAW
  W == 0 batch state commit keeps the pinned rev-9 narrowed
  word fl(fl(Σregisters)/k) as its single-batch EMISSION,
  while its tail is the GENUINE complete residual — rebuilt
  from ALL compensated registers, no premature
  (cx + cxl) / (ca_ + ca_lo) float64 folds inside the
  reconstruction's own lists (rev 14 kept the folded-tail
  exception and its continuation merges emitted 0.0 where
  fl(1/7)/fl(0.1) — initial — and fl(2/11)/fl(1/6) —
  incoming — are exact; reproduced, fourteenth-review
  finding 1) — so the committed (word, tail) IS an exact
  split of the computed complete mean and stores/merges
  consume true pair semantics (Decision 54 rescinded;
  Invariant 31). Rev-20 continuation (ninth-review P2 finding
  2; Invariant 38, Decision 62): the packing step at every
  normalization and merge boundary — (P), (F), (C), the
  absolute-mass merge and the pair-native normalization —
  retains the FULL extended-minus-narrowed difference:
  `pack_remainder` enters the PRIMARY
  xfold(extended lead, −narrowed word) result into the tail
  accumulator TOGETHER with its exact fold loss and the
  caller's existing low word (rev 19 kept only the fold loss,
  so the singleton sequence [0], [2u], [4u] under α=0.5
  adjust=true lost ≈u/3 of its committed complete mean at the
  first merge — exact 4u/3, committed u plus a tiny fold
  residue — and the final landed on 3u against the exact
  20u/7 with std 2u against the exact-rounding u, the
  variance being 104u²/49; adjust=false kept its green u first
  merge but committed the fl(2.5u) = 2u tie; both decay modes).
  Benign exact-narrowing folds append NOTHING (the primary
  result is the exact zero pair), so every preserved row stays
  bit-identical.
  Rev-21 continuation (twentieth-review P2; Invariant 40,
  Decision 64): the retention rule owns the MERGED-state tail
  stores too — after pack_remainder folds the merged tail, each
  committed (word, tail) is again an exact split of the
  computed complete mean: the store keeps the extended pair
  whenever re-normalizing the narrowed value fails to reproduce
  it. The singleton chain [B], [B+2u], [B+4u] at B = 2^-1021,
  u = 2^-1074 under α=0.5 adjust=true keeps its genuine −2u/3
  merged tail (rev 20 narrowed it to the scalar −u and
  committed the completes B+u, B+3u, B+5u with std 2u against
  the exact 4u/3, 20u/7, 68u/15 and the exact 104u²/49 → u) on
  every side of all eight row/time × ignoreNA × sign
  combinations with the committed account asserted directly;
  the translated [0], [2u], [4u] controls stay green (r21F;
  all 174 preserved rows byte-identical, NO reassessed
  literal).
30. **Mixed retained-tail and scalar cross-centering
  (thirteenth-review finding 2):** the batch cross-centering
  identity sxy := sxy − τx·τy covers EVERY retained/scalar
  combination: both tails scalar keep the pinned float64
  product; an already-retained sxy takes the xsub pair route
  unchanged; and whenever EITHER τ is a retained pair while
  sxy stays a finite scalar — first tied batch x=[M,0,0] at
  M = maxfloat64 meeting y=[1+2^-52,1,1] — the complete cross
  moment re-forms through the exact chain: per-row deviations
  formed EXACTLY (twosum against the un-narrowed means — a raw
  float64 difference rounds an M-scale deviation by a half
  ulp, 9.98e291, more than the entire tail correction at the
  division scale), every product error-free (xtwoprod over the
  four sub-product pieces), the correction −k·τx·τy error-free,
  the pile cascaded through ddsum/dddiv and narrowed by ONE
  final fold. Covariance EXACTLY 8.870401375709865e+291 and
  the finite x std EXACTLY 8.474406707690974e+307 follow under
  BOTH adjust policies and on the swapped stream (rev 13
  raised TypeError multiplying the retained tail as a scalar),
  while the all-scalar controls keep their established bytes
  except the ONE documented rev-16 reassessment (the [M/2,0,0]
  sentinel keeps its EXACT complete cov 4.4352006878549327e+291,
  one ulp above the rev-13..15 pin whose one-rounded formation
  lost the ulp — fifteenth review, Invariant 32; the x=y honest
  +Inf-covariance control is unchanged).
31. **Genuine batch mean-tail residuals (fourteenth-review finding
  1):** the batch residual τx := (Σx − k·m̄x)/k is rebuilt from
  ALL compensated registers — every register enters the cascaded
  reconstruction as its OWN list item (no premature (cx + cxl)
  or (ca_ + ca_lo) float64 folds inside the reconstruction's
  lists) — so the committed (word, tail) is a genuine
  complete-mean split in EVERY peer order: the cross-scale
  [H=2^400, M=2^200, 1, −H, −M] batch commits the true residual
  fl(−2^-54/5) ≈ −1.1102230246251565e-17 in the review order AS
  WELL AS the [H,−H,M,−M,1] reorder (rev 14 read it at −0.2 —
  the word's own scale — order-dependently, and its continuation
  merges emitted 0.0 where fl(1/7)/fl(0.1), initial state, and
  fl(2/11)/fl(1/6), incoming state, are exact). The residual is
  preserved through centering (the identity consumes it), the
  state commit and every merge; the raw single-batch emission
  keeps the pinned narrowed word (the r11F4 pins); the
  retained-pair fallbacks (k·m̄x overflow; below-lattice tails)
  and the merge layers are unchanged (Decision 55; r15F).
32. **Error-free batch deviation and product residuals
  (fifteenth-review finding):** every finite batch-moment term
  carries its subtraction and multiplication residuals into the
  compensated accumulation, for scalar AND retained inputs,
  zero tails included: each per-row deviation forms EXACTLY
  (twosum against the narrowed mean — a raw one-rounded
  x − m̄x loses a half ulp per row: the H=2^52 tied batch
  [H,1,−H]×[1,−2,1] measured its narrowed deviations
  [H−0.5, 2/3, −H] and emitted −0.6111111111111112 in all six
  peer orders, both decay modes and both adjust policies where
  the complete covariance fl(−2/3) is exact), and every
  product contributes its EXACT error-free pieces (xtwoprod
  over the four sub-product parts — fl(K²−1) = K² discarded
  the −1 before any compensation could see it: the zero-mean
  [K+1,K,−K−1,−K]×[K−1,1−K,K,−K] batch at K=2^27 emitted
  exactly 0.0 across all 24 permutations, both decay modes and
  both adjust policies where the complete covariance is exactly
  −1/4); the mean-tail corrections −k·τx² / −k·τx·τy join
  the numerator as error-free pieces gated by VALUE (zero-tail
  batches carry their own zero), and the quotient narrows
  through a single final fold. The SCATTER piles keep the
  double-double pipeline (a deviation-square pile is
  positive-dominant: every discarded piece is ≈106+ binary
  orders below the lead sum, under its half-ulp); the CROSS
  pile CASCADES through the full register accumulator (xsum4)
  because full cancellation is admitted — the complete term can
  sit hundreds of binary orders below every product's own
  rounding residual, beyond any two-word window; the poisoned
  continuations heal to their exact values (initial
  [0,0,0,1] → fl(−2/5) adjT / fl(−1/3) adjF; incoming
  [0,1,1,1] → fl(−4/7) adjT / fl(−1/2) adjF), every expectation
  independently derived from the exact decay-mixture definition
  (Decision 56; r16F; the r14F2 [M/2,0,0] sentinel literal
  reassessed to its exact value
  4.4352006878549327e+291; rev 17 extends the same contract to
  the lattice and anchored routes — Invariant 33 / Decision 57 /
  r17F; rev 18 pins the cancellation-preserving SIGNED-CROSS
  quotient on every route — Invariant 34 / Decision 58 / r18F).

33. **Error-free batch moments on the lattice and anchored routes
  (sixteenth-review finding):** Invariant 32's contract reaches
  EVERY finite batch on every route: the lattice and anchored
  formations run the normal route's helpers — twosum deviations
  (against the narrowed mean on lattice sides, against the exact
  range midpoint on overflowing sides — range safety and the
  routing/retention policies untouched), error-free xtwoprod
  products, and the shared anchor/mean-tail correction −k·δx·δy
  whose delta parts are the exact twosum(m̄x, −a) words plus the
  tail (a mean-anchored side contributes δ = τx, keeping the
  mixed −k·δx·τy term) — the scatter quotients keep their
  exact-remainder dd pipeline (ddsum → dddiv → xadd) while the
  SIGNED cross quotients run the cancellation-preserving CASCADE
  (rev 18: the full register cascade for the pile, the first
  quotient word, and the EXACT −q1·d residual re-cascaded WITH
  the pile so the second word carries the quotient remainder —
  Invariant 34; the rev-17 two-word pile compression is
  superseded). Both review witnesses are
  pinned (r17F): the one-sided-scaled lattice batch emits EXACTLY
  fl(−2^-602) and the anchored [B,B,B,−B]×[2^52,1,−2^52,0] batch
  EXACTLY fl(B/8) = `2.125e307` in every peer order, both decay
  modes, both adjust policies and both stream orders (rev 16:
  exactly `0.0` — one-rounded products — and `4.25e307` —
  one-rounded deviations); the routing-boundary pair spans the
  frexp fence with exact content on both sides; and the
  continuations heal — exact everywhere the merge's moment
  assembly carries the content and within ONE documented ulp of
  the exact decay-mixture ideal where the 55-bit numerator of the
  B-scale mixture exceeds the merge's single-pair restore
  (Decision 57; r17F).


34. **Cancellation-preserving signed-cross accumulation on every
  route (seventeenth-review finding):** a routed SIGNED cross
  pile is never compressed before its quotient: the FULL xsum4
  register cascade feeds the first quotient word
  (`q1 = xdiv(xsum4(items), d)`), and the EXACT −q1·d residual
  re-cascades WITH the pile so the second word carries the true
  quotient remainder (`xq_cascade` — the dddiv structure
  generalized to a multi-item numerator). The rev-17 cross
  pipeline folded the whole uncancelled pile into two ddsum
  words before dddiv, so tied ±H-scale pair products whose
  exact cancellation leaves content hundreds of binary orders
  below the running lead emitted cov EXACTLY `0.0` in the
  order-dependent executions: the tied anchored batch
  [B,B,B,B,B,−B]×[−H,M,1,H,−M,−1] (B=1.7e308, H=2^400, M=2^200;
  covariance fl(B/3) = `5.666666666666667e+307` exact) in both
  decay modes, both adjust policies and both stream
  orientations, and the ten-pair lattice batch
  [(H,H),(−H,−H),(M,M),(−M,−M),(1,1),(−1,−1),(H,−H),(−H,H),
  (M,−M),(−M,M)] with x scaled by 2^−512 (covariance
  fl(2^−512/5) = `1.4916681462400414e-155` and the nonzero
  correlation `7.498484069478155e-242` exact) in the stated
  order — while the REVERSED peer orders of both witnesses, the
  swapped-reversed anchored orders and the normal-route 2^−511
  scaling kept their exact content, proving the
  order-dependence. The scatter quotients keep the dd pipeline
  (positive-dominant piles: every discarded piece sits ≈106+
  binary orders below the lead). Pinned (r18F): both witnesses
  with the reversed/swapped and rotated peer-order controls, the
  routing-boundary pair (lattice side 2^−512 exact; normal side
  2^−511 → exactly `2.983336292480083e-155` in both orders,
  both policies and both modes), the witness-B correlation
  pinned nonzero and bit-equal across peer orders, all eight
  initial/incoming continuations healed at both adjust
  policies — witness B EXACTLY at the closed-form mixture
  ideals and witness A within the merge's documented ONE-ulp
  bound at the B-scale ideals (observed exact in the pinned
  order) — and every r17F/r16F pinned family reproducing
  BYTE-IDENTICALLY through the changed sites (Decision 58;
  r18F).

## 4. Public interface

### Normative signature table

All five names share one positional policy grammar. **The decay argument is
required** — a call without one has no defined result and is rejected at bind:

`ewmMean(x, alpha [, adjust [, ignoreNA [, minPeriods]]])` (row-decay form)
`ewmMean(x, 'halflife', h [, adjust [, ignoreNA [, minPeriods]]])`
(event-time form). `ewmVar`/`ewmStd` identical to `ewmMean`; `ewmCov`/`ewmCorr`
add `y` after `x` and shift the policy suffix accordingly.

| Positions | mean/var/std | cov/corr | Type at bind | Default | Violation |
|---|---|---|---|---|---|
| 1: x | × | × | numeric (tinyint..bigint, unsigned variants, float/double, decimal up to decimal256; or exact numeric constant expr) — cast to float64 at bind-established arg cast; every SAMPLE VALUE must be FINITE after the cast (an ±Inf/NaN sample is the §7 execution rejection; rev-20, Invariant 39) | required | registry overload-check failure (bind); non-finite sample VALUE → §7 execution rejection |
| 2: decay | ×(pos 2) | ×(pos 3) | numeric constant folding to α ∈ (0,1] **or** string constant `'halflife'` followed by h > 0 at the next position | **none — required**; missing decay = bind error `invalid argument: ewm statistics require a declared decay (alpha or 'halflife' half-life)` | §7 |
| (decay+1): h | with tag only | with tag only | numeric constant h > 0 (event-time mode) | — with α | §7 |
| after decay: adjust | optional | optional | constant boolean | `true` | §7 |
| then: ignoreNA | optional | optional | constant boolean | `false` | §11 (prepared markers); §7 (resolved values) |
| then: minPeriods | optional | optional | constant integer ≥ 0 | `0` (effective ≥ 1) | §7 |

- **Arity bounds (registry checkFn-enforced):** mean/var/std: `2..5` with a
  numeric decay, `3..6` with the tag; cov/corr: `3..6` / `4..7`. Anything
  outside is the wrong-arity bind failure of §7.
- Policy arguments are constant expressions; prepared markers are accepted
  at every policy/decay position and resolved per execution (§9; §11).
- `x`, `y` non-numeric (string/bool/temporal/uuid/vector…) is an
  overload-check failure at bind.
- `OVER w` — required; must contain at least one `ORDER BY` expression
  (partition-only OVER is an error, §7); explicit frame clauses are errors
  (§7). In event-time mode the OVER clause must contain **exactly one
  ORDER BY expression** whose direction is **ASC** (default or explicit) and
  whose type is numeric or TIMESTAMP/DATETIME/DATE (bind-time rules; NULL
  key values are execution errors, §7). Row-decay mode accepts any number of
  ORDER BY expressions and any direction (full-tuple peers; no time axis).

`DISTINCT` and `WITHIN GROUP` forms are errors. Result type for all five
functions: **DOUBLE (float64)**.

### Decay conventions (documented equivalences)

| Convention | Mode | Relation / declaration |
|---|---|---|
| `alpha` α ∈ (0,1] | row | declared directly: `ewmMean(x, 0.5, false)` |
| `com` c ≥ 0 | row | α = 1/(1+c) — `ewmMean(v, 1/(1.0+1), false)` |
| `span` s ≥ 1 | row | α = 2/(s+1) — `ewmMean(v, 2/(3.0+1), false)` (span=3 reproduces the verified profile) |
| `halflife` (rows) h > 0 | row | α = 1−exp(−ln(2)/h) — `ewmMean(v, 1-exp(-ln(2.0)/4), false)` |
| `halflife` (event-time) h > 0 | event-time | declared by tag: `ewmMean(v, 'halflife', 3600)` (Δt in key units) |

### NULL-policy decisions (explicit)

- `ignoreNA` role: *row decay* — whether NULL-valued rows still age the
  account's state (absolute-row vs observation-batch clocks, §5);
  *event-time decay* — weights are physical time (immortal to the flag);
  the flag selects whether rows whose inputs are NULL emit the carried
  current estimate (`false`) or a NULL bit (`true`), the value being shared
  by all rows of the peer group either way.
- `minPeriods` counts this account's observations (non-null x for
  mean/var/std; both non-null for cov/corr) since the partition start, and
  `max(1, minPeriods)` gates NULL outputs (§5).
- A decayed-to-zero or zero-arrival-mass state never produces NULL by
  itself; only the C-gate (and zero-variance correlation) does.

### Examples

```sql
-- the issue's verified profile
WITH raw AS (SELECT 1 id, CAST(1 AS DOUBLE) v UNION ALL SELECT 2, 2
             UNION ALL SELECT 3, 3 UNION ALL SELECT 4, 4)
SELECT ewmMean(v, 0.5, false) OVER (ORDER BY id) FROM raw;
-- 1, 1.5, 2.25, 3.125

-- row-decay conventions
SELECT ewmMean(v, 2/(3+1.0), false) OVER (ORDER BY id) FROM raw;   -- span=3
SELECT ewmMean(v, 1/(1+1.0), false) OVER (ORDER BY id) FROM raw;   -- com=1
SELECT ewmMean(v, 1-exp(-ln(2)/1.0), false) OVER (ORDER BY id) FROM raw; -- h=1 row

-- event-time half-life (exactly one ASC ORDER BY key)
SELECT ewmMean(v, 'halflife', 3600) OVER (ORDER BY secs ASC) FROM metrics;
SELECT ewmCorr(a, b, 'halflife', 60, true) OVER (ORDER BY ts) FROM metrics;

-- bind-time rejections (§7 and §4 signature table)
SELECT ewmMean(v)            OVER (ORDER BY id) FROM raw;  -- ERROR: missing decay
SELECT ewmMean(v, 'halflife', 10) OVER (ORDER BY t, id) FROM m;  -- ERROR
SELECT ewmMean(v, 'halflife', 10) OVER (ORDER BY t DESC) FROM m;-- ERROR
```

## 5. Mathematical specification

Throughout: one *partition* = the rows covered by one OVER(PARTITION BY …)
group, in the ORDER BY order. `λ = 1 − α`. All state values are float64,
with one defined exception (rev-7 finding 4): a centered moment whose
true value is out of float64 range is carried in the **extended
retained-magnitude form** below, and converted (to honest signed ±Inf
if still out of range) only at emission. An **observation** is a row
whose statistical inputs are all non-null (`x`
for mean/var/std; both `x` and `y` for cov/corr). The function evaluation
keeps two independent **stream accounts**: the x-observation stream
(mean/var/std) and the full-pair stream (cov/corr). Both accounts decay
under the same clock rules at the same peer-group boundaries; each account
counts only its own observations, and each account's *first* arrival batch
is the first batch containing its own observations.

`adjust` and the decay axis change only the *total mass generator*;
evaluation formulas never differ.

### State (per partition, per account; O(1) memory; normalized)

- `W` — total decayed weight mass consumed (the only quantity any
  transition scales);
- `μx`, `μy` — current weighted means (carried live);
- `vxx`, `vyy` — normalized centered second moments (`= Mxx/W`,
  `= Myy/W`); `vxy` — normalized centered cross moment (`= Mxy/W`); all
  three decay-invariant (ratios). **Retained-magnitude form (rev-7
  finding 4):** each of the three is either a plain float64 (whenever
  its value is in float64 range — the only form any pinned vector ever
  produces) or an extended pair `(mantissa m ∈ [0.5,1), integer
  exponent e)` with the value `m·2^e` (exact power-of-two scalings via
  frexp/ldexp; float64 mantissa arithmetic; integer exponents), used
  when the true moment is out of float64 range. Extended moments flow
  through the merge's absolute-mass layer, whose weight products add
  exponents; a merged result back in float64 range resumes the plain
  float64 state (no downstream difference). Conversion of the extended
  form — to the float64 value, or to **honest sign-pinned ±Inf** when
  the value remains out of range — happens **only at emission**
  (§ Outputs): an intermediate out-of-range moment must never destroy
  the magnitude that later weighting can recover (fault vectors:
  `4e308` state, arrival mass `0.02` → merged `7.92e306`;
  `4e308` state, ten half-lives of decay → `3.90625e305`);
- `C` — observations consumed by this account.

**The state itself is the carried estimate.** Rev 3's separate snapshot
field is removed (superseded): because transitions touch only `W`, the
normalized quantities *are* the g→0+ limit; after a partial underflow the
outputs are unchanged; after a full underflow (`W*x… g·W = 0` exactly:
α=1, or `exp(−c) = 0`) with `C ≥ max(1,minPeriods)` the outputs remain the
state's normalized values, and the next batch merge degenerates (below) to
fresh batch-local ratios. An unseen account (`C = 0`) outputs NULL
everywhere (partition start or an all-NULL partition — never confused with
a decayed account).

### Transition decay

- **Row mode** (per account, applied lazily at each of the account's
  observation-batch arrivals; row rows never observe `W`, so laziness is
  unobservable — the emitted means/moments are decay-invariant):
  - `ignoreNA=false` (absolute rows): `g = λ^s` with
    `s = firstRow(this batch) − lastRow(previous observation batch of this
    account)`. Every intervening row counts once — including every row
    of intervening NULL-value peers: `[1,NULL,NULL,3]` with keys
    `[1,2,2,3]` spans s=3 (25/9 verified under adjust=true).
  - `ignoreNA=true` (observation batches): `g = λ^1` between consecutive
    observation batches of this account (one batch *step*, regardless of
    intervening NULL rows; the rev-3 rule that counted a tied batch's
    observation *rows* is superseded — it broke Clock-agreement
    Invariant 15 and gave 13.333333… where 12 is correct).
  - Consequence (Invariant 15): whenever every row is an observation of
    the account, both clocks give `λ^1` per batch — identical outputs
    (verified for all five statistics on `[10,20]/[20,40]` tie rows:
    12 adjust=true, 10 adjust=false, both policies).
- **Event-time mode:** `g = exp(−ln2·Δt/h)` per peer group, `Δt` = gap from
  the previous peer group's key (telescoping across NULL-valued instants),
  computed in the key's **native comparison domain before any float
  conversion**:

| ORDER BY key type | Δt domain |
|---|---|
| `TINYINT..BIGINT`, `UTINYINT..UBIGINT` | **unsigned-magnitude** subtraction: signed `uint64(b) − uint64(a)` (ascending ⇒ `b−a ∈ [0, 2^64−1]` — fits uint64; the Go int64 wrap that rev 3 allowed is forbidden — fault vector: `[MinInt64, MaxInt64]` → wrapped `−1` → mass `−1` → mean `−10`); unsigned direct; one float64 conversion of the ≤64-bit magnitude |
| `FLOAT` / `DOUBLE` | **quotient-domain ratio** (rev-5 finding 6): `r = Δt/h` is formed directly — if `b − a` is finite, `r = (b−a)/h`; if the subtraction overflows (finite opposite-sign extremes), `r = b/h − a/h`, each term finite, no cancellation; `c = ln2·r` (`+inf` ⇒ saturation: `g = 0`, `A = 1` — the §5 defined states); the raw subtraction `[−1e308, 1e308] → +inf` is the recorded fault (it produced mean 10 where 7.5 is correct). Non-finite key values (NaN, ±Inf) are rejections (§7) |
| `DECIMAL(64/128/256)` | exact decimal subtraction (widened domain); if the subtraction overflows the widest decimal domain → the §7 execution-boundary invalid-input error (never wrapping); one float64 conversion of the resulting gap |
| `TIMESTAMP` / `DATETIME` / `DATE` | int64 microseconds subtraction (native domains; range gaps fit int64) |

  `Δt ≥ 0` always: event-time OVERs must be ASC (§7), NULL key values are
  execution errors. NULL-*valued* rows contribute their instants exactly
  once; `ignoreNA` does not modify `g` (physical time). Transitions scale
  `W` only; all rows of the partition see the live normalized state.

**Only `W` is scaled by `g`.** This is the rev-4 core change (rev-3's
absolute `Mxx/Myy/Mxy` scaling is superseded — it underflowed at
subnormal-but-live masses; recorded vector: `t=[0,1,501]`, h=1,
streams `[1e-100, 2e-100, NULL]`, with `W ≈ 4.582404545249564e-151`
nonzero, absolute `Mxx` underflowed and reported var 0 / corr NULL where
`2.222222222222222e-201` / 1 is correct and is now carried exactly).

### Arrival masses (per batch)

Every peer group is one arrival batch of k observation rows carrying a
**total** mass `A` (weights are uniform; per-observation masses are never
materialized — see Batch update):

| Batch | `adjust=true` A | `adjust=false` A |
|---|---|---|
| first batch of the partition (per account) | `k` | `1` |
| later batch, row mode | `k` | `k·α` |
| later batch, event-time mode | `k` | `A = −expm1(−ln2·Δt/h)` (cancellation-safe; never the literal `1−g`) |

- A singleton first observation receives `1` under both adjust modes —
  the issue's first-sample initialization — and the gap-free row-decay
  chain keeps `W ≡ 1` exactly under adjust=false (profile oracle).
- **Zero-mass arrivals are defined states, computed before any division**
  (rev-4 findings 6): if `ln2·Δt/h` rounds to 0.0 (e.g. `Δt = 1e-20`,
  `h = 1e308`) then `A = −expm1(−0.0) = 0.0`; the batch processing order is
  (i) advance clock, (ii) `C += k`, (iii) if `A == 0.0`: end — state carried
  (outputs: the live normalized state), (iv) otherwise compute batch
  statistics and merge. A representable **subnormal** `A` (down to
  `4.94e-324`) enters the merge whole — `A/k` per-observation masses are
  never formed, so a representable total cannot be lost to k-division
  (fault vector recorded: `A = 4.94e-324`, `A/2 = 2.47e-324 → 0.0` under
  the superseded per-observation scheme).

### Batch update (two steps; deterministic accumulation)

Step 1 — **batch-local statistics, unweighted, compensated, range-safe
(rev-6 finding 3, corrected in rev 7 for findings 1/4).** For the batch's
k observation values (x_i, y_i) — uniform weights make weighted
statistics unweighted. The direct shapes below are the primary
computation (bit-identical for every preserving vector); each is paired
with a **detected fallback** that recomputes exactly when finite inputs
produce a non-finite result:

```
sx = Neumaier(x_1..x_k)          sy = Neumaier(y_1..y_k)
m̄x = sx/k                        m̄y = sy/k
sxx = the rev-16 ERROR-FREE deviation-square contract:
      every deviation twosum-exact against m̄x, every square
      in its xtwoprod pieces, the −k·τx² numerator piece
      error-free gated by VALUE, the dd cascade + division
      and ONE final fold (syy identical on m̄y)
sxy = the SAME common term for the cross: deviations exact,
      products error-free over the four sub-product pieces,
      −k·τx·τy error-free, the xsum4 REGISTER cascade
      (full cancellation admitted) and ONE final quotient fold

# detected fallbacks (finite-input non-finite results only):
m̄x, m̄y   :=  midpoint-anchored accumulation (rev-7 finding 1):
              a    = min(x_i)/2 + max(x_i)/2          # exact halves
              m̄x   = a + Neumaier((x_i − a)/k …)
# rev 19 (eighteenth review, P2): the quotients above serve the
# MEAN accumulation alone — the carried TAIL is never folded from
# them (rev 18 made that fold and lost every per-quotient division
# residual; the rev-19 tail contract follows in the τx block below:
# ONE shared genuine reconstruction from the ORIGINAL observations,
# Invariants 35/36, Decision 60).
# rev-8 finding 1 (seventh review): the RAW deviation lists
# d_i := x_i − m̄x are checked for finiteness BEFORE any
# multiplication or accumulation — a finite mean does not promise
# finite raw deviations ([1.7e308,1.7e308,−1.7e308]: m̄x =
# 5.666666666666667e307 while the min-side deviation is −2.2667e308
# = −Inf in EVERY peer permutation; rev 7 multiplied the −Inf into
# the extended layer and its normalization looped forever).
if any d_i is non-finite, per side (the cross routes here when
   either side's list is non-finite) — rev 17 (Invariant 33): the
   anchored route runs the SAME error-free batch-moment contract as
   the normal route, range safety untouched:
    a     = min(x_i)/2 + max(x_i)/2     # exact midpoint, per side
    dax_i = twosum(x_i, −a)             # exact pair; |dax_i| ≤
                                          # half the batch range
    δx    = twosum(m̄x, −a) words + τx   # the EXACT delta parts
    sxx   = error-free scatter: every (dax_i)² in its xtwoprod
            pieces over the pair, the −k·δx·δx correction as
            error-free extended pieces of the exact delta parts
            (xanchor_k_items), the dd cascade + division and ONE
            final fold
    sxy   = the SAME common cross term per row (twosum-exact
            deviations against the per-side anchor), with the
            −k·δx·δy correction folding the exact per-side delta
            parts of BOTH sides — a mean-anchored side contributes
            δ = τx, so the mixed −k·δx·τy term of an overflowing
            side merged with a mean-anchored side carrying a
            tail is kept; a constant side's zero deviations
            contribute nothing by VALUE; the SIGNED cross quotient
            runs the cancellation-preserving CASCADE (`xq_cascade` —
            rev 18, Invariant 34: full register cascade, first
            quotient word, EXACT −q1·d residual re-cascaded with
            the pile; the SCATTER quotient keeps the dd cascade)
            # Σ(x−m̄x)² = Σ(x−a)² − k·δx²: every subtraction is
            # range-preserving BEFORE any multiplication or
            # accumulation
else if any d_i or dy_i is nonzero with frexp-exponent ≤ −511
  (rev-9, eighth-review finding 4 — the deviation's square reaches
  the subnormal lattice or below), rev 17 (Invariant 33): the
  lattice route runs the SAME error-free batch-moment contract as
  the normal route on the extended channel that retains the
  subnormal content, so BOTH the lattice moment AND every residual
  survive:
    sxx, syy, sxy := error-free pieces (twosum deviations against
                     the narrowed means, xtwoprod squares and
                     products over the four sub-product parts,
                     −k·τx² / −k·τx·τy error-free gated by VALUE);
                     the SCATTER quotients keep the dd cascade +
                     division and ONE final fold, the SIGNED cross
                     quotient runs the cancellation-preserving
                     CASCADE (`xq_cascade` — rev 18, Invariant 34:
                     full register cascade, first quotient word,
                     EXACT −q1·d residual re-cascaded WITH the
                     pile, so no uncancelled cross pile is ever
                     compressed to two words before the division);
                     the commit stays extended (std/corr stay
                     computable on the retained moment)
else:
    sxx, syy :=  Σ (d_i·(d_i/k))        # each term = its true 1/k share
    sxy      :=  Σ ((dx_i/k)·dy_i)
    # and when even those per-term shares overflow (individual
    # squares beyond float64), the moment is RETAINED, not saturated
    # (rev-7 finding 4) — accumulated CASCADING (rev-9, superseding
    # the rev-8 single residual register, eighth-review finding 2;
    # rev-10, ninth-review finding 2: SYMMETRIC alignment with the
    # domain-justified capacity — see the accumulator contract at (C)):
    sxx, syy, sxy := extended (mantissa, exponent) accumulation of the
                     same terms with exact power-of-two scalings and
                     float64 mantissa arithmetic — a Neumaier two-sum
                     level-0 loop whose FIRST residual register
                     follows the rev-8 fold sequence bit-identically,
                     while every bit a register fold discards cascades
                     into the next bounded register (rev-10 capacity
                     128) instead of vanishing: the registers hold
                     precisely the discarded bits, so a no-loss
                     vector keeps the rev-8 value byte-for-byte and
                     cross-scale residuals survive in EVERY peer order
                     at ANY span the supported term grammar can
                     produce — every fold residual is bounded by half
                     an ulp of its folded operands, so each cascade
                     level descends at least 53 binary orders below
                     the previous level's scale, and relevable term
                     exponents are bounded to [−3222, 3074] (≤ 6296
                     exponent orders, at most ~120 levels, the
                     capacity exceeds the domain worst case); the
                     normalization REQUIRES finite
                     normalized mantissas (a non-finite operand is a
                     defined internal error, never a loop)

# rev-9, eighth-review finding 3 — the carried mean tail: the batch
# mean stays the narrow float64 fl((Σx)/k) bit-identical (emitted as
# m̄x), but the account also carries the compensated tail
#     τx := (Σx − k·m̄x)/k          (Dekker two-product k·m̄x + exact
#                                  two-sums — exact to one ulp of the
#                                  mass-weighted residual; rev 12,
#                                  eleventh-review finding 3: the
#                                  product's residual forms in the
#                                  MANTISSA domain and reassembles at
#                                  ea+eb — half an ulp of p, in range
#                                  whenever fl(k·m̄x) is finite;
#                                  Invariant 26; rev 13,
#                                  twelfth-review findings 2+3: the
#                                  SAME residual is also formed in
#                                  the retained pair channel and
#                                  commits there whenever the
#                                  float64 tail path cannot carry it —
#                                  k·m̄x overflow (first tied batch
#                                  [M,0,0], M=maxfloat64: finite Σx,
#                                  finite mean, finite std, honest
#                                  +Inf variance) or an exact tail
#                                  below the subnormal lattice (first
#                                  tied batch [0,2u,6u], u=2^-1074:
#                                  exact tail −u/3 — the retained
#                                  centering keeps (56/9)u² and std
#                                  correctly rounds to 2u); the
#                                  retained pair also centers the
#                                  scatter x.y at the state through
#                                  pair-native merges; Invariants 28,
#                                  narrowed values at emission only;
#                                  rev 15, fourteenth-review finding
#                                  1: the reconstruction lists ALL
#                                  compensated registers as separate
#                                  cascade items — no premature
#                                  (cx + cxl) / (ca_ + ca_lo) fold
#                                  inside its own list (rev 14 read
#                                  the review-order [H,M,1,−H,−M]
#                                  tail at −0.2, the word's own
#                                  scale, and its continuation
#                                  merges emitted 0.0 where
#                                  fl(1/7)/fl(0.1) initial and
#                                  fl(2/11)/fl(1/6) incoming are
#                                  exact, while the [H,−H,M,−M,1]
#                                  reorder read the true
#                                  fl(−2^-54/5): order-dependent,
#                                  corrupting, now repaired; the
#                                  committed (word, tail) is a
#                                  genuine complete-mean split whose
#                                  single-batch emission keeps the
#                                  pinned narrowed word; Invariant
#                                  31, Decision 55)
# and every scatter above is the moment of the UN-NARROWED mean:
#     sxx := sxx − τx²              sxy := sxy − τx·τy
# (the identity Σ(x−μ)²/k = Σ(x−m̄x)²/k − τx²; without it the tied
# batch [2^53, 2^53+2] measures its scatter around the narrowed
# 2^53 and returns 2.0 where the exact population variance is 1.0 —
# the 100% translation-variance error of Invariant 5). The anchored
# branch likewise centers on (m̄x + τx): its identity is
#     Σ(x−μ)²/k = Σ(x−a)²/k − (m̄x + τx − a)²
# rev-16, fifteenth-review finding (Invariants 30/32; Decision
# 56): the exact-chain contract covers EVERY batch shape —
# scalar tails, ZERO tails and retained (pair) tails alike, for
# scatter and cross (rev 14 restricted the exact re-formation to
# retained-tail mixes, and rev 13 multiplied a retained tail as
# a scalar and raised TypeError). Per-row deviations form
# twosum-exact against the un-narrowed means (a raw float64
# difference rounds an M-scale deviation a half ulp, more than
# the entire tail correction at the division scale), every
# product error-free over the four sub-product pieces, the
# correction −k·τx·τy error-free and gated by VALUE; the
# scatter piles fold through the dd pipeline and the CROSS pile
# CASCADES through the xsum4 register accumulator (full
# cancellation admitted: the complete term can sit far below
# every product's own rounding residual, beyond a two-word
# window), the quotient divided once and narrowed by ONE final
# fold: the first tied batch x=[M,0,0] at M=maxfloat64 meeting
# y=[1+2^-52,1,1] yields cov EXACTLY 8.870401375709865e+291 and
# x std EXACTLY 8.474406707690974e+307 under BOTH adjust policies
# and on the swapped stream, and the [M/2,0,0] all-scalar control
# keeps its EXACT complete cov 4.4352006878549327e+291 (the
# rev-13..15 one-rounded route pinned one ulp below).
# rev-19 continuation (eighteenth review, P2 — Invariants 35/36,
# Decisions 59/60): the reconstruction is the ONE shared
# complete-residual channel on EVERY route: the batch sum forms in
# the xsum4 register cascade over the RAW xfrexp(x_i) items of the
# ORIGINAL observations, the k·m̄x subtrahend enters as its TWO
# exact xtwoprod words (the subtraction residuals chase through the
# cascade registers) and ONE final xdiv keeps the single division
# rounding. NOTHING is pre-divided and there is no second variant:
# the rev-18 anchored rebuild folded the ALREADY-ROUNDED
# per-observation quotients (x_i − a)/k — which the fallback above
# defines for the MEAN alone — into the tail, losing every
# quotient's division residual: the tied batch [H,H,−H] at
# H = 2^1023 routed to the anchored lane (raw partial sum H+H
# overflows) and committed (m̄x, τx) = (b, EXACTLY 0.0), b =
# fl(H/3) = 2.9961552247705263e+307, against the genuine
# (b, 1.6632002579455998e+291) that BOTH reordered peers [H,−H,H]
# and [−H,H,H] committed through the ordinary route — a
# peer-ORDER violation of the complete-mean account (16 failing
# combinations against 32 correct reordered controls across
# {initial, incoming} × {row, time} × {adjust T/F} × {ignoreNA
# T/F}, reproduced on the frozen rev-18 archive with every
# committed account checked inside the engine; appending −b under
# adjust=false left the merged mean EXACTLY 0.0 where
# fl((H−3b)/6) = 8.316001289727999e+290 is required, and −H/2
# under adjust=true emitted −9.9792015476736e+290 = −(H−3b)/5
# where EXACTLY 0.0 is required; the incoming shapes corrupted at
# fl(H/18) and −(H−3b)/5.5 = −9.072001406976e+290). The commit
# lane narrows ONCE and is IDENTICAL on both routes: the xval lead
# when it carries the content (bit-for-bit the ordinary route's
# committed pair — peer-order invariance of the account), the
# retained (pair) shape whenever narrowing loses ANY content
# — re-normalizing the narrowed lead does not reproduce the
# pair (Invariant 37, rev 20, ninth-review P2 finding 1,
# lane_retains): a NONZERO lead quantized onto the subnormal
# lattice (the tied batch [B, B+2u, B+8u] at B = 2^-1021,
# u = 2^-1074 narrows its genuine fl(4/3)·u tail to u — the
# exact variance is 104u²/9 and its std is 3u, while committing
# the narrowed u emitted std 4u in all 48 executions), a lead
# flushed below the lattice (the rev-13 [0, 2u, 6u] retention —
# xval 0.0 with a nonzero mantissa) — never when narrowing is
# exact: zero, an exact subnormal word like 3u, every normal
# lead and the honest ±Inf out-of-range lead re-normalize to
# themselves and keep the narrowed scalar lane byte-for-byte.
# Rev 21 (twentieth-review P2; Invariant 40, Decision 64): the
# SAME retention rule owns EVERY tail-storage boundary — not only
# this batch commit lane but ALSO both merged-state tail stores
# (x and y) after pack_remainder packs the merged tail: the
# merged store narrows once and keeps the extended pair whenever
# re-normalizing the narrowed value fails to reproduce it (the
# singleton witness [B], [B+2u], [B+4u] at B = 2^-1021,
# u = 2^-1074 under α=0.5 adjust=true narrows its genuine −2u/3
# merged tail to the scalar −u; rev 20 committed the completes
# B+u, B+3u, B+5u with std 2u although the exact are 4u/3, 20u/7,
# 68u/15 and the exact 104u²/49 → u, in all eight row/time ×
# ignoreNA × sign combinations; the translated [0], [2u], [4u]
# controls stay green below the lattice), while every exact
# shape keeps its narrowed scalar lane byte-for-byte.
# The
# ordinary route keeps its float64 fast lane with the shared
# channel as its retained fallback (the rev-13 triggers unchanged;
# the former register-sourced fallback variant is superseded). The
# merged continuations heal to the genuine orders — initial
# fl((H−3b)/6) and EXACTLY 0.0; at the incoming shapes the merged
# mean lands on nextafter(fl(H/18)) = 4.993592041284211e+306 at
# row decay (ONE documented merge ulp above the strict closed form
# fl(H/18) = 4.9935920412842106e+306 — the corrupt account emitted
# exactly the strict value) and on fl(−H/12) =
# −7.490388061926316e+306 at time decay (the corrupt emission
# coincided there while the committed ACCOUNT still differed); the
# r19F suite row pins the whole matrix with all three peer orders
# bit-identical, and all 172 preserved suite rows stayed
# byte-identical (the pre-row full run on the rebuilt core is
# byte-identical to the frozen rev-18 log; NO reassessed literal).
```

- **Identity fast path:** if every `x_i` is bit-identical, `m̄x := x_1` and
  the x-deviations are exactly 0 (so sxx = 0 and the x side of sxy is 0);
  likewise for y. Constant batches are therefore *exact at any magnitude*
  (Invariant 4): `9007199254740991`×3 → mean 9007199254740991 (exactly
  representable), var 0, corr NULL; `0.1`×3 and `3.25e300`×64 likewise.
- **Neumaier compensated sums** make moderate-cancellation batches
  order-reproducible: `[1e16, 1, −1e16]` gives mean exactly `1/3` in all 6
  row orders (rev-3 naive sums gave 0 vs 1/3). Cost: ~2× the adds of
  naive summation; two extra float64 registers per accumulator; O(k) per
  batch against the batch's actual row contents, O(1) persistent state.
  Batch order itself is deterministic (the operator's ORDER BY sort), so
  only intra-batch order needs this guarantee.
- **Cascaded compensated sums (rev-11, tenth-review finding 4):** the
  single compensation register is itself compensated — the residual of
  each register fold cascades into the next bounded register (the same
  ≥53-order descent as the extended accumulator) and the chains return
  (sum, comp, comp_lo). Finite cross-scale sums keep every bit in
  EVERY peer order where the single register lost them INSIDE itself:
  `[H=2^400, M=2^200, 1, −H, −M]` returns mean 0.2 in the review order
  AND its reorder (rev 10: 0.0 — comp accumulated (H−H)+M and the +1
  then rounded away inside comp), the finite cross-scale covariance
  quartet batch returns cov 0.2 as-written AND grouped (rev 10: 0.0
  as-written), and the merged-mean tail numerators inherit the same
  chain. Benign sums (no register-fold loss) are bit-identical to
  (sum, comp). A NON-FINITE residual — an overflowing partial sum in
  a branch the range-safety gates reject anyway (the rev-6 lists) —
  folds ONCE into the first register and never enters the loop
  (−inf − (−inf) = NaN would otherwise never terminate);
  downstream propagation is identical to the single-compensation
  form, and the touched preserved rows stay bit-identical (the
  145-row preservation run shows zero shifts). (r11F4,
  Invariant 24.)
  (Rev 12, eleventh-review finding 2a:) the registers themselves
  restore through the shared bounded reconstruction — comp and comp_lo
  come from the exact-alignment chain over ALL registers, so the
  mirror cancellation the naive `comp_lo += r` pass rounded away
  survives: the tied batch [-2^54, 2^216, -2^162, 0.75, 2^162, 2^54,
  -2^216] (registers [2^162, -2^162, 0.75]) returns mean 3/28 in the
  review order AS WELL AS the magnitude reorder, where rev 11 emitted
  0.0 in the review order alone. Registers ≥ 53 binary orders apart —
  every well-formed cascade — fold losslessly, so benign walks stay
  bit-identical (r12F2; Invariant 24 extended).
- **Why the fallbacks are needed and safe (rev-6 finding 3, rev-7
  findings 1/4):** the compensated sum of `[1e308, 1.1e308]` overflows
  to Inf and Neumaier's compensation computes `Inf − Inf = NaN` — a NaN
  *mean* where the representable answer is `1.05e308`; the square sum
  `1e308 + 1e308` overflows for `[−1e154, 1e154]` where var `1e308` is
  representable. The rev-6 fallback — the shifted running mean
  `m += (x_i − m)/i`, claimed impossible to overflow — is **superseded**
  (rev-7 finding 1): its *subtraction* overflows for opposite-sign
  extremes; the tied batch `[1e308, 1.1e308, −1e308]` returned **−Inf**
  for the two same-sign-first orders (reproduced on the archived rev-6
  simulator; only 4 of 6 permutations gave the correct finite
  `3.6666666666666666e307`). The **midpoint-anchored** replacement
  cannot overflow in any order: the anchor
  `a = min/2 + max/2` uses exact power-of-two halves with `|a|` bounded
  by max float64; every deviation `x_i − a` has an exact value bounded
  by the half batch range, which is representable, so the correctly
  rounded difference never overflows; every per-term `(x_i − a)/k` and
  every partial sum is a convex share of that half range; and the mean
  itself is a convex combination of the observations, hence
  representable — the final `a + Σ` can only overflow when the true
  mean is out of float64 range, which then honestly yields ±Inf by
  sign. The anchor is permutation-invariant, so peer order affects
  nothing beyond Neumaier's usual ±1-ULP summation noise (§10 pins all
  six permutations finite). Likewise, each regrouped scatter term
  equals its true `1/k` share of the total, so no term (and no partial
  sum of non-negative terms) can overflow while the result is
  representable. A square/cross sum whose *individual terms* are
  themselves beyond float64 (deviations `±5e306` of `[1e308, 1.1e308]`,
  squares `2.5e613`) is **retained in the extended form** (rev-7
  finding 4 supersedes the rev-6 +Inf sign pin *for state and merge
  inputs*): the state keeps the magnitude — later weighting can bring
  it back (the `4e308 → 3.90625e305` recovery) — and emission alone
  decides the honest output (var +Inf by sign; corr NULL on the
  non-finite variance; the signed cross moment keeps its signed
  mantissa, so the rev-6 `cov = NaN` leak on `[0, −2e154, 2e154]`
  batches is closed). Verified: `[1e308, 1.1e308]` → mean `1.05e308` /
  emitted var +Inf / corr NULL; `[−1e154, 1e154]` → mean 0 / var
  `1e308` / corr 1; all six permutations of
  `[1e308, 1.1e308, −1e308]` → finite means at
  `3.6666666666666666e307` ±1 ULP.
- **A finite mean does not promise finite raw deviations (rev-8
  finding 1, seventh review):** `[1.7e308, 1.7e308, −1.7e308]` has the
  finite mean `5.666666666666667e307` while the min-side raw deviation
  `x − m̄x = −2.2667e308` overflows *in every peer permutation* —
  rev 7 formed those raw deviations unguarded and the −Inf operands
  reached the extended multiplication, whose mantissa normalization
  became a non-terminating loop (a hang: 6/6 permutations and the
  4-key low-weight sequence both never returned; watchdog-evidenced).
  The rev-8 form therefore gates the deviation lists before any
  multiplication and evaluates the exact anchored identity
  `Σ(x−m̄x)² = Σ(x−a)² − k·δx²` (cross pairing per-side anchors with
  δ = 0 on a finite-deviation side) whenever a list is non-finite —
  every operand is at most the half batch range — and the identity's
  bounded δ² cancellation is the accepted trade for overflow-freedom
  (the raw-deviation channel stays primary and bit-identical for
  every finite-deviation batch). The extended normalization itself is
  **contract-checked**: a non-finite value or mantissa reaching it is
  a defined internal error (never a loop), because no guarded
  formation step above may produce one.

Step 2 — **the layered merge: scaled primary form + gated share
layers + the absolute-mass layer** (rev-5 findings 1/4/5, corrected in
rev 6 for findings 1/2, gated and extended in rev 7 for findings
2/3/4), with the account's decayed `W`:

```
if C-is-first (W == 0):
    W, μx, μy, vxx, vyy, vxy = A, m̄x, m̄y, sxx, syy, sxy   # degenerate
    # (sxx/syy/sxy may be extended pairs — first-batch moments can be
    #  out of float64 range while the mean is fine; § State)
    # rev 15, Decision 55: this raw batch state commit keeps the
    # pinned rev-9 narrowed word fl(fl(Σregisters)/k) as its
    # single-batch emission and commits the GENUINE residual as
    # the tail — rebuilt from ALL compensated registers as separate
    # cascade items (rev 14's own list folded (cx + cxl) in float64
    # first: the cross-scale [H=2^400, M=2^200, 1, -H, -M] batch
    # read the tail at -0.2 -- the word's own scale -- and every
    # continuation merged the corrupt 0.0 complete mean while the
    # single-batch emission alone masked it; reproduced, then
    # repaired — fourteenth-review finding 1, Invariant 31). The
    # merge layers own the complete-mean folds; the raw word keeps
    # the pinned narrowed arithmetic (rev-9 finding-3 pins).
else if any of vxx, vyy, vxy, sxx, syy, sxy is an extended pair:
    layer (C) directly                          # float64 layers can't see
                                                # retained magnitude
else:
    # (P) PRIMARY — power-of-two normalized mass form (rev 5):
    # frexp on max(W, A) gives e; w1 = ldexp(W, −e), w2 = ldexp(A, −e)
    # are exact scalings while they stay off the subnormal lattice
    # (never a materialized 2^−e, which itself overflows for
    # deep-subnormal masses); n' = w1 + w2.
    # rev-8 finding 2: the scaling is TRUSTED only when it
    # ROUND-TRIPS bit-exactly — ldexp(w_i, e) == mass on both
    # sides. On the subnormal lattice ldexp ties-to-even and
    # reweights by whole subnormal units (arrival A = 3·2^-1074
    # scales to w2 = 2·2^-1074: a silent 33% reweight that the
    # rev-7 w == 0-only gate could not see).
    # rev-10, ninth-review finding 1 — COMPLETE-mean merge algebra:
    # equality requires m̄x == μx AND τx == μx_lo (the COMPLETE means
    # coincide; likewise y). Equal hi parts with different tails
    # ([2^53, 2^53+2, 2^53]) must NOT take the fast path.
    eqx := (m̄x == μx and τx == μx_lo);  eqy := (m̄y == μy and τy == μy_lo)
    if eqx:  μx' = μx                    # exact fast path (constants)
    else:    μx' = (w1·μx + w2·m̄x)/n'
    if eqy:  μy' = μy
    else:    μy' = (w1·μy + w2·m̄y)/n'
    # merged mean TAILS FIRST — the deviations need them; νx keeps
    # the same compensated mass·mean numerators as rev 9 (Dekker
    # two-products + two-sums, including the mass·lo carries), and
    # μx' = fl(νx) stays BIT-IDENTICAL to the form above:
    #     νx  := (w1·(μx+μx_lo) + w2·(m̄x+τx) + residuals)/n'
    #     τx' := νx − μx'   (the new merged tail; μx_lo on the eqx
    #     fast path);      νy, τy' likewise
    # rev 14, thirteenth-review finding 1: the merged pair folds
    # through the SHARED complete-mean normalization before the
    # store — the exact two-sum fold of (hi, tail): hi' = fl(hi +
    # tail), tail' = the exact residual, for EVERY high word (rev 13
    # folded only a high word that cancelled to exactly ZERO). Keys
    # [0,0,1] x=y=[2^53,2^53+2,-2^53+2]: the share products s1·μ +
    # s2·m̄ each round once and the merged pair holds hi 1.0 with the
    # EXACT tail .5 — the fold commits and emits EXACTLY 1.5 (rev 13
    # emitted 1.0 while the tail held the content). A benign pair
    # (tail below its half-ulp boundary) folds as an exact identity,
    # so preserved rows stay bit-identical and the deviations/scatter
    # arithmetic (identical content either way) is unchanged.
    # deviations BETWEEN COMPLETE MEANS — state hi+lo and arrival
    # hi+lo to the merged fl(ν)+tail — NO end correction exists:
    #     dx  := (μx − μx') + (μx_lo − τx')
    #     dbx := (m̄x − μx') + (τx   − τx')    # dy, dby likewise
    # (the rev-9 chain centered on the hi parts and subtracted a δ²
    # with no cross term: [2^53, 2^53+2, 2^53+4] committed 4.25
    # where the complete-mean merge returns EXACTLY 2.75 — the one
    # Invariant-5 violation class the rev-9 recentering could not
    # see; §10 r10F1).
    vxx' = (w1·(vxx + dx²) + w2·(sxx + dbx²))/n'
    vyy' = (w1·(vyy + dy²) + w2·(syy + dby²))/n'
    vxy' = (w1·(vxy + dx·dy) + w2·(sxy + dbx·dby))/n'
    # The complete-equality fast paths keep constant streams exact
    # (every deviation exactly 0; the state tail stays canonical).
    # commit P unless ANY:
    #   (L) lattice rebind (rev-9 eighth-review finding 4;
    #       rev-10 ninth-review finding 3): any nonzero stored or
    #       incoming moment with frexp-exponent ≤ −511 (already
    #       subnormal, or subnormal once the merge weights multiply
    #       it), or any deviation (dx, dbx, y-side likewise) nonzero
    #       at that scale — its weighted square reaches the subnormal
    #       lattice — rebinds this merge to (C), whose extended
    #       products retain the magnitude for the emitted variance
    #       AND the representable std/corr (singletons
    #       [−2^-537, +2^-537]: the float64 merge rounds each
    #       0.5·2^-1074 term to zero BEFORE the summation; (C) keeps
    #       var/cov exactly 2^-1074, std exactly 2^-537, corr
    #       exactly 1) — AND a WEIGHTED-PRODUCT rebind (rev-10,
    #       ninth-review finding 3): the float64 product of any
    #       nonzero moment with ITS side's weight would itself land
    #       on the subnormal lattice (mantissas in [0.5,1): the
    #       product sits at exponent e_val + e_weight, subnormal
    #       from −1021 down). The unweighted exponents miss NORMAL
    #       moments whose weighting destroys them: keys [0,0,1000],
    #       h=1, adjF, x=y=[−2^-50, +2^-50, 0] — the state's variance
    #       2^-100 passed the rev-9 gate, the decayed mass
    #       w1 ≈ 2^-1000 multiplied it to ≈2^-1100 and the float64
    #       product rounded to ZERO before the division and store, so
    #       ewmStd emitted 0.0 while the engine's own rounded masses
    #       make ≈2^-550 = 2.713328551617619e-166 exact (variance
    #       honestly 0.0). (P) tests w1/w2; the share layers test
    #       s1/s2; the incoming-side counterpart (keys [0, 1e-300],
    #       tie arrival, variance 2^-100 under arrival mass
    #       A ≈ 6.93e-301 → std 7.394570388520962e-166) is pinned
    #       the same way (§10 r10F3);
    #   (L2) WEIGHTED-DEVIATION products (rev-11, tenth-review
    #       finding 2): with both source moments ZERO the merged
    #       moment's entire content is w·dev² (cross w·dx·dy), and
    #       rev 10's moment-only gates underflowed those products
    #       unwitnessed (keys [0,1000] h=1 adjF x=y=[2^-100,0] and
    #       the incoming-side counterpart [0,1e-300] emitted std 0.0
    #       where 2.4099198651029665e-181 / 6.567697842037889e-181
    #       are exact on the engine's own rounded masses, variance
    #       honestly 0.0). Rebind when any nonzero pair
    #       (dx,dx,w1), (dbx,dbx,w2), (dy,dy,w1), (dby,dby,w2),
    #       (dx,dy,w1), (dbx,dby,w2) reaches frexp-sum −1021 — before
    #       the assembly multiplies (§10 r11F2; Invariant 25);
    #   (E) extreme-ratio: ldexp rounds one weight to 0 while its mass
    #       is positive — an asymmetric ratio beyond the scaled domain;
    #   (X) inexact scaling: the power-of-two round-trip failed on a
    #       nonzero weight (subnormal-lattice tie, rev-8 finding 2);
    #   (S) finite inputs produced a non-finite result — an intermediate
    #       numerator addition or pre-weight square overflowed.
    #
    # (F) SHARE LAYERS — computed into temporaries, then GATED (rev 7):
    #     s1, s2 with s1 + s2 = 1;
    #     if (E) or (X): s1 = W/(W+A), s2 = A/(W+A)  (direct shares
    #                   over RAW masses — the true mass ratio down to
    #                   the smallest subnormal; also the route for
    #                   every inexact subnormal-lattice scaling, so
    #                   no committed share inherits the tie)
    #     else:          s1 = w1/n', s2 = w2/n'
    # rev-10, ninth-review finding 1 — the share-form mirror of
    # the (P) algebra: equality of the COMPLETE means, merged tails
    # FIRST (ν over the share-consistent raw masses m1/m2 and the
    # denominator — same compensated numerators as rev 9), then
    # complete-mean deviations; no end correction exists.
    eqx := (m̄x == μx and τx == μx_lo);  eqy likewise
    if eqx:  μx' = μx                   # exact fast path (constants)
    else:    μx' = s1·μx + s2·m̄x        # convex: no overflow while
    if eqy:  μy' = μy                    # the result is representable
    else:    μy' = s1·μy + s2·m̄y
    #     dxs  := (μx−μx') + (μx_lo−τx')
    #     dbxs := (m̄x−μx') + (τx  −τx')    # dys, dbys likewise
    τvxx = s1·vxx + dxs·(dxs·s1) + s2·sxx + dbxs·(dbxs·s2)
    τvyy = s1·vyy + dys·(dys·s1) + s2·syy + dbys·(dbys·s2)
    τvxy = s1·vxy + dxs·(dys·s1) + s2·sxy + dbxs·(dbys·s2)
    # rev-10: the (L) lattice rebind — now including the
    # weighted-product rule against s1/s2 — applies through this
    # layer's deviations; the raw differences of the weight-inside
    # terms are formed from the complete means.
    # rev-11: the (L2) weighted-deviation products mirror with s1/s2
    # (tenth-review finding 2; §10 r11F2; Invariant 25).
    # commit (F) ONLY IF ALL THREE GATES PASS:
    #   G1 no side drop: NOT (s1 == 0 with W > 0) and NOT (s2 == 0
    #      with A > 0) — a materialized share that rounds a positive
    #      side to zero discards a representable contribution
    #      (rev-7 finding 3: W = 2^−1074 against arrival A = 2,
    #      s1 = half the smallest subnormal → 0);
    #   G3 never-commit-subnormal shares (rev-8 finding 2, superseded
    #      in rev 9 by the eighth-review finding 1): a share below the
    #      smallest NORMAL value is a quantized multiple of 2^-1074
    #      and NEVER commits in the float64 share layers — the rev-8
    #      reconstruction certificate (s·denominator == mass) also
    #      ROUNDS and admitted a share understating its mass by 25%
    #      (retained W = 2^-1074 under arrival A = 0.75: the share
    #      rounds to u, "reconstructs" W, and committed mean
    #      4.940656458412465e-124 where the true merged value on the
    #      same rounded masses is 6.587541944549954e-124). Every
    #      subnormal share on either side rebinds to (C), where
    #      contributions form as mass·value products with the mass
    #      exponent preserved; pinned on BOTH merge sides (§10);
    #   G2 finite results from finite inputs — the raw differences
    #      dxs/dbxs are formed before their weight enters, so a 2e308
    #      deviation still overflows INSIDE this layer (rev-7 finding 2);
    # …otherwise fall through to (C). (Inputs already outside float64 —
    #  honest carried infinities from INTERNAL overflow products of
    #  finite samples — keep the rev-6 pass-through behavior. REV 20,
    #  ninth-review P2 finding 3 / Invariant 39: a non-finite SAMPLE
    #  VALUE can no longer reach these layers at all — the public
    #  finite-sample gate rejects every ±Inf/NaN sample at the row
    #  pre-scan before any group is processed, including one posed
    #  through these operators' own permitted infinite OUTPUTS via
    #  a subquery; the rev-6 clause's "non-finite data values" is
    #  thereby superseded by internal overflow products, and the
    #  extended-normalization assertion is a true internal
    #  invariant.)
    #
    # (C) ABSOLUTE-MASS LAYER (rev 7) — no raw difference, no
    #     materialized share, extended operands allowed:
    T  = W + A                          # true total mass (extended sum)
    # rev-11, tenth-review finding 1: the complete means are carried
    # as DOUBLE-DOUBLE pairs through EVERY (C) step — a single
    # extended word still has a 53-bit mantissa, and rev 10 folded
    # the numerator and each source hi+lo pair through ONE xadd,
    # losing the merged tail ([2^600, 2^600+2^548] measured its
    # arrival deviation as 2^548 and emitted std 2^547.5 where 2^547
    # is exact; the translated [0, 2^548] control was right; the
    # mixed-scale [2^53,+2,+4] × 2^-260 continuation emitted cov
    # 1.8891618714269098e-78 / corr 0.9949366763261822).
    Nx    := ddsum_e(W·μx, A·m̄x, W·μx_lo, A·τx)  # (hi, lo) extended:
    Ny    := ddsum_e(W·μy, A·m̄y, W·μy_lo, A·τy)  # rev 12,
    νx    := dddiv(Nx, T)   # eleventh-review finding 1: every
    νy    := dddiv(Ny, T)   # product forms ERROR-FREE (xtwoprod —
    # the one-rounded leading word bit-identical to xmul PLUS its
    # exact residual pair), the exact-alignment fold chain keeps
    # every fold loss in the lo word, and dddiv subtracts the
    # error-free q_hi·T so the remainder is EXACT (rev 11 subtracted
    # the one-rounded product: dddiv(1,3) returned a zero tail word
    # and the singleton centers lost the products' tails — std
    # 5.150657059589176e+164 / 5.319575737017211e+164 where
    # √15/8·2^548 and √2/3·2^548 are exact) — (ν_hi, ν_lo) is the
    # COMPLETE un-narrowed merged mean.
    eqx := m̄x == μx and τx == μx_lo            # COMPLETE equality,
    eqy := m̄y == μy and τy == μy_lo            # as in (P)/(F)
    # rev 14, thirteenth-review finding 1: the dd quotient folds
    # through the SAME shared complete-mean normalization — an
    # EXACT pair two-sum over (ν_hi, ν_lo) whose fold-loss residual
    # stays retained in the tail slot — BEFORE the narrowing, for
    # EVERY quotient (rev 13 swapped only a quotient whose high word
    # cancelled to exactly ZERO). The singletons [2^600,
    # -fl(5/3)·2^600+2^549] under alpha=3/8 adjust=false leave in
    # dddiv the lead 4·2^545 with the EXACT +2^545 in the remainder
    # tail — the folded high word is EXACTLY 5·2^545 =
    # 5.758609657015291e+164 (rev 13 emitted the lead
    # 4.606887725612233e+164); a zero high word with a nonzero tail
    # folds the tail into the high slot unchanged (the rev-12/13
    # cases), and a benign quotient (tail below its half-ulp
    # boundary) folds as an exact identity:
    νx := exact_pair_twosum_fold(νx_hi, νx_lo)   # (hi', tail') with
    νy := exact_pair_twosum_fold(νy_hi, νy_lo)   # the fold loss kept
    μx' = μx   if eqx  else fl64(νx_hi)        # the narrowed hi; the
    μy' = μy   if eqy  else fl64(νy_hi)        # carried tail is
    δx  = νx − μx'                            # (ν_hi − fl(ν_hi)) plus
    δy  = νy − μy'                            # the dd tail word,
    # folded once through pack_remainder (rev 20, Invariant 38)
    # and committed through the ONE shared retention rule
    # lane_retains (rev 21, twentieth-review P2; Invariant 40 /
    # Decision 64): the packed merged tail keeps its extended
    # pair whenever re-normalizing the narrowed value does NOT
    # reproduce it — the nonzero subnormal tail included — and
    # only an exact narrowing keeps the narrowed scalar tail
    # lane (the rev-9 finding-3 narrowed-once merged-tail store
    # is superseded).
    # the state and arrival sources enter as their OWN hi+lo pairs —
    # nothing is crushed — against the double-double targets:
    #     targetx := (μx, μx_lo)  if eqx  else νx   (likewise y)
    # — on an equality path sources and target coincide exactly, so
    # every deviation term is exactly zero and constants stay
    # bitwise-exact through (C) at any magnitude.
    # every deviation term as a double-double weighted difference,
    # each product error-free (rev 12, eleventh-review finding 1):
    # q(S, S_lo, target, mass) := ddsum_e(mass·S, mass·S_lo,
    #                                   −mass·target_hi,
    #                                   −mass·target_lo)
    #                        # the weighted difference, both words;
    #                        # each product's EXACT residual joins
    #                        # the fold losses in the lo word
    # term(S, S_lo, target, mass) := dd_mul(q, D)
    #     with D := ddsub(S, S_lo, target_hi, target_lo)
    #                        # the raw difference, both words; the
    #                        # extended products never overflow, so
    #                        # NO representable-difference fast path
    #                        # and no q·q/mass fallback exists
    #                        # anymore (2e308 at mass 5e-324 retains
    #                        # its extended magnitude term ~2e293)
    # cross((Sa,Sa_lo,Ta),(Sb,Sb_lo,Tb),mass) := dd_mul(q_a, D_b)
    #                        # the a-side weighted difference times
    #                        # the b-side raw difference, both words
    vxx' = ( term(μx, μx_lo, targetx, W)
           + term(m̄x, τx, targetx, A)
           + W·vxx + A·sxx ) / T  # deviations already center on the
    vyy'  = (… likewise with y …) / T  # complete merged means — NO
    vxy'  = ( cross((μx,μx_lo,targetx),(μy,μy_lo,targety), W)
           + cross((m̄x,τx,targetx),(m̄y,τy,targety), A)
           + W·vxy + A·sxy ) / T  # δ² end correction exists; no raw
                                  # subtraction quantizes a centering
                                  # term
    # rev-9, eighth-review finding 2 / rev-10, ninth-review finding 2:
    # the four-term moment assemblies (like the batch extended
    # fallbacks) use the CASCADING compensation — the level-0 loop
    # and first residual register are exactly rev 8's compensated
    # accumulation, and every bit a register fold discards cascades
    # into the next bounded register instead of vanishing (the
    # registers hold precisely the discarded bits): the 1e200/1e150
    # pair batch keeps its covariance 3.333333333333333e299 in EVERY
    # peer permutation (rev 7's plain fold returned 0.0 in the
    # small-pairs-early order), and the three-scale batch
    # [H=2^600, M=2^400, L=2^200] — where rev 8's single register
    # swallowed the L² residuals only 800 orders below the running
    # register and returned a covariance of exactly 0.0 in the
    # interleaved order — returns 5.1644997561738175e+119 in BOTH
    # review orderings; error bound: one rounding of the final
    # restoration plus a second-order perturbation at the running-sum
    # scale.
    # rev-12, eleventh-review finding 2b: the SHARED bounded
    # restoration. The head fold (acc + regs[0]) keeps its EXACT loss
    # (rev 11 xadd'd it and dropped whatever fell outside the 53-bit
    # window); the head loss, every register and every subsequent
    # fold loss process through ONE descending-magnitude queue in
    # which mirror pairs cancel exactly before small content folds:
    # a fold whose loss returns the WHOLE item (nothing fits) is held
    # back and retried only after the running total changed — a pass
    # that changes nothing can never host a held item, and the
    # remainder flushes through xadd, the documented float64
    # information limit (rev 11's restore discarded its head-fold and
    # leftover losses: the extended sum [-2^323, 2^215, 2^431,
    # 2^323, 0.75, -2^431, -2^215] whose registers [ACC=-B, 0.75, +B]
    # restore to 0.75 returned 0.0). Termination is structural, no
    # countdown loop; single-register restores keep the unchanged
    # fast path.
    # rev-10 accumulator domain contract (ninth-review finding 2):
    # the cascade handles BOTH alignment directions SYMMETRICALLY —
    # when a larger incoming term's scaled alignment makes the OLD
    # accumulator's mantissa underflow, the OLD accumulator cascades
    # intact as the residual, exactly like the first branch's rule
    # for an underflowing incoming term (rev 9 read zero residuals
    # there and silently DROPPED the old accumulator: the
    # unit-rows-first [(1,1),(−1,−1),(H,H),(−H,−H),(H,−H),(−H,H)]
    # batch at H = 2^600 returned a covariance of exactly 0.0 while
    # 1/3 is exact, and the 10-scale 30-row batch returned 0.0 where
    # 2^500/15 is exact). Capacity: 128 bounded registers — every
    # fold residual is bounded by half an ulp of its folded operands,
    # so each cascade level descends at least 53 binary orders below
    # the previous level's scale, and the supported term grammar
    # bounds relevable term exponents to [−3222, 3074] (subnormal
    # masses against subnormal deviations at the floor; the
    # q·q/mass overflow fallback at the top) — at most ~120 register
    # levels can occur; the capacity exceeds the domain worst case
    # with margin and the depth (128) is the documented limit.
    # rev-11, tenth-review finding 3: EXACT-ALIGNMENT two-sums. An
    # ldexp that lands on the subnormal mantissa lattice quantizes
    # the smaller operand BEFORE either two-sum — rev 9/10 recorded
    # only the post-quantization bits (0.75 aligned against 2^1073
    # came back as 1.0; the H=2^537 unit batch returned
    # 0.6666666666666666 small-pairs-first where 0.75 is exact; the
    # direct accumulator 0.75 + 2^1073 − 2^1073 returned 1.0). Every
    # fold — both directions, any gap, main accumulator AND register
    # folds — runs at a scale where BOTH mantissa views are normal:
    # for gaps in [1022, 2043] both operands scale (the big mantissa
    # up by gap−1021 ≤ 1022, finite/normal/exact; the small mantissa
    # into [2^-1022, 2^-1021)), the two-sum and its exact error are
    # computed with no lattice rounding and the complement rescales;
    # gaps beyond 2043 cascade the smaller operand INTACT
    # (out = big, loss = sml) — no 53-bit window at the big scale can
    # hold it. Capacity stays 128: each register level's residual is
    # the EXACT complement now, so the ≥53-order descent holds a
    # fortiori (r11F3, Invariant 23).
    # results are stored as float64 when back in NORMAL range; a
    # NONZERO result that would narrow to a subnormal value or to
    # zero STAYS an extended pair (rev-9, finding 4 — its std/corr
    # remain computable; narrowing is an emission-time event), as do
    # out-of-range magnitudes (retained magnitude; § State)
    W, μx, μy, vxx, vyy, vxy = W + A, μx', μy', vxx', vyy', vxy'
    μx_lo, μy_lo = (μx_lo, μy_lo)  (equality fast path)  else  retained(δx), retained(δy)
    # retained(δ) := the xval-narrowed value, kept as the extended
    # pair whenever lane_retains says narrowing loses ANY content
    # — the merged-state (word, tail) is an EXACT split of the
    # computed complete mean on BOTH streams after packing
    # (singleton chain [B], [B+2u], [B+4u] at B = 2^-1021,
    # u = 2^-1074 under α=0.5 adjust=true commits the genuine
    # retained pair −2u/3 and the completes 4u/3, 20u/7, 68u/15
    # with std u — the exact 104u²/49 rounding — in all eight
    # row/time × ignoreNA × sign combinations; rev 20 committed
    # the narrowed −u scalar with completes B+u, B+3u, B+5u and
    # std 2u).
C += k   # batch processing order (§ Arrival masses): counted also for a
         # zero-mass arrival, i.e. before any A == 0.0 early return
```

- The `W == 0` branch initializes (partition start, per account) and
  re-anchors (full decay) in one rule; ratios are `A`-independent.
- **The incoming batch's own cross-scatter `sxy` is retained in every
  non-degenerate merge** (rev-5 finding 1 — the rev-4 line dropped it and
  returned cov −36 / corr −0.6428571428571429 for keys `[1,2,2]`,
  x=[0,10,20], y=−x, where the documented −56 / −1 is correct; singletons
  (`sxy = 0`) and first batches had masked it).
- **Why the primary path stays (Invariant 16):** the power-of-two
  scalings are exact exponent adjustments while they stay off the
  subnormal lattice — rev-8 finding 2 adds the bit-exact round-trip
  gate, and an ldexp that ties on the lattice (odd multiple of
  2^-1074, e.g. `A = 3·2^-1074` scaling to `2·2^-1074`) no longer
  runs the primary form; no pinned vector ever hit that case (all 116
  rev-7 rows stay byte-identical) — and because a power of two
  commutes with every rounding step and cancels in the quotient, any
  merge whose intermediates stay in the normal range is
  **bit-identical to the plain mass form** — every pinned oracle
  value of rev 2–5 survives bit-exactly (verified by rerunning all
  vectors; zero ULP change). The normalization also makes *comparable* masses at any
  absolute scale normal shares — `W = A = 5e-324` normalize to `0.5/0.5`
  and keep var `.125`, cov `−.125`, corr `−1` exactly, where rev-4's
  absolute products rendered var 0 / corr NULL.
- **Why the layers are needed (rev-6 findings 1/2 and rev-7 findings
  2/3, superseding the rev-5/6 safety claims):** weights ≤ 1 bound
  individual *products* only — the third merge of
  `[1.6e308, 1.6e308, 1.7e308]` α=.25 adjT overflows its *numerator
  addition* (`1.05e308 + 0.85e308` → Inf where
  `1.6432432432432432e308` is representable) and `[0, 2e154]` α=.01
  adjF overflows its *pre-weight deviation square* (`(1.98e154)²` → Inf
  while the weighted contribution is `1.96e306`) — and the scaling
  itself can round a weight to zero while its weight-times-value
  product stays representable (keys `[0, 8e-16]` h=1e308: the
  subnormal arrival `A = 5e-324` scales to `w2 = 0`, but the true
  contribution requires mean `4.940656458412465e-174`, var/cov
  `4.940656458412465e-24`, corr 1). Rev 6 routed both cases to the
  *direct-share* path and claimed its shares were "exactly
  representable down to the smallest subnormal" — **superseded
  (rev-7 finding 3):** a share can itself round *below* the smallest
  subnormal to zero while its mass-times-value products remain
  representable (retained `W = 2^−1074` meeting a tied arrival `A = 2`:
  `W/(W+A)` is exactly half the smallest subnormal and ties-to-even
  rounds to **0**, silently discarding the old state — mean/var/cov
  0 / corr NULL returned where ≈ `2.4703e-320` / `2.47032823e-316` /
  1 are required). And the weight-inside-product terms were formed
  from the **raw** difference `dbxs := m̄x − μx'` before the weight
  entered — **superseded (rev-7 finding 2):** with keys `[0, 8e-16]`,
  h=1e308, adjF, `x=y=[−1e308, 1e308]` the incoming deviation is
  `2e308` → Inf before weighting, returning var/cov Inf / corr NULL
  where independent weighted arithmetic gives
  `≈ 1.9762625833649863e293` / 1. The (C) fixes use exact algebra and
  exact power-of-two scalings with no unguarded intermediate: an
  absolute-mass mean `(W·μ + A·m̄)/(W+A)` is a mass-normalized convex
  combination (each product is the side's true absolute contribution);
  a weighted difference `q = mass·S − mass·T` never evaluates the raw
  difference and each product is a true weighted value; the term
  `q·D`/`q²/mass` equals the true `mass·(S−T)²` in either shape, so
  the choice between them is purely a range decision (D
  representable or not); the cross terms pair the weighted
  differences symmetrically. The gated (F) keeps its unchanged
  float64 shapes whenever G1+G2 hold, so every previously pinned
  share-layer value survives bit-identically, and the two preserved
  share-layer vector families remain green (verified 116/116; the
  one preserved non-finite-(F) row `[1.6e308, 1.6e308, 1.7e308]` now
  flows through (C) with a recorded 1-ULP mean shift inside its pinned
  tolerance, §12). The rev-5 "a weight that still rounds to zero is a
  genuinely negligible relative share" claim stays **withdrawn**
  (Alternative R); the rev-6 "direct shares preserve every
  representable contribution" claim is **withdrawn** with its fault
  values (Alternative W); the honest float64 limit — a weighted
  product below `5e-324` rounds away, and with the compensated
  extended accumulation a term is lost only when its alignment shift
  exceeds ~1074 binary orders below the running partial sum (the
  subnormal lattice; rev-8 finding 3 supersedes the rev-7 ~1020-order
  wording) — is documented and asserted where relevant.
- **Retained magnitude through the merge (rev-7 finding 4):** the
  (C) layer is the *only* consumer of extended operands, because a
  moment beyond float64 (batch or state) must survive the mass
  weighting before anyone decides it is too big: products of an
  extended operand with a float64 mass add exponents (mantissas are
  multiplied), sums align exponents (float64 information limit above),
  and the quotient by the true total mass `T` divides mantissas. Two
  fault vectors drive it: keys `[1, 2, 2]`, α=.01, adjF,
  `x=y=[0, −2e154, 2e154]` — the batch variance is truly `4e308`
  (beyond float64) while the merged variance
  `≈ 7.920792079207922e306` is representable (rev 6 returned Inf and
  a NaN covariance); and a first tied batch `[−2e154, 2e154]` whose
  state variance `4e308` emits honest +Inf *at its own rows* but must
  recover `3.90625e305` when a zero arrives ten half-lives later (rev
  6's stored Inf stayed Inf forever). Both are §10 rows. Results back
  in range resume the plain float64 state after (C), so later normal
  merges run the primary path exactly as before.
- **Honest IEEE limits (rev-7: emission-only):** a moment whose
  *true* value still exceeds float64 **when a row is emitted** yields
  ±Inf by sign — never NaN (non-negative sums carry a non-negative
  mantissa; the signed cross moment keeps its sign; means are computed
  convexly) — and correlation is NULL when either normalized
  variance's *emitted value* is non-finite (§ Outputs). A non-finite
  emission means the value left the merge out of range *after
  weighting*, never that an intermediate gave up mid-flight.
- The f-form (`f = W/(W+A)`, `x' = f·x + (1−f)·y`) is a **rejected
  alternative**: it is not bit-identical to the mass form (it drifts the
  pinned gap-matrix value 2.6 and the agreement row 10 to
  `2.6000000000000005` / `10.000000000000002`), and for tiny-but-
  significant arrival shares `1 − f` rounds the share away entirely
  (`dt=1`, `h=1e17`: the row `0.6931471805599453` collapses to 0) —
  recorded as Alternative O (§8).
- **Forbidden forms** (explicitly): raw-moment `Q/D − μ²`; any update
  consulting an undefined previous mean; absolute-moment state (`Mxx`
  mass-scaled) — superseded by this normalized representation; the
  superseded rev-3 per-observation `a_i = A/k` materialization; the
  absolute-domain mass merge of rev 4; the f-form convex merge
  (Alternative O); and — superseded in rev 6 — any single-arithmetic
  claim that one unguarded float64 expression serves the whole merge
  domain (the layered detection is required; unguarded numerator
  additions and pre-weight squares are forbidden).
- The `max(vxx, 0)` applied at *output* time only is a round-off guard;
  every accumulated term is non-negative in exact arithmetic, so any
  substantial negative moment is an implementation fault, and tests
  assert unclamped values.

### Outputs (after each peer group; C-gate)

- **Emission is the only conversion point (rev-7 finding 4, amended
  in rev 8 for ewmStd):** each state moment is emitted as its float64
  view — plain values pass through untouched; an extended
  (retained-magnitude) value converts once here (exact `ldexp(m, e)`),
  landing on the float64 value when it is in range and on the honest
  sign-pinned ±Inf when it is not. The single exception is
  `ewm_std`, which applies its square root to the retained moment
  BEFORE the narrowing (rev-8 finding 4, next bullet): an extended
  variance whose sqrt is representable emits that finite standard
  deviation while `ewm_var` may honestly emit +Inf on the same row.
- `ewm_mean = μx`; `ewm_var = max(f64(vxx), 0)`; `ewm_std` =
  `f64_apply(√, vxx_retained)` — the square root applied **to the
  retained scaled moment, before the float64 narrowing** (rev-8
  finding 4): an extended pair splits its exponent exactly (an odd
  exponent borrows one) so a single float64 mantissa sqrt decides the
  value — the retained `4e308` emits std EXACTLY `2e154` while
  `ewm_var` honestly emits +Inf on the same row; a plain float64
  variance passes through the plain float64 sqrt. Variance overflow
  does not imply standard-deviation overflow: the var/std verdicts
  may legitimately differ on one row. Rev-9 (eighth-review finding 4)
  extends the retained domain DOWNWARD: a nonzero moment that would
  narrow to a subnormal value or to zero stays extended in the state,
  so its representable square root and correlation still emit — tied
  `[−1e-200, 1e-200]` emits var honestly 0.0 (the true 1e-400 is
  below the emission lattice) with std ~1e-200 and corr NULL, and the
  singleton merge `[−2^-537, +2^-537]` emits var/cov EXACTLY 2^-1074,
  std EXACTLY 2^-537, corr EXACTLY 1 — values the float64 merge
  destroyed before its summation.
  (`ewm_cov = f64(vxy)` on the pair account;
  `ewm_corr = f64(vxy_ret ÷ (√ret(vxx)·√ret(vyy)))` — the geometric
  normalization computed from the RETAINED moments (rev-10,
  ninth-review finding 4): the square root applies to the retained
  extended moment exactly as specified for ewmStd, and the product
  and quotient stay in the extended channel; only the final verdict
  narrows. Rev 9 narrowed vxy/vxx/vyy FIRST, so subnormal quantization
  emitted corr 2.0/−2.0 on the perfectly correlated streams
  [(±2^-537, ±1.5·2^-537)] (tied and singleton paths) where every
  retained bit says exactly ±1.0 (the unit-scale control returned
  1.0). On plain float64 states the form is bit-identical to
  `f64(vxy)/(√f64(vxx)·√f64(vyy))` — the same sqrt/product/quotient
  roundings, power-of-two rescalings only — NULL routing unchanged:
  NULL if the emitted variance is ≤ 0 **or either emitted variance is
  non-finite** (rev-6: a variance beyond float64 makes the geometric
  normalization Inf/Inf = NaN — documented NULL, never a NaN output;
  rev-7 keeps this rule on the *emitted* value while the state may
  hold a recoverable magnitude — the float64 VIEW keeps driving the
  routing), separate zero checks; the variance product is never formed
  as float64);
- **NULL iff `C < max(1, minPeriods)`** (the account's own count). A
  zero-mass or fully-decayed state with consumed observations emits the
  carried state; an unseen account emits NULL everywhere.

### Peer groups, NULL-valued rows and gaps

- Consecutive rows sharing equal ORDER BY keys form one **peer group**
  (one arrival batch). All state updates are order-free batch aggregates
  (`pkg/sort/sort.go:866-868` — the sorter is not stable; Invariant 7).
  All rows of a group emit the group-post value; NULL *bits* appear at
  rows whose own inputs are NULL under event-time ignoreNA=true only
  (the value is still shared); rows of a group whose own inputs are NULL
  join no account's batch (they are "NULL-valued" rows below).
- Row mode: `ignoreNA=false` — NULL-valued groups apply their rows'
  aging (counted by the span rule when the next observed batch arrives);
  `ignoreNA=true` — they are invisible to the clock. Either way they emit
  the carried current state (C-gated).
- Event mode: NULL-valued groups always apply their instant's transition
  (physical time), emit the carried estimate, and (inaT) set NULL bits on
  their rows.
- Constant-signal proof: for constant observed `c`, every batch mean/`m̄`
  is `c`, every merge hits the merge-level equality fast path (the merged
  mean is the shared exact value, both deltas exactly 0 at any
  magnitude), the identity fast path gives exact batch-local zeros, and
  zero-mass/decayed states carry the last computed ratios — outputs are
  `c` at every row in every policy combination (Invariant 4; asserted
  incl. α=1, underflow, ties, gaps, and multi-batch constant chains).

### Hand-verified acceptance vectors (independent oracles for BVT/UT)

Every row below is asserted in the archived **rev-8** simulator
(`state/artifacts/issue-29945/ewm_rev8_oracle_simulator.py`; log
`ewm_rev8_oracle_run.log` — 123/123 PASS, exit 0 — the 116 rev-7
rows byte-identical by log diff plus the seven rev-8 rows): profile
block,
tolerance-meta block (now range-safe), anti-correlation block, null-gap
block, carry block, tie block (revised), clock block, accumulation block,
stability block, event-time blocks, underflow block, subnormal-share
block, constant-merge block, float-key extreme block, contract block,
range-safety block (rev-6), retained-magnitude block (rev-7),
integer-domain
block, zero-mass block, precision block, mixed-stream block, and the
rev-8 block (anchored scatter permutations + low-weight arrival;
exact-scaling/inexact-scaling on both merge sides; compensated cross
accumulation incl. the 720-permutation invariance; ewmStd from the
retained moment). All
previously pinned rows were **reassessed under the strict comparator**
(rev-6 finding 4): no row was found accepting an Inf/NaN against a
finite expectation, and every preserved value is unchanged bit-exactly
— with the single recorded rev-7 exception (the non-finite share-layer
row `[1.6e308,1.6e308,1.7e308]`, a 1-ULP mean shift inside its pinned
tolerance; §12). The mean rows
for `x=[1,2,3,4]` (profile; adjT), the gap matrix (2.6 / 7-3 / 7-3 / 2.0),
the constant rows (10s), the tie rows (15, 15, 22.5 and variances 25/25/
68.75; 3-tie first batch 20 and 200/3), the large-offset rows (2/9; tied
first batch 14/9; tied later batch 56.0 = its translation, cov −56.0,
corr −1), the correlation scale rows (s = 1e±100 → 1), the event-time
mean rows (10, 15, 37.5, 37.5 and 10, 50/3, 770/19, 770/19) with
scatter-retaining variances (193.75 adjF, 131.30193905817174 adjT),
repeated-time single-key (10, 20, 20; var 150), the NULL-handling rows
(8.381487 with inaT NULL bit), the carry rows (α=1 `[10,NULL,10]` → 10s;
α=1 `[10,NULL,30]` → 10,10,30 fresh ratios; event `[10,NULL]` at
`[0,1100], h=1` → 10,10 carried; `[10,NULL,4]` → re-anchored 4; the
`[3,5,NULL]` carried variance `0.11753902662993572`; all-NULL partition
NULLs), and the precision rows (BIGINT neighbors → 0,5; tiny ratio →
`0.6931471805599453`; int64-µs subsecond → `2.928932188134525`; decimal
keys → 3.0) are all unchanged from rev 2/3/4/5 and re-verified bit-exactly
on the rev-6 machine under the strict comparator (the layered merge keeps
its primary path, so the mass form's
roundings hold; Invariant 16/17). Vectors introduced in rev 4–6 (the
appended blocks hold the rev-5 and rev-6 rows):

| Vector | asserted |
|---|---|
| clocks: `[0,10,20]` keys `[1,2,2]` | adjT: 12 (inaF == inaT); adjF: 10 (inaF == inaT) |
| clocks: `[1,NULL,NULL,3]` keys `[1,2,2,3]` | adjT inaF → 25/9 (multi-row NULL peers fully aged; was 2.6); adjT inaT → 7/3; adjF inaF → 2.6; adjF inaT → 2.0 |
| clocks: mixed full-pair stream `[ (1, y=NULL), (2,2) ]` | row1 mean 1 (x-account C=1), pair cov NULL (pair C=0); row2 mean 1.5, W=1, pair cov 0, corr NULL (pair first batch) |
| accumulation: `[1e16, 1, −1e16]` (adjust=true first batch) | mean exactly `1/3` in all 6 row orders (unrounded) |
| accumulation: `9007199254740991`×3 | mean exactly `9007199254740991`, var exactly 0, corr NULL (identity fast path; rev-3 fault: 9007199254740990 / var 1 / corr 1) |
| accumulation: `0.1`×3; `3.25e300`×64 | mean exact, var exactly 0, corr NULL |
| accumulation: 3-tie permutation, 6 orders | all five statistics equal unrounded within 1e-12 (and constant batches exactly) |
| underflow: `[1e-100, 2e-100, NULL]`, t=`[0,1,501]`, h=1, adjT | decaying row keeps var `2.222222222222222e-201` and corr 1 (W ≈ 4.582404545249564e-151; rev-3 fault: 0/NULL) |
| underflow continuation: same + `3e-100` at t=501 | fresh ratios: mean `3e-100`, var 0, corr NULL (old share below float64 resolution) |
| integer gaps: `[MinInt64, MaxInt64]` h=1; `[0, MaxUint64]` h=1 | `0, 10` both (unsigned-magnitude domain; fault record: wrapped `−1` → mass `−1` → mean `−10`) |
| zero mass: `[7, 9]`, t=`[0, 1e-20]`, h=1e308 | carried `7, 7`, C=2, no error (rev-3 fault: ZeroDivisionError) |
| zero mass: pairs `(7,−7),(9,−9)` then `(11,−11)` at t=`1e-20` | cov −1 / corr −1 carried, no error |
| subnormal A: two-obs instant, A=`4.94e-324` (A/2 → 0.0 fault record) | no error; C counted; state carried (no per-obs A/k exists) |
| merge cov (rev-5 finding 1): row mode, keys `[1,2,2]`, x `[0,10,20]`, y=−x, α=.5, adjT | mean 12, var 56, **cov −56, corr −1** (rev-4 fault: cov −36 / corr −0.6428571428571429) |
| merge cov (rev-5 finding 1): same, adjF | mean 10, var 200/3, **cov −200/3, corr −1** |
| merge cov (rev-5 finding 1): event mode, non-initial multi-obs batches, both adjust modes | mean/var/**cov/corr** match direct absolute-position-weight closed forms (adjT row: mean 23.63636363636364, var 186.77685950413223, cov −186.77685950413223, corr −1; adjF row: 17.5 / 218.75 / −218.75 / −1) |
| scaled merge (rev-5 finding 4): keys `[−1075,−1074,0,5e-324]`, h=1, adjF, x `[.5,1.5,NULL,1]`, y=−x | final merge at `W = A = 5e-324`: mean 1.25, **var .125, cov −.125, corr −1** (rev-4 fault: var 0 / cov 0 / corr NULL); asserted with purely relative comparisons |
| constant merges (rev-5 finding 5): two singleton batches x=y=`.1`, α=.5, adjT and adjF | mean exactly `.1`, var exactly 0, corr NULL (rev-4 fault: `0.10000000000000002` / `1.925929944387236e-34` / corr 1) |
| constant merges (rev-5 finding 5): two singleton batches x=y=`1.3e308` | mean EXACTLY `1.3e308`, var 0, corr NULL (rev-4 fault: Inf / Inf / NaN) |
| constant merges (rev-5 finding 5): tie batch then singleton; 64-row constant first batch; 4-singleton constant chain | mean exact, var 0, corr NULL at every row (merge-level constancy, any magnitude) |
| float-key extreme (rev-5 finding 6): keys `[−1e308, 1e308]`, h=1e308, adjF, x `[0,10]` | `0, 7.5` (fault record: raw subtraction → `+inf` → mean 10); the quotient rule gives r = 2 exactly; equal subnormal/finite check: constant x=7 across the same gap stays exactly 7 |
| contracts (rev-5 findings 2/6): non-finite DOUBLE keys (NaN/±Inf); non-finite resolved α/h | rejected (bind/execution-boundary; §7); finite keys and policies accepted |
| tolerance meta-check (finding 4 note) | the suite's expected-value comparator is **purely relative** (plus exact equality): a wrong 0 for a tiny nonzero expectation (2.22e-201) FAILS; exact-equal zeros PASS |
| comparator range-safety (rev-6 finding 4) | NaN matches nothing; ±Inf matches only the same-signed ±Inf: `close(inf,1)`, `close(1,inf)`, `close(inf,−inf)` are False — an overflowing engine result can no longer PASS a finite assertion; `close(-inf,-inf)` True |
| range-safe merge mean (rev-6 finding 1a) | `[1.6e308,1.6e308,1.7e308]` α=.25 adjT → rows 1–2 EXACT `1.6e308`, row 3 `1.6432432432432432e308` (rev-5 fault: Inf), var honest Inf (true ~1e615), corr NULL |
| range-safe merge moments (rev-6 finding 1b) | `x=y=[0,2e154]` α=.01 adjF → mean `2e152`, var/cov `3.96e306`, corr 1 (rev-5 fault: Inf/Inf/NaN) |
| extreme-ratio shares, incoming (rev-6 finding 2) | keys `[0,8e-16]` h=1e308 adjF `x=y=[0,1e150]` → mean `4.940656458412465e-174`, var/cov `4.940656458412465e-24`, corr 1 (rev-5 fault: 0/0/0/NULL — the subnormal arrival `A=5e-324` scaled to `w2=0`) |
| extreme-ratio shares, retained (rev-6 finding 2) | keys `[0,1074]` h=1 adjF `x=y=[1e4,0]` → mean `s1·1e4` ≈ `4.9407e-320`, var/cov `s1·1e8` ≈ `4.9407e-316`, corr 1 (rev-5 fault: the `W=2^-1074` state was silently dropped; both asymmetries covered) |
| range-safe batch mean (rev-6 finding 3a) | first tied batch `[1e308,1.1e308]` → mean `1.05e308` (rev-5 fault: NaN from Inf−Inf compensation), honest Inf var (deviations `±5e306`, squares beyond float64), corr NULL |
| range-safe batch scatter (rev-6 finding 3b) | first tied batch `x=y=[−1e154,1e154]` → mean 0, var/cov `1e308`, corr 1 (rev-5 fault: NaN from the `1e308+1e308` square sum) |
| anchored batch mean, 6 permutations (rev-7 finding 1) | tied batch `[1e308,1.1e308,−1e308]` in **all six peer orders** → finite mean `3.6666666666666666e307` (±1 ULP; independent 900-digit expectation), var honest +Inf (≈1e615, out of float64 at emission), corr NULL (rev-6 fault: mean −Inf in the two same-sign-first orders — only 4/6 orders correct) |
| weighted-difference moments (rev-7 finding 2) | keys `[0,8e-16]` h=1e308 adjF `x=y=[−1e308,1e308]` → mean −1e308, var/cov `1.9762625833649863e293`, corr 1 (rev-6 fault: Inf/Inf/NULL — the raw `2e308` incoming deviation overflowed before weighting; A = 5e-324) |
| absolute-mass shares, retained side (rev-7 finding 3) | keys `[0,1074,1074]` h=1 adjT `x=y=[1e4,0,0]` → mean `2.4703e-320`, var/cov `2.47032823e-316`, corr 1 (rev-6 fault: 0/0/0/NULL — `W/(W+A)` with W=2^-1074, A=2 rounds to half the smallest subnormal → ties-to-even → 0, dropping the old state) |
| absolute-mass shares, incoming side, below subnormal (rev-7 finding 3) | merge-layer unit: W=2, `A=5e-324`, x=y=1e150 → mean `2.4703282292062327e-174`, var/cov `2.4703282292062325e-24`, corr 1 (the incoming share `A/(W+A)` rounds below the smallest subnormal; unreachable through this engine's bounded row/event mass accounting — W is ~1 there — but the merged operator's input contract covers it; mirrors the in-range W=1 arrival at half scale) |
| constants through the absolute-mass layer (rev-7 finding 3) | merge-layer unit: W=2, `A=5e-324`, x=y=`1.3e308` constant → mean EXACTLY `1.3e308`, var 0, corr NULL (equality fast paths skip every deviation term; merge-level constancy survives the new layer at any magnitude) |
| retained magnitude, low-weight batch (rev-7 finding 4a) | keys `[1,2,2]` α=.01 adjF `x=y=[0,−2e154,2e154]` → mean 0, merged var/cov `7.920792079207922e306`, corr 1 (rev-6 fault: Inf/NaN — the batch's true `4e308` saturated before the `0.02` mass weighting) |
| retained magnitude, overflow-then-decay recovery (rev-7 finding 4b) | tied `[−2e154,2e154]` at t=0 h=1 adjF then `0` at t=10: rows 1–2 emit honest +Inf var / corr NULL (state keeps `4e308` extended), final row recovers var/cov `3.90625e305`, corr 1 (rev-6 fault: the stored Inf stayed Inf forever) |
| anchored batch scatter, 6 permutations (rev-8 finding 1) | tied batch `[1.7e308,1.7e308,−1.7e308]` in **all six peer orders** → finite mean `5.666666666666667e307` (independent 900-digit expectation), honest +Inf var/cov with **finite std `1.6027753706895077e308`** (sqrt of the extended scatter — var/std verdicts differ, rev-8 finding 4), corr NULL (rev-7 fault: the raw `−2.2667e308` deviation overflowed, the −Inf operands reached the extended normalization and the engine **hangs** — 6/6 permutations, watchdog-evidenced) |
| anchored batch scatter, low-weight arrival (rev-8 finding 1) | keys `[0,8e-16,8e-16,8e-16]` h=1e308 adjF `x=y=[0,1.7e308,1.7e308,−1.7e308]` → final var/cov `1.4278497164812023e293`, corr 1, mean `2.7997053264337305e-16` (rev-7 fault: the simulator never returned) |
| exact-scaling gate, incoming side (rev-8 finding 2) | keys `[0,2e-15]` h=1e308 adjF `x=y=[0,1e150]` → mean `1.4821969375237395e-173`, var/cov `1.4821969375237397e-23`, corr 1 — the arrival mass is exactly three smallest subnormals and the raw-mass share route preserves it (rev-7 fault: ldexp ties `A = 3·2^-1074` to `2·2^-1074`, committing the 33%-high `1.976262583364986e-173` / `1.976262583364986e-23`) |
| exact-scaling gate, retained side (rev-8 finding 2) | keys `[0,0,0,1074,1074]` h=1 adjT `x=y=[1e4,1e4,1e4,0,0]` — the three-observation first batch (W=3) decays one 1074-half-life to the odd-subnormal `W = 3·2^-1074` under arrival `A = 2` → mean `7.4109847e-320`, var/cov `7.4109847e-316`, corr 1 (rev-7 fault: the scaled `w1 = 1·2^-1074` committed the 33%-high `9.8813e-320` / `9.8813129e-316`; the share reconstruction fails `s1·nsum == W` and rebinds to the absolute-mass layer with the mass exponent preserved) |
| compensated accumulation, cross moments (rev-8 finding 3) | tied pair batch `[(1e200,1e200),(1e150,1e150),(1e200,−1e200),(−1e200,1e200),(−1e150,−1e150),(−1e200,−1e200)]` → covariance `3.333333333333333e299` in the review's both orderings AND **bit-identical across all 720 peer permutations** (mean exactly 0 every time; honest +Inf var; finite std `8.16496580927726e199`; corr NULL) (rev-7 fault: the uncompensated extended fold returned cov **0.0** in the small-pairs-early order — a peer-order-dependent result violating Invariant 7) |
| ewmStd from the retained moment (rev-8 finding 4) | the existing retained-4e308 vector (tied `[−2e154,2e154]` then `0` ten half-lives later): the +Inf-variance rows emit std **EXACTLY `2e154`** and the recovered row emits var `3.90625e305` with std `6.25e152` (rev-7 fault: std = sqrt(narrowed Inf) = Inf at the +Inf-variance rows) |
| extended-alignment information limit (rev-7 finding 4; corrected rev 8; rebuilt rev 11) | every fold's two-sum is EXACT at every representable gap: subnormal-lattice alignments are computed in a scaled normal-mantissa domain (gaps [1022, 2043]) and wider gaps cascade the smaller operand INTACT — no quantization happens before a two-sum, superseding both rev 7's ~1020-order and rev 8's ~1074-order "lost term" figures (0.75 aligned against 2^1073 returns 0.75, not 1.0); a weighted product below `5e-324` still rounds to 0.0 — the honest emission limit; asserted with the honest-limit rows above, the 720-permutation row and the r11F3 rows |

## 6. Frame, partition, and tie semantics

- **Default (and only) frame:** the SQL default cumulative frame implied by
  `OVER (… ORDER BY …)`. `ewm*` is *defined* on the partition prefix.
  Explicit frame clauses are bind-time errors (§7). Bounded moving frames
  cannot be supported by any exponential smoothing state; re-scanning each
  frame would be O(n·w) — the exact regression the feature exists to avoid.
- **Partitions:** state resets at each partition (Invariant 8). Equal
  partitions with equal values produce identical output vectors, under any
  internal row order.
- **Ties:** per §5 — peer batches carry total masses with uniform weights;
  batch-local statistics are compensated and order-free; all group rows
  share the group-post value; NULL bits may differ per row only under
  event-time ignoreNA=true.
- **Event-time instants:** with exactly one ORDER BY key (§4/§7), rows with
  equal key values are the same instant *and* the same SQL peer group —
  `ctr.os` boundaries are instant boundaries. NULL-valued rows still
  advance the timeline exactly once per instant. A NULL *key value* is an
  execution error (§7). In row-decay mode there is no time axis; any key
  count and direction are accepted and peers are full-tuple peers.

## 7. Error and unhappy-path catalog

| Condition | Error (bound by whom) |
|---|---|
| Name used without `OVER` — the issue's probe | parses (opt-window production, `approx_percentile` precedent); binder: `Window function 'ewmMean' requires an OVER clause` (`window_binder.go:1065`) |
| `ewmMean()` zero arguments | parser 1064 (non-empty `expression_list`) |
| **Missing decay argument** (`ewmMean(v) OVER (ORDER BY …)`) | binder: `invalid argument: ewm statistics require a declared decay (alpha or 'halflife' half-life)` (rev-4; §4 table) |
| Wrong arity / misplaced argument per the §4 signature table | registry checkpoint failure (checkFn) surfacing as §4's bind error family |
| `'halflife'` tag misused (no `h`, `h ≤ 0`, non-numeric `h`, tag in wrong position) | binder: `invalid argument: ewm halflife decay requires a strictly positive half-life constant` |
| `OVER` without `ORDER BY` | binder: `ewm statistics require an ORDER BY in the OVER clause` |
| Explicit frame clause | binder: `ewm statistics do not support explicit window frames` |
| Event-time OVER with more than one ORDER BY expression | binder: `invalid argument: event-time ewm statistics require exactly one ORDER BY expression` |
| Event-time OVER whose ORDER BY is DESC | binder: `invalid argument: event-time ewm statistics require ascending ORDER BY` |
| Event-time ORDER BY type (non-numeric non-temporal single key) | binder: `event-time ewm statistics require a numeric or temporal ORDER BY key` |
| Non-constant policy/decay argument (column, correlated query) | binder: `ewm statistics require constant decay and policy arguments` |
| α ≤ 0 or > 1 (literal at bind; resolved value at execution) | binder / execution-boundary: `invalid argument: ewm statistics require alpha in (0,1] (resolved …)` |
| **Non-finite resolved policy values** (α or h = NaN / ±Inf at execution; literal at bind) | binder / execution-boundary: the same invalid-argument families (α in (0,1]; strictly positive finite half-life) — NaN/±Inf are never silently saturated (rev-5 finding 6) |
| Non-numeric `x`/`y` | registry overload-check failure (bind) |
| **Non-finite SAMPLE VALUE (x or y = NaN / ±Inf after the §4 cast — direct or composed: e.g. an infinite variance/covariance OUTPUT of these operators selected as a later call's sample)** | execution: `invalid input: ewm sample values must be finite` — raised by the operator's row pre-scan BEFORE any peer group is processed (initial, tied or later samples, either stream): nothing is merged, no state is published, later statements are unaffected (clean reuse; rev-20 Invariant 39 / Decision 63). Non-finite RESULT emissions of finite inputs (an honest +Inf variance with a finite std, corr NULL) remain §5 outputs and are never sample inputs; the internal extended-normalization assertion stays a true internal invariant behind the public gate |
| Prepared α/`h`/policy executes with an out-of-range value (e.g. α=3) | **execution-boundary error** from per-execution config validation; the statement's plan stays reusable — next valid EXECUTE works (Invariant 12; cache exclusion §9) |
| Event-time execution sees NULL key | execution: invalid input (the single ASC ORDER BY value is the time axis) |
| **Event-time execution sees a non-finite DOUBLE key** (NaN / +Inf / −Inf; literal at bind) | binder / execution: invalid input — event-time keys must be finite; the finite-extreme `Δt` is computed by the §5 quotient rule, overflow of the raw subtraction is *not* an error (rev-5 finding 6) |
| DECIMAL key-gap subtraction overflowing the widest decimal domain | execution: invalid-input error (never wrapping; §5 domain table) |
| Zero arrival mass / subnormal arrival mass / decay-mass underflow | *not errors*: defined states (§5); outputs are C-gated or carried |
| Runtime NULL-by-policy states — *not errors*: zero-variance corr | NULL |
| Runtime NULL-by-policy state (rev-6, rev-7): a variance whose true magnitude still exceeds float64 **at emission** | corr NULL (documented float64 limit; §5 Outputs) — never NaN; the state may still hold the retained-magnitude form that a later merge can scale back into range |
| Results beyond the float64 range (true mean/variance beyond ±1.8e308; weighted products below `5e-324`) | *not errors*: honest IEEE values **at emission** — ±Inf by sign (non-negative variances carry a non-negative mantissa; means computed convexly never overflow while representable), sub-resolution products zero — like ordinary float SQL functions (§5's honest-limits clause) |
| Operator lifecycle events mid-generation (downstream LIMIT stops consuming, pipeline cancellation, an execution error later reused) | *not SQL error rows*: EWM state, cursor and backfill follow the container ownership contract of §9 (reset/freed at every generation boundary, partition boundary, `Window.Reset` and `Window.Free`); pipeline **cancellation is honored inside group processing** at §9's bounded in-loop interval (≤1024 rows/steps) — no partial group is published (the exactly-once cursor stays behind the cancelled group) and a reused container starts fresh |
| Fewer than `max(1,minPeriods)` observations (C-gate) | NULL |
| No observation ever (all-NULL partition, C=0) | NULL everywhere |
| Empty partition (0 rows) | no rows (framework behavior) |

## 8. Standards, precedent, alternatives considered

- **pandas ewm / DolphinDB ewmMean family** — the policy vocabulary
  (alpha/com/span/halflife, adjust, ignore_na, min_periods) the issue names.
  MatrixOne matches pandas exactly for adjust=true weighting semantics and
  for the gap-free adjust=false recurrence (the issue's verified profile),
  and pandas likewise computes normalized weighted averages, so the gap
  comparison does not turn on normalization. The operative distinction is
  continuation after a gap: for [1, NULL, 3, 4], alpha=0.5, adjust=false,
  ignoreNA=false this design's retained-mass continuation emits the four
  rows `1, 1, 7/3` (`2.3333333333333335`) and `23/7`
  (`3.2857142857142856`) — corpus cases `p3_continuation_adjF/adjT` —
  while the per-observation recursive continuation used by pandas under
  the same flags reaches `19/6` (`3.1666666666666665`) at the final row;
  constant signals stay exactly constant through gaps under this design's
  retention. Documented divergences: per-batch totals under ties
  (batch-atomic rather than per-row weights —
  a tied batch shares one arrival mass; rev-3/4); ignoreNA=true as an
  *observation-batch* clock (identical to ignoreNA=false whenever every
  row is observed — Invariant 15; pandas positions are per-row, which is
  only observable for ties). DolphinDB interop: no wire-level contract;
  the issue disclaims DolphinDB-runtime parity.
- **ClickHouse `exponentialTimeDecayedAvg`** — confirms per-event-time decay
  as a native-engine feature; singleton instants follow the same recurrence
  family, generalized to all five statistics.
- **Alternative A — aggregate machinery** — rejected (rev 1:
  commutative-merge assumption breaks EWM).
- **Alternative B — dedicated window-operator path (chosen)** with the
  rev-4 normalized batch-merge state (§5).
- **Alternative C — self-join emulation** — rejected as the product answer;
  retained as the BVT cross-check oracle for the one profile it can express.
- **Alternative D — 20 registry spellings** — rejected for v1 (conversions
  expressible as SQL constants; `'halflife'`-as-event-time is native).
- **Alternative E — named-argument grammar** — rejected (parser surface).
- **Alternative F — extend `function_call_generic` with `window_spec_opt`**
  — rejected (reproduced evidence: `abs(1) OVER (…)` is a parse error
  today; the extension would change every existing function call's
  observable errors).
- **Alternative G — event-time via separate function names** — rejected
  (the `'halflife'` tag keeps the five-name surface and one grammar form).
- **Alternative H — per-instant per-observation masses in event-time
  (chosen, rev 3)** — an instant's total arrival mass is shared uniformly
  by its observations; the batch's mean, scatter and cross-scatter are
  retained by the batch-local statistics (rev-3 finding-3 fix; rev 4 keeps
  this and notes the uniform weights also make the batch statistics
  unweighted and compensated, rev-4 finding 3).
- **Alternative I — per-observation first-sample initialization** —
  rejected (rev-3 finding 1; order-dependent under the unstable sorter).
- **Alternative J — direction-aware elapsed time under DESC** — rejected
  (rev-3 finding 5; bind-time ASC rule instead).
- **Alternative K — multi-key event-time instants by sorting every order
  vector** — rejected (rev-3 finding 4; single-key rule instead).
- **Alternative L — absolute-moment state (`W, Mxx, Myy, Mxy` all
  mass-scaled)** — rejected in rev 4 (finding 4): the absolute moments
  underflow independently of `W` while the normalized ratios remain
  live; recorded vector: variance `2.222222222222222e-201` collapsing to 0
  and correlation 1 to NULL with `W ≈ 4.58e-151` nonzero. The normalized
  representation (§5) also absorbs the rev-3 snapshot field.
- **Alternative M — naive float64 batch sums with byte-identical
  permutation promises** — rejected in rev 4 (finding 3): naive sums are
  order-dependent under cancellation (`[1e16,1,−1e16]` → 0 vs 1/3) and
  constant huge batches produce nonzero variance; the honest contract is
  compensated unweighted batch statistics + an identity fast path, exact
  for constant batches, bounded otherwise, tested unrounded.
- **Alternative N — operator-runtime policy evaluation (evaluate the decay
  expressions inside the window operator at each execution)** — rejected
  (rev-4 finding 1 resolution): runtime re-evaluation in the operator would
  duplicate binder/config semantics in the executor and skirt the
  repository's established pattern; the chosen mechanism is the percentile
  cache-exclusion (compile-time construction, per-execution because the
  statement recompiles).
- **Alternative O — f-form convex merge (`f = W/(W+A)`;
  `x' = f·x + (1−f)·y`)** — rejected in rev 5 (findings 4/5 follow-up):
  it is scale-free for comparable masses, but not bit-identical to the
  mass form (the pinned gap-matrix 2.6 and agreement row 10.0 drift to
  `2.6000000000000005` / `10.000000000000002`), and for tiny-but-
  significant arrival shares `1 − f` rounds to 1 so the new observation's
  share disappears entirely — `dt=1`, `h=1e17` reports mean 0 where
  `0.6931471805599453` (the preserved rev-2 precision row) is correct;
  both verified on an intermediate attempt this revision. The
  power-of-two-normalized mass form (§5) matches the f-form's scale
  freedom while keeping bit-identity in the normal range.
- **Alternative P — a plain Go config struct through the aggregate-config
  channel (`AggFuncExecExpression.cfg` → `SetExtraInformation`)** —
  rejected in rev 5 (finding 3): the real channel carries **bytes**
  (`extraConfig []byte` + `configType`, `aggexec/types.go:37-50`;
  `MakeAggFunctionExpression(id, isDistinct, args, config []byte,
  configType...)`), the percentile precedent passes encoded bytes
  (`EncodeApproxPercentileConfig`), and `SetExtraInformation` delivery
  happens only inside `newAggregateExecutor` (`window.go:562-566`) — a
  code path the EWM dispatch expressly bypasses. The chosen channel is
  percentile-shaped from construction to consumption: encode once at
  `constructEwmConfig`, decode once in the EWM path (which reads its own
  expression's bytes via `GetExtraConfig()`), bytes immutable and shared
  across operator copies.
- **Alternative Q — the rev-4 merge weights (multiply normalized moments
  by the absolute masses; products that round to zero declared
  negligible)** — superseded in rev 5 (finding 4): comparable masses are
  never negligible relative to *each other* (`W = A = 5e-324` requires
  var `.125`, not 0), and the absolute products overflow at the double
  limit where the result is representable (`1.3e308` constants).
  Kept as the recorded supersedence with its fault vectors (§5, §12).
- **Alternative R — the rev-5 scaled-weight negligibility rule ("a
  weight that still rounds to zero after normalization is a genuinely
  negligible relative share")** — withdrawn in rev 6 (finding 2): the
  information of a mass contribution lives in the weight-times-value
  *product*, never in the weight alone — the subnormal arrival
  `A = 5e-324` scales to `w2 = 0` while its contribution requires mean
  `4.94e-174` / var `4.94e-24` / corr 1 (keys
  `[0,8e-16]`, h=1e308, x=y=`[0,1e150]`); the retained-side asymmetry
  drops an aged state's mean to 0 (keys `[0,1074]`, h=1). The chosen
  rule: a contribution is negligible exactly when its weighted *product*
  is below float64 resolution; extreme mass ratios take the
  absolute-mass layer (§5), which preserves every representable
  contribution (the rev-6 "direct-shares" form this used to point to is
  itself withdrawn — Alternative W).
- **Alternative S — one unguarded float64 expression for the whole merge
  domain** — rejected in rev 6 (findings 1 and 3): no single ordinary
  float64 expression is range-safe across the supported input domain
  (the scaled mass form itself overflows on `[1.6e308, 1.6e308,
  1.7e308]` and `[0, 2e154]`, and its scaling can zero a subnormal
  weight; the plain Neumaier sums NaN on `[1e308, 1.1e308]` and
  `[−1e154, 1e154]`). The Chosen design keeps the bit-identical primary
  shapes and adds *detected* fallbacks (extreme-ratio shares;
  overflow-safe convex shares; midpoint-anchored mean; per-term
  regrouped scatter accumulation; — rev 7 — the gated absolute-mass
  layer with retained-magnitude extended forms; and — rev 8 —
  anchored deviations for overflowing raw-deviation lists, the
  exact-scaling and share-reconstruction gates, and the compensated
  extended accumulation) — detection is exact
  (finite inputs → non-finite result, a zero-rounded positive weight,
  a dropped positive-mass side, or an out-of-range moment), so no
  *finite* committed value changes at all while every representable
  result becomes computable; the single exception is recorded in §12
  (the one preserved row whose non-finite share-layer result enters
  (C), a 1-ULP mean shift inside its pinned tolerance).
- **Alternative T — recomputing EWM output slices from scratch per emit
  chunk (no cross-call cursor)** — rejected in rev 6 (finding 5): the
  operator's publish loop calls `processFuncRange(start, end)` per
  `DefaultBatchSize` slice and returns to receive
  (`window.go:414-436`); recomputation would re-merge peer groups
  (double-counting statistics and breaking constant/exactly-once
  semantics) or mis-handle groups straddling slice boundaries. The
  chosen design is the monotonic peer-group cursor with an in-flight
  group cache (§9's lifecycle), mirroring the existing
  `runningNextRow`/`runningPeerEnd` resumable-cursor precedent.
- **Alternative U — the shifted running-mean batch recurrence**
  `m := x_1; m += (x_i − m)/i` — withdrawn in rev 7 (finding 1): it
  was accepted in rev 6 with the claim that a convex update of
  representable values "cannot overflow while the mean is
  representable"; that is false because the *subtraction* `x_i − m`
  overflows first — the tied batch `[1e308, 1.1e308, −1e308]` returned
  mean **−Inf** for the two same-sign-first orders (only 4 of 6
  permutations correct), reproduced on the archived rev-6 simulator.
  Replaced by the midpoint-anchored accumulation (§5): no subtraction
  in it can exceed the half batch range, which is representable.
- **Alternative V — saturating an out-of-range intermediate moment to
  ±Inf (the rev-6 state/batch overflow pin)** — withdrawn in rev 7
  (finding 4): the sign pin was honest for *emission* but it destroyed
  information the merge still needed — a batch variance of `4e308`
  saturated before an arrival mass of `0.02` could scale it back to
  the representable `7.92e306` (rev-6 result: Inf var, NaN cov), and a
  stored state variance of `4e308` stayed Inf forever instead of
  recovering `3.90625e305` after ten half-lives. Replaced by the
  extended (mantissa, exponent) retained-magnitude form through state
  and merge, with the sign-pinned honest ±Inf applied **only at
  emission**.
- **Alternative W — materialized merge shares (`s1 = W/(W+A)`,
  `s2 = A/(W+A)`) and the claim that they "are exactly representable
  down to the smallest subnormal, so a representable contribution is
  never discarded"** — withdrawn in rev 7 (finding 3): the *share*
  itself can round below the smallest subnormal to zero — retained
  `W = 2^-1074` meeting a tied arrival `A = 2` gives
  `W/(W+A) = half the smallest subnormal → ties-to-even → 0`, and the
  merge silently dropped the old state (mean/var/cov 0, corr NULL,
  returned where `2.4703e-320` / `2.47032823e-316` / 1 are required).
  Replaced by absolute-mass means `(W·μ + A·m̄)/(W+A)` and the
  weighted-difference identities (§5 (C)), in which no share is ever
  evaluated; the share layers survive only as gated float64
  fast-paths that commit when finite and side-preserving.
- **Alternative X — bounding the EWM cancellation latency by the
  Call/slice contract alone ("per-call work is bounded by the slice
  plus the groups it must complete")** — withdrawn in rev 7 (finding
  6 of record / finding 5 of the sixth review): a peer group can
  contain the entire partition, so whole-group scans and statistical
  passes run unbounded between Call iterations; rev 6 also mis-cited
  the cumulative precedent (it polls in-loop through `checkCanceled`,
  `window.go:57` with `cancellationCheckInterval = 1024`, called at
  `:989`/`:1031`). Replaced by the in-loop bounded polling contract
  (§9): the same helper, the same interval, inside every EWM scan,
  statistical pass and recomputation/emit loop, with per-group atomic
  publication preserved.
- **Alternative Y — raw mean-deviations extended to the fallthrough
  layers** (the rev-7 batch formed `d_i := x_i − m̄x` unconditionally,
  even where a deviation overflows while the mean is finite) —
  withdrawn in rev 8 (finding 1): the −Inf operand reaches the
  extended multiplication and `xfrexp`'s normalization never
  terminates on an infinite mantissa — the engine **hangs**
  (`[1.7e308,1.7e308,−1.7e308]`: 6/6 permutations; watchdog
  evidence). Replaced by the finiteness gate on the raw deviation
  lists and the anchored-deviation identity (§5; Decision 30), with
  the extended normalization contract-checked (a non-finite operand
  is a defined internal error, not a loop).
- **Alternative Z — "the power-of-two scaling is exact"** (the rev-7
  trust in `ldexp` beyond the zero-rounded-weight check) — withdrawn
  in rev 8 (finding 2): `ldexp` ties-to-even on the subnormal lattice
  and reweights by whole subnormal units (arrival `A = 3·2^-1074`
  scales to `2·2^-1074`), committing a silent 33% reweight. Replaced
  by the bit-exact round-trip gate, raw-mass share construction, and
  the subnormal share-reconstruction gate (§5; Decision 31).
- **Alternative AA — the plain (uncompensated) extended accumulation
  fold** — withdrawn in rev 8 (finding 3): an aligned add is a
  rounding at the running-sum scale, so cancelling signed terms
  below its half-ULP window vanish even though their exponents were
  retained (`[(H,H),(s,s),(H,−H),(−H,H),(−s,−s),(−H,−H)]`,
  H=1e200, s=1e150: cov 0.0 in one peer order and
  `3.333333333333333e299` in another) — the peer-permutation contract
  (Invariant 7) breaks. Replaced by the compensated extended
  accumulation (§5; Decision 32); the rev-7 "~1020 binary orders"
  information-limit claim is superseded by the ~1074-order
  subnormal lattice of the running partial sum.
- **Alternative AB — `ewm_std = sqrt(f64(vxx))`** (the square root of
  the already narrowed float64 variance) — withdrawn in rev 8
  (finding 4): `sqrt` consumes the *emitted* value, so an honest +Inf
  variance drags a perfectly representable standard deviation (the
  retained `4e308` with std `2e154`) to Inf. Replaced by the
  retained-moment sqrt `f64_apply(√, vxx_retained)` (§5 Outputs;
  Decision 33): variance and standard deviation may reach different
  honest verdicts on the same row.
- **Alternative AC — the rounded share-reconstruction certificate**
  (`s·denominator == mass` decides a subnormal share) — withdrawn in
  rev 9 (eighth-review finding 1): the certificate's own
  multiplication ROUNDS, so a 25%-understated share still
  "reconstructs" its mass (retained `W = 2^-1074` under arrival
  `A = 0.75` committed mean `4.940656458412465e-124` where
  `6.587541944549954e-124` is required on the same masses).
  Replaced by the never-commit rule (§5 (F) G3; Decision 34): every
  subnormal share rebinds to (C).
- **Alternative AD — a single residual register per accumulator**
  (fold every two-sum residual into one extended register by plain
  `xadd`) — withdrawn in rev 9 (eighth-review finding 2): cross-scale
  residuals only 800 binary orders apart are swallowed at the
  running register (the three-scale tied batch returned a covariance
  of exactly 0.0 in one peer order while its true L²/5 is
  representable). Replaced by the cascading registers (§5; Decision
  35), which keep the level-0 loop bit-identical and hold every
  discarded bit.
- **Alternative AE — centering the scatter on the rounded float64
  mean** (batch and merge moments measured around `fl(ν)`) —
  withdrawn in rev 9 (eighth-review finding 3): at the magnitude
  boundary the narrowed mean understates the centering and doubles
  the scatter (`[2^53, 2^53+2]` → var/cov/std `2/2/√2` where the
  exact population values are `1/1/1`, while the `[0,2]` translation
  already returned 1 — Invariant 5 violated). Replaced by the
  carried-tail δ² recentering (§5; Decision 36).
- **Alternative AF — non-finite-only range gates with a narrowed
  std** (recompute extended moments only when a result is
  non-finite; take `sqrt` of the stored float) — withdrawn in rev 9
  (eighth-review finding 4): subnormal-lattice roundings destroy
  REPRESENTABLE moments with no gate witnessing the loss (tied
  `[−1e-200, 1e-200]` emitted std exactly 0.0 while 1e-200 is
  representable; singleton merges `[−2^-537, +2^-537]` emitted
  Replaced by the lattice rebinds and the extended store (§5;
  Decision 37).
- **Alternative AG — hi-only merge deviations with a δ² end
  correction** (center the deviations on the narrowed hi parts and
  subtract the merged-tail square afterwards; fire the equality fast
  path on the hi parts alone) — withdrawn in rev 10 (ninth-review
  finding 1): the end correction has no cross term — centering on
  `fl(ν)` while the sources carry their own tails double-counts the
  tail mass ([2^53, 2^53+2, 2^53+4] committed 4.25 where the exact
  merge is 2.75; the hi-only equality shortcut read 0.5 for
  [2^53, 2^53+2, 2^53] where 0.75 is exact; the equal-hi/
  different-tails continuation committed 3.0 instead of 29/9).
  Replaced by the complete-mean deviation algebra (§5; Decision 38;
  Invariant 21).
- **Alternative AH — one-direction alignment folding with a
  six-register cap** (drop the old accumulator when a larger
  incoming term's alignment underflows; cap cascades at six
  registers) — withdrawn in rev 10 (ninth-review finding 2): the
  larger-incoming branch read zero two-sum residuals and silently
  discarded the old accumulator (unit-rows-first
  [(1,1),(−1,−1),(H,H),(−H,−H),(H,−H),(−H,H)] at H = 2^600 returned
  a covariance of exactly 0.0 while 1/3 is exact), and six registers
  fail within a 700-bit span (the 30-row ten-scale batch returned
  0.0 where 2^500/15 is exact). Replaced by the symmetric alignment
  guard and the domain-justified capacity (§5; Decision 39;
  Invariant 23).
- **Alternative AI — unweighted-exponent retention gates** (decide
  lattice rebinds from a moment's own exponent) — withdrawn in rev
  10 (ninth-review finding 3): a normal moment whose WEIGHTED product
  underflows still passed the gate (keys [0,0,1000] h=1 adjF: the
  variance 2^-100 passed, the decayed mass w1 ≈ 2^-1000 multiplied it
  to ≈2^-1100 and rounded to zero BEFORE storage, and ewmStd emitted
  0 where ≈2^-550 is representable). Replaced by the weighted-product
  gates (§5; Decision 40; Invariant 19).
- **Alternative AJ — narrowing correlation moments before
  normalization** (compute corr from the emitted float64 variances
  and covariance) — withdrawn in rev 10 (ninth-review finding 4):
  subnormal quantization of the numerator and the geometric
  denominator emitted ±2.0 on perfectly correlated streams
  [(±2^-537, ±1.5·2^-537)] where the retained bits say exactly ±1.
  Replaced by the retained-moment normalization (§5; Decision 41;
  Invariant 22).
- **Alternative AK — one-word (53-bit) complete means in the
  absolute-mass layer** (fold the merged-mean numerator by a single
  extended addition, divide once, and crush each source `(hi + lo)`
  pair to one word) — withdrawn in rev 11 (tenth-review finding 1):
  the single extended word still has a 53-bit mantissa, so a 54-bit
  merged center lost its tail — [2^600, 2^600+2^548] measured its
  arrival deviation as 2^548 and emitted std 2^547.5 where 2^547 is
  exact, and the mixed-scale [2^53,+2,+4] × 2^-260 continuation
  emitted cov 1.8891618714269098e-78 / corr 0.9949366763261822.
  Replaced by the double-double mean machinery (§5; Decision 42;
  Invariant 21 rebuilt).
- **Alternative AL — moment-only retention gates** (rebind on the
  weighted moment products alone and test deviations against an
  unweighted threshold) — withdrawn in rev 11 (tenth-review finding
  2): with both source moments zero the weighted deviation products
  are the merged moment's entire content and underflowed unwitnessed
  (std 0.0 where the engine's own rounded masses make
  2.4099198651029665e-181 / 6.567697842037889e-181 exact). Replaced
  by the six weighted-deviation tuples per branch (§5; Decision 43;
  Invariant 25).
- **Alternative AM — in-place ldexp alignment with post-hoc
  residuals** (align the smaller operand by ldexp and record only
  what the two-sum returns) — withdrawn in rev 11 (tenth-review
  finding 3): on the subnormal mantissa lattice the alignment
  quantizes away real bits BEFORE the two-sum (0.75 against 2^1073
  returned 1.0; the H=2^537 batch drifted to 0.6666666666666666).
  Replaced by the scaled-domain exact two-sum with intact-operand
  cascades (§5; Decision 44; Invariant 23 rebuilt).
- **Alternative AN — single-compensation float chains** (one Neumaier
  register per chain) — withdrawn in rev 11 (tenth-review finding 4):
  finite cross-scale residuals were clobbered INSIDE the register
  itself (cov 0.0 and mean 0.0 in the review orders where 0.2 is
  exact), while a non-finite residual would have spun the cascaded
  loop forever — both closed by the non-finite fold. Replaced by the
  cascaded (sum, comp, comp_lo) chains (§5; Decision 45;
  Invariant 24).
- **Alternative AO — one-rounded complete-mean products** (xmul
  feeding ddsum and the dddiv remainder against a rounded `q_hi·T`)
  — withdrawn in rev 12 (eleventh-review finding 1): the products'
  tails died before any compensation acted (dddiv(1,3) returned a
  zero tail word; the singleton centers [2^600, 2^600+2^548]
  emitted std 5.150657059589176e+164 / 5.319575737017211e+164 where
  √15/8·2^548 and √2/3·2^548 are exact, the translated controls
  already right). Replaced by the error-free products with the
  exact quotient remainder (§5; Decision 46; Invariant 21 rebuilt).
- **Alternative AP — countdown-capped refeed restoration** (loop the
  restore refolds up to a fixed large count, then flush) — withdrawn
  while building rev 12: an item refed intact re-folds identical
  content whenever the running total cannot change, so the countdown
  saturated repeatedly on the PRESERVED 720-peer-permutation rows
  (each restore ground through the full cap before its xadd flush,
  turning a bounded reconstruction into a pathological scan) and the
  flush dropped exactly the content the counter was protecting.
  Replaced by the structural deferred/progress rule (a pass that
  changes nothing can never host a held item) (§5; Decision 47;
  Invariant 24 extended).
- **Alternative AQ — clamping a negative retained variance at
  emission** (emit 0.0/std 0.0 and carry on) — rejected in rev 12
  (the eleventh review's finding 3 explicitly forbids suppressing
  the failure with output clamping): a negative retained variance
  is an internal arithmetic-invariant violation, never a
  representability boundary case; §7's honest-error shape keeps
  raising. Not adopted (§5; Decision 48; Invariant 26).
- **Alternative AR — commit the narrowed high word alone** (store
  and emit fl(ν_hi) of each merged complete mean and let the tail
  stay a carry) — withdrawn in rev 13 (twelfth-review finding 1): a
  merged high word that cancels to ZERO drops the tail's real
  content — keys [0,0,1] x=y=[2^53, 2^53+2, −2^53] committed 0.0
  with the stored tail holding the exact 0.5, and the (C) singletons
  [2^600, −fl(5/3)·2^600] emitted 0.0 over dddiv's exact −2^545
  remainder. Replaced by the canonical complete-mean normalization at
  every commit/emission layer (§5; Decision 49; Invariant 27).
- **Alternative AS — float64 k·m̄x batch tail across the whole range**
  (materialize the batch residual through the float64 two-product
  only) — withdrawn in rev 13 (twelfth-review finding 2): a first
  tied batch [M, 0, 0] at M = maxfloat64 has finite Σx, finite mean
  and std, but fl(3·fl(M/3)) overflows — rev 12 returned (inf, 0.0)
  and the merge raised the non-finite normalization AssertionError.
  Replaced by the retained pair residual committed whenever the
  float64 path cannot carry it (§5; Decision 50; Invariant 28).
- **Alternative AT — narrow the below-lattice tail to float64 at
  commit** (quantize to keep every state tail float-typed) —
  withdrawn in rev 13 (twelfth-review finding 3): the first tied
  batch [0, 2u, 6u] at u = 2^-1074 has the exact tail −u/3, which
  quantized to 0.0 and lost the centering (retained variance
  (19/3)u² instead of (56/9)u², std 3u instead of 2u). Replaced by
  the retained pair tail with pair-native merge routing (§5;
  Decision 51; Invariant 28).
- **Alternative AU — normalize the raw W == 0 batch state commit**
  (extend the complete-mean fold into the first-batch / re-anchor
  initialization) — withdrawn in rev 15 (fourteenth-review finding
  1): rev 14 justified it with the raw tail's reconstruction fold
  loss — (cx + cxl) collapsing in float64, the review-order
  [H=2^400, M=2^200, 1, −H, −M] batch reading the "residual" at
  −0.2 against its mean 0.2 — and treated that loss as a tolerated
  pin. The loss was live corruption: the committed pair fed every
  continuation merge 0.0 where fl(1/7)/fl(0.1) (initial state) and
  fl(2/11)/fl(1/6) (incoming state) are exact, while the
  [H,−H,M,−M,1] reorder read the true residual — the order
  dependence itself breaching the batch-order contract. The
  repair rebuilds the residual from all registers (Decision 55;
  Invariant 31), removing the premise; re-deriving raw words at
  commit stays rejected (the pinned narrowed words are rev-9
  finding-3 pins and the merge layers own the folds).
- **Alternative AV — form the mixed retained/scalar cross-centering
  by multiplying the tails as scalars** (route every sxy − τx·τy
  through the float64 product) — withdrawn in rev 14
  (thirteenth-review finding 2): with the x tail retained (a first
  tied batch [M,0,0] at M = maxfloat64), a finite scalar sxy and a
  scalar y tail (y = [1+2^-52, 1, 1]), the route raised TypeError
  under BOTH adjust policies and on the swapped stream — and even a
  successful multiplication could not carry the exact content: a
  raw float64 difference rounds an M-scale deviation by a half ulp
  (9.98e291), more than the entire tail correction at the division
  scale. Replaced by the exact rebuild chain (§5; Decision 53;
  Invariant 30; r14F2).

## 9. Architecture and data flow (chosen design)

### Lexer and parser (`pkg/sql/parsers/dialect/mysql`)

Unchanged from rev 1–3 (verified this base): 5 `keywords.go` entries
(`"ewmmean"|"ewmvar"|"ewmstd"|"ewmcov"|"ewmcorr"` → `EWM_MEAN|…`), 5 `%token`
declarations beside `RANK…NTILE`, 5 productions
`EWM_XX '(' expression_list ')' window_spec_opt` (the
`approx_percentile`/`group_concat` opt-window precedent), the same 5 tokens
appended to the `not_keyword` identifier-compat list (BVT-guarded), goyacc
regeneration with zero-new-conflicts discipline. The string-literal
`'halflife'` tag needs no grammar change (`expression_list` already accepts
string constants); disambiguation is by fold-time type at the decay
position.

### Registration (`pkg/sql/plan/function`)

5 IDs (window region), names lower-case (case-insensitive resolution
verified — `NewCStr($1,1)` lowercasing), `list_window.go` `FuncNew` entries
with `class: plan.Function_WIN_VALUE`, and overload `checkFn`s enforcing the
**§4 signature table** (arity bounds per form, numeric value args cast to
float64, decay position numeric-α-or-string-tag with its `h`, policy
positions boolean/integer-foldable), plus a
`GetFunctionIsWinEwmFunByName` helper used by binder validations,
cache-exclusion detection and window-operator dispatch. checkFns run at
bind on folded literal types; prepared markers pass through (below).

### Binder validation (`pkg/sql/plan/window_binder.go`, `base_binder.go`)

Missing OVER, ORDER BY required, explicit-frame rejection, the **§4
signature-table rules** (missing decay argument, decay-position shape,
arity, literal value validation α ∈ (0,1] / h > 0 / minPeriods ≥ 0 /
booleans), event-time ORDER BY type rule, and the event-time
single-ORDER-BY-expression and ASC-direction rules — all as §7 assigns
them. Prepared markers are accepted and stay unresolved in the plan; the
plan proto is untouched end-to-end (rev-1's `AggConfig` bytes were dropped
in rev 2 and stay dropped).

### Per-execution configuration and cache ownership (`pkg/sql/compile`,
`pkg/sql/plan/utils.go`, `pkg/frontend`) — rev-4 finding 1, rev-5 findings 2/3

The lifecycle is the repository's percentile pattern, adapted completely
(verified in source this revision, including the two *separate* caches and
the byte-shaped config channel):

- `constructWindow` (`operator.go:1899`) builds operator arguments when the
  physical scopes are **compiled**. A new `constructEwmConfig` (beside
  `constructApproxPercentileConfig`, `operator.go:2128`) evaluates the decay
  and policy constants against the **current compilation's `proc`** —
  `GetReadonlyResultFromNoColumnExpression`, exactly the percentile
  constructor's evaluation path — validates resolved values (invalid → the
  §7 execution-boundary error; validation happens at construction, so a bad
  EXECUTE fails before any state exists), **encodes the configuration once
  to bytes** (`aggexec.EncodeEwmConfig`, a new encode/decode pair beside
  `EncodeApproxPercentileConfig`), and returns
  `(args, bytes, nil)` through the channel the repository actually
  provides: `MakeAggFunctionExpression(id, isDistinct, args, config []byte,
  configType...)` stores them in `AggFuncExecExpression.extraConfig`
  (`aggexec/types.go:37-50`) — the same way the percentile constructor
  returns `(args[:1], aggexec.EncodeApproxPercentileConfig(…), nil)`.
  The cached plan is never mutated; rev 4's "plain Go struct through
  `cfg`" wording misdescribed the channel's type and is superseded
  (Alternative P).
- **The EWM dispatch consumes the bytes directly** (rev-5 finding 3): the
  dispatch (`processFuncRange` → `processEwmFuncRange`, below) runs
  *outside* `newAggregateExecutor`, whose `SetExtraInformation` delivery
  (`window.go:562-566`, inside the executor constructor) therefore never
  applies to it. Instead, the EWM path reads **its own**
  `AggFuncExecExpression`'s config bytes — `GetExtraConfig()`
  (`types.go:114`) — and decodes them (`aggexec.DecodeEwmConfig`) once at
  container setup, before row processing. Decode failure or an invalid
  encoded value is a compile-time construction error (§7). The bytes are
  immutable, shared inputs across operator copies — identical ownership to
  the percentile — and carry no execution state (the running state is the
  operator's own `ewmState`, §9 executor).
- **Two independent caches must both exclude marker-parameterized ewm
  statements** (rev-5 finding 2):
  1. **Prepare-compile reuse.** A cached prepared pipeline runs directly
     after `Reset` and does not re-compile (`compile.go:367-368`), so
     constructor-time resolution is per-execution only if the statement
     re-attaches a fresh `Compile`. The repository owns this for
     percentiles: `shouldCachePrepareCompile`
     (`computation_wrapper.go:2802`) returns false when
     `PreparedPlanHasPercentileParams` (`utils.go:4958-4989`) finds a
     percentile marker — at both call sites (prepare-time compile caching
     `mysql_cmd_executor.go:3196`; execute-time reuse
     `computation_wrapper.go:1663`). Rev 4 added the planned ewm branch
     here; a new `PreparedPlanHasEwmParams` (beside the percentile
     helper; visits owner expressions, detects an `ewm*` call whose decay
     or policy positions contain a prepared marker) feeds this
     disjunction.
  2. **Runtime-specialization reuse.** A *second* cache admits, per
     execution, a diagnostic-free plan whose parameters keep their types:
     `cacheable := bound.DiagnosticFree && !bound.ValueDependent &&
     !prepareStmt.hasPaginationParams && !prepareStmt.hasPercentileParams`
     (`computation_wrapper.go:1916`); retrieval matches
     `prepareStmt.runtimeSpecializationKey` and assigns
     `executionPlan = prepareStmt.runtimePlan; cachedRuntimeCompile =
     prepareStmt.runtimeCompile` (`:1859-1868`); the cached compile is
     returned at `:1969` (`retComp = cachedRuntimeCompile`). Rev 4 gated
     only cache 1, so a diagnostic-free EWM SELECT with a policy marker
     and *unchanged parameter types* would still reuse a frozen
     compile. Rev 5 records `hasEwmParams` at PREPARE (beside
     `hasPercentileParams`) and adds `!prepareStmt.hasEwmParams` to the
     `:1916` admission — the percentile precedent gates this cache
     independently of `shouldCachePrepareCompile`, and so does ewm.
  Consequently every EXECUTE of a marker-parameterized ewm statement
  re-attaches a fresh compile and re-enters `constructEwmConfig` with that
  execution's `proc` (including for values that change the semantics but
  not the types). Statements whose ewm policies are all literals keep
  preparing/caching normally — their compiled configuration is constant,
  so reuse is exact (in both caches).
  §10 places this sequence inside a **frontend-level** PREPARE/EXECUTE
  test through the real cache decisions, plus unit rows replicating
  **both** percentile tests: the `shouldCachePrepareCompile` shape
  (`prepared_percentile_cache_test.go:45-50`) and the mixed
  runtime-specialization shape (`:53`,
  `TestPreparedPercentileDisablesMixedRuntimeSpecializationCache`) —
  both as the new `prepared_ewm_cache_test.go`.

### Executor (`pkg/sql/colexec/window/window.go`)

- `processFuncRange` gains a dispatch *before* the WIN_VALUE case:
  `GetFunctionIsWinEwmFunByName(name)` → `ctr.processEwmFuncRange(...)`.
- `processEwmFuncRange`:
  1. reads the per-execution config by **decoding its own aggregate
     expression's bytes** (`GetExtraConfig()` →
     `aggexec.DecodeEwmConfig`; once per container setup, per §9's
     cache-ownership lifecycle — this branch never goes through
     `newAggregateExecutor`/`SetExtraInformation`);
  2. evaluates the value-argument expressions into vectors (existing
     window arg-eval machinery);
  3. in event-time mode, reads the ORDER BY key vector exactly like the
     existing RANGE machinery does — `ctr.orderVecs[len(ctr.orderVecs)−1].Vec[0]`
     (`window.go:2260`; in-memory shuffles exactly this last vector at
     `:2758-2761`, the external path rebinds all order vectors at
     `:2605-2610`; `PartitionTopN` keeps the SQL key last after its
     identity prefix; single-key precedent `boundedSlidingRangeFrame`
     at `:864-872`). Keys convert per the §5 domain table — **unsigned-
     magnitude integer subtraction** (`uint64(b)−uint64(a)` for signed
     keys; direct for unsigned), exact decimal subtraction with overflow
     surfaced as the §7 error, int64 µs for temporal keys, and
     **FLOAT/DOUBLE keys via the quotient-domain ratio**
     (`r = (b−a)/h`, falling back to `r = b/h − a/h` when the
     subtraction overflows; §5) with one float64 conversion of each
     term;
  4. walks rows in order over peer-group boundaries (with the single-key
     rule, peers are instants) **in complete-group units only** (the
     lifecycle's cursor contract, below), applying §5: event-mode
     group-to-group transitions of `W`; row-mode lazy spans per account
     (inaF: first-row-to-last-row spans with all intervening rows
     counted; inaT: one λ per observation batch); batch-local compensated
     unweighted statistics with the identity fast path, the
     midpoint-anchored mean fallback and the retained-magnitude extended
     form for out-of-range moments; the zero-mass early branch; the
     **layered merge** of §5/rev 7 (rev-5 scaled mass-form primary
     path — power-of-two normalized weights, merge-level equality fast
     path, incoming cross-scatter retained, `W == 0` degenerate branch —
     then the two *gated* share layers, then the absolute-mass layer
     with its weighted-difference identities and extended operands);
     group publication (shared value; NULL bits under event-time
     inaT). Every long-running loop here — the peer-group walk, the
     batch statistical passes, any range-safe recomputation and the
     output fill — polls the shared `checkCanceled` helper at the
     cumulative paths' interval (1024; the lifecycle contract below);
  5. writes float64 outputs with NULL bits per §7 for exactly the
     requested slice rows `[start, end)`; the group-post values of a
     group that spans this slice's end have already been computed by
     the cursor (below).
- State & ownership: one `ewmState` per container — two accounts (x / full
  pairs) × (`W`, μx, μy, vxx, vyy, vxy, C) ≈ 15 float64 + counters,
  with each centered moment optionally a 2-word extended
  (mantissa, exponent) pair while its value is out of float64 range;
  compensated accumulators are two extra registers per batch-local
  accumulator, freed with the batch; no goroutines, files, caches or queued
  work.

### EWM execution lifecycle (rev-6 finding 5, rev-7 findings 5/6; container ownership)

The window operator is **incremental**: it materializes the sorted input
into `ctr.bat` per generation, then publishes bounded output slices —
`processFuncRange(start, end)` is called once per `DefaultBatchSize`
chunk with `emitOffset` advancing (`window.go:414-436`), returning to
receive between chunks. A window aggregate therefore needs resumable
cursors; the existing paths carry them in the container
(`runningNextRow`, `runningPartition`, `runningLeft/Right/PeerEnd` —
`processCumulativeAggregateFuncRange`). The EWM dispatch owns its own
analog, specified completely:

- **Initialization.** The container's `ewmState` is nil until the EWM
  path first runs in a generation; a nil state initializes lazily at the
  first `processEwmFuncRange` call (all fields zero — exactly a fresh
  partition: W=0, C=0, accounts unseen), alongside the cursor fields
  (`ewmCompletedRow = 0`; `ewmPartitionSeen = false`) and the in-flight
  group cache (`ewmGroupStart/ewmGroupEnd/ewmGroupValues` — empty). The
  config bytes are decoded once per container build (§9, config
  channel) and never mutated.
- **Publication contract (exactly once).** For each call with slice
  `[start, end)`: the cursor advances **whole peer groups at a time** —
  while the group containing row `end−1` is not yet complete, the next
  complete group (look-ahead inside the materialized `ctr.bat` of this
  generation) is merged exactly once and its `group-post` outputs are
  cached as the in-flight group (`ewmGroupStart/End/Values`); slice rows
  fill from the cached value of the group they belong to. Because the
  operator's `emitOffset` only advances and slices are never
  re-requested, a group straddling a slice boundary merges exactly
  once and every one of its rows — in whichever slices they fall —
  emits the same group-post value (Invariant 18; §10 split-peer test).
  The cursor is monotonic; a re-invocation with an already-completed
  range re-fills values from the cache without any new merge. The
  look-ahead can be as large as the partition (a single peer group may
  contain every row), so this contract is bounded by the *cancellation*
  rule below, not by `DefaultBatchSize`.
- **Partition boundary — account statistics and clocks (rev-7 finding
  6; corrected wiring).** `processEwmFuncRange` performs its own
  partition-boundary check **directly inside its row loop**, at the
  same predicate the cumulative aggregate path uses — `j ==
  currentPartitionEnd` with the running-aggregate group-state free
  under it (`window.go:988` and the free at `:992`, `runningPartition++`
  at `:997` in `processCumulativeAggregateFuncRange`): when the EWM
  row loop crosses into a new partition, the dispatch resets the EWM
  **account statistics and clocks** (W, μx/μy, the centered moments
  — in either plain or extended retained-magnitude form — the C
  gates, and the per-account decay clocks) to their unseen values.
  *(Recorded correction: rev 6 cited `window.go:968/:1053/:1072` as
  in-generation partition transitions — they are, respectively, the
  cumulative "output is not sequential" error guard, the final-output
  cleanup, and the sliding "output is not sequential" error guard;
  none of the three resets partition state, and the BVT/UT wiring
  must not free per-partition EWM state at those sites.)* The
  **generation's absolute output cursor** — `ewmCompletedRow`,
  `ewmPartitionSeen`, the in-flight group cache — is *generation*
  -scoped: it advances across partition boundaries inside a
  generation (partitions are absolute row ranges of the materialized
  `ctr.bat`) and is never reset by a partition
  transition. The EWM dispatch mirrors the cumulative path's
  sequential-order guard (the `:968`-style `outputStart !=
  runningNextRow` check — its own error, no state change) and its
  final-output cleanup (`:1053`-style `outputEnd == n`: the last
  slice frees the in-flight group cache; the generation start does
  the rest).
- **Generation boundary.** `case eval` calls a new `freeEwmState()`
  **beside** `freeRunningAgg()` (`window.go:371` — the source comment
  there: "A new materialized input batch starts a new aggregate
  generation. Normally the previous generation is released after its
  last chunk; this also closes reuse after an interrupted or failed
  generation"): the full `ewmState` (accounts, moments, clocks) *and*
  the generation-scoped cursor/cache reset. In plain terms: a LIMIT
  consumer that stops after the first chunk, a canceled pipeline, or a
  failed generation leaves at most a stale — and now freed — EWM
  state; the next generation initializes from zero.
- **Container reset/teardown.** `Window.Reset` (`types.go:177`, reset at
  `:195`) and `Window.Free` (`types.go:208`, free at `:217`, with the
  pipelineFailed context) call `freeEwmState()` beside
  `freeRunningAgg()` (`types.go:283-292` pattern) — the EWM fields are
  added to exactly the lifecycle sites the existing aggregate state
  owns; nothing EWM-related survives an operator reset, free or
  teardown (the decoded config bytes are immutable, shared inputs of
  the operator arguments and are never freed here — §9 config
  channel).
- **Cancellation (rev-7 finding 5; in-loop, bounded).** The operator's
  Call loop polls every Call/status iteration (`vm.CancelCheck`,
  `window.go:310-312`) — but that alone does not bound a single group:
  a peer group can contain the entire partition, so a whole-group
  scan/statistical pass/recomputation could otherwise run unbounded
  between Call boundaries (rev 6's claim that Call-boundary polling
  matches the cumulative paths' bounds is **superseded**). The
  cumulative paths' actual contract is in-loop: they poll through the
  shared helper `checkCanceled(proc, iteration)` —
  `cancellationCheckInterval = 1024`, `window.go:57` — on every
  examined row of their loops (`:989` per output row, `:1031` per fill
  row). The EWM dispatch adopts the identical bound inside **every**
  long-running EWM loop: group scans (walking `ctr.bat` for peer
  boundaries and look-ahead), the batch statistical passes, any range-
  safe recomputation of the merged statistics, and the output-fill
  loops — each polls `checkCanceled` at the same 1024-row/step
  interval. Publication stays **atomic per group**: a cancellation
  observed mid-group surfaces as the cancellation error with the
  in-flight group cache and cursor untouched (the cursor never
  advances past an incomplete group), so no partial group is ever
  visible downstream; §10 adds the deterministic cancel-mid-group
  test the finding demands.
- **Errors.** An error inside a group (arg evaluation, key validation,
  §7 policy/contract errors surfacing at execution) propagates before
  any state advance: the cursor does NOT move past a failed group, so
  no partial backfill exists; because generation start resets the
  state (`freeEwmState`), a retried or reused container computes from
  scratch. Error reporting and recovery therefore follow the existing
  operator contract with no EWM-specific protocol.

### EXPLAIN

`explain_expr.go` prints committed literals (`alpha=0.5 adjust=false`;
event-time `halflife=10 asc`) and prints prepared markers as markers —
matching the per-execution resolution semantics.

### Performance model

Per input row: O(#ewm-calls) merge steps with O(1) state (layered
normalized representation: no snapshot field; a cursor row index and the
small in-flight group cache group-post outputs,
§9 lifecycle), outputs pre-allocated float64 vectors, total O(r) per
partition in both modes. No fixed constant-factor flops estimate is
claimed for the primary merge: the specified arithmetic forms error-free
deviations and products (twoproduct transforms), compensated mean tails
and complete-mean normalization, so each merge and each batch-local
scatter pass performs a small constant number of fused-multiply
transforms whose exact count depends on the route the (P)/(F)/(C)
retention gates select; event mode adds one `exp`/`expm1` per instant.
The rev-6 fallback paths cost only their exact-detection checks
(finite/non-finite and zero-weight tests) until they fire, so the hot
path is unchanged until a fallback is routed. The measured cost is the
post-implementation responsibility this design pins instead: focused
benchmarks on one ordinary singleton path and one tied-batch path over a
large synthetic partition (plus the state-size assert), deferred to the
implementation round, replacing the naive per-row re-scan it replaces is
O(r²).

## 10. Verification plan

Cheapest deterministic proof per claim; no sleep, no large fixtures:

| Claim | Cheapest proof |
|---|---|
| Parser: keywords parse OVER; 0-arg rejected; names valid identifiers; case-insensitive | Lexer/grammar UT (`sqlparse_test.go` suite precedent) + BVT (identifier-compat guard `CREATE TABLE ewmmean(ewmStd INT)`) |
| Entry-name resolution (issue's five probes) | Registry UT + SQL-level BVT asserting post-fix error strings (no-over, no-order, frame, bad-α, misuse of `'halflife'`, **missing decay**) |
| §3 profile invariant | BVT self-contained fixture; adjust=false → `1, 1.5, 2.25, 3.125`; the issue's control self-join kept verbatim and cross-asserted equal |
| Signature contract (§4) | Binder UT SQL→error table: missing decay (`ewmMean(v) OVER (ORDER BY id)`), every arity bracket of both decay forms, misplaced policy args, decay-type errors, `ewmCov` missing y |
| Constant-signal preservation (Invariant 4) incl. identity **and merge-level** fast paths (rev-5 finding 5) | Operator UT + BVT: `[10,NULL,10]` (α=.5 all policies **and** α=1), `[10,10,10]` keys `[1,2,2]`, event `t=[0,35,100,100]`, event underflow rows; constant huge/non-representable batches (`9007199254740991`×3 var 0 corr NULL; `0.1`×3; `3.25e300`×64); **constant merges across batches**: two singleton batches `x=y=.1` under both adjust modes → mean exactly `.1`, var 0, corr NULL; `x=y=1.3e308` → mean exactly `1.3e308`, var 0, corr NULL; tie batch then singleton; real 64-row constant first batch; 4-singleton constant chain |
| Order-free batches (Invariant 7) | Operator UT: unrounded permutation equality at 1e-12 over 6 orders of a 3-row tie (all five statistics) + exact equality for constant batches; cancellation vector `[1e16,1,−1e16]` exact 1/3 in all orders |
| Clocks (Invariant 15; finding 2) | Operator UT + BVT: `[0,10,20]` keys `[1,2,2]` → 12 (adjT) / 10 (adjF) under **both** ignoreNA policies; `[1,NULL,NULL,3]` keys `[1,2,2,3]` → 25/9 (adjT inaF), 7/3 (adjT inaT), 2.6 (adjF inaF), 2.0 (adjF inaT); mixed full-pair stream rows (pair-account first batch, x-account continuation) |
| Gap continuation profile (rev 22 §8) | Operator UT + BVT: `[1,NULL,3,4]` α=0.5 adjust=false ignoreNA=false → the four rows `1, 1, 7/3` (`2.3333333333333335`), `23/7` (`3.2857142857142856`) (corpus `p3_continuation_adjF`, with the `p3_continuation_adjT` counterpart `1, 1, 2.6, 3.4615384615384617`); the pandas recursive continuation `19/6` contrast is recorded in §8 |
| Centered/translation stability (Invariant 5) | Operator UT: `[100000001,100000002]` var `2/9`; +1e9 identical; tied first batch 14/9; tied later batch 56.0 ≡ translation (cov −56.0, corr −1); unclamped raw values asserted |
| Scale-safe correlation (Invariant 6) | Operator UT: `corr([s,2s],[s,2s]) = 1` at `s=1e-100`, `1e100`; zero-variance rows NULL |
| **Merge cross-moments (Invariant 16; rev-5 finding 1)** | Operator UT + BVT: covariance *and* correlation asserted at **non-initial multi-observation batches in both decay modes and both adjust modes** — row mode adjT keys `[1,2,2]` x `[0,10,20]` y=−x → mean 12, var 56, cov −56, corr −1; adjF → mean 10, var 200/3, cov −200/3, corr −1; event mode multi-obs batches cross-checked against direct absolute-position-weight closed forms for mean/var/cov/corr (adjT 23.63636363636364 / 186.77685950413223 / −186.77685950413223 / −1; adjF 17.5 / 218.75 / −218.75 / −1); rev-4 fault (−36 / −0.6428571428571429) recorded |
| **Scaled merge weights (Invariant 16; rev-5 finding 4)** | Operator UT: keys `[−1075,−1074,0,5e-324]` h=1 adjF x `[.5,1.5,NULL,1]` y=−x → final row var `.125`, cov `−.125`, corr `−1` (fault record: rev-4 var 0 / corr NULL at `W=A=5e-324`); expected-value comparisons in the ewm tests are **purely relative and range-safe** (exact equality plus: a wrong zero against a tiny nonzero expectation FAILS; NaN matches nothing; ±Inf matches only the same sign) — the comparator contract itself is unit-tested with the rejection meta-rows (rev-6 finding 4) |
| **Layered range-safety (Invariant 17; rev-6 findings 1/2/3)** | Operator UT + BVT: `[1.6e308,1.6e308,1.7e308]` α=.25 adjT → `1.6432432432432432e308` with rows 1–2 exact (rev-5 fault Inf); `x=y=[0,2e154]` α=.01 adjF → var/cov `3.96e306`, corr 1 (fault Inf/Inf/NaN); extreme-ratio shares BOTH asymmetries — keys `[0,8e-16]` h=1e308 `x=y=[0,1e150]` → `4.94e-174 / 4.94e-24 / 1` (fault 0/0/NULL), keys `[0,1074]` h=1 `x=y=[1e4,0]` → mean `s1·1e4 ≈ 4.9407e-320`, var/cov `s1·1e8 ≈ 4.9407e-316`, corr 1 (fault: dropped old state); tied first batch `[1e308,1.1e308]` → mean `1.05e308` (fault NaN), honest Inf variance, corr NULL; tied `x=y=[−1e154,1e154]` → var/cov `1e308`, corr 1 (fault NaN); comparator rows: `close(inf,1)`, `close(1,inf)`, `close(inf,−inf)` False, `close(±inf,±inf)` True, NaN never matches |
| **Layered range-safety, retained magnitude (Invariant 17; rev-7 findings 1/2/3/4)** | Operator UT + BVT, every vector asserted against independently computed (900-digit) expectations: all **six permutations** of the tied batch `[1e308,1.1e308,−1e308]` → finite mean `3.6666666666666666e307` (±1 ULP), honest +Inf var, corr NULL (rev-6 fault: −Inf in 2 of 6 orders); keys `[0,8e-16]` h=1e308 adjF `x=y=[−1e308,1e308]` → var/cov `1.9762625833649863e293`, corr 1 (fault Inf/Inf/NULL); keys `[0,1074,1074]` h=1 adjT `x=y=[1e4,0,0]` → mean `2.4703e-320`, var/cov `2.47032823e-316`, corr 1 (fault 0/0/0/NULL); merge-layer unit rows (direct `Stream`-level merge calls): W=2 vs `A=5e-324` with x=y=`1e150` → mean `2.4703282292062327e-174`, var/cov `2.4703282292062325e-24`, corr 1 (the below-subnormal incoming share, incoming asymmetry), and the same state against a `1.3e308` constant → mean EXACT, var 0, corr NULL (constancy through the absolute-mass layer); keys `[1,2,2]` α=.01 adjF `x=y=[0,−2e154,2e154]` → merged var/cov `7.920792079207922e306`, corr 1 (fault Inf/NaN); tied `[−2e154,2e154]` t=0 then `0` at t=10 h=1 adjF → rows 1–2 honest +Inf var / corr NULL **while the state keeps the extended 4e308**, final row var/cov `3.90625e305`, corr 1 (fault: Inf forever) |
| **Anchored scatter, exact scaling, compensated accumulation, retained std (Invariant 17; seventh-review findings 1–4)** | Operator UT + BVT, every expectation independently computed (900-digit): all six permutations of `[1.7e308,1.7e308,−1.7e308]` → finite mean `5.666666666666667e307`, honest +Inf var/cov, **finite std `1.6027753706895077e308`**, corr NULL (fault: engine never returned — the raw deviation overflows and the extended normalization loops on the −Inf mantissa); keys `[0,8e-16,8e-16,8e-16]` h=1e308 adjF `x=y=[0,1.7e308,1.7e308,−1.7e308]` → final var/cov `1.4278497164812023e293`, corr 1 (fault: hang); incoming odd-subnormal arrival keys `[0,2e-15]` h=1e308 adjF `x=y=[0,1e150]` → `1.4821969375237395e-173` / `1.4821969375237397e-23` / 1 (fault: the ldexp tie committed the 33%-high `1.976262583364986e-173` / `1.976262583364986e-23`); retained odd-subnormal `W=3·2^-1074` under `A=2` → `7.4109847e-320` / `7.4109847e-316` / 1 (fault: `9.8813e-320` / `9.8813129e-316`, 33% high); the 1e200/1e150 six-pair cross batch → cov `3.333333333333333e299` in both review orderings **and bit-identically across all 720 peer permutations**, means exactly 0, honest +Inf var, finite std `8.16496580927726e199`, corr NULL (fault: cov 0.0 in one order); the retained `4e308` vector → +Inf-variance rows emit std EXACTLY `2e154`, recovery row var `3.90625e305` with std `6.25e152` (fault: std Inf); **independent ewmStd** literals accompany the variance assertions on these rows in both UT and BVT |
| **Never-commit-subnormal shares, cascaded accumulation, carried mean tails, sub-lattice retention (Invariants 19/20; eighth-review findings 1–4)** | Operator UT + BVT, every expectation independently computed (900-digit Decimal in the archived simulator): keys `[0,1072,1074]` h=1 adjF x=y=`[1e200,NULL,0]` → mean `6.587541944549954e-124`, var/cov `6.587541944549954e+76`, corr 1 (fault: the certificate committed `4.940656458412465e-124` / `4.940656458412465e+76`, 25% low); the three-scale tied batch (H=2^600, M=2^400, L=2^200, adjust=true) in BOTH review orderings → cov `5.1644997561738175e+119`, means exactly 0, honest +Inf var, finite std, corr NULL (fault: the interleaved order returned cov exactly 0.0); x=y=`[2^53, 2^53+2]` α=.5 adjF **both** the tied batch and the two-singleton merges → var/cov/std EXACTLY 1 with the `[0,2]` translations pinned alongside (fault: `2 / 2 / 1.4142135623730951`); tied `[−1e-200, 1e-200]` → std `~1e-200` with var/cov honestly 0.0, corr NULL (fault: std exactly 0.0); singletons `[−2^-537, +2^-537]` → var/cov exactly `2^-1074`, std exactly `2^-537`, corr exactly 1 (fault: 0/0/0/NULL) |
| **Complete-mean merges, accumulator domain, weighted-product retention, retained-moment correlation (Invariants 21/22/23; ninth-review findings 1–4)** | Operator UT + BVT, every expectation independently computed (900-digit Decimal on the engine's own rounded masses), each vector with its REVIEW-DEMANDED REORDERED CONTROL: x=y=`[2^53, 2^53+2, 2^53+4]` α=.5 adjF on the singleton chain AND the tied-first-pair path → var/cov EXACTLY 2.75 with the `[0,2,4]` translation control equal (fault: 4.25); `[2^53, 2^53+2, 2^53]` → var EXACTLY 0.75 (fault: 0.5 — the hi-only equality shortcut); equal-hi/different-tails continuation (state `2^53`+tail 1 vs tie `[2^53−2, 2^53+2]`) → var 29/9 (fault: 3.0); the tied batch `[(1,1),(−1,−1),(H,H),(−H,−H),(H,−H),(−H,H)]` at H=2^600 adjust=true in BOTH peer orders → cov 1/3 (fault: exactly 0.0 in one order); the 30-row ten-scale batch (h = 2^600 … 2^250; cross-negatives except the last scale) in BOTH orders → cov `2^500/15 = 2.1822604052640945e+149` (fault: 0.0); keys `[0,0,1000]` h=1 adjF `x=y=[−2^-50, +2^-50, 0]` → var honestly 0.0 with std `2.713328551617619e-166` (fault: std 0.0), pinned against the engine's own rounded decay/arrival masses; the incoming-side counterpart keys `[0, 1e-300]` tie arrival → var 0.0, std `7.394570388520962e-166` (fault: 0.0); `[(±2^-537, ±1.5·2^-537)]` tied batch AND singleton merges → corr EXACTLY ±1.0 with the unit-scale control 1.0 (fault: ±2.0) |
| **Double-double complete means, weighted-deviation retention, exact alignment, finite cascades (Invariants 21/23/24/25; tenth-review findings 1–4)** | Operator UT + BVT, every expectation independently computed (900-digit Decimal on the engine's own rounded masses), each vector with its REVIEW-DEMANDED REORDERED CONTROL: x=y=`[2^600, 2^600+2^548]` α=.5 adjF singletons → mean `2^600`, honest Inf var/cov, **std EXACTLY 2^547** equal to the translated `[0, 2^548]` control (fault: std `6.515123101890962e+164`); x=`[2^53, 2^53+2, 2^53+4]` with y=2^-260·`[0,2,4]` → **var exactly 2.75, cov exactly 2.75·2^-260, corr 1** (fault: cov `1.8891618714269098e-78` / corr `0.9949366763261822`); keys `[0,1000]` h=1 adjF x=y=`[2^-100,0]` → honest var 0.0 with **std `2.4099198651029665e-181`**, incoming-side counterpart keys `[0,1e-300]` x=y=`[0,2^-100]` → std `6.567697842037889e-181` (faults: std 0.0 both sides); tied batch `[(1.5,1.5),(−1.5,−1.5),(H,H),(−H,−H),(H,−H),(−H,H)]` at H=2^537 adjT in BOTH peer orders → cov EXACTLY 0.75, honest Inf var, finite std, corr NULL (fault: `0.6666666666666666` small-pairs-first), plus the direct accumulator `xval(xsum4([0.75, 2^1073, −2^1073]))` → 0.75 (fault: 1.0); the finite cross-scale covariance batch (H=2^400, M=2^200, L=1: same-sign quartets then the two cross quartets) → cov EXACTLY 0.2 as-written AND grouped (fault: 0.0 as-written), and the mean `[H,M,1,−H,−M]` → 0.2 in the review order AND the reorder (fault: 0.0 review order); the non-finite-residual fold keeps the preserved `[1.6e308,1.6e308,1.7e308]` rows bit-identical while guaranteeing loop termination |
| **Error-free complete-mean products, shared bounded restoration, range-safe mean tails (Invariants 21/24/26; eleventh-review findings 1–3)** | Operator UT + BVT, every expectation independently computed (900-digit Decimal), each vector with its translated/reordered control: singletons x=y=`[2^600, 2^600+2^548]` α=3/8 adjF → **std EXACTLY `4.460599859785886e+164`** (fault: `5.150657059589176e+164`) and α=.5 adjT → **std EXACTLY `4.343415401260641e+164`** (fault: `5.319575737017211e+164`), both EQUAL to their translated `[0, 2^548]` controls; unit row `dddiv(1,3)` keeps a **NONZERO exact-remainder tail word** (fault: zero tail — the remainder computed the residual of a one-rounded product); the tied batch `[−2^54,2^216,−2^162,.75,2^162,2^54,−2^216]` α=.5 adjT → **mean EXACTLY 3/28 in the review order AND the magnitude reorder** (fault: 0.0 review order); the accumulator sum `[−2^323,2^215,2^431,2^323,.75,−2^431,−2^215]` → **0.75 EXACTLY** (fault: 0.0 — the restore discarded its head-fold and leftover losses); the first tied batch `[H,H+U,H+U]` at `H=2^1000`, `U=2^948` → **std EXACTLY `1.1215783890386092e+285` with NO ValueError and NO output clamping** (fault: `out_std` raised a math-domain ValueError on the negative retained variance from the `−2U/3` tail) equal to the translated `[0,U,U]` control; `twoprod(3, 2^1000+2^948)` keeps the EXACT residual `p+e` (fault: `e 0.0` — the splitter returned the operand unsplit past ~1.34e300); boundary rows: raw split EXACT at `2^1000+2^948` (hi+lo), and the top-band operand `1.7976931348623157e+308` — whose raw high word `2^1024` is NOT representable — products exactly through the mantissa-domain form wherever `fl(a*b)` is finite |
| **Canonical complete-mean normalization, retained batch residuals at the overflow boundary, retained below-lattice tails (Invariants 27/28; twelfth-review findings 1–3)** | Operator UT + BVT, every expectation independently computed (900/1200-digit Decimal) with translated/permutation/continuation controls: keys `[0,0,1]` x=y=`[2^53,2^53+2,-2^53]` α=.5 BOTH adjust → **mean EXACTLY 0.5** (fault: 0.0 — the share products each rounded once and the cancelled high word dropped the stored tail that held 0.5), translated `[0,2,0]` control 0.5; singletons `[2^600,-fl(5/3)*2^600]` α=3/8 adjF → **mean EXACTLY `-2^545` = `-1.1517219314030583e+164`** (fault: 0.0 — dddiv's zero high word over its exact remainder tail), α=.25 hi-word-dominant control == 900-digit expectation; first tied batch `[M,0,0]` M=maxfloat64 BOTH adjust → **mean EXACTLY `fl(M/3)` = `5.992310449541053e+307`, std EXACTLY `8.474406707690974e+307`, honest +Inf var/cov, corr NULL, NO AssertionError** (fault: the non-finite normalization AssertionError — `3*fl(M/3)` overflowed into the float64 tail), `[0,M,0]`/`[0,0,M]` permutations equal, `[M/2,0,0]` control std `fl(M*sqrt(2)/6)` unchanged; first tied batch `[0,2u,6u]` u=2^-1074 → **mean EXACTLY 3u, std EXACTLY the correctly rounded 2u, honest var 0.0** (fault: std 3u — the exact tail `−u/3` fell below the subnormal lattice), unit-scale `[0,2,6]` control std `fl(sqrt(56/9))`, continuation keys `[0,1]` singleton `[1u]` → **merged mean EXACTLY 2u through the retained pair tail** |
| **Every-high-word complete-mean folds and mixed retained/scalar cross-centering (Invariants 29/30; thirteenth-review findings 1–2)** | Operator UT + BVT, every expectation independently computed (900/1200-digit Decimal) with translated/permutation/swapped-stream/continuation controls: keys `[0,0,1]` x=y=`[2^53,2^53+2,-2^53+2]` α=.5 BOTH adjust → **mean EXACTLY 1.5** (fault: 1.0 — hi 1.0 with the exact tail .5, a NONZERO high word the zero-only swap never touched), translated `[0,2,2]` control 1.5; singletons `[2^600,-fl(5/3)*2^600+2^549]` α=3/8 adjF → **mean EXACTLY `5*2^545` = `5.758609657015291e+164`** (fault: `4.606887725612233e+164` — the dd lead `4*2^545` with the exact `+2^545` remainder), α=.25 hi-word-dominant control == its 900-digit expectation; the in-suite `[1.6e308,1.6e308,1.7e308]` α=.25 adjT merged mean now equals its own pinned `1.6432432432432432e308` EXACTLY (rev 13 emitted the one-ulp-low dd lead, tolerated at 1e-12); first tied batch x=`[M,0,0]` y=`[1+2^-52,1,1]` at M=maxfloat64 BOTH adjust → **cov EXACTLY `8.870401375709865e+291` and x std EXACTLY `8.474406707690974e+307`, NO TypeError** (fault: TypeError multiplying the retained x tail as a scalar with a finite scalar sxy and a scalar y tail), swapped stream equal, `[0,M,0]`/`[0,0,M]` permutations equal, `[M/2,0,0]` all-scalar sentinel keeps its EXACT complete cov `4.4352006878549327e+291` (rev-16 reassessed: one ulp above the rev-13..15 pin whose one-rounded formation lost the ulp — fifteenth review), x=y control keeps the honest +Inf covariance with std `8.474406707690974e+307`, continuation keys `[0,1]` singleton merges to the 900-digit doctrinal mean/std/cov; the raw W==0 commit keeps the rev-9 pinned narrowed word (r11F4 `[2^400,2^200,1,-2^400,-2^200]` mean EXACTLY 0.2 stays untouched by the fold) |
| **Genuine batch mean-tail residuals; continuation through initial and incoming state (Invariant 31; fourteenth-review finding 1)** | Operator UT + BVT, every expectation independently derived (Decimal): the cross-scale tied batch `[2^400,2^200,1,-2^400,-2^200]` commits the GENUINE tail `fl(-2^-54/5) = -1.1102230246251566e-17` in the review order AND the `[2^400,-2^400,2^200,-2^200,1]` reorder (fault: the review order folded the registers to the tail `-0.2` at the word's own scale, order-dependent); initial-state continuation keys `[0,0,0,0,0,1]` x=y=`[H,M,1,-H,-M,0]` α=.5 → final mean EXACTLY `fl(1/7)` = `0.14285714285714285` adjT and EXACTLY `0.1` adjF (fault: 0.0 under both policies); incoming-state keys `[0,1,1,1,1,1]` → final mean EXACTLY `fl(2/11)` = `0.18181818181818182` adjT and EXACTLY `fl(1/6)` = `0.16666666666666666` adjF (fault: 0.0 under both policies); both reordered controls keep all four values; the raw single-batch emission keeps `0.2` under both policies (the r11F4 pins) |
| **Error-free batch deviation and product residuals; initial/incoming continuation healing (Invariants 30/32; fifteenth-review finding)** | Operator UT + BVT, every expectation independently derived (exact Fractions from the fixture values and the decay-mixture definition), plus minimal SQL BVT witnesses in `ewm_statistics.sql` (the runner fixture rows below drive `SELECT ewmCov(x, y, 0.5, <adjust>, false, 0) OVER (ORDER BY id)` with the stated literals asserted): the tied batch `[H,1,-H]`×`[1,-2,1]` at H=2^52 → covariance EXACTLY `fl(-2/3)` = `-0.6666666666666666` in ALL six peer orders, both decay modes (row α=.5 / event-time h=1) and both adjust policies (fault: `-0.6111111111111112` everywhere — the one-rounded deviations `[H-0.5, 2/3, -H]` against `mb=fl(1/3)`), with the translated (+H,+H)-streams control and the swapped-stream control both equal (`fl(-2/3)`; rev 15 was already correct there — every addition exactly representable); the zero-mean batch `[K+1,K,-K-1,-K]`×`[K-1,1-K,K,-K]` at K=2^27 → covariance EXACTLY `-0.25` across ALL 24 permutations, both decay modes and both adjust policies (fault: EXACTLY `0.0` in 96/96 executions — the first product rounds `fl(K²-1) = K²`, discarding the -1 before any compensation could see it; correct mean tails cannot recover it), the batch var/std unchanged (`var 1.801439864369971e+16`, `std 134217728.5`); the poisoned continuations heal: initial keys `[0,0,0,1]` → merged covariance EXACTLY `fl(-2/5)` = `-0.4` adjT and EXACTLY `fl(-1/3)` = `-0.3333333333333333` adjF, incoming keys `[0,1,1,1]` → merged covariance EXACTLY `fl(-4/7)` = `-0.5714285714285714` adjT and EXACTLY `fl(-1/2)` = `-0.5` adjF (faults: `-0.3666666666666667`, `-0.3055555555555556`, `-0.5238095238095238`, `-0.45833333333333337` — the corrupt committed covariance poisoned every subsequent merge) |
| **Error-free batch moments on the lattice and anchored routes; routing-boundary and continuation witnesses (Invariant 33; sixteenth-review finding)** | Operator UT + BVT, every expectation independently derived (exact Decimals from the fixture values and the 900-digit decay-mixture model, which first reproduces the four r16F continuation literals), plus minimal SQL BVT witnesses in `ewm_statistics.sql` (the runner fixture rows drive `SELECT ewmCov(x, y, 0.5, <adjust>, false, 0) OVER (ORDER BY id)` with the stated literals asserted): the one-sided-scaled lattice batch x = 2^-600·[K+1,K,-K-1,-K] × y = [K-1,1-K,K,-K] (K=2^27) → covariance EXACTLY fl(-2^-602) = `-6.02479966275721e-182` across ALL 24 permutations, both decay modes and both adjust policies, and the y-scaled one-sided control equals it (fault: EXACTLY `0.0` in 96/96+96/96 executions — each one-rounded product discards the row's -2^-600-scale residual 54 binades below its lead); the anchored batch [B,B,B,-B], B=1.7e308, × y=[2^52,1,-2^52,0] → covariance EXACTLY fl(B/8) = `2.125e+307` in every peer order, both modes, both policies AND on the swapped streams (fault: `4.25e+307` everywhere — the one-rounded deviations read [2^52, 0.75, -2^52, -0.25] and both ±0.25 residuals multiply against the B-rows); the routing-boundary pair at the frexp |d| fence — scale 2^-538 (max-deviation exponent -510, the normal route) → EXACTLY `-2^-540` = `-2.778448436856347e-163` and scale 2^-539 (exponents -511/-512, the lattice route) → EXACTLY `-2^-541` = `-1.3892242184281734e-163`, each across 24 permutations + both policies (the fence is content-safe on both sides); the continuations heal at both policies — (a): initial `fl((2/3)·(-2^-602))` = `-4.0165331085048066e-182` adjT, `fl((1/2)·(-2^-602))` = `-3.012399831378605e-182` adjF, incoming `fl((8/9)·(-2^-602))` = `-5.355377478006409e-182` adjT / `fl((4/5)·(-2^-602))` = `-4.8198397302057685e-182` adjF, all EXACT (fault: `0.0` ×4); (b): initial `fl(3B/32)` = `1.5937499999999998e+307` adjF EXACT, and initial adjT `fl(B/9)` = `1.8888888888888888e+307`, incoming adjT `fl(10B/81)` = `2.0987654320987653e+307`, incoming adjF `fl(3B/25)` = `2.04e+307` asserted exactly-or-within-ONE-ulp-of-ideal with the ideals recorded (the merge's own moment assembly — rev-8 finding 3 — cannot carry the 55-bit numerator of the B-scale mixture; faults: `3.3055555555555557e+307`, `2.65625e+307`, `3.9876543209876543e+307`, `3.74e+307`); the UT/BVT literals also cross-check the var/std subnormal-retention contracts (the `[0,2u,6u]` control and the r13F3 family stay byte-identical) |
| **Cancellation-preserving signed-cross accumulation across every route; routed cross-pile order, boundary and continuation witnesses (Invariant 34; seventeenth-review finding)** | Operator UT + BVT, every expectation independently derived (exact Fractions/Decimals of the fixture values and the 900-digit decay-mixture model, which first reproduces the four r17F (a)-family continuation literals bit-for-bit), plus minimal SQL BVT witnesses in `ewm_statistics.sql` (the runner fixture rows drive `SELECT ewmCov(x, y, 0.5, <adjust>, false, 0) OVER (ORDER BY id)` with the stated literals asserted): the tied anchored batch x=[B,B,B,B,B,−B], y=[−H,M,1,H,−M,−1] (B=1.7e308, H=2^400, M=2^200) → covariance EXACTLY fl(B/3) = `5.666666666666667e+307` in the stated, reversed, swapped and swapped-reversed peer orders across BOTH decay modes and BOTH adjust policies, plus five rotated orders at row decay (fault: EXACTLY `0.0` in the stated and swapped orders — the ddsum two-word compression of the UNCANCELLED pile erased the ±BH-scale pair content before dddiv, while the reversed order already cancelled inside the register word); the ten-pair lattice batch [(H,H),(−H,−H),(M,M),(−M,−M),(1,1),(−1,−1),(H,−H),(−H,H),(M,−M),(−M,M)] with x scaled by 2^−512 → covariance EXACTLY fl(2^−512/5) = `1.4916681462400414e-155` in the stated, reversed, swapped and swapped-reversed orders across both modes and policies, plus four rotated orders at row decay, with the correlation pinned NONZERO = `7.498484069478155e-242` and bit-equal between the stated and reversed orders (fault: cov AND corr exactly `0.0` in the stated order; the normal-route 2^−511 control green in both orders); the routing-boundary pair — lattice side 2^−512, normal side 2^−511 → EXACTLY `2.983336292480083e-155` in both orders, both policies and both modes; the continuations heal at both policies — witness B EXACTLY at the closed-form mixture ideals `1.2430567885333677e-155` / `7.458340731200207e-156` / `1.4206363297524203e-155` / `1.3560619511273104e-155` (initial adjT/adjF, incoming adjT/adjF) and witness A at the B-scale ideals `4.25e+307` / `2.8333333333333334e+307` / `5.23076923076923e+307` / `4.857142857142857e+307`, asserted with the ideals recorded (observed EXACT this round; the merge's own single-pair assembly bound — rev-8 finding 3 — remains documented) — rev 17 committed cov `0.0` for both witnesses and poisoned all eight continuations with EXACTLY `0.0`; the UT/BVT literals cross-check that every preserved r15F/r16F/r17F row reproduces byte-identically through the changed sites |
| **Genuine complete batch residual from the ORIGINAL observations on both the ordinary and overflowing-sum routes; tied-batch overflow, peer-order, policy and continuation witnesses (Invariants 35/36; eighth-review P2 finding)** | Operator UT + BVT, every expectation independently derived (exact Fractions of the fixture values cross-checked by the 900-digit Decimal model and the archived simulator), plus minimal SQL BVT witnesses in `ewm_statistics.sql` (the runner fixture rows drive `SELECT ewmMean(x, 0.5, <adjust>, <ignoreNA>, 1) OVER (ORDER BY id)` with the stated literals asserted): the tied overflow batch [H,H,−H] at H = 2^1023 (raw partial sum H+H overflows float64, the anchored route fires) commits the account (b, τ) with b = fl(H/3) = `2.9961552247705263e+307` and the genuine complete residual τ = (H−3b)/3 = `1.6632002579455998e+291` — asserted EXACTLY for the stated order AND both reordered peers [H,−H,H] and [−H,H,H], on both streams (x and y), proving peer-order invariance of the committed account (rev 18 committed tail EXACTLY `0.0` in the stated and swapped orders while both peers committed τ — 16 failing combinations against 32 correct reordered controls across initial/incoming × row/time × adjust T/F × ignoreNA T/F, the corrupt continuations `0.0`, `−9.9792015476736e+290` = −(H−3b)/5, `−9.072001406976e+290` = −(H−3b)/5.5, `fl(H/18)` and `−H/12`); the merged continuations heal — appended −b at α=0.5 adjust=false → mean EXACTLY fl((H−3b)/6) = `8.316001289727999e+290` (rev 18: EXACTLY `0.0`); appended −H/2 under adjust=true → mean EXACTLY `0.0` (rev 18: `−9.9792015476736e+290`); the incoming shapes: row decay → EXACTLY `4.993592041284211e+306` = nextafter(fl(H/18)) (ONE documented merge ulp above the strict `4.9935920412842106e+306`, where the corrupt account emitted) and time decay → EXACTLY fl(−H/12) = `−7.490388061926316e+306` (the corrupt emission coincided — the committed account is the discriminator), with the merged mean emissions BIT-IDENTICAL across all three peer orders at every combination; the genuine_tail unit vectors keep the rev-13 contracts — the first tied batch [M,0,0] (M = maxfloat64) keeps its exact complete residual and [0,2u,6u] (u = 2^−1074) retains the below-lattice pair (xval `0.0`, nonzero mantissa); the UT/BVT literals cross-check that the pre-row full run on the rebuilt core stayed byte-identical to the frozen rev-18 run log (all 172 preserved rows, NO reassessed literal) |
| **Subnormal tail retention, complete-mean packing and the public finite-sample contract (Invariants 37/38/39; nineteenth-review P2 findings)** | Operator UT + BVT, every expectation independently derived (exact Fractions of the fixture values cross-checked by the 900-digit Decimal model and the archived simulator), plus minimal SQL BVT witnesses in `ewm_statistics.sql` (the runner fixture rows drive `SELECT ewmStd(x, 0.5, <adjust>, <ignoreNA>, 1) OVER (ORDER BY id)` for the subnormal witness plus the §7 invalid-input rejection row, with the stated literals asserted): the tied subnormal batch [B, B+2u, B+8u] at B = 2^-1021, u = 2^-1074 (the batch mean commits hi B+2u and its genuine complete tail is the pair fl(4/3)·u narrows to u) commits the GENUINE PAIR on all 6 peer orders and both streams — rev 19 committed the narrowed scalar u on every order — and all 48 executions (membership permutations × both decay modes × both adjust policies × both ignoreNA policies) emit std EXACTLY 3u, the exact 104u²/9 variance's correctly rounded std (rev 19 emitted 4u in all 48); the translated [0, 2u, 8u] controls emit the same 3u on both modules; the retention boundary vectors are pinned (zero pair narrows exact → scalar lane; an exact subnormal 3u word re-normalizes to itself → scalar lane; a normal lead → scalar lane; the lossy fl(4/3)·u pair → retained pair; an out-of-range lead narrows honestly to ±Inf → scalar lane); the singleton sequence [0], [2u], [4u] under α=0.5 pins the committed (hi, tail) account after EVERY merge boundary (x-stream snapshots at the first and final merge): adjust=true → first-merge complete within the extended-mantissa bound of the exact 4u/3 (rev 19 landed ON u, a ≈u/3 loss — the discarded primary xfold remainder), final within the bound of the exact 20u/7 (rev 19 landed 3u, drift u/7) with final std EXACTLY u — the exact 104u²/49 rounds to u (rev 19 emitted 2u), adjust=false → the first merge stays EXACTLY u (green in rev 19 too) and the final heals to the bound of 5u/2 (rev 19 committed the fl(2.5u) = 2u tie), both decay modes; the non-finite-sample contract: every singleton or tied/later ±Inf/NaN x or y sample raises the defined `invalid input: ewm sample values must be finite` execution error at the row pre-scan before any state is published with clean finite reuse (mean 2.0) — rev 19 leaked the internal extended-normalization AssertionError; the UT/BVT literals cross-check that the pre-row full run on the rebuilt core stayed byte-identical to the frozen rev-19 run log (all 173 preserved rows, NO reassessed literal) |
| **Merged-state tail retention at every tail-storage boundary (Invariant 40; twentieth-review P2)** | Operator UT + BVT, every expectation independently derived (exact Fractions of the fixture values cross-checked by the 900-digit Decimal model and the archived simulator), plus a minimal SQL BVT witness in `ewm_statistics.sql` driving `SELECT ewmStd(x, 0.5, <adjust>, <ignoreNA>, 1) OVER (ORDER BY id)` over the subnormal singleton fixture with the stated literals asserted: the singleton chain [B], [B+2u], [B+4u] at B = 2^-1021, u = 2^-1074 under α=0.5 adjust=true pins the committed (hi, tail) account after EVERY merge boundary on all eight {row, time} × {ignoreNA} × {sign} combinations — the first merge commits hi B+2u with the RETAINED pair −2u/3 (rev 20 narrowed the tail to the scalar −u) so the complete mean reads the exact 4u/3 (rev 20 committed B+u), the final reads the exact 20u/7 (rev 20: B+3u) with final std EXACTLY u — the exact 104u²/49 rounding (rev 20 emitted 2u) — and the fourth-singleton continuation reads the exact 68u/15 (rev 20: B+5u) in both signs; the translated [0], [2u], [4u] controls (signed, with the continuation variant) keep their genuine completes and std u; the merged-scale retention unit vectors pin the lossy −2u/3 pair, the exact 3u word and the below-lattice class — 20/20 assertions; and the UT/BVT literals cross-check that the pre-row full run on the rebuilt core stayed byte-identical to the frozen rev-20 run log (all 174 preserved rows, NO reassessed literal) |
| **EWM execution lifecycle (Invariant 18; rev-6 finding 5, rev-7 findings 5/6)** | Operator UT (container-level, window harness precedents `window_test.go:2658` `t.Cleanup(ctr.freeRunningAgg)` drive pattern, `interval_test.go:197`): (a) **split-peer exactly once** — one partition wider than `DefaultBatchSize` with a peer group straddling the slice boundary: all of the group's rows (across both slices) emit the identical group-post value, and the account's `C`/`W` delta equals one merge (tie group of ≥ 2 rows counted once; hand-computed group-post value verified); (b) **partition/generation isolation** — two partitions delivered across two generations: partition 2's outputs equal its single-partition fresh run, AND partition 2's first rows prove the account statistics/clocks reset while the generation's absolute cursor and in-flight cache were *not* reset by the partition transition (the corrected wiring: the boundary check lives inside `processEwmFuncRange` at the cumulative `j == currentPartitionEnd` predicate, `window.go:988/:992`-style — never at the sequential-order guards `:968/:1072` or the final cleanup `:1053`); (c) **LIMIT then Reset** — publish the first slice then stop consuming, run `Window.Reset`, re-execute: outputs equal a fresh container's (no leaked `ewmCompletedRow`, state or cache); (d) **error then clean reuse** — an injected §7 error mid-generation keeps the cursor behind the failed group (no partial backfill; error surfaces), and after the framework's reset the same input produces full correct outputs; (e) **cancellation mid-group** — one partition whose single peer group contains the whole partition (>1024 rows): flag the process canceled after group processing starts, drive `processEwmFuncRange`: the dispatch returns the cancellation error within the bounded interval (≤1024 rows/steps of remaining work — iteration-polled at the shared `checkCanceled` helper, `window.go:57` sites `:989/:1031` pattern) BEFORE the group finishes; no rows are emitted and the exactly-once cursor stays behind the group; after the framework's Reset, re-execution of the full input produces the complete correct outputs (generation-scoped reset proven — the per-group atomic publication is never violated). Executed engine tests are a post-approval task; these are the planned deterministic minimum for implementation review |
| Decay-depth invariance (Invariant 13; finding 4) | Operator UT: `[1e-100,2e-100,NULL]` t=`[0,1,501]` h=1 carries var `2.222222222222222e-201` / corr 1 at the decaying row (W ≈ 4.58e-151); continuation row restarts fresh (mean `3e-100`, var 0, corr NULL); α=1 and event-underflow carry rows; carried variance `0.11753902662993572` (`[3,5,NULL]`) |
| Zero-mass arrivals (finding 6) | Operator UT: `[7,9]` t=`[0,1e-20]` h=1e308 → carried 7, C=2, no error; pair stream carries cov −1 / corr −1; subnormal-A two-obs instant carries with C counted; **negative test at rev-3 semantics**: the ZeroDivisionError fault recorded |
| Integer gap domains (Invariant 14) | Operator UT: `[MinInt64, MaxInt64]` h=1 → 0,10; `[0, MaxUint64]` → 0,10; DECIMAL extreme pair → the §7 overflow error row; the wrapped-dt fault (−1 → mass −1 → mean −10) recorded as the prevented behavior |
| **Finite-DOUBLE-key decay without overflow (rev-5 finding 6)** | Operator UT + BVT: keys `[−1e308, 1e308]` h=1e308 adjF x `[0,10]` → `0, 7.5` (fault record: raw subtraction overflow → mean 10); constant `x=7` across the same gap stays exactly 7; non-finite DOUBLE keys (NaN/±Inf) and non-finite resolved α/h are rejected per §7 with their error strings |
| Event-time precision gates | Operator UT + BVT: BIGINT neighbors → 0,5; tiny ratio → `0.6931471805599453`; int64-µs subsecond → `2.928932188134525`; DECIMAL keys → 3.0 |
| Event-time contract rejections | Binder UT: multi-ORDER-BY event-time rejected, DESC rejected, single ASC accepted, key-type rule; same statements in BVT |
| Within-instant scatter (rev-3 finding 3) | Operator UT + BVT: same-instant `(-1,1),(1,-1)` → var 1, cov −1, corr −1; instant var 193.75 / 131.30193905817174 with means preserved; repeated-time single-key 10,20,20 / var 150 |
| Time-vector alignment + unsorted input | Operator UT: reverse-insertion fixture asserting per-instant Δt after the operator's own sort; read path equals the RANGE frame's last-vector read |
| NULL-bit vs value policy matrix | Operator UT with §5 NULL-gap rows (both carry rows) + event-time inaT rows (bit at null-input row only) + tie rows |
| Prepared-execution stability (Invariant 12; rev-4 finding 1, rev-5 finding 2) — the real cache paths | Frontend-level UT/BVT in `pkg/frontend`: `PREPARE s FROM SELECT ewmMean(v, ABS(?), ?, ?, ?) OVER (ORDER BY id) …` (mixed scalar expression exactly as the percentile precedent `:53` uses `abs(?)`; unchanged parameter *types* across all EXECUTEs); EXECUTE `0.5, true, false, 2` → correct rows (ABS(0.5)=0.5); EXECUTE `−1.0, true, false, 2` → rows for α=1 (ABS(−1.0)=1.0 — a **different valid** value; config rebuilt, not frozen); EXECUTE `3.0, …` → §7 error on that execution only; EXECUTE `0.5, …` → correct again. Assert both in-sentence results *and* cache exclusion: no reused compile and no runtime-specialization admission. Plus unit rows in `prepared_ewm_cache_test.go` replicating **both** percentile tests: `shouldCachePrepareCompile` false for ewm-marker plans / true for literal-policy plans (`prepared_percentile_cache_test.go:45-50` shape) **and** the mixed runtime-specialization shape (`:53`, `TestPreparedPercentileDisablesMixedRuntimeSpecializationCache` — `hasEwmParams` recorded at prepare, `cw.runtimeCacheTarget` nil, compile nil); `PreparedPlanHasEwmParams` positive/negative cases |
| **Config channel producer→consumer (rev-5 finding 3)** | UT: `constructEwmConfig` returns encoded bytes through `MakeAggFunctionExpression`'s `extraConfig` (`operator_test.go:483` pattern: valid → different-valid → invalid → valid; no plan mutation) and validation errors surface at construction; `EncodeEwmConfig`/`DecodeEwmConfig` round-trip rows (incl. invalid bytes → the §7 error); **end-to-end**: the decoded config drives execution — an operator run asserts the selected α/h/adjust/ignoreNA/minPeriods appear in the row results (e.g. the profile row under α=0.5/adjust=false vs a different config) |
| Binder validations (§7) | UT in `pkg/sql/plan` (SQL→bind-error table) incl. all signature/decay rows |
| Regression Invariant 10 | Existing `pkg/sql/colexec/window` + `pkg/sql/plan/function` + parser suites rerun; window-suite BVT (`mysql_compat_window_*`) around the dispatch insertion point |
| Red/green discipline | The same focused tests recorded on base (probes error 20105/parse) and post-fix |
| Performance Invariant 9 | By construction (single-pass O(1) state; compensated adds ~2×); state-size assert across a large synthetic partition; full-scale benchmarking deferred to PR CI |
| Oracle cross-checks | Every §5 vector appears in the archived rev-8 simulator (`ewm_rev8_oracle_simulator.py`; log `ewm_rev8_oracle_run.log`, 123/123 PASS, exit 0, under the strict range-safe comparator — the 116 rev-7 rows byte-identical by log diff plus the seven rev-8 rows) with hand-derived, direct-closed-form and 900-digit independent cross-checks; the seventh review's own fault values — including the watchdog-evidenced 6/6-permutation + sequence HANG, the 33% reweights on both merge sides, the order-dependent covariance 0.0 vs 3.333333333333333e299, and the narrowed-Infinite std — were reproduced on the archived rev-7 simulator (`repro_review7_rev7.log`) before the fixes; the rev-9 supersession: every §5 vector also appears in the archived rev-9 simulator (`ewm_rev9_oracle_simulator.py`; log `ewm_rev9_oracle_run.log`, 131/131 PASS, exit 0 — 122/123 prior rows byte-identical with the single tolerance-level shift recorded in `rev9_preservation_diff.log`; the eight new rows carry the eighth review's exact expectations; the archive rerun reproduces the log byte-for-byte) and the four eighth-review fault values were reproduced on the archived rev-8 simulator (`repro_review8_rev8.log`) before the fixes; the rev-11 supersession: every §5 vector also appears in the archived rev-11 simulator (`ewm_rev11_oracle_simulator.py`; log `ewm_rev11_oracle_run.log`, 153/153 PASS — the 145 preserved rows byte-identical with ZERO ulp shifts, `rev11_preservation_diff.log`; the archive rerun reproduces the log byte-for-byte) and the four tenth-review fault values were reproduced on the archived rev-10 simulator (`repro_review10_rev10.log`, 4/4 findings observed, exit 0) before the fixes; the rev-12 supersession: every §5 vector also appears in the archived rev-12 simulator (`ewm_rev12_oracle_simulator.py`; log `ewm_rev12_oracle_run.log`, 161/161 PASS — the 153 preserved rows BYTE-IDENTICAL with ZERO ulp shifts, `rev12_preservation_diff.log`; the archive rerun reproduces the log byte-for-byte) and the three eleventh-review fault values were reproduced on the archived rev-11 simulator (`repro_review11_rev11.log`, 3/3 findings observed, controls green, exit 0) before the fixes; the rev-13 supersession: every §5 vector also appears in the archived rev-13 simulator (`ewm_rev13_oracle_simulator.py`; log `ewm_rev13_oracle_run.log`, 165/165 PASS — the 161 preserved rows BYTE-IDENTICAL with ZERO ulp shifts, `rev13_preservation_diff.log`; the archive rerun reproduces the log byte-for-byte) and the three twelfth-review fault values were reproduced on the archived rev-12 simulator (`repro_review12_rev12.log`, 3/3 findings observed, controls green, exit 0) before the fixes; the rev-14 supersession: every §5 vector also appears in the archived rev-14 simulator (`ewm_rev14_oracle_simulator.py`; log `ewm_rev14_oracle_run.log`, 168/168 PASS — the 165 preserved rows verified 164 BYTE-IDENTICAL plus ONE documented lift: the `[1.6e308,1.6e308,1.7e308]` merged mean moved from its tolerated one-ulp-low dd lead to its own pinned complete-mean literal `1.6432432432432433e+308`, the finding-1 semantics on a NONZERO high word (`rev14_preservation_diff.log`), and both thirteenth-review fault values were reproduced on the archived rev-13 simulator (`repro_review13_rev13.log`, 2/2 findings observed, controls green, exit 0) before the fixes; the rev-15 supersession: every §5 vector also appears in the archived rev-15 simulator (`ewm_rev15_oracle_simulator.py`; log `ewm_rev15_oracle_run.log`, 169/169 PASS — all 168 prior rows BYTE-IDENTICAL with zero shifted rows, `rev15_preservation_diff.log`; the archive rerun reproduces the log byte-for-byte), and the fourteenth-review continuation faults were reproduced on the archived rev-14 simulator (`repro_review15_rev14.log`, 4/4 initial/incoming continuation faults observed with the reorder controls green, exit 0) before the fixes; the rev-16 supersession: every §5 vector also appears in the archived rev-16 simulator (`ewm_rev16_oracle_simulator.py`; log `ewm_rev16_run.log`, 170/170 PASS, exit 0 — the 169 prior rows verified 168 BYTE-IDENTICAL plus ONE documented reassessment: the r14F2 `[M/2,0,0]`×`[1+2^-52,1,1]` all-scalar control moved from its one-ulp-low pin `4.435200687854932e+291` to the independently re-derived EXACT complete covariance `4.4352006878549327e+291`, per the review's own reassessment instruction (`rev16_preservation_diff.log`); the archive rerun reproduces the log byte-for-byte), and the fifteenth-review faults were reproduced on the archived rev-15 simulator (`repro_review16_rev15.log`, exit 0 — 20 fault observations: the (a) six-peer-order × two-policy × two-mode violations at `-0.6111111111111112`, the (b) exactly-`0.0` in 96/96 executions, and the four poisoned continuations, with the two translated controls green) before the fixes; the rev-17 supersession: every §5 vector also appears in the archived rev-17 simulator (`ewm_rev17_oracle_simulator.py`; log `ewm_rev17_run.log`, 171/171 PASS, exit 0 — ALL 170 prior rows BYTE-IDENTICAL with zero shifted rows and NO reassessed literal, `rev17_preservation_diff.log`; the archive rerun reproduces the log byte-for-byte), and the sixteenth-review faults were reproduced on the archived rev-16 simulator (`repro_review17_rev16.log`, exit 0 — 488 fault observations: the lattice-routed one-sided-scaled batches at EXACTLY `0.0` in 96/96+96/96 executions, the anchored `4.25e+307` in 96/96+96/96 including the swapped streams, the routing-boundary lattice-side witness at `0.0` and the eight poisoned continuations, with the routing-boundary normal-side control and the unscaled fifteenth-review witness green) before the fixes; the rev-18 supersession: every §5 vector also appears in the archived rev-18 simulator (`ewm_rev18_oracle_simulator.py`; log `ewm_rev18_run.log`, 172/172 PASS, exit 0 — ALL 171 prior rows BYTE-IDENTICAL with zero shifted rows and NO reassessed literal, `rev18_preservation_diff.log`; the archive rerun reproduces the run log byte-for-byte), and the seventeenth-review faults were reproduced on the archived rev-17 simulator (`repro_review18_rev17.log`, exit 0 — 20 fault observations: the anchored-routed tied batch [B,B,B,B,B,−B]×[−H,M,1,H,−M,−1] at EXACTLY `0.0` across both decay modes, both adjust policies and both stream orientations, the lattice-routed ten-pair 2^−512-scaled batch at cov AND corr `0.0` in the stated order, and the eight poisoned continuations at EXACTLY `0.0`, with the reversed-order and normal-route boundary controls green — the order-dependence proving the two-word pile compression) before the fixes; the rev-19 supersession: every §5 vector also appears in the archived rev-19 simulator (`ewm_rev19_oracle_simulator.py`; log `ewm_rev19_run.log`, 173/173 PASS, exit 0 — all 172 prior rows BYTE-IDENTICAL with the pre-row full run byte-equal to the frozen rev-18 log, `rev19_preservation_diff.log`; the archive rerun reproduces the log byte-for-byte), and the eighteenth-review faults were reproduced on the frozen rev-18 archive (`repro_review19_rev18.log`, 16/16 fault combinations with 32 green reordered controls, exit 0); the rev-20 supersession: every §5 vector also appears in the archived rev-20 simulator (`ewm_rev20_oracle_simulator.py`; log `ewm_rev20_run.log`, 174/174 PASS, exit 0 — all 173 prior rows BYTE-IDENTICAL with the pre-row full run byte-equal to the frozen rev-19 log, `rev20_preservation_diff.log`; the archive rerun reproduces the run log byte-for-byte), and the nineteenth-review faults were reproduced on the frozen rev-19 archive (`repro_review20_rev19.log`, 72 fault observations against 59 green controls, exit 0) before the fixes; the rev-21 supersession: every §5 vector also appears in the archived rev-21 simulator (`ewm_rev21_oracle_simulator.py`; log `ewm_rev21_run.log`, 175/175 PASS, exit 0 — all 174 prior rows BYTE-IDENTICAL with the pre-row full run byte-equal to the frozen rev-20 log, `rev21_preservation_diff.log`; the archive rerun reproduces the run log byte-for-byte), and the twentieth-review merged-tail faults were reproduced on the frozen rev-20 archive (`repro_review21_rev20.log`, 26 fault observations against 26 green controls, exit 0) before the fixes; BVT results are generated-then-reviewed literals cross-asserted against these |

Test file locations:
- unit: `pkg/sql/parsers/dialect/mysql` (parse shapes), `pkg/sql/plan/function`
  (registry/checkFn signature table), `pkg/sql/plan` (binder), `pkg/frontend`
  (`prepared_ewm_cache_test.go`, prepared sequence), `pkg/sql/compile`
  (constructEwmConfig), `pkg/sql/colexec/aggexec` (encode/decode rows),
  `pkg/sql/colexec/window` (operator: all §5 oracle
  vectors, permutation/clock/carry/precision/zero-mass/merge-cov/
  subnormal-share/constant-merge/float-extreme/**range-safety rows**,
  the **rev-7 rows** (six permutations; weighted-difference vector;
  both below-subnormal asymmetries; retained-magnitude/decay-recovery),
  the **rev-8 rows** (anchored-scatter permutations + low-weight
  arrival; exact-scaling/inexact-scaling on both merge sides;
  compensated cross accumulation incl. the 720-permutation
  invariance; ewmStd at the retained-4e308 rows; the **rev-9 rows**
  (fractional-denominator subnormal share; three-scale cascade in
  both peer orders; adjacent-float translation at the tied batch and
  the singleton merge; sub-lattice std; representable-variance
  merge), plus the four
  **lifecycle rows** — split-peer exactly-once, partition/
  generation isolation, LIMIT-then-Reset, cancellation/error reuse —
  driven at the container level).
- BVT: `test/distributed/cases/window/ewm_statistics.sql` (+ `.result`).

## 11. Decision log (v1, rev 2–8; superseding earlier decisions where noted)

1. `ewm*` are window functions on a dedicated operator path — merge-free,
   single-pass serial state (aggregate machinery rejected).
2. Decay declared by: numeric α ∈ (0,1] (row mode) **or** `'halflife'` + h > 0
   (event-time mode); com/span/row-halflife remain documented conversions
   over α. **Decay is a required argument** (rev 4 finding 7; §4 table) —
   bind error when missing.
3. All statistics are mass-normalized with tracked per-batch arrival totals
   (`1`/`k` first; `kα`/`k` row-mode later; `−expm1(−ln2·Δt/h)`/`k`
   event-time later); gap-free row-mode `W ≡ 1` reproduces the plain
   recurrence and the issue's profile exactly.
4. Variance/covariance use exact centered moments: batch-local
   **unweighted compensated** statistics (Neumaier; identity fast path for
   bit-identical observations) merged by the **scale-safe mass-form
   parallel combination** — power-of-two normalized weights, merge-level
   equality fast path, incoming cross-scatter `sxy` retained — with the
   explicit `W == 0` degenerate branch; raw-moment forms,
   previous-mean-dependent batch terms, absolute-moment state,
   per-observation `A/k` materialization, the rev-4 absolute-domain
   merge and the f-form convex merge are all forbidden (rev-4 findings
   2/3/6 + rev-5 findings 1/4/5; superseding rev 3's absolute-state +
   `a_i` formulas and rev 4's mass-multiplied merge).
5. Event-time mode: exactly one **ascending** ORDER BY expression
   (findings 4/5); transition factor `exp(−ln2·Δt/h)` group-to-group
   (telescoping); Δt in the key's native domain — **unsigned-magnitude
   for integers** (finding 5), exact decimal (overflow → §7 error),
   int64 µs temporal, and **quotient-domain ratio for FLOAT/DOUBLE keys
   (`r = b/h − a/h` when the subtraction overflows; rev-5 finding 6)** —
   one float64 conversion per term; arrival mass
   `−expm1(−ln2·r)`; same-instant observations share the instant's
   total mass with scatter retained (rev-2/3 findings 3/7); non-finite
   keys and policy values are rejections (§7).
6. Ties are order-free arrival batches; outputs identical under
   intra-batch permutation — exactly for constant batches, within the
   compensated bound otherwise; permutation tests compare unrounded
   values (rev-4 finding 3; superseding rev 3's byte-identity wording).
7. Explicit frames and no-ORDER-BY OVER are bind-time errors.
8. NULL outputs are policy-only: the C-gate
   (`C < max(1, minPeriods)`) and zero-variance correlation; decayed and
   zero-mass states emit the carried state (rev-4 finding 6; W==0 is not
   a NULL rule).
9. Policy constants resolve at the compile that builds the operator's
   physical scope, and **statements with an ewm prepared marker are
   excluded from both compile-reuse caches** — prepare-compile reuse via
   the percentile mechanism (`PreparedPlanHasEwmParams` +
   `shouldCachePrepareCompile`, `computation_wrapper.go:2802`,
   `mysql_cmd_executor.go:3196`, `computation_wrapper.go:1663`;
   `compile.go:367` documents the reuse) **and** runtime-specialization
   reuse via `hasEwmParams` in the `:1916` admission (retrieval
   `:1859-1868`, return `:1969`; rev-5 finding 2) — so each EXECUTE
   recompiles per-execution configs (rev-4 finding 1 + rev-5 finding 2);
   the cached plan proto is never mutated; literal-policy statements
   cache normally.
10. Parser goes the five-keyword route with `window_spec_opt` +
    `not_keyword` identifier compatibility (Alt F rejected with
    reproduced evidence).
11. Adjust=true uses fresh per-observation mass in both modes
    (pandas-documented convention); adjust=false uses per-batch totals
    with the cancellation-safe `−expm1` in event mode.
12. Bounded moving frames are out of scope by construction (§6).
13. Event-time instants = SQL peers under the single-key bind rule; the
    executor consumes the time vector via the RANGE frame's last-vector
    read (Alt K rejected).
14. DESC event-time is a bind error (Alt J rejected); row-decay mode is
    direction-independent.
15. (Superseded by 4/8:) the rev-3 snapshot field is removed — the
    normalized state *is* the carried estimate.
16. Row-mode clocks (rev-4 finding 2): inaF spans
    first-row→previous-observation-batch-last-row (all rows counted);
    inaT one λ per observation batch; the policies coincide when every
    row is observed.
17. Batch accumulation: Neumaier compensated sums, unweighted means
    (`Σx/k`), identity fast path; ~2× adds, O(1) registers; batch order
    deterministic (sorted), intra-batch order only bounded (finding 3).
18. Cache ownership: per-execution resolution *requires* the exclusion
    from **both** caches for marker statements (findings 1 + rev-5
    finding 2); operator-runtime policy evaluation rejected (Alt N).
19. Config channel (rev-5 finding 3): the EWM configuration travels as
    **bytes** — `constructEwmConfig` encodes once
    (`aggexec.EncodeEwmConfig`), `MakeAggFunctionExpression` carries them
    in `extraConfig` (percentile-shaped; the AggFuncExecExpression has no
    struct field), and the EWM dispatch decodes its own expression's
    bytes via `GetExtraConfig()` — never through
    `newAggregateExecutor`'s `SetExtraInformation`, a path this dispatch
    bypasses; the bytes are immutable and shared across operator copies
    and hold no execution state.
20. Merge scale (rev-5 findings 4/5): merge weights are computed in the
    power-of-two normalized mass domain (bit-identical to the plain mass
    form in the normal range, so every pinned oracle value survives;
    comparable subnormal masses keep exact relative shares; no product
    overflows while its result is representable), constant signals are
    exact across merges through the bitwise-equality fast path, and a
    merge whose true result exceeds the float64 range yields ±Inf / Inf
    per IEEE — the f-form merge is rejected (Alt O), the rev-4
    absolute-domain merge superseded (Alt Q).
21. FLOAT/DOUBLE event keys (rev-5 finding 6): the decay exponent is
    formed in the quotient domain (`r = b/h − a/h` on subtraction
    overflow; saturation at infinite `c`), so every **finite** key pair
    decays correctly across the full double range; non-finite keys and
    non-finite policy values are defined rejections (§7).
22. Layered range safety (rev-6 findings 1/3, amended rev 7): the
    primary shapes (scaled mass-form merge; direct compensated batch
    statistics) stay untouched and bit-identical, and exactly-tuned
    *detected* fallbacks recompute where the primaries are
    arithmetic-defective (convex share means; weight-inside-product
    moment terms; the midpoint-anchored mean of decision 26; per-term
    regrouped scatter sums; and the gated absolute-mass layer of
    decision 27 with its retained-magnitude extended forms) — no
    single unguarded float64 expression is trusted with the whole
    domain (Alt S rejected).
23. Extreme mass ratios (rev-6 finding 2, amended rev 7): a mass
    contribution is negligible exactly when its weighted *product* is
    below float64 resolution — never because its weight alone rounded
    away (Alt R withdrawn); and the share-based routes are themselves
    only gated fast-paths (decision 27) — the absolute-mass layer,
    which materializes no share at all, preserves every representable
    contribution on both asymmetries, including shares below the
    smallest subnormal (Alt W withdrawn).
24. Correlation and IEEE limits (rev-5/6, amended rev 7): corr is NULL
    on zero or **non-finite** normalized variance (evaluated on the
    *emitted* value); results beyond float64 are honest IEEE values
    converted **only at emission** (±Inf by sign; means computed
    convexly never overflow while representable); the oracle/test
    comparator is itself range-safe (NaN never matches; ±Inf matches
    only by sign).
25. EWM execution lifecycle (rev-6 finding 5, amended rev 7): the EWM
    state carries its own monotonic exactly-once peer-group cursor
    with an in-flight group cache (Alt T rejected); `freeEwmState`
    joins the generation/Reset/Free aggregate lifecycle sites
    (`window.go:371`, `types.go:195/:217`) and the dispatch's own
    partition-boundary check resets partition-scoped account
    statistics and clocks (corrected wiring: `window.go:988/:992/
    :997`-style, directly inside `processEwmFuncRange`); the
    generation-scoped absolute cursor is never reset by partition
    transitions; LIMIT interruption, cancellation and mid-generation
    errors leave no partial state and a reused container behaves as
    new; cancellation is polled **in-loop at a bounded interval**
    (the shared `checkCanceled`, 1024) inside every EWM
    scan/statistical/recompute pass; five deterministic lifecycle
    tests are the planned minimum (§10).
26. Midpoint-anchored batch mean (rev-7 finding 1): the batch-mean
    fallback anchors at `min/2 + max/2` and accumulates compensated
    per-observation `(x_i − a)/k` shares — a permutation-invariant
    anchor, a bounded-to-half-range deviation domain and a convex
    result mean make every intermediate representable in any peer
    order (Alt U withdrawn; the shifted recurrence is dead).
27. Gated share layers and the absolute-mass merge (rev-7 findings
    2/3): the primary form commits unchanged whenever finite; the
    share layers compute into temporaries and commit only when they
    neither drop a positive-mass side nor produce a non-finite result
    from finite inputs; every failure rebinds to the absolute-mass
    layer in which means are mass-normalized convex combinations and
    every moment term is a weighted-difference identity — no raw
    difference, no materialized share (Alts W/X withdrawn; §5 (C)).
28. Retained out-of-range magnitude (rev-7 finding 4): batch and
    state centered moments beyond float64 live in extended
    (mantissa, exponent) form through state and merge; results back
    in range resume plain float64; the honest ±Inf conversion and the
    corr NULL rule act **only at emission**, after weighting had its
    chance to recover a representable value.
29. Bounded in-loop cancellation (rev-7 finding 6 of record): the EWM
    polls the shared `checkCanceled(proc, iteration)` helper at the
    cumulative paths' own interval (1024) inside every long-running
    loop, so a single partition-sized peer group cannot run unbounded
    between Call iterations; per-group publication stays atomic
    (Alt X withdrawn; §9 lifecycle, §10 test (e)).
30. Anchored batch deviations (rev-8 finding 1): whenever a raw
    mean-deviation list is non-finite — a finite mean does not promise
    finite raw deviations — the scatter is computed from exact-midpoint
    anchored deviations via the identity
    `Σ(x−m̄x)² = Σ(x−a)² − k·δx²` (per-side anchors, the cross pairing
    the same per-side deviations and offsets), in the extended
    compensated form; the extended normalization REQUIRES finite
    normalized mantissas — a non-finite operand is a defined internal
    error, never a loop (Alt Y withdrawn).
31. Exact mass scaling and share reconstruction (rev-8 finding 2):
    the power-of-two merge normalization runs only when it
    round-trips bit-exactly on both nonzero weights; degenerate OR
    inexact scalings build shares from the raw masses, and a
    subnormal share commits only when it reconstructs its mass
    bit-exactly — otherwise that side rebinds to the absolute-mass
    layer, where every contribution forms as a mass·value product
    with the mass exponent preserved; pinned on both merge sides
    (incoming odd-subnormal arrival and the engine-reachable retained
    odd-subnormal state) (Alt Z withdrawn).
32. Compensated extended accumulation (rev-8 finding 3): all
    extended moment sums — batch fallbacks and merge (C) assemblies —
    are Neumaier two-sums on the aligned mantissas with the exact
    residual folded into a second extended register; the result
    differs from the exact sum by at most one rounding of the result
    plus `k·u²·max|term|` at the running-sum scale, and the alignment
    information limit is the ~1074-order subnormal lattice of the
    running partial sum (superseding the rev-7 ~1020-order claim)
    (Alt AA withdrawn).
33. ewmStd from the retained moment (rev-8 finding 4): the standard
    deviation is the square root applied to the retained scaled
    moment BEFORE the float64 narrowing (exact power-of-two exponent
    split; one float64 mantissa sqrt); variance and standard deviation
    may therefore reach different honest verdicts on the same row
    (var +Inf, std EXACTLY `2e154`), and an in-range state resumes the
    plain float64 sqrt (Alt AB withdrawn).

34. Never-commit-subnormal shares (eighth-review finding 1): a share
    below the smallest normal value never commits in the float64
    share layers on either side; the rev-8 reconstruction certificate
    also rounded and admitted a 25%-understated share. Every
    subnormal share rebinds to the absolute-mass layer (Alt AC
    withdrawn; the G3 gate of §5 is rewritten).
35. Cascading compensation (eighth-review finding 2): every extended
    accumulation keeps rev-8's level-0 two-sum loop and first
    residual register bit-identical and cascades each fold's exact
    discarded bits into the next bounded register (depth 6); the
    broken-order covariance at three tied scales is fixed in BOTH
    orderings (`L^2/5`) and the peer-permutation contract is
    re-anchored on cross-scale order-robustness (Alt AD withdrawn;
    the falsified ~1074-order claim in §12 is superseded by the
    cascade-depth bound).
36. Carried mean tails (eighth-review finding 3): the account carries
    (hi, lo) mean parts and each batch a compensated tail; merges
    compute ν from compensated mass·mean numerators, keep `fl(ν)`
    bit-identical, and subtract the δ² (cross δx·δy) recentering —
    the centered statistics are moments of the UN-NARROWED mean
    while the emitted mean stays the narrowed float (Alt AE
    withdrawn; Decision 4's "exact centered moments" includes the
    centering correction).
37. Subnormal-lattice retention (eighth-review finding 4): deviations
    at/below 2^-511, subnormal nonzero moments, and same-scale merge
    deviations rebind the affected statistic to the extended
    channel; the store keeps nonzero results that would narrow to
    subnormal-or-zero, and ewmStd/ewmCorr read the retained values —
  the representable verdicts survive at both magnitude extremes
  (Alt AF withdrawn; Decision 33 extended downward).
38. Complete-mean merge algebra (ninth-review finding 1): the merged
    mean tails are formed FIRST (the same compensated mass·mean
    numerators as rev 9; `fl(ν)` bit-identical) and every merge
    deviation is measured between COMPLETE means — state hi+lo,
    arrival hi+lo, merged `fl(ν)`+tail; the δ² end correction is
    GONE (the rev-9 chain had no cross term); every equality fast
    path requires hi AND tail equality on both sides (Alt AG
    withdrawn; Invariant 21).
39. Accumulator domain (ninth-review finding 2): the cascading
    accumulation handles both alignment directions symmetrically
    (the old accumulator cascades intact when a larger incoming
    term's alignment underflows it) and the capacity is 128
    registers — every fold residual descends ≥ 53 binary orders per
    level across a relevable span ≤ 6296 exponent orders, at most
    ~120 levels (Alt AH withdrawn; supersedes Decision 35's depth-6
    figure; Invariant 23).
40. Weighted-product retention (ninth-review finding 3): the (P)/(F)
    layers additionally rebind to (C) whenever a nonzero moment's
    float64 product with its side's weight would land subnormal
    (exponent sum ≤ −1021); the extended mass·moment product,
    division and store keep every representable verdict, and ewmVar
    still honestly emits 0.0 for unrepresentable variances whose
    std IS representable (Alt AI withdrawn; extends Decision 37
    downward through the weight product).
41. Retained-moment correlation (ninth-review finding 4): the
    geometric normalization is computed from the retained moments —
    extended square roots, product and quotient — narrowing only the
    final verdict; the float64 view keeps driving the NULL/
    finiteness routing, and plain float64 states stay bit-identical
    to the old form (Alt AJ withdrawn; Invariant 22).
42. Double-double complete means (tenth-review finding 1): through
    the absolute-mass layer the merged means, sources, subtractions
    and weighted products carry (hi, lo) pairs of extended words —
    ddsum/dddiv/ddsub/dd_mul with the exact-alignment fold chain;
    the stored mean keeps `fl(ν_hi)` and the tail narrows the dd
    residual once; the extended products never overflow, so the
    representable-difference fast paths and the q·q/mass fallback
    are gone (Alt AK withdrawn; Invariant 21 rebuilt; extends
    Decision 38 into the extended layer).
43. Weighted-deviation retention gates (tenth-review finding 2):
    the (P)/(F) rebind decisions additionally read the six
    weighted deviation products per branch — squares and crosses
    against the side weights or shares — at frexp-sum −1021, so
    zero-moment merges whose deviation products would underflow
    still reach the extended channel before the assembly
    multiplies (Alt AL withdrawn; Invariant 25; extends Decision 40
    to the deviation terms).
44. Exact alignment two-sums (tenth-review finding 3): every fold's
    two-sum runs in a scaled normal-mantissa domain for gaps in
    [1022, 2043] and cascades the smaller operand intact beyond
    2043 — subnormal-lattice ldexp bits are recorded exactly, never
    quantized away; capacity stays 128 (Alt AM withdrawn;
    Invariant 23 rebuilt).
45. Cascaded finite chains with the non-finite fold (tenth-review
    finding 4): the plain float64 compensated chains expose
    (sum, comp, comp_lo) with register-cascaded residuals; a
    non-finite residual folds once into the first register and
    never enters the loop (a NaN residual would otherwise never
    terminate); benign sums stay bit-identical to (sum, comp)
    (Alt AN withdrawn; Invariant 24; extends Decision 35 to the
    plain float chains).
46. Error-free complete-mean products (eleventh-review finding 1):
    every weighted product in the (C) complete-mean sums forms
    through xtwoprod (the xmul leading word plus its EXACT residual
    pair), the sums run through ddsum_e, and dddiv keeps the EXACT
    remainder; the carried-moment assembly products stay
    one-rounded (the shared reconstruction owns their losses) (Alt
    AO withdrawn; Invariant 21 rebuilt; r12F1).
47. Shared bounded restoration (eleventh-review finding 2): both
    cascade sites — the plain chains' comp/comp_lo words and the
    accumulator's register restore — use ONE
    descending/deferred/shared procedure that keeps every fold loss
    until its own final rounding, retries intact items only after a
    total-changing event, and flushes un-hostable residues through
    the documented information limit; termination is structural
    (Alt AP withdrawn; Invariant 24 extended; r12F2).
48. Range-safe two-products (eleventh-review finding 3): the Dekker
    split and the raw k·m̄x products form in the MANTISSA domain with
    ldexp/pair reassembly at ea+eb — exact wherever fl(a·b) is
    finite; the raw-domain split is impossible exactly at the top
    band [2^1024·(1−2^-27), maxfloat64], so no raw-domain caller
    exists above that frontier; a negative retained variance stays
    an internal error — NO output clamping (Alt AQ rejected;
    Invariant 26; r12F3).
49. Canonical complete-mean normalization (twelfth-review finding
    1): every merge layer — (P), (F) and (C) — normalizes the
    complete mean before committing and emitting it; a narrowed
    high word that cancels to exactly ZERO carries its tail's
    content into the narrowed word (the tail IS the complete value
    there), benign commits are zero-ops and the emitted means stay
    bit-identical (Alt AR withdrawn; Invariant 27; r13F1).
50. Retained batch residuals at the overflow boundary
    (twelfth-review finding 2): the complete batch residual also
    forms in the retained pair channel and commits there whenever
    the float64 tail path cannot carry it — k·m̄x overflow keeps the
    finite mean/std while variance emits its honest +Inf (Alt AS
    withdrawn; Invariant 28; r13F2).
51. Retained below-lattice tails (twelfth-review finding 3): an
    exact batch tail below the subnormal lattice commits as the
    retained pair, centers the extended scatter on the un-narrowed
    mean, stays retained through the state commit, and the
    retained-tail routing sends subsequent merges to the pair-native
    absolute layer; narrowing happens only at emission (Alt AT
    withdrawn; Invariant 28; r13F3).
52. Complete-mean normalization for every high word
    (thirteenth-review finding 1): the merge layers — (P), (F) and
    (C) — fold (hi, tail) through the exact two-sum fold before
    every commit and emission: hi′ = fl(hi + tail), tail′ = the
    exact residual; float tails fold via the exact two-sum (an
    identity whenever the tail stays below the half-ulp boundary),
    retained pair tails via the exact pair two-sum with the fold
    loss retained, and a non-finite fl(hi+tail) keeps the pair raw.
    164/165 preserved rows verified byte-identical; the boundary
    pairs fold to the correctly rounded complete mean
    ([2^53, 2^53+2, −2^53+2] → exactly 1.5; the dd lead 4·2^545
    with the exact +2^545 remainder → exactly 5·2^545; the in-suite
    one-ulp-low dd word → its own pinned literal) (supersedes
    Decision 49's zero-high-word-only scoping; Invariant 29;
    r14F1).
53. Mixed retained/scalar cross-centering (thirteenth-review
    finding 2): whenever EITHER mean tail is a retained pair while
    sxy stays a finite scalar, the complete cross moment re-forms
    through the exact chain — twosum-exact per-row deviations,
    error-free products, error-free −k·τx·τy, the ddsum/dddiv
    cascade, ONE final fold; both tails scalar keep the pinned
    float64 product and an already-retained sxy keeps the xsub
    route (Alt AV withdrawn; Invariant 30; r14F2).
54. (Rescinded in rev 15 — fourteenth-review finding 1.) Raw
    W == 0 batch state commits keep the rev-9 pinned narrowed
    word plus the compensated tail (scope correction, rev 14):
    the claimed tolerance — the raw tail's reconstruction fold
    loss documented instead of repaired — was a live defect. The
    committed pair fed every continuation merge: the review-order
    cross-scale batch read the "correction" at −0.2 against its
    mean 0.2, its continuation merges emitted 0.0 where
    fl(1/7)/fl(0.1) reach as the initial state and fl(2/11)/
    fl(1/6) as the incoming state, and only the [H,−H,M,−M,1]
    reorder was correct — order dependence inside a single
    batch. Superseded by Decision 55 and Invariant 31.
55. Genuine batch mean-tail residuals (fourteenth-review finding
    1): the batch residual τx/τy is rebuilt from ALL compensated
    registers — every register enters the cascaded reconstruction
    as its own list item, with no premature (cx + cxl) /
    (ca_ + ca_lo) float64 folds — so the committed (word, tail)
    is a genuine complete-mean split in every peer order; the raw
    single-batch emission keeps the pinned rev-9 narrowed word
    fl(fl(Σregisters)/k) (the r11F4 pins), while centering,
    storage and every merge consume the true pair; the
    retained-pair fallbacks and merge-layer folds are unchanged
    (Invariant 31; Alternative AU rewritten; r15F).
56. Common batch-moment error-free formation (fifteenth-review
    finding): the finite batch-moment contract carries
    subtraction and multiplication residuals into compensated
    accumulation for scalar and retained inputs including
    zero-tail cases (Invariant 32): per-row twosum deviations,
    per-product xtwoprod pieces, error-free −k·τx² /
    −k·τx·τy numerator pieces gated by VALUE, and ONE final
    quotient fold; the scatters keep the dd pipeline
    (positive-dominant piles), the cross cascades through the
    full register accumulator (full cancellation admitted —
    beyond any two-word window); the rev-14 retained-tail-only
    restriction is superseded, and the r14F2 [M/2,0,0] sentinel
    literal is reassessed to the independently re-derived exact
    complete moment 4.4352006878549327e+291 (the equal-tails
    float64 product and the per-fixture detection-branch shape
    were both rejected: the shared contract has neither).
57. Common batch-moment formation on the lattice and anchored
    routes (sixteenth-review finding): the SAME error-free helpers
    and the SAME contract as the normal route — twosum deviations
    (against the narrowed mean on lattice sides, the exact range
    midpoint on overflowing sides, range safety untouched),
    xtwoprod pieces, the shared xanchor_k_items −k·δx·δy
    correction folding the exact delta parts (keeping the mixed
    −k·δx·τy term), and the ddsum/dddiv/xadd quotient keeping the
    exact remainder (the first cut's restored-fold xdiv committed
    one ulp high on the rev-8 low-weight-arrival fixture and was
    corrected before archiving) — run on every route; the routing
    itself (devfin/frexp fence, midpoint anchors, extended-commit
    retention) is unchanged (Invariant 33; r17F; the rev-16 §12
    “established shapes” exemption is corrected to the
    ROUTING only).
58. Cancellation-preserving signed-cross quotient, `xq_cascade`
    (rev 18, seventeenth-review finding): the two routed cross
    sites divide the signed pile through the register cascade —
    the FULL xsum4 cascade feeds the first quotient word, the
    EXACT −q1·d residual (an xtwoprod) re-cascades WITH the pile
    and the two-word quotient keeps its true remainder (dddiv's
    structure generalized to the multi-item numerator). This
    entry SUPERSEDES Decision 57's signed-CROSS quotient
    prescription (the ddsum → dddiv two-word compression folded
    the UNCANCELLED pile before the division and erased the
    cancelable bits in the review's stated orders); Decision 57's
    scatter-quotient contract, its range-safety routing and its
    merged-weighting layer are RETAINED unchanged (Invariant 34;
    r18F; the rev-18 revision cited this decision in §5/§10/§12 —
    the eighteenth review's P3 finding recorded it here).
59. Decision-log realignment (rev 19, eighteenth-review P3): every
    decision this design cites is appended to THIS log — rev 18
    cited Decision 58 without a log entry (§11 ended at Decision
    57, still prescribing the superseded every-route dd quotient);
    this entry records the omission's repair (Decision 58 above,
    with the Decision 57 supersession) and the convention that
    later revisions append their decisions here first.
60. ONE shared genuine complete-residual reconstruction,
    `genuine_tail(obs, k, mean)` (rev 19, eighth-review P2):
    Invariants 35/36 — the complete batch residual (Σx−k·m̄x)/k is
    rebuilt from the ORIGINAL observations (raw xfrexp items in
    the xsum4 register cascade, the k·m̄x subtrahend as its two
    exact xtwoprod words, ONE final xdiv; never a pre-divided
    per-observation quotient, never a second register-sourced
    variant) and committed on BOTH the ordinary and
    overflowing-sum routes with the SAME lane (narrowed xval lead,
    retained pair below the lattice). The rev-18 anchored
    per-quotient tail lane and the ordinary route's register-sourced
    fallback variant are SUPERSEDED; the ordinary route's float64
    fast lane, the rev-13 fallback triggers, every scatter, cross
    formation, routing fence, retention policy and merge layer are
    RETAINED unchanged (r19F; the merged continuations healed with
    all 172 preserved rows byte-identical).
61. ONE shared tail-retention rule, `lane_retains` (rev 20, ninth-review P2 finding 1; Invariant 37): a batch commit lane keeps the extended (pair) tail whenever narrowing the pair would lose ANY content — re-normalizing the narrowed lead does not reproduce the pair covers the nonzero lead quantized onto the subnormal lattice AND the below-lattice flush (the rev-19 "quantizes to 0.0 with a nonzero mantissa" zero-only trigger is superseded: it committed the narrowed u for the tied batch [B, B+2u, B+8u] at B = 2^-1021, u = 2^-1074 — genuine tail fl(4/3)·u — and emitted std 4u in all 48 executions against the exact 104u²/9 → 3u); exact shapes (zero, an exact subnormal word, every normal lead, the honest ±Inf out-of-range lead) re-normalize to themselves and keep the narrowed scalar lane; applied at every batch commit lane on BOTH streams and BOTH routes (r20F; all 173 preserved rows byte-identical, NO reassessed literal).
62. ONE shared complete-mean packing, `pack_remainder` (rev 20, ninth-review P2 finding 2; Invariant 38): every normalization/merge boundary — (P), (F), (C), the absolute-mass merge and the pair-native normalization — enters the FULL primary xfold(extended lead, −narrowed word) result into the tail accumulator TOGETHER with its exact fold loss and the caller's existing low word (rev 19 discarded the primary result and the singleton sequence [0], [2u], [4u] lost ≈u/3 of its committed complete mean at the first merge, landed 3u at the final against 20u/7 and emitted std 2u against the exact 104u²/49 → u; the adjust=false lane also committed the fl(2.5u) = 2u tie); benign exact folds append NOTHING, preserving every pinned byte (r20F).
63. The public finite-sample contract (rev 20, ninth-review P2 finding 3; Invariant 39): a row sample (x or y) whose float64 value is non-finite is a DEFINED execution rejection — `invalid input: ewm sample values must be finite` — raised by the engine's row pre-scan BEFORE any group is processed and any state is published, on initial, tied or later samples of either stream, with clean reuse after the rejection; composed non-finite samples (these operators' own permitted infinite outputs selected through a subquery) are equally rejected; the internal extended-normalization assertion remains a true internal invariant behind the gate; the §4 numeric-input contract, the §7 catalog and the §5 pass-through clause are aligned to it (the rev-19 leak of the internal AssertionError to callers is superseded; r20F).
64. The ONE shared retention rule owns EVERY tail-storage boundary (rev 21, twentieth-review P2; Invariant 40): `lane_retains` applies at every batch commit lane (rev 20, Decision 61) AND at BOTH merged-state tail stores, x and y, AFTER `pack_remainder` packs the merged tail — the store narrows once and keeps the extended pair whenever re-normalizing the narrowed value does NOT reproduce the pair (the nonzero subnormal tail included), while every exact shape keeps its unchanged narrowed scalar lane; the committed merged (word, tail) stays an exact split of the computed complete mean and continuation merges consume true pair semantics (rev 20 kept the merged pair only when its narrowed value was zero, so the singleton chain [B], [B+2u], [B+4u] at B = 2^-1021, u = 2^-1074 lost its genuine −2u/3 tail, committed the completes B+u, B+3u, B+5u and emitted std 2u against the exact 4u/3, 20u/7, 68u/15 and the exact 104u²/49 → u in all eight row/time × ignoreNA × sign combinations — the translated [0, 2u, 4u] control stayed green; r21F; all 174 preserved rows byte-identical, NO reassessed literal).

## 12. Risks and drawbacks (accepted; rev-9 updated)

- **Design-only revision:** no engine test has run; 'planned' entries are
  pre-approval plans.
- **rev-11 increment:** the plain-chain non-finite fold deliberately
  propagates ±Inf/NaN residuals EXACTLY like the old
  single-compensation form (branches the range-safety gates reject
  anyway) — the preserved `[1.6e308,1.6e308,1.7e308]` rows verified
  bit-identical, and loop termination is now guaranteed; the spawner
  of that state is the rev-10 mean-tail numerator lists whose
  `twoprod` overflows only in branches already committed to (C).
  The rev-8 "~1074-order" alignment figure in §5's acceptance table
  is superseded twice (rev-9 cascade; rev-11 exact complements —
  the row now records this). Cost of the dd (C) layer: bounded
  constant work per merge step (a fixed small handful of extended
  pairs per term), same O(1) state (§5).
- **rev-12 increment:** three arithmetic contracts were promoted from
  aspiring claims to pinned ones: (1) every complete-mean weighted
  product is error-free and the quotient remainder exact — preserved
  rows stay byte-identical because power-of-two masses produce exact
  products anyway; (2) both restorations share one structural
  descending/deferred bounded procedure (the earlier countdown
  variant saturated on preserved 720-permutation rows and was
  withdrawn before adoption — Alt AP); (3) the Dekker split and the
  raw k·m̄x products live in the mantissa domain, exact wherever
  fl(a·b) is finite, with the impossible raw top band documented and
  NO output clamping. The restores add a bounded constant per fold;
  state stays O(1) (§5); no engine test has run (§10 stays
  post-approval).
- **rev-13 increment:** three normalization/retention contracts were
  pinned: (1) the canonical complete-mean normalization reconciles
  the bit-identity promise with correct cancellation behavior —
  benign commits are provably zero-ops and all 161 preserved rows
  stayed byte-identical; (2) the batch residual is also formed in
  the retained pair channel and commits there whenever the float64
  tail path cannot carry it, keeping [M,0,0] finite mean/std with
  an honest +Inf variance and no AssertionError; (3) below-lattice
  exact tails stay retained pairs through centering, the state
  commit and pair-native merges (continuation mean exactly 2u).
  The retained channel adds bounded constant work per batch/merge;
  state stays O(1) (§5). The midpoint-anchored overflow branch
  keeps its float64 tail by construction — the anchored identity
  Σ(x−a)² − k·δx² materializes no k·m̄x product and its anchor-scale
  tails cannot land below the subnormal lattice, so the retained
  tail contract targets the primary batch-tail path where the
  twelfth-review vectors live. No engine test has run (§10 stays
  post-approval).
- **rev-14 increment:** two merge-layer contracts were pinned and
  one scope corrected: (1) the complete-mean fold covers EVERY high
  word — the exact two-sum fold at (P)/(F) and the exact pair fold
  with retained fold loss at (C) — replacing the zero-high-word-only
  swap; the review's nonzero-cancellation pairs fold to their
  correctly rounded complete means (exactly 1.5; exactly 5·2^545)
  and preservation verified 164/165 preserved rows byte-identical
  plus the ONE in-suite lift of the [1.6e308,1.6e308,1.7e308]
  merged mean from its tolerated one-ulp-low dd lead to its own
  pinned literal — a direct counterexample to rev 13's "nonzero
  high word — below half-ulp" claim, so the lift IS the contract,
  not a regression; (2) mixed retained/scalar cross-centering
  re-forms exactly (twosum deviations, error-free products,
  ddsum/dddiv, one final fold), multiplying no retained tail as a
  scalar; (3) the first rev-14 build's fold at the raw W==0 batch
  state commit was REVERTED after the r11F4 preservation row broke
  (0.2 → 0.0) — that raw tail is the rev-9 compensated correction
  and the fold's scope is the merge layers (Decision 54). The
  exact chains add bounded constant work per merge step; state
  stays O(1) (§5). No engine test has run (§10 stays
  post-approval).
- **rev-15 increment:** the batch mean-tail residual is rebuilt
  from ALL compensated registers (each register an individual
  cascade item; no premature (cx + cxl) / (ca_ + ca_lo) folds),
  replacing the rescinded Decision-54 tolerance: the committed
  (word, tail) is a genuine complete-mean split in every peer
  order, the raw single-batch emission keeps the pinned narrowed
  word, and ALL 168 prior suite rows stayed BYTE-IDENTICAL
  (zero shifted rows, zero ulp shifts) with the review's four
  continuation vectors pinned green (fl(1/7), 0.1 — initial;
  fl(2/11), fl(1/6) — incoming; reorders equal; the raw 0.2
  emission). An editorial repair: the rev-14 splice of the §10
  oracle-cross-checks cell had dropped the cell's final
  "BVT results are generated-then-reviewed" clause (leaving
  "before the fixes literals cross-asserted…"); the clause is
  restored with this revision. The reconstruction adds the
  cascade cost it already carried (the registers were already
  computed); state stays O(1) (§5). No engine test has run (§10
  stays post-approval).
- **Superseded values recorded in-revision:** the rev-3 inaT tie value
  13.333333…, zero-division behavior on zero arrival mass, the
  absolute-moment underflow row, the per-observation (`A/k`) mass scheme,
  byte-identity permutation wording, and in rev 5 additionally the rev-4
  cov-merge omission (−36 / −0.6428571428571429), the rev-4
  absolute-domain merge (var 0 / corr NULL at `W=A=5e-324`; var
  1.925929944387236e-34 / mean 0.10000000000000002 for `.1` constants;
  Inf/Inf/NaN for `1.3e308`), the rev-4 "negligible products" rule, the
  float-key raw subtraction (mean 10 instead of 7.5), and rev 4's §10
  prepared signature and §9 struct-channel wording; and in rev 6
  additionally the rev-5 "weights <= 1 protect the merge" claim (Inf
  mean for [1.6e308,1.6e308,1.7e308]; Inf var / NaN corr for
  [0,2e154]), the rev-5 comparator's Inf-tolerance (close(inf,1),
  close(1,inf), close(inf,-inf) all passed), the rev-5 scaled-weight
  negligibility rule (the subnormal arrival at [0,8e-16] returning
  0/0/0/NULL; the retained 2^-1074 mass dropped), and the rev-5
  assumption that one guarded expression serves the whole merge domain;
  and in rev 7 additionally the rev-6 shifted-recurrence containment
  claim (mean −Inf for [1e308,1.1e308,−1e308] in 2 of 6 peer orders
  where 3.6666666666666666e307 is representable), the rev-6
  raw-difference term shape (var/cov Inf, corr NULL at [0,8e-16]
  h=1e308 x=y=[−1e308,1e308] where 1.9762625833649863e293 / 1 is
  correct), the rev-6 "direct shares are exactly representable down to
  the smallest subnormal" claim (mean/var/cov 0, corr NULL at
  [0,1074,1074] adjT where ≈2.4703e−320 / ≈2.47032823e−316 / 1 are
  required), the rev-6 saturating overflow pin (Inf var / NaN cov at
  [1,2,2] where 7.920792079207922e306 is representable, and a never-
  recovering 4e308 state where 3.90625e305 is correct), and the rev-6
  Call-boundary cancellation claim with its mislabeled partition
  wiring cites (`:968/:1053/:1072`; the cumulative transition is at
  `:992`) — each with its replacement and the fault value (§5, §8,
  §9).
- **Documented pandas divergences:** mass normalization through gaps;
  batch-atomic tie weights; the observation-batch ignoreNA clock (identical
  to pandas ignore_na except across NULL rows and between tied rows).
- **Documented API restriction:** event-time calls accept exactly one
  ASCENDING ORDER BY expression; row-decay mode unaffected.
- **Carried estimates:** a fully decayed state emits the last normalized
  estimate until new data — required by constant preservation; unseen
  partitions still emit NULL everywhere.
- **Cache exclusion cost:** marker-parameterized ewm statements recompile
  per EXECUTE (the percentile precedent's exact trade-off; literal-only
  statements unaffected).
- **Permutation bound:** non-constant batches agree within the
  compensated-sum bound rather than byte-identity; asserted unrounded at
  1e-12 and exact for constant batches (huge-magnitude adversarial
  inputs beyond the compensated bound retain their ULP-scale
  differences). Test comparisons in the ewm suites are **purely
  relative and range-safe** (plus exact equality; NaN matches nothing;
  ±Inf only by sign) — a wrong zero against a tiny nonzero expectation
  and an Inf against a finite expectation both fail by construction
  (§10 meta-rows).
- **Bit-identity domain of the layered merge:** the primary path is
  bit-identical to the plain mass form while every intermediate stays
  in the normal range (and every fallback triggers on an exact
  detection condition where the primary path is arithmetic-defective),
  so the preserved values change by zero ULP — verified by rerunning
  all vectors under the strict comparator — with one recorded
  exception (rev 7): the single preserved row whose *share-layer*
  result was non-finite (`[1.6e308,1.6e308,1.7e308]`, whose variance is
  genuinely out of range) now commits from the absolute-mass layer and
  its mean moves 1 ULP (`1.6432432432432433e308` →
  `1.643243243243243e308`, relative 6e−17), far inside its own pinned
  1e−12 tolerance and with the emitted +Inf variance / NULL corr
  unchanged (§10 row re-verified). Every finite share-layer commit is
  unchanged; every finite share-layer commit stays bit-for-bit because
  the share layers run only when their two gates pass.
- **Layered-complexity cost:** the merge and batch statistics carry
  detected fallback paths (§5): detection is exact
  (finite-inputs→non-finite-result; zero-rounded positive weight;
  dropped positive-mass side; out-of-range moment; non-finite raw
  deviation list; inexact subnormal-lattice scaling round-trip;
  subnormal share failing bit-exact mass reconstruction), the primary
  shapes are untouched, the share layers are gated temporaries, and
  the added code is a handful of small branches plus the extended
  (mantissa, exponent) representation for out-of-range moments —
  accepted as the only way to compute every representable result and
  retain recoverable magnitude without perturbing any pinned value
    Rev 8 added the single residual register per accumulator and the
  anchored-deviation branch; rev 9 replaced the single register with
  the bounded cascading register list (the registers hold exactly the
  bits each fold discards, so no-loss vectors are untouched), added
  the carried mean-tail recentering (a Dekker two-product and
  two-sum per merged mean and a compensated tail per batch) and
  routed only already-detected subnormal-lattice/subnormal-mass
  cases to the extended layer (constant cost; 122/123 preserved
  rows byte-identical); rev 10 makes the cascade alignment
  SYMMETRIC, raises the capacity to the domain-justified 128
  registers (≥53-order descent per level across the ≤6296-order
  relevable span), replaces the δ² end correction with complete-mean
  deviations, adds the weighted-product retention gate and moves
  the correlation normalization to the retained moments (129/131
  preserved rows byte-identical; the two recorded last-ulp shifts
  audited against 900-digit exact values on the engine's own masses
  and inside their pinned tolerances).
- **Honest float64 limits (documented, not hidden):** a result still
  out of range **at emission** yields ±Inf by sign (means computed
  convexly never overflow while representable; a non-negative variance
  carries a non-negative mantissa; the signed cross moment keeps its
  sign); weighted products below the smallest subnormal round away AT
  EMISSION ONLY (a below-lattice or subnormal-lattice nonzero moment
  stays retained in the state, so its std/corr remain computable —
  rev-9, finding 4); in the compensated extended form the eighth
  review FALSIFIED the ~1074-order claim (the single residual
  register swallowed cross-scale residuals only 800 orders apart in
  one peer order) — the rev-9 cascading registers preserve every
  fold's discarded bits and the documented bound is one rounding of
  the final restoration plus a second-order perturbation; rev-10
  (ninth-review finding 2) makes the cascade SYMMETRIC in both
  alignment directions and raises the depth to the domain-justified
  128 (the unit-row and ten-scale counterexamples are pinned in §10)
  — the symmetric alignment and the domain depth are the honest
  limits;
  ewmStd is the sqrt of the retained moment, so a var +Inf row can
  still emit its representable std;
  correlation is NULL on non-finite *emitted* variances; a
  cross-scatter with opposing unrepresentable cancelling terms follows
  the §5 IEEE clause like ordinary float SQL expressions.
- **Time-vector alignment** rides on the existing operator contract;
  implementation must read exactly like the RANGE path (§9 with UT).
- **Lifecycle dependency:** the exactly-once cursor and generation
  isolation are pinned to the aggregate lifecycle sites
  (`window.go:371` eval-start generation, `types.go:177/:195/:208/:217`,
  `:283-292`), the partition-boundary account reset lives directly
  inside `processEwmFuncRange` at the cumulative `j ==
  currentPartitionEnd` predicate (`window.go:988/:992/:997`-style —
  the rev-6 cites `:968/:1053/:1072` are corrected as mislabeled),
  and cancellation polling reuses the shared `checkCanceled` helper
  (`window.go:57`, interval 1024, sites `:989/:1031`-style); the five
  deterministic lifecycle tests (§10) are the planned minimum and
  their container-drive harness follows the existing precedents
  (`window_test.go:2658`, `interval_test.go:197`).
- **ignoreNA dual role** (weights in row mode, output visibility in event
  mode): documented twice and asserted separately.
- **Estimates** (line counts, goyacc surface) may shift at
  implementation; the design gate stands on complexity triggers.
- **Web search remains unavailable:** DolphinDB details beyond the issue
  text stay unverified; the design derives its API from the issue's
  stated policy axes.
- **rev-16 increment:** the finite batch-moment formations now
  guarantee residual-exact accumulation at the cost of per-row
  twosum/xtwoprod work in the finite paths (fast-path identity
  batches keep their established shape, and the merged weighting
  arithmetic and every merge layer are untouched; the
  overflow/lattice/anchored ROUTES keep their range-safety routing
  and retention policy, while their ARITHMETIC — the sixteenth
  review's finding — is superseded in rev 17 by the common
  contract of Invariant 33 / Decision 57); 168 of the 169 preserved
  suite rows stay byte-identical with the one documented
  reassessment (the [M/2,0,0] control's one-ulp-low pin lifted
  to the exact complete moment) recorded in the revision record
  and Invariant 30/32 text.
- **rev-17 increment:** one error-free batch-moment contract now
  spans the normal, lattice and anchored routes (Invariant 33 /
  Decision 57); the range-safety routing (devfin/frexp fences,
  midpoint anchors, extended-commit retention) is untouched and
  the correction above limits the rev-16 “established shapes”
  claim to the ROUTING. The quotient change (ddsum/dddiv/xadd on
  the routes' six formations) costs a bounded constant per batch
  term; state stays O(1) (§5); all 170 preserved suite rows stayed
  byte-identical (NO reassessed literal this round), and the
  B-scale continuation pins document the merge layer's own
  single-pair assembly bound (one ulp of the exact ideal) — if a
  later review demands exact merged ideals at that scale, the
  moment-assembly pipeline (the dd_mul-folded single-pair term
  feeds, the one-rounded W·vxx / A·sxx assembly products and the
  single-pair numerator restore feeding the quotient) needs its
  own finding. No engine test has run (§10 stays post-approval).
- **rev-18 increment:** the routed cross quotients now run the
  cancellation-preserving cascade (Invariant 34 / Decision 58):
  the full register cascade feeds the first quotient word and
  the EXACT −q1·d residual re-cascades WITH the pile (a bounded
  constant cost per routed cross term); the dd pipeline survives
  on every SCATTER quotient, the normal route's own quotient,
  the routing/retention layers and every merge layer are untouched,
  and all 171 preserved suite rows stayed byte-identical (NO
  reassessed literal this round); the r15F/r16F/r17F pinned
  families stay green through the changed sites and the archived
  rev-17 faults remain reproducible on the frozen rev-17 archive
  (the final probe re-observes them); preservation was rechecked
  against independent mathematical expectations (the Fraction
  mixture model re-derives every continuation ideal, and the
  archived rev-17 module bit-matches the repaired core on
  every preserved vector). Any future change to the cross-pile
  cascade or the merge's moment assembly needs its own finding;
  the runtime engine ladder stays post-approval.
- **rev-19 increment:** the complete-residual channel is now ONE
  shared reconstruction on both routes (Invariants 35/36 /
  Decision 60): the anchored and ordinary tail lanes read the SAME
  genuine_tail (raw originals through the xsum4 cascade, exact
  k·mean words, one final division — a bounded constant cost per
  batch; state stays O(1)). The ordinary float64 fast lane, every
  scatter, cross formation, routing fence, retention policy and
  merge layer stayed byte-identical; the pre-row full run on the
  rebuilt core is byte-identical to the frozen rev-18 log and all
  172 preserved suite rows stayed byte-identical (NO reassessed
  literal this round). The rev-18 anchored fault stays reproducible
  on the frozen rev-18 archive while the archived rev-19 heals the
  full 16-combination matrix with 32 green reordered controls. The
  incoming row-decay merged mean is documented ONE merge ulp above
  the strict closed form fl(H/18) (the corrupt account emitted
  exactly the strict value) — if a later review demands exact
  strict ideals at that merge scale, the merge's own mean assembly
  needs its own finding; the same holds for the merged complete
  mean at B-scale (the rev-17/18 one-ulp merge bound). The §11
  decision log now carries Decisions 58–60 (the P3 repair). The
  runtime engine ladder stays post-approval.
- **rev-20 increment:** three contracts are pinned: (1) the ONE shared retention rule `lane_retains` (Invariant 37 / Decision 61) keeps any narrowed-away content in the pair channel — the nonzero subnormal tail included — at every commit lane on both streams and routes, a bounded constant test per lane; the narrowed scalar lanes stay EXACTLY as before for every exact shape, and all 173 preserved rows stayed byte-identical (the pre-row full run byte-equal to the frozen rev-19 log; NO reassessed literal); (2) the ONE shared packing `pack_remainder` (Invariant 38 / Decision 62) retains the primary narrowing remainder at every normalization/merge boundary — a bounded constant cost per boundary; (3) the public finite-sample gate (Invariant 39 / Decision 63) turns the leaked internal assertion into a deterministic execution rejection before any state publication. The rev-19 faults stay reproducible on the frozen rev-19 archive while the archived rev-20 heals all three (the final probe re-observes and heals them; 15/15). Scope note: the retention rule and the packer touch ONLY the commit-lane fallback structure and the packing-fold sites; no scatter, cross formation, routing fence or merge assembly changed, and any future change to those layers needs its own finding. The runtime engine ladder stays post-approval.
- **rev-21 increment:** one contract is completed: the ONE shared retention rule `lane_retains` (Invariant 40 / Decision 64) now owns EVERY tail-storage boundary — both merged-state streams (x and y) after `pack_remainder` packing join the batch commit lanes under the SAME one test, a bounded constant cost per store; the packed merged tail stays an exact (hi, pair) split of the computed complete mean, so continuation merges consume true pair semantics; the narrowed scalar lanes stay EXACTLY as before for every exact shape, and all 174 preserved rows stayed byte-identical (the pre-row full run byte-equal to the frozen rev-20 log; NO reassessed literal; the appended r21F row makes 175/175). The rev-20 faults stay reproducible on the frozen rev-20 archive (26 fault observations against 26 green controls) while the archived rev-21 heals all eight combinations plus both signed continuations (the final probe re-observes and heals them; 15/15). Scope note: the repair touches ONLY the two merged-tail store sites; no batch commit lane, scatter, cross formation, routing fence or merge assembly changed, and any future change to those layers needs its own finding. The runtime engine ladder stays post-approval.

## 13. Open questions (non-blocking, owned)

1. **Alias spellings** (`ewm_mean`, etc.): not registered in v1; revisit on
  user demand (owner: issue #29945).
2. **Unbiased variance option:** only with demonstrated consumer need.
3. **decimal256:** accepted via the numeric-arg cast to float64 (approver
   check pinned: if the shared cast path rejects dec256 at implementation,
   escalate as material deviation, do not narrow silently).
4. **halflife temporal resolution:** int64 microseconds (finding-7 rule);
   coarser/finer units remain user-side key transforms.

Revision history:
- rev 1 — initial design-first artifact (2026-10-23, base f8c8720016,
  SHA256 `c2590c4eff168a03c12d42b10af580c0c55dcd0081b5996cbb5ae3b4ee414985`).
- rev 2 — mass-normalized state, centered/scale-safe moments, event-time
  mode, per-execution prepared policy resolution (2026-10-23, SHA256
  `3fe50a217557fb63e1445bab0083232dbc82303a2ce6d6b16ea0e2a2b1479f9a`).
  Rejected (five findings).
- rev 3 — order-free batch initialization, parallel merge with the
  zero-mass degenerate branch, within-instant scatter, single ascending
  event-time axis, native-domain gaps with −expm1 masses, carried snapshot
  (2026-10-24, SHA256
  `cd7bb8501a6204e56522d1f515ecb0d7cd49f4c663d671480625d791831ec21d`).
  Rejected (seven findings).
- rev 4 — (2026-10-24): resolves the seven rev-3 findings —
  complete cache ownership for policy resolution (percentile exclusion
  adopted with `PreparedPlanHasEwmParams`), consistent per-account
  row-decay clocks with the no-NULL agreement invariant, deterministic
  compensated unweighted batch accumulation with an identity fast path,
  normalized decay-invariant state (absolute-moment underflow fixed;
  snapshot folded away), unsigned-magnitude integer gap domains with
  decimal-overflow semantics, zero-mass arrivals processed before any
  division without per-observation masses, and the normative §4 signature
  table with the required-decay bind error. Rejected (six findings:
  incoming cov term dropped, second prepared cache ungated,
  struct-vs-bytes config channel, absolute-domain merge weights,
  merge-level constants broken, float-key subtraction overflow).
- rev 5 — (2026-10-24): resolves the six rev-4 findings —
  every non-degenerate merge retains the incoming batch's cross-scatter
  (cov/corr asserted at non-initial multi-observation batches in both
  decay modes and both adjust modes, with closed-form cross-checks);
  policy markers are excluded from **both** prepared caches
  (`shouldCachePrepareCompile` at both call sites plus the
  runtime-specialization admission at `computation_wrapper.go:1916`,
  with the corrected `ewmMean(v, ABS(?), ?, ?, ?)` prepared sequence);
  the EWM configuration travels as bytes through `MakeAggFunctionExpression`'s
  `extraConfig` and is decoded by the EWM dispatch itself
  (percentile-shaped, never through `SetExtraInformation`); merges run
  in a power-of-two normalized mass domain (comparable subnormal masses
  keep exact shares — var .125 / cov −.125 / corr −1; bit-identical to
  the plain mass form in the normal range, so every pinned oracle value
  survives exactly); a merge-level equality fast path keeps constant
  signals exact across batches at any magnitude (`.1`, `1.3e308`);
  FLOAT/DOUBLE event keys derive decay in the quotient domain
  (`[−1e308,1e308]` h=1e308 → 0, 7.5) with non-finite keys/policies
  rejected; the expected-value comparators in the ewm test plan are
  purely relative. Rejected (five findings: the scaled merge still
  overflows for representable results; mass normalization discards
  significant contributions; batch accumulation not range-safe; the
  oracle comparator accepts infinity; EWM execution lifecycle
  undefined).
- rev 6 — (2026-10-24, SHA256
  `e34b32e650895d91722d7eb0f711dd1ffa01856ca18a1675d566f854021c11dc`):
  resolves the five rev-5 findings
  — the merge and batch statistics become **layered** (the rev-5
  bit-identical primary shapes stay, with exactly-detected fallbacks:
  convex-share means and weight-inside-product moment terms for
  intermediate overflow; direct shares `W/(W+A)`/`A/(W+A)` for
  extreme mass ratios; a shifted running mean and per-term regrouped
  scatter sums for overflowing batch sums, with non-negative sums
  sign-pinned to honest +Inf); the oracle/test comparator is
  range-safe (NaN matches nothing; infinity only by sign) and every
  preserved row was reassessed under it; the corr rule NULLs on
  non-finite variances; and §9 gained the complete EWM execution
  lifecycle (monotonic exactly-once peer-group cursor with in-flight
  group cache for split emit slices; `freeEwmState` at the aggregate
  lifecycle sites; cancellation polling per Call; errors never advance
  the cursor) with four planned deterministic lifecycle tests.
  Archived rev-6 oracle artifacts: `ewm_rev6_oracle_simulator.py` —
  109/109 PASS, exit 0, under the strict comparator. **Rejected (six
  findings: the batch-mean recurrence still overflows at the
  subtraction for opposite-sign extremes; the safe-merge moments
  evaluate raw differences before weighting; materialized shares can
  round a positive side below the smallest subnormal to zero;
  out-of-range intermediate moments saturate and lose recoverable
  magnitude; cancellation is not polled inside whole-group processing;
  the partition-reset wiring cites mislabeled window.go lines).**
- rev 7 — (2026-10-24, base f8c8720016, SHA256
  `37b81fbcf3bf45dd8f80643ac4d1f5173a79beccd1597c18e13901b790dc55e5`):
  resolves the six
  rev-6 findings — the batch-mean fallback becomes the
  **midpoint-anchored accumulation** (permutation-invariant anchor
  `min/2 + max/2`, per-observation deviation shares, no overflowable
  difference in any peer order; all six permutations of
  `[1e308,1.1e308,−1e308]` pinned finite at `3.6666666666666666e307`); the
  merge gains a **gated absolute-mass layer** in which no raw
  difference and no materialized share is ever evaluated (means
  `(W·μ + A·m̄)/(W+A)`; weighted-difference identities `q·D` / `q²/mass`
  for every deviation and cross term), reached when the share layers
  fail their two gates (a positive-mass side rounded to zero; a
  non-finite result from finite inputs); batch and state moments out
  of float64 range are **retained** as extended (mantissa, exponent)
  pairs through state and merge with the honest sign-pinned ±Inf
  applied **only at emission** (the `4e308` cases recover
  `7.920792079207922e306` at mass 0.02 and `3.90625e305` after ten
  half-lives); the lifecycle adopts **bounded in-loop cancellation
  polling** (the cumulative paths' own `checkCanceled`, interval 1024,
  `window.go:57/:989/:1031`) inside every EWM scan/statistical/recompute
  loop while keeping per-group atomic publication, and the
  partition-boundary check is specified **directly inside
  `processEwmFuncRange`** with the generation-scoped absolute cursor
  distinguished from partition-scoped account statistics (corrected
  cites: the cumulative transition is at `window.go:988/:992/:997`;
  `:968/:1053/:1072` are sequential-order guards and final cleanup).
  Archived rev-7 oracle artifacts: `ewm_rev7_oracle_simulator.py` —
  116/116 PASS, exit 0 (all 109 preserved rows keep their pinned
  assertions; 108 bit-identical, one recorded 1-ULP layered shift),
  plus `repro_review6_rev6.log` (the six findings reproduced on the
  archived rev-6 simulator with independent 900-digit expectations).
  **Rejected (seventh review, run 1792793541181: four P2 findings —
  batch raw mean-deviations overflow to −Inf operands and the
  extended normalization loops forever (6/6 permutations); ldexp
  ties an odd subnormal mass silently to a 33% reweight with no
  detection; the uncompensated extended accumulation loses
  cancelling cross terms and makes the covariance peer-order
  dependent; ewmStd computes the sqrt of the narrowed Infinite
  variance where the true standard deviation is representable).**
- rev 8 — this revision (2026-10-24, base f8c8720016): resolves the
  four seventh-review findings — the batch statistics **gate the raw
  deviation lists and route non-finite lists to anchored deviations**
  (exact identity Sum(x−mean)^2 = Sum(x−a)^2 − k·delta^2 with
  per-side anchors; finite normalized mantissas enforced in the
  extended layer — a non-finite operand is a defined internal error,
  never a loop); the merge requires the **bit-exact ldexp
  round-trip** before the primary form, builds shares from **raw
  masses** whenever the scaling is degenerate or inexact, and
  commits a **subnormal share only when it reconstructs its mass
  bit-exactly** (else the absolute-mass layer, mass exponents
  preserved — pinned on both merge sides, incoming and retained);
  the extended accumulations (batch fallbacks and merge (C)
  assemblies) are **compensated Neumaier two-sums with a residual
  register** (covariance bit-identical across all 720 peer
  permutations; the ~1020-order limit claim superseded by the
  ~1074-order subnormal lattice); and **ewmStd takes its square root
  on the retained moment before the float64 narrowing** (var +Inf /
  std EXACTLY 2e154 on the same row). Archived rev-8 oracle
  artifacts: `ewm_rev8_oracle_simulator.py` /
  `ewm_rev8_oracle_run.log` — 123/123 PASS, exit 0 (the 116 rev-7
  rows byte-identical, verified by log diff; the seven new rows
  carry the seventh review's exact expectations), plus
  `repro_review7_rev7.log` (the four findings reproduced on the
  archived rev-7 simulator with 900-digit independent expectations,
  incl. the watchdog-evidenced 6/6-permutation + sequence HANG).
  The worktree diff is this document only; no implementation has
  begun (implementation remains blocked until this revision passes
  review).
- **Rejected (eighth review, run 1792797859202, rev-8 archive SHA256
  `abc36eac9be303a6447b6331db4150369b4b676af2105d1db2f333f0a08a5c57`:
  four P2 findings — the subnormal-share reconstruction certificate
  (`s·denominator == mass`) also rounds and committed a
  25%-understated share (retained `W = 2^-1074` under arrival
  `A = 0.75`: mean `4.940656458412465e-124` where
  `6.587541944549954e-124` is required on the same rounded masses);
  the single compensation register loses cross-scale residuals only
  800 binary orders apart (the three-scale tied batch returns a
  covariance of exactly 0.0 in one peer order and its true L²/5 in
  another); the batch and merge moments center on the already-
  rounded float64 mean (`[2^53, 2^53+2]` returns var/cov/std
  2/2/√2 at the magnitude boundary where the exact population values
  are 1/1/1 and the `[0,2]` translation already returned 1 —
  Invariant 5 violated in both the tied-batch and the two-singleton
  merge paths); and the non-finite-only range gates with the
  narrowed std let subnormal-lattice roundings destroy representable
  moments (tied `[−1e-200, 1e-200]` emits std 0 where 1e-200 is
  representable; singleton merges `[−2^-537, +2^-537]` emit
  0/0/0/NULL where `2^-1074`, `2^-1074` and `2^-537` are exactly
  representable). Reproduced 4/4 on the archived rev-8 simulator
  (`repro_review8_rev8.log`) with independent 900-digit
  expectations; the review also reproduced 123/123 PASS and verified
  preservation of all 116 prior rows.**
- rev 9 — this revision (2026-10-24, base `f8c8720016`): resolves the
  four eighth-review findings — the float64 layers **never commit a
  subnormal share** (every subnormal share rebinds to (C); the
  certificate is deleted); all extended accumulations **cascade**
  (rev-8 level-0 loop and first register bit-identical; each fold's
  exact discarded bits pass to the next bounded register — both
  three-scale peer orders return L²/5, and the falsified
  ~1074-order claim is replaced by the cascade-depth contract); the
  means are carried as **hi+lo pairs** and the batch/merge moments
  are **recentered by the exact δ² (cross δx·δy) corrections**
  computed from compensated mass·mean numerators (the 2^53 boundary
  vectors return var/cov/std exactly 1 on both the tied-batch and
  the two-singleton merge paths, matching their [0,2] translations);
  and the **subnormal-lattice rebinds** route at/below-2^-511
  deviations, subnormal nonzero moments, and same-scale merge
  deviations to the extended channel, whose store keeps below-
  lattice and lattice-quantized results extended (tied
  `[−1e-200, 1e-200]` emits std 1e-200 with var honestly 0.0;
  `[−2^-537, +2^-537]` emits var/cov exactly 2^-1074, std exactly
  2^-537, corr exactly 1). Archived rev-9 oracle artifacts:
  `ewm_rev9_oracle_simulator.py` / `ewm_rev9_oracle_run.log` —
  131/131 PASS, exit 0 (122/123 prior rows byte-identical by log
  diff; the single shift is the recentering on a sub-ulp batch-mean
  tail, inside the row's pinned tolerance — recorded in
  `rev9_preservation_diff.log`); the four fault values were first
  reproduced on the archived rev-8 simulator
  (`repro_review8_rev8.log`). The worktree diff is still this
  document only; implementation remains blocked until this revision
  passes review.
