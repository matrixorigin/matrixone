# Sequence ownership and state consistency across DML, DDL, DATA BRANCH MERGE, and transactions

- Design revision: **v21 (2026-10-30)** — independent design review of
  v20's D1: two corrections (Z1–Z2) applied to the contention-set budget
  and baseline transitions of the watch-set lifecycle, on the unchanged
  v18 premise — a fixed wall-clock D loses
  deterministically to any sustained contention herd whose serialized
  drain exceeds D, while the base binary drains the same workload with
  zero client-visible errors through its unbounded in-statement retry
  loop. Z1 CLOSES the retry episode when its contention resolves: D is
  retired exactly when the watch set becomes empty — by the statement's
  own committed allocation, by an all-resolved probe round, or by an
  unresolvable identity's retirement — and the statement returns to the
  base per-invocation `min(statement, 10 s)` budget, so an expired D can
  never strand an uncontended statement with a nonpositive attempt
  budget and a later conflict re-admits with a FRESH window. Z2 makes
  every watch entry carry its baseline from CREATION — first admission
  by the admission probe, later conversions by the same bounded probe
  taken at the conversion — and gives each identity a terminal-
  eligibility clock measured from its own baseline once for each such
  identity: an identity terminals only after one full renewal interval
  observed unchanged AND held, a fresh baseline can never renew a
  stalled co-identity's interval, and a never-baselined entry fails
  safe after one full interval from its creation. The retained watch
  set (A1), cancellation routing (A2), availability discriminant (A3),
  per-decision accounting (A4), the v19 marker (B1) and pending-set
  ownership (B3), the lazy creation point (G2), two-phase arbitration
  (G1/F1), frozen tier 1 (I6), the E1 ALTER rule, and every v12
  rejection are unchanged; end-to-end boundedness is unchanged in
  kind — the session statement deadline, with the no-timeout ceiling
  at one full interval ending with some terminal-eligible contended row
  committed-unchanged AND unavailable to the statement.
 matrixorigin/matrixone#30359 (consolidation tracker for #30360, #30679, #30896)
- Implementation PR/series: TBD (single PR, one coherent contract)
- Base commit analyzed: `4769417cdc` (main; base reproductions executed on a local single-CN
  cluster built from this commit, isolated ports)
- Status: design v10 **approved** by independent design review (2026-10-27, artifact
  `pre-pr-review-1793100961226.json`, 1 P3 finding accepted into v11);
  v11 applied that P3 (editorial only); v12 calibrates one named failure bound
  from live 2-CN 16-client measurement (I6) and otherwise changes no contract;
  v13 adds the bounded statement-retry tier (I7) from live §6 re-verification
  and is submitted for independent design review before that tier is
  implemented (the F5-reviewed terminal end-to-end bound is preserved by
  construction — no unbounded retry at any tier); v14 applies the v13 design
  review's three corrections (H1–H3); v15 applies the v14 design review's
  two corrections (G1–G2); v16 applies the v15 design review's correction
  (F1); v17 applies the v16 design review's correction (E1); v18 applies
  the pre-PR implementation review's four conformance corrections (C1–C4)
  and one retry-window design correction (D1) and is submitted for
  independent design review before the D1 semantics are implemented (the
  v17-conformance implementation of C1–C4 is complete and its proof is
  preserved); that review (artifact `pre-pr-review-1793337892295.json`,
  REQUEST_CHANGES) returned four D1 corrections, applied in v19 (B1–B4):
  the progress marker is made authoritative across complete CYCLE wraps,
  SETVAL/reconfiguration and generation changes; every expiry decision
  probes before terminating; pending conversions form a per-identity set
  with conjunctive renewal; and the live no-progress oracle becomes a
  sustained uncommitted-lock blocker (producer-stop demoted to a
  successful-drain control). v19 is submitted for independent design
  review BEFORE any D1 semantics are implemented; the C1–C4
  implementation and its proof remain preserved;
  that review (artifact `pre-pr-review-1793339343909.json`,
  REQUEST_CHANGES) returned three further D1 corrections plus one
  probe-cost correction, applied in v20 (A1–A4): the expiry probe set
  is the retained statement-lifetime contention set rather than the
  consumed pending set; D-derived attempt cancellation is routed into
  the shared expiry decision instead of failing the transient
  classifier; expiry terminates only on committed-unchanged AND
  unavailable, so a finished herd never false-terminals; and the
  probe-cost bound is restated per expiry decision. v20 was submitted
  for independent design review; that review (artifact
  `pre-pr-review-1793340688831.json`, REQUEST_CHANGES) returned two
  further corrections, applied in v21 (Z1–Z2): D retires exactly when
  its watch set empties — an expired window can never strand an
  uncontended statement without a usable attempt budget, and a later
  conflict re-admits with a fresh window; and every watch entry carries
  its baseline from creation with a per-identity terminal-eligibility
  clock — a late-joining identity can neither terminal before observing
  one full no-progress interval nor renew a stalled co-identity's
  interval. v21 is submitted for independent design review BEFORE any
  D1 semantics are implemented; the C1–C4 implementation and its proof
  remain preserved.

## 0. Revision history

- **v1** (`867670b1…dda04`): initial proposal; v1 review R1–R6 accepted into v2.
- **v2** (`5ba739f0…3f91`): watermarks, direction-aware restart, row-lock protocol, scoped I4, failure bounds; v2 review N1–N5 accepted into v3.
- **v3** (`d5f577b9…da54`): two-namespace lock order, transaction registry + in-caller fallback, equality predicate, rollout phasing, START-WITH-only rescope; v3 review M1–M3 accepted into v4.
- **v4** (`118f9c74…44933`): always-autonomous allocation, autonomous atomic reconcile unit, final-state ALTER validation; v4 review L1–L2 accepted into v5.
- **v5** (`44bef1d0…941e1`): generation-visibility routing, visibility-partitioned reconciliation, global canonical lock order with whole-unit restart; v5 review K1–K2 accepted into v6.
- **v6** (`ffabbe62…f67ad9`): per-(txn, identity) floor + pre-commit advance stamp replacing the fate-sharing premise; validate-before-proceed acquisition; v6 review J1–J3 accepted into v7.
- **v7** (`bb9ec0d0…e9d074`): pass-scoped obligations, boundary invalidation, relation-id-keyed commit-time lifecycle; v7 review I1 accepted into v8.
- **v8** (`a1d419a7…1960dc`): preserve-or-restore commit stamp; **independent design review PASS** (2026-10-27, `pre-pr-review-1793076397213.json`) with one P3 finding, accepted into v9 (below).
- **v9** (`73ca8a0d…3a673a1a`): v8-review P3 support-claim correction (approval condition): on this base a transaction-local generation arises only from an explicit-transaction CLONE or snapshot restore; the rejected foreground-DDL shapes become explicit rejection-control assertions.
- **v10** (`bf62c26c…e9cb979`): I5 oracle placement (20101 boundary control + committed-route equivalents; UT authoritative for the transaction-local preserve arm); **independent design review PASS** (2026-10-27, `pre-pr-review-1793100961226.json`) with one P3 finding, accepted into v11 (below).
- **v11** (`76305cc8…4c4f`): v10-review P3 condition (approval condition): revision metadata aligned with the shipped v10 dispositions, and the §4 B supported-path example now names the verified 20101 SQL boundary; editorial only — no contract change.
- **v12** (`4e08e515…c5e40a4`): implementation-turn retry-bound calibration (I6): on the isolated 2-CN 16-client uniqueness control, the approved retry machine (4 attempts) left 2–3 clients with retry-exhaustion errors after the wrapper-traversing classification repair (raw `ErrTxnNeedRetry` arriving as `errors.Join(attemptErr, rollbackErr)` had leaked past `moerr.IsMoErrCode`'s direct assertion; repairing that traversal is implementation conformance, not a contract change). The base binary returns 16/16 twice on the same topology, so the accepted bound must drain a 16-way cross-CN stampede: 8 attempts, the same 25/50/100 ms backoff schedule (now explicitly capped per step) + jitter, deadline unchanged `min(statement, 10 s)`, non-retryable set unchanged, single publish point unchanged, exhaustion error unchanged.
- **v13** (`5cf7305d…12606a`): implementation-turn live re-verification (I7): with the pre-PR review's F5 correction applied exactly as reviewed (tier-1 exhaustion terminal, the unspecified compiler-retry conversion removed), the §6 measured experiment (16 clients × 250 `NEXTVAL`, 4,000 allocations, exactly unique) fails deterministically on the isolated 2-CN topology — 14/16 clients receive the terminal error inside the frozen ~0.6 s tier-1 window (`ERROR 20101 ... within 8 attempts: txn need retry in rc mode` at statements 1–22 of 250; phase wall 3 s, 286/4,000 allocations) — while the unchanged base binary drains the identical workload in 95 s with 4,000/4,000 unique allocations and zero client errors, and the earlier conversion build passed it in 86 s. Root cause: the I6 calibration drained only the single-shot 16-client control, where every winner permanently leaves the contention set; under §6's sustained stampede the contention set never drains within one statement's frozen tier-1 window, and v12's rejection list rules out the in-attempt remedies (RC-semantics change, in-attempt wait-retry, write-first blind increment). v13 therefore restores statement-level retry as a specified, bounded tier 2 (§4 D): conversion only for the tier-1 transient-conflict exhaustion class, gated by a statement-scoped sticky deadline `min(statement, 10 s)` created once per statement and preserved across compiler retry transitions; on expiry the named exhaustion error escapes unconverted. The F5-reviewed requirements — terminal bound enforced end-to-end, no unbounded retry, the overall bound proved through `Compile.Run` — are unchanged and are now met by construction.
- **v14** (`001265ff…a80e540a`): independent design review of v13 (H1–H3 below; tier-2 retry retained, its unspecified parts made authoritative). H1: tier-2 admission becomes a named predicate — eligible shapes are single client DML/SELECT statements; the transaction predicate requires the dispatch envelope {Autocommit=true, ByBegin=false} stamped at statement creation (`frontend/txn.go:812` → `txn/client/operator.go:225-229`), RC isolation, and the compiler's generic `canRetry` acceptance (`compile.go:793-797`), default-deny for envelopes lacking these facts; the result-publication restriction requires zero delivered result batches at conversion time (`SessionInfo.ResultRows==0`, `resp_client.go:239-246`), enforced at the conversion site in `seqfunc_autonomous.go`; the compiler's generic retry policy is unchanged for other errors. H2: the sticky deadline D (`min(statement, 10 s)`, created at the statement's first sequence-allocation entry) becomes the single absolute budget of the tier-2 envelope — every tier-1 execution derives its attempt context from D instead of opening a fresh `now+10 s` window per invocation (`seqfunc_autonomous.go:184-186`), conversion admits only while D is unexpired, and terminal-error precedence is ordered (caller cancellation propagates unchanged; D-expiry surfaces the named exhaustion error exactly once and forbids further conversion). Rebuild/replan work of an admitted retry remains bounded by the caller's statement deadline (`compile2.go:801`); only allocation-path work counts against D. H3: the public projection is corrected to the approved semantics — `ALTER … START WITH` restarts to exactly `n` only when `n` is ahead of the consumed frontier or the sequence is fresh (§4 C clamping: behind-frontier starts keep position and allocation continues past the frontier; pinned both directions by `alter_sequence_start_with.sql:31-55`), and a caller merge failure/rollback after the reconcile unit has committed leaves the consumed advance (documented gap, §4 B, §8 v4) while a reconcile-unit failure remains fully atomic. New validation oracles: explicit-`BEGIN` rejection control, `SET autocommit=0` rejection control, already-streamed-prefix rejection control (no tier-2 retry, terminal error once, no duplicated prefix), admitted auto-commit controls (single-row nextval and bulk default insert retry to success), the deterministic just-before-expiry `Compile.Run` case (admitted retry at D-ε with a blocked next attempt terminates with the named error, resources released, no further publication or retry), the existing behind-frontier START pins, and the post-reconcile-rollback gap pin (new).
- **v15** (`72dae772…16c4d6`): independent design review of v14 (G1–G2 below; tier-2 retry retained, its admission safety and budget scope corrected). G1: v14's publication term (`SessionInfo.ResultRows==0` observed at the conversion site) is not safe against concurrent publication — sequence-bearing AP statements stay parallel within the initiating CN (`compile.go:1476-1486`), producer scopes run concurrently (`scope.go:606-628` publishing through `resp_client.go:241-249`), and `SessionInfo.ResultRows` itself is unsynchronized (`process/types.go:950-962`). Admission becomes two-phase (§4 D): a cheap pre-filter at the conversion site plus authoritative arbitration at the retry quiescence boundary — after `prepareRetryTransition`'s producer join (`compile2.go:752` → `cancelAndWaitRunningSQL` `:916-918`), before the retry generation is rebuilt (`buildRetryCompile` `:801-805`) — against a synchronized per-statement publication flag; when publication wins the race the retry signal is dropped and the ORIGINAL named exhaustion error is returned to the client exactly once (no replay, no duplicated prefix). G2: v14 created D at the statement's first sequence-allocation entry and derived EVERY tier-1 execution from D, turning a contention-retry budget into a statement-wide timeout for ordinary uncontended statements (base behavior is a fresh per-invocation `min(statement, 10 s)` window, `seqfunc_autonomous.go:185-189`). D becomes the retry-envelope window only (§4 D): created lazily at the statement's first tier-2 admission, sticky across retries of that statement, never reset; pre-admission executions keep the base per-invocation budgets, so a statement without allocation conflicts observes no new time limit; the public projection is aligned accordingly. New validation oracles (in addition to the v14 oracle list, which stands): the deterministic publish-after-pre-filter barrier case (a batch is published after the conversion site observed zero rows but before the retry transition; retry is declined, the client receives exactly the published prefix plus the terminal error, no replay or duplication, `-race` clean), the fake-clock no-conflict multi-allocation control crossing the retry-window duration (every later allocation succeeds with a fresh per-call budget), and the no-reset window-identity case (two arbitration events in one statement reuse the same D instance).
- **v16** (`efde00fe…6be76`): independent design review of v15 (F1 below; tier-2 two-phase admission retained, its remaining unsynchronized read removed). F1: v15 explicitly permitted the conversion-site pre-filter to additionally consult the unsynchronized `SessionInfo.ResultRows` counter (`process/types.go:950-962`, written through `AddResultRows` at `resp_client.go:241-242`). After the pre-filter observes the atomic publication flag unset, publication can begin while that hint reads the counter — synchronizing a separate flag does not synchronize these counter accesses, and quiescent arbitration prevents replay but cannot eliminate the earlier data race. v16 removes the hint entirely: BOTH admission phases (pre-filter and arbitration) read ONLY the synchronized per-statement publication flag, and the `ResultRows` accounting is never consulted by either phase (`process/types.go:950-962` is left untouched as ordinary statistics). The removal is behavior-neutral — the flag is set by the single publication path before any client bytes, so the hint could not catch any state the flag misses — and it closes the race window the review identified. The deterministic publish-after-observation barrier oracle stands unchanged (publish one batch after the pre-filter's flag-unset observation and before the retry transition; retry declined; the client receives exactly the published prefix plus the terminal error; no replay or duplication) and its implementation is REQUIRED to execute race-clean under `-race` (zero warnings), which now also proves the pre-filter reads no unsynchronized state.
- **v17 (this document)**: independent design review of v16 (E1 below; F1's flag-only admission passes review; one ALTER contract contradiction remains). E1: §4 C's uniform restart rule (restart iff strictly ahead of the pending value, explicitly covering both `is_called` states) contradicted §8 and the public projection, which promised an exact restart whenever the sequence has never issued ("restarts to exactly `n` … or the sequence is fresh"). For `CREATE SEQUENCE s … START WITH 10` (never issued) followed by `ALTER SEQUENCE s START WITH 5` they require different fresh-session results — 10 (keep pending) versus 5 (restart) — while the pinned UT matrix (`alter_sequence_state_test.go:217,:224`) already encodes keep-position and the uncalled BVT (`alter_sequence_start_with.sql:58-62`) tests only an ahead start and cannot expose the contradiction. Resolved on contract grounds, not derived from current code (review action): §4 C is the single authoritative rule — restart iff strictly ahead of the pending value, uniform for both `is_called` states; a behind-start on a never-issued sequence KEEPS the unissued pending value (ascending example ⇒ next value 10; descending mirror: pending 10, `START WITH 15` behind ⇒ next 10, `START WITH 5` ahead ⇒ next 5), with the recorded `start_value` boundary on that arm being the declared start `n` (empty consumed range — wholesale-redefinition replay stays legal). The ONLY exception restarts a pending value outside the effective final bounds at the declared start, for CYCLE and NO CYCLE alike, with the declared start itself validated atomically against the final bounds. §8 and the public projection are aligned to this text; §9 gains behind-start uncalled BVT pins in both directions with fresh-session assertions.
- **v18 (this document)**: independent pre-PR implementation review of v17 (C1–C4 below; G1/F1/E1 and the four-issue contract pass review; one design contradiction surfaced only after C1 made the sticky window enforceable). C1: `compile.go:157`'s per-`NewCompile` fresh `SeqRetry` interacted with the retry rebuild (`compile2.go` `buildRetryCompile`), which discarded the instance arbitration had just admitted on — the sticky window D was reseeded away on every generation, each generation received a fresh 10 s budget (de-facto unbounded retry), and the previous turn's §6 green (59 s, 4,000/4,000, rc=0) ran entirely under that defect, so no valid fixed-D evidence ever existed. C2: the obligation-floor rewrite in the transaction-local allocation path (`seqfunc_autonomous.go:775`) was unconditional, so a statement that SETVAL-advanced the position with no prior obligation and then failed could leave the floor regressed ([60,60] → [60,10]) and later reissue 60 — a K1 violation in the no-prior-obligation shape the existing boundary tests never construct. C3: merge reconciliation derived its numeric comparison domain from the destination COLUMN type (`data_branch_sequence_reconcile.go:271`), so a `BIGINT UNSIGNED` sequence behind a VARCHAR default compared uint64 watermarks as int64 and reconciled to 9223372036854775807, letting the next default insert regenerate the already-merged 9223372036854775808 (duplicate key) — the existing VARCHAR pins use small signed values only. C4: the publication/retry barrier proof was helper-level only (flag setter plus a sequential helper call), missing §4 D's required executed proof through real producer, response-publication and `Compile.Run` paths executing race-clean under `-race`. All four are conformance corrections with no contract change: the retry state instance is carried across rebuilds; the obligation floor extends forward-only; the watermark carries both numeric domains with the comparison arm chosen from the referenced sequence's stored metadata; the barrier is proven by executed integration tests, and the evidence report distinguishes executed integration proof from helper checks. D1 (design correction): §6 re-verification on the C1-corrected binary then exposed that v17's fixed wall-clock D and §6's zero-client-error drain requirement (v13 I7) are jointly unattainable — on the isolated 2-CN topology the 16-client × 250 herd drained 3,285/4,000 allocations with 3 terminal sessions in 38 s in a quiet window (host load ≈7) and 2,072/4,000 with 8 terminals in 51 s under load (≈17), while the 8-client × 250 calibration completed 2,000/2,000 with rc=0 in 18 s, every terminal surfaced exactly once with duplicate count 0 and delivered prefixes intact, and ≈97% of the 383 tier-1 exhaustions logged across all corrected-build windows were absorbed transparently (11 named terminals, one per affected statement) — while the unchanged base binary drains the same herd (95 s, 4,000/4,000, rc=0) through its unbounded in-statement retry loop. A fixed D loses deterministically whenever the herd's serialized drain exceeds D: a statement admitted while more than D of contention remains loses every generation inside the same window. Resolution: D becomes **progress-renewed** — `min(session statement deadline, now + 10 s)` opened lazily at the first tier-2 admission exactly as v17/G2, renewed on observed peer progress (per-generation committed-position probe of the contended sequence row; direction- and CYCLE-aware; a wrap counts as advance), terminating exactly once after a full renewal interval with NO observed advance; probe failure counts as no-progress. Peer advance is the exact observable signal that a base-drainable herd is still draining, and boundedness is unchanged in kind: base's own retry loop is bounded only by the same session statement deadline. Rejected: (i) a larger fixed window or attempt cap — curve-fit, deterministically defeated by any thicker herd; (ii) accepting §6-with-terminals — regresses base UX and contradicts v13 I7's acceptance basis; (iii) in-attempt remedies (participation leases, lock-free queues, per-CN caches) — already rejected by v12, grounds unchanged; (iv) renewal on ANY transaction conflict — conflates unrelated contention with sequence-row progress and breaks the no-progress bound.
- **v19 (this document)**: independent design review of v18 (B1–B4 below; the progress-renewal direction, the C1–C4 conformance records and the four-issue scope pass review; the D1 mechanism gains four resolutions). B1: v18's strict-advance-beyond-extremum marker ("a wrap counts as advance") is unobservable across complete CYCLE passes — the sequence row persists only the final `(last_seq_num, is_called)` tuple (`seqfunc_autonomous.go:645-658`), with no committed wrap counter or version — so `MINVALUE 1 MAXVALUE 2` peers committing 2 then 1 between every probe leave identical samples despite continuing allocation; resolved by the complete-tuple marker with explicit complete-cycle ABA, SETVAL/reconfiguration and generation-change classifications (§4 D). B2: v18 renewed only at arbitration while three terminal-first exits (`seqfunc_autonomous.go:212-217`, `:339-341`, `sequence_state.go:101-103`) bypassed it — an attempt entered at D−ε or a rebuild crossing D could terminate despite peer advances committed before D; resolved by the probe-first expiry ordering with a specified bounded probe context at every decision point (§4 D). B3: the single overwriteable pending slot (`sequence_retry.go:63,91-118`) plus indistinguishable `NewTxnNeedRetry` conversions let a later payload from sequence B shadow sequence A's exhaustion, so B's committed progress could renew A's stalled window; resolved by a per-`(relation id, generation)` pending map consumed as a unit with conjunctive whole-set renewal (§4 D). B4: the producer-killed live oracle removed contention — the remaining allocator acquires the row, commits and returns success (`seqfunc_autonomous.go:237-240`) — so it could never establish bounded no-progress termination; replaced by the sustained uncommitted-lock blocker oracle, with producer-stop demoted to the successful-drain control (§4 D, §6). All v18 invariants stand: exactly-once terminal, publication precedence, no replay, per-execution D-derived budgets, default-deny admission envelope, frozen tier 1 (I6).
- **v20 (this document)**: independent design review of v19 (A1–A4 below; the progress-renewal direction, the v19 marker and conjunctive pending set, the C1–C4 conformance records and the four-issue scope pass review; the D1 mechanism gains four resolutions). A1: §4 D required every expiry decision to probe the consumed pending set while arbitration consumes it — the post-rebuild entry had no probe set, and an empty set could not be distinguished from resolved contention or a still-stalled sequence (`sequence_retry.go:107-118` removes the payload); resolved by a retained statement-lifetime contention set (the watch set) created per identity at conversion, surviving verdicts/rebuilds/generations, probed by every expiry decision, and removed exactly when its contention observably ends (own committed allocation, availability resolution, or retirement — §4 D). A2: an attempt cancelled by its D-derived context records the context error (`seqfunc_autonomous.go:242-246`) but fails the `retryableSeqTxnErr` conversion gate (`:107-113`, `:280`) — the fixture returns `ctx.Err()` verbatim (`seqfunc_autonomous_test.go:210-212`) — so in-flight expiries terminated without any progress probe; resolved by routing this budget-expiry class into the shared expiry decision (caller cancellation precedence and unrelated immediate-return errors unchanged; §4 D). A3: §4 D forbade opening attempts at expired D and terminated on any unchanged tuple, but a completed herd leaves the row available and unchanged, so the remaining admitted statements would false-terminal against the successful-drain control (§4 D validation list); resolved by the availability discriminant — unchanged watch identities receive one bounded lock-acquisition check inside the probe round and terminate only when committed-unchanged AND locked; acquired identities resolve and a fully resolved statement renews and proceeds (§4 D); the public projection's examples are corrected (a completed herd is never a no-progress terminal cause). A4: the probe-cost bound claimed one round per retry generation, but renewed entries can re-cross expiry inside one generation and allocation calls can span renewal intervals; restated per expiry decision including baseline probes and the identity count (§4 D accounting, §6). All v18/v19 invariants stand: exactly-once terminal, publication precedence, no replay, per-execution D-derived budgets, default-deny admission envelope, frozen tier 1 (I6).
- **v21 (this document)**: independent design review of v20 (Z1–Z2 below; the retained contention set, the availability discriminant, the cancellation routing, the probe-cost accounting, the v19 marker and conjunctive pending set, the v18 C1–C4 conformance records and the four-issue scope pass review; the D1 mechanism gains two transition resolutions). Z1 (expired-D empty-set budget): v20 derived every post-admission attempt context from D while its empty-set decision proceeded attempt-first WITHOUT renewing or retiring D, so an admitted statement that served its contention and processed past D faced a nonpositive attempt budget — `execSeqAutonomous` surfaces the named terminal at/after D before any attempt loop opens (`seqfunc_autonomous.go:213-223`), and a deadline pinned at D−ε fires at the loop's first context check (`:230-236`); the planned never-admitted control could not cover this transition because it never creates a D. Resolved by closing the episode (§4 D): D retires exactly when the watch set becomes empty (own committed-allocation removal, all-resolved probe round, or unresolvable-retirement removal), the statement returns to the base per-invocation `min(statement, 10 s)` budget, and a later conflict is a fresh first admission opening a fresh D with a fresh baselined watch entry; the exactly-once terminal still requires a retained unresolved terminal-eligible identity, and caller-cancellation precedence is untouched. Z2 (late-join baseline initialization): v20 promised baselines at entry creation (the A1 disposition row, §4 D, §10) while also initialising later conversions' baselines lazily at the next probe (§4 D A1 block), and its shared procedure skips probing while `now < D` and then classifies against the last-observed sample — an identity joining just before D reached its first expiry decision with no baseline, so an init-as-unchanged classification could terminal without any observed full no-progress interval and an init-as-progress classification introduced an unspecified renewal. Resolved by one authoritative creation-time initialization with per-identity terminal eligibility (§4 D): every watch entry is created CARRYING its baseline — first admission via the admission probe, later conversions via the conversion path's own bounded `min(statement, 1 s)` committed-tuple sample taken before the entry is recorded (the lazy wording is removed); an initialization failure records the entry young and retries the sample first inside the next decision's single probe round; an entry is terminal-eligible only one full renewal interval after its own baseline (or its creation while un-baselined), only then can a committed-unchanged AND unavailable observation terminal, and initialization/youth vote neither terminal nor renewal — a fresh baseline can never renew a stalled co-identity; with no terminal-eligible identity the decision renews once so each young identity receives its first full observed interval; an entry un-baselined for one full renewal interval from its creation fails safe to the exactly-once terminal. All v18/v19/v20 invariants stand: exactly-once terminal, publication precedence, no replay, per-execution D-derived budgets, default-deny admission envelope, frozen tier 1 (I6).
- **v21 dispositions:**

| # | Finding | Disposition | Where |
|---|---|---|---|
| Z1 | Expired D with an empty watch set has no usable budget: §4 D derives every admitted attempt context from D (remaining = D − now; at/after D no attempt loop opens) while the empty-set decision proceeds attempt-first without renewing or retiring D and the later-admission clause reuses D unconditionally, and `execSeqAutonomous` surfaces the named terminal at/after D (`seqfunc_autonomous.go:213-223`) or fires the pinned deadline at the loop's first context check (`:230-236`) — after an admitted statement allocates successfully, removes its final watch entry and processes beyond D, its next allocation is terminally gated despite resolved contention, and the never-admitted control never exercises the transition. | Accepted (contract correction). The retry episode closes when its contention resolves: D is RETIRED exactly when the watch set becomes empty (the statement's own committed-allocation removal, an all-resolved probe round, or an unresolvable identity's retirement); the statement then runs under the base per-invocation `min(statement deadline, 10 s)` budget — always positive, statement-deadline-capped (`seqfunc_autonomous.go:199-224` construction) — and a later conflict is a FRESH first admission (new D = `min(session statement deadline, now + 10 s)`, new watch entry with its Z2 baseline). The later-admissions-reuse-D clause applies only while D is live with unresolved contention retained. The exactly-once terminal still requires a retained unresolved terminal-eligible identity; caller-cancellation precedence is unchanged. Deterministic cases added: admitted ⇒ successful allocation ⇒ empty set ⇒ cross the old D ⇒ next allocation succeeds on a fresh per-invocation budget; re-contention re-admits fresh; caller cancellation after closure propagates unchanged; an all-resolved probe round retires instead of renewing. | §4 D (budget header, A1 block, decision steps (3)/(6)), §6, §10 |
| Z2 | Baseline initialization undefined for late-joining identities: the v20 A1 disposition row and v20 §10 promised a baseline at entry creation, but v20's A1 block initialised later conversions' baselines "lazily at the next probe", the shared procedure skips probing while `now < D` (step (2)) and then classifies against the last-observed sample (step (4)) — an identity joining shortly before D reached its first expiry decision with no baseline, leaving init-as-unchanged (terminal without a full no-progress interval) and init-as-progress (an unspecified renewal) both contradictory. | Accepted (contract correction). One authoritative initialization: every watch entry is created WITH its baseline sample — first admission by the admission probe, later conversions by the conversion path's own bounded `min(statement, 1 s)` lock-free committed-tuple sample taken before the entry is recorded (the lazy wording is removed). Initialization failure records the entry YOUNG without a baseline and retries the sample first inside the next decision's single probe round. Each entry carries a terminal-eligibility clock measured from its own baseline (or creation while un-baselined): an identity terminals only after one full renewal interval observed unchanged AND held; a young identity never votes terminal and never renews — including a fresh baseline taken while a terminal-eligible co-identity is stalled-and-held (the cannot-renew-A control); with no terminal-eligible identity the decision renews once so each young identity gets its first full interval; an entry un-baselined for one full renewal interval from creation fails safe to the terminal. Cases added: B joining at D−ε, first-sample behavior, the subsequent static-and-held interval, the cannot-renew-A control, and the init-failure arm. | §4 D (A1 block, B1 block, decision steps (4)/(5), accounting), §6, §10 |

- **v20 dispositions:**

| # | Finding | Disposition | Where |
|---|---|---|---|
| A1 | Retained probe set undefined outside arbitration: v19 §4 D required every expiry decision to probe the consumed pending set, but arbitration consumes that set (`sequence_retry.go:107-118` removes its payload), the post-rebuild entry precedes any new conversion, and the baselines' relationship to active contention, newly encountered sequences and retired identities was undefined — an empty pending set could not be distinguished from resolved contention or a previously stalled sequence. | Accepted (contract correction). §4 D gains the retained contention set (watch set): one entry per contended identity created at its first admission/conversion and carrying the baseline sample; the consumable pending payloads stay per-generation (B3) while the watch set persists across verdicts, rebuilds and generations; every expiry decision probes the CURRENT watch set; an entry is removed exactly when its contention observably ends (own committed allocation, A3 availability resolution, or unresolvable retirement); an empty set proceeds attempt-first at entry. Deterministic cases added: successive-generation exhaustion, watch survival across consumption/rebuild, successful-allocation removal. | §4 D (A1 block), §10 |
| A2 | D-derived cancellation bypassed the renewal decision: an attempt cancelled when its D-derived context fires is recorded (`seqfunc_autonomous.go:242-246`) but conversion is gated on `retryableSeqTxnErr` (`:280`), which excludes plain context errors (`:107-113`); the blocked-attempt fixture returns `ctx.Err()` verbatim (`seqfunc_autonomous_test.go:210-212`), so in-flight expiries terminate before any progress probe despite observable peer advancement. | Accepted (contract correction). The recording site tags the attempt error with its cancellation provenance; in an admitted statement the derived-budget-expiry class bypasses the transient classifier for the conversion gate ONLY and routes into the shared probe-first expiry decision, which renews or terminals exactly once. Caller-cancellation precedence and the immediate return of unrelated terminal errors are unchanged; a pre-admission budget expiry keeps its unchanged named surface. The in-flight `Compile.Run` case must exercise actual derived-context cancellation (a real mid-flight deadline crossing), never an injected retry-class error. | §4 D (A2 block), §10 |
| A3 | Expiry after contention has disappeared: v19 §4 D forbade opening an attempt at expired D and terminated on an unchanged tuple; a completed herd leaves the row available and unchanged, so adherence terminals the remaining admitted statements, contradicting the successful-drain control (§4 D validation list), and the public projection mislisted a completed herd as a no-progress terminal cause. | Accepted (contract correction). The shared expiry decision terminates only on committed-unchanged AND unavailable: each unchanged watch identity receives ONE bounded lock-acquisition check inside the probe round (acquire-and-release, no values consumed); an acquired identity is RESOLVED and removed, and a fully resolved set renews D and proceeds — preserving the successful-drain contract; a locked unchanged identity yields the exactly-once no-progress terminal; check failure fails safe to locked. Deterministic cases added: unchanged-but-available resolution, producer-stop with rebuild crossing D (fake-clock and live). | §4 D (decision text), §6, §10, projection |
| A4 | Probe-cost bound misstated: v19 §4 D (`:666-670`) and §6 (`:1041-1043`) claimed at most one probe round per retry generation, but an expired entry can probe and renew, then consume the renewed interval and reach another expiry decision within the same generation, and multiple allocation calls of one generation can cross renewal intervals without a compiler retry. | Accepted (editorial correction). Cost is expressed per expiry decision: one bounded tuple-probe round of every watch identity, plus at most one bounded lock-acquisition check per unchanged identity in that round, plus one baseline-initialization sample per newly contended identity at admission/conversion; distinct decisions of one statement are bounded by the number of renewal intervals the statement consumes. Never per retry generation. | §4 D (accounting), §6 |
- **v19 dispositions:**

| # | Finding | Disposition | Where |
|---|---|---|---|
| B1 | Progress marker unobservable across complete CYCLE wraps: v18 §4 D required strict numeric advance beyond a recorded extremum while counting wraps as advance, but the sequence row persists only `(last_seq_num, is_called)` (`seqfunc_autonomous.go:645-658`) with no committed wrap counter, so `MINVALUE 1 MAXVALUE 2` emission patterns can return an identical sampled tuple after every full pass; a simple observed backward transition is also not wrap evidence, and SETVAL/reconfiguration/generation changes were unspecified. | Accepted (contract correction). §4 D gains the authoritative marker — the complete committed tuple keyed per `(relation id, generation)` — with exact classifications: identical tuple ⇒ no-progress (the accepted complete-cycle ABA quantization limit, pinned by planned deterministic full-wrap and genuinely-unchanged cases); changed tuple ⇒ progress, labelled direction-consistent emission (strict advance or the boundary wrap signature) or reconfiguration (SETVAL/ALTER/generation change); backward transitions are never wrap evidence; reconfiguration renews once per observation and re-baselines. | §4 D (B1 block), §6, §10 |
| B2 | Terminal-first expiry bypassed renewal: v18 renewed only at arbitration, while the allocation-entry zero-attempt surface (`seqfunc_autonomous.go:212-217`), the conversion pre-filter (`:339-341`) and the arbitration check (`sequence_state.go:101-103`) terminate on the wall clock alone — attempts entered at D−ε or rebuilds crossing D could terminate despite peer advances committed before D, contradicting the transparency promise of §6. | Accepted (contract correction). One shared probe-first expiry procedure at every decision point: caller cancellation first; `now < D` proceeds without probing; `now ≥ D` takes ONE bounded probe round (derived context `min(statement, 1 s)`, lock-free RC snapshot read over the consumed pending set) — observable change on every contended identity renews and proceeds, any failed probe or unchanged identity yields the exactly-once named terminal. Planned fake-clock `Compile.Run` cases pin progress-before-D with execution crossing D (in-flight conversion and post-rebuild entry), static-position expiry, and caller cancellation. | §4 D (B2 block), §6, §10 |
| B3 | Concurrent conflict payloads uncorrelated with arbitration: v18 added a relation id to each pending conversion but left selection undefined when parallel allocations both exhaust; the single overwriteable pending slot (`sequence_retry.go:63,91-118`) plus indistinguishable `NewTxnNeedRetry` signals let a later payload from sequence B shadow sequence A's exhaustion, so B's committed progress could renew A's stalled window, violating the per-contended-row no-progress rule. | Accepted (contract correction). The pending collection becomes a per-`(relation id, generation)` map; arbitration consumes the whole current-generation set and answers conjunctively — D renews only when EVERY contended identity shows an observable committed change; a declined verdict surfaces the first-recorded entry's named terminal. Planned two-sequence barrier case: A static under a held uncommitted row lock, B advancing throughout (B must never renew A's window; exactly one named terminal after one full no-progress interval), plus the symmetric arm and the both-advancing renewal control. | §4 D (B3 block), §10 |
| B4 | Live no-progress oracle invalid: v18's producer-killed starved control removes contention — the remaining allocator acquires the row, commits and returns success (`seqfunc_autonomous.go:237-240`) — so it cannot establish bounded termination while conflicts persist, although §4 D and §6 designated it as that proof. | Accepted (oracle correction). Replaced by the sustained-no-progress blocker: a peer session holds an explicit uncommitted `SELECT … FOR UPDATE` on the sequence row (the documented-misuse shape); the admitted statement must receive exactly one named no-progress terminal after one full renewal interval, with cleanup verified; the blocker is then released (COMMIT) and a fresh session's `NEXTVAL` must succeed with the next value. Producer-stop is demoted to the successful-drain control (remaining admitted statements complete successfully, exactly-unique). | §4 D (validation list), §6, §10 |
- **v18 dispositions:**

| # | Finding | Disposition | Where |
|---|---|---|---|
| C1 | Retry rebuild discards the admitted `SeqRetry`: `compile.go:157` installs fresh state per `NewCompile` and `buildRetryCompile` (`compile2.go:1038`) constructs the retry generation through it, so the sticky window D created at arbitration was dropped and every generation re-budgeted (de-facto unbounded retry; the previous turn's §6 green ran under this defect). | Accepted (conformance; no contract change). `buildRetryCompile` saves the admitted instance across `NewCompile` and re-installs it (sticky window and publication survive the retry transition by construction); `compile.go:157` creates fresh state only for genuinely new statements (pooled `Reset`). Deterministic `Compile.Run` regressions prove identity preservation across multiple rebuilds, window-identity across arbitration events, and exactly one terminal error after the original deadline. | §4 D (referenced), §0 |
| C2 | Unconditional obligation-floor rewrite after statement rewind (`seqfunc_autonomous.go:775`): SETVAL to 60 with no prior obligation, allocate 60, statement fails ⇒ rollback restores pending 10 while the obligation was rewritten to [60,10], protecting nothing; a later allocation can reissue 60 and COMMIT can persist the regressed position, violating K1. Existing failed-boundary tests always seed an earlier obligation and miss this shape. | Accepted (conformance; no contract change). The floor move becomes forward-only (direction-aware monotone `Extend`): repairing candidates inside [PassStart, Floor] never lowers the recorded frontier, and candidates before PassStart keep their protection. UT oracles: failed SETVAL then allocation with no previous obligation, ascending and descending, plus continuation after COMMIT. | §4 A/D (referenced), §0 |
| C3 | Merge watermark compared in the destination column's domain (`data_branch_sequence_reconcile.go:271-290`): a `BIGINT UNSIGNED` sequence backing a VARCHAR(32) default, start 9223372036854775806, branch allocations …807/…808 ⇒ the uint64-as-int64 maximum picks 9223372036854775807, reconciles there, and the next default insert regenerates the already-merged …808 (duplicate key). Existing VARCHAR pins use small signed values only. | Accepted (conformance; no contract change). The watermark carries both domain extremes {maxU, minU, maxS, minS}; the comparison arm is chosen from the referenced sequence's stored metadata (its AS type), never from the destination column. Regression: merge crossing the signed 64-bit boundary through a VARCHAR(32) default — reconcile to …808, next default insert …809, fresh-session continuation …810 on the second CN. | §4 B (referenced), §0 |
| C4 | Publication/retry barrier proof only helper-level: `pkg/frontend/sequence_retry_published_test.go:28` calls the flag setter; `pkg/sql/compile/sequence_retry_arbitration_test.go:105` sets a flag then invokes the arbitration helper — neither runs `Compile.Run`, real producer quiescence, or a response writer, and the Challenger plan carried no partially-streamed conflict case. | Accepted (conformance; no contract change). Executed integration barrier tests drive a real sink through publication and the retry boundary under `-race` (zero warnings): publish-after-check ⇒ retry declined, exact delivered prefix, the original named terminal exactly once, no replay or duplication; admitted (unpublished) ⇒ transparent retry to full completion. The evidence report distinguishes executed integration proof from retained helper checks. | §4 D (referenced), §0 |
| D1 | Fixed wall-clock D vs §6 zero-error drain (v13 I7): with C1 making D enforceable, any sustained herd whose serialized drain exceeds D terminally errors admitted statements deterministically (measured on the corrected 2-CN build: 16×250 quiet 3,285/4,000 with 3 terminals in 38 s; loaded 2,072/4,000 with 8 terminals in 51 s; 8×250 clean 2,000/2,000 rc=0 in 18 s; base drains 4,000/4,000 rc=0 in 95 s via its unbounded in-statement loop). The prior turn's fixed-D green is explained by C1 and cited only as defect evidence. | Accepted (contract correction). D becomes progress-renewed per §4 D (v18, D1): same lazy creation point, precedence, and sticky identity; renewal on observed committed-position advance (one probe per retry generation, direction/CYCLE-aware, per-relation-id extremum, wrap = advance, probe failure = no-progress); exactly-once terminal after one full no-progress interval. Invariants preserved: exactly-once terminal, publication precedence, no replay, per-execution D-derived budgets, default-deny admission envelope, frozen tier 1 (I6). New oracles: fake-clock renewal matrix, `Compile.Run` renewed-D identity with a single terminal after the first full no-progress interval, producer-killed starved control asserting the no-progress terminal, and the live §6 gate re-run under renewed D (zero client errors wherever base completes; ≤3× wall clock stands). | §4 D, §6, §10 |

- **v17 dispositions:**

| # | Finding | Disposition | Where |
|---|---|---|---|
| E1 | §4 C (`:329-335`) requires restart only when the requested start is strictly ahead, covering both `is_called` states (else the pending position stays), while §8 (`:801-803`) and the public projection promise an exact restart whenever the sequence has never issued: for `CREATE SEQUENCE s AS BIGINT START WITH 10 NO CYCLE` + `ALTER SEQUENCE s START WITH 5` these contracts require different fresh-session results (10 versus 5). The pinned UT matrix already encodes keep-position (`alter_sequence_state_test.go:217,:224`); the uncalled BVT (`alter_sequence_start_with.sql:58-62`) tests only an ahead start and cannot expose the contradiction. | Accepted. One authoritative rule recorded in §4 C (new “Uncalled (never-issued) START WITH” block): restart iff strictly ahead of the pending value, uniform for `is_called=0/1`; a behind-start on a never-issued sequence keeps the unissued pending value (exact ascending and descending examples with state and fresh-session nextval); the single exception is the out-of-bounds pending restart at the declared start (CYCLE and NO CYCLE alike; the declared start is validated atomically, out-of-bounds ⇒ atomic rejection). §8 aligned; the public projection rewritten to match; §9 gains behind-start uncalled BVT pins in both directions with fresh-session assertions. | §4 C, §8, §9, §10 |

- **v16 dispositions:**

| # | Finding | Disposition | Where |
|---|---|---|---|
| F1 | v15 §4 D (publication term and arbitration block) explicitly permitted using `SessionInfo.ResultRows` as a pre-filter hint, but `RespResult` writes that counter through `AddResultRows` (`resp_client.go:241-242`) and `process/types.go:955`/`:962` access it unsynchronized: after the pre-filter observes the atomic flag unset, publication can begin while the hint reads the counter, so v15 still contained a data race that the quiescent arbitration cannot eliminate. | Accepted. Both admission phases read ONLY the synchronized per-statement publication flag; the `ResultRows` hint is removed from the contract (its accounting stays untouched as ordinary statistics, never consulted for admission). The removal is behavior-neutral: the flag is set before the first batch's bytes by the single publication path, so the hint could observe nothing the flag misses. The publish-after-observation barrier oracle is kept and its `-race`-clean execution (zero warnings) is required, proving the pre-filter reads no unsynchronized state. | §4 D, §0, §10 |

- **v15 dispositions:**

| # | Finding | Disposition | Where |
|---|---|---|---|
| G1 | v14 made `ResultRows==0` at the `seqfunc_autonomous.go` conversion site authoritative, but sequence-bearing AP statements remain parallel within one CN (`compile.go:1476-1486`) and producer scopes run concurrently (`scope.go:606-628`): another pipeline can publish a batch (`resp_client.go:241-249`) after that check but before the compiler selects retry (`compile2.go:708-709`) and quiesces producers (`:916-918`); `ResultRows` reads/writes are themselves unsynchronized (`process/types.go:950-962`). The converted retry signal would stay eligible despite an already-delivered prefix, and the planned streamed-prefix control misses the check-to-publication interleaving. | Accepted. Admission is two-phase: cheap pre-filter at the conversion site; authoritative arbitration at the retry quiescence boundary (post-producer-join, pre-rebuild, `compile2.go:752`→`:916-918`→`:801-805`) against a synchronized per-statement publication flag set by the single publication path before client bytes are written. Publication always wins: the retry signal is dropped and the original named exhaustion error is returned exactly once; no replay or duplicated prefix. The producer join supplies the happens-before edge; the atomic flag makes the pre-filter hint well-defined. Deterministic barrier oracle (publish-after-check, `-race` validated) added to the plan. The compiler's generic retry policy for other errors is unchanged. | §4 D, §0, §9, §10 |
| G2 | v14 created D at the first sequence-allocation entry and derived every tier-1 execution from D, so a statement that allocates, spends longer than the window processing or streaming WITHOUT any conflict, and allocates again fails immediately on D expiry — broadening a contention-retry budget into a timeout for ordinary successful statements (the base per-invocation `now+10 s` window at `seqfunc_autonomous.go:184-189` imposes no such statement-wide limit), a compatibility change that §8 and the projection did not expose; the validation plan lacked a no-conflict control. | Accepted. Budget lifecycle split: D (`min(statement, 10 s)`) bounds only the admitted conflict-replay envelope and is created lazily at the statement's first tier-2 admission; pre-admission executions keep the base per-invocation budgets (`seqfunc_autonomous.go:185-189` semantics unchanged); post-admission executions derive from the sticky D, which never resets across that statement's retries; a fresh statement has no D until its own first admission. Terminal-once precedence, caller-cancellation passthrough, and `ExecTxn` defer cleanup are unchanged; the end-to-end boundedness claim is scoped to post-admission allocation work. Fake-clock no-conflict multi-allocation control and the no-reset window-identity case added to the plan; the public projection is aligned. | §4 D, §0, §9, §10 |

- **v14 dispositions:**

| # | Finding | Disposition | Where |
|---|---|---|---|
| H1 | v13's admission story rested on "retry-safe auto-commit shapes" and the claim that the compiler retries only auto-commit statements. In fact the generic policy admits RC retry errors without checking autocommit, ByBegin, or statement shape (`isRetryErr`/`canRetry`, `compile.go:713-720,793-797`), and `RespResult` streams batches before statement completion (`resp_client.go:239-246`, flushed per packet): v13 neither excluded explicit transactions nor established that replay after a later sequence conflict cannot repeat an already-delivered result prefix. | Accepted. Tier-2 conversion is conditioned on a named admission predicate evaluated at the conversion site (the enforcement owner): (1) dispatch envelope {Autocommit,¬ByBegin} (`frontend/txn.go:812` → `txn/client/operator.go:225-229`; a CN-side read path ships with the implementation), default-deny otherwise; (2) RC isolation; (3) generic `canRetry` acceptance (unchanged for other errors); (4) `SessionInfo.ResultRows==0` at conversion time. Explicit `BEGIN`/multi-statement transactions, `autocommit=0`, partially streamed result sets, and background envelopes receive the terminal error unconverted. Rejection and admission control oracles added to the validation plan. | §4 D, §0, §9 |
| H2 | v13 checked the sticky deadline only at the conversion instant: `execSeqAutonomous` opened a fresh `now+10 s` context on every invocation (`seqfunc_autonomous.go:184-186`) and retry rebuild reuses the original top context (`compile2.go:801`), so a retry admitted just before expiry could enter another full allocation budget; conversion-time checking alone did not establish the promised end-to-end bound. | Accepted. Every admitted tier-1 execution derives its attempt context from the original D (remaining = D−now), so an execution entered at D−ε has only ε of attempt budget; conversion at or after D returns the terminal error without opening an attempt loop. Precedence: caller-cancellation unchanged; D-expiry → named error exactly once, no further conversion; retryable classes under an expired D lose retryability. Cleanup remains the per-attempt `ExecTxn` defer. The end-to-end claim is scoped to allocation-path work; replan/rebuild stays bounded by the session statement deadline (`compile2.go:801`). Deterministic just-before-expiry `Compile.Run` oracle added. | §4 D, §0, §9 |
| H3 | The submitted public behavior projection claimed every `ALTER … START WITH s` makes a fresh session issue `s`, contradicting §4 C direction-aware clamping (called position 100, increment 1, `START WITH 50` must continue at 101 — pinned by `alter_sequence_start_with.sql:31-55`), and claimed any failed merge leaves sequence state unchanged, contradicting the accepted reconciliation gap for caller failure/rollback after the reconcile unit committed (§4 B, §8 v4). | Accepted. Projection corrected: restart-to-exactly-`n` is scoped to ahead-of-frontier or fresh sequences, clamped-behind starts keep position and allocation continues past the frontier; merge failure is split into the fully atomic reconcile-unit arm and the documented consumed-advance gap arm. Behind-frontier oracles preserved (existing BVT pins); the post-reconcile-rollback gap pin is added to the validation plan; §8 phrasing aligned. | §4 C, §8, §9 |

- **v13 dispositions:**

| # | Finding | Disposition | Where |
|---|---|---|---|
| I7 | Pre-PR review F5 required the reviewed terminal exhaustion bound end-to-end: the implementation's unspecified conversion of tier-1 exhaustion into compiler retry had made the effective bound unbounded (`seqfunc_autonomous.go` conversion → `Compile.Run` re-enters with a fresh 8-attempt budget). Applied verbatim, the terminal-only machine fails the §6 measured experiment on the isolated 2-CN topology: 16 clients × 250 `NEXTVAL` — 14/16 clients die with `ERROR 20101 (HY000): internal error: sequence seqperf.u: allocation could not acquire the sequence row within 8 attempts: txn need retry in rc mode` at statements 1–22 of 250 (phase wall 3 s; 286 of 4,000 allocations; `PERF_v3_EXIT=1`), while the unchanged base binary completes the same workload in 95 s with 4,000/4,000 unique allocations and rc=0 for every client (`PERF_base_EXIT=0`), and the conversion build completed it in 86 s. Root cause: the I6 calibration drained only the single-shot 16-client control (each winner leaves the contention set permanently); under §6's sustained stampede the set never drains within the frozen ~0.6 s tier-1 window, and v12 forbids the in-attempt remedies. | Accepted (bounded tier 2; contract change in the retry envelope only): (i) tier 1 frozen per I6 — 8 attempts, capped 25/50/100 ms + jitter backoff, `min(statement, 10 s)` attempt-loop deadline, non-retryable set, single publish point; (ii) tier-1 exhaustion on the transient-conflict class (and only that class) converts to the compiler retry signal, for retry-safe auto-commit shapes only, while the statement-scoped sequence deadline has not elapsed; the deadline (`min(statement, 10 s)`) is created once per statement at the first sequence-allocation entry, lives in the compiler's `sequenceStatementState` (`pkg/sql/compile/sequence_state.go:28`), and is preserved across `prepareRetryTransition` (`compile2.go:913`), so one statement's overall work is bounded end-to-end; a fresh statement execution creates a fresh deadline; (iii) on deadline expiry, or for any other error class, the named lock-wait/conflict exhaustion error escapes unconverted and is returned to the client exactly once — the terminal surface is unchanged from v12; (iv) explicit-transaction callers never enter tier 2 (the compiler retries only auto-commit shapes) and their conflict error surfaces as today; (v) the v12 rejections stand: no unbounded retry (upheld — tier 2 keeps every caller statement deadline-bounded), no RC-semantics change, no in-attempt wait-retry, no write-first blind increment. Oracles: compiler UT driving the real retry boundary with injected tier-1 conflict exhaustion — retry under the deadline with the *same* deadline instance, terminal escape after expiry exactly once, fresh statement → fresh deadline, non-conflict class never converted — proving the overall bound through `Compile.Run` (`compile2.go:432`, retry loop `:745-775`); allocation-surface matrix UT updated to the two-tier truth; live §6 gate re-run as the first check of the implementation turn (post ≤ 3× base, 4,000 exactly-unique, zero client errors); 16-client single-shot uniqueness control re-run; `-race` owning-package ladder re-run. | §0, §4 D, §6, §9, §10 |

- **v12 dispositions:**

| # | Finding | Disposition | Where |
|---|---|---|---|
| I6 | Implementation-turn live verification on final-candidate code: the 2-CN 16-client `nextval` control (one `nextval` per client, split across both CN endpoints) failed deterministically even on a contamination-free cluster — every loser surfaced a raw `ERROR 20628 (HY000): txn need retry in rc mode` after exactly one autonomous attempt (service logs: one `internal sql executor error` per failing session, client failure ~5 ms later). Root cause: `txnExecutor.rollback` returns `errors.Join(attemptErr, rollbackErr)` (`compile/sql_executor.go:901-909`), whose dynamic type defeats `moerr.IsMoErrCode`'s direct `*Error` assertion (`moerr/error.go:887-898`), so the transient-conflict set the approved retry contract names was never matched for attempt-raised conflicts; happy-path evidence never exercised the retry path (UT injected raw moerr values). After repairing classification to traverse wrappers (`errors.As`; allocation + commit-stamp classifiers), the same control improved to 13–14/16 unique with no duplicates but left 2–3 clients at retry **exhaustion** (`within 4 attempts: txn need retry in rc mode`); the unchanged base binary passes 16/16 twice on the identical A/B single-CN-process topology (no built-in 6001 proxy). | Accepted (calibration only): the wrapper-traversing classification is contract conformance (the transient set is retried as specified) and needs no contract text. The named retry bound calibrates 4 → **8 attempts** with the documented backoff schedule explicitly capped per step (25/50/100 ms + jitter); deadline `min(statement, 10 s)`, non-retryable set, single publish point, and the named-exhaustion error are unchanged. UT pins wrapper traversal and join-wrapped retry engagement; the 2-CN 16-client control is re-run on the calibrated build in the implementation turn. | §0, §4 D, §10 |

- **v10 dispositions:**

| # | Finding | Disposition | Where |
|---|---|---|---|
| I5 | Implementation-turn verification on base `4769417cdc` with the v9 implementation live: the v8/v9 §9 I-block oracles require `BEGIN; CREATE DATABASE dst CLONE src; …; DATA BRANCH MERGE br.t INTO dst.t; COMMIT` — a merge whose destination *database* exists only inside the caller's uncommitted transaction. Such a merge fails on this base *before any row apply* at the branch-meta created-time lookup: `ERROR 20101 (HY000): internal error: cannot find database <id> (<db>) created_time at snapshot <ts>-1` (call site `pkg/frontend/data_branch.go:3343` → probe `getDatabaseCreatedTimeLowerBoundByPK` `:3355`, miss error `:3408-3412`; the file carries no diff from base). INSERT/SELECT into the same transaction-local clone succeeds — only the merge machinery's snapshot catalog probe misses the caller's uncommitted database (that catalog read is not a read-your-own-writes path). Consequence: no SQL statement on this base can leave a surviving reconcile advance on a *transaction-local* generation at COMMIT — in-caller non-allocation advance exists only through merge reconciliation (§4 B step 3), J2 SETVAL boundaries clear obligations by design, and J1 wraps re-seed pass-scoped obligations. The I1 preserve-arm scenario the v8 protocol defends is reachable today only by driving the reconcile unit's in-caller partition directly. Making the merge probe RYOW-correct is merge-machinery work outside the assigned four-defect scope. | Accepted. Contract and production code **unchanged** — the preserve-or-restore stamp stays, UT verification becomes authoritative for the transaction-local preserve arm, and only §9 oracle placement moves: (a) the I1/K1-harness UT (fake workspace + reconcile-update primitive; ascending, descending, CYCLE pass cross variants) is the single discriminating layer for the commit-stamp preserve-vs-restore decision on transaction-local generations; (b) the SQL cover becomes a *boundary control* pinning the verified 20101 atomic rejection — `BEGIN; CREATE DATABASE dst CLONE src;` + default `INSERT` (obligation floor raised) + `DATA BRANCH MERGE br.t INTO dst.t` ⇒ `20101`, statement-only failure, no partial rows, sequence state unchanged, transaction remains usable; `COMMIT` ⇒ the pre-merge position persists and a fresh session continues past it; the `ROLLBACK` arm vanishes the generation; and (c) committed-route equivalents for the same allocation/commit shapes (explicit-txn merge into a *committed* destination reconciles through the autonomous unit; COMMIT persists the reconciled counter; a fresh-session default INSERT lands beyond merged keys). The L1 "merge against a transaction-local destination" oracle is reshaped the same way; its post-merge INSERT allocation never discriminated I1 and the committed-route hybrid-routing oracle is unchanged. No merge/DDL admission is expanded; the base rejection is documented as a compatibility boundary (§8). A future RYOW fix in the merge machinery can promote the UT preserve oracles to BVT unchanged. | §0, §8, §9 |

The v1–v9 disposition table follows unchanged (v9's P3 correction row first):

| # | Finding | Disposition | Where |
|---|---|---|---|
| P3 | The L1-era text classified `BEGIN; CREATE SEQUENCE` and in-transaction `ALTER` as existing supported preservation flows. Foreground `CREATE SEQUENCE` is rejected under explicit `BEGIN` (`stmt_kind.go:237-238` — admitted only for background sessions or without `OPTION_BEGIN`); `ALTER SEQUENCE` is absent from the explicit-transaction admission switch, which returns `false` (`stmt_kind.go:360`); `DROP SEQUENCE` forces the active transaction to commit first (`:166-170`), so `DROP`+`CREATE` cannot form an in-txn generation either. Supported shared-background clone execution does not establish these foreground controls. | Accepted (approval condition). Support claims corrected: on this base a transaction-local generation arises **only** from an explicit-transaction CLONE or snapshot restore (`clone.go:629-635`, pinned by `issue_27055_clone_database_transaction.sql:15-28`; DDL+state through the same caller-txn executor, `clone.go:1331-1354` → `snapshot.go:2943-2963`). Routing/oracle text updated; the rejected foreground shapes become explicit rejection-control assertions. DDL admission is **not** expanded — the routing contract, obligation protocol, and all acceptance criteria are unchanged from approved v8. (The L1 row's "same shape" sentence is review evidence quoted from its round, superseded by this correction.) | §2 I4, §4 D, §8, §9 |
| M1 | The v3 registry + in-caller fallback made returned values reclaimable: with state (1,true), `BEGIN; MERGE` (key 2), `nextval` (returns 3), `ROLLBACK` restores (1,true) while the client observed 3 and CURRVAL still reports it — later allocations return 2 and 3 again, retaining #30896 on a supported path. | Accepted. The in-caller allocation fallback and the I4 same-transaction carve-out are **removed**. Allocation is *always* autonomous; reconciliation moves out of the merge transaction into one autonomous, atomic reconcile unit sequenced inside the merge flow. Merge rollback then produces gaps, never reuse. (v5 L1 refines: "always autonomous" applies to committed generations; transaction-local generations allocate in-caller — M1's scenario touches a committed sequence and is unaffected.) | §2 I4, §4 B/D, §8, §9 |
| M2 | v3 validated new MIN/MAX before applying the restart rule but did not specify validating the final clamped state; existing code already validates the computed start via `valueCheckOut` and rejects final conflicts atomically. | Accepted. The C contract now explicitly validates the *final effective* `(min, max, startN)` — after the restart/clamp decision — with `valueCheckOut` semantics before persistence, rejecting atomically; ascending/descending boundary oracles and a combined START+MIN/MAX CREATE-replay oracle added. | §4 C, §9 C |
| M3 | v3's registry had no owner, bound, or terminal cleanup. | Accepted by removal: the registry concept is deleted with the fallback that motivated it; no session state is introduced. | §4 D |
| L1 | v4's "always autonomous" allocation breaks supported pre-COMMIT use of generations that exist only inside the caller's transaction: `BEGIN; CREATE DATABASE dst CLONE src; USE dst; SELECT nextval('s'); ROLLBACK` (explicit-txn clone borrows the caller txn, `clone.go:629-635`, pinned as supported by `issue_27055_clone_database_transaction.sql:15-28`; the cloned sequence's DDL and state are written through that same executor, `clone.go:1331-1354` → `snapshot.go:2943-2963`) — transaction-local databases and sequences resolve only through the owning transaction's workspace (`engine.go:500-512`), so no autonomous transaction can see them. Same shape for `BEGIN; CREATE SEQUENCE s; nextval` and in-txn ALTER drop/recreate. | Accepted. Allocation is routed by **generation visibility**: committed generations keep the v4 always-autonomous path; transaction-local generations allocate inside the caller transaction — the only transaction that can resolve them. Rollback non-reuse survives by fate-sharing: the generation and its in-caller allocations commit or vanish together (SAVEPOINT unimplemented, `mysql_cmd_executor.go:5703`; a statement error rolls back the whole transaction, `txn.go:44-75`), so COMMIT persists them atomically and ROLLBACK leaves no surviving generation that could reissue a published value (I4 extended, still generation-scoped). Merge reconciliation partitions along the same visibility rule. (v6 K1 retracts the fate-sharing premise: default-classified statement errors roll back only the statement, so a local generation can outlive rewound allocations; the floor + commit-stamp protocol replaces the premise — the routing decision itself is unaffected.) | §2 I2/I4, §4 B/D, §5, §8, §9 |
| L2 | v4's reconcile unit locks every affected sequence in one transaction but specified only per-sequence "catalog before row" ordering. Two merges sharing sequences S1/S2 in reversed watermark order hold *shared* (compatible) catalog locks (`lock_meta.go:188-201`, `LockMode_Shared`) and can wait on each other's row locks — a cycle the ordering rule permits; the "nobody holds row while requesting catalog" deadlock-freedom argument was false for sequential multi-identity acquisition, and an ALTER competitor can close a cycle through its exclusive catalog lock. | Accepted. All sequence lock acquisition now follows one **global canonical order** — identities ranked by `(account_id, relation_id)` ascending, catalog lock before data-row lock within each identity — applied by every participant (reconcile units walk their sorted set; allocators and ALTER/DROP touch one identity at a time). After acquiring the full ordered set, a reconcile unit revalidates every identity and performs a **whole-unit abort, re-plan, bounded restart** on any change. Deterministic barrier-based oracles for reversed-order reconcile pairs and an ALTER competitor added; the false v4 sentence is corrected in §4 D. (v6 K2 refines: validation moved *per identity, before proceeding*, because acquire-then-validate leaves a rank-change window — see below.) | §2 I2, §4 B/D, §9 |
| K1 | v5's fate-sharing premise — "a statement error rolls back the whole transaction" — is false for the default error classification: in an explicit transaction a nonfatal statement error invokes `RollbackLastStatement` (`txn.go:1164-1199`), truncating only the failing statement's workspace writes (`disttae/types.go:1174-1194`); only ~12 infrastructure codes escalate (`util.go:2277-2286`, `moerr.MustRollbackTxn`, `moerr/txn.go:31-37`), and NO CYCLE exhaustion is a generic internal error (`seqfunc.go:395/402`). Result batches are written per batch (`resp_client.go:234-249`, flushed per packet `mysql_buffer.go:899-920`), so a multi-batch NEXTVAL on a transaction-local generation can stream values, then fail, have its counter updates rewound, and leave the generation alive — the streamed values are re-issuable; likewise a failed default `INSERT`'s generated id reissues on retry. | Accepted. The fate-sharing premise is retracted and replaced by an allocation/publication protocol for transaction-local generations: a per-(txn, identity) monotonic **floor** in session state, raised at allocation time before emission (independent of wire streaming and of statement outcome), applied by every later allocation in the txn (never re-issue ≤ floor, repairing a workspace rewind forward) plus a **pre-commit monotonic advance stamp** for any floored local generation so COMMIT persists a counter strictly past every issued value. Existing transaction error semantics are untouched (statement rollback still rewinds the workspace; whole-txn limits unchanged); the floor is bounded, txn-owned, terminally cleaned (contrast M3). Deliberate SETVAL/restart resets the floor (I4 boundary). Deterministic oracles added. | §2 I4, §4 B/D, §5, §6, §8, §9, §10 |
| K2 | v5's reconcile unit acquires the complete ordered lock set *before* validating identities. Acquisition statements resolve names at statement-plan time (`sql_executor.go:604-635`), so a rank-changing recreation between planning and acquisition lets unit A (stale order 10→20, actually locking recreated id 30) and unit B (current order 20→30) wait on each other's rows under compatible shared catalog locks — neither reaches the final revalidation. | Accepted. Identity validation moves into the acquisition loop: per planned identity, acquire the shared catalog lock, validate the committed relation id **under that lock**, and only then take the data-row lock bound to the validated id (id-pinned, not name-re-resolved); **any mismatch aborts the whole unit immediately**, before any further acquisition, and re-plans bounded. A waiting unit holds the catalog lock of every acquired identity shared, pinning all ranks current, so wait-for ranks strictly increase — no cycle. The barrier oracle extends to the rank-change recreation race with a competing unit. | §4 B/D, §9 |
| J1 | v6's floor is a raw numeric max/min with strictly-beyond enforcement and a numerically monotonic stamp, contradicting the retained CYCLE contract (§2 I4, §8): a descending clone must emit `100,60,100` (`seq_func.result:2-11`; the wrap itself is explicit at `seqfunc.go:392-405` — descending re-enters at `maxv`, ascending at `minv`). After issuing 100,60 the descending floor forbids the required wrapped 100; statement rollback across a wrap is unspecified; and the existing committed-route CYCLE tests never exercise the new local route. | Accepted. Obligations become **pass-scoped**: the floor records issued values of the current CYCLE pass only; every wrap — reached by natural advance or by rewind repair — retires the current pass's obligation and seeds a fresh pass with the wrapped start-side value (a deliberate I4 boundary; retired-pass values are re-issuable from the wrap onward, non-reuse obligations never span passes). The pre-commit stamp writes the final pass's `(floor, is_called=true)`; numeric monotonicity across passes is rejected as meaningless (a wrapped pass floor can be behind a rewound workspace value). Deterministic ascending/descending wrap and rewind oracles added, preserving the pinned streams on both routes. | §2 I4, §4 D, §5, §6, §8, §9, §10 |
| J2 | v6 resets the floor to the repositioned state on a successful SETVAL and stamps every floored generation with `is_called=true`. After a rewound first allocation (floor=10 while statement rollback restored `is_called=false`), `SETVAL('s',60,true)` succeeds; base stores `is_called=false` for an uncalled sequence (`seqfunc.go:524-527,595-600`) and the next NEXTVAL must return 60 (`seq_func.result:49-58`). Resetting the floor to 60 makes enforcement skip to 63; committing first stamps 60 called and also produces 63 — yet no NEXTVAL issued 60 after that deliberate boundary. | Accepted. A successful SETVAL/restart **invalidates** the identity's outstanding obligation — the entry is cleared, *not* reseeded to the repositioned state — because a reposition carries no issued values and the I4 boundary already permits deliberate re-issue of pre-boundary values. Obligations therefore contain only values issued after the latest successful boundary, and stamps exist only for such obligations: a repositioned-uncalled state is neither advanced by enforcement nor stamped-called at commit. Immediate-NEXTVAL and COMMIT-before-NEXTVAL controls planned after a rewound first allocation. | §4 D, §5, §8, §9, §10 |
| J3 | v6 stamps every floored local generation at commit, but `BEGIN; CREATE DATABASE dst CLONE src;` allocate from `dst.s`; `DROP DATABASE dst; COMMIT` is a supported path: DROP DATABASE is admitted inside an explicit transaction (`stmt_kind.go:235-236`) and requires no statement-end commit (`:166-170`), deleting sequence relations through `engine.go:803-808` → `txn_database.go:185-189`. The retained floor then targets a vanished relation at COMMIT; the stamp fails and aborts an otherwise valid transaction; replacement (drop+recreate) identity handling is unspecified. | Accepted. Obligations are keyed by **relation id**, and the commit loop resolves each obligated id through the committing transaction's *own final catalog view*: a generation deleted by this transaction is **retired unstamped** (its obligation dies with it — no continuation exists that could re-issue); a deleting statement that was rolled back leaves the generation present, so the obligation is **preserved and stamped**; a drop+recreate replacement gets a new relation id, so an old obligation can never be applied to it (retired for the old id; the new generation builds its own). Clone/allocate/drop/commit, failed-drop, and replacement oracles planned. | §4 D, §5, §6, §8, §9, §10 |
| I1 | v7's stamp writes `(floor, is_called=true)` unconditionally (modulo a no-op elision), while transaction-local merge reconciliation advances the same workspace counter *outside* the obligation (§4 B step 3; §4 D item 7): explicit-transaction MERGE reuses the caller's BEGIN transaction (`stmt_kind.go:337-341`, admitted via `OPTION_BEGIN`) and applies branch keys explicitly (`data_branch_output.go:3083-3090`, staged `INSERT (cols) SELECT` that never invokes the default). Explicit-txn clone + NEXTVAL (issues 2, floor=2) + MERGE (imports keys 2,3; reconciles the workspace counter to 3) + COMMIT with no further allocation stamps the counter back to `(2,true)`; the merged key-3 row survives and a fresh session's default INSERT regenerates 3 — duplicate key; descending is symmetric. The v7 local-merge oracle (§9 L1 block) allocates after the merge, raising the floor and masking the conflict. | Accepted. The stamp becomes **preserve-or-restore**: restore `(floor,true)` only when the generation's surviving commit-time position is *unsafe* for the final pass (ascending: below the floor, or equal with `is_called=false`; descending: above the floor, or equal uncalled — i.e. the rewind cases), and preserve any surviving position at or beyond the obligation unchanged (natural no-op, surviving reconcile advance, or an older-pass rewind — resolved by explicit pass analysis: an ascending beyond-floor rewind re-enters issued values only through a deliberate wrap, so it is safely preserved; a descending beyond-floor rewind would walk through them, so it is restored). A rolled-back merge rewinds its rows and its reconcile update in the same statement, so restoration is then exact. New allocate→merge→COMMIT oracles with no intervening allocation (ascending + descending) assert the preserved counter and a successful fresh-session default INSERT, with a failed-merge control separating surviving watermarks from rolled-back reconciliation. | §4 B/D, §5, §6, §8, §9, §10 |

Note on the R1→M1 design tension: round 1 asked to "preserve transactional
reconciliation"; N2 asked for usable post-merge DML before COMMIT; M1 forbids
caller-rollback-reclaimable returned values. All three are satisfiable together
only if reconciliation is *not* executed inside the caller's merge transaction:
the merges's sequence writes run as one autonomous transaction (§4 B), while the
merge statement's success remains sequenced after reconciliation succeeds. The
issue-level contract — after a successful merge the source sequence is beyond the
merged keys, and an aborted merge never leaves a path to a duplicate — is
preserved in full. L1 (v5) refines the M1 half of this resolution without
reopening the tension: "always autonomous" becomes "autonomous for committed
generations"; a transaction-local generation is unreachable by any autonomous
transaction and is safe in-caller under the K1 floor/stamp protocol (§4 D), while M1's reclaim
scenario (`BEGIN; MERGE; nextval; ROLLBACK`) runs against a *committed* sequence
and therefore keeps the autonomous, never-reclaim path. The routing key is
fixed per call; no registry or caller-mode fallback returns (M3).

## 1. Problem and evidence

A sequence in MatrixOne is a catalog-owned relation with one row
`(last_seq_num, min_value, max_value, start_value, increment_value, cycle, is_called)`.
Storage convention (verified at `seqfunc.go:270-330`, `setIsCalled` `:427-442`):
next issued value = `last_seq_num` when `is_called=false`, else `last+increment`;
increment sign defines direction. The product contract: **every issued value comes
from the intended sequence state, exactly once per allocation generation, regardless
of the caller's current database, structural copy operations, sequence
reconfiguration, or the calling transaction's outcome.**

Issue #30359's checklist names four violations, all reproduced on base
(`repro-A.out`, `repro-A3.out`, `repro-B.out`, `repro-CD.out`):

- **A (#30360):** one-arg stored DEFAULT + session-db fallback (`seqfunc.go:213-215`);
  owner pin only in view expansion (`base_binder.go:4671-4692`); `getDefaultExpr`
  restores without owner context (`build_util.go:2590,2610`). Base: `ERROR 1146`;
  silent caller-sequence consumption.
- **B (#30359):** merge apply materializes keys, no reconciliation
  (`data_branch_output.go:3083-3090`); source sequence stale → `ERROR 1062`.
- **C (#30679):** START WITH folded only via altering-session curval
  (`ddl.go:6094-6120`); `start_value` discarded by `Nextval`; `rel_createsql`
  overwritten with ALTER text (`build_ddl.go:2381-2384`). Base: fresh sessions issue
  101/102 after `START WITH 500`; `rel_createsql='alter sequence seqw.s start with 500'`.
- **D (#30896):** counter read-modify-write runs in the caller's transaction
  (`seqfunc.go:75-77`); ROLLBACK restores `last_seq_num` and reissues values;
  SETVAL shares it. `issue_28246.result` pins this defect as expected behavior.

## 2. Invariants and non-goals

- **I1 (owner resolution):** unqualified sequence references in a table DEFAULT
  resolve in the table-owner database at every evaluation, in every session.
- **I2 (merge reconciliation):** after a *successful* table merge, for every
  reconcilable column (§4 B) backed by sequence `S`, the destination database's `S`
  is strictly ahead of every merged value for that column (ascending
  `last>=w, is_called=true`; descending symmetric): the next ordinary default insert
  cannot collide with a merged row. Reconciliation for one merge is applied
  atomically per visibility partition and is idempotent under retry: all
  affected *committed* generations advance in one autonomous unit (§4 B) that
  acquires its locks in the §4 D global canonical order; affected
  *transaction-local* generations (§4 D — e.g. an in-txn CLONE destination)
  advance inside the caller (merge) transaction, sharing commit/rollback fate
  with the merged rows. An aborted/rolled-back merge reverts its rows but may
  consume counter ranges on committed generations — gaps, never reuse, never a
  collision path; ranges consumed on transaction-local generations vanish with
  the caller's rollback, leaving no committed trace.
- **I3 (restart visibility):** after `ALTER SEQUENCE ... START WITH n`, the next
  issued value reflects the effective restart in *every* session per §4 C; a
  consumed range is never re-entered; the final effective definition is validated
  like a CREATE before persistence; `rel_createsql` always holds a valid,
  replayable `CREATE SEQUENCE`.
- **I4 (issue-once, scoped):** within one allocation generation, a value returned
  to any session is never returned again as a consequence of caller `ROLLBACK`,
  statement retry, cancellation, crash, CN restart/failover, or merge
  rollback — **no exceptions for explicit transactions or merge flows**. Gaps are
  permitted (PostgreSQL semantics). A value issued from a **transaction-local
  generation** (created in the caller's uncommitted transaction — on this base
  only via an explicit-txn CLONE or snapshot restore, §4 D) is protected by the K1
  allocation/publication protocol: a per-(txn, identity) session floor records
  every issued value and a pre-commit advance stamp persists a counter strictly
  past it, because a nonfatal statement error rolls back only the statement's
  workspace writes (`txn.go:1164-1199`, `disttae/types.go:1174-1194`;
  default classification `util.go:2277-2286`) and can leave the generation
  alive while issued values — possibly already streamed to the client
  (`resp_client.go:234-249`) — would otherwise be re-issuable. Whole-txn
  ROLLBACK still erases the generation itself (no partial undo below the
  statement level exists: SAVEPOINT is unimplemented,
  `mysql_cmd_executor.go:5703`). **Generation boundaries** — `CREATE SEQUENCE`,
  successful `SETVAL`, successful `ALTER` restart per §4 C, and each documented
  `CYCLE` wrap (`seq_func.result:2-11` pin `100,60,100` retained; under the K1
  protocol every wrap retires the current pass's obligation and opens a fresh
  pass, §4 D) — deliberately reposition or re-enter and may re-issue values.
  **Rollout phasing (N4):** (a) per-CN rollback-reclamation
  protection from upgrade onward; (b) catalog-writer resurrection protection once
  every CN runs this fix; sequence DDL fenced operationally in between (§8).

Non-goals (unchanged): no range caching / allocator redesign; no new SQL verbs; no
`CREATE SEQUENCE` semantics change; no cross-account/subscription change; no
`CURRVAL`/`LASTVAL` locality change beyond this document.

## 3. Root cause and first owners

| Defect | First owner | File:line (base 4769417cdc) |
|---|---|---|
| A caller-db resolution | default restoration lacks owner context | `build_util.go:2590,2610`; `seqfunc.go:211-215`; precedent `base_binder.go:4671-4692` |
| B stale destination sequence | merge apply materializes keys, no reconciliation | `data_branch_output.go:384, 1920, 2757/2813, 3030, 3083-3099, 2556-2564, 207/244` |
| C START WITH folded per-session + metadata loss | ALTER row construction + plan CreateSQL | `ddl.go:5401, 6053-6132, 5682-5693 (valueCheckOut 6136-6141)`; `build_ddl.go:2299, 2381-2384` |
| D transactional advancement + uncoordinated ALTER | counter SQL joins caller txn; ALTER reads state unlocked | `seqfunc.go:75-77`; `ddl.go:5401, ~5414-5428, 6677-6701`; `compile2.go:319-352`; `lock_meta.go:188-201` |

## 4. Design

Precedents carried: PostgreSQL (rollback non-reclamation; generation boundaries;
session `currval`), incrservice (non-caller-txn allocation), #27581 (trailing-db
pin), #30371 (cross-session preserve), #28246 (introspection expectations, §8).

### A — owner resolution (A2, unchanged since v1)

Evaluation-time pin at `getDefaultExpr`/`getDefaultExprForAssignment`
(`build_util.go:2590/2610`) using `plan.TableDef.DbName`, appending the internal
trailing db argument to unqualified `nextval`/`currval`/`setval`
(mirror of `base_binder.go:4671-4692` incl. SETVAL arity normalization). Covers old
catalogs and all consumers. A1/A3 remain rejected.

### B — merge reconciliation (B1, placement v4 per M1; visibility partition + global lock order v5 per L1/L2)

**Reconcilable shape (unchanged).** A destination column is reconcilable iff its
restored default is a single `nextval` with constant arguments (planner-inserted
type-preserving assignment cast allowed); transformed/multiple/dynamic references
are excluded (documented residual limitation).

**Watermark capture (unchanged).** Merge-scope accumulator updated while typed rows
are live at `appendDataBranchApplyRowAsSQLValues` (`:1920`), covering staged
inserts, `disableInsertStage` direct applies, and staged-update values; deletes
never advance; multi-flush merges reconcile to the global extreme.

**Reconciliation unit (placement v5 per L1; validate-before-proceed order v6 per K2).**
After all apply batches are flushed and watermarks are final — at `mergeDiffs`
completion, before the merge statement reports success — reconciliation runs,
partitioned by generation visibility (§4 D):
1. Classify each `(db, seq)` watermark identity as *committed* or
   *transaction-local* (a generation that exists only inside the caller's
   current transaction — e.g. an in-txn CLONE destination; invisible to every
   other transaction by workspace scoping, `engine.go:500-512`).
2. *Committed partition:* execute **one autonomous short transaction** (the same
   executor machinery as allocation, §4 D, never the caller/merge txn). The unit
   computes the affected identity set and sorts it by `(account_id, relation_id)`
   ascending, then walks the plan; **for each planned identity, in that order**
   (K2 — validate before proceeding):
   a. Acquire the identity's shared catalog lock (`lock_meta.go:188-201`).
   b. Validate the committed relation id matches the planned id **under that
      lock** (re-read of committed state; a matching id is then stable while the
      lock is held, because drop/recreate needs the exclusive catalog lock,
      which conflicts with the shared one). Any mismatch ⇒ **immediate
      whole-unit abort** — release everything acquired so far, re-plan from
      fresh identities, bounded restart (the §4 D retry matrix governs) — never
      acquire one more lock with a stale plan, never a half-updated set, never
      a lock set mixing generations.
   c. On match, acquire the data-row lock **bound to the validated id** — the
      `SELECT … FOR UPDATE`/UPDATE must target the resolved relation id
      (re-resolution by name at statement-plan time, `sql_executor.go:604-635`,
      is not authoritative for acquisition order; implementation pins the id).
   d. Proceed to the next planned identity only after a–c complete for this one.
   e. Steps a–d replace v5's acquire-full-set-then-revalidate scheme: validation
      is per identity, before any further acquisition, so a rank change after
      planning can never be carried past the identity it affects.
   f. Guards: per direction, NO CYCLE range guard (a watermark whose
      post-transition position cannot issue a next value aborts); any guard
      failure rolls back *this reconcile txn only* — no partial advance
      persists — and fails the MERGE statement (row apply rolls back with the
      merge transaction).
   g. Apply the monotonic conditional UPDATEs (N3 predicate): ascending
      `set last_seq_num=w, is_called=true where last_seq_num < w or (last_seq_num = w and is_called = false)`;
      descending `> w or (= w and is_called = false)`. Equality with
      `is_called=true` matches no row (genuine already-ahead no-op); idempotent
      under merge retry and monotonic under concurrent allocation; a concurrent
      explicit restart landing below `w` is superseded for the occupied merged
      range (I2 wins there).
   h. Commit. Only then does the merge flow proceed to success/return.
3. *Transaction-local partition:* apply the same guards and monotonic
   conditional updates **inside the caller transaction**, before the merge
   statement reports success. These rows are private to the caller, so no
   ordering/interlock with other transactions applies; the updates share the
   caller's commit/rollback fate with the merged rows (whole-txn rollback
   removes rows, counter growth, and possibly the generation itself in one
   revert; commit persists them atomically). When the same transaction also
   holds a §4 D allocation obligation for the identity, the pre-commit stamp
   must preserve this surviving advance rather than stamping back to the
   allocation floor (I1; §4 D item 5c case iii) — otherwise a fresh session
   would regenerate a value already present as a merged key. A statement-level
   rewind of the merge statement itself removes its rows and its reconcile
   stamps together — a merge emits no sequence values, so reconciliation needs
   no K1 floor;
   *allocation* on the same local generation (pre/post-merge `nextval`,
   default inserts) is covered by §4 D K1.
4. Empty/no-op: zero diff rows, zero reconcilable columns, or all watermarks
   not-ahead ⇒ no writes in either partition (and no autonomous txn is opened
   when the committed partition is empty).

**Transaction-ownership consequences (M1/N2; scoped in v5).** The merge
transaction takes *no* sequence locks on **committed** generations: ordinary
post-merge default `INSERT` before COMMIT calls the autonomous allocator, which
finds the rows unlocked and proceeds — the supported `BEGIN; DATA BRANCH MERGE
…; INSERT … DEFAULT nextval(…)` sequence works. If the merge transaction later
aborts or the user rolls back an explicit `BEGIN`, the merged rows disappear
while the reconcile unit already committed: the counter keeps the consumed
range — a **gap, never reuse** (I4; PG-consistent), and no duplicate-key path
exists because the rows that held those values cannot coexist with a later
allocation of them. Merge-statement retry re-runs reconciliation with the same
watermarks; the conditional updates make that a no-op. For **transaction-local**
generations the merge work, reconciliation, and default inserts execute inside
the caller transaction, and §4 D routes the allocation in-caller — pre-COMMIT
use keeps working, post-ROLLBACK nothing survives to collide. The verified SQL
boundary on this base (I5, §8): the transaction-local clone and its default
inserts are supported, but a merge whose destination *database* exists only in
that transaction is rejected atomically with `ERROR 20101` before any row
apply (`data_branch.go:3343` created-time probe) — so a surviving
transaction-local reconcile advance is reachable only through the reconcile
unit's in-caller partition (§9 oracles: UT authoritative; committed-route
equivalents cover the protocol end to end).

### C — ALTER semantics and metadata (C1, validation formalized in v4 per M2)

**Scope split (N5, unchanged).** The uniform restart rule applies only to
statements carrying a `START WITH` clause; statements without one keep base
behavior byte-identically: same-session curval pre-advance
(`ddl.go:6110-6116`; pins `alter_sequence_state_test.go:82-90/~134`,
`alter_sequence_1.result` `8,18,28`) and cross-session preserve (#30371).

**Uniform restart rule for START WITH (direction-aware, unchanged).** Compute the
effective post-ALTER definition first (INCREMENT/MIN/MAX/CYCLE applied). With
`d` = sign of effective increment and `f = preLastSeqNum`: restart iff strictly
ahead (`n > f` ascending / `n < f` descending, covering both `is_called` states),
persisting `(last=n, is_called=false, start_value=n)` so every session's next
`NEXTVAL` returns exactly `n`; otherwise keep position and record the
direction-aware boundary as start_value (clamped-behind starts stay accepted,
never re-enter consumed ranges; for a never-issued sequence the consumed range
is empty, so the declared start itself is the recorded boundary — this keeps
wholesale-redefinition ALTERs on fresh sequences, e.g. the
`clone_database_sequence.sql` flip/rebase case, legal while issued sequences
still clamp to the frontier). Combined clauses take direction from the new
increment.

**Uncalled (never-issued) START WITH — one authoritative rule (E1, v17).**
The uniform rule above governs this arm unchanged: for a never-issued
sequence `f` is its unissued pending value (the stored start candidate).
(i) `n` strictly ahead of `f` restarts to exactly `n` — persisted state
reads `(n, 0)` and EVERY session's next `NEXTVAL` returns `n`.
(ii) `n` equal to `f` leaves the state untouched.
(iii) `n` behind `f` KEEPS the pending value: the state stays `(f, 0)` and
every session's next `NEXTVAL` returns `f` — e.g. `CREATE SEQUENCE s …
START WITH 10` never issued followed by `ALTER SEQUENCE s START WITH 5`
yields next value **10** (fresh session included); the descending mirror
(`INCREMENT BY -1 START WITH 10` never issued): `START WITH 15` is behind
⇒ next value 10, `START WITH 5` is ahead ⇒ restart, next value 5. On this
clamped-behind uncalled arm the recorded `start_value` boundary is the
declared start `n` (the consumed range is empty), which keeps
wholesale-redefinition replay (the `clone_database_sequence.sql`
flip/rebase case) legal; on a called clamp the recorded boundary is the
frontier `f`. **Only exception:** a never-issued pending value that lies
OUTSIDE the effective final bounds (ascending `f >` new `MAXVALUE`,
descending `f <` new `MINVALUE`) can never be emitted under the final
definition, so the ALTER restarts it at the declared start `n` — for
CYCLE and NO CYCLE alike — and `n` itself is validated atomically against
the final bounds (out of bounds ⇒ `STARTVALUE (n) … must between MINVALUE
(m) and MAXVALUE (M)`, the statement rejects, and the stored row is
unchanged). This rule is pinned by the 12-case matrix
{asc,desc}×{called,uncalled}×{ahead,equal,behind}
(`alter_sequence_state_test.go:199-238`) and the uncalled out-of-bounds
restart tests (`:356` and `:562`); §8 and the public projection track this
text, and the BVT gains behind-start uncalled pins in both directions
with fresh-session assertions (§9).

**Final-state validation (M2).** The persisted definition must pass the existing
CREATE-equivalence validation on its *final effective* values: after computing
`(lastNum, incrNum, minValue, maxValue, startNum, cycle, isCalled)` — including the
restart/clamp decision and all combined clauses — `valueCheckOut(maxV, minV, startN)`
(`ddl.go:5682-5693`, `:6136-6141`) runs on those final values before the row is
built. Any conflict (e.g. ascending last=100, `ALTER … START WITH 50 MAXVALUE 60`
clamps startN to 100 > 60 ⇒ invalid) rejects the statement and, because the ALTER
executes as one transaction, rolls back the drop/recreate with it — atomic, nothing
persisted. Descending symmetric (`startN < new min` rejects). This documents the
existing check placement as contractual for the new restart paths.

**Catalog metadata (unchanged):** `rel_createsql` = synthesized canonical
`CREATE SEQUENCE` of the validated effective definition (`build_ddl.go:2381-2384`),
so a successful ALTER always replays.

C2 (new verb) remains rejected.

### D — issue-once allocation and lock protocol (D1; routing v5 per L1, publication v8 per K1/J1–J3/I1, order v6 per L2/K2)

**Routing by generation visibility (L1).** Every `NEXTVAL`/`SETVAL` first resolves
the sequence *identity* (relation id + generation) in the caller context, then
classifies it:

- **Committed** — the identity is visible in committed catalog state (probed
  through the same engine path an autonomous transaction would use — the latest
  catalog cache / committed fall-through of `engine.go:520-540`, bypassing the
  caller workspace): allocation is **autonomous** with the v4 machinery
  unchanged — one autonomous short transaction per attempt (`ExecTxn` without
  `WithTxn`, `sql_executor.go:206-229,337-354`): open txn with a 5 s lock-wait
  budget capped by the statement deadline (`applyExecutorLockWaitTimeout`,
  `:868-880`) → `SELECT … FOR UPDATE` + identity revalidation (relation id +
  `requireSequence`; stale ⇒ abort + bounded retry with re-resolution) →
  compute + UPDATE → commit → publish to session state only on clean commit
  (single publish point). Retry bounds (calibrated at v12 per I6):
  wrapper-owned, 8 attempts, deadline `min(statement, 10 s)`, backoff
  25/50/100 ms per step (steps beyond the third stay at 100 ms) + jitter;
  non-retryable errors return immediately. Tier-1 exhaustion is handled by a
  bounded two-tier machine (v13, I7; admission and deadline propagation made
  authoritative in v14, H1/H2; publication/retry arbitration and the lazy
  retry-window lifecycle corrected in v15, G1/G2): on exhaustion of the
  transient-conflict class (and only that class), tier 2 retries the
  statement at statement level only when the two-phase admission predicate
  below holds and only while the retry-window deadline D has not elapsed.

  **Tier-2 admission predicate (v14, H1; evaluation made two-phase in v15,
  G1; publication term flag-only in v16, F1).** Conversion requires ALL of
  the following terms, and the predicate
  is evaluated in two phases (the arbitration phase is authoritative; see
  the G1 block): a cheap pre-filter at the conversion site
  (`seqfunc_autonomous.go` — the enforcement owner), then authoritative
  arbitration at the retry quiescence boundary. (1) *envelope* — the
  statement was dispatched with `{Autocommit=true, ByBegin=false}` in its
  transaction envelope
  (`frontend/txn.go:812` → `txn/client/operator.go:225-229`; a CN-side read
  path ships with the implementation), i.e. session autocommit=1 and no
  explicit BEGIN; envelopes lacking these facts (background/internal SQL)
  are default-deny; (2) *isolation* — the statement runs under RC (tier-1
  conflicts are RC-mode; snapshot-isolation sessions keep their existing
  conflict policy); (3) *generic acceptance* — the compiler's `canRetry`
  (`compile.go:793-797`: not `disableRetry`, not `IncrStatementDisabled`)
  would accept the converted error; tier 2 layers this predicate on top and
  changes nothing for other errors; (4) *publication* — the statement's
  synchronized publication flag is unset, i.e. no result batch has entered
  the client write path for this statement (`resp_client.go:241-249` is the
  single publication point; the flag is set before the first batch's bytes
  are written, and BOTH admission phases read ONLY this flag — no other
  publication accounting, in particular not the unsynchronized
  `SessionInfo.ResultRows` counter, is ever consulted): DML shapes (e.g.
  `INSERT` with nextval
  defaults) publish only the final OK packet — written after statement
  completion, never from the pipeline — and are retry-safe, while `SELECT`
  shapes are admitted only before their first batch reaches the client.
  Everything else — explicit `BEGIN` / multi-statement transactions,
  `autocommit=0`, partially streamed result sets — receives the terminal
  named exhaustion error, unconverted.

  **Retry/publication arbitration (v15, G1; flag-only reads in v16, F1).**
  A sequence-bearing AP
  statement remains parallel within its initiating CN
  (`compile.go:1476-1486`) and its producer scopes run concurrently
  (`scope.go:606-628`), so a batch can reach the client AFTER the
  pre-filter observed an unpublished state but BEFORE the compiler selects
  retry (`compile2.go:708-709`). Admission is therefore finalized only at
  the retry quiescence boundary: in the `Compile.Run` retry branch,
  immediately after `prepareRetryTransition` (`compile2.go:752`) — whose
  `cancelAndWaitRunningSQL` (`:916-918`) cancels and joins every running
  producer scope — and before the retry generation is rebuilt
  (`buildRetryCompile`, `:801-805`). The producer join is the
  happens-before edge that makes the arbitration read race-free; the
  publication signal itself is a synchronized (atomic) per-statement flag
  owned with the compiler's sequence statement state, set by the single
  publication path before any client bytes — the existing
  `SessionInfo.ResultRows` accounting (`process/types.go:950-962`) remains
  unsynchronized ordinary statistics and is NEVER consulted by either
  admission phase; v16 removes the v15 pre-filter hint entirely because an
  unsynchronized read of that counter would race with concurrent
  `AddResultRows` writes (`resp_client.go:241-242`) even while the flag
  itself is properly synchronized. If
  arbitration finds publication won the race (flag set), the retry is
  declined and the ORIGINAL named exhaustion error carried by the
  converted signal is returned to the client exactly once — publication
  always wins over retry, and the already-delivered prefix is never
  replayed or duplicated. Arbitration re-verifies the full predicate
  (envelope facts are frozen at dispatch; isolation and generic acceptance
  are static; publication and the budget term are the live ones) and its
  verdict is final for that exhaustion event; it applies only to the
  tier-2 converted signal, so the compiler's generic retry policy for
  other errors is unchanged. Deterministic validation: a plan-hook
  barrier case publishes one batch after the pre-filter's zero
  observation and before the retry transition, asserting retry is
  declined, the client receives exactly the published prefix plus the
  terminal error, and no batch is replayed or duplicated; the case MUST
  execute race-clean under `-race` (zero warnings), which also proves the
  pre-filter reads no unsynchronized publication state (F1).

  **Budget lifecycle and deadline ownership (v21, D1 — progress-renewed;
  contention-set lifecycle, cancellation routing, availability
  discriminant and probe-cost accounting corrected per A1–A4; episode
  closure and per-identity terminal eligibility corrected per Z1–Z2;
  supersedes the v20/v19 D1 text and the v15 fixed-wall-clock D).** The
  retry window D (`min(session statement deadline, now + 10 s)`) bounds the
  ADMITTED conflict-replay envelope only — it is created LAZILY at the
  statement's first tier-2 admission (the first arbitrated conversion of a
  tier-1 transient-conflict exhaustion), never at first allocation entry.
  Before any admission, sequence allocation keeps the base contract: each
  `execSeqAutonomous` invocation opens its own per-invocation
  `min(statement, 10 s)` budget (`seqfunc_autonomous.go:185-189` semantics
  unchanged), so a statement that allocates, then processes or streams
  longer than the window WITHOUT any conflict, and allocates again,
  observes no new limit — its next allocation opens a fresh per-invocation
  budget. Once admitted, the first admission performs one bounded
  committed-tuple probe of the contended sequence row to create the retained
  watch-set entry (A1) of that `(relation id, generation)` with the
  initialized baseline sample (B1), and D is stored in
  the compiler's `sequenceStatementState`
  (`pkg/sql/compile/sequence_state.go:28`), preserved across
  `prepareRetryTransition` (`compile2.go:913`), and — conformance
  correction C1 — carried across the retry rebuild itself
  (`buildRetryCompile` re-installs the same `SeqRetry` instance on the new
  `Compile`; `compile.go:157` creates fresh state only for a genuinely new
  statement). Later admissions of the same statement reuse the same D
  WHILE THAT EPISODE IS LIVE (its watch set retains unresolved
  identities); the episode CLOSES — D is retired and removed from
  `sequenceStatementState` — exactly when the watch set becomes empty
  (Z1 below), and a conflict after closure is a FRESH first admission.
  A fresh statement execution has no D until its own first admission.
  Every tier-1 execution while an admission episode is LIVE, in any
  retry generation of that statement, derives its attempt context from D
  (remaining = D − now): an execution entered at D−ε has only ε of attempt
  budget, and at or after D no new attempt loop opens — the decision
  between renewed continuation, episode closure and terminal is taken
  by the probe-first expiry rule below (B2/Z1), never by the wall clock
  alone. Once the episode has CLOSED (empty watch set, Z1) the statement
  again runs under the base per-invocation `min(statement, 10 s)` budget
  exactly like a never-admitted statement, so a closed-episode
  wall-clock crossing can never strand a later allocation with a
  nonpositive budget.

  **Progress marker (B1, v19).** The authoritative progress marker of one
  contended sequence is the row's COMPLETE committed tuple
  `m = (relation id, generation, is_called, last_seq_num)`, read through
  the same bounded committed-visibility path as the routing probe above
  (an autonomous RC snapshot read — it never waits on the sequence row
  lock). The statement records one baseline sample per
  `(relation id, generation)` in its `SequenceRetryState`, initialised at
  the entry's CREATION — first admission by the admission probe, later
  conversions by the conversion path's own bounded sample (Z2); the
  initialization-failure and terminal-eligibility rules are in the A1
  block below. Each later probe classifies against the baseline:

  - *generation change* (the identity re-resolves to a different
    generation per the routing rule, or no longer resolves) ⇒
    **reconfiguration**: re-baseline to the freshly resolved tuple and
    count the reconfiguration itself as the observed change; an
    unresolvable identity is a probe failure.
  - *tuple identical to the baseline* ⇒ **no-progress**. This class
    deliberately includes the complete-cycle ABA case: the probe observes
    committed contents only, so a CYCLE sequence whose committed
    emissions between two consecutive probes total an exact multiple of
    one full pass (for `MINVALUE 1 MAXVALUE 2`, peers committing 1 then
    2, or 2 then 1, between every probe) returns an identical tuple
    although allocation continued. No marker derivable from the row's
    committed contents distinguishes that shape from stasis, so the
    design classifies it as no-progress (accepted quantization limit):
    the sanctioned outcome is the named no-progress terminal, whose
    surface semantics equal the base conflict-exhaustion contract, and
    the §6 zero-new-errors promise is scoped to shapes whose contended
    rows observably change. Both arms are pinned deterministically —
    identical samples after exactly one full wrap, and a genuinely
    unchanged row.
  - *tuple changed* ⇒ **progress**: D renews. The change is labelled for
    diagnostics as *direction-consistent emission* — reachable from the
    baseline by n ≥ 1 direction-consistent emissions under the
    sequence's `(increment, minvalue, maxvalue, cycle)` (strict advance
    within one pass, or the boundary-consistent wrap signature: baseline
    in the terminal region of one pass, sample in the restart region of
    the next) — or as *reconfiguration* (everything else: SETVAL jumps
    in either direction, ALTER-bound changes, drop+create of the same
    name). A simple observed backward transition is NOT accepted as wrap
    evidence: it classifies as reconfiguration. Both labels renew the
    window exactly once per observation: every renewal requires a NEW
    observable change strictly after the previous baseline, so a row that
    stalls immediately after a SETVAL/ALTER is terminal one interval
    later. Renewing on reconfiguration is deliberate: SETVAL is the
    documented in-field remediation for a stalled sequence (the #30359
    reproducer repaired a merge-stalled sequence with exactly it), one
    fresh interval lets remediation take effect, and no invariant
    (uniqueness, monotonicity, issue-once, no-reuse) depends on renewal
    timing.

  **Retained contention set (A1, v20; episode closure and creation-time
  initialization specified per Z1–Z2, v21).** The statement's retry
  state carries a retained CONTENTION SET (the watch set): one entry
  per contended sequence identity, keying the baseline and
  last-observed committed tuples above. Its lifecycle is independent of
  the consumable pending payloads (B3): an entry is created the FIRST
  time a conversion contends that identity, and (Z2) EVERY entry is
  created CARRYING its baseline sample — at first admission by the
  admission probe above; at a later conversion on a new identity by the
  conversion path's own bounded committed-tuple sample (the same
  lock-free RC snapshot read, derived context `min(statement, 1 s)`),
  taken BEFORE the entry is recorded — and a repeated conversion
  on the same identity appends only to that identity's pending payload
  (B3), leaving one watch entry with a stable first-recorded order
  across parallel conversions. If that creation sample fails or times
  out, the entry is recorded YOUNG (no baseline; its creation
  timestamp is kept) and its initialization is retried FIRST inside
  the next expiry decision's single probe round. The watch set is NOT
  consumed at arbitration (pending payloads are), survives the retry
  rebuild (C1
  instance carry) and generation replacement keyed by relation id
  (generation moves follow the B1 marker rule), and an entry is REMOVED
  exactly when its contention observably ends for this statement: the
  statement's own committed allocation on the identity (its ordinary
  attempt succeeds — contention served), the A3 availability resolution
  below (the row is acquirable again), or retirement (the identity no
  longer resolves — the statement's own next allocation surfaces the
  deterministic not-found error, so the removal cannot mask a usable
  sequence). Every probe-first expiry decision probes the CURRENT watch
  set — never the consumed pending set — so a post-rebuild entry
  re-checks exactly the identities that have contended and not yet
  resolved, regardless of how arbitration consumed payloads or how
  long ago they were converted. Episode closure (Z1): D is RETIRED and
  dropped from `sequenceStatementState` exactly when the watch set
  becomes empty — every recorded contention was resolved by this
  statement, so the conflict-replay envelope it funded no longer
  exists; the statement returns to the base per-invocation
  `min(statement, 10 s)` budget, and a later conflict is a fresh first
  admission opening a fresh D with a fresh baselined entry. Renewal can
  never substitute for retirement here: renewal requires an observable
  change and an empty set offers nothing to observe, while keeping the
  dead deadline would strand the statement's next allocation against a
  wall clock it already crossed (`seqfunc_autonomous.go:213-223`
  surfaces the named terminal at/after D before any attempt loop
  opens) — the exact v20 defect this correction closes. Per-identity
  terminal eligibility (Z2): each entry ages from its own baseline (or
  from its creation while un-baselined); an identity is terminal-
  eligible only after one full renewal interval has passed under
  observation. A young identity neither votes terminal (its first
  comparison cannot be a full no-progress interval) nor renews
  (initialization and youth are not observable change): a D−ε join
  whose first decision finds the tuple unchanged rides the renewal
  decided by the eligible identities, and with NO terminal-eligible
  identity in the set the decision renews once so each young identity
  receives its first full interval under observation; an entry still
  un-baselined one full renewal interval after its creation fails safe
  to the exactly-once terminal (its unavailability has persisted a full
  interval). It can never terminal on an empty set: the exactly-once
  terminal requires a retained, unresolved, terminal-eligible identity
  committed-unchanged AND held. Deterministic coverage: different
  sequences exhausting in SUCCESSIVE generations (generation 1 exhausts
  on A, generation 2 on B — the generation-2 decision probes {A, B}
  conjunctively); watch entries surviving arbitration consumption
  across a rebuild; successful-allocation removal followed by
  re-contention (fresh admission, fresh D, fresh baseline); Z1's
  closed-episode crossing (admitted ⇒ successful allocation ⇒ empty
  set ⇒ process past the old D ⇒ next allocation succeeds on a fresh
  per-invocation budget; caller cancellation after closure propagates
  unchanged); Z2's late-join matrix (B joins at D−ε with its creation
  baseline; first decision unchanged ⇒ renewal, never terminal; a
  static, held B terminals exactly once one full interval after ITS
  baseline; B's initialization while a terminal-eligible A is
  stalled-and-held neither delays nor prevents A's exactly-once
  terminal); and the initialization-failure arm (a failed creation
  sample retries inside the next decision's round; a never-baselined
  entry fails safe after one full interval from creation).

  **Probe-first expiry ordering (B2, v19) with the availability
  discriminant (A3, v20), episode closure (Z1, v21) and per-identity
  terminal eligibility (Z2, v21).** D-expiry is never asserted by a
  wall-clock comparison alone. Every expiry decision — the arbitration seam
  (`sequence_state.go:101-103`), the zero-attempt entry surface
  (`seqfunc_autonomous.go:212-217`), the conversion pre-filter
  (`:339-341`), and the in-flight budget-expiry route (A2) — executes
  one shared decision procedure in this order: (1) caller-context
  cancellation propagates the caller's own error unchanged (first,
  existing precedence); (2) `now < D` ⇒ proceed, no probe; (3) empty
  watch set ⇒ retire D (Z1: all recorded contention has resolved) and
  proceed attempt-first under an ordinary per-invocation budget — by
  construction an empty set cannot coexist with a live D, so this step
  is the defensive residual of episode closure, never a terminal path;
  (4) ONE bounded probe round over the watch set — YOUNG entries
  (failed creation samples, Z2) are re-sampled first inside this same
  round, and every entry whose committed tuple changed since its
  last-observed sample (B1 classification) is renew-eligible and
  re-baselined; (5) for each still-unchanged TERMINAL-ELIGIBLE identity
  (Z2), in first-recorded order, ONE bounded
  lock-acquisition check of the sequence row inside the same probe
  round (acquire-and-release; the probe transaction rolls back and
  consumes no values): an acquired identity is RESOLVED (contention
  gone) and removed from the watch set; a failed or timed-out tuple
  probe of any identity, a lock-wait timeout/conflict, or any other
  lock-check error means that identity persists as committed-unchanged
  AND unavailable ⇒ D has expired: the named exhaustion error surfaces
  exactly once (first-recorded order selects the surfaced identity),
  further conversion is forbidden for that statement, and retryable
  classes under the expired D lose their retryability (unchanged
  precedence); (6) renewal: with no unchanged-and-unavailable
  TERMINAL-ELIGIBLE identity remaining — every eligible watch identity
  either changed or resolved, with young identities (Z2) riding without
  a vote — D renews to
  `min(session statement deadline, now + 10 s)` and the
  statement proceeds (the entry case derives its attempt context from
  the renewed D; the conversion and arbitration cases continue their
  ordinary paths). If instead the round removed the LAST watch entry
  (all-resolved), the episode closes: D is retired (Z1) and the
  statement proceeds attempt-first under an ordinary per-invocation
  budget rather than renewing a deadline with nothing left to
  observe. The discriminant separates persistent
  contention from an available, unchanged row: a drained herd leaves
  the row acquirable, so its remaining admitted statements resolve and
  complete (the successful-drain contract), while a peer holding the
  row uncommitted keeps its tuple committed-unchanged AND the row
  unavailable, yielding exactly one no-progress terminal after one
  full interval. A terminal verdict therefore requires BOTH
  observations in one round against a TERMINAL-ELIGIBLE identity (Z2 —
  one full renewal interval old from its own baseline, or from its
  creation while never baselined): no committed change since its
  baseline, AND the row not acquirable within the probe's bounded wait;
  initialization and youth vote neither terminal nor renewal. Probe
  budget: one dedicated derived context, `min(statement deadline, 1 s)`,
  covering the tuple reads and the lock checks of ONE decision; the
  snapshot reads never take the sequence row lock, and the lock checks
  use the ordinary bounded lock wait, so the round is bounded by
  construction. Ordering consequences (a)–(d) stand as v19 — (a)
  progress-before-D in-flight renewal, now reached also through the A2
  route; (b) rebuilds crossing D meet the next entry decision probing
  the retained watch set (A1 — its contents no longer depend on
  consumed payloads); (c) the arbitration probe executes after producer
  quiescence (G1's happens-before edge), reading committed state only;
  (d) caller cancellation preempts everywhere — plus one new
  deterministically pinned arm: producer-stop with a rebuild crossing D
  (the baseline sampled the final peer commit; the row is unchanged but
  acquirable) resolves and completes instead of terminalling. Probe
  accounting (A4/Z2): each expiry decision costs ONE bounded round —
  tuple probes of every watch identity, including at most one
  re-initialization sample per YOUNG identity, plus at most one lock
  check per still-unchanged terminal-eligible identity — and
  admission/conversion adds one creation-time baseline sample per newly
  contended identity (Z2); distinct
  decisions of one statement are bounded by the number of renewal
  intervals the statement consumes (each decision renews, resolves,
  closes, or terminates). Cost is therefore expressed per expiry
  decision and per
  renewal interval — never per retry generation: a renewed statement
  can cross several decisions within one generation, and several
  allocation calls of one generation can cross renewal intervals.

  **D-derived cancellation routing (A2, v20).** An attempt cancelled by
  the firing of its D-derived context (the recorded budget-fires-in-
  flight arm, `seqfunc_autonomous.go:242-246`, with the caller's
  context alive) reaches the transient classifier as a plain context
  error, which `retryableSeqTxnErr` (`:107-113`) rejects — so absent
  this correction it surfaces terminally without any progress probe.
  In an admitted statement this budget-expiry class is a conversion
  candidate: the recording site tags the attempt error with its
  cancellation provenance (derived-budget expiry versus caller
  cancellation), the tagged class bypasses the transient classifier for
  the conversion gate ONLY, and the conversion pre-filter routes it
  into the shared probe-first expiry decision above, which renews on
  observable change or resolution — or terminals exactly once. Order
  (1)/(d) precedence is unchanged: caller-context cancellation still
  returns the caller's own error before any classification, and
  unrelated terminal errors (not retryable, not budget-expiry) still
  return immediately (`:248-249` semantics). A derived-budget expiry in
  a PRE-admission statement (no D) retains its unchanged named surface
  — there is nothing to renew. The planned in-flight `Compile.Run`
  case exercises ACTUAL derived-context cancellation — a real deadline
  crossing mid-flight (the fixture shape that returns `ctx.Err()`
  verbatim, `seqfunc_autonomous_test.go:210-212`) — never an injected
  retry-class error, and pins both outcomes: peer progress committed
  before D renews (no terminal), and a static, still-contended row
  terminals exactly once.


  **Pending-set ownership (B3, v19; probe set corrected per A1, v20).** The statement's pending collection
  is a per-`(relation id, generation)` map (bounded by the number of
  distinct sequences one statement can exhaust on), not a single
  overwriteable slot: every conversion records its identity with the
  payload {Original, StmtDeadline, Cause}, and a repeated conversion on
  the SAME identity replaces only that entry. Arbitration consumes the
  WHOLE set of the current statement generation at the retry quiescence
  boundary and answers for it as a unit: the frozen envelope, isolation,
  publication and budget terms are re-verified once for the statement,
  The payload map alone is consumed; the retained watch set (A1) is
  not, and the probe round covers EVERY identity in the CURRENT watch
  set. Renewal is CONJUNCTIVE over terminal-eligible identities (Z2): D
  renews only when every terminal-eligible retained
  identity either shows an observable committed change since its last
  sample or resolves as available (A3), young identities voting neither
  way (a set holding ONLY young identities renews once so each receives
  its first full observed interval); any committed-unchanged AND
  unavailable terminal-eligible identity means no renewal. A declined arbitration surfaces the
  named terminal of the FIRST-recorded pending entry — deterministic
  insertion order; every entry is an equivalent surface of the same
  statement's lock-wait/conflict exhaustion. This removes the v18
  mis-correlation: an exhaustion on sequence A can no longer be shadowed
  by a later write from sequence B, and B's progress cannot renew A's
  window because the conjunctive rule probes both. Generic (non-sequence)
  retry signals are untouched: an empty set skips arbitration exactly as
  before, and when a verdict admits, the re-executed statement resolves
  any coincident generic cause on rebuild as on base. Planned barrier
  case: two sequences on one statement with A permanently static (a held
  uncommitted row lock, B4) and B advancing throughout — B's progress
  must NEVER renew A's window; exactly one named terminal fires after one
  full no-progress interval measured from the last all-advance probe;
  plus the symmetric arm (A advancing, B static), the both-advancing
  renewal control, and the SUCCESSIVE-GENERATIONS arm (A exhausts in
  generation 1, B in generation 2; the generation-2 decision probes
  {A, B} conjunctively from the retained watch set).

  Uncommitted attempts never move the committed position, so a statement
  that only conflicts — its own or its peers' — observes no change and
  terminates after one full interval without progress; a statement whose
  own allocations keep committing observes change and renews, which is
  correct: it is being served. The end-to-end claim: all
  sequence-allocation work of one statement AFTER its first admission —
  attempts, backoff, probe rounds, conversion and arbitration checks
  across every retry — is bounded by D renewed only while every
  contended row keeps observably changing, and is therefore bounded
  absolutely by the session statement deadline; with no statement timeout
  configured the ceiling is one full renewal interval — measured for
  each identity from its own creation baseline, or from its creation
  while never baselined (Z2) — ending with
  some terminal-eligible contended row committed-unchanged AND
  unavailable to the
  statement — the exact condition under which the base binary's
  unbounded in-statement loop would also be making no progress on that
  row; a row that has become acquirable is a resolution, never a
  terminal, and a statement whose recorded contention has fully
  resolved holds no deadline at all (Z1). Pre-admission statements
  keep the base per-invocation bounds, and non-allocation statement work
  stays bounded by the session statement deadline as on base. Cleanup on
  every path is the existing per-attempt `ExecTxn` defer (aborted attempt
  transactions are released when the derived context fires).
  Deterministic validation: (i) a fake-clock marker/decision matrix UT
  — strict-advance renewal; boundary wrap-signature renewal;
  reconfiguration (SETVAL backward) renews once, then terminal after
  the next static interval; IDENTICAL-TUPLE cases — exactly one full
  CYCLE pass between probes with the row still contended, AND a
  genuinely unchanged, still-locked row — both classify no-progress
  and terminate exactly once, while the identical-tuple ACQUIRABLE arm
  resolves instead (A3); generation change re-baselines, renews once,
  then static ⇒ terminal; chained renewals advance the baseline; probe
  failure = no-progress; descending direction mirror; the YOUNG-
  IDENTITY arms (Z2) — B joins at D−ε with its creation baseline:
  the first comparison unchanged ⇒ renewal, never terminal; one full
  renewal interval after THAT baseline with the tuple still unchanged
  AND the row held ⇒ exactly one terminal; B's initialization while a
  terminal-eligible A is stalled-and-held neither renews nor delays
  A's exactly-once terminal (the cannot-renew-A control); a failed
  creation sample retries inside the next decision's round and a
  successful retry arms the baseline; never baselined for one full
  renewal interval from creation ⇒ fail-safe terminal; the descending
  mirror of the late-join arms; (ii) fake-clock
  expiry-ordering `Compile.Run` cases — peer progress committed before
  D with execution crossing D at the in-flight conversion AND at the
  post-rebuild entry (both renew, no terminal), the in-flight arm
  exercised through ACTUAL derived-context cancellation (A2), never an
  injected retry-class error; static, still-locked position (exactly
  one terminal after the first full no-progress interval); caller
  cancellation at/after D (the caller's error, no named terminal); the
  C1 identity regression — the SAME `SeqRetry` instance, watch set and
  renewed D survive multiple rebuilds with exactly one terminal after
  the original deadline's first no-progress interval; the
  window-identity case (two arbitration events share one D and watch
  set); the no-conflict multi-allocation control crossing the renewal
  interval; the closed-episode crossing (Z1): admitted ⇒ successful
  allocation ⇒ the watch set empties and D retires ⇒ processing
  continues PAST the retired deadline ⇒ the next allocation opens an
  ordinary per-invocation `min(statement, 10 s)` budget and SUCCEEDS
  (no named terminal); re-contention after closure is a fresh first
  admission (fresh D, fresh baselined entry, no stale watch state);
  caller cancellation after closure propagates the caller's own error
  unchanged (no D, no named terminal); an all-resolved probe round
  retires D instead of renewing; the successive-generations case (A exhausts in generation
  1, B in generation 2 — the generation-2 decision probes {A, B}
  conjunctively from the retained watch set; B's advance cannot renew
  a locked A); the unchanged-but-available arm (the identity's tuple is
  unchanged since its baseline but the row is acquirable ⇒ resolved;
  the statement renews and proceeds); the producer-stop-with-rebuild-
  crossing-D case (the baseline sampled the final peer commit; peers
  stopped; the row is unchanged and acquirable ⇒ the statement
  completes); (iii) the two-sequence conjunctive-renewal barrier case
  above, including the successive-generations arm; (iv) a `-race`
  re-ladder of the touched packages; (v) live controls on the isolated
  2-CN runtime — uniq16; then the sustained-no-progress blocker
  control (B4): a peer session holds an EXPLICIT uncommitted
  `SELECT … FOR UPDATE` on the sequence row while an autocommit
  `NEXTVAL` contends it: exactly one named no-progress terminal after
  one full renewal interval, cleanup verified (no leaked attempt
  transactions or locks; the retry state consumed); the blocker is
  then released (COMMIT) and a fresh `NEXTVAL` from a new session MUST
  succeed with the next value and correct committed state; then the
  successful-drain control (B4, now exercised across the A3
  discriminant): halting ordinary producers mid-herd MUST let the
  remaining admitted statements COMPLETE successfully with
  exactly-unique values (no false terminal when contention disappears),
  including a herd phase whose final commit is sampled as a baseline
  and whose admitted statement rebuilds across D; and — first live
  gate — (vi) the §6 gate re-run under the renewed rule (zero new
  client-visible errors wherever base completes, exactly-unique
  results, post ≤ 3× base wall clock).

  When the probe-first expiry rule declares D expired (B2), or the
  exhaustion is any other error class, or admission fails at the
  pre-filter or at arbitration (including
  publication winning the race), the named lock-wait/conflict exhaustion
  error escapes unconverted and is returned to the client exactly once —
  the terminal surface is unchanged from v12.
  Publication matrix (unchanged): success ⇒ publish; definite abort ⇒ never
  publish; ambiguous commit ⇒ bounded probe, never publish (gap); cancellation
  ⇒ pre-commit abort or post-commit gap. Cleanup by defer on all paths. A user
  txn holding an explicit `SELECT … FOR UPDATE` on the sequence row and then
  allocating in the same txn hits the bounded lock wait and receives the named
  error (unchanged, documented misuse).
- **Transaction-local** — the generation exists only inside the caller's
  current transaction. On this base the only admitted producers are an
  explicit-transaction CLONE or snapshot restore: foreground `CREATE SEQUENCE`
  is rejected under explicit `BEGIN` (`stmt_kind.go:237-238`), `ALTER
  SEQUENCE` is not admitted inside an uncommitted explicit transaction
  (`stmt_kind.go:360`), and `DROP SEQUENCE` forces the active transaction to
  commit first (`:166-170`), so `DROP`+`CREATE` cannot form one either. An
  explicit-txn CLONE/restore produces the local generation —
  `clone.go:629-635` shares the caller txn in explicit mode and the cloned
  sequence's DDL + state run through that same executor, `clone.go:1331-1354` →
  `snapshot.go:2943-2963`): allocation **joins the caller transaction** (the
  base mechanism, retained for this case only), because no autonomous
  transaction can resolve the identity — transaction-local databases are found
  only through the owning workspace (`engine.go:500-512`), otherwise resolution
  falls through to committed state. Session publication for this route happens
  at statement success, via the pre-existing `SeqAddValues`/`SeqLastValue`
  point; the only added session state is the K1 floor below (M3: still no
  registry — bounded, txn-owned, terminally cleaned). The classification is
  recomputed per call — a generation is either committed-visible or it is not,
  and a fixed key cannot straddle both, so the v3 caller-mode fallback shape
  (which let caller rollback reclaim committed values, M1) cannot recur. The
  decision must be conservative in the safe direction: the in-caller route is
  selected only on *positive* proof of in-transaction creation (identity
  resolved through the caller's own transaction-local workspace writes —
  `existAndActive`/table ops — coincident with the committed-visibility probe
  failing); when the probe is inconclusive (cache lag) the call errs or takes
  the autonomous route, which fails loudly with a resolution error on a
  genuinely local identity, and never silently sends a committed generation
  in-caller (that is the direction in which caller rollback could reclaim —
  M1).

**K1 — allocation/publication protocol on transaction-local generations (v7: pass-scoped obligations, boundary invalidation, commit-time lifecycle per J1–J3; v8: preserve-or-restore stamp per I1).** The
v5 fate-sharing premise was false: with the default error classification, a
nonfatal statement error in an explicit transaction rolls back only the
statement — `RollbackLastStatement` truncates the failing statement's workspace
writes (`txn.go:1164-1199`, `disttae/types.go:1174-1194`); only ~12
infrastructure codes escalate to whole-txn rollback (`util.go:2277-2286`,
`moerr/txn.go:31-37`), and NO CYCLE exhaustion is a generic internal error
(`seqfunc.go:395/402`). Result batches are streamed per batch
(`resp_client.go:234-249`; packets flush as they fill, `mysql_buffer.go:899-920`),
so a multi-batch `NEXTVAL` query on a transaction-local generation can emit
values, fail mid-stream, have its counter writes rewound, and leave the
generation alive — the emitted values would be re-issuable, and a failed
default `INSERT`'s generated id would reissue on retry. The protocol behind
I4 on this route (transaction error semantics unchanged):

1. **Issued-value obligation (floor), scoped to a CYCLE pass (J1).** The session
   maintains at most one obligation per (txn, identity) — keyed by **relation
   id** (J3) — for transaction-local generations it allocates from:
   `(pass, floor)`, where `floor` is the last value issued in the identity's
   current CYCLE pass (ascending pass: max issued in the pass; descending:
   min), raised in **session state at allocation time, before the row is
   emitted** — independent of wire streaming and of the statement's later
   fate. Emitted values of the current pass are a subset of floored progress.
   One entry per identity at any time: a pass transition reseeds the single
   entry (retired passes hold no obligation), so the structure stays bounded
   by the number of local identities, never by passes (M3).
2. **Floor enforcement within the pass.** Every allocation from that identity
   within the same transaction computes from the workspace counter as today;
   when the candidate is not strictly beyond the current pass's floor (a
   statement rollback rewound the workspace), the position is repaired forward
   to floor+step before emission — never re-issuing any value of the current
   pass. Retries of the failed statement and later statements alike receive
   fresh values ≥ floor+step (acceptance "valid retry"). In the unwound common
   case the candidate is strictly beyond by construction, so enforcement is a
   no-op: base allocation behavior is unchanged.
3. **CYCLE wrap = deliberate pass boundary (J1).** When the computed advance
   crosses the configured edge, base CYCLE handling (`seqfunc.go:392-405` —
   descending re-enters at `maxv`, ascending at `minv`; pins
   `seq_func.result:2-11` = `100,60,100`) emits the start-side value. At every
   wrap — reached by natural advance or by the repair-forward of step 2 — the
   current pass's obligation retires and a fresh pass begins, seeded with the
   wrapped value (issued at allocation time as usual). Retired-pass values are
   deliberately re-issuable from the wrap onward (I4 generation boundary);
   non-reuse obligations never span passes. Consequently a statement rollback
   whose rewound emission had crossed a wrap re-wraps on the next allocation
   into a *new* pass rather than resurrecting the old pass's floor — the
   pinned streams (e.g. descending `100,60,100`) are preserved identically on
   the in-caller route. A NO CYCLE edge-crossing errors as today (nonfatal,
   `seqfunc.go:395/402`); the pass floor stands for later retries.
4. **Deliberate boundaries invalidate obligations (J2).** A successful
   SETVAL/restart on the identity **clears** its obligation entry — it is
   *not* reseeded to the repositioned state: a reposition carries no issued
   values, and the I4 boundary already permits deliberate re-issue of
   pre-boundary values. Entries therefore only ever contain values issued
   after the latest successful boundary. A failed SETVAL/restart rolls back at
   statement level and leaves the obligation standing. This is what preserves
   the pinned uncalled-SETVAL contract on the local route
   (`seqfunc.go:524-527,595-600`: SETVAL stores `storedCalled = prevCalled ||
   !iscalled`, so a never-called sequence keeps `is_called=false`;
   `seq_func.result:49-58`):
   after a rewound first allocation (floor=10, workspace restored to
   `is_called=false`), `SETVAL('s',60,true)` clears the entry, and the next
   in-txn NEXTVAL returns exactly 60 — no enforcement advance, no called-stamp.
5. **Pre-commit advance stamp — per surviving obligation (J1/J2/J3).** Before
   a transaction commits, the commit path applies, inside the committing
   transaction, one advance stamp per remaining obligation entry:
   a. **Lifecycle resolution (J3).** Resolve the entry's relation id through
      the committing transaction's *own final catalog view*. If the generation
      no longer exists in this transaction — deleted by it (e.g. in-txn
      `DROP DATABASE`, step 6) — the obligation retires unstamped; its values
      die with the generation, so no continuation exists that could re-issue.
   b. **Obligation gate (J2).** Entries exist only where a value was issued
      after the latest boundary (step 4), so a repositioned-but-uncalled state
      is never stamped: a commit with no post-boundary issue writes nothing
      and commits the repositioned state as-is (e.g. `(60,false)` stays
      `(60,false)`; a fresh session's NEXTVAL then returns exactly 60).
   c. **Stamp write — preserve surviving progress, restore issued obligations
      (J1/I1).** Let `W = (last_seq_num, is_called)` be the generation's
      surviving position inside the committing transaction. The stamp restores
      `(floor, is_called=true)` exactly when `W` is *unsafe* for the final
      pass's issued values — ascending: `W < floor`, or `W == floor` with
      `is_called=false`; descending symmetric (`W > floor`, or equality
      uncalled) — and otherwise writes nothing: a surviving position at or
      beyond the obligation already guarantees that no issued value of the
      pass is silently re-issued, and is preserved as-is. Every `W` the caller
      transaction can hold falls into one of these cases: (i) the natural
      case `W == floor`, called (no-op elision); (ii) a statement rewind
      within the pass (`W` behind the floor → restore, so emitted values
      stay un-reissuable); (iii) a surviving non-allocation advance such as
      transaction-local merge reconciliation (§4 B step 3 — ascending
      `W > floor` / descending `W < floor` → preserve, so merged keys stay
      below the committed position and the next allocation cannot collide
      with them); (iv) a rewind across a CYCLE wrap — an ascending `W` beyond
      the current-pass floor belongs to an older pass and is safe to preserve
      (the continuation stays beyond the pass's issued range until a
      deliberate wrap re-enters it), while a descending `W` above the floor
      would walk the continuation back down through the pass's issued values,
      so the `W > floor` arm restores the floor. Explicit-pass reasoning, not
      a cross-pass monotonic predicate, is what makes the numeric arms safe.
   If a stamp or the commit fails, the whole transaction rolls back by
   existing semantics and the generation dies. An ambiguous commit outcome
   leaves exactly two worlds — committed-with-stamp (never reissue) or
   rolled-back (generation dead) — both safe.
6. **Generation deletion and replacement transitions (J3).** Deleting the
   generation inside the caller transaction — on this base, `DROP DATABASE`
   (admitted in an explicit transaction, `stmt_kind.go:235-236`; no
   statement-end commit, `:166-170`; relation deletion `engine.go:803-808` →
   `txn_database.go:185-189`) — retires the obligation iff the deletion is
   still effective at commit, per step 5a's resolution through the txn's final
   view; evaluating existence at commit time, rather than flagging session
   state at deletion time, means a *rolled-back* deleting statement leaves the
   generation present, so its obligation is preserved and stamped. A
   replacement (drop + recreate under the same name) receives a new relation
   id: the old id resolves absent at commit (retired, never stamped onto
   anything) and the new generation builds its own obligation by id — an old
   obligation can never apply to a replacement. `DROP SEQUENCE` forces the
   active transaction to commit first (`IsCreateDropSequence`,
   `stmt_kind.go:166-170`), so an obligated deletion through it stamps at that
   commit and deletes after — no conflict.
7. **Reconcile distinction.** Merge-reconciliation advances on local
   generations are not allocations: they stamp merged-row watermarks in the
   same statement as their rows (same fate; idempotent conditional updates
   make retry a no-op) and never enter the floor. At commit the step-5 stamp
   preserves such a surviving advance (case iii above) rather than stamping
   back to the allocation floor; a rolled-back MERGE rewinds its rows and its
   reconcile update in the same statement (§4 B step 3), leaving nothing to
   preserve — restoration to the floor then covers exactly the issued values.
8. **Boundaries and cleanup (M3 contrast).** The obligation table is bounded
   (at most one `(pass, floor)` entry per relation id allocated from in this
   txn), owned by the session's transaction lifecycle, and terminally cleaned:
   consumed by the pre-commit stamp, discarded on rollback or session end.
   Session death before commit rolls the txn back (generation dies; the
   obligation dies with the session). No cross-session visibility exists for a
   local generation, so an obligation is never shared. CURRVAL publication
   stays at statement success (v5); CURRVAL never issues.

Composition with M1 is unchanged: allocations on committed generations are
autonomous (statement rollback cannot rewind an already-committed autonomous
update), and the routing rule never sends a committed generation in-caller.

**Global canonical lock order (L2).** All sequence lock acquisition follows one
total order. Identities are ranked by `(account_id, relation_id)` ascending
(relation id is unique per account and immutable within a generation); within
one identity, the catalog key (`mo_catalog.mo_tables` row for
`(account, db, table)`) precedes the sequence data row. Participants:

- *Allocator* (single identity per call): shared catalog lock (LOCK_OP metadata
  lock — `compile2.go:319-352` → `compile.go:846-850` → `lock_meta.go:188-201`,
  `LockMode_Shared`), then the data-row lock.
- *Reconcile unit* (multi identity): walks its sorted identity set; per identity
  it acquires the shared catalog lock, validates the committed id under it
  (abort the whole unit immediately on mismatch, §4 B K2), then takes the
  data-row lock bound to the validated id, before proceeding to the next
  identity.
- *`DROP SEQUENCE`*: `lockMoTable` exclusive, then delete (`ddl.go:4544,
  6677-6701`).
- *`ALTER SEQUENCE`*: `lockMoTable` exclusive first, then `SELECT … FOR UPDATE`
  capture inside its txn, then drop/recreate under the held exclusive
  (implementation must verify same-txn re-lock compatibility of the nested
  DROP, fallback: engine-API delete under the held lock).

*Deadlock-freedom (K2).* Every multi-identity acquirer proceeds in strictly
ascending identity rank and validates each identity's id under its shared
catalog lock *before* acquiring its row lock and before touching any further
identity; every single-identity acquirer touches one rank only. A unit blocked
on a row lock holds the shared catalog locks of every identity it has acquired
*and of the identity it is requesting*: those shared locks pin the
corresponding committed ids (drop/recreate needs the conflicting exclusive
lock), so every rank the unit holds — and the rank it is waiting for — is a
*current* committed rank. A wait edge therefore targets a holder whose
validated, still-pinned ranks equal the waiter's, and whose own outstanding
requests are at strictly higher current ranks; induction along the wait-for
chain gives strictly increasing current ranks, so no cycle exists. A rank
change committed after a unit's planning is detected at that identity's
validation step, before the unit acquires anything past it: the v5 L2
acquire-then-validate window (a stale rank carried to acquisition) is closed —
the reviewer's A(10→20-stale)/B(20→30) interleaving aborts A at its first
validation because planned id 10 ≠ committed id 30, and A restarts with the
current order matching B. Within one identity, catalog-before-row holds for
every participant, and the catalog key's FIFO prevents barging: an ALTER
waiting for the exclusive catalog lock blocks only short, bounded shared
holders ahead of it, then captures the row either including a just-committed
allocation or preceding post-restart re-resolution — allocation can neither be
lost nor resurrected (N1 invariant preserved). Shared catalog locks never
invert the order — they exclude only ALTER/DROP exclusives at their own rank,
and no participant requests a lower rank after a higher one. In-caller
(transaction-local) rows are private to the caller transaction and cannot be
contended by any other transaction, so they take no part in the ordering; the
v4 sentence "nobody holds row while requesting catalog" described only the
single-identity allocator and is superseded by this global-order argument.

D2/D3 remain rejected.

## 5. Logical closure (lifecycle, error paths, ownership)

- Single owner of allocation state: the sequence row; `select * from seq` stays the
  introspection contract, with the §8 RYOW note.
- Error surface: A names the correct database; B adds the reconcile-unit failure
  (range guard / identity / lock-conflict exhaustion) failing the MERGE statement
  and rolling back its rows — and nothing else, since the reconcile unit is
  atomic; C keeps existing acceptance for clamped starts and adds documented
  final-state rejection semantics; D adds the bounded lock-wait/conflict error
  naming `db.sequence` and transparent stale-identity retry.
- Concurrency: same-process gate; short autonomous row locks for allocation and
  reconciliation under one global canonical order (identity rank, catalog
  before row); ALTER/DROP exclusive at their identity's rank; whole-unit
  restart on identity change; merge transactions hold no sequence locks on
  committed generations (transaction-local generations stay in the caller txn,
  unreachable by others).
- Crash/restart: committed counter persists; autonomous commits precede publish
  (crash between ⇒ gap). A crash between reconcile-unit commit and merge-txn
  commit aborts the merge with gaps and no collision path.
- Session state: single publish point; `CURRVAL` after rollback reports the last
  obtained value (PG). Publication from a transaction-local generation happens
  at statement success; after whole-txn rollback its target no longer resolves
  (PG-consistent). The only added session state is the K1 per-(txn, identity)
  obligation — at most one `(pass, floor)` entry per relation id, reseeded at
  each CYCLE wrap (J1), cleared at a successful SETVAL/restart boundary (J2),
  retired unstamped for generations the txn itself deleted (J3), consumed by
  a preserve-or-restore pre-commit stamp (I1: restore rewound positions,
  preserve surviving advances), discarded on rollback/session end — not the
  unbounded cross-txn registry rejected at M3.
- Retry: caller statement retry after a failed allocation issues a fresh value
  (acceptance "valid retry"): on committed generations the autonomous update
  survived, on transaction-local generations the floor raises past the
  rewound values; merge retry re-runs idempotent reconciliation.

## 6. Performance / resource model

- Costs: one extra committed txn per issued value on committed generations (a
  transaction-local allocation adds no commit); one committed-visibility probe
  per allocation (in-memory catalog-cache lookup, no I/O); obligation
  bookkeeping per in-txn-local allocation (at most one `(pass, floor)` map
  entry per relation id; write per emission; reseed at wrap; clear at
  boundary); at most one advance stamp per surviving obligated local
  generation at commit (retired for self-deleted generations; write elided
  whenever the surviving position is already safe-or-beyond — the natural
  `(floor,true)` row or a preserved non-allocation advance, I1); bulk default
  inserts pay per row; one short reconcile txn per table merge at completion
  (zero when no reconcilable committed watermark exists); per expiry
  decision (arbitration, entry, conversion, or the in-flight
  budget-expiry route): one bounded tuple-probe round of every retained
  watch identity (including at most one re-initialization sample per
  young identity), plus at most one bounded lock-acquisition check per
  still-unchanged terminal-eligible identity in that round, plus one
  creation-time baseline-initialization
  sample per newly contended identity at admission/conversion — distinct
  decisions of one statement bounded by the number of renewal intervals
  it consumes (D1 renewal, §4 D, A4/Z2); B otherwise reads
  in-memory extremes only; A adds one plan-build walk. A closed episode
  (Z1: watch set empty) adds no probes or renewals — the statement runs
  under the base per-invocation budget until a fresh admission.
- **Measured experiment:** base vs fix on the isolated single-CN cluster — 16
  clients × 250 `NEXTVAL` (4,000 allocations, exactly unique) and a 1,000-row
  default insert; acceptance `post ≤ 3× base` wall time, recorded in PR evidence;
  exceeding the gate blocks delivery. (v13 re-verification on the terminal-only
  tier-1 machine after the F5 correction: 14/16 clients errored inside the
  frozen tier-1 window — 3 s, 286/4,000 allocations — while the unchanged base
  binary drained the same topology in 95 s with 4,000/4,000 unique and zero
  client errors. The gate is therefore a hard requirement for the tier-2 build
  and is re-run as the first live check of the next implementation turn.)
  (v18 re-verification on the C1-corrected binary — the v17 green ran under
  the C1 rebuild-reseed defect, so this is the first valid fixed-D
  measurement: the 16-client herd exceeds a fixed 10 s window — quiet window
  (host load ≈7): 3,285/4,000 allocations, 3 sessions terminally errored,
  38 s; under load (≈17): 2,072/4,000, 8 terminals, 51 s; 8-client
  calibration: 2,000/2,000, rc=0, 18 s; unchanged base on the same host
  class: 4,000/4,000, rc=0, 95 s. ≈97% of the 383 tier-1 exhaustions
  logged across all corrected-build windows were absorbed transparently
  (11 named terminals, one per affected statement); every terminal
  surfaced exactly once with duplicate count 0 and delivered prefixes
  intact. Consequence recorded as
  D1 (§4 D): the gate is reinterpreted, not weakened — zero NEW
  client-visible errors wherever the base completes (the contended rows
  keep observably changing, so renewal keeps admitted statements alive
  through a draining herd, and rows whose contention clears resolve and
  proceed — a completed herd never false-terminals (A3); the promise
  excludes only shapes whose contended row stays committed-unchanged AND
  unavailable across a full interval — the accepted complete-cycle ABA
  quantization limit included (B1) — whose sanctioned outcome is the
  named no-progress terminal); the `post ≤ 3× base` wall-clock gate
  stands; the only sanctioned client-visible failure is the no-progress
  terminal, asserted by the sustained-no-progress blocker control (a held
  uncommitted sequence-row lock; the producer-stop shape is exercised as
  the successful-drain control across the availability discriminant,
  B4/A3).)

## 7. Security and tenancy

Unchanged: A2 resolves within the caller's account via the owner table's database
name only; autonomous allocation runs under the same account id; subscription
restrictions and sequence-DML guards (issue_28246's ordinary-table rejection)
unchanged.

## 8. Compatibility and delivery

- **Intended observable changes:** owner-db default resolution (A); post-merge
  default inserts succeed for reconcilable columns, including pre-COMMIT inside an
  explicit transaction (B); `ALTER … START WITH n` restarts to exactly `n` in
  all sessions when `n` is strictly ahead of the pending value — for a
  never-issued sequence, its unissued start value — while a start at or
  behind the pending value keeps the position and allocation continues
  from the pending value (never-issued included: created `START WITH 10`,
  never issued, `ALTER … START WITH 5` ⇒ next value 10); the single
  uncalled exception restarts a pending value outside the effective final
  bounds at the declared start, for CYCLE and NO CYCLE alike —
  direction-aware clamping and final-state validation with replayable
  `rel_createsql` (C); rollback never
  reclaims allocated values (D, scoped I4); cross-session allocation waits
  become bounded (named error instead of indefinite wait). **Newly documented
  (v4; arms distinguished in v14):** an aborted or rolled-back merge may leave
  the destination sequence advanced across the would-have-been-merged range —
  consumed gaps, never reuse; PG-consistent. This is the caller arm (merge
  statement failure or transaction rollback *after* the reconcile unit has
  committed); a failure of the reconcile unit itself remains fully atomic —
  no row apply, no sequence movement.
  **Newly documented (v5; producer list corrected in v9):** pre-COMMIT use of
  generations created inside an explicit transaction (explicit-transaction
  CLONE, snapshot restore — the only admitted producers; foreground
  `CREATE SEQUENCE`/`ALTER SEQUENCE` are rejected inside an explicit
  transaction, §4 D) keeps base behavior — allocation and merge reconciliation for those
  identities join the caller transaction because no autonomous transaction can
  resolve them; caller ROLLBACK erases generation and issued values together
  (re-creation re-issues deliberately, I4 boundaries); an aborted merge can
  leave gaps only on committed generations. **Newly documented (v6):** on a
  transaction-local generation, a value emitted before a nonfatal statement
  failure (streamed rows or a failed INSERT's generated key) is never
  re-issued — the session floor remembers it and the pre-commit stamp persists
  the advance even when statement rollback rewound the workspace counter;
  retries receive fresh values. Existing transaction error semantics are
  untouched: statement-only rollback, escalation set, and escalation behavior
  are unchanged. **Newly documented (v7):** on a transaction-local CYCLE
  generation, issue-once holds within each cycle pass, and every wrap —
  natural or rewind-repaired — is a deliberate boundary that re-enters the
  range from the start side, so the pinned streams (`seq_func.result:2-11` —
  `100,60,100` descending) are observed identically on the in-caller route;
  a successful SETVAL/restart on the identity invalidates outstanding
  obligations, so the pinned uncalled-SETVAL contract
  (`seqfunc.go:524-527,595-600`; `seq_func.result:49-58` — store
  `is_called=false`, next NEXTVAL returns the positioned value exactly) holds
  identically there; an in-txn DROP DATABASE over a floored generation commits
  normally — a generation deleted by its own transaction carries no obligation
  into commit, while a rolled-back DROP leaves the obligation to be stamped.
  **Newly documented (v8):** on a transaction-local generation, COMMIT
  preserves surviving non-allocation counter progress made inside the same
  transaction — transaction-local merge reconciliation (§4 B step 3): the
  issued-value stamp restores rewound positions but never regresses a
  surviving advance, so merged keys remain below the committed position and
  the first post-commit default INSERT cannot collide with them; a merge that
  is rolled back (statement failure or guard abort) rewinds rows and
  reconciliation together, and the stamp then covers exactly the issued
  obligation. **Retained:** non-START ALTER semantics in both sessions
  (`8,18,28` pin, state tests, #30371), CYCLE wrap (`100,60,100` pin, both
  routes), SETVAL repositioning (deliberate boundary; invalidates — rather
  than reseeds — outstanding obligations; uncalled-state contract
  `seq_func.result:49-58` unchanged), RC introspection.
- **Read-your-own-writes note (#28246 lineage):** default read-committed
  introspection (`select * from seq`) continues to observe committed allocations
  including its own; under repeatable-read snapshots inside an explicit
  transaction the counter reflects the snapshot rather than the txn's own
  allocations — consequence of the non-transactional allocation the acceptance
  checklist requires; documented, with `issue_28246`'s other executable oracles
  retained.
- **Deliberate pin updates (inventoried, unchanged from v3):**
  1. `alter_sequence.sql` / `alter_sequence_1.result`: START-WITH pins move to the
     uniform restart rule (next=900 after `start with 900`, was 910).
  2. `issue_28246.result`: D shifts `begin/nextval(1)/rollback/nextval` to `1→2`,
     the later pair `2,3 → 3,4`, singleton `4 → 5`; setval/currval, multi-row
     `12,13`, UNION `14,15`, introspection `(15,1)`, and the ordinary-table
     rejection pins are unchanged.
  No other expected-result files change.
- **Documented limitations:** transformed/dynamic sequence-referencing defaults are
  not reconciled (residual collision risk, counterexample oracle); stale
  `rel_createsql` heals on the next post-fix ALTER. **DATA BRANCH MERGE into a
  transaction-local destination (v10 boundary, pre-existing base behavior):** a
  merge whose destination database exists only inside the caller's uncommitted
  transaction is rejected atomically with `ERROR 20101` at the branch-meta
  created-time probe (`pkg/frontend/data_branch.go:3343/3355/3408-3412`) before
  any row apply; the transaction remains usable. Unrelated to sequences and
  unchanged by this work; pinned by the boundary control in
  `sequence/sequence_txn_local.sql`.
- **Upgrade / rolling deploy (N4, unchanged):** phase (a) per-CN rollback
  protection from upgrade onward; phase (b) catalog-writer protection at full
  upgrade; sequence DDL fenced operationally during the window (documented, no code
  enforcement). One-arg catalogs healed at evaluation time; no migration.
- **Rollback of this PR:** behavior-restoring only, no persisted format change.

## 9. Verification plan (cheapest discriminating evidence first)

Red/green protocol: every new BVT first fails on base with the documented defect
output, then passes post-fix; one focused deterministic file per defect before any
suite run.

- **A (UT + BVT):** plan UT (pin only when absent; SETVAL arity; nested/cast
  untouched; `TableDef.DbName` literal) + BVT
  `test/distributed/cases/sequence/qualified_insert_owner_db.sql` (missing caller
  sequence — base 1146; same-named caller sequence untouched; prepared variant;
  same-db control; exact ids and both sequences' states).
- **B (UT + BVT):** white-box UT `pkg/frontend` (fake `BackgroundExec`): multi-flush
  watermark accumulation; staged vs `disableInsertStage` layouts; update-apply
  column set vs skipped deletes; reconcile-unit SQL per direction incl. the
  equality-with-uncalled arm; idempotent re-reconcile (rows-affected 0 = strictly
  ahead); range-guard failure aborts the reconcile txn atomically and fails the
  merge; reconcile not executed in the caller/merge transaction (executor identity
  asserted). BVT under `git4data/branch/merge/`: base repro green (exact ids,
  post-merge `last_seq_num/is_called`); non-unit increment; explicit-high merged
  value; >`maxSqlBatchCnt` multi-flush global maximum; fakeKind no-staging layout;
  never-called destination with watermark == position (ascending and descending);
  called-equality no-op control; repeated merge reconcile; merge abort: rows
  reverted, **next default insert still succeeds beyond the watermark** (gap, no
  collision); transformed-default counterexample; descending reconcile to numeric
  min.
- **M1 oracles (B+D, planned per finding):** same-session —
  `BEGIN; MERGE` (imports key 2 over state (1,true)); `SELECT nextval` returns 3;
  `ROLLBACK`; next allocation returns 4 (neither 2 nor 3 ever reissued; CURRVAL
  semantics per PG); separate-session — a second session allocating during and
  after the rolled-back explicit merge receives values strictly ahead of every
  returned value; retry — a default `INSERT` whose statement fails after allocation
  retries with a fresh id, never the failed statement's value.
- **L1 oracles (B/D, planned per finding):** these flows work on base by joining
  the caller transaction, so they are regression-preservation oracles (green on
  base, must stay green — the red/green defect oracles above are unchanged):
  - BVT `test/distributed/cases/git4data/clone/clone_sequence_txn.sql` —
    `BEGIN; CREATE DATABASE dst CLONE src; USE dst; SELECT nextval('s')`
    pre-COMMIT returns the cloned state's next value; (i) `COMMIT` ⇒ a fresh
    session continues strictly past the consumed value; (ii) `ROLLBACK` ⇒ `dst`
    is gone and a later clean clone re-issues from the cloned state (new
    generation, deliberate); (iii) default-`INSERT` variant pre-COMMIT into the
    cloned table with `DEFAULT nextval('s')`; plus explicit admission-
    rejection controls: `BEGIN; CREATE SEQUENCE s;` fails with the
    foreground-DDL admission error (`stmt_kind.go:237-238`) and `BEGIN; ALTER
    SEQUENCE …` likewise (`stmt_kind.go:360`), each leaving the transaction
    usable and routing unaffected; hybrid routing in one txn: a committed sequence and a
    transaction-local sequence — rollback must not reissue the *committed*
    sequence's value (autonomous) while the local one vanishes entirely.
  - Merge against a transaction-local destination (v10 per §0 I5): the
    `BEGIN; CREATE DATABASE dst CLONE src; DATA BRANCH MERGE br.t INTO dst.t`
    shape is rejected on this base with `ERROR 20101` before any row apply —
    the I1 block's boundary control below pins that rejection, its COMMIT
    arm asserts `(rows, position, generation) commit atomically from the
    pre-merge state`, and its ROLLBACK arm asserts `(rows, counter growth,
    generation) vanish together`. The committed-route counterpart
    (`BEGIN; DATA BRANCH MERGE br.t INTO committed_dst.t; INSERT INTO
    committed_dst.t` with the default key; `COMMIT` ⇒ no collision and the
    counter advanced with the same commit) remains executable and covered
    (`sequence/sequence_txn_local.sql` committed block and the unchanged v4
    autocommit oracles). The discarded variant never discriminated the
    commit stamp's preserve-or-restore decision (its post-merge INSERT
    allocation raised the obligation floor past the watermark).
  - UT: routing classification (committed vs transaction-local identity →
    route), committed-visibility probe + identity revalidation on the probe
    result, publication-at-statement-success on the in-caller route, rollback ⇒
    resolution failure for the dead generation (no resurrecting publish), and
    that `issue_27055_clone_database_transaction.sql`, `clone_sequence.sql`,
    `clone_database_sequence.sql`, `clone_auto_increment_state.sql` stay green
    unedited.
- **L2 oracles (B/D, planned per finding):**
  - Deterministic UT (reconcile package + lock hook; barrier gates between
    lock-acquisition steps, no sleeps): two reconcile units over shared
    identities `{S1, S2}` submitted in *reversed canonical order* both complete
    — no lock-service deadlock victim, no named lock-wait error, final states
    monotone-consistent under either serialization; an `ALTER SEQUENCE S2 …`
    competitor injected at the barrier completes alongside both merges under
    bounded waits with §4 C semantics and no lost allocation; the acquisition
    trace asserts strictly ascending identity rank for every unit; whole-unit
    restart: drop+recreate of one identity between a unit's plan and lock steps
    forces abort → re-plan → bounded retry → success carrying the new identity
    (never a half-updated set); a single-identity allocator concurrent with a
    multi-identity unit under the same barrier completes without victimization.
  - BVT (SQL-level integration cover; determinism lives in the UT, the BVT
    asserts final state and absence of errors): two tables whose defaults use
    the same two sequences in reversed column order, two sessions merging
    concurrently in a loop (≥20 rounds) while a third session issues repeated
    ALTERs; afterwards, exact watermark/residual states verified and every
    admitted outcome explained by the monotone conditional predicate.
- **K1 oracles (D, planned per finding; pass/boundary/lifecycle/stamp semantics
  per the v8 protocol — J1–J3 and I1 oracles below extend the same UT
  harness):**
  - Deterministic UT (session obligation + fake workspace implementing
    `RollbackLastStatement` truncation, fake wire sink, no sleeps): allocate a
    batch of values on a transaction-local generation, assert the floor rises
    before each emission; rewind the statement's workspace writes (statement
    rollback); assert the next allocation in the same txn returns floor+step —
    never any emitted value of the current pass; assert a commit executes the
    advance stamp and persists a counter past the floor; assert rollback
    discards obligation +
    generation together; assert obligation entries are removed at txn end
    (cleanup).
  - BVT/multi-session SQL cover of the finding's required shapes plus controls
    (post-state assertions are deterministic even where wire streaming is
    timing-based):
    (i) *emitted-batch + nonfatal failure*: explicit txn, in-txn clone, a
    multi-row `SELECT nextval('s')` over a rows-source larger than a small
    `MAXVALUE` so exhaustion (`seqfunc.go:395/402`) hits mid-statement; assert
    the error, that the txn stays open (statement-only rollback per
    `txn.go:1164-1199`), and that every subsequent allocation in that txn is
    strictly ahead of every value the failed statement could have emitted,
    with no reuse across retry; (ii) *failed default `INSERT` retry* on the
    txn-local sequence: first attempt fails after its allocation (later-row
    conflict), retry returns a fresh id ≥ failedid+step; (iii) *commit stamp*:
    COMMIT after a rewound allocation leaves the committed counter strictly
    past the highest emitted value — a fresh session continues beyond it;
    (iv) full `ROLLBACK` control: generation and values vanish; (v) deliberate
    boundary: successful `SETVAL` invalidates the obligation (J2 controls
    below); a failed SETVAL leaves the floor standing.
- **K2 oracles (B/D, planned per finding — extends the L2 barrier UT):**
  - Recreate-one-identity-between-plan-and-acquisition: unit A plans order
    (S1=10, S2=20); barrier; S1 is dropped+recreated and committed as id 30;
    unit B plans current order (S2=20, S1=30); barrier releases both: A must
    abort at S1's validation step *before acquiring S1's row or touching S2*,
    restart bounded with the current order (20, 30), and both units complete —
    no cycle, no wait on a stale rank, final states monotone. Assert the
    acquisition trace shows validation-before-row per identity and the
    immediate whole-unit abort (all prior locks released in the same step).
  - Name-vs-id binding check: a statement-plan-time re-resolution is rejected
    as an acquisition source (UT asserts each row statement targets the
    relation id validated at that step, `sql_executor.go:604-635`
    notwithstanding).
- **J1 oracles (D, planned per finding — CYCLE pass transitions on the local
  route, extending the K1 UT harness):**
  - Deterministic UT (fake `RollbackLastStatement` workspace + fake wire, no
    sleeps), ascending and descending CYCLE generations (descending mirrors
    the pinned s1 shape: increment −40, min 30, max 100): the in-txn stream is
    exactly `100,60,100` — the pass increments at the wrap, the wrapped value
    seeds the new pass, and no floor blocks the pinned re-entry; COMMIT after
    the wrap stamps the final pass's `(floor, is_called=true)`; *rewind across
    a wrap*: emit through a wrap, roll the statement back, then assert the
    next allocation never re-issues the live pass's values (repair forward),
    that a repaired wrap opens a *fresh* pass whose start-side emission is
    asserted as the deliberate boundary, and that retired-pass values re-enter
    only via a wrap; NO CYCLE exhaustion during repair keeps the pass floor
    standing for later retries.
  - BVT: explicit-txn clone of ascending and descending small-range CYCLE
    sequences; assert the exact pinned streams in-txn (`100,60,100`
    descending) and the fresh-session continuation after COMMIT (continuation
    re-enters values only through a deliberate wrap). Committed-route CYCLE
    controls (`seq_func*.sql` pins, incl. `100,60,100`) stay green unedited —
    they exercise the autonomous route where no obligation exists.
- **J2 oracles (D, planned per finding — repositioned-uncalled state vs
  issued-value obligation):**
  - The finding's exact shape as UT and BVT: explicit txn, in-txn clone of an
    uncalled sequence (increment 3, start 10); a failed default `INSERT`
    allocates 10 (floor=10) and is statement-rolled-back (workspace restores
    `is_called=false`); successful `SETVAL('s',60,true)` must succeed and
    invalidate the obligation. (a) *immediate-NEXTVAL control*: the next
    in-txn NEXTVAL returns exactly 60 — the `seq_func.result:49-58` shape on
    the local route; (b) *COMMIT-before-NEXTVAL control*: a commit with no
    post-boundary allocation performs no stamp — the committed state stays
    `(60,false)` and a fresh session's NEXTVAL returns exactly 60 (never 63);
    (c) *failed-SETVAL control*: a statement-failed SETVAL leaves floor=10
    standing, and the next allocation returns 13.
  - Pin preservation: `seq_func.result:49-58` and the `storedCalled` contract
    (`seqfunc.go:524-527,595-600`) stay green unedited on the committed route.
- **J3 oracles (D, planned per finding — generation deletion/replacement
  lifecycle):**
  - UT (fake final catalog view at commit) + BVT: `BEGIN; CREATE DATABASE dst
    CLONE src; SELECT nextval('dst.s'); DROP DATABASE dst; COMMIT` — COMMIT
    succeeds; no stamp targets the deleted relation id; `dst` is gone;
    re-cloning the same name is a new generation that deliberately issues from
    its own cloned state.
  - *Failed-drop control*: force the DROP statement to fail after touching the
    generation (deterministic UT injection is the authoritative control — SQL
    lock errors escalate to whole-txn rollback and cannot express this shape)
    — statement rollback restores the generation; COMMIT stamps the
    obligation; a fresh continuation runs strictly past every value issued
    before the failed drop.
  - *Replacement control*: `BEGIN; CREATE DATABASE dst CLONE src;` allocate
    from `dst.s`; `DROP DATABASE dst`; `CREATE DATABASE dst CLONE src`;
    `COMMIT` — the old obligation is retired for the old relation id; a stamp
    lands only for post-recreate issues and only on the new relation id; the
    old obligation never applies to the replacement; allocations on the
    recreated generation start from its own state.
- **I1 oracles (B/D, planned per finding — commit stamp vs surviving
  reconciliation; UT extends the K1 harness):**
  - Deterministic UT (fake workspace + a reconcile-update primitive, no
    sleeps), ascending and descending: allocate (obligation floor `f`), apply
    a *surviving* reconcile update beyond `f`, COMMIT with no intervening
    allocation ⇒ the persisted counter is the reconcile position `(w,true)`,
    never stamped back to `f`; a fresh (second fake-session) allocation issues
    strictly past the watermark. Rewind-only counterpart (no reconcile): the
    stamp restores `(f,true)`. CYCLE-pass cross variants: an ascending
    older-pass rewind beyond `f` is preserved (continuation re-enters issued
    values only via a deliberate wrap); a descending older-pass position above
    `f` is restored.
  - BVT (v10 per §0 I5 — the transaction-local preserve arm is SQL-infeasible
    on this base, so the discriminating cover stays in the UT above):
    - *Boundary control* (`sequence/sequence_txn_local.sql`): `BEGIN; CREATE
      DATABASE dst CLONE src;` default `INSERT INTO dst.t` (issues 2,
      obligation floor=2); `DATA BRANCH MERGE br.t INTO dst.t` ⇒
      `ERROR 20101` (branch-meta created-time probe miss,
      `data_branch.go:3343/3355/3408-3412`) — statement-only failure: the
      destination rows hold exactly the pre-merge row, the txn-local
      sequence keeps its pre-merge position, and the transaction remains
      usable; `COMMIT` ⇒ committed `dst.s` is `(2,true)` (the stamp is
      unaffected by the rejected merge), and a fresh session's default
      INSERT emits 3; a `ROLLBACK` arm vanishes row, position, and
      generation together. This control pins the base merge-machinery
      limitation so oracle intent survives a future RYOW fix (the UT
      preserve oracles then promote to BVT unchanged).
    - *Committed-route equivalent* (`sequence/sequence_txn_local.sql`):
      explicit-txn merge into a *committed* clone destination —
      `BEGIN; DATA BRANCH MERGE br.t INTO dst.t; COMMIT` — reconciles
      through the autonomous unit with the same commit; a fresh session's
      default INSERT lands beyond the merged keys (no collision).
      Descending mirror.
  - *Failed-merge control:* deterministic UT injection is authoritative for
    the transaction-local composition (rolled-back merge rewinds rows and
    reconcile together ⇒ COMMIT persists exactly the issued-obligation
    position `(2,true)`, fresh allocation issues 3); the SQL-level control
    runs on the committed route (`merge_sequence_reconcile.sql` NO CYCLE
    range-guard abort): the merge statement fails at reconcile, no rows are
    applied, the committed sequence is unchanged, and the next default
    INSERT succeeds past the last committed value (gap, no reuse).
- **C (UT + BVT):** UT `alter_sequence_state_test.go`: existing non-START cases
  (82-90/~134) untouched; 12-case START-WITH matrix
  {asc,desc}×{called,uncalled}×{ahead,equal,behind} + START+INCREMENT flip —
  the matrix's keep-position pins (`:217` ascending uncalled behind, `:224`
  descending uncalled behind) encode the v17 authoritative uncalled rule
  (§4 C); uncalled out-of-bounds pending restarts pinned for NO CYCLE and
  CYCLE (`TestMakeAlterSequenceParamUncalledOutOfBoundsRestart` `:356`,
  `TestMakeAlterSequenceParamCycleOutOfBoundsPendingRestarts` `:562`);
  **final-state validation matrix (M2):** combined START+MIN/MAX that clamp to an
  out-of-bounds effective start rejects atomically (ascending `MAXVALUE` below
  clamped frontier; descending `MINVALUE` above it), persisted state unchanged
  afterward (statement rows verify the atomic rejection); synthesis text.
  BVT `alter_sequence_start_with.sql`: cross-session restart; descending
  100/99/98 + START 99 ⇒ 97, START 50 ⇒ 50; uncalled restart (ahead ⇒ exact
  restart — covered; **behind ⇒ the never-issued sequence KEEPS its created
  start, pinned both directions with fresh-session assertions (E1): ascending
  created `START WITH 10` + `ALTER … START WITH 5` ⇒ the state stays `(10,0)`
  and a fresh session's next `NEXTVAL` returns 10; descending created
  `INCREMENT BY -1 START WITH 10` + `ALTER … START WITH 15` ⇒ fresh next 10,
  while `START WITH 5` ⇒ restart to exactly 5**); **CREATE replay
  after successful combined START+MIN/MAX** (`rel_createsql` under a new name
  replays and allocates correctly); boundary-rejection cases from the matrix;
  non-START pins green unedited.
- **D (UT + BVT):** fake-executor UT: publication matrix (success/abort/ambiguous/
  cancel), retry bounds + exhaustion, identity revalidation, defer cleanup, and
  merge-reconcile path using the same autonomous executor. Compile-package hook
  interleave UT both directions (allocation waits at the catalog key during ALTER
  exclusive; allocation committed before ALTER exclusive is captured). BVT
  `sequence/nextval_rollback.sql`: rollback non-reissue same/other session —
  including the post-merge rollback case; committed continuity; setval gap; fresh
  retry id; interleave uniqueness; concurrent ALTER+NEXTVAL uniqueness.
  `issue_28246.sql` re-run with updated pins (§8). Durability: CN restart
  continues past committed values; 2-CN 16-client uniqueness control. Perf
  experiment per §6.
- **Regression preservation:** `seq_func*.sql` incl. CYCLE pins, `sequence.sql`,
  `issue_27581.sql`, `qualified_view_nextval.sql`, `alter_sequence_cross_session.sql`,
  existing git4data branch suites, and the clone sequence suites
  (`clone_sequence.sql`, `clone_database_sequence.sql`,
  `clone_auto_increment_state.sql`, `issue_27055_clone_database_transaction.sql`)
  — unedited; broader suites CI-scoped.

## 10. Decision log / drawbacks

- Accepted (v1→v3, carried): gap-after-rollback; direction-aware clamped-keep;
  reconcilable-shape restriction; monotonic conditional predicate incl.
  equality-with-uncalled; catalog-first two-namespace lock order; ambiguous-commit
  never-publish; perf 3× gate; named pin inventory; rollout phasing + fencing.
- Accepted (new in v4): **always-autonomous allocation with the in-caller fallback
  and registry removed** (M1/M3) — returned values can no longer be reclaimed by
  any caller rollback, including post-merge flows; **reconciliation as one
  autonomous atomic unit sequenced before merge success** (over in-merge-txn
  placement, which cannot coexist with autonomous allocation and usable pre-commit
  DML — the R1/N2/M1 tension), with merge abort yielding documented gaps; **final
  effective-state validation** as a documented C contract (M2).
- Accepted (new in v5): **generation-visibility routing** (L1) refines the v4
  "always autonomous" rule — autonomous allocation is confined to committed
  generations; allocation and reconciliation for transaction-local generations
  join the caller transaction by necessity (workspace-scoped visibility); the
  v5 safety argument for that route (fate-sharing) was retracted at K1 and
  replaced by the floor/stamp protocol (below); the M1 never-reclaim guarantee
  on committed generations is unchanged and the routing key never selects
  in-caller for them; **one global canonical lock order** (`(account_id,
  relation_id)` ascending, catalog before row) with whole-unit reconcile
  restart after identity changes (L2 — its acquire-then-validate form was
  retracted at K2 and replaced by validate-before-proceed, below), correcting
  v4's per-sequence-only ordering and its deadlock-freedom claim; reconcile
  atomicity restated per visibility partition (committed: one autonomous unit;
  transaction-local: the caller transaction).
- Accepted (new in v6): **K1 floor + pre-commit stamp** replaces the retracted
  fate-sharing premise for transaction-local generations — per-(txn, identity)
  session floor raised at allocation time (covers emitted-but-rewound values),
  enforced by every later in-txn allocation, persisted by a monotonic
  conditional stamp before COMMIT, reset only by deliberate SETVAL/restart, and
  terminally cleaned at txn end (v7 refines the floor into pass-scoped
  obligations and the stamp into the obligation-gated, lifecycle-resolved
  form below); transaction error semantics (statement-only
  rollback, escalation set) are untouched; **K2 validate-before-proceed**
  replaces acquire-then-validate: per identity, catalog lock → id validation
  under the lock → row lock bound to the validated id → next identity, with
  immediate whole-unit abort/re-plan on mismatch, closing the stale-rank
  acquisition window and yielding deadlock-freedom by strictly increasing
  current wait-for ranks.
- Accepted (new in v7): **pass-scoped issued-value obligations** (J1) — the
  K1 floor records issued values of the current CYCLE pass only; every wrap
  (natural or rewind-repaired) retires the pass obligation and seeds a fresh
  pass with the wrapped start-side value, so pinned CYCLE streams
  (`100,60,100`) survive identically on the in-caller route while issue-once
  holds within each pass; the pre-commit stamp writes the final pass's
  `(floor, is_called=true)` per obligation (raw numeric monotonicity across
  passes rejected as meaningless); **boundary invalidation** (J2) — a
  successful SETVAL/restart clears the obligation instead of reseeding it to
  the repositioned state, so a repositioned-uncalled state is neither advanced
  nor stamped-called (`seq_func.result:49-58` contract preserved on the local
  route); **commit-time lifecycle resolution** (J3) — obligations are keyed by
  relation id and the stamp loop resolves each id through the committing
  transaction's final catalog view: a generation deleted by its own
  transaction is retired unstamped, a rolled-back deleting statement leaves
  the obligation to be stamped, and an old obligation can never land on a
  drop+recreate replacement.
- Accepted (new in v10): **oracle placement for the transaction-local
  preserve arm** (I5) — the commit-stamp preserve-or-restore contract is
  unchanged; because this base rejects `MERGE` into a transaction-local
  destination before row apply (`ERROR 20101` at the branch-meta
  created-time probe), the preserve arm on transaction-local generations is
  discriminated by the deterministic UT (fake workspace + reconcile-update
  primitive) alone, while SQL pins the rejection boundary and the
  committed-route equivalents. Contract text, production behavior, and all
  four acceptance areas are untouched.
- Accepted (new in v12): **autonomous retry-bound calibration** (I6) —
  8 attempts with the documented per-step backoff window (25/50/100 ms + jitter,
  explicitly capped), deadline and non-retryable set unchanged, after live 2-CN
  16-client measurement showed the 4-attempt bound cannot drain a 16-way
  cross-CN stampede once the wrapper-traversing classification repair lets the
  transient set retry as specified; unchanged base passes 16/16 on the same
  topology and the calibrated bound restores that acceptance control.
- Accepted (new in v13): **bounded statement-level retry tier** (I7) — tier 1
  stays frozen per I6; tier-1 transient-conflict exhaustion converts to the
  compiler retry signal only for retry-safe auto-commit shapes and only while
  a statement-scoped sticky deadline (`min(statement, 10 s)`, created once per
  statement in `sequenceStatementState`, preserved across
  `prepareRetryTransition`) has not elapsed; on expiry the named exhaustion
  error escapes unconverted, exactly once. Restores the §6 gate drain the
  terminal-only machine cannot provide (base drains the same topology) while
  keeping every caller statement deadline-bounded end-to-end; the overall bound
  is proved through `Compile.Run`.
- Accepted (new in v14): **tier-2 admission predicate and deadline
  propagation** (H1/H2) — conversion is conditioned on a named predicate
  (dispatch envelope `{Autocommit,¬ByBegin}`, RC isolation, generic
  `canRetry`, zero delivered result batches; default-deny otherwise),
  enforced at the conversion site, and every admitted execution derives its
  attempt budget from the original sticky deadline D instead of a fresh
  window; terminal-error precedence is ordered, explicit-`BEGIN` /
  `autocommit=0` / partially-streamed shapes are terminal,
  and the public projection is aligned with §4 C clamping and the two-arm
  merge-failure gap (H3). Validation oracles: explicit-`BEGIN`,
  `SET autocommit=0`, and already-streamed-prefix rejection controls;
  admitted auto-commit controls; the deterministic just-before-expiry
  `Compile.Run` case; the existing behind-frontier START pins; a new
  post-reconcile-rollback gap pin.
- Accepted (new in v15): **retry/publication arbitration and lazy retry
  window** (G1/G2) — tier-2 admission is finalized at the producer-
  quiescence boundary (post-`cancelAndWaitRunningSQL`, pre-rebuild) against
  a synchronized per-statement publication flag, so an already-delivered
  prefix is never replayed or duplicated and publication always wins over
  retry (the original terminal exhaustion error escapes exactly once in
  that case); the sticky deadline D bounds only the admitted
  conflict-replay envelope and is created lazily at the statement's first
  admission (never reset across that statement's retries), leaving
  uncontended first-pass execution on the base per-invocation budget.
  Validation oracles: the deterministic publish-after-check barrier case
  (no replay, no duplicated prefix, `-race` clean), the fake-clock
  no-conflict multi-allocation control crossing the retry-window duration
  (no statement-wide timeout regression), and the no-reset window-identity
  case.
- Accepted (new in v16): **flag-only admission reads** (F1) — both
  admission phases (conversion-site pre-filter and quiesced arbitration)
  read ONLY the synchronized per-statement publication flag; the
  unsynchronized `SessionInfo.ResultRows` pre-filter hint is removed
  (behavior-neutral: the flag is set before any client bytes by the single
  publication path). The publish-after-observation barrier oracle stands
  and MUST execute race-clean under `-race` (zero warnings), proving the
  pre-filter reads no unsynchronized state.
- Accepted (new in v17): **one authoritative uncalled START WITH rule** (E1) —
  §4 C governs the never-issued arm: restart iff the declared start is
  strictly ahead of the unissued pending value, uniform for
  `is_called=0/1`; a start at or behind the pending value keeps it
  (created `START WITH 10` never issued + `ALTER … START WITH 5` ⇒ next
  value 10; descending mirror: pending 10 + `START WITH 15` ⇒ 10,
  `START WITH 5` ⇒ restart to exactly 5). Single exception: an uncalled
  pending value outside the effective final bounds restarts at the declared
  start, for CYCLE and NO CYCLE alike, with the declared start validated
  atomically. Section 8 and the public projection are aligned to this text;
  the BVT gains behind-start uncalled pins in both directions with
  fresh-session assertions; the pinned UT matrix
  (`alter_sequence_state_test.go:213-224`) already encodes this rule and
  stays authoritative.
- Accepted (new in v18): **progress-renewed retry window** (D1) — the v17
  fixed wall-clock D is measured unattainable together with v13 I7's
  zero-client-error drain (§6 v18 evidence): any sustained herd whose
  serialized drain exceeds D terminally errors admitted statements no
  matter how large the fixed window, while the base drains the same herd
  through its unbounded in-statement loop bounded only by the session
  statement deadline. D keeps v15's lazy creation point, precedence, and
  sticky identity (now carried across rebuilds, C1) and becomes
  progress-renewed — v21 per Z1–Z2 (refining v20 per A1–A4, itself
  refining v19 per B1–B4): the
  progress marker is the contended row's complete committed tuple
  `(relation id, generation, is_called, last_seq_num)`; an identical
  tuple is no-progress (the complete-cycle ABA shape is an accepted
  quantization limit pinned by deterministic full-wrap and
  genuinely-unchanged cases), a changed tuple is renew-eligible once per
  observation (direction-consistent emission or reconfiguration —
  SETVAL/ALTER/generation change; a simple backward transition is never
  wrap evidence). The statement keeps a retained contention set of
  unresolved identities (the watch set, A1): every entry is created at
  admission/conversion CARRYING its baseline sample (the lazy
  next-probe wording is removed, Z2) — a failed creation sample records
  the entry young and retries inside the next decision's single probe
  round — entries survive arbitration consumption,
  rebuilds and generations, and are removed exactly when their
  contention observably ends; each entry is terminal-eligible only one
  full renewal interval after its own baseline (or creation while
  un-baselined, which then fails safe), and young identities vote
  neither terminal nor renewal, so a late join can never terminal
  before observing a full no-progress interval and a fresh baseline can
  never renew a stalled co-identity. Every expiry decision — the arbitration
  seam, the zero-attempt entry, the conversion pre-filter, and the
  in-flight budget-expiry route (A2: a D-derived attempt cancellation is
  routed INTO this decision instead of failing the transient classifier;
  caller-cancellation precedence and unrelated immediate-return terminal
  errors unchanged) — runs ONE bounded probe round over the watch set
  and terminates only on committed-unchanged AND unavailable (A3): each
  unchanged identity passes a bounded lock-acquisition check, an
  acquired identity resolves, and a resolved-or-advanced set renews D
  while unresolved contention remains — or CLOSES the episode (Z1: D
  retired, the statement returns to the base per-invocation budget, a
  later conflict re-admits fresh) when the last entry resolves; a
  completed herd never false-terminals, while a held
  uncommitted row lock yields exactly one no-progress terminal after
  one full interval measured from that identity's own baseline
  (Z2). Pending payloads form a per-`(relation id, generation)`
  map consumed as a unit with conjunctive renewal (B3); a declined
  verdict surfaces the first-recorded entry's named terminal. The live
  oracle is the sustained-no-progress blocker (exactly one named
  terminal, cleanup verified, then release and successful fresh
  allocation), with producer-stop exercised as the successful-drain
  control across the discriminant. Boundedness is unchanged in kind:
  the session statement deadline — the same ceiling that bounds the
  base's own retry loop — and with no timeout configured the effective
  ceiling is one full interval ending with some contended row
  committed-unchanged AND unavailable, the exact condition under which
  base also makes no progress on that row. The earlier review's C1–C4
  are implementation conformance with no contract change (retry-state
  carry-over across rebuilds with a `Compile.Run` identity regression;
  forward-only obligation floor after statement rewind with
  failed-SETVAL/no-prior-obligation oracles both directions and
  post-COMMIT continuation; sequence-metadata-domain merge watermark
  comparison with a signed-64-bit-boundary VARCHAR merge regression;
  executed `-race`-clean barrier integration proof through real
  producer, publication and `Compile.Run` paths). The C1–C4
  implementation and proof are complete and preserved; the D1 semantics
  (v20) are submitted for independent design review before
  implementation. Validation oracles: fake-clock marker/decision matrix
  (strict advance, boundary wrap signature, reconfiguration renew-once,
  identical-tuple full-wrap row-contended and genuinely-unchanged-locked
  terminal, generation re-baseline, chained renewals, probe failure;
  the young-identity matrix (Z2): D−ε join first-sample renewal (never
  terminal), the subsequent static-and-held interval terminals exactly
  once, the cannot-renew-A control, initialization-failure retry, and
  never-baselined-one-full-interval fail-safe);
  fake-clock expiry-ordering `Compile.Run` cases (progress-before-D
  crossing D at the in-flight conversion — through ACTUAL derived-
  context cancellation, not an injected retry-class error — and at the
  post-rebuild entry; static-locked terminal exactly once; caller
  cancellation; C1 identity preservation; unchanged-but-available
  resolution and completion; producer-stop crossing D completes; the
  Z1 closed-episode cases: admitted ⇒ successful allocation ⇒ empty
  set ⇒ process past the retired D ⇒ next allocation succeeds on a
  fresh per-invocation budget; re-contention re-admits with a fresh D
  and baseline; caller cancellation after closure propagates unchanged;
  an all-resolved round retires instead of renewing); the
  two-sequence conjunctive-renewal barrier case with the
  successive-generations arm; `-race` re-ladder; live uniq16, the
  sustained-no-progress blocker control, the successful-drain control,
  and the §6 gate re-run under the v21 rule (zero new client-visible
  errors wherever base completes; ≤3× wall clock stands).
- Rejected (new in v12): unbounded retry (caller statements must stay
  deadline-bounded), changing RC conflict semantics or adding wait-retry
  conversion inside the engine to avoid retrying (semantic changes far outside
  this scope), and restructuring the attempt to write-first blind increment to
  avoid re-read invalidation (numeric overflow/CYCLE boundary handling across
  the type matrix is out of proportion to the gain; the calibrated bound
  preserves the attempt's exact pipeline). (v13: upheld — tier 2 keeps every
  caller statement bounded by the sticky deadline; none of the rejected
  in-attempt remedies is used.) (v18: upheld — D1 renewal never exceeds
  the session statement deadline, the same ceiling that bounds base's own
  loop; the in-attempt remedies remain unused.) (v19: upheld — the
  probe-first expiry ordering and conjunctive renewal keep the same
  ceilings: the session statement deadline, or one full interval with
  zero observable committed change on some contended row.) (v20: upheld
  — the retained watch set, availability discriminant and cancellation
  routing keep the same ceilings: the session statement deadline, or one
  full interval ending with a committed-unchanged AND unavailable
  contended row; no in-attempt remedy from the v12 rejection list is
  used.) (v21: upheld — episode closure keeps a closed statement under
  base's own per-invocation ceilings, and terminal eligibility keeps an
  open episode under the same two ceilings: the session statement
  deadline, or one full interval measured from the identity's own
  baseline ending with that row committed-unchanged AND unavailable;
  no in-attempt remedy from the v12 rejection list is used.)
- Rejected (new in v10): repairing the merge machinery's snapshot
  catalog probe to read the caller's uncommitted database (RYOW) so the
  SQL-level preserve oracle becomes reachable — genuine cross-cutting
  change outside the assigned four-defect scope, with lock/snapshot
  semantics of its own; the UT-discriminated contract already covers the
  reachable behavior, and a later RYOW fix can promote the oracles without
  touching this design. Also rejected: narrowing the preserve-or-restore
  stamp to restore-only on transaction-local generations (would recreate
  the v7 regression the moment the merge machinery becomes RYOW-correct,
  with no added safety today — the preserve arm is semantically required by
  the same composition rule regardless of reachability).
- Accepted (new in v8): **preserve-or-restore commit stamp** (I1) — the
  pre-commit stamp restores `(floor, is_called=true)` only when the
  generation's surviving commit-time position is unsafe for the final pass
  (ascending: below the floor or equal-uncalled; descending: above the floor
  or equal-uncalled — the rewind cases) and preserves any surviving
  at-or-beyond position unchanged, so transaction-local merge reconciliation
  committed in the same transaction is never stamped back over merged rows;
  CYCLE-pass handling is explicit (an ascending beyond-floor position —
  reconcile advance or older-pass rewind — is safe to preserve, re-entry only
  via deliberate wrap; the descending arm restores because the continuation
  would silently walk through issued pass values).
- Rejected (new in v8): keeping v7's unconditional-by-obligation floor restore
  (regresses a surviving reconcile watermark below merged keys — the reported
  defect); never stamping while an obligation exists (loses the rewind
  restoration the stamp exists for — rewound emitted values would re-issue);
  mirroring reconcile watermarks into the obligation floor (redundant and
  wrong-shaped: the reconcile advance already lives in the same committed
  workspace position the stamp evaluates, and the obligation must stay
  allocation-scoped for the J1 pass semantics and the J2 boundary gate).
- Rejected (new in v7): suppressing the wrapped start-side emission after a
  rewind to dodge re-issue (diverges from base CYCLE behavior and the
  `100,60,100` pin; the wrap is a deliberate boundary, not a defect);
  numeric-monotonic stamp predicates across CYCLE passes (a wrapped pass floor
  can sit behind a rewound workspace value, so the unconditional-by-obligation
  restore with a no-op elision is the correct form); reseeding the floor from
  the repositioned state on SETVAL (proven to force 63 over the pinned 60 and
  to stamp an un-issued state called); keying obligations by sequence name (a
  drop+recreate replacement could inherit the old obligation — id-keyed
  entries plus commit-time resolution prevent it); deletion-time pending-retire
  flags in session state (statement rollback of the DROP would strand the
  flag — commit-time existence evaluation through the txn's final catalog view
  is authoritative).
- Rejected (carried): A1; B2; C2; D2; D3; blanket in-caller fallback for explicit
  transactions (v3, retracted at M1); in-caller allocation for *committed*
  generations under any caller mode (retired again with v5 routing — the L1
  route selects in-caller only where autonomy is impossible); session registry
  (v3, retracted at M1/M3 — the K1 floor is not a resurrection: it is bounded,
  txn-owned, stamp-consumed, and terminally cleaned); fate-sharing as the
  non-reuse argument on transaction-local generations (v5, retracted at K1 —
  nonfatal statement errors roll back only the statement, so a surviving
  generation could re-issue rewound values); validate-the-full-set-after-
  acquiring-it (v5 L2, retracted at K2); deadlock
  detection/timeout as the multi-sequence ordering mechanism (v4, replaced by
  canonical-order prevention at L2); changing transaction error classification
  or statement-rollback behavior to make v5's premise true (would alter
  existing error semantics, which K1 preserves); autonomous post-commit
  reconciliation (success would no longer guarantee I2).
- Open (non-blocking, owner: implementation): exact wording of the reconcile
  range-guard error; same-txn re-lock compatibility check for the nested DROP
  under the pre-held exclusive (fallback: engine-API delete under the held lock).
