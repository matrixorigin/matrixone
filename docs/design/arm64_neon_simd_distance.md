# Design: arm64 NEON SIMD vector-distance kernels

- Issue: #29495 · PR: #29496
- Author: cpegeric

### Status (by revision)

- **v1 — Approved.** Scope: the core arm64 NEON kernels for the full metric set and
  the build/delivery contract (§1, §3–§6). Approved by fengttt as the implementation
  at PR revision 2b3efca (2026-09-30); this document (first added at a964312) records
  that approved design.
- **v2 — Approved by fengttt (revision af0eb99, 2026-10-01).** Scope: the §2
  numerical/selection contract (the **NaN→+Inf** rule) and the static-analysis / Go
  1.27 delivery additions (§5–§6) introduced with this document; these post-date the
  v1 approval. fengttt approved this design at PR #29496 revision af0eb99 (2026-10-01
  19:21 UTC). XuPeng-SH's change requests on the same revision are tracked in the PR
  thread.
- **v3 — Pending design review.** Scope: the §2 numerical contract changes from
  "NaN→+Inf, divergence accepted by design" (v2) to **recover the in-order reference at
  the metric owner** on a non-finite SIMD result, per XuPeng-SH's review. This covers
  inner product, spherical, **and narrow bf16/f16 cosine** (fast-fail genuine overflow),
  so the SIMD and scalar paths now **agree** on the cancellation input instead of
  diverging. The recompute runs in the kernel's own accumulation type (float32 for
  f32/bf16/f16, float64 for f64) via the source-order reference, not unconditionally in
  float64. It reuses the existing scalar reference / `cosineRecomputeF64` exception and is
  free on the **common** fast path — the O(d) second pass runs on any non-finite SIMD
  result, which includes finite extreme-magnitude inputs, so a finite-data workload is not
  exempt (§2 Cost). A genuine overflow fast-fails to a well-ordered ±Inf of the metric's
  own sign (only NaN is normalized), caught by the sign-agnostic serve boundary. The v1
  scope (§3–§6 kernels/build) is unaffected.

## 1. Context & goal

Vector distance functions had SIMD kernels only on amd64; on arm64 every distance
fell back to pure Go, for both float (f32/f64) and narrow (bf16/f16/int8/uint8)
vectors. The goal is first-class arm64 NEON kernels for the full metric set (L2,
L2sq, L1, inner product, cosine, spherical), selected by the **same build
contract as amd64**, with **identical observable results**.

## 2. Numerical / result contract (the central decision)

- A distance kernel computes in IEEE-754. A multi-lane SIMD accumulation can
  produce **NaN** on an extreme input whose products are each finite but whose
  per-lane partial sums reach +Inf and −Inf before the cross-lane reduction
  cancels them, where a **sequential (in-order) scalar sum cancels and stays
  finite**. NaN is **unordered**, so it corrupts a top-k ranking: it never compares
  as the maximum a heap evicts, so it is retained in a slot and discards a
  genuinely-nearer finite candidate — a silent wrong result no final-score check can
  catch, because the returned winner's score is a finite value. A distance function
  never returns NaN.
- **Resolution — recover the in-order reference at the metric owner.** The ordinary
  fast path is the SIMD kernel, unchanged. Only when its result is **non-finite** does
  the metric owner take an exceptional path: recompute via the **source-order reference**
  in the kernel's own accumulation type — float32 for f32/bf16/f16, float64 for f64 —
  and return it. (This is the type the parallel lanes diverge from; it is NOT an
  unconditional float64 recompute.) So the SIMD result agrees with the scalar oracle on
  the cancellation input: inner product → 0, spherical → acos(0)/π = 0.5, cosine → 1.
  This mirrors the cosine kernel's existing `cosineRecomputeF64` exception, which this
  package has shipped since the float32-domain standardization (#29040/#29050/#29100).
- **Fast-fail genuine overflow.** When the in-order reference also leaves the element
  domain, the overflow is **genuine** — not a lane-ordering artifact — so the recompute
  returns a well-ordered ±Inf that the serve boundary rejects. The sign is the metric's
  own: inner product (distance = −dot) can return −Inf; the non-negative metrics
  (L2/L2sq/L1) and the bounded ones (cosine/spherical) return +Inf. The result is **not**
  sign-normalized — only NaN is (see the finiteness-boundary paragraph below).
- The recompute is applied at the **metric owner**, reusing the existing scalar reference
  kernels rather than a second algorithm:
  - **inner product** — `InnerProduct[T]` (via `InnerProductUnrolled`, summed in T) and
    every narrow bf16/f16 tier kernel (via `innerProductBF16`/`innerProductF16`), across
    scalar/NEON/AVX-512/AVX2;
  - **spherical** — recompute the dot, clamp, re-derive `acos/π`;
  - **cosine** — f32/f64 recover via the existing f64 **norm** recompute
    (`cosineNormsOK`/`cosineRecomputeF64`); the narrow bf16/f16 tiers recover on a
    non-finite result via the scalar `cosineDistanceBF16`/`cosineDistanceF16` reference
    (bf16 products are each finite but a lane overflows before cancellation; f16 cannot
    overflow float32, so its recover is a no-op guard).
  - L2/L2sq/L1 are non-negative sums and cannot produce NaN; int8/uint8 accumulate in
    integers and cannot either.
- **Cost.** The exceptional recompute adds nothing to the **common** fast path: a finite
  SIMD result pays only one finiteness test (what the prior +Inf sentinel already paid).
  The O(d) in-order second pass runs on any input whose SIMD result is **non-finite** —
  and that includes **finite** stored vectors at extreme magnitude whose lanes overflow
  or cancel before the cross-lane reduction (the case this fix targets and the
  counterexample tests exercise), not only genuine overflow. A finite-data workload is
  therefore **not** exempt: a query against such a vector pays one extra O(d) pass on that
  pair — bounded at ~2× the kernel cost, incurred only on the pairs that actually go
  non-finite, and never on the ordinary finite case. This is still distinct from
  **always** widening the accumulator to float64, which measured ~2.5× slower on *every*
  pair (and still cannot fix f64 — no f128 lane); see §5.
- **Finiteness-as-an-error is still enforced once, at the consumer score boundary**
  — the scalar/SQL array-distance builtins (`moarray`, `arrayDistanceNarrow`), index
  Search (`CheckFiniteDists`), and ivfflat's `HasFloat64DistanceOverflow`. Every one of
  these rejects a **non-finite of either sign** (the test is `d-d != 0` / `math.IsInf(_, 0)`),
  so a genuine ±Inf is reported as an overflow error regardless of sign. Only **NaN** is
  rewritten (to +Inf) before ranking, and only because NaN is **unordered**: it is never
  the maximum a top-k heap evicts, so it is retained in a slot and silently drops a nearer
  finite candidate. A genuine ±Inf is **well-ordered** and cannot cause that silent
  lost-winner, so it keeps its own sign and is caught sign-agnostically at the boundary;
  normalizing its sign would be unnecessary and would mis-rank a genuine inner-product
  overflow (a maximal dot) as the farthest instead of letting the boundary reject it.
- **Equivalence.** On finite inputs every SIMD tier and architecture agrees with the
  scalar reference up to ordinary floating-point rounding; on the degenerate
  cancellation input they now **also** agree, because both yield the in-order reference
  (the fast path recovers it, the scalar path computes it directly). A genuine overflow
  is the same well-ordered ±Inf everywhere and the same error at the same boundary. Tests
  assert this equivalence in both the default and the SIMD-opt-out modes.

## 3. Ownership: one scalar oracle, per-arch SIMD

- Each metric has a single **scalar reference** implementation that is the
  correctness oracle and the fallback when SIMD is disabled.
- SIMD kernels are **architecture-specific** (amd64 vector tiers; arm64 NEON) and
  live behind build tags. They must match the scalar oracle on finite inputs up
  to ordinary floating-point rounding; equivalence is asserted by tests that run
  the same inputs through both.
- Orchestration that is **not** architecture-specific (dispatch, metric
  selection, result shaping, the boundary finiteness checks) is shared and must
  not branch on architecture.

## 4. Architecture split

- amd64 and arm64 expose the **same function surface and semantics** (error
  conditions, zero-magnitude conventions, clamps); only the vector width and the
  number of accumulator chains differ, tuned per micro-architecture.
- Narrow types decode to float32 and the kernel accumulates in float32; integer
  narrow types accumulate in a wider integer.

## 5. Precision / performance tradeoffs

- Float kernels (narrow and real f32) accumulate in **float32**, not a wider type.
  A wider accumulator is rejected: it does not actually remove overflow (an input can
  be built at any accumulation type's limit — f64 inputs overflow an f64 accumulator,
  and there is no f128 lane) and it halves the usable SIMD lane width, erasing the
  throughput the feature exists to deliver. Measured (NEON, 768-D inner product): an
  f64-accumulator kernel is ~2.5× slower than the f32 kernel and only ~1.1× faster
  than the pre-SIMD scalar loop, versus ~2.8× for the f32 kernel — it gives back
  almost the entire SIMD gain. The residual non-finite case is handled by the §2
  **exceptional recompute** (recover the in-order reference in the kernel's native
  accumulation type — float32 for f32/bf16/f16, float64 for f64 — only when the SIMD
  result is non-finite; fast-fail genuine overflow), which runs only on the affected
  pairs (§2 Cost) — not by widening every accumulation.
- f16 cannot overflow the float32 accumulator (its magnitude range is small);
  bf16 shares float32's exponent range and therefore can — the §2 contract covers
  it.
- No diagnostics or per-distance branches are added to the per-element loop.

## 6. Delivery / build contract

- SIMD is gated by the vector-intrinsics build experiment and is the **default**
  on supported architectures, with a documented **opt-out** that forces the
  scalar path (used for coverage and incident mitigation).
- **Toolchain move (Go 1.27).** The arm64 vector intrinsics only exist in the Go
  toolchain's SIMD experiment at this release, so the minimum supported Go version
  moves to 1.27 and the build/CI/base images move with it. This is a **hard
  build-compatibility change**, not a silent default: downstream builders adopt
  the new toolchain deliberately, and a build on an older toolchain fails rather
  than quietly dropping to the scalar path. The amd64 kernels are brought onto the
  same toolchain so one build contract covers both architectures.
- **Static-analysis fallout is in-scope cleanup, not a relaxed gate.** Moving the
  toolchain forward also moves the static-analysis toolchain forward (it must
  understand the new compiler's export data and not crash on the new standard
  library). The newer analyzers surface **pre-existing, latent** findings across
  the repository that the old toolchain never reported. Decision: **resolve those
  findings in place**; where a finding is a deliberate exception, suppress it
  narrowly with a recorded reason. The repo's fail-closed static-check gate is
  **not** disabled, downgraded, or scoped-down to absorb the upgrade — the gate
  keeps the same meaning before and after.
- GPU and narrow-type paths register under the existing capability gating; a
  build without a capability fails closed rather than silently falling back.

## 7. Invariants (review checklist)

- No distance function returns NaN. On a non-finite SIMD result the metric owner
  recomputes via the source-order reference in the kernel's own accumulation type
  (float32 for f32/bf16/f16, float64 for f64) — inner product (`InnerProduct[T]` +
  narrow bf16/f16 kernels), spherical (`acos`), and cosine (f32/f64 via the f64 norm
  recompute, narrow bf16/f16 via the scalar `cosineDistanceBF16`/`F16` reference). A
  genuine overflow — one the reference cannot represent either — fast-fails to a
  well-ordered ±Inf (the metric's own sign; only NaN is normalized, see §2).
- Scalar oracle ≡ every SIMD tier ≡ every architecture: on finite inputs up to FP
  rounding, AND on the degenerate cancellation input, where both yield the same
  in-order reference (SIMD recovers it, scalar computes it directly). A genuine
  overflow is the same well-ordered ±Inf everywhere. The SIMD contract test runs the SIMD
  kernels directly (narrow by name; real kernels with the tier flag forced on), not
  through the resolver, so it asserts the recovered value regardless of the opt-out override.
- The recompute is free on the **common** fast path: a finite SIMD result pays one
  finiteness test. The O(d) second pass runs on any input whose SIMD result is
  non-finite — including **finite** extreme-magnitude vectors, not only genuine overflow —
  so a finite-data workload pays it on exactly those pairs (bounded ~2×, §2 Cost), never
  on the ordinary case. Never widen every accumulation (§5).
- Finiteness-as-an-error is enforced at the consumer serve boundary
  (`CheckFiniteDists`, ivfflat `HasFloat64DistanceOverflow`), whose finiteness test is
  **sign-agnostic**; a genuine non-finite result of either sign is reported as an overflow
  error there. Only NaN is sign-normalized (to +Inf) before ranking; a well-ordered ±Inf
  is not (§2).
- No architecture branch outside build-tagged kernel files.
- Build-experiment default on + opt-out honored; capability-gated paths fail
  closed.
- Minimum toolchain is Go 1.27 across build/CI/images; an older toolchain fails
  the build rather than falling back silently.
- The static-check gate stays fail-closed and un-scoped: lint findings surfaced
  by the toolchain move are fixed (or narrowly suppressed with a reason), never
  waved through by disabling a linter or relaxing the gate.
