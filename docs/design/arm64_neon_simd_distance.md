# Design: arm64 NEON SIMD vector-distance kernels

- Issue: #29495 · PR: #29496
- Author: cpegeric

### Status (by revision)

- **v1 — Approved.** Scope: the core arm64 NEON kernels for the full metric set and
  the build/delivery contract (§1, §3–§6). Approved by fengttt as the implementation
  at PR revision 2b3efca (2026-09-30); this document (first added at a964312) records
  that approved design.
- **v2 — Pending design review.** Scope: the §2 numerical/selection contract,
  corrected to the **NaN→+Inf** rule in response to the #29496 review, plus the
  static-analysis / Go 1.27 delivery additions (§5–§6) introduced with this document.
  These post-date fengttt's v1 approval. XuPeng-SH requested a versioned,
  design-first review per `.agents/skills/mo-dev/references/feature-design-review.md`;
  update to "v2 — Approved by &lt;name&gt; (revision &lt;sha&gt;, &lt;date&gt;)" once that review lands.

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
  cancels them (a sequential scalar sum would stay finite). NaN is **unordered**,
  so it corrupts a top-k ranking: it never compares as the maximum a heap evicts,
  so it is retained in a slot and discards a genuinely-nearer finite candidate — a
  silent wrong result no final-score check can catch, because the returned winner's
  score is a finite value. Therefore **every distance function maps NaN to +Inf**
  (the largest distance): the overflowing candidate ranks last / is excluded, and
  every finite candidate ranks correctly. A distance function never returns NaN.
- A genuine ±Inf — a non-cancelling overflow (a huge-similarity dot → −Inf, a
  squared magnitude → +Inf) — is already well-ordered and is left as is. Only the
  unordered NaN is mapped.
- The map is applied **inside each distance function** that can produce NaN: inner
  product (the f32/f64 `InnerProduct[T]` entry, and every narrow bf16/f16 tier
  kernel — scalar/NEON/AVX-512/AVX2, which share no finalizer), cosine (the single
  shared `cosineDistClamped` finalizer, covering all tiers), and spherical (its
  `acos` result). L2/L2sq/L1 are non-negative sums and cannot produce NaN;
  cosine-similarity f32/f64 has its own normal-norm recompute. The map is a
  single-constant sentinel, not a scalar recompute — it costs one NaN test, keeps
  search selection correct, and needs no second pass.
- **Finiteness-as-an-error is still enforced once, at the consumer score boundary**
  — the scalar/SQL array-distance builtins (`moarray`, `arrayDistanceNarrow`) and
  index Search (`CheckFiniteDists`). A non-finite result (now ±Inf, never NaN)
  handed back there is reported as an overflow error; inside search ranking it is
  simply ordered last.
- A result must not depend on *which* kernel ran. For one input, every SIMD tier
  and architecture agrees with the scalar reference up to ordinary FP rounding, and
  a non-finite case is the same well-ordered ±Inf (never NaN) everywhere.

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

- Narrow float kernels accumulate in **float32**, not a wider type. A wider
  accumulator is rejected: it does not actually remove overflow (an input can be
  built at any accumulation type's limit) and it halves the usable SIMD lane
  width, erasing the throughput the feature exists to deliver. The residual
  overflow case is handled by the §2 boundary contract, not by widening.
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

- No distance function returns NaN: a NaN is mapped to +Inf; a genuine ±Inf is left
  well-ordered. The map is inside each NaN-capable function (inner product, cosine's
  `cosineDistClamped`, spherical), never a scalar recompute.
- Search/selection relies on well-ordered distances: the overflow candidate ranks
  last (never a wrong winner). Finiteness-as-an-error is enforced only at the
  consumer score boundary.
- Scalar oracle ≡ every SIMD tier ≡ every architecture on finite inputs; a
  non-finite case is the same well-ordered ±Inf (never NaN) everywhere, and the same
  error at the same boundary when handed back as a score.
- No architecture branch outside build-tagged kernel files.
- Build-experiment default on + opt-out honored; capability-gated paths fail
  closed.
- Minimum toolchain is Go 1.27 across build/CI/images; an older toolchain fails
  the build rather than falling back silently.
- The static-check gate stays fail-closed and un-scoped: lint findings surfaced
  by the toolchain move are fixed (or narrowly suppressed with a reason), never
  waved through by disabling a linter or relaxing the gate.
