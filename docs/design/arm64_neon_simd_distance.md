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
- **§2/§5/§7 editorial correction (post-af0eb99).** The contract wording was
  corrected to state the SIMD/scalar divergence on degenerate overflow inputs as an
  explicit **by-design** decision (one kernel per deployment → internally consistent;
  toy 2^63 input only; +Inf internal, error at the serve boundary), with the
  supporting benchmark (§2 reason 1 / §5). This aligns the document with the behavior
  of the already-approved code (af0eb99); it does not change the approved contract.

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
- **Finite inputs:** every SIMD tier and architecture agrees with the scalar
  reference up to ordinary floating-point rounding. This is the equivalence the
  tests assert.
- **Degenerate overflow inputs — paths may diverge, and this is not fixed, by
  design.** A SIMD kernel sums strided lanes, so a 2^63-magnitude input overflows a
  lane to ±Inf → NaN → +Inf; the scalar reference sums in source order, so the same
  input can cancel and stay finite. The divergence is accepted for three reasons:
  1. **The f32 accumulator is the point of SIMD.** Widening it to f64 to force
     agreement was measured (NEON, 768-D inner product) at ~2.5× slower than the f32
     kernel and only ~1.1× faster than the pre-SIMD scalar loop — versus ~2.8× for
     the f32 kernel — i.e. it hands back almost the entire SIMD gain, and f64 inputs
     still overflow (no wider lane to catch them), so it buys nothing there either.
  2. **One path runs per deployment, never mixed.** A given build uses either SIMD
     or the scalar oracle for every candidate of every query, so its ranking is
     internally self-consistent; the divergence is only observable across builds.
  3. **The input cannot arise from real data.** A 2^63-magnitude element is a
     toy/adversarial input; stored vectors are finite (#28688) and real embeddings
     are small-magnitude.
- **The +Inf is an internal, ranking-safe intermediate — a toy vector's final
  answer is an error.** Mapping NaN→+Inf only keeps internal selection well-ordered
  (brute_force centroid/candidate ranking, which has no serve-time check because it
  is internal, never retains a NaN). The user-facing serve boundary then rejects any
  non-finite result with an error (ivfflat's `HasFloat64DistanceOverflow`,
  hnsw/usearch `CheckFiniteDists`, GPU `CheckFiniteDists64`). So a toy vector is
  never *served* a +Inf distance: it is either excluded (ranked last behind finite
  candidates) or errored out at the boundary.

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
  almost the entire SIMD gain. The residual overflow case is handled by the §2
  boundary contract (NaN→+Inf internally, error at the serve boundary), not by
  widening.
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
- Scalar oracle ≡ every SIMD tier ≡ every architecture **on finite inputs** (up to
  FP rounding). On a degenerate overflow input the SIMD and scalar paths MAY diverge
  (SIMD → +Inf; the scalar oracle may cancel to a finite value) — accepted by design
  (§2): one path runs per deployment, so ranking stays internally consistent, and the
  toy input cannot arise from real finite data. The SIMD contract test therefore runs
  the SIMD kernels directly (narrow by name; real kernels with the tier flag forced
  on), not through the resolver, so it asserts the +Inf contract regardless of the
  opt-out override.
- A non-finite result is never NaN (always well-ordered +Inf) on every path. It is
  the internal, ranking-safe intermediate only: inside search it is ordered last, and
  at the consumer serve boundary it is rejected with an error. A toy vector is never
  served a +Inf distance.
- No architecture branch outside build-tagged kernel files.
- Build-experiment default on + opt-out honored; capability-gated paths fail
  closed.
- Minimum toolchain is Go 1.27 across build/CI/images; an older toolchain fails
  the build rather than falling back silently.
- The static-check gate stays fail-closed and un-scoped: lint findings surfaced
  by the toolchain move are fixed (or narrowly suppressed with a reason), never
  waved through by disabling a linter or relaxing the gate.
