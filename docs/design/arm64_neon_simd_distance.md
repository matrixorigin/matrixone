# Design: arm64 NEON SIMD vector-distance kernels

- Issue: #29495 · PR: #29496
- Status: Approved by fengttt
- Author: cpegeric
- Reviewers: fengttt (approved PR #29496); XuPeng-SH (requested a versioned design review)

## 1. Context & goal

Vector distance functions had SIMD kernels only on amd64; on arm64 every distance
fell back to pure Go, for both float (f32/f64) and narrow (bf16/f16/int8/uint8)
vectors. The goal is first-class arm64 NEON kernels for the full metric set (L2,
L2sq, L1, inner product, cosine, spherical), selected by the **same build
contract as amd64**, with **identical observable results**.

## 2. Numerical / result contract (the central decision)

- A distance kernel computes in IEEE-754 and returns its **raw result, including
  ±Inf and NaN, with no error**. It never rejects or clamps a non-finite value.
  Rationale: the kernel is the innermost hot-path primitive (per element, per
  candidate); a per-call finiteness branch is overhead on the common path for a
  condition that stored data cannot produce (an indexed vector is never non-finite
  by construction). A non-finite result is the correct, information-preserving
  signal that an accumulation left the element domain.
- **Finiteness is enforced exactly once, at the boundary where a distance is
  handed back as a user-visible score** — the scalar distance builtins, the
  per-row vector functions, and the index-search result path. Overflow there is
  reported as an error, not returned as Inf/NaN.
- **Search / selection does not check.** A non-finite distance cannot win a
  nearest (min/argmin) comparison, so it never changes ranking; an
  all-out-of-domain query is caught by the no-winner guard. Centroid assignment
  and brute-force selection therefore intentionally pass non-finite distances
  through untouched.
- A result must not depend on *which* kernel ran. For one input, the scalar
  reference, every SIMD tier, and each architecture must agree — the same finite
  value, or the same error at the same boundary. Making an operand constant
  (which may select a batch kernel) or changing architecture must not change what
  a query returns.

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

- Kernels return raw non-finite values; no kernel rejects a non-finite result.
- Every user-score boundary rejects non-finite; search/selection does not.
- Scalar oracle ≡ every SIMD tier ≡ every architecture on finite inputs; same
  error at the same boundary on out-of-domain inputs.
- No architecture branch outside build-tagged kernel files.
- Build-experiment default on + opt-out honored; capability-gated paths fail
  closed.
- Minimum toolchain is Go 1.27 across build/CI/images; an older toolchain fails
  the build rather than falling back silently.
- The static-check gate stays fail-closed and un-scoped: lint findings surfaced
  by the toolchain move are fixed (or narrowly suppressed with a reason), never
  waved through by disabling a linter or relaxing the gate.
