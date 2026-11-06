# Decimal expression SIMD

Owner: #30860. The user approved this expanded two-PR plan on2026-11-05.
This PR owns expression kernels; an independent PR owns aggregate reductions.
#32175 is merged. Arithmetic and aggregation are validated against clean main
independently; neither PR depends on the other. Risk R3: exact hot arithmetic.

## Contract and consumers

Keep SQL types, scales, precision, implicit-cast proofs and SUM/AVG accumulators.
Keep original expression allocation/reset/free ownership. New helpers borrow
input views synchronously and retain no references, buffers or state. No new
CGo, big.Int, unsafe import, cache, runtime configuration, wire or disk format.
The existing bounded chain remains four nodes/five leaves with256-row chunks
and at most24KiB scratch. No additional expression layer or register-chain
fusion is introduced.

Ordinary unscaled Decimal64 multiplication, fused x*(c-y), and lossless
same-scale Decimal64-to-Decimal128 casts use private slice kernels. Ordinary
checked Decimal64 add/sub retains its scalar implementation after qualification
rejected those candidates. Native Decimal128 add/sub now has the checked-prefix
revision below, with performance qualification still required. Prepared aggregate arguments use
their existing admission/proof boundary before optional SIMD dispatch. They
support D64 add/sub to64/128, D128 add/sub to128/256, and D64xD64 orD128xD64
multiplication to128/256. Unsupported shapes and scale changes remain scalar.

## Exact kernels

Go1.27 goexperiment.simd/amd64 helpers check runtime AVX2; other builds use
scalar stubs. Minimum lengths are operation-specific and qualified over32/64/128/256 candidates; scalar tails. Flat non-NULL operands and
constant broadcasts are supported. Existing CASE selection/packing owns active rows; NULL/grouping fallbacks remain scalar.
Earlier checked ordinary candidates were dropped under the approved gate;
their qualification decisions remain below alongside the native128 revision.

Prepared add/sub uses original per-node precision proofs; wide output retains
the exact extra sign/carry limb. D64-output kernels omit unused high limbs.
Multiplication chooses physical width, result width and broadcast orientation
once. It vectorizes only coefficients proved signed32, with canonical high
limbs for D128 inputs. Constants are checked once. First-block refusal leaves
output untouched; later refusal completes the untouched suffix through the
original exact scalar span, without replaying the prefix. Products retain their
declared carriers. Do not remove necessary Decimal256 results or accumulators.

The2026-11-05 kernel correction retains those admitted arithmetic families.
Signed64 vector shifts require AVX-512 and cannot be used under an AVX2 guard.
Sign extension instead uses AVX2 signed comparisons with zero. The signed32
coefficient test adds2^31 modulo2^64 and tests whether the upper32 bits are
zero, including the signed64 endpoints. Compiled-kernel instruction inspection
must reject EVEX/AVX-512 instructions, in addition to arithmetic boundary tests.

Widened add/sub derives the extra sign limb from the operands' signs and the
wrapped result. For addition the sign is the majority of the two operand signs
and result sign; subtraction complements the second operand sign. Equal signs
retain their sign, while opposite signs use the wrapped result's sign. This
preserves all65/129 significant bits while avoiding redundant upper carries.
Multiplication establishes its checked input/output slice views once before
the loop, retaining original row offsets for scalar-tail/refusal handling.

## Validation and delivery

Independent exact boundary tests cover carry/borrow, all signed64 product
endpoints, 257/263-row tails, offsets, broadcasts and source immutability. Prepared full-width tests compare128/256 carriers with unchanged
scalar arithmetic. Existing Group fixtures prove real argument activation,
metadata and lifecycle. The existing expression SQL case adds257-row native
arithmetic and nullable fallbacks; normal mo-tester comparison is separate
from timing. No data/result verification is performed in benchmark scripts.

Component controls include dispatch, guards, packing, tiny inputs and wide
fallbacks, with the production64-byte function alignment. Test families
independently before final full Q1-Q22 acceptance. First cold suite is excluded
per independent startup. Report wall/CPU per query and Total, plus separate
instruction profiles and actual SIMD activation. No confirmed query/control/
Total regression; component speedup alone is insufficient. Keep the outstanding
Q4 disk signal visible until equivalent full-suite evidence clears it.

Local SF10 uses all32 CPUs,48GiB application/16GiB cache, RAM and disk modes.
SF100 on a quiet server129 is also mandatory, all64 CPUs/interleaved NUMA with
the established216GiB/192GiB RAM and84GiB/16GiB disk protocols. No spill/swap/
OOM/interference is accepted. Reuse loaded data; no transfer or reload.
Templated draft PR only after local acceptance; readiness also requires SF100.
Fresh main, pre-push gate and self-review are required before publication.

## Approved native/fused coverage revision,2026-11-06

The requester approved this revision with “Implement the plan” after selecting
Target-query gain: at least one of Q5/Q9/Q14 must have a confirmed wall-time gain
and fewer retired instructions, with no confirmed regression across all22.
This replaces the initial native-D64 exclusion and blanket256 minimum; the
bounded chain's256-row chunks and24KiB scratch limit do not change.

First vectorize the existing decimalMulSub64 coefficient loop directly. Compute
x*(c-y) in registers; reject a block if c-y overflows signed64 or either x or
the exact difference exceeds signed32. AVX2 signed32 products fit signed64 and
are sign-extended into the existing Decimal128 output. Four-row admission is
transactional, and only the untouched suffix resumes the retained total scalar
loop, including65-bit differences. No new intermediate vector exists.

Then connect private slice kernels to ordinary checked same-scale Decimal64
add/sub and unscaled Decimal64 multiplication. Exact aliases/broadcasts and
publication through the first failing row retain the current semantics. Reuse
these kernels from prepared wrappers; do not synthesize vectors or add dispatch
state. Preserve EvalCase/evalSelectedRows: arithmetic consumes their existing
compact selected vectors, and skipped branches cannot produce new diagnostics.

Vector sign extension accelerates only existing lossless, same-scale64-to128
casts. Existing result allocation/NULL metadata, narrowing, rescaling and
precision validation keep their owners. No new cast, Decimal256 promotion,
public API, production big.Int, unsafe import, CGo dependency or cache exists.

Threshold qualification uses fixed five-pair dispatched controls at32/64/128/256:
select the smallest candidate faster at T,2T,8192 and without a slower nearby,
NULL, wide-refusal, alias or tiny control. Reject a new family if none qualifies.
All authoritative length checks follow that operation's threshold. A temporary
profile-only probe establishes actual Q5/Q9/Q14 calls/rows/refusals and is absent
from timed and delivered production code.

Retain every point/raw interval. Total keeps the unadjusted95% primary gate;
query regressions use Holm one-sided alpha0.025 across22. Target expression
speedups use Holm alpha0.025 across the three predeclared targets, with lower
instructions in both alternating diagnostic pairs for a qualifying query.
Component results do not substitute for the full22 local/SF100 protocol.
Draft updates are permitted with open performance gates stated explicitly;
readiness still requires complete accepted performance. No historical24257
percentage is a promised gain. Source builds may proceed while machines are
busy; no timing is queued into competing jobs and original datasets/seeds stay.

## Qualification decisions, 2026-11-06

Five alternating current-main component pairs reject ordinary checked D128
arithmetic: subtraction is slower at all measured minima and8192; addition has
no confirmed win at any allowed minimum. Drop that entire optional family and
its dispatch rather than retain it behind a high threshold. Its ordinary scalar
owners and error semantics stay intact; prepared128 add/sub remains separate.
The32-row cast and checked64 minima also fail their controls; measure64 next.
Initial fused/multiply wide-first refusal costs are rejected, so make the first
coefficient check before entering their SIMD frame and retain zero-based scalar
suffix loops. These are qualification outcomes under the approved drop rule.

The64-row checked D64 kernels win on large arrays but still add a confirmed
1–3% to tiny/NULL public-owner controls. No permitted minimum removes that
entry cost. Apply the same approved drop rule to checked D64 add/sub; preserve
the exact pre-existing scalar functions. Retain their paired rejection results.
The fused owner also keeps its original scalar leaf byte-for-byte: optional
SIMD admission belongs in the vector wrapper, avoiding altered scalar loop
bounds and live registers. Cast fallback keeps its original scalar loop too.

### Scalar dispatch-cost closure

The total scalar signed64 product can use unsigned Mul64 followed by high-limb
correction instead of magnitude conversion and conditional negation. For
u=x+sx*2^64 and v=y+sy*2^64 (sx/sy are sign bits), signed product modulo2^128
is u*v-(sx*v+sy*u)*2^64. Thus subtract (x>>63)&y and (y>>63)&x from the unsigned
high limb; the low limb is unchanged. This is exact for all signed64 inputs,
including MinInt64 squared, and preserves every carrier/error/publication
contract. Use this smaller shared scalar primitive to avoid making rejected,
nullable and tiny multiply calls pay for the new dispatch. Existing endpoint,
broadcast, rounding and independent fused big.Int models cover the closure.

For same-scale D64-to-D128 casts, canWidenDecimalScale is necessarily false
because it requires strictly greater target scale. Short-circuit that pure
predicate on unequal scales before calling it. Existing narrow-width parsing,
scale growth, NULL publication and explicit casts retain the same decisions.
This removes redundant setup rather than adding another cast path.

The remaining single-row NULL control performs no multiplication, so handle
that masked one-row vector directly, including one constant input and borrowed
validity. Publish its exact NULL bit or scalar result without batch bitmap
setup. The constant cast broadcast loop also bounds its destination slice once,
then ranges over it, removing a repeated per-row bounds check. This retains
the existing scalar constant fill rather than adding a SIMD kernel or copy.

Constant cast broadcasts fill four output rows per scalar chunk, with a tail of
at most three rows. This removes loop/index overhead and repeated bounds checks;
it adds no reads, allocation or SIMD family. Existing constant-batch tests add
four/five-row boundaries while retaining scale changes, narrowing and errors.
One-row fused broadcasts inline their ordinary exact64-bit difference/product;
the65-bit difference still uses the original total scalar owner.

## Native Decimal128 add/sub scope revision, 2026-11-06

The requester explicitly adds Decimal128 add/sub to this PR and defers other
new kernels. Retain archsimd. Earlier rejected checked-kernel measurements stay
historical evidence, and the new implementation must qualify independently.

The native same-scale owners also serve in-place scale alignment. Their slice
interface has no precision proof, so SQL type metadata alone cannot authorize
unchecked arithmetic here. Keep their API and full signed128 domain. Introduce
private AVX2 checked-prefix kernels for flat inputs and either constant operand,
using eight-row blocks with a combined overflow test before publishing stores.
On rejection, leave that block and its suffix untouched; the scalar owner
continues from the completed prefix and publishes only through its first
overflowing row. Cache broadcast values before any aliased result stores.
Existing allocation, NULL/selection handling, scale alignment and diagnostics
retain their owners. Actual callers provide disjoint or exact in-place spans.

Use the signed overflow identities on original high words and the computed
high result: add `(a^r)&^(a^b)`, subtract `(a^r)&(a^b)`. Their sign bit is the
overflow flag. The scalar continuation uses the same identities, eliminating
redundant sign extraction while preserving wrapped result and first-error
publication. Unsigned low-word carry/borrow uses sign-biased AVX2 comparisons;
no AVX-512 instructions, temporary result array, production big.Int or new API.

The first screen shows larger-array wins but rejects the added one-row entry
cost. A one-row add/sub uses direct NULL membership and the exact scalar
operation, retaining alias/overflow semantics without batch bitmap setup.
All scalar add/sub branches use the same original-high-word overflow identities,
including nullable branches, so a declined SIMD call adds no per-row arithmetic
work. This revision changes only the Decimal128 add/sub owners. Retain the first
screen alongside the final paired measurements rather than label it accepted.

The five-pair refined check finds one remaining scalar-control regression:
15-row constant-left subtraction +2.43% [1.86%,3.01%]. That input never reaches
SIMD. Specialize its scalar overflow predicate once on the constant's sign:
nonnegative-left subtraction overflows only when both subtrahend and result
are negative; negative-left subtraction overflows only when both are
nonnegative. This removes repeated XORs without weakening the full-range check.
Retain that failed comparison and requalify the changed path before acceptance.

Start qualification at128 rows; qualify the operation-specific minimum using
the existing32/64/128/256 candidates. Place admission only in existing non-NULL
branches so NULL/selected inputs retain their scalar path. Unsupported builds
use constant-refusal stubs. Preserve all pre-existing carrier/overflow checks.

Prove exact arithmetic with independent big.Int models, every overflow lane,
carry/borrow and signed128 endpoints, broadcasts, exact aliases, offsets,
minimum/tail boundaries, and untouched output suffixes. Exercise rescaling,
NULL/CASE selection and public plus/minus through their existing fixtures.
Benchmark against an independent pre-extension checkout before broader timing;
retain tiny/NULL/alias/refusal controls and every paired observation. No confirmed
regression is accepted. Full Q1-Q22 uses the already loaded local datasets, cold
exclusion and established no-spill/resource rules; the requester permits local
disk-backed SF100. Do not claim old-head timings qualify changed arithmetic.
