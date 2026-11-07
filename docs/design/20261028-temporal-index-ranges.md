# Mixed DATETIME/TIMESTAMP candidate-range semantics

Owning issue: #31619 (correctness fixed by #31624); performance integration: #31944.
The type-specific access and admission contract is
[temporal CBO design](20261030-temporal-cbo.md); generic read, resolution,
execution and cache ownership is defined in
[index planning owners](20261107-index-planning-owners.md). This document defines only the
scalar semantics and the mathematical necessary-range transformation. It does not
promise prepared, backfill, equality-prefix, arbitrary OR/IN, named-zone AUTO or
multi-CN optimization support.

The invariant is **SQL TRUE implies that the physical candidate contains the row**. A candidate can over-fetch. The original bound SQL predicate remains a residual evaluated on the same logical row, before LIMIT/OFFSET/top-N and before any consumer that assumes one physical key per equality. `NULL` and SQL UNKNOWN never count as TRUE. This is a candidate design, not a cast of a SQL bound into the key type. `compareDatetimeAndTimestamp` (`pkg/sql/plan/function/func_compare.go`) converts DATETIME with the execution's zone and then calls `Timestamp.TruncateToScale` at the TIMESTAMP operand's scale. Despite its name, that method rounds nonnegative values when the next digit is >=5; signed division truncates toward zero, and the `-1` zero sentinel is returned unchanged. For scale 3 and an ordinary positive value, a `.123` result includes `.122500` through `.123499`; a `.123500` value rounds to `.124`.

`Datetime.convertToGoTime` is not just Go `time.Date`: if the normalized result lies in a detected forward-transition gap, `nonexistentLocalTimeBoundary` chooses the first valid second after the transition and keeps the original microseconds. In a gap, successive local seconds can map to the same absolute second; do not assume linearity within a local interval. In a fold Go may select one of two offsets. Both `ZeroDatetime` and `ZeroTimestamp` are `-1`, and valid early-year DATETIMEs can convert to other negative timestamps. The safe implementation cannot assume modern positive dates, an invertible cast, or that a single boundary's offset applies to an unbounded range. See also the Go [`time.Date` and `Time.ZoneBounds` contract](https://pkg.go.dev/time@go1.27.1) and [TZif RFC 9636](https://www.rfc-editor.org/rfc/rfc9636); MatrixOne's gap behavior above is the controlling implementation, not a new SQL-standard interpretation.

## Selection: bounded conservative range

The execution-local candidate builder in `pkg/container/types/temporal_index_bounds.go`; it depends only on temporal types, `time` and `context`, not on the SQL planner or readers. Its concrete internal contract is:

```go
// Full and Empty are disjoint. Full is the non-NULL physical domain.
type TemporalCandidate struct {
    Lo, Hi int64 // inclusive raw physical component values
    Sentinel int64 // -1 if needed; otherwise Lo, a duplicate point
    Full bool
    Empty bool // SQL TRUE is proven impossible, never a calculation failure
}
func BuildTemporalCandidate(ctx context.Context, keyKind T, timestampScale int32,
    bound int64, boundIsNull bool, op ComparisonOp, zone *time.Location) (TemporalCandidate, error)
```

`ComparisonOp` is the shared internal enum for the canonical atomic `=`, `<`, `<=`, `>`, `>=`; SQL-function binding supplies the actual timestamp scale and operand orientation. The inverse builder retains temporal type semantics; comparison classification, costing and physical leaf construction belong to the common index owners. The builder represents one atom over the physical component, not serialized bytes. `Full` returns `[math.MinInt64,math.MaxInt64]` for the non-NULL temporal key and is never empty. A NULL bound is `Empty`, because the admitted eager-safe atomic comparison cannot be SQL TRUE; the positive equality case below provides another exact `Empty`. Unknown/unsupported, malformed or overflow always means `Full`, never `Empty`. The extra point covers the zero sentinel if the original atom matches it, otherwise duplicates `Lo`. This avoids a scan of a distant zero-value block when the sentinel cannot match. Every caller also retains the original SQL residual. Cancellation propagates `ctx.Err`; optional calculation/representation failure widens to the physical full range, not a new SQL error. The residual preserves original SQL errors and diagnostics only when the whole-owner diagnostic admission proof above holds. `Full` is safe for an atom because its SQL TRUE requires the temporal key to be non-NULL; it is not an ALL arm for an arbitrary OR that can be true on NULL.

### TIMESTAMP key, DATETIME bound

At execution, for a non-NULL bound whose evaluation is proven statement-stable and diagnostic-free, compute `K = bound.ToTimestamp(zone).TruncateToScale(key.Scale)` using the same code and execution location as the scalar comparator. Compare the physical TIMESTAMP key to `K` with the canonical original operator; for an encoded multi-part key use the closed/prefix range rules below. The candidate is exact for the temporal comparison in this direction, but still retain the SQL residual and existing NULL/prefix checks. `K` may be negative or `-1`; never substitute a parsed timestamp, local display string, or a cast with different SQL mode. Checked conversion/arithmetic and a valid scale are mandatory; if input falls outside the proven conversion domain, use ALL.

### DATETIME key, TIMESTAMP bound

All arithmetic below is signed integer microseconds in MatrixOne's year-1 epoch, with checked/saturating operations. For scale `s`, let `q=10^(6-s)` for `0<=s<=6`; let `F(D)=D.ToTimestamp(zone)` and `R_s(t)=t.TruncateToScale(s)`. For every supported, non-overflowing `t`, including negative values and sentinel `-1`, `abs(R_s(t)-t) < q` (and is zero for scale 6). This is a deliberately conservative alternative to a complex exact inverse near `-1`. Given TIMESTAMP bound `B`, any nonzero SQL match satisfies:

| SQL predicate after orientation to `D op B` | Necessary UTC condition | Safe closed DATETIME condition when `e_min <= D-F(D) <= e_max` |
|---|---|---|
| `=` | `B-q <= F(D) <= B+q` | `[B-q+e_min, B+q+e_max]` |
| `<` or `<=` | `F(D) <= B+q` | `D <= B+q+e_max` |
| `>` or `>=` | `F(D) >= B-q` | `D >= B-q+e_min` |

This table widens open endpoints; exact strictness is restored by the original residual. For scale 6 use expansion zero. The extra width is bounded by two precision units plus the offset envelope, not by days or decades in normal operation. `D == ZeroDatetime` is handled separately: evaluate the sentinel comparison `R_s(ZeroTimestamp) op B`; union the `-1` point if it can be TRUE. Do not include a sentinel point by stretching a modern equality range down to `-1`. A nonzero `D` whose conversion happens to equal the timestamp sentinel is still covered by the same inequality: `R_s(-1)=-1` has zero rounding error. Check `B±q`, offset scaling, serialization and extrema for overflow; any unsupported/invalid type, invalid precision, arithmetic overflow or malformed representation produces ALL. Do not silently use `TimestampMinValue` as a hard validity bound (`ParseTimestampWithContext` explicitly does not enforce it). Use existing Packer typed ordering; do not alter any bytes.

The wall-time proof itself covers `D in [0,Dmax]`, where `Dmax=DatetimeFromClock(9999,12,31,23,59,59,999999)`. Do not assume every physically stored DATETIME belongs to that interval: `timestampToDatetime` and `datetimeToDatetime` append conversions/scale rounding without checking the resulting calendar year, and DATETIME preinsert validation checks zero, not the full year. A source-level boundary to exercise is a valid `9999-12-31 23:59:59.999999` TIMESTAMP written in UTC, then converted to DATETIME after switching the session to `+14:00`; another is rounding a typed DATETIME(6) at the 9999 limit to DATETIME(0). These SQL paths are proposed tests, not a claim they have already run. To make the candidate safe even before storage-domain assumptions, put **two constant physical tails once around the whole selected temporal candidate group**: `D in [MinInt64,-2] OR D in [Dmax+1,MaxInt64]`. OR them with the main range and conditional `-1` point in the *same* scan. The original residual determines their result; the normal empty-tail case should be eliminated by zone maps without extra block reads. If the reader cannot represent these disjoint branches without widening to a useless hull, it must decline pruning; this is an explicit reader/benchmark gate.

For the common positive-date case, use the exact **raw timestamp** bucket before adding the offset envelope. When `s<6`, `q>=10`, `h=q/2`, and `B>q`, let `floor=floor(B/q)*q` and `ceil=ceil(B/q)*q` with checked integer arithmetic:

| `R_s(F(D)) op B` | Necessary raw timestamp interval |
|---|---|
| `=` and `B%q==0` | `[B-h, B+h-1]` |
| `=` and `B%q!=0` | no TRUE is possible; return `Empty` plus residual |
| `<` | `F(D) <= ceil-h-1` |
| `<=` | `F(D) <= floor+h-1` |
| `>` | `F(D) >= floor+h` |
| `>=` | `F(D) >= ceil-h` |

For nonnegative `t`, this is the existing half-up rule, including the precise microsecond immediately before a half-unit. For negative `t`, Go signed division makes `R_s(t)<=0`: it cannot satisfy equality or a lower-bound comparison with `B>q`, and is already on the included side of an upper-bound comparison. The `-1` sentinel has the same ordering property. Thus the table is safe for *all* source rows, not an assumption that source timestamps are positive. Translate a raw lower/upper endpoint to DATETIME using `e_min`/`e_max` and handle sentinel separately as above. For `s=6`, use the exact bound and checked `-1`/`+1` microsecond for strict inequalities. Near zero, for negative bounds, or if the positive condition cannot be proved, retain the conservative `q` table. This common-domain precision avoids a dense boundary's extra bucket while avoiding a complicated full-domain negative/sentinel inverse framework.

#### Proving the fixed offset

`FixedZoneOffset` checks the public zone transition bounds, including the
ambiguous zero time boundary. For a proved fixed zone the actual offset is
used for both `e_min` and `e_max`; offsets outside the signed 32-bit range
and locations with transitions return `Full`. The planner and compiler only
admit fixed-zone candidates. Scalar comparisons and ordinary scan matching
continue to use the existing calendar conversion for named zones.

## Planner and reader obligations

Only an original owner proved diagnostic-safe may receive this optional pruning.
A pure bound alone cannot authorize skipping row-level warnings, errors or effects.
Known prepared owners and unsupported shapes retain their ordinary safe paths.
The current AUTO path uses one mixed atom over the leading component of a covering
index and retains every remapped original SQL predicate in the row filter.

The generated necessary condition is separately marked as `CandidateFilter`.
Preserve this marker through deep copy, folding and process transport. It identifies
necessary candidate ranges, including the reader's OR and seek behavior. Unknown,
malformed or nonconstant folds and unsupported storage filters fail open. Reader
outputs still run the full original predicate, including physical tails and sentinel
values. The filter proof and execution lifetime are specified in
[index planning ownership](20261107-index-planning-owners.md).

Time, timezone, placement and transaction identity belong to the current execution.
The cost observation never replaces the runtime expression. A changed execution
context triggers the existing safe rebuild; dynamic BASE and INDEX choices are
excluded from ordinary cache reuse. See the current access contract for exact
admission, publication, cleanup, resource limits and acceptance checks.
