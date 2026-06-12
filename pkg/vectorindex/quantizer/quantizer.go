// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package quantizer is the single source of truth for ivfflat QUANTIZATION: the
// mapping from a CREATE INDEX QUANTIZATION='...' name to the narrow entry type,
// the cuVS-style asymmetric int8 scalar quantizer (training the [min,max] bounds,
// deriving the q(x)=round(x*mul+add) transform, applying it to a query vector,
// and emitting the equivalent SQL entry projection), and the SQL type names.
//
// The same q(x)=x*mul+add must be applied identically on three sides — the
// synchronous build (compile), the CDC delta writer (iscp), and search — so the
// formula lives here once and each side calls in rather than re-deriving it.
package quantizer

import (
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

const (
	// Int8Lo/Int8Hi are the signed int8 range the quantizer maps [min,max] onto.
	Int8Lo = -128.0
	Int8Hi = 127.0
	// int8Span is the number of quantization steps (Int8Hi-Int8Lo = 255).
	int8Span = Int8Hi - Int8Lo
)

// ToVectorType maps a CREATE INDEX QUANTIZATION='...' value to the vector element
// type the ivfflat ENTRIES are down-cast to (the base column and centroids are
// unaffected). The accepted names are the canonical metric.Quantization_*_Str
// constants (case-insensitive): float32 -> vecf32, float16 -> vecf16,
// bf16 -> vecbf16, int8 -> vecint8. float32/float16/bf16 are float formats (plain
// cast); int8 uses the trained scalar quantizer. float32 is accepted because it is
// a real down-cast for an f64 base; float64 (an up-cast) and uint8 / "" return
// ok=false (no quantization; entries keep the base type).
func ToVectorType(q string) (types.T, bool) {
	switch strings.ToLower(strings.TrimSpace(q)) {
	case metric.Quantization_F32_Str:
		return types.T_array_float32, true
	case metric.Quantization_F16_Str:
		return types.T_array_float16, true
	case metric.Quantization_BF16_Str:
		return types.T_array_bf16, true
	case metric.Quantization_INT8_Str:
		return types.T_array_int8, true
	}
	return 0, false
}

// SQLTypeName returns the SQL type name for a vector element type, for use in
// CAST(... AS <name>(dim)). Thin alias over types.T.ArraySQLName (the canonical
// source of the lowercase SQL spellings).
func SQLTypeName(t types.T) string {
	return t.ArraySQLName()
}

// Int8Params returns (mul, add) for the cuVS-style asymmetric int8 scalar
// quantizer that maps [min,max] onto the full int8 range [Int8Lo,Int8Hi]:
//
//	q(x) = round(x*mul + add), clamped to [-128,127]
//
// add folds the -min offset and the -128 int8 shift into one constant, so both
// the build (cast(base*mul+add as vecint8)) and search apply it with a single
// multiply-add. A degenerate range (max<=min) falls back to identity.
func Int8Params(min, max float64) (mul, add float64) {
	rng := max - min
	// !(rng > 0) also rejects NaN (every NaN comparison is false), and the IsInf
	// guard rejects a non-finite range — either would otherwise yield NaN/Inf mul
	// that poisons the build SQL and the query transform.
	if !(rng > 0) || math.IsInf(rng, 0) {
		return 1.0, 0.0
	}
	mul = int8Span / rng
	add = -min*mul + Int8Lo
	return mul, add
}

// TrainInt8 returns (P0.1, P99.9) of the sample values — the bounds for the
// asymmetric int8 scalar quantizer. Percentiles (not raw min/max) clip outliers
// so the quantization grid isn't wasted on a few extreme values. Returns (-1,1)
// for empty data and widens a degenerate range. Subsamples to bound cost.
func TrainInt8[T types.RealNumbers](data [][]T) (min, max float64) {
	const maxVals = 2_000_000
	total := 0
	for _, v := range data {
		total += len(v)
	}
	if total == 0 {
		return -1, 1
	}
	stride := 1
	if total > maxVals {
		stride = total/maxVals + 1
	}
	vals := make([]float64, 0, total/stride+1)
	k := 0
	for _, v := range data {
		for _, x := range v {
			if k%stride == 0 {
				f := float64(x)
				// Skip NaN/Inf: they make slices.Sort's order undefined (so a
				// percentile pick could land on NaN) and would poison the trained
				// bounds and the SQL literal.
				if !math.IsNaN(f) && !math.IsInf(f, 0) {
					vals = append(vals, f)
				}
			}
			k++
		}
	}
	if len(vals) == 0 {
		return -1, 1
	}
	slices.Sort(vals)
	lo := vals[int(float64(len(vals)-1)*0.001)]
	hi := vals[int(float64(len(vals)-1)*0.999)]
	if hi <= lo {
		hi = lo + 1
	}
	return lo, hi
}

// ApplyInt8 applies q(x)=x*mul+add to a float32 query vector and narrows to int8
// (round+clamp), matching the entry build. (mul,add)=(1,0) is identity (no
// quantizer trained), so the raw narrowing cast is used. The multiply-add is done
// in float64 (then narrowed) to match the build side, whose entry SQL
// `cast(base*mul+add as vecint8)` evaluates the f64 literals in the base column's
// arithmetic — doing it in float32 here could bucket a boundary component
// differently. qf32 is never mutated.
func ApplyInt8(qf32 []float32, mul, add float64) []int8 {
	if mul == 1.0 && add == 0.0 {
		return types.Float32ToInt8Slice(qf32)
	}
	sq := make([]float32, len(qf32))
	for i, x := range qf32 {
		sq[i] = float32(float64(x)*mul + add)
	}
	return types.Float32ToInt8Slice(sq)
}

// CastSQL builds `cast(<colExpr> as <type>(dim))` for narrowing an entry to a
// quantization type without scaling — float formats (float16/bf16/float32), or
// int8 when no [min,max] bounds were trained (the implicit cast does identity
// round+clamp). colExpr is the already-quoted column reference or sub-expression.
func CastSQL(colExpr string, t types.T, dim int32) string {
	return fmt.Sprintf("cast(%s as %s(%d))", colExpr, SQLTypeName(t), dim)
}

// Int8EntrySQL builds the int8 entry projection `cast(<colExpr> * mul + add as
// vecint8(dim))` from precomputed literal bounds (the synchronous build path,
// where compile has already read the trained [min,max] from metadata). colExpr is
// the already-quoted column reference.
func Int8EntrySQL(colExpr string, mul, add float64, dim int32) string {
	return fmt.Sprintf("cast(%s * %.9g + (%.9g) as vecint8(%d))", colExpr, mul, add, dim)
}

// Int8EntrySQLFromBounds builds the int8 entry projection where the bounds are SQL
// expressions (e.g. metadata-table subqueries) rather than precomputed literals —
// the CDC delta path, which cannot read metadata into Go before building the
// REPLACE. It inlines q(x)=x*mul+add with mul=int8Span/(max-min) and
// add=-min*mul-128, and wraps mul/add in COALESCE so an absent bound (a pure-async
// index that never trained) falls back to identity (1,0) — matching what search
// does when the bounds are missing. colExpr/minExpr/maxExpr are SQL sub-expressions.
func Int8EntrySQLFromBounds(colExpr, minExpr, maxExpr string, dim int32) string {
	// 255.0 == int8Span, 128.0 == -Int8Lo, kept as literals so the division/offset
	// evaluate in DOUBLE in SQL (and the generated text is stable).
	rng := fmt.Sprintf("(%s - %s)", maxExpr, minExpr)
	mul := fmt.Sprintf("COALESCE(255.0 / %s, 1.0)", rng)
	add := fmt.Sprintf("COALESCE(0.0 - %s * 255.0 / %s - 128.0, 0.0)", minExpr, rng)
	return fmt.Sprintf("cast(%s * %s + %s as vecint8(%d))", colExpr, mul, add, dim)
}
