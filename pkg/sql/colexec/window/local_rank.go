// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package window

// Local sliding-window ranks (issue #29947): frame-local competition rank
// kernels shared by mrank, cumrank and tmrank in processValueFuncRange.

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// tmRankFrame builds the transient RANGE frame clause the operator resolves
// per row: INTERVAL width PRECEDING through the current row. The width is the
// materialized (count, unit) pair, possibly null-marked for the NULL-arithmetic
// outcome, so the shared resolver's NULL-interval branch produces the
// key-dependent membership (decision D17) and the current-row end resolves the
// peer group. The frame exists only in operator memory; the window spec stays
// frameless.
func tmRankFrame(materialized *plan.Expr) *plan.FrameClause {
	return &plan.FrameClause{
		Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  materialized,
		},
		End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
}

// localRankRow maps a logical row to the physical row of a possibly const
// vector argument.
func localRankRow(vec *vector.Vector, row int) int {
	if vec.IsConst() || vec.IsConstNull() {
		return 0
	}
	return row
}

// localRankValueIsNull reports whether the value vector's logical row is
// NULL (const vectors keep their single physical logical at row 0).
func localRankValueIsNull(vec *vector.Vector, row int) bool {
	return vec.IsConstNull() || vec.IsNull(uint64(localRankRow(vec, row)))
}

// localRankValueLess reports v[a] < v[b] over the typed value vector. Only
// the domains the catalog admits reach here: numeric and temporal scalar
// vectors (decision D3 keeps strings out of scope).
func localRankValueLess(vec *vector.Vector, rowA, rowB int) (bool, error) {
	a := localRankRow(vec, rowA)
	b := localRankRow(vec, rowB)
	// typeswitch:partial bf16/float16/float8/float4 arguments are cast to float32 when the window function binds
	switch vec.GetType().Oid {
	case types.T_int8:
		col := vector.MustFixedColNoTypeCheck[int8](vec)
		return col[a] < col[b], nil
	case types.T_int16:
		col := vector.MustFixedColNoTypeCheck[int16](vec)
		return col[a] < col[b], nil
	case types.T_int32:
		col := vector.MustFixedColNoTypeCheck[int32](vec)
		return col[a] < col[b], nil
	case types.T_int64:
		col := vector.MustFixedColNoTypeCheck[int64](vec)
		return col[a] < col[b], nil
	case types.T_uint8:
		col := vector.MustFixedColNoTypeCheck[uint8](vec)
		return col[a] < col[b], nil
	case types.T_uint16:
		col := vector.MustFixedColNoTypeCheck[uint16](vec)
		return col[a] < col[b], nil
	case types.T_uint32:
		col := vector.MustFixedColNoTypeCheck[uint32](vec)
		return col[a] < col[b], nil
	case types.T_uint64, types.T_bit:
		col := vector.MustFixedColNoTypeCheck[uint64](vec)
		return col[a] < col[b], nil
	case types.T_float32:
		col := vector.MustFixedColNoTypeCheck[float32](vec)
		return col[a] < col[b], nil
	case types.T_float64:
		col := vector.MustFixedColNoTypeCheck[float64](vec)
		return col[a] < col[b], nil
	case types.T_decimal64:
		col := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)
		return col[a].Compare(col[b]) < 0, nil
	case types.T_decimal128:
		col := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
		return col[a].Compare(col[b]) < 0, nil
	case types.T_timestamp:
		col := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)
		return col[a] < col[b], nil
	case types.T_datetime:
		col := vector.MustFixedColNoTypeCheck[types.Datetime](vec)
		return col[a] < col[b], nil
	case types.T_date:
		col := vector.MustFixedColNoTypeCheck[types.Date](vec)
		return col[a] < col[b], nil
	case types.T_time:
		col := vector.MustFixedColNoTypeCheck[types.Time](vec)
		return col[a] < col[b], nil
	case types.T_year:
		// YEAR is part of the catalog-admitted temporal domain; MoYear is the
		// int16-backed named type the vector layer stores year columns as.
		col := vector.MustFixedColNoTypeCheck[types.MoYear](vec)
		return col[a] < col[b], nil
	case types.T_decimal256:
		col := vector.MustFixedColNoTypeCheck[types.Decimal256](vec)
		return col[a].Compare(col[b]) < 0, nil
	default:
		return false, moerr.NewInternalErrorNoCtxf(
			"unsupported local rank value type %v", vec.GetType().Oid)
	}
}

// localRankWidthFromVec reads a catalog-admitted INTEGER row-count width in
// its source integer domain. Positivity is validated in that domain, so an
// unsigned magnitude above MaxInt64 stays a valid positive width (it simply
// covers the partition prefix available to the row); ok is false only for
// NULL values, non-positive widths and non-integer payload types.
func localRankWidthFromVec(vec *vector.Vector, row int) (uint64, bool) {
	if vec.Length() == 0 || vec.IsNull(uint64(row)) {
		return 0, false
	}
	switch vec.GetType().Oid {
	case types.T_int8:
		if v := vector.MustFixedColNoTypeCheck[int8](vec)[row]; v > 0 {
			return uint64(v), true
		}
	case types.T_int16:
		if v := vector.MustFixedColNoTypeCheck[int16](vec)[row]; v > 0 {
			return uint64(v), true
		}
	case types.T_int32:
		if v := vector.MustFixedColNoTypeCheck[int32](vec)[row]; v > 0 {
			return uint64(v), true
		}
	case types.T_int64:
		if v := vector.MustFixedColNoTypeCheck[int64](vec)[row]; v > 0 {
			return uint64(v), true
		}
	case types.T_uint8:
		if v := vector.MustFixedColNoTypeCheck[uint8](vec)[row]; v > 0 {
			return uint64(v), true
		}
	case types.T_uint16:
		if v := vector.MustFixedColNoTypeCheck[uint16](vec)[row]; v > 0 {
			return uint64(v), true
		}
	case types.T_uint32:
		if v := vector.MustFixedColNoTypeCheck[uint32](vec)[row]; v > 0 {
			return uint64(v), true
		}
	case types.T_uint64:
		if v := vector.MustFixedColNoTypeCheck[uint64](vec)[row]; v > 0 {
			return v, true
		}
	}
	return 0, false
}

// countLessInRankRange counts k in [start, end) whose v[k] is strictly lower
// than curRow's value, skipping NULL rows (they never rank). checkCanceled runs
// every frame-row iteration on its bounded polling cadence (decision: the
// flagless kernel keeps the scan cancellable).
func countLessInRankRange(
	proc *process.Process,
	srcVec *vector.Vector,
	start, end, curRow int,
) (uint64, error) {
	count := uint64(0)
	for k := start; k < end; k++ {
		if err := checkCanceled(proc, k); err != nil {
			return 0, err
		}
		if localRankValueIsNull(srcVec, k) {
			continue
		}
		less, err := localRankValueLess(srcVec, k, curRow)
		if err != nil {
			return 0, err
		}
		if less {
			count++
		}
	}
	return count, nil
}
