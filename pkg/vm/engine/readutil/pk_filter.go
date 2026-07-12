// Copyright 2021-2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package readutil

import (
	"bytes"
	stdcmp "cmp"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap"
)

/* Don't remove me. will be used lated
func DirectConstructBlockPKFilter(
	isFakePK bool,
	val []byte,
	inVec *vector.Vector,
	oid types.T,
) (f objectio.BlockReadFilter, err error) {
	var base BasePKFilter
	if inVec != nil {
		base.Op = function.IN
		base.Vec = inVec
		base.Oid = oid
	} else {
		base.Op = function.EQUAL
		base.LB = val
		base.Oid = oid
	}
	base.Valid = true
	return ConstructBlockPKFilter(isFakePK, base)
}
*/

func ConstructBlockPKFilter(
	isFakePK bool,
	basePKFilter BasePKFilter,
	bf engine.MembershipFilter,
) (f objectio.BlockReadFilter, err error) {
	if bf != nil && !bf.Valid() {
		bf = nil
	}
	if !basePKFilter.Valid && bf == nil {
		basePKFilter.Cleanup()
		return objectio.BlockReadFilter{}, nil
	}

	readFilter := objectio.BlockReadFilter{
		HasFakePK: isFakePK,
	}
	if basePKFilter.cleanup != nil {
		readFilter.Cleanup = basePKFilter.cleanup.run
	}

	defer func() {
		if err != nil && readFilter.Cleanup != nil {
			readFilter.Cleanup()
			readFilter.Cleanup = nil
		}
		if readFilter.SortedSearchFunc == nil && readFilter.UnSortedSearchFunc == nil {
			logutil.Warn("ConstructBlockPKFilter skipped data type",
				zap.Int("expr op", basePKFilter.Op),
				zap.String("data type", basePKFilter.Oid.String()))
		}
	}()

	disjuncts := basePKFilter.Disjuncts
	if len(disjuncts) == 0 {
		disjuncts = []BasePKFilter{basePKFilter}
	}

	var (
		sortedMissing bool
		unsMissing    bool
		sortedFuncs   []func(*vector.Vector) []int64
		unsFuncs      []func(*vector.Vector) []int64
	)

	for idx := range disjuncts {
		sortedFunc, unsortedFunc, err := buildBlockPKSearchFuncs(disjuncts[idx])
		if err != nil {
			return objectio.BlockReadFilter{}, err
		}
		if sortedFunc == nil {
			sortedMissing = true
		} else {
			sortedFuncs = append(sortedFuncs, sortedFunc)
		}
		if unsortedFunc == nil {
			unsMissing = true
		} else {
			unsFuncs = append(unsFuncs, unsortedFunc)
		}
	}

	var (
		sortedSearchFunc   func(*vector.Vector) []int64
		unSortedSearchFunc func(*vector.Vector) []int64
	)
	if !sortedMissing && len(sortedFuncs) > 0 {
		sortedSearchFunc = combineOffsetFuncs(sortedFuncs)
	}
	if !unsMissing && len(unsFuncs) > 0 {
		unSortedSearchFunc = combineOffsetFuncs(unsFuncs)
	}

	// If no BloomFilter, keep original behavior: only use BasePKFilter's search functions.
	// Wrap the inner functions to match the new signature
	wrapInner := func(inner func(*vector.Vector) []int64) func(containers.Vectors) []int64 {
		if inner == nil {
			return nil
		}
		return func(cacheVectors containers.Vectors) []int64 {
			if len(cacheVectors) == 0 || cacheVectors[0].Length() == 0 {
				return nil
			}
			return inner(&cacheVectors[0])
		}
	}

	if bf == nil {
		if sortedSearchFunc != nil {
			readFilter.SortedSearchFunc = wrapInner(sortedSearchFunc)
			readFilter.UnSortedSearchFunc = wrapInner(unSortedSearchFunc)
			readFilter.Valid = true
			return readFilter, nil
		}
		// No BF, and no search func constructed from PK, equivalent to "no block filtering"
		if readFilter.Cleanup != nil {
			readFilter.Cleanup()
			readFilter.Cleanup = nil
		}
		return readFilter, nil
	}

	// Case with a membership filter: wrap existing search func.

	// Reusable temporary variables (defined outside closure to avoid allocation on each call)
	var (
		reusableSels []int64
	)

	bfVec := func(cacheVectors containers.Vectors) (*vector.Vector, bool) {
		if len(cacheVectors) == 0 || cacheVectors[0].Length() == 0 {
			return nil, false
		}
		// Prefer optimized BF column when provided.
		if len(cacheVectors) >= 2 {
			if cacheVectors[1].Length() == 0 {
				return &cacheVectors[0], true
			}
			if cacheVectors[1].Length() != cacheVectors[0].Length() {
				// The secondary vector can be a different key domain (for example,
				// an IVF entry's original PK).  Never fall back to probing the
				// compound PK when that explicitly supplied vector is malformed.
				return nil, false
			}
			return &cacheVectors[1], true
		}
		// Fallback to primary key column.
		return &cacheVectors[0], true
	}

	// Pure BloomFilter searchFunc: directly filter entire column with BloomFilter.
	// Uses cacheVectors[1] when available (optimization), otherwise falls back to cacheVectors[0].
	bfOnlySearch := func(cacheVectors containers.Vectors) []int64 {
		if len(cacheVectors) == 0 || cacheVectors[0].Length() == 0 {
			return nil
		}
		rowCount := cacheVectors[0].Length()
		sels := reusableSels[:0]
		if cap(sels) < rowCount {
			sels = make([]int64, 0, rowCount)
		}
		vec, usable := bfVec(cacheVectors)
		if !usable {
			for row := 0; row < rowCount; row++ {
				sels = append(sels, int64(row))
			}
		} else if hits := bf.TestVector(vec, nil); len(hits) != rowCount {
			// A malformed/unsupported membership filter must fail open.  The
			// residual SQL filter still guarantees correctness.
			for row := 0; row < rowCount; row++ {
				sels = append(sels, int64(row))
			}
		} else {
			for row, hit := range hits {
				if hit != 0 {
					sels = append(sels, int64(row))
				}
			}
		}
		reusableSels = sels
		return sels
	}

	// Wrap: if inner is nil, degenerate to pure BF; otherwise run inner first, then intersect with BF results.
	// The inner function receives *vector.Vector (PK column) for PK filtering.
	// BF filtering uses cacheVectors[1] when available (optimization), otherwise cacheVectors[0].
	wrap := func(inner func(*vector.Vector) []int64) func(containers.Vectors) []int64 {
		if inner == nil {
			return bfOnlySearch
		}
		return func(cacheVectors containers.Vectors) []int64 {
			if len(cacheVectors) == 0 || cacheVectors[0].Length() == 0 {
				return nil
			}

			pkVec := &cacheVectors[0]
			rowCount := pkVec.Length()
			offsets := inner(pkVec)
			innerCount := len(offsets)

			if innerCount == 0 {
				return offsets
			}

			// Test BloomFilter on rows filtered by inner function.
			vec, usable := bfVec(cacheVectors)
			if !usable {
				// No optimization: skip BF filtering, return all offsets that passed inner filter.
				return offsets
			}

			hits := bf.TestVector(vec, nil)
			if len(hits) != rowCount {
				return offsets
			}

			out := offsets[:0]
			for _, off := range offsets {
				if off >= 0 && off < int64(rowCount) && hits[off] != 0 {
					out = append(out, off)
				}
			}
			return out
		}
	}

	readFilter.SortedSearchFunc = wrap(sortedSearchFunc)
	readFilter.UnSortedSearchFunc = wrap(unSortedSearchFunc)
	readFilter.Valid = true
	// Set cleanup function
	baseCleanup := readFilter.Cleanup
	readFilter.Cleanup = func() {
		reusableSels = nil
		if baseCleanup != nil {
			baseCleanup()
		}
	}

	readFilter.Valid = true
	return readFilter, nil
}

func linearBoolSearchOffsetByValFactory(values []bool) func(*vector.Vector) []int64 {
	return func(vec *vector.Vector) []int64 {
		rows := vector.MustFixedColNoTypeCheck[bool](vec)
		result := make([]int64, 0, len(rows))
		for row, value := range rows {
			for _, candidate := range values {
				if value == candidate {
					result = append(result, int64(row))
					break
				}
			}
		}
		return result
	}
}

func allBlockRowOffsets(vec *vector.Vector) []int64 {
	if vec == nil || vec.Length() == 0 {
		return nil
	}
	offsets := make([]int64, vec.Length())
	for row := range offsets {
		offsets[row] = int64(row)
	}
	return offsets
}

func guardBlockPKSearchInput(
	search func(*vector.Vector) []int64,
	accept func(*types.Type) bool,
) func(*vector.Vector) []int64 {
	if search == nil {
		return nil
	}
	return func(vec *vector.Vector) []int64 {
		if vec == nil || !accept(vec.GetType()) {
			return allBlockRowOffsets(vec)
		}
		return search(vec)
	}
}

func compareBool(left, right bool) int {
	if left == right {
		return 0
	}
	if !left {
		return -1
	}
	return 1
}

func combineOffsetFuncs(funcs []func(*vector.Vector) []int64) func(*vector.Vector) []int64 {
	if len(funcs) == 1 {
		return funcs[0]
	}

	return func(vec *vector.Vector) []int64 {
		if vec == nil || vec.Length() == 0 {
			return nil
		}
		// One dense allocation serves as both the seen bitmap and the returned
		// offsets.  Rewriting marks into the prefix is safe because the output
		// cursor never advances beyond the row currently being inspected.
		marks := make([]int64, vec.Length())
		for _, fn := range funcs {
			if fn == nil {
				continue
			}
			offsets := fn(vec)
			for _, off := range offsets {
				if off < 0 || off >= int64(len(marks)) {
					continue
				}
				marks[off] = 1
			}
		}
		out := marks[:0]
		for row, hit := range marks {
			if hit != 0 {
				out = append(out, int64(row))
			}
		}
		return out
	}
}

func buildBlockPKSearchFuncs(
	basePKFilter BasePKFilter,
) (
	sortedSearchFunc func(*vector.Vector) []int64,
	unSortedSearchFunc func(*vector.Vector) []int64,
	err error,
) {
	if !validBlockPKSearchFilter(basePKFilter) {
		return nil, nil, nil
	}
	switch basePKFilter.Op {
	case function.EQUAL:
		switch basePKFilter.Oid {
		case types.T_bool:
			compare := func(x, y bool) int {
				if x == y {
					return 0
				}
				if !x {
					return -1
				}
				return 1
			}
			values := []bool{types.DecodeBool(basePKFilter.LB)}
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory(values, compare)
			unSortedSearchFunc = linearBoolSearchOffsetByValFactory(values)
		case types.T_bit:
			values := []uint64{types.DecodeUint64(basePKFilter.LB)}
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(values)
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(values, nil)
		case types.T_int8:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]int8{types.DecodeInt8(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]int8{types.DecodeInt8(basePKFilter.LB)}, nil)
		case types.T_int16:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]int16{types.DecodeInt16(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]int16{types.DecodeInt16(basePKFilter.LB)}, nil)
		case types.T_int32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]int32{types.DecodeInt32(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]int32{types.DecodeInt32(basePKFilter.LB)}, nil)
		case types.T_int64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]int64{types.DecodeInt64(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]int64{types.DecodeInt64(basePKFilter.LB)}, nil)
		case types.T_float32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]float32{types.DecodeFloat32(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]float32{types.DecodeFloat32(basePKFilter.LB)}, nil)
		case types.T_float64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]float64{types.DecodeFloat64(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]float64{types.DecodeFloat64(basePKFilter.LB)}, nil)
		case types.T_uint8:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]uint8{uint8(types.DecodeUint8(basePKFilter.LB))})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]uint8{uint8(types.DecodeUint8(basePKFilter.LB))}, nil)
		case types.T_uint16:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]uint16{uint16(types.DecodeUint16(basePKFilter.LB))})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]uint16{uint16(types.DecodeUint16(basePKFilter.LB))}, nil)
		case types.T_uint32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]uint32{types.DecodeUint32(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]uint32{types.DecodeUint32(basePKFilter.LB)}, nil)
		case types.T_uint64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]uint64{types.DecodeUint64(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]uint64{types.DecodeUint64(basePKFilter.LB)}, nil)
		case types.T_date:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Date{types.DecodeDate(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]types.Date{types.DecodeDate(basePKFilter.LB)}, nil)
		case types.T_time:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Time{types.DecodeTime(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]types.Time{types.DecodeTime(basePKFilter.LB)}, nil)
		case types.T_datetime:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Datetime{types.DecodeDatetime(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]types.Datetime{types.DecodeDatetime(basePKFilter.LB)}, nil)
		case types.T_timestamp:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Timestamp{types.DecodeTimestamp(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]types.Timestamp{types.DecodeTimestamp(basePKFilter.LB)}, nil)
		case types.T_year:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.MoYear{types.DecodeMoYear(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]types.MoYear{types.DecodeMoYear(basePKFilter.LB)}, nil)
		case types.T_decimal64:
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory([]types.Decimal64{types.DecodeDecimal64(basePKFilter.LB)}, types.CompareDecimal64)
			unSortedSearchFunc = vector.FixedSizeLinearSearchOffsetByValFactory([]types.Decimal64{types.DecodeDecimal64(basePKFilter.LB)}, types.CompareDecimal64)
		case types.T_decimal128:
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory([]types.Decimal128{types.DecodeDecimal128(basePKFilter.LB)}, types.CompareDecimal128)
			unSortedSearchFunc = vector.FixedSizeLinearSearchOffsetByValFactory([]types.Decimal128{types.DecodeDecimal128(basePKFilter.LB)}, types.CompareDecimal128)
		case types.T_decimal256:
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory([]types.Decimal256{types.DecodeDecimal256(basePKFilter.LB)}, types.CompareDecimal256)
			unSortedSearchFunc = vector.FixedSizeLinearSearchOffsetByValFactory([]types.Decimal256{types.DecodeDecimal256(basePKFilter.LB)}, types.CompareDecimal256)
		case types.T_varchar, types.T_char, types.T_binary, types.T_varbinary, types.T_json:
			sortedSearchFunc = vector.VarlenBinarySearchOffsetByValFactory([][]byte{basePKFilter.LB})
			unSortedSearchFunc = vector.VarlenLinearSearchOffsetByValFactory([][]byte{basePKFilter.LB})
		case types.T_enum:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Enum{types.DecodeEnum(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]types.Enum{types.DecodeEnum(basePKFilter.LB)}, nil)
		case types.T_uuid:
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory([]types.Uuid{types.Uuid(basePKFilter.LB)}, types.CompareUuid)
			unSortedSearchFunc = vector.FixedSizeLinearSearchOffsetByValFactory([]types.Uuid{types.Uuid(basePKFilter.LB)}, types.CompareUuid)
		default:
		}

	case function.PREFIX_EQ:
		sortedSearchFunc = vector.CollectOffsetsByPrefixEqFactory(basePKFilter.LB)
		unSortedSearchFunc = vector.LinearCollectOffsetsByPrefixEqFactory(basePKFilter.LB)

	case function.PREFIX_BETWEEN:
		sortedSearchFunc = vector.CollectOffsetsByPrefixBetweenFactory(basePKFilter.LB, basePKFilter.UB)
		unSortedSearchFunc = vector.LinearCollectOffsetsByPrefixBetweenFactory(basePKFilter.LB, basePKFilter.UB)

	case PrefixRangeLeftOpen, PrefixRangeRightOpen, PrefixRangeBothOpen:
		var hint uint8
		switch basePKFilter.Op {
		case PrefixRangeLeftOpen:
			hint = 1
		case PrefixRangeRightOpen:
			hint = 2
		case PrefixRangeBothOpen:
			hint = 3
		}
		sortedSearchFunc = vector.CollectOffsetsByPrefixInRangeFactory(basePKFilter.LB, basePKFilter.UB, hint)
		unSortedSearchFunc = vector.LinearCollectOffsetsByPrefixInRangeFactory(basePKFilter.LB, basePKFilter.UB, hint)

	case function.IN:
		vec := basePKFilter.Vec

		switch vec.GetType().Oid {
		case types.T_bool:
			compare := func(x, y bool) int {
				if x == y {
					return 0
				}
				if !x {
					return -1
				}
				return 1
			}
			values := vector.MustFixedColNoTypeCheck[bool](vec)
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory(values, compare)
			unSortedSearchFunc = linearBoolSearchOffsetByValFactory(values)
		case types.T_bit:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint64](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint64](vec), nil)
		case types.T_int8:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int8](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int8](vec), nil)
		case types.T_int16:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int16](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int16](vec), nil)
		case types.T_int32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int32](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int32](vec), nil)
		case types.T_int64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int64](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int64](vec), nil)
		case types.T_uint8:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint8](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint8](vec), nil)
		case types.T_uint16:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint16](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint16](vec), nil)
		case types.T_uint32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint32](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint32](vec), nil)
		case types.T_uint64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint64](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint64](vec), nil)
		case types.T_float32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[float32](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[float32](vec), nil)
		case types.T_float64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[float64](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[float64](vec), nil)
		case types.T_date:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Date](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Date](vec), nil)
		case types.T_time:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Time](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Time](vec), nil)
		case types.T_datetime:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Datetime](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Datetime](vec), nil)
		case types.T_timestamp:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Timestamp](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Timestamp](vec), nil)
		case types.T_year:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.MoYear](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.MoYear](vec), nil)
		case types.T_decimal64:
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Decimal64](vec), types.CompareDecimal64)
			unSortedSearchFunc = vector.FixedSizeLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Decimal64](vec), types.CompareDecimal64)
		case types.T_decimal128:
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Decimal128](vec), types.CompareDecimal128)
			unSortedSearchFunc = vector.FixedSizeLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Decimal128](vec), types.CompareDecimal128)
		case types.T_decimal256:
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Decimal256](vec), types.CompareDecimal256)
			unSortedSearchFunc = vector.FixedSizeLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Decimal256](vec), types.CompareDecimal256)
		case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
			types.T_array_float32, types.T_array_float64, types.T_datalink:
			sortedSearchFunc = vector.VarlenBinarySearchOffsetByValFactory(vector.InefficientMustBytesCol(vec))
			unSortedSearchFunc = vector.VarlenLinearSearchOffsetByValFactory(vector.InefficientMustBytesCol(vec))
		case types.T_enum:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Enum](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Enum](vec), nil)
		case types.T_uuid:
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Uuid](vec), types.CompareUuid)
			unSortedSearchFunc = vector.FixedSizeLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Uuid](vec), types.CompareUuid)
		default:
		}

	case function.PREFIX_IN:
		vec := basePKFilter.Vec

		sortedSearchFunc = vector.CollectOffsetsByPrefixInFactory(vec)
		unSortedSearchFunc = vector.LinearCollectOffsetsByPrefixInFactory(vec)

	case function.LESS_EQUAL, function.LESS_THAN:
		closed := basePKFilter.Op == function.LESS_EQUAL
		switch basePKFilter.Oid {
		case types.T_bool:
			value := types.DecodeBool(basePKFilter.LB)
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByLessTypeChecked(value, closed, true, compareBool)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByLessTypeChecked(value, closed, false, compareBool)
		case types.T_bit:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint64(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint64(basePKFilter.LB), closed, false)
		case types.T_int8:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt8(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt8(basePKFilter.LB), closed, false)
		case types.T_int16:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt16(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt16(basePKFilter.LB), closed, false)
		case types.T_int32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt32(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt32(basePKFilter.LB), closed, false)
		case types.T_int64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt64(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt64(basePKFilter.LB), closed, false)
		case types.T_float32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeFloat32(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeFloat32(basePKFilter.LB), closed, false)
		case types.T_float64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeFloat64(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeFloat64(basePKFilter.LB), closed, false)
		case types.T_uint8:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint8(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint8(basePKFilter.LB), closed, false)
		case types.T_uint16:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint16(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint16(basePKFilter.LB), closed, false)
		case types.T_uint32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint32(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint32(basePKFilter.LB), closed, false)
		case types.T_uint64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint64(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint64(basePKFilter.LB), closed, false)
		case types.T_date:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeDate(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeDate(basePKFilter.LB), closed, false)
		case types.T_time:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeTime(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeTime(basePKFilter.LB), closed, false)
		case types.T_datetime:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeDatetime(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeDatetime(basePKFilter.LB), closed, false)
		case types.T_timestamp:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeTimestamp(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeTimestamp(basePKFilter.LB), closed, false)
		case types.T_year:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeMoYear(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeMoYear(basePKFilter.LB), closed, false)
		case types.T_decimal64:
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.DecodeDecimal64(basePKFilter.LB), closed, true, types.CompareDecimal64)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.DecodeDecimal64(basePKFilter.LB), closed, false, types.CompareDecimal64)
		case types.T_decimal128:
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.DecodeDecimal128(basePKFilter.LB), closed, true, types.CompareDecimal128)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.DecodeDecimal128(basePKFilter.LB), closed, false, types.CompareDecimal128)
		case types.T_decimal256:
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.DecodeDecimal256(basePKFilter.LB), closed, true, types.CompareDecimal256)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.DecodeDecimal256(basePKFilter.LB), closed, false, types.CompareDecimal256)
		case types.T_varchar, types.T_char, types.T_binary, types.T_varbinary, types.T_json:
			sortedSearchFunc = vector.VarlenSearchOffsetByLess(basePKFilter.LB, closed, true)
			unSortedSearchFunc = vector.VarlenSearchOffsetByLess(basePKFilter.LB, closed, false)
		case types.T_enum:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeEnum(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeEnum(basePKFilter.LB), closed, false)
		case types.T_uuid:
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.Uuid(basePKFilter.LB), closed, true, types.CompareUuid)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.Uuid(basePKFilter.LB), closed, false, types.CompareUuid)
		default:
		}

	case function.GREAT_EQUAL, function.GREAT_THAN:
		closed := basePKFilter.Op == function.GREAT_EQUAL
		switch basePKFilter.Oid {
		case types.T_bool:
			value := types.DecodeBool(basePKFilter.LB)
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByGTTypeChecked(value, closed, true, compareBool)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByGTTypeChecked(value, closed, false, compareBool)
		case types.T_bit:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint64(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint64(basePKFilter.LB), closed, false)
		case types.T_int8:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt8(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt8(basePKFilter.LB), closed, false)
		case types.T_int16:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt16(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt16(basePKFilter.LB), closed, false)
		case types.T_int32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt32(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt32(basePKFilter.LB), closed, false)
		case types.T_int64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt64(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt64(basePKFilter.LB), closed, false)
		case types.T_float32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeFloat32(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeFloat32(basePKFilter.LB), closed, false)
		case types.T_float64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeFloat64(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeFloat64(basePKFilter.LB), closed, false)
		case types.T_uint8:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint8(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint8(basePKFilter.LB), closed, false)
		case types.T_uint16:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint16(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint16(basePKFilter.LB), closed, false)
		case types.T_uint32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint32(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint32(basePKFilter.LB), closed, false)
		case types.T_uint64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint64(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint64(basePKFilter.LB), closed, false)
		case types.T_date:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeDate(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeDate(basePKFilter.LB), closed, false)
		case types.T_time:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeTime(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeTime(basePKFilter.LB), closed, false)
		case types.T_datetime:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeDatetime(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeDatetime(basePKFilter.LB), closed, false)
		case types.T_timestamp:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeTimestamp(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeTimestamp(basePKFilter.LB), closed, false)
		case types.T_year:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeMoYear(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeMoYear(basePKFilter.LB), closed, false)
		case types.T_decimal64:
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.DecodeDecimal64(basePKFilter.LB), closed, true, types.CompareDecimal64)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.DecodeDecimal64(basePKFilter.LB), closed, false, types.CompareDecimal64)
		case types.T_decimal128:
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.DecodeDecimal128(basePKFilter.LB), closed, true, types.CompareDecimal128)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.DecodeDecimal128(basePKFilter.LB), closed, false, types.CompareDecimal128)
		case types.T_decimal256:
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.DecodeDecimal256(basePKFilter.LB), closed, true, types.CompareDecimal256)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.DecodeDecimal256(basePKFilter.LB), closed, false, types.CompareDecimal256)
		case types.T_varchar, types.T_char, types.T_json, types.T_binary, types.T_varbinary:
			sortedSearchFunc = vector.VarlenSearchOffsetByGreat(basePKFilter.LB, closed, true)
			unSortedSearchFunc = vector.VarlenSearchOffsetByGreat(basePKFilter.LB, closed, false)
		case types.T_enum:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeEnum(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeEnum(basePKFilter.LB), closed, false)
		case types.T_uuid:
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.Uuid(basePKFilter.LB), closed, true, types.CompareUuid)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.Uuid(basePKFilter.LB), closed, false, types.CompareUuid)
		default:
		}

	case function.BETWEEN, RangeLeftOpen, RangeRightOpen, RangeBothOpen:
		var hint uint8
		switch basePKFilter.Op {
		case RangeLeftOpen:
			hint = 1
		case RangeRightOpen:
			hint = 2
		case RangeBothOpen:
			hint = 3
		}
		switch basePKFilter.Oid {
		case types.T_bool:
			if hint == 0 {
				lb := types.DecodeBool(basePKFilter.LB)
				ub := types.DecodeBool(basePKFilter.UB)
				sortedSearchFunc = vector.CollectOffsetsByBetweenWithCompareFactory(lb, ub, compareBool)
				unSortedSearchFunc = vector.FixedSizedLinearCollectOffsetsByBetweenFactory(lb, ub, compareBool)
			}
		case types.T_bit:
			lb := types.DecodeUint64(basePKFilter.LB)
			ub := types.DecodeUint64(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_int8:
			lb := types.DecodeInt8(basePKFilter.LB)
			ub := types.DecodeInt8(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_int16:
			lb := types.DecodeInt16(basePKFilter.LB)
			ub := types.DecodeInt16(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_int32:
			lb := types.DecodeInt32(basePKFilter.LB)
			ub := types.DecodeInt32(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_int64:
			lb := types.DecodeInt64(basePKFilter.LB)
			ub := types.DecodeInt64(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_float32:
			lb := types.DecodeFloat32(basePKFilter.LB)
			ub := types.DecodeFloat32(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_float64:
			lb := types.DecodeFloat64(basePKFilter.LB)
			ub := types.DecodeFloat64(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_uint8:
			lb := types.DecodeUint8(basePKFilter.LB)
			ub := types.DecodeUint8(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_uint16:
			lb := types.DecodeUint16(basePKFilter.LB)
			ub := types.DecodeUint16(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_uint32:
			lb := types.DecodeUint32(basePKFilter.LB)
			ub := types.DecodeUint32(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_uint64:
			lb := types.DecodeUint64(basePKFilter.LB)
			ub := types.DecodeUint64(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_date:
			lb := types.DecodeDate(basePKFilter.LB)
			ub := types.DecodeDate(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_time:
			lb := types.DecodeTime(basePKFilter.LB)
			ub := types.DecodeTime(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_datetime:
			lb := types.DecodeDatetime(basePKFilter.LB)
			ub := types.DecodeDatetime(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_timestamp:
			lb := types.DecodeTimestamp(basePKFilter.LB)
			ub := types.DecodeTimestamp(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_year:
			lb := types.DecodeMoYear(basePKFilter.LB)
			ub := types.DecodeMoYear(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_decimal64:
			val1 := types.DecodeDecimal64(basePKFilter.LB)
			val2 := types.DecodeDecimal64(basePKFilter.UB)

			sortedSearchFunc = vector.CollectOffsetsByBetweenWithCompareFactory(val1, val2, types.CompareDecimal64)
			unSortedSearchFunc = vector.FixedSizedLinearCollectOffsetsByBetweenFactory(val1, val2, types.CompareDecimal64)
		case types.T_decimal128:
			val1 := types.DecodeDecimal128(basePKFilter.LB)
			val2 := types.DecodeDecimal128(basePKFilter.UB)

			sortedSearchFunc = vector.CollectOffsetsByBetweenWithCompareFactory(val1, val2, types.CompareDecimal128)
			unSortedSearchFunc = vector.FixedSizedLinearCollectOffsetsByBetweenFactory(val1, val2, types.CompareDecimal128)
		case types.T_decimal256:
			val1 := types.DecodeDecimal256(basePKFilter.LB)
			val2 := types.DecodeDecimal256(basePKFilter.UB)

			sortedSearchFunc = vector.CollectOffsetsByBetweenWithCompareFactory(val1, val2, types.CompareDecimal256)
			unSortedSearchFunc = vector.FixedSizedLinearCollectOffsetsByBetweenFactory(val1, val2, types.CompareDecimal256)
		case types.T_text, types.T_datalink, types.T_varchar, types.T_char, types.T_binary, types.T_varbinary, types.T_json:
			lb := string(basePKFilter.LB)
			ub := string(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenString(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenString(lb, ub, hint)
		case types.T_enum:
			lb := types.DecodeEnum(basePKFilter.LB)
			ub := types.DecodeEnum(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)

		case types.T_uuid:
			val1 := types.Uuid(basePKFilter.LB)
			val2 := types.Uuid(basePKFilter.UB)

			sortedSearchFunc = vector.CollectOffsetsByBetweenWithCompareFactory(val1, val2, types.CompareUuid)
			unSortedSearchFunc = vector.FixedSizedLinearCollectOffsetsByBetweenFactory(val1, val2, types.CompareUuid)
		default:
		}
	}
	if sortedSearchFunc == nil && unSortedSearchFunc == nil {
		return nil, nil, nil
	}
	acceptInput := func(typ *types.Type) bool { return typ.Oid == basePKFilter.Oid }
	switch basePKFilter.Op {
	case function.IN:
		expected := basePKFilter.Vec.GetType().Oid
		acceptInput = func(typ *types.Type) bool { return typ.Oid == expected }
	case function.PREFIX_EQ, function.PREFIX_IN, function.PREFIX_BETWEEN,
		PrefixRangeLeftOpen, PrefixRangeRightOpen, PrefixRangeBothOpen:
		acceptInput = func(typ *types.Type) bool { return typ.IsVarlen() }
	}
	sortedSearchFunc = guardBlockPKSearchInput(sortedSearchFunc, acceptInput)
	unSortedSearchFunc = guardBlockPKSearchInput(unSortedSearchFunc, acceptInput)
	return
}

func mergeBaseFilterInKind(
	left, right BasePKFilter, isOR bool, mp *mpool.MPool,
) (ret BasePKFilter, err error) {
	var allocated *vector.Vector
	defer func() {
		if allocated != nil {
			allocated.Free(mp)
		}
	}()
	var va, vb *vector.Vector
	va = left.Vec
	vb = right.Vec
	// Constant folding deliberately leaves nullable IN vectors unsorted so the
	// null bitmap stays aligned with its values.  The ordered set helpers below
	// require sorted inputs and do not account for nulls, so merging such vectors
	// could create false negatives.  Refuse the optimization and let the caller
	// keep a conservative conjunct/disjunct representation instead.
	if va == nil || vb == nil || left.Oid != right.Oid ||
		left.Oid != va.GetType().Oid || va.GetType().Oid != vb.GetType().Oid ||
		va.GetNulls().Any() || vb.GetNulls().Any() {
		return BasePKFilter{}, nil
	}
	ret.Vec = vector.NewVec(left.Oid.ToType())
	allocated = ret.Vec

	switch va.GetType().Oid {
	case types.T_bool:
		a := vector.MustFixedColNoTypeCheck[bool](va)
		b := vector.MustFixedColNoTypeCheck[bool](vb)
		compare := func(x, y bool) int {
			if x == y {
				return 0
			}
			if !x {
				return -1
			}
			return 1
		}
		err = mergeFixedInValues(a, b, ret.Vec, mp, isOR, compare)
	case types.T_bit:
		a := vector.MustFixedColNoTypeCheck[uint64](va)
		b := vector.MustFixedColNoTypeCheck[uint64](vb)
		compare := func(x, y uint64) int { return stdcmp.Compare(x, y) }
		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, compare)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, compare)
		}
	case types.T_int8:
		a := vector.MustFixedColNoTypeCheck[int8](va)
		b := vector.MustFixedColNoTypeCheck[int8](vb)
		cmp := func(x, y int8) int { return stdcmp.Compare(x, y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_int16:
		a := vector.MustFixedColNoTypeCheck[int16](va)
		b := vector.MustFixedColNoTypeCheck[int16](vb)
		cmp := func(x, y int16) int { return stdcmp.Compare(x, y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_int32:
		a := vector.MustFixedColNoTypeCheck[int32](va)
		b := vector.MustFixedColNoTypeCheck[int32](vb)
		cmp := func(x, y int32) int { return stdcmp.Compare(x, y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_int64:
		a := vector.MustFixedColNoTypeCheck[int64](va)
		b := vector.MustFixedColNoTypeCheck[int64](vb)
		cmp := func(x, y int64) int { return stdcmp.Compare(x, y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_float32:
		a := vector.MustFixedColNoTypeCheck[float32](va)
		b := vector.MustFixedColNoTypeCheck[float32](vb)
		cmp := func(x, y float32) int { return stdcmp.Compare(x, y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_float64:
		a := vector.MustFixedColNoTypeCheck[float64](va)
		b := vector.MustFixedColNoTypeCheck[float64](vb)
		cmp := func(x, y float64) int { return stdcmp.Compare(x, y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_uint8:
		a := vector.MustFixedColNoTypeCheck[uint8](va)
		b := vector.MustFixedColNoTypeCheck[uint8](vb)
		cmp := func(x, y uint8) int { return stdcmp.Compare(x, y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_uint16:
		a := vector.MustFixedColNoTypeCheck[uint16](va)
		b := vector.MustFixedColNoTypeCheck[uint16](vb)
		cmp := func(x, y uint16) int { return stdcmp.Compare(x, y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_uint32:
		a := vector.MustFixedColNoTypeCheck[uint32](va)
		b := vector.MustFixedColNoTypeCheck[uint32](vb)
		cmp := func(x, y uint32) int { return stdcmp.Compare(x, y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_uint64:
		a := vector.MustFixedColNoTypeCheck[uint64](va)
		b := vector.MustFixedColNoTypeCheck[uint64](vb)
		cmp := func(x, y uint64) int { return stdcmp.Compare(x, y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_date:
		a := vector.MustFixedColNoTypeCheck[types.Date](va)
		b := vector.MustFixedColNoTypeCheck[types.Date](vb)
		cmp := func(x, y types.Date) int { return stdcmp.Compare(x, y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_time:
		a := vector.MustFixedColNoTypeCheck[types.Time](va)
		b := vector.MustFixedColNoTypeCheck[types.Time](vb)
		cmp := func(x, y types.Time) int { return stdcmp.Compare(x, y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_datetime:
		a := vector.MustFixedColNoTypeCheck[types.Datetime](va)
		b := vector.MustFixedColNoTypeCheck[types.Datetime](vb)
		cmp := func(x, y types.Datetime) int { return stdcmp.Compare(x, y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_timestamp:
		a := vector.MustFixedColNoTypeCheck[types.Timestamp](va)
		b := vector.MustFixedColNoTypeCheck[types.Timestamp](vb)
		cmp := func(x, y types.Timestamp) int { return stdcmp.Compare(x, y) }
		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_year:
		a := vector.MustFixedColNoTypeCheck[types.MoYear](va)
		b := vector.MustFixedColNoTypeCheck[types.MoYear](vb)
		compare := func(x, y types.MoYear) int { return stdcmp.Compare(x, y) }
		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, compare)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, compare)
		}
	case types.T_decimal64:
		a := vector.MustFixedColNoTypeCheck[types.Decimal64](va)
		b := vector.MustFixedColNoTypeCheck[types.Decimal64](vb)
		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, types.CompareDecimal64)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, types.CompareDecimal64)
		}

	case types.T_decimal128:
		a := vector.MustFixedColNoTypeCheck[types.Decimal128](va)
		b := vector.MustFixedColNoTypeCheck[types.Decimal128](vb)
		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp,
				func(x, y types.Decimal128) int { return types.CompareDecimal128(x, y) })
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp,
				func(x, y types.Decimal128) int { return types.CompareDecimal128(x, y) })
		}

	case types.T_decimal256:
		a := vector.MustFixedColNoTypeCheck[types.Decimal256](va)
		b := vector.MustFixedColNoTypeCheck[types.Decimal256](vb)
		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp,
				func(x, y types.Decimal256) int { return types.CompareDecimal256(x, y) })
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp,
				func(x, y types.Decimal256) int { return types.CompareDecimal256(x, y) })
		}

	case types.T_varchar, types.T_char, types.T_json, types.T_binary, types.T_varbinary, types.T_text, types.T_datalink:
		if isOR {
			err = vector.Union2VectorValen(va, vb, ret.Vec, mp)
		} else {
			err = vector.Intersection2VectorVarlen(va, vb, ret.Vec, mp)
		}

	case types.T_enum:
		a := vector.MustFixedColNoTypeCheck[types.Enum](va)
		b := vector.MustFixedColNoTypeCheck[types.Enum](vb)
		cmp := func(x, y types.Enum) int { return stdcmp.Compare(x, y) }
		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_uuid:
		a := vector.MustFixedColNoTypeCheck[types.Uuid](va)
		b := vector.MustFixedColNoTypeCheck[types.Uuid](vb)
		err = mergeFixedInValues(a, b, ret.Vec, mp, isOR, types.CompareUuid)

	default:
		allocated.Free(mp)
		allocated = nil
		ret.Vec = nil
		return BasePKFilter{}, nil
	}
	if err != nil {
		return BasePKFilter{}, err
	}
	ret.Vec.SetSorted(true)

	// The merged vector remains live in reader search closures. Hand its mpool
	// ownership to BlockReadFilter.Cleanup instead of serializing and rebuilding
	// the complete vector on the Go heap.
	cleanup := left.cleanup
	otherCleanup := right.cleanup
	if cleanup == nil {
		cleanup = otherCleanup
		otherCleanup = nil
	}
	if cleanup == nil {
		cleanup = &basePKFilterCleanup{}
	}
	if otherCleanup != nil && otherCleanup != cleanup {
		cleanup.add(otherCleanup.run)
	}
	merged := ret.Vec
	resource := &basePKFilterResource{free: func() { merged.Free(mp) }}
	cleanup.add(resource.release)
	ret.cleanup = cleanup
	ret.resource = resource
	allocated = nil

	ret.Valid = true
	ret.Op = left.Op
	ret.Oid = left.Oid

	return ret, err
}

// mergeFixedInValues is the ordered set merge used for fixed-size types that
// are not covered by constraints.Ordered (notably bool and UUID).
func mergeFixedInValues[T types.FixedSizeTExceptStrType](
	a, b []T,
	ret *vector.Vector,
	mp *mpool.MPool,
	isOR bool,
	compare func(T, T) int,
) error {
	capacity := min(len(a), len(b))
	if isOR {
		capacity = len(a) + len(b)
	}
	if err := ret.PreExtend(capacity, mp); err != nil {
		return err
	}
	i, j := 0, 0
	appendUnique := func(value T) error {
		if ret.Length() > 0 {
			last := vector.MustFixedColNoTypeCheck[T](ret)[ret.Length()-1]
			if compare(last, value) == 0 {
				return nil
			}
		}
		return vector.AppendFixed(ret, value, false, mp)
	}
	for i < len(a) && j < len(b) {
		order := compare(a[i], b[j])
		if isOR {
			if order <= 0 {
				if err := appendUnique(a[i]); err != nil {
					return err
				}
				i++
			} else {
				if err := appendUnique(b[j]); err != nil {
					return err
				}
				j++
			}
			continue
		}
		if order == 0 {
			if err := appendUnique(a[i]); err != nil {
				return err
			}
			i++
			j++
		} else if order < 0 {
			i++
		} else {
			j++
		}
	}
	if isOR {
		for ; i < len(a); i++ {
			if err := appendUnique(a[i]); err != nil {
				return err
			}
		}
		for ; j < len(b); j++ {
			if err := appendUnique(b[j]); err != nil {
				return err
			}
		}
	}
	return nil
}

func compareBasePKValues(oid types.T, left, right []byte) int {
	// Intermediate filters that only carry Disjuncts intentionally have no
	// atomic OID/value.  They can reach mergeFilters' conservative fallback;
	// retain byte ordering there rather than attempting to decode T_any.
	if oid == types.T_any {
		return bytes.Compare(left, right)
	}
	return types.CompareValue(types.DecodeValue(left, oid), types.DecodeValue(right, oid))
}

func validEncodedBasePKValue(oid types.T, value []byte) bool {
	switch oid {
	case types.T_any:
		return true
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text,
		types.T_binary, types.T_varbinary, types.T_array_float32, types.T_array_float64,
		types.T_datalink, types.T_geometry, types.T_geometry32:
		return true
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp, types.T_year,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_uuid, types.T_TS, types.T_Rowid, types.T_enum:
		return len(value) == oid.TypeLen()
	default:
		return false
	}
}

func validBlockPKSearchFilter(filter BasePKFilter) bool {
	// A BasePKFilter can cross planner/read boundaries as serialized data.  Keep
	// both persisted-block and workspace pruning on the same fail-open contract,
	// including for malformed vector layouts supplied by older/newer versions.
	switch filter.Op {
	case function.IN:
		return filter.Vec != nil && filter.Vec.GetType().Oid == filter.Oid &&
			supportedPKInType(filter.Oid) && !filter.Vec.GetNulls().Any() &&
			validatePKInVectorShape(filter.Vec) == nil &&
			!basePKFilterHasNaN(filter)
	case function.PREFIX_IN:
		return filter.Vec != nil && filter.Vec.GetType().Oid == filter.Oid &&
			supportedPKPrefixType(filter.Oid) && !filter.Vec.GetNulls().Any() &&
			validatePKInVectorShape(filter.Vec) == nil
	case function.BETWEEN, RangeLeftOpen, RangeRightOpen, RangeBothOpen:
		return validEncodedBasePKValue(filter.Oid, filter.LB) &&
			validEncodedBasePKValue(filter.Oid, filter.UB) &&
			!basePKFilterHasNaN(filter)
	case function.PREFIX_EQ, function.PREFIX_BETWEEN,
		PrefixRangeLeftOpen, PrefixRangeRightOpen, PrefixRangeBothOpen:
		return true
	case function.EQUAL, function.LESS_EQUAL, function.LESS_THAN,
		function.GREAT_EQUAL, function.GREAT_THAN:
		return validEncodedBasePKValue(filter.Oid, filter.LB) &&
			!basePKFilterHasNaN(filter)
	default:
		return false
	}
}

func supportedPKPrefixType(oid types.T) bool {
	switch oid {
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary:
		return true
	default:
		return false
	}
}

func supportedPKInType(oid types.T) bool {
	switch oid {
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp, types.T_year,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json,
		types.T_blob, types.T_text, types.T_array_float32, types.T_array_float64,
		types.T_datalink, types.T_enum, types.T_uuid:
		return true
	default:
		return false
	}
}

func basePKFilterHasNaN(filter BasePKFilter) bool {
	switch filter.Oid {
	case types.T_float32:
		if filter.Op == function.IN {
			for _, value := range vector.MustFixedColNoTypeCheck[float32](filter.Vec) {
				if math.IsNaN(float64(value)) {
					return true
				}
			}
			return false
		}
		return math.IsNaN(float64(types.DecodeFloat32(filter.LB))) ||
			(len(filter.UB) > 0 && math.IsNaN(float64(types.DecodeFloat32(filter.UB))))
	case types.T_float64:
		if filter.Op == function.IN {
			for _, value := range vector.MustFixedColNoTypeCheck[float64](filter.Vec) {
				if math.IsNaN(value) {
					return true
				}
			}
			return false
		}
		return math.IsNaN(types.DecodeFloat64(filter.LB)) ||
			(len(filter.UB) > 0 && math.IsNaN(types.DecodeFloat64(filter.UB)))
	default:
		return false
	}
}

// left op in (">", ">=", "=", "<", "<="), right op in (">", ">=", "=", "<", "<=")
// left op AND right op
// left op OR right op
func mergeFilters(
	left, right *BasePKFilter,
	connector int,
	mp *mpool.MPool,
) (finalFilter BasePKFilter, err error) {
	unsafeInput := false
	defer func() {
		finalFilter.Oid = left.Oid

		if !finalFilter.Valid && connector == function.AND && !unsafeInput {
			// nonempty disjuncts makes the memory pk filter invalid,
			// so we choose the one whose disjuncts is empty as default here.
			if len(left.Disjuncts) > 0 {
				finalFilter = *right
				if right.Vec != nil {
					// why dup here?
					// once the merge filter generated, the others will be free.
					//finalFilter.Vec, err = left.Vec.Dup(proc.Mp())

					// or just set the left.Vec = nil to avoid free it
					(*right).Vec = nil
				}
			} else {
				finalFilter = *left
				if left.Vec != nil {
					(*left).Vec = nil
				}
			}
		}
	}()
	unsafeInput = (len(left.Disjuncts) == 0 && !validBasePKMergeOperand(*left)) ||
		(len(right.Disjuncts) == 0 && !validBasePKMergeOperand(*right))
	if unsafeInput ||
		(len(left.Disjuncts) == 0 && len(right.Disjuncts) == 0 && left.Oid != right.Oid) ||
		(left.Op != function.IN && len(left.Disjuncts) == 0 && !validEncodedBasePKValue(left.Oid, left.LB)) ||
		(right.Op != function.IN && len(right.Disjuncts) == 0 && !validEncodedBasePKValue(right.Oid, right.LB)) {
		return BasePKFilter{}, nil
	}

	switch connector {
	case function.AND:
		switch left.Op {
		case function.IN:
			switch right.Op {
			case function.IN:
				// a in (...) and a in (...) and a in (...) and ...
				finalFilter, err = mergeBaseFilterInKind(*left, *right, false, mp)
			}

		case function.GREAT_EQUAL:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a >= x and a >= y --> a >= max(x, y)
				// a >= x and a > y  --> a > y or a >= x
				if compareBasePKValues(left.Oid, left.LB, right.LB) >= 0 { // x >= y
					return *left, nil
				} else { // x < y
					return *right, nil
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a >= x and a <= y --> [x, y]
				// a >= x and a < y  --> [x, y)
				finalFilter.LB = left.LB
				finalFilter.UB = right.LB
				if right.Op == function.LESS_THAN {
					finalFilter.Op = RangeRightOpen
				} else {
					finalFilter.Op = function.BETWEEN
				}
				finalFilter.Valid = true

			case function.EQUAL:
				// a >= x and a = y --> a = y if y >= x
				if compareBasePKValues(left.Oid, left.LB, right.LB) <= 0 {
					finalFilter.Op = function.EQUAL
					finalFilter.LB = right.LB
					finalFilter.Valid = true
				}
			}

		case function.GREAT_THAN:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a > x and a >= y
				// a > x and a > y
				if compareBasePKValues(left.Oid, left.LB, right.LB) >= 0 { // x >= y
					return *left, nil
				} else { // x < y
					return *right, nil
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a > x and a <= y --> (x, y]
				// a > x and a < y  --> (x, y)
				finalFilter.LB = left.LB
				finalFilter.UB = right.LB
				if right.Op == function.LESS_THAN {
					finalFilter.Op = RangeBothOpen
				} else {
					finalFilter.Op = RangeLeftOpen
				}
				finalFilter.Valid = true

			case function.EQUAL:
				// a > x and a = y --> a = y if y > x
				if compareBasePKValues(left.Oid, left.LB, right.LB) < 0 { // x < y
					finalFilter.Op = function.EQUAL
					finalFilter.LB = right.LB
					finalFilter.Valid = true
				}
			}

		case function.LESS_EQUAL:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a <= x and a >= y --> [y, x]
				// a <= x and a > y  --> (y, x]
				finalFilter.LB = right.LB
				finalFilter.UB = left.LB
				if right.Op == function.GREAT_EQUAL {
					finalFilter.Op = function.BETWEEN
				} else {
					finalFilter.Op = RangeLeftOpen
				}
				finalFilter.Valid = true

			case function.LESS_EQUAL, function.LESS_THAN:
				// a <= x and a <= y --> a <= min(x,y)
				// a <= x and a < y  --> a <= x if x < y | a < y if x >= y
				if compareBasePKValues(left.Oid, left.LB, right.LB) < 0 { // x < y
					return *left, nil
				} else {
					return *right, nil
				}

			case function.EQUAL:
				// a <= x and a = y --> a = y if x >= y
				if compareBasePKValues(left.Oid, left.LB, right.LB) >= 0 {
					finalFilter.Op = function.EQUAL
					finalFilter.LB = right.LB
					finalFilter.Valid = true
				}
			}

		case function.LESS_THAN:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a < x and a >= y --> [y, x)
				// a < x and a > y  --> (y, x)
				finalFilter.LB = right.LB
				finalFilter.UB = left.LB
				if right.Op == function.GREAT_EQUAL {
					finalFilter.Op = RangeRightOpen
				} else {
					finalFilter.Op = RangeBothOpen
				}
				finalFilter.Valid = true

			case function.LESS_EQUAL, function.LESS_THAN:
				// a < x and a <= y --> a < x if x <= y | a <= y if x > y
				// a < x and a < y  --> a < min(x,y)
				finalFilter.Op = function.LESS_THAN
				if compareBasePKValues(left.Oid, left.LB, right.LB) <= 0 {
					finalFilter.LB = left.LB
				} else {
					finalFilter.LB = right.LB
					if right.Op == function.LESS_EQUAL {
						finalFilter.Op = function.LESS_EQUAL
					}
				}
				finalFilter.Valid = true

			case function.EQUAL:
				// a < x and a = y --> a = y if x > y
				if compareBasePKValues(left.Oid, left.LB, right.LB) > 0 {
					finalFilter.Op = function.EQUAL
					finalFilter.LB = right.LB
					finalFilter.Valid = true
				}
			}

		case function.EQUAL:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a = x and a >= y --> a = x if x >= y
				// a = x and a > y  --> a = x if x > y
				if ret := compareBasePKValues(left.Oid, left.LB, right.LB); ret > 0 {
					return *left, nil
				} else if ret == 0 && right.Op == function.GREAT_EQUAL {
					return *left, nil
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a = x and a <= y --> a = x if x <= y
				// a = x and a < y  --> a = x if x < y
				if ret := compareBasePKValues(left.Oid, left.LB, right.LB); ret < 0 {
					return *left, nil
				} else if ret == 0 && right.Op == function.LESS_EQUAL {
					return *left, nil
				}

			case function.EQUAL:
				// a = x and a = y --> a = y if x = y
				if bytes.Equal(left.LB, right.LB) {
					return *left, nil
				}
			}
		}

	case function.OR:
		switch left.Op {
		case function.IN:
			switch right.Op {
			case function.IN:
				// a in (...) and a in (...)
				finalFilter, err = mergeBaseFilterInKind(*left, *right, true, mp)
			}

		case function.GREAT_EQUAL:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a >= x or a >= y --> a >= min(x, y)
				// a >= x or a > y  --> a >= x if x <= y | a > y if x > y
				if compareBasePKValues(left.Oid, left.LB, right.LB) <= 0 { // x <= y
					return *left, nil
				} else { // x > y
					return *right, nil
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a >= x or a <= y --> all if x <= y | [] or []
				// a >= x or a < y  -->
				//finalFilter.LB = left.LB
				//finalFilter.UB = right.LB
				//if right.Op == function.LESS_THAN {
				//	finalFilter.Op = RangeRightOpen
				//} else {
				//	finalFilter.Op = function.BETWEEN
				//}
				//finalFilter.Valid = true

			case function.EQUAL:
				// a >= x or a = y --> a >= x if x <= y | [], x
				if compareBasePKValues(left.Oid, left.LB, right.LB) <= 0 {
					finalFilter.Op = function.GREAT_EQUAL
					finalFilter.LB = left.LB
					finalFilter.Valid = true
				}
			}

		case function.GREAT_THAN:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a > x or a >= y --> a >= y if x >= y | a > x if x < y
				// a > x or a > y  --> a > y if x >= y | a > x if x < y
				if compareBasePKValues(left.Oid, left.LB, right.LB) >= 0 { // x >= y
					return *right, nil
				} else { // x < y
					return *left, nil
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a > x or a <= y --> (x, y]
				// a > x or a < y  --> (x, y)
				//finalFilter.LB = left.LB
				//finalFilter.UB = right.LB
				//if right.Op == function.LESS_THAN {
				//	finalFilter.Op = rangeBothOpen
				//} else {
				//	finalFilter.Op = rangeLeftOpen
				//}
				//finalFilter.Valid = true

			case function.EQUAL:
				// a > x or a = y --> a > x if x < y | a >= x if x == y
				if ret := compareBasePKValues(left.Oid, left.LB, right.LB); ret < 0 { // x < y
					return *left, nil
				} else if ret == 0 {
					finalFilter = *left
					finalFilter.Op = function.GREAT_EQUAL
				}
			}

		case function.LESS_EQUAL:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a <= x or a >= y -->
				// a <= x or a > y  -->
				//finalFilter.LB = right.LB
				//finalFilter.UB = left.LB
				//if right.Op == function.GREAT_EQUAL {
				//	finalFilter.Op = function.BETWEEN
				//} else {
				//	finalFilter.Op = rangeLeftOpen
				//}
				//finalFilter.Valid = true

			case function.LESS_EQUAL, function.LESS_THAN:
				// a <= x or a <= y --> a <= max(x,y)
				// a <= x or a < y  --> a <= x if x >= y | a < y if x < y
				if compareBasePKValues(left.Oid, left.LB, right.LB) >= 0 { // x >= y
					return *left, nil
				} else {
					return *right, nil
				}

			case function.EQUAL:
				// a <= x or a = y --> a <= x if x >= y | [], x
				if compareBasePKValues(left.Oid, left.LB, right.LB) >= 0 {
					return *left, nil
				}
			}

		case function.LESS_THAN:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a < x or a >= y
				// a < x or a > y
				//finalFilter.LB = right.LB
				//finalFilter.UB = left.LB
				//if right.Op == function.GREAT_EQUAL {
				//	finalFilter.Op = RangeRightOpen
				//} else {
				//	finalFilter.Op = rangeBothOpen
				//}
				//finalFilter.Valid = true

			case function.LESS_EQUAL, function.LESS_THAN:
				// a < x or a <= y --> a <= y if x <= y | a < x if x > y
				// a < x or a < y  --> a < y if x <= y | a < x if x > y
				if compareBasePKValues(left.Oid, left.LB, right.LB) <= 0 { // a <= y
					return *right, nil
				} else {
					return *left, nil
				}

			case function.EQUAL:
				// a < x or a = y --> a < x if x > y | a <= x if x = y
				if ret := compareBasePKValues(left.Oid, left.LB, right.LB); ret > 0 {
					return *left, nil
				} else if ret == 0 {
					finalFilter = *left
					finalFilter.Op = function.LESS_EQUAL
				}
			}

		case function.EQUAL:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a = x or a >= y --> a >= y if x >= y
				// a = x or a > y  --> a > y if x > y | a >= y if x = y
				if ret := compareBasePKValues(left.Oid, left.LB, right.LB); ret > 0 {
					return *right, nil
				} else if ret == 0 {
					finalFilter = *right
					finalFilter.Op = function.GREAT_EQUAL
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a = x or a <= y --> a <= y if x <= y
				// a = x or a < y  --> a < y if x < y | a <= y if x = y
				if ret := compareBasePKValues(left.Oid, left.LB, right.LB); ret < 0 {
					return *right, nil
				} else if ret == 0 {
					finalFilter = *right
					finalFilter.Op = function.LESS_EQUAL
				}

			case function.EQUAL:
				// a = x or a = y --> a = x if x = y
				//                --> a in (x, y) if x != y
				if bytes.Equal(left.LB, right.LB) {
					return *left, nil
				}

			}
		}
	}
	return
}

func validBasePKMergeOperand(filter BasePKFilter) bool {
	switch filter.Op {
	case function.IN:
		return validBlockPKSearchFilter(filter)
	case function.EQUAL, function.LESS_EQUAL, function.LESS_THAN,
		function.GREAT_EQUAL, function.GREAT_THAN:
		return validEncodedBasePKValue(filter.Oid, filter.LB) &&
			!basePKFilterHasNaN(filter)
	default:
		return true
	}
}
