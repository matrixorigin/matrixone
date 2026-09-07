// Copyright 2021 - 2022 Matrix Origin
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

package aggexec

import (
	"math/bits"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// XXX:
//
// This returned type thing definitely belongs to plan, function.
// However, exec cannot import plan, function due to circular dependency.
// See list_agg.go, we need to remove dependency of plan on exec.
//

func AvgReturnType(typs []types.Type) types.Type {
	typ := typs[0]
	switch typ.Oid {
	case types.T_int8, types.T_uint8, types.T_int16, types.T_uint16,
		types.T_int32, types.T_uint32, types.T_int64, types.T_uint64, types.T_year:
		return avgIntegerReturnType(typ)
	case types.T_decimal64:
		return types.New(types.T_decimal128, typ.Width+4, avgDecimalScale(typ.Scale))
	case types.T_decimal128:
		precision := typ.Width + 4
		if precision <= maxDecimal128Precision {
			return types.New(types.T_decimal128, precision, avgDecimalScale(typ.Scale))
		}
		precision = min(precision, maxAvgDecimalPrecision)
		scale := avgDecimalScale(typ.Scale)
		if precision < scale {
			precision = scale
		}
		return types.New(types.T_decimal256, precision, scale)
	case types.T_decimal256:
		precision := min(typ.Width+4, maxAvgDecimalPrecision)
		scale := avgDecimalScale(typ.Scale)
		if precision < scale {
			precision = scale
		}
		return types.New(types.T_decimal256, precision, scale)
	default:
		return types.T_float64.ToType()
	}
}

const (
	maxDecimal128Precision  int32 = 38
	maxAvgDecimalPrecision  int32 = 65
	avgResultScaleIncrement int32 = 4
	maxAvgIntegerPrecision        = maxAvgDecimalPrecision - avgResultScaleIncrement
)

// avgIntegerReturnType keeps the planner-visible AVG type valid even when the
// binder records precision for a constant integer expression. Decimal128 can
// expose at most 38 digits; wider expressions promote to Decimal256, whose
// public AVG precision is capped at 65 digits.
func avgIntegerReturnType(typ types.Type) types.Type {
	precision := avgIntegerPrecision(typ)
	if precision <= maxDecimal128Precision-avgResultScaleIncrement {
		return types.New(types.T_decimal128, precision+avgResultScaleIncrement, avgResultScaleIncrement)
	}
	if precision >= maxAvgIntegerPrecision {
		return types.New(types.T_decimal256, maxAvgDecimalPrecision, avgResultScaleIncrement)
	}
	return types.New(types.T_decimal256, precision+avgResultScaleIncrement, avgResultScaleIncrement)
}

// Integer Type.Width is normally zero (the storage width is carried by Size),
// but the planner fills it with an expression's decimal precision for an AVG
// constant expression. Explicit integer casts carry a bit width and Scale ==
// -1; those must fall back to the complete integer domain. Keeping expression
// precision on the argument type makes all executor construction paths agree.
func avgIntegerPrecision(typ types.Type) int32 {
	if typ.Width > 0 && typ.Scale >= 0 {
		return typ.Width
	}
	switch typ.Oid {
	case types.T_int8, types.T_uint8:
		return 3
	case types.T_int16, types.T_uint16:
		return 5
	case types.T_int32, types.T_uint32:
		return 10
	case types.T_int64:
		return 19
	case types.T_uint64:
		return 20
	case types.T_year:
		return 4
	default:
		return 0
	}
}

const maxAvgDecimalScale int32 = 38

func avgDecimalScale(inputScale int32) int32 {
	resultScale := inputScale + 4
	if resultScale > maxAvgDecimalScale {
		resultScale = maxAvgDecimalScale
	}
	// Never silently discard fractional digits from a valid input type. The
	// Decimal256 result can retain scale 38 even when the usual +4 increment
	// would exceed the supported public scale.
	if resultScale < inputScale {
		resultScale = inputScale
	}
	return resultScale
}

func SumReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_float32, types.T_float64:
		return types.T_float64.ToType()
	case types.T_int8, types.T_int16, types.T_int32, types.T_year:
		return types.T_int64.ToType()
	case types.T_int64:
		return types.New(types.T_decimal128, 38, 0)
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_bit:
		return types.T_uint64.ToType()
	case types.T_uint64:
		return types.New(types.T_decimal128, 38, 0)
	case types.T_decimal64:
		return types.New(types.T_decimal128, 38, typs[0].Scale)
	case types.T_decimal128:
		return types.New(types.T_decimal128, 38, typs[0].Scale)
	case types.T_decimal256:
		return types.New(types.T_decimal256, 65, typs[0].Scale)
	}
	panic(moerr.NewInternalErrorNoCtxf("unsupported type '%v' for sum", typs[0]))
}

func int64OfCheck(v1, v2, sum int64) error {
	if (v1 > 0 && v2 > 0 && sum <= 0) || (v1 < 0 && v2 < 0 && sum >= 0) {
		return moerr.NewOutOfRangeNoCtxf("int64", "(%d + %d)", v1, v2)
	}
	return nil
}

func uint64OfCheck(v1, v2, sum uint64) error {
	if sum < v1 || sum < v2 {
		return moerr.NewOutOfRangeNoCtxf("uint64", "(%d + %d)", v1, v2)
	}
	return nil
}

func float64OfCheck(v1, v2, sum float64) error {
	// MySQL behavior: SUM() aggregation allows overflow to +Infinity without error
	// This matches MySQL 8.0 where SUM() silently returns +Infinity on overflow
	return nil
}

func windowRowIsNull(vec *vector.Vector, row int) bool {
	if vec.IsConst() {
		row = 0
	}
	return vec.IsNull(uint64(row))
}

type sumAvgExec[T float64 | int64 | uint64, A types.Ints | types.UInts | types.Floats | types.MoYear] struct {
	aggExec
	isSum              bool
	exactAvg           bool
	ofCheck            func(T, T, T) error
	windowNonNullCount int64
}

func (*sumAvgExec[T, A]) sourcePreservingMerge() {}

func (exec *sumAvgExec[T, A]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *sumAvgExec[T, A]) windowSlidingSupported() bool {
	if exec.IsDistinct() {
		return false
	}
	// Floating subtraction can change results for infinities and accumulate
	// rounding drift. Keep those inputs on the ordinary frame evaluator.
	switch exec.aggInfo.argTypes[0].Oid {
	case types.T_float32, types.T_float64:
		return false
	default:
		return true
	}
}

func (exec *sumAvgExec[T, A]) addWindowRow(row int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if windowRowIsNull(vec, row) {
		return nil
	}
	if vec.IsConst() {
		row = 0
	}
	value := T(vector.MustFixedColNoTypeCheck[A](vec)[row])
	sums := chunkArr[T](exec.state[0].vecs[0])
	result := sums[0] + value
	if err := exec.ofCheck(sums[0], value, result); err != nil {
		return err
	}
	sums[0] = result
	if exec.isSum {
		exec.state[0].vecs[0].UnsetNull(0)
	} else {
		vector.MustFixedColNoTypeCheck[int64](exec.state[0].vecs[1])[0]++
	}
	exec.windowNonNullCount++
	return nil
}

func (exec *sumAvgExec[T, A]) removeWindowRow(row int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if windowRowIsNull(vec, row) {
		return nil
	}
	if exec.windowNonNullCount <= 0 {
		return moerr.NewInternalErrorNoCtx("sliding SUM/AVG state is empty")
	}
	if vec.IsConst() {
		row = 0
	}
	value := T(vector.MustFixedColNoTypeCheck[A](vec)[row])
	sums := chunkArr[T](exec.state[0].vecs[0])
	sums[0] -= value
	exec.windowNonNullCount--
	if !exec.isSum {
		counts := vector.MustFixedColNoTypeCheck[int64](exec.state[0].vecs[1])
		if counts[0] <= 0 {
			return moerr.NewInternalErrorNoCtx("sliding AVG count state is empty")
		}
		counts[0]--
	}
	if exec.windowNonNullCount == 0 {
		sums[0] = 0
		if exec.isSum {
			exec.state[0].vecs[0].SetNull(0)
		}
	}
	return nil
}

func (exec *sumAvgExec[T, A]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	if exec.IsDistinct() {
		return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
	}
	if exec.isSum {
		return exec.bulkFillSumSingleGroup(groupIndex, vectors)
	}
	return exec.bulkFillAvgSingleGroup(groupIndex, vectors)
}

func (exec *sumAvgExec[T, A]) bulkFillSumSingleGroup(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	n := vec.Length()
	if n == 0 {
		return nil
	}

	vals := vector.MustFixedColNoTypeCheck[A](vec)
	isConst := vec.IsConst()
	hasNull := vec.HasNull()

	g := uint64(groupIndex)
	x := int(g >> aggBatchSizeShift)
	y := g & aggBatchSizeMask
	sums := chunkArr[T](exec.state[x].vecs[0])
	sumVec := exec.state[x].vecs[0]

	var localSum T
	filled := false
	for i := 0; i < n; i++ {
		row := i
		if isConst {
			row = 0
		}
		if hasNull && vec.IsNull(uint64(row)) {
			continue
		}

		val := T(vals[row])
		if !filled {
			localSum = val
			filled = true
			continue
		}
		result := localSum + val
		if err := exec.ofCheck(localSum, val, result); err != nil {
			return err
		}
		localSum = result
	}
	if !filled {
		return nil
	}

	old := sums[y]
	result := old + localSum
	if err := exec.ofCheck(old, localSum, result); err != nil {
		return err
	}
	sums[y] = result
	if sumVec.IsNull(y) {
		sumVec.UnsetNull(y)
	}
	return nil
}

func (exec *sumAvgExec[T, A]) bulkFillAvgSingleGroup(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	n := vec.Length()
	if n == 0 {
		return nil
	}

	vals := vector.MustFixedColNoTypeCheck[A](vec)
	isConst := vec.IsConst()
	hasNull := vec.HasNull()

	g := uint64(groupIndex)
	x := int(g >> aggBatchSizeShift)
	y := g & aggBatchSizeMask
	sums := chunkArr[T](exec.state[x].vecs[0])
	cnts := vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])

	var localSum T
	var localCnt int64
	for i := 0; i < n; i++ {
		row := i
		if isConst {
			row = 0
		}
		if hasNull && vec.IsNull(uint64(row)) {
			continue
		}

		val := T(vals[row])
		if localCnt == 0 {
			localSum = val
			localCnt = 1
			continue
		}
		result := localSum + val
		if err := exec.ofCheck(localSum, val, result); err != nil {
			return err
		}
		localSum = result
		localCnt++
	}
	if localCnt == 0 {
		return nil
	}

	old := sums[y]
	result := old + localSum
	if err := exec.ofCheck(old, localSum, result); err != nil {
		return err
	}
	sums[y] = result
	cnts[y] += localCnt
	return nil
}

func (exec *sumAvgExec[T, A]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if exec.IsDistinct() {
		return exec.batchFillArgs(offset, groups, vectors, true)
	}
	if exec.isSum {
		return exec.batchFillSum(offset, groups, vectors)
	}
	return exec.batchFillAvg(offset, groups, vectors)
}

func (exec *sumAvgExec[T, A]) batchFillSum(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	n := len(groups)
	if n == 0 {
		return nil
	}
	vals := vector.MustFixedColNoTypeCheck[A](vec)
	isConst := vec.IsConst()

	const slotEmpty = 0xFF
	const maxSlots = 255
	var slotOf [256]uint8
	var localSums [maxSlots]T
	var localGrps [maxSlots]uint64
	nSlots := 0

	for i := range slotOf {
		slotOf[i] = slotEmpty
	}

	hasNull := vec.HasNull()
	for i := 0; i < n; i++ {
		grp := groups[i]
		if grp == GroupNotMatched {
			continue
		}
		if hasNull && vec.IsNull(uint64(i)+uint64(offset)) {
			continue
		}

		g := grp - 1
		var val T
		if isConst {
			val = T(vals[0])
		} else {
			val = T(vals[i+offset])
		}

		h := uint8(g) ^ uint8(g>>8)
		for {
			s := slotOf[h]
			if s == slotEmpty {
				if nSlots >= maxSlots {
					// Local table full — direct scatter for this row.
					x := int(g >> aggBatchSizeShift)
					y := g & aggBatchSizeMask
					sums := chunkArr[T](exec.state[x].vecs[0])
					old := sums[y]
					result := old + val
					if err := exec.ofCheck(old, val, result); err != nil {
						return err
					}
					sums[y] = result
					if exec.state[x].vecs[0].IsNull(y) {
						exec.state[x].vecs[0].UnsetNull(y)
					}
					break
				}
				s = uint8(nSlots)
				slotOf[h] = s
				localGrps[nSlots] = g
				localSums[nSlots] = val
				nSlots++
				break
			}
			if localGrps[s] == g {
				old := localSums[s]
				result := old + val
				if err := exec.ofCheck(old, val, result); err != nil {
					return err
				}
				localSums[s] = result
				break
			}
			h++
		}
	}

	lastX := -1
	var sums *[AggBatchSize]T
	var sumVec *vector.Vector
	for s := 0; s < nSlots; s++ {
		g := localGrps[s]
		x := int(g >> aggBatchSizeShift)
		if x != lastX {
			lastX = x
			sums = chunkArr[T](exec.state[x].vecs[0])
			sumVec = exec.state[x].vecs[0]
		}
		y := g & aggBatchSizeMask
		old := sums[y]
		add := localSums[s]
		result := old + add
		if err := exec.ofCheck(old, add, result); err != nil {
			return err
		}
		sums[y] = result

		if sumVec.IsNull(y) {
			sumVec.UnsetNull(y)
		}
	}
	return nil
}

func (exec *sumAvgExec[T, A]) batchFillAvg(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	n := len(groups)
	if n == 0 {
		return nil
	}
	vals := vector.MustFixedColNoTypeCheck[A](vec)
	isConst := vec.IsConst()

	const slotEmpty = 0xFF
	const maxSlots = 255
	var slotOf [256]uint8
	var localSums [maxSlots]T
	var localCnts [maxSlots]int64
	var localGrps [maxSlots]uint64
	nSlots := 0

	for i := range slotOf {
		slotOf[i] = slotEmpty
	}

	hasNull := vec.HasNull()
	for i := 0; i < n; i++ {
		grp := groups[i]
		if grp == GroupNotMatched {
			continue
		}
		if hasNull && vec.IsNull(uint64(i)+uint64(offset)) {
			continue
		}

		g := grp - 1
		var val T
		if isConst {
			val = T(vals[0])
		} else {
			val = T(vals[i+offset])
		}

		h := uint8(g) ^ uint8(g>>8)
		for {
			s := slotOf[h]
			if s == slotEmpty {
				if nSlots >= maxSlots {
					x := int(g >> aggBatchSizeShift)
					y := g & aggBatchSizeMask
					sums := chunkArr[T](exec.state[x].vecs[0])
					old := sums[y]
					result := old + val
					if err := exec.ofCheck(old, val, result); err != nil {
						return err
					}
					sums[y] = result
					cnts := vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])
					cnts[y]++
					break
				}
				s = uint8(nSlots)
				slotOf[h] = s
				localGrps[nSlots] = g
				localSums[nSlots] = val
				localCnts[nSlots] = 1
				nSlots++
				break
			}
			if localGrps[s] == g {
				old := localSums[s]
				result := old + val
				if err := exec.ofCheck(old, val, result); err != nil {
					return err
				}
				localSums[s] = result
				localCnts[s]++
				break
			}
			h++
		}
	}

	lastX := -1
	var sums *[AggBatchSize]T
	var cnts []int64
	for s := 0; s < nSlots; s++ {
		g := localGrps[s]
		x := int(g >> aggBatchSizeShift)
		if x != lastX {
			lastX = x
			sums = chunkArr[T](exec.state[x].vecs[0])
			cnts = vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])
		}
		y := g & aggBatchSizeMask
		old := sums[y]
		add := localSums[s]
		result := old + add
		if err := exec.ofCheck(old, add, result); err != nil {
			return err
		}
		sums[y] = result
		cnts[y] += localCnts[s]
	}
	return nil
}

func (exec *sumAvgExec[T, A]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *sumAvgExec[T, A]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*sumAvgExec[T, A])
	if exec.IsDistinct() {
		return exec.batchMergeArgs(&other.aggExec, offset, groups, true)
	}

	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		g1 := grp - 1
		g2 := uint64(offset + i)
		x1 := int(g1 >> aggBatchSizeShift)
		y1 := g1 & aggBatchSizeMask
		x2 := int(g2 >> aggBatchSizeShift)
		y2 := g2 & aggBatchSizeMask

		sums1 := chunkArr[T](exec.state[x1].vecs[0])
		sums2 := chunkRows[T](other.state[x2].vecs[0])

		if exec.isSum {
			if other.state[x2].vecs[0].IsNull(y2) {
				continue
			} else if exec.state[x1].vecs[0].IsNull(y1) {
				exec.state[x1].vecs[0].UnsetNull(y1)
				sums1[y1] = sums2[y2]
			} else {
				result := sums1[y1] + sums2[y2]
				if err := exec.ofCheck(sums1[y1], sums2[y2], result); err != nil {
					return err
				}
				sums1[y1] = result
			}
		} else {
			result := sums1[y1] + sums2[y2]
			if err := exec.ofCheck(sums1[y1], sums2[y2], result); err != nil {
				return err
			}
			sums1[y1] = result
			cnts1 := vector.MustFixedColNoTypeCheck[int64](exec.state[x1].vecs[1])
			cnts2 := vector.MustFixedColNoTypeCheck[int64](other.state[x2].vecs[1])
			cnts1[y1] += cnts2[y2]
		}
	}
	return nil
}

func (exec *sumAvgExec[T, A]) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (exec *sumAvgExec[T, A]) Flush() (_ []*vector.Vector, retErr error) {
	if exec.exactAvg {
		return exec.flushExactAvg()
	}

	resultType := exec.aggInfo.retType
	vecs := make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, v := range vecs {
				if v != nil {
					v.Free(exec.mp)
				}
			}
		}
	}()

	if exec.IsDistinct() {
		for i := range vecs {
			var err error
			vecs[i], err = exec.allocation.newVector(resultType)
			if err != nil {
				return nil, err
			}
			if err := vecs[i].PreExtend(int(exec.state[i].length), exec.mp); err != nil {
				return nil, err
			}
		}
		for i := range vecs {
			for j := 0; j < int(exec.state[i].length); j++ {
				if exec.state[i].argCnt[j] == 0 {
					if err := vector.AppendNull(vecs[i], exec.mp); err != nil {
						return nil, err
					}
					continue
				} else {
					sum := T(0)
					xcnt := 0
					err := exec.state[i].iter(uint16(j), func(k []byte) error {
						ptr := util.UnsafeFromBytes[A](k[kAggArgPrefixSz:])
						tmp := sum + T(*ptr)
						if err := exec.ofCheck(sum, T(*ptr), tmp); err != nil {
							return err
						}
						sum = tmp
						xcnt++
						return nil
					})

					if err != nil {
						return nil, err
					}
					if int(exec.state[i].argCnt[j]) != xcnt {
						panic(moerr.NewInternalErrorNoCtxf("invalid count: %d for y: %d, expected: %d", xcnt, j, exec.state[i].argCnt[j]))
					}

					if exec.isSum {
						if err := vector.AppendFixed(vecs[i], sum, false, exec.mp); err != nil {
							return nil, err
						}
					} else {
						if err := vector.AppendFixed(vecs[i], float64(sum)/float64(exec.state[i].argCnt[j]), false, exec.mp); err != nil {
							return nil, err
						}
					}
				}
			}
		}
	} else {
		for i := range vecs {
			sumVec := exec.state[i].vecs[0]
			sums := vector.MustFixedColNoTypeCheck[T](sumVec)

			// transfer sumVec
			vecs[i] = sumVec
			exec.state[i].vecs[0] = nil

			if !exec.isSum {
				// hack: avgs will reuse sums slice, float64 and int64 are the same size.
				avgs := util.UnsafeSliceCast[float64](sums)
				cntVec := exec.state[i].vecs[1]
				cnts := vector.MustFixedColNoTypeCheck[int64](cntVec)
				if err := preflightNullsForZeroCounts(sumVec, cnts, exec.mp); err != nil {
					return nil, err
				}
				for j, cnt := range cnts {
					if cnt == 0 {
						sumVec.SetNull(uint64(j))
					} else {
						avg := float64(sums[j]) / float64(cnt)
						avgs[j] = avg
					}
				}
				// free cntVec
				cntVec.Free(exec.mp)
				exec.state[i].vecs[1] = nil
			}

			// Fix result type.   note that for avg, the result type is
			// float64, for any int/uint type, the sum type is int64/uint64.
			// they are different types but SAME SIZE.   Let's just fix the
			// result type and be happy.
			*sumVec.GetType() = resultType

			// done transfer,
			exec.state[i].length = 0
			exec.state[i].capacity = 0
		}
	}
	return vecs, nil
}

func decimal128FromNativeSum[T float64 | int64 | uint64](sum T) types.Decimal128 {
	switch value := any(sum).(type) {
	case int64:
		return types.Decimal128FromInt64(value)
	case uint64:
		return types.Decimal128{B0_63: value}
	default:
		panic(moerr.NewInternalErrorNoCtxf("unsupported native AVG sum type %T", sum))
	}
}

func decimal256FromNativeSum[T float64 | int64 | uint64](sum T) types.Decimal256 {
	switch value := any(sum).(type) {
	case int64:
		return types.Decimal256FromInt64(value)
	case uint64:
		return types.Decimal256{B0_63: value}
	default:
		panic(moerr.NewInternalErrorNoCtxf("unsupported native AVG sum type %T", sum))
	}
}

// decimal128NativeIntegerAvg handles the usual native-integer AVG result
// scale without going through Decimal128.Scale/Mul128. The result scale for
// native integer AVG is four, so a single machine-word multiply followed by
// Div128 is both exact and materially cheaper than the general decimal path.
// An overflow asks the caller to retry through the Decimal256-capable path.
func decimal128NativeIntegerAvg[T float64 | int64 | uint64](sum T, count int64, resultScale int32) (types.Decimal128, error) {
	if count <= 0 {
		return types.Decimal128{}, moerr.NewInvalidInputNoCtxf("Decimal128 Div by Zero")
	}
	if resultScale < 0 || resultScale >= int32(len(types.Pow10)) {
		return types.Decimal128{}, moerr.NewInternalErrorNoCtxf("invalid native AVG result scale %d", resultScale)
	}

	var magnitude uint64
	negative := false
	switch value := any(sum).(type) {
	case int64:
		negative = value < 0
		if negative {
			magnitude = uint64(-(value + 1)) + 1
		} else {
			magnitude = uint64(value)
		}
	case uint64:
		magnitude = value
	default:
		return types.Decimal128{}, moerr.NewInternalErrorNoCtxf("unsupported native AVG sum type %T", sum)
	}

	hi, lo := bits.Mul64(magnitude, types.Pow10[resultScale])
	if hi>>63 != 0 {
		return types.Decimal128{}, moerr.NewInvalidInputNoCtxf("Decimal128 scale overflow")
	}
	var avg types.Decimal128
	if hi == 0 {
		// Most INT32 groups fit in one word after scaling. Use the same
		// half-up rule as Div128 without constructing a multi-word divisor.
		divisor := uint64(count)
		quotient, remainder := bits.Div64(0, lo, divisor)
		if remainder >= (divisor+1)/2 {
			quotient++
		}
		avg = types.Decimal128{B0_63: quotient}
	} else {
		var err error
		avg, err = (types.Decimal128{B0_63: lo, B64_127: hi}).Div128(types.Decimal128FromInt64(count))
		if err != nil {
			return types.Decimal128{}, err
		}
	}
	if negative {
		avg = avg.Minus()
	}
	return avg, nil
}

// flushExactAvg converts the native integer accumulator to the declared
// Decimal128/Decimal256 result only once per group. Row and batch filling stay
// on the compact integer path, which is important for the common INT AVG
// workload while still producing exact, scale-aware results.
func (exec *sumAvgExec[T, A]) flushExactAvg() (_ []*vector.Vector, retErr error) {
	resultType := exec.aggInfo.retType
	vecs := make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, vec := range vecs {
				if vec != nil {
					vec.Free(exec.mp)
				}
			}
		}
	}()

	if exec.IsDistinct() {
		for i := range vecs {
			var err error
			vecs[i], err = exec.allocation.newVector(resultType)
			if err != nil {
				return nil, err
			}
			if err = vecs[i].PreExtend(int(exec.state[i].length), exec.mp); err != nil {
				return nil, err
			}
			for j := 0; j < int(exec.state[i].length); j++ {
				count := exec.state[i].argCnt[j]
				if count == 0 {
					if err = vector.AppendNull(vecs[i], exec.mp); err != nil {
						return nil, err
					}
					continue
				}
				var sum T
				xcnt := 0
				err = exec.state[i].iter(uint16(j), func(k []byte) error {
					ptr := util.UnsafeFromBytes[A](k[kAggArgPrefixSz:])
					value := T(*ptr)
					result := sum + value
					if err := exec.ofCheck(sum, value, result); err != nil {
						return err
					}
					sum = result
					xcnt++
					return nil
				})
				if err != nil {
					return nil, err
				}
				if int(count) != xcnt {
					panic(moerr.NewInternalErrorNoCtxf("invalid count: %d for y: %d, expected: %d", xcnt, j, count))
				}
				var avg any
				if resultType.Oid == types.T_decimal256 {
					avg, err = decAvg[types.Decimal256](
						decimal256FromNativeSum(sum), int64(count), 0, resultType)
				} else {
					var avg128 types.Decimal128
					avg128, err = decimal128NativeIntegerAvg(sum, int64(count), resultType.Scale)
					if err != nil {
						avg128, err = decAvg[types.Decimal128](
							decimal128FromNativeSum(sum), int64(count), 0, resultType)
					}
					avg = avg128
				}
				if err != nil {
					return nil, err
				}
				if err = appendNativeAvgResult(vecs[i], avg, exec.mp); err != nil {
					return nil, err
				}
			}
		}
	} else {
		for i := range vecs {
			sumVec := exec.state[i].vecs[0]
			sums := vector.MustFixedColNoTypeCheck[T](sumVec)
			cntVec := exec.state[i].vecs[1]
			cnts := vector.MustFixedColNoTypeCheck[int64](cntVec)

			var err error
			vecs[i], err = exec.allocation.newVector(resultType)
			if err != nil {
				return nil, err
			}
			if err = vecs[i].PreExtend(int(exec.state[i].length), exec.mp); err != nil {
				return nil, err
			}
			// Build the whole fixed-width result in one append so the common
			// non-DISTINCT path does not pay per-group vector metadata/allocation
			// overhead. Wide integer constant expressions use Decimal256 here;
			// their native accumulator is still kept on the compact integer path.
			if resultType.Oid == types.T_decimal128 {
				values := make([]types.Decimal128, exec.state[i].length)
				nulls := make([]bool, exec.state[i].length)
				for j, count := range cnts {
					if count == 0 {
						nulls[j] = true
						continue
					}
					avg, avgErr := decimal128NativeIntegerAvg(sums[j], count, resultType.Scale)
					if avgErr != nil {
						avg, avgErr = decAvg(decimal128FromNativeSum(sums[j]), count, 0, resultType)
					}
					if avgErr != nil {
						return nil, avgErr
					}
					values[j] = avg
				}
				if err = vector.AppendFixedList(vecs[i], values, nulls, exec.mp); err != nil {
					return nil, err
				}
				sumVec.Free(exec.mp)
				cntVec.Free(exec.mp)
				exec.state[i].vecs[0] = nil
				exec.state[i].vecs[1] = nil
				exec.state[i].length = 0
				exec.state[i].capacity = 0
				continue
			}
			if resultType.Oid == types.T_decimal256 {
				values := make([]types.Decimal256, exec.state[i].length)
				nulls := make([]bool, exec.state[i].length)
				for j, count := range cnts {
					if count == 0 {
						nulls[j] = true
						continue
					}
					values[j], err = decAvg[types.Decimal256](
						decimal256FromNativeSum(sums[j]), count, 0, resultType)
					if err != nil {
						return nil, err
					}
				}
				if err = vector.AppendFixedList(vecs[i], values, nulls, exec.mp); err != nil {
					return nil, err
				}
				sumVec.Free(exec.mp)
				cntVec.Free(exec.mp)
				exec.state[i].vecs[0] = nil
				exec.state[i].vecs[1] = nil
				exec.state[i].length = 0
				exec.state[i].capacity = 0
				continue
			}
			for j, count := range cnts {
				if count == 0 {
					err = vector.AppendNull(vecs[i], exec.mp)
				} else {
					var avg any
					avg, err = decAvg[types.Decimal128](
						decimal128FromNativeSum(sums[j]), count, 0, resultType)
					if err == nil {
						err = appendNativeAvgResult(vecs[i], avg, exec.mp)
					}
				}
				if err != nil {
					return nil, err
				}
			}
			sumVec.Free(exec.mp)
			cntVec.Free(exec.mp)
			exec.state[i].vecs[0] = nil
			exec.state[i].vecs[1] = nil
			exec.state[i].length = 0
			exec.state[i].capacity = 0
		}
	}
	return vecs, nil
}

func appendNativeAvgResult(vec *vector.Vector, value any, mp *mpool.MPool) error {
	switch value := value.(type) {
	case types.Decimal128:
		return vector.AppendFixed(vec, value, false, mp)
	case types.Decimal256:
		return vector.AppendFixed(vec, value, false, mp)
	default:
		return moerr.NewInternalErrorNoCtxf("unsupported native AVG result type %T", value)
	}
}

type sumAvgDecimalArg interface {
	int64 | uint64 | types.Decimal64 | types.Decimal128 | types.Decimal256
}

type sumAvgDecimalState interface {
	types.Decimal128 | types.Decimal256
}

func sumAvgDecimalStateType[S sumAvgDecimalState](scale int32) types.Type {
	var state S
	switch any(state).(type) {
	case types.Decimal128:
		return types.New(types.T_decimal128, 38, scale)
	case types.Decimal256:
		return types.New(types.T_decimal256, 65, scale)
	}
	panic("unreachable")
}

func decimalStateFromArg[A sumAvgDecimalArg, S sumAvgDecimalState](v A, argScale int32) S {
	var state S
	switch any(state).(type) {
	case types.Decimal128:
		switch value := any(v).(type) {
		case int64:
			return any(types.Decimal128FromInt64(value)).(S)
		case uint64:
			return any(types.Decimal128{B0_63: value, B64_127: 0}).(S)
		case types.Decimal64:
			return any(types.Decimal128FromDecimal64(value, argScale)).(S)
		case types.Decimal128:
			return any(value).(S)
		}
	case types.Decimal256:
		switch value := any(v).(type) {
		case int64:
			return any(types.Decimal256FromInt64(value)).(S)
		case uint64:
			return any(types.Decimal256{B0_63: value}).(S)
		case types.Decimal64:
			return any(types.Decimal256FromDecimal128(types.Decimal128FromDecimal64(value, argScale))).(S)
		case types.Decimal128:
			return any(types.Decimal256FromDecimal128(value)).(S)
		case types.Decimal256:
			return any(value).(S)
		}
	}
	panic(moerr.NewInternalErrorNoCtxf("unsupported decimal conversion from %T", v))
}

func decimalStateAdd[S sumAvgDecimalState](left, right S) (S, error) {
	switch value := any(left).(type) {
	case types.Decimal128:
		result, err := value.Add128(any(right).(types.Decimal128))
		return any(result).(S), err
	case types.Decimal256:
		result, err := value.Add256(any(right).(types.Decimal256))
		return any(result).(S), err
	}
	panic(moerr.NewInternalErrorNoCtxf("unsupported decimal state type %T", left))
}

func decimalStateAddUnchecked[S sumAvgDecimalState](left, right S) S {
	switch value := any(left).(type) {
	case types.Decimal128:
		return any(value.Add128Unchecked(any(right).(types.Decimal128))).(S)
	case types.Decimal256:
		result, _ := value.Add256(any(right).(types.Decimal256))
		return any(result).(S)
	}
	panic("unreachable")
}

func decimalStateMinus[S sumAvgDecimalState](value S) S {
	switch v := any(value).(type) {
	case types.Decimal128:
		return any(v.Minus()).(S)
	case types.Decimal256:
		return any(v.Minus()).(S)
	}
	panic("unreachable")
}

type sumAvgDecExec[A sumAvgDecimalArg, S sumAvgDecimalState] struct {
	aggExec
	isSum              bool
	localAddSafe       bool // true when state type is wider than arg type (overflow impossible in local buffer)
	windowNonNullCount int64
}

func (*sumAvgDecExec[A, S]) sourcePreservingMerge() {}

func (exec *sumAvgDecExec[A, S]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *sumAvgDecExec[A, S]) windowSlidingSupported() bool {
	return !exec.IsDistinct() && exec.localAddSafe
}

func (exec *sumAvgDecExec[A, S]) addWindowRow(row int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if windowRowIsNull(vec, row) {
		return nil
	}
	if vec.IsConst() {
		row = 0
	}
	value := decimalStateFromArg[A, S](
		vector.MustFixedColNoTypeCheck[A](vec)[row], exec.aggInfo.argTypes[0].Scale)
	sums := chunkArr[S](exec.state[0].vecs[0])
	var err error
	if sums[0], err = decimalStateAdd(sums[0], value); err != nil {
		return err
	}
	if exec.isSum {
		exec.state[0].vecs[0].UnsetNull(0)
	} else {
		vector.MustFixedColNoTypeCheck[int64](exec.state[0].vecs[1])[0]++
	}
	exec.windowNonNullCount++
	return nil
}

func (exec *sumAvgDecExec[A, S]) removeWindowRow(row int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if windowRowIsNull(vec, row) {
		return nil
	}
	if exec.windowNonNullCount <= 0 {
		return moerr.NewInternalErrorNoCtx("sliding SUM/AVG state is empty")
	}
	if vec.IsConst() {
		row = 0
	}
	value := decimalStateFromArg[A, S](
		vector.MustFixedColNoTypeCheck[A](vec)[row], exec.aggInfo.argTypes[0].Scale)
	sums := chunkArr[S](exec.state[0].vecs[0])
	sums[0] = decimalStateAddUnchecked(sums[0], decimalStateMinus(value))
	exec.windowNonNullCount--
	if !exec.isSum {
		counts := vector.MustFixedColNoTypeCheck[int64](exec.state[0].vecs[1])
		if counts[0] <= 0 {
			return moerr.NewInternalErrorNoCtx("sliding AVG count state is empty")
		}
		counts[0]--
	}
	if exec.windowNonNullCount == 0 {
		var zero S
		sums[0] = zero
		if exec.isSum {
			exec.state[0].vecs[0].SetNull(0)
		}
	}
	return nil
}

func (exec *sumAvgDecExec[A, S]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	if exec.IsDistinct() {
		return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
	}
	if exec.isSum {
		return exec.bulkFillSumSingleGroup(groupIndex, vectors)
	}
	return exec.bulkFillAvgSingleGroup(groupIndex, vectors)
}

func (exec *sumAvgDecExec[A, S]) bulkFillSumSingleGroup(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	n := vec.Length()
	if n == 0 {
		return nil
	}

	argScale := exec.aggInfo.argTypes[0].Scale
	args := vector.MustFixedColNoTypeCheck[A](vec)
	isConst := vec.IsConst()
	hasNull := vec.HasNull()

	g := uint64(groupIndex)
	x := int(g >> aggBatchSizeShift)
	y := g & aggBatchSizeMask
	sums := chunkArr[S](exec.state[x].vecs[0])
	sumVec := exec.state[x].vecs[0]

	var localSum S
	filled := false
	for i := 0; i < n; i++ {
		row := i
		if isConst {
			row = 0
		}
		if hasNull && vec.IsNull(uint64(row)) {
			continue
		}

		val := decimalStateFromArg[A, S](args[row], argScale)
		if !filled {
			localSum = val
			filled = true
			continue
		}
		if exec.localAddSafe {
			localSum = decimalStateAddUnchecked(localSum, val)
		} else {
			var err error
			if localSum, err = decimalStateAdd(localSum, val); err != nil {
				return err
			}
		}
	}
	if !filled {
		return nil
	}

	if sumVec.IsNull(y) {
		sumVec.UnsetNull(y)
		sums[y] = localSum
		return nil
	}
	var err error
	if sums[y], err = decimalStateAdd(sums[y], localSum); err != nil {
		return err
	}
	return nil
}

func (exec *sumAvgDecExec[A, S]) bulkFillAvgSingleGroup(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	n := vec.Length()
	if n == 0 {
		return nil
	}

	argScale := exec.aggInfo.argTypes[0].Scale
	args := vector.MustFixedColNoTypeCheck[A](vec)
	isConst := vec.IsConst()
	hasNull := vec.HasNull()

	g := uint64(groupIndex)
	x := int(g >> aggBatchSizeShift)
	y := g & aggBatchSizeMask
	sums := chunkArr[S](exec.state[x].vecs[0])
	cnts := vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])

	var localSum S
	var localCnt int64
	for i := 0; i < n; i++ {
		row := i
		if isConst {
			row = 0
		}
		if hasNull && vec.IsNull(uint64(row)) {
			continue
		}

		val := decimalStateFromArg[A, S](args[row], argScale)
		if localCnt == 0 {
			localSum = val
			localCnt = 1
			continue
		}
		if exec.localAddSafe {
			localSum = decimalStateAddUnchecked(localSum, val)
		} else {
			var err error
			if localSum, err = decimalStateAdd(localSum, val); err != nil {
				return err
			}
		}
		localCnt++
	}
	if localCnt == 0 {
		return nil
	}

	var err error
	if sums[y], err = decimalStateAdd(sums[y], localSum); err != nil {
		return err
	}
	cnts[y] += localCnt
	return nil
}

func (exec *sumAvgDecExec[A, S]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if exec.IsDistinct() {
		return exec.batchFillArgs(offset, groups, vectors, true)
	}
	if exec.isSum {
		return exec.batchFillSum(offset, groups, vectors)
	}
	return exec.batchFillAvg(offset, groups, vectors)
}

func (exec *sumAvgDecExec[A, S]) batchFillSum(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	n := len(groups)
	if n == 0 {
		return nil
	}
	argScale := exec.aggInfo.argTypes[0].Scale
	args := vector.MustFixedColNoTypeCheck[A](vec)
	isConst := vec.IsConst()

	const slotEmpty = 0xFF
	const maxSlots = 255
	var slotOf [256]uint8
	var localSums [maxSlots]S
	var localGrps [maxSlots]uint64
	nSlots := 0

	for i := range slotOf {
		slotOf[i] = slotEmpty
	}

	hasNull := vec.HasNull()
	for i := 0; i < n; i++ {
		grp := groups[i]
		if grp == GroupNotMatched {
			continue
		}
		if hasNull && vec.IsNull(uint64(i)+uint64(offset)) {
			continue
		}

		g := grp - 1
		var raw A
		if isConst {
			raw = args[0]
		} else {
			raw = args[i+offset]
		}
		val := decimalStateFromArg[A, S](raw, argScale)
		h := uint8(g) ^ uint8(g>>8)
		for {
			s := slotOf[h]
			if s == slotEmpty {
				if nSlots >= maxSlots {
					x := int(g >> aggBatchSizeShift)
					y := g & aggBatchSizeMask
					sums := chunkArr[S](exec.state[x].vecs[0])
					sumVec := exec.state[x].vecs[0]
					if sumVec.IsNull(y) {
						sumVec.UnsetNull(y)
						sums[y] = val
					} else {
						var err error
						if sums[y], err = decimalStateAdd(sums[y], val); err != nil {
							return err
						}
					}
					break
				}
				s = uint8(nSlots)
				slotOf[h] = s
				localGrps[nSlots] = g
				localSums[nSlots] = val
				nSlots++
				break
			}
			if localGrps[s] == g {
				if exec.localAddSafe {
					localSums[s] = decimalStateAddUnchecked(localSums[s], val)
				} else {
					var err error
					if localSums[s], err = decimalStateAdd(localSums[s], val); err != nil {
						return err
					}
				}
				break
			}
			h++
		}
	}

	lastX := -1
	var sums *[AggBatchSize]S
	var sumVec *vector.Vector
	for s := 0; s < nSlots; s++ {
		g := localGrps[s]
		x := int(g >> aggBatchSizeShift)
		if x != lastX {
			lastX = x
			sums = chunkArr[S](exec.state[x].vecs[0])
			sumVec = exec.state[x].vecs[0]
		}
		y := g & aggBatchSizeMask
		if sumVec.IsNull(y) {
			sumVec.UnsetNull(y)
			sums[y] = localSums[s]
		} else {
			var err error
			if sums[y], err = decimalStateAdd(sums[y], localSums[s]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (exec *sumAvgDecExec[A, S]) batchFillAvg(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	n := len(groups)
	if n == 0 {
		return nil
	}
	argScale := exec.aggInfo.argTypes[0].Scale
	args := vector.MustFixedColNoTypeCheck[A](vec)
	isConst := vec.IsConst()

	const slotEmpty = 0xFF
	const maxSlots = 255
	var slotOf [256]uint8
	var localSums [maxSlots]S
	var localCnts [maxSlots]int64
	var localGrps [maxSlots]uint64
	nSlots := 0

	for i := range slotOf {
		slotOf[i] = slotEmpty
	}

	hasNull := vec.HasNull()
	for i := 0; i < n; i++ {
		grp := groups[i]
		if grp == GroupNotMatched {
			continue
		}
		if hasNull && vec.IsNull(uint64(i)+uint64(offset)) {
			continue
		}

		g := grp - 1
		var raw A
		if isConst {
			raw = args[0]
		} else {
			raw = args[i+offset]
		}
		val := decimalStateFromArg[A, S](raw, argScale)
		h := uint8(g) ^ uint8(g>>8)
		for {
			s := slotOf[h]
			if s == slotEmpty {
				if nSlots >= maxSlots {
					x := int(g >> aggBatchSizeShift)
					y := g & aggBatchSizeMask
					sums := chunkArr[S](exec.state[x].vecs[0])
					var err error
					if sums[y], err = decimalStateAdd(sums[y], val); err != nil {
						return err
					}
					cnts := vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])
					cnts[y]++
					break
				}
				s = uint8(nSlots)
				slotOf[h] = s
				localGrps[nSlots] = g
				localSums[nSlots] = val
				localCnts[nSlots] = 1
				nSlots++
				break
			}
			if localGrps[s] == g {
				if exec.localAddSafe {
					localSums[s] = decimalStateAddUnchecked(localSums[s], val)
				} else {
					var err error
					if localSums[s], err = decimalStateAdd(localSums[s], val); err != nil {
						return err
					}
				}
				localCnts[s]++
				break
			}
			h++
		}
	}

	lastX := -1
	var sums *[AggBatchSize]S
	var cnts []int64
	for s := 0; s < nSlots; s++ {
		g := localGrps[s]
		x := int(g >> aggBatchSizeShift)
		if x != lastX {
			lastX = x
			sums = chunkArr[S](exec.state[x].vecs[0])
			cnts = vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])
		}
		y := g & aggBatchSizeMask
		var err error
		if sums[y], err = decimalStateAdd(sums[y], localSums[s]); err != nil {
			return err
		}
		cnts[y] += localCnts[s]
	}
	return nil
}

func (exec *sumAvgDecExec[A, S]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *sumAvgDecExec[A, S]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	var err error
	other := next.(*sumAvgDecExec[A, S])
	if exec.IsDistinct() {
		return exec.batchMergeArgs(&other.aggExec, offset, groups, true)
	}

	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		g1 := grp - 1
		g2 := uint64(offset + i)
		x1 := int(g1 >> aggBatchSizeShift)
		y1 := g1 & aggBatchSizeMask
		x2 := int(g2 >> aggBatchSizeShift)
		y2 := g2 & aggBatchSizeMask

		sums1 := chunkArr[S](exec.state[x1].vecs[0])
		sums2 := chunkRows[S](other.state[x2].vecs[0])

		if exec.isSum {
			if other.state[x2].vecs[0].IsNull(y2) {
				continue
			} else if exec.state[x1].vecs[0].IsNull(y1) {
				exec.state[x1].vecs[0].UnsetNull(y1)
				sums1[y1] = sums2[y2]
			} else {
				if sums1[y1], err = decimalStateAdd(sums1[y1], sums2[y2]); err != nil {
					return err
				}
			}
		} else {
			if sums1[y1], err = decimalStateAdd(sums1[y1], sums2[y2]); err != nil {
				return err
			}
			cnts1 := vector.MustFixedColNoTypeCheck[int64](exec.state[x1].vecs[1])
			cnts2 := vector.MustFixedColNoTypeCheck[int64](other.state[x2].vecs[1])
			cnts1[y1] += cnts2[y2]
		}
	}
	return nil
}

func (exec *sumAvgDecExec[A, S]) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

var (
	decimal128PrecisionLimits = buildDecimal128PrecisionLimits()
	decimal256PrecisionLimits = buildDecimal256PrecisionLimits()
)

func buildDecimal128PrecisionLimits() (limits [39]types.Decimal128) {
	limits[0] = types.Decimal128FromInt64(1)
	ten := types.Decimal128FromInt64(10)
	for i := 1; i < len(limits); i++ {
		var err error
		limits[i], err = limits[i-1].Mul128(ten)
		if err != nil {
			panic(err)
		}
	}
	return limits
}

func buildDecimal256PrecisionLimits() (limits [77]types.Decimal256) {
	limits[0] = types.Decimal256FromInt64(1)
	ten := types.Decimal256FromInt64(10)
	for i := 1; i < len(limits); i++ {
		var err error
		limits[i], err = limits[i-1].Mul256(ten)
		if err != nil {
			panic(err)
		}
	}
	return limits
}

func decimal128FitsPrecision(value types.Decimal128, width int32) bool {
	limit := decimal128PrecisionLimits[width]
	if value.Sign() {
		return limit.Minus().Less(value)
	}
	return value.Less(limit)
}

func decimal256FitsPrecision(value types.Decimal256, width int32) bool {
	limit := decimal256PrecisionLimits[width]
	if value.Sign() {
		return limit.Minus().Less(value)
	}
	return value.Less(limit)
}

func decimal256AvgAtScale(value types.Decimal256, count int64, argScale, resultScale int32) (types.Decimal256, error) {
	if count <= 0 {
		return value, moerr.NewInvalidInputNoCtxf("Decimal256 Div by Zero")
	}
	if resultScale < argScale {
		return value, moerr.NewInternalErrorNoCtxf(
			"decimal avg result scale %d is below input scale %d", resultScale, argScale)
	}
	if value.Sign() {
		value = value.Minus()
	}
	var err error
	if value, err = value.Scale(resultScale - argScale); err != nil {
		return value, err
	}
	value, err = value.Div256(types.Decimal256FromInt64(count))
	if err != nil {
		return value, err
	}
	// Div256 operates on magnitudes. Restore the sign after the one and only
	// division/rounding step so negative AVG values use the same half-up rule.
	// The sign was stripped above, so this branch is intentionally based on the
	// original value captured before scaling.
	return value, nil
}

func decimal256AvgAtScaleSigned(value types.Decimal256, count int64, argScale, resultScale int32) (types.Decimal256, error) {
	negative := value.Sign()
	avg, err := decimal256AvgAtScale(value, count, argScale, resultScale)
	if err != nil {
		return avg, err
	}
	if negative {
		avg = avg.Minus()
	}
	return avg, nil
}

func decimal128FromDecimal256(value types.Decimal256) (types.Decimal128, bool) {
	if value.Sign() {
		if value.B128_191 != ^uint64(0) || value.B192_255 != ^uint64(0) || value.B64_127>>63 != 1 {
			return types.Decimal128{}, false
		}
	} else if value.B128_191 != 0 || value.B192_255 != 0 || value.B64_127>>63 != 0 {
		return types.Decimal128{}, false
	}
	return types.Decimal128{B0_63: value.B0_63, B64_127: value.B64_127}, true
}

// decimal128AvgAtScaleSigned keeps the common exact AVG path in Decimal128.
// Scaling the numerator before division makes Div128 perform the only
// rounding step at the declared result scale. The caller can fall back to
// Decimal256 when the scaled numerator does not fit in 128 bits.
func decimal128AvgAtScaleSigned(value types.Decimal128, count int64, argScale, resultScale int32) (types.Decimal128, error) {
	if count <= 0 {
		return value, moerr.NewInvalidInputNoCtxf("Decimal128 Div by Zero")
	}
	if resultScale < argScale {
		return value, moerr.NewInternalErrorNoCtxf(
			"decimal avg result scale %d is below input scale %d", resultScale, argScale)
	}
	negative := value.Sign()
	if negative {
		value = value.Minus()
	}
	scaled, err := value.Scale(resultScale - argScale)
	if err != nil {
		return value, err
	}
	avg, err := scaled.Div128(types.Decimal128FromInt64(count))
	if err != nil {
		return value, err
	}
	if negative {
		avg = avg.Minus()
	}
	return avg, nil
}

func decAvg[S sumAvgDecimalState](sum S, count int64, argScale int32, resultType types.Type) (S, error) {
	var zero S
	switch value := any(sum).(type) {
	case types.Decimal128:
		if resultType.Oid != types.T_decimal128 || resultType.Width <= 0 ||
			resultType.Width >= int32(len(decimal128PrecisionLimits)) ||
			resultType.Scale < 0 || resultType.Scale > resultType.Width {
			return zero, moerr.NewInternalErrorNoCtxf("invalid decimal avg result type %s", resultType.String())
		}
		if avg, err := decimal128AvgAtScaleSigned(value, count, argScale, resultType.Scale); err == nil {
			if !decimal128FitsPrecision(avg, resultType.Width) {
				return zero, moerr.NewInvalidInputNoCtxf(
					"%s beyond the range, can't be converted to Decimal128(%d,%d).",
					avg.Format(resultType.Scale), resultType.Width, resultType.Scale)
			}
			return any(avg).(S), nil
		}
		avgWide, err := decimal256AvgAtScaleSigned(
			types.Decimal256FromDecimal128(value), count, argScale, resultType.Scale)
		if err != nil {
			return zero, moerr.NewInvalidInputNoCtxf(
				"Decimal128 Div overflow: %s/%d", value.Format(argScale), count)
		}
		avg, ok := decimal128FromDecimal256(avgWide)
		if !ok {
			return zero, moerr.NewInvalidInputNoCtxf(
				"Decimal128 Div overflow: %s/%d", value.Format(argScale), count)
		}
		if !decimal128FitsPrecision(avg, resultType.Width) {
			return zero, moerr.NewInvalidInputNoCtxf(
				"%s beyond the range, can't be converted to Decimal128(%d,%d).",
				avg.Format(resultType.Scale), resultType.Width, resultType.Scale)
		}
		return any(avg).(S), nil
	case types.Decimal256:
		if resultType.Oid != types.T_decimal256 || resultType.Width <= 0 ||
			resultType.Width >= int32(len(decimal256PrecisionLimits)) ||
			resultType.Scale < 0 || resultType.Scale > resultType.Width {
			return zero, moerr.NewInternalErrorNoCtxf("invalid decimal avg result type %s", resultType.String())
		}
		avg, err := decimal256AvgAtScaleSigned(value, count, argScale, resultType.Scale)
		if err != nil {
			return zero, moerr.NewInvalidInputNoCtxf(
				"Decimal256 Div overflow: %s/%d", value.Format(argScale), count)
		}
		if !decimal256FitsPrecision(avg, resultType.Width) {
			return zero, moerr.NewInvalidInputNoCtxf(
				"%s beyond the range, can't be converted to Decimal256(%d,%d).",
				avg.Format(resultType.Scale), resultType.Width, resultType.Scale)
		}
		return any(avg).(S), nil
	}
	panic(moerr.NewInternalErrorNoCtxf("unsupported decimal avg state type %T", sum))
}

func sumAvgDecimalArgScale(typ types.Type) int32 {
	switch typ.Oid {
	case types.T_int64, types.T_uint64:
		return 0
	default:
		return typ.Scale
	}
}

func preflightNullsForZeroCounts(
	result *vector.Vector,
	counts []int64,
	mp *mpool.MPool,
) error {
	for _, count := range counts {
		if count == 0 {
			return result.PreExtendNulls(len(counts), mp)
		}
	}
	return nil
}

func (exec *sumAvgDecExec[A, S]) Flush() (_ []*vector.Vector, retErr error) {
	var err error
	resultType := exec.aggInfo.retType
	vecs := make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, v := range vecs {
				if v != nil {
					v.Free(exec.mp)
				}
			}
		}
	}()

	if exec.IsDistinct() {
		for i := range vecs {
			var err error
			vecs[i], err = exec.allocation.newVector(resultType)
			if err != nil {
				return nil, err
			}
			if err := vecs[i].PreExtend(int(exec.state[i].length), exec.mp); err != nil {
				return nil, err
			}
		}

		for i := range vecs {
			for j := 0; j < int(exec.state[i].length); j++ {
				if exec.state[i].argCnt[j] == 0 {
					if err := vector.AppendNull(vecs[i], exec.mp); err != nil {
						return nil, err
					}
					continue
				} else {
					var sum S
					xcnt := 0

					err = exec.state[i].iter(uint16(j), func(k []byte) error {
						ptr := util.UnsafeFromBytes[A](k[kAggArgPrefixSz:])
						val := decimalStateFromArg[A, S](*ptr, exec.aggInfo.argTypes[0].Scale)
						var fnerr error
						if sum, fnerr = decimalStateAdd[S](sum, val); fnerr != nil {
							return fnerr
						}
						xcnt++
						return nil
					})

					if err != nil {
						return nil, err
					}
					if int(exec.state[i].argCnt[j]) != xcnt {
						panic(moerr.NewInternalErrorNoCtxf("invalid count: %d for y: %d, expected: %d", xcnt, j, exec.state[i].argCnt[j]))
					}

					if exec.isSum {
						if err := vector.AppendFixed(vecs[i], sum, false, exec.mp); err != nil {
							return nil, err
						}
					} else {
						avg, err := decAvg(sum, int64(exec.state[i].argCnt[j]), sumAvgDecimalArgScale(exec.aggInfo.argTypes[0]), resultType)
						if err != nil {
							return nil, err
						}
						if err := vector.AppendFixed(vecs[i], avg, false, exec.mp); err != nil {
							return nil, err
						}
					}
				}
			}
		}
	} else {
		for i := range vecs {
			sumVec := exec.state[i].vecs[0]
			sums := vector.MustFixedColNoTypeCheck[S](sumVec)

			if !exec.isSum {
				cntVec := exec.state[i].vecs[1]
				cnts := vector.MustFixedColNoTypeCheck[int64](cntVec)
				if err := preflightNullsForZeroCounts(sumVec, cnts, exec.mp); err != nil {
					return nil, err
				}
				for j, cnt := range cnts {
					if cnt == 0 {
						sumVec.SetNull(uint64(j))
					} else {
						avg, err := decAvg(sums[j], cnt, sumAvgDecimalArgScale(exec.aggInfo.argTypes[0]), resultType)
						if err != nil {
							return nil, err
						}
						vector.SetFixedAtNoTypeCheck(sumVec, j, avg)
					}
				}
				cntVec.Free(exec.mp)
				exec.state[i].vecs[1] = nil
			}

			*sumVec.GetType() = resultType

			// transfer sumVec
			vecs[i] = sumVec
			exec.state[i].vecs[0] = nil
			exec.state[i].length = 0
			exec.state[i].capacity = 0
		}
	}
	return vecs, nil
}

func makeSumAvgExec(
	mp *mpool.MPool, isSum bool,
	aggID int64, isDistinct bool,
	param types.Type) AggFuncExec {

	switch param.Oid {
	case types.T_int8:
		return newSumAvgExec[int64, int8](mp, int64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_int16:
		return newSumAvgExec[int64, int16](mp, int64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_year:
		return newSumAvgExec[int64, types.MoYear](mp, int64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_int32:
		return newSumAvgExec[int64, int32](mp, int64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_int64:
		if !isSum && AvgReturnType([]types.Type{param}).Oid == types.T_decimal256 {
			return newSumAvgDecExec[int64, types.Decimal256](mp, isSum, aggID, isDistinct, param)
		}
		return newSumAvgDecExec[int64, types.Decimal128](mp, isSum, aggID, isDistinct, param)
	case types.T_uint8:
		return newSumAvgExec[uint64, uint8](mp, uint64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_uint16:
		return newSumAvgExec[uint64, uint16](mp, uint64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_uint32:
		return newSumAvgExec[uint64, uint32](mp, uint64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_uint64:
		if !isSum && AvgReturnType([]types.Type{param}).Oid == types.T_decimal256 {
			return newSumAvgDecExec[uint64, types.Decimal256](mp, isSum, aggID, isDistinct, param)
		}
		return newSumAvgDecExec[uint64, types.Decimal128](mp, isSum, aggID, isDistinct, param)
	case types.T_bit:
		return newSumAvgExec[uint64, uint64](mp, uint64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_float32:
		return newSumAvgExec[float64, float32](mp, float64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_float64:
		return newSumAvgExec[float64, float64](mp, float64OfCheck, isSum, aggID, isDistinct, param)
	case types.T_decimal64:
		return newSumDecimal64FastExec(mp, isSum, aggID, isDistinct, param)
	case types.T_decimal128:
		if !isSum && AvgReturnType([]types.Type{param}).Oid == types.T_decimal256 {
			return newSumAvgDecExec[types.Decimal128, types.Decimal256](mp, isSum, aggID, isDistinct, param)
		}
		return newSumDecimal128FastExec(mp, isSum, aggID, isDistinct, param)
	case types.T_decimal256:
		return newSumAvgDecExec[types.Decimal256, types.Decimal256](mp, isSum, aggID, isDistinct, param)
	default:
		panic(moerr.NewInternalErrorNoCtxf("unsupported type '%v' for sum/avg", param.Oid))
	}
}

func newSumAvgExec[T float64 | int64 | uint64, A types.Ints | types.UInts | types.Floats | types.MoYear](mp *mpool.MPool, ofCheck func(T, T, T) error, isSum bool, aggID int64, isDistinct bool, param types.Type) AggFuncExec {
	var exec sumAvgExec[T, A]
	exec.mp = mp
	exec.isSum = isSum
	exec.ofCheck = ofCheck
	var rt types.Type
	sumTyp := SumReturnType([]types.Type{param})
	avgTyp := AvgReturnType([]types.Type{param})
	if isSum {
		rt = sumTyp
	} else {
		rt = avgTyp
		exec.exactAvg = rt.Oid == types.T_decimal128 || rt.Oid == types.T_decimal256
	}
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: isDistinct,
		argTypes:   []types.Type{param},
		retType:    rt,
		emptyNull:  isSum,
		saveArg:    isDistinct,
	}

	if isSum {
		exec.aggInfo.stateTypes = []types.Type{sumTyp}
	} else {
		exec.aggInfo.stateTypes = []types.Type{sumTyp, types.T_int64.ToType()}
	}
	return &exec
}

func newSumAvgDecExec[A sumAvgDecimalArg, S sumAvgDecimalState](mp *mpool.MPool, isSum bool, aggID int64, isDistinct bool, param types.Type) AggFuncExec {
	var exec sumAvgDecExec[A, S]
	exec.mp = mp
	exec.isSum = isSum
	// Local buffer overflow is impossible when sizeof(S) > sizeof(A):
	//   Int64/Uint64→Decimal128: 255 × 10^20 < 10^38 ✓
	//   Decimal64→Decimal128: 255 × 10^18 < 10^38 ✓
	//   Decimal128→Decimal256: 255 × 10^38 < 10^76 ✓
	//   Decimal256→Decimal256: 255 × 10^76 > 10^76 ✗
	// Valid instantiations: [Int64,Decimal128], [Uint64,Decimal128],
	// [Decimal64,Decimal128], [Decimal128,Decimal256], [Decimal256,Decimal256].
	// If a [Decimal128,Decimal128] instantiation is ever added, this must be updated.
	var a A
	switch any(a).(type) {
	case int64, uint64, types.Decimal64, types.Decimal128:
		exec.localAddSafe = true
	default:
		exec.localAddSafe = false
	}

	var rt types.Type
	sumTyp := SumReturnType([]types.Type{param})
	avgTyp := AvgReturnType([]types.Type{param})
	if isSum {
		rt = sumTyp
	} else {
		rt = avgTyp
	}

	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: isDistinct,
		argTypes:   []types.Type{param},
		retType:    rt,
		emptyNull:  isSum,
		saveArg:    isDistinct,
	}

	if isSum {
		exec.aggInfo.stateTypes = []types.Type{sumTyp}
	} else {
		exec.aggInfo.stateTypes = []types.Type{
			sumAvgDecimalStateType[S](sumAvgDecimalArgScale(param)),
			types.T_int64.ToType(),
		}
	}

	return &exec
}
