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

package aggexec

// Specialized non-generic SUM/AVG executors for Decimal64 and Decimal128 inputs.
// Eliminates interface boxing, type switches, and per-add overflow checks.

import (
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// ---- Decimal64 SUM/AVG ----

type sumDecimal64FastExec struct {
	aggExec
	isSum bool
}

func newSumDecimal64FastExec(mp *mpool.MPool, isSum bool, aggID int64, isDistinct bool, param types.Type) AggFuncExec {
	var exec sumDecimal64FastExec
	exec.mp = mp
	exec.isSum = isSum
	sumTyp := SumReturnType([]types.Type{param})
	avgTyp := AvgReturnType([]types.Type{param})
	var rt types.Type
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
		emptyNull:  false, // Flush uses cnts==0 for null detection; must be false for spill correctness
		saveArg:    isDistinct,
	}
	// Always allocate sum + count. Count tracks group-has-data for SUM
	// and row count for AVG division. This avoids per-row null checks on accumulator.
	exec.aggInfo.stateTypes = []types.Type{sumTyp, types.T_int64.ToType()}
	return &exec
}

func (exec *sumDecimal64FastExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *sumDecimal64FastExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *sumDecimal64FastExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if exec.IsDistinct() {
		return exec.batchFillArgs(offset, groups, vectors, true)
	}
	return exec.batchFill(offset, groups, vectors)
}

func (exec *sumDecimal64FastExec) batchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConstNull() {
		return nil
	}
	vals := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)
	constMask := -min(1, len(vals)-1)
	hasNull := vec.HasNull()
	var np *bitmap.Bitmap
	if hasNull {
		np = vec.GetNulls().GetBitmap()
	}

	const maxSlots = 255
	var slotOf [256]uint8
	var localSums [maxSlots]types.Decimal128
	var localCnts [maxSlots]int64
	var localGrps [maxSlots]uint64
	nSlots := 0
	for i := range slotOf {
		slotOf[i] = 0xFF
	}

	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		idx := i + offset
		if hasNull && np.Contains(uint64(idx)) {
			continue
		}
		g := grp - 1
		raw := vals[idx&constMask]
		val := types.Decimal128{B0_63: uint64(raw), B64_127: uint64(int64(raw) >> 63)}

		for h := uint8(g) ^ uint8(g>>8); ; h++ {
			s := slotOf[h]
			if s == 0xFF {
				if nSlots >= maxSlots {
					x := int(g >> aggBatchSizeShift)
					y := g & aggBatchSizeMask
					sums := chunkArr[types.Decimal128](exec.state[x].vecs[0])
					sums[y] = sums[y].Add128Unchecked(val)
					vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])[y]++
					break
				}
				slotOf[h] = uint8(nSlots)
				localGrps[nSlots] = g
				localSums[nSlots] = val
				localCnts[nSlots] = 1
				nSlots++
				break
			}
			if localGrps[s] == g {
				localSums[s] = localSums[s].Add128Unchecked(val)
				localCnts[s]++
				break
			}
		}
	}

	lastX := -1
	var sums *[AggBatchSize]types.Decimal128
	var cnts []int64
	for s := 0; s < nSlots; s++ {
		g := localGrps[s]
		x := int(g >> aggBatchSizeShift)
		if x != lastX {
			lastX = x
			sums = chunkArr[types.Decimal128](exec.state[x].vecs[0])
			cnts = vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])
		}
		y := g & aggBatchSizeMask
		sums[y] = sums[y].Add128Unchecked(localSums[s])
		cnts[y] += localCnts[s]
	}
	return nil
}

func (exec *sumDecimal64FastExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *sumDecimal64FastExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*sumDecimal64FastExec)
	if exec.IsDistinct() {
		return exec.batchMergeArgs(&other.aggExec, offset, groups, true)
	}

	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		x1, y1 := exec.getXY(grp - 1)
		x2, y2 := other.getXY(uint64(offset + i))
		sums1 := vector.MustFixedColNoTypeCheck[types.Decimal128](exec.state[x1].vecs[0])
		sums2 := vector.MustFixedColNoTypeCheck[types.Decimal128](other.state[x2].vecs[0])
		cnts1 := vector.MustFixedColNoTypeCheck[int64](exec.state[x1].vecs[1])
		cnts2 := vector.MustFixedColNoTypeCheck[int64](other.state[x2].vecs[1])

		sums1[y1] = sums1[y1].Add128Unchecked(sums2[y2])
		cnts1[y1] += cnts2[y2]
	}
	return nil
}

func (exec *sumDecimal64FastExec) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (exec *sumDecimal64FastExec) Flush() (_ []*vector.Vector, retErr error) {
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
			vecs[i] = vector.NewOffHeapVecWithType(resultType)
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
				}
				var sum types.Decimal128
				err := exec.state[i].iter(uint16(j), func(k []byte) error {
					ptr := util.UnsafeFromBytes[types.Decimal64](k[kAggArgPrefixSz:])
					raw := *ptr
					hi := uint64(int64(raw) >> 63)
					val := types.Decimal128{B0_63: uint64(raw), B64_127: hi}
					sum = sum.Add128Unchecked(val)
					return nil
				})
				if err != nil {
					return nil, err
				}
				if exec.isSum {
					if err := vector.AppendFixed(vecs[i], sum, false, exec.mp); err != nil {
						return nil, err
					}
				} else {
					cnt := int64(exec.state[i].argCnt[j])
					avg := decAvg[types.Decimal128](sum, cnt, exec.aggInfo.argTypes[0].Scale, resultType.Scale)
					if err := vector.AppendFixed(vecs[i], avg, false, exec.mp); err != nil {
						return nil, err
					}
				}
			}
		}
	} else {
		for i := range vecs {
			sumVec := exec.state[i].vecs[0]
			sums := vector.MustFixedColNoTypeCheck[types.Decimal128](sumVec)
			cntVec := exec.state[i].vecs[1]
			cnts := vector.MustFixedColNoTypeCheck[int64](cntVec)

			if exec.isSum {
				for j, cnt := range cnts {
					if cnt == 0 {
						sumVec.SetNull(uint64(j))
					} else {
						sumVec.UnsetNull(uint64(j))
					}
				}
			} else {
				for j, cnt := range cnts {
					if cnt == 0 {
						sumVec.SetNull(uint64(j))
					} else {
						avg := decAvg[types.Decimal128](sums[j], cnt, exec.aggInfo.argTypes[0].Scale, resultType.Scale)
						vector.SetFixedAtNoTypeCheck(sumVec, j, avg)
					}
				}
			}
			cntVec.Free(exec.mp)
			exec.state[i].vecs[1] = nil

			sumVec.GetType().Scale = resultType.Scale
			vecs[i] = sumVec
			exec.state[i].vecs[0] = nil
			exec.state[i].length = 0
			exec.state[i].capacity = 0
		}
	}
	return vecs, nil
}

// ---- Decimal128 SUM/AVG ----

type sumDecimal128FastExec struct {
	aggExec
	isSum         bool
	overflowCheck bool // true when input precision > 28 (SUM can overflow)
}

func newSumDecimal128FastExec(mp *mpool.MPool, isSum bool, aggID int64, isDistinct bool, param types.Type) AggFuncExec {
	var exec sumDecimal128FastExec
	exec.mp = mp
	exec.isSum = isSum
	// Width ≤ 28: max input value ≈ 10^28; overflow requires ≈17 billion rows per group.
	// Width > 28: overflow is reachable with fewer rows, so use checked Add128.
	exec.overflowCheck = param.Width > 28
	sumTyp := SumReturnType([]types.Type{param})
	avgTyp := AvgReturnType([]types.Type{param})
	var rt types.Type
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
		emptyNull:  false, // Flush uses cnts==0 for null detection; must be false for spill correctness
		saveArg:    isDistinct,
	}
	// Always allocate sum + count. Count tracks group-has-data for SUM
	// and row count for AVG division. This avoids per-row null checks on accumulator.
	exec.aggInfo.stateTypes = []types.Type{sumTyp, types.T_int64.ToType()}
	return &exec
}

func (exec *sumDecimal128FastExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *sumDecimal128FastExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *sumDecimal128FastExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if exec.IsDistinct() {
		return exec.batchFillArgs(offset, groups, vectors, true)
	}
	return exec.batchFill(offset, groups, vectors)
}

func (exec *sumDecimal128FastExec) batchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConstNull() {
		return nil
	}
	vals := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
	constMask := -min(1, len(vals)-1)
	hasNull := vec.HasNull()
	var np *bitmap.Bitmap
	if hasNull {
		np = vec.GetNulls().GetBitmap()
	}
	checked := exec.overflowCheck

	const maxSlots = 255
	var slotOf [256]uint8
	var localSums [maxSlots]types.Decimal128
	var localCnts [maxSlots]int64
	var localGrps [maxSlots]uint64
	nSlots := 0
	for i := range slotOf {
		slotOf[i] = 0xFF
	}

	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		idx := i + offset
		if hasNull && np.Contains(uint64(idx)) {
			continue
		}
		g := grp - 1
		val := vals[idx&constMask]

		for h := uint8(g) ^ uint8(g>>8); ; h++ {
			s := slotOf[h]
			if s == 0xFF {
				if nSlots >= maxSlots {
					x := int(g >> aggBatchSizeShift)
					y := g & aggBatchSizeMask
					sums := chunkArr[types.Decimal128](exec.state[x].vecs[0])
					if checked {
						var err error
						if sums[y], err = sums[y].Add128(val); err != nil {
							return err
						}
					} else {
						sums[y] = sums[y].Add128Unchecked(val)
					}
					vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])[y]++
					break
				}
				slotOf[h] = uint8(nSlots)
				localGrps[nSlots] = g
				localSums[nSlots] = val
				localCnts[nSlots] = 1
				nSlots++
				break
			}
			if localGrps[s] == g {
				if checked {
					var err error
					if localSums[s], err = localSums[s].Add128(val); err != nil {
						return err
					}
				} else {
					localSums[s] = localSums[s].Add128Unchecked(val)
				}
				localCnts[s]++
				break
			}
		}
	}

	lastX := -1
	var sums *[AggBatchSize]types.Decimal128
	var cnts []int64
	for s := 0; s < nSlots; s++ {
		g := localGrps[s]
		x := int(g >> aggBatchSizeShift)
		if x != lastX {
			lastX = x
			sums = chunkArr[types.Decimal128](exec.state[x].vecs[0])
			cnts = vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[1])
		}
		y := g & aggBatchSizeMask
		if checked {
			var err error
			if sums[y], err = sums[y].Add128(localSums[s]); err != nil {
				return err
			}
		} else {
			sums[y] = sums[y].Add128Unchecked(localSums[s])
		}
		cnts[y] += localCnts[s]
	}
	return nil
}

func (exec *sumDecimal128FastExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *sumDecimal128FastExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*sumDecimal128FastExec)
	if exec.IsDistinct() {
		return exec.batchMergeArgs(&other.aggExec, offset, groups, true)
	}

	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		x1, y1 := exec.getXY(grp - 1)
		x2, y2 := other.getXY(uint64(i) + uint64(offset))
		sums1 := vector.MustFixedColNoTypeCheck[types.Decimal128](exec.state[x1].vecs[0])
		sums2 := vector.MustFixedColNoTypeCheck[types.Decimal128](other.state[x2].vecs[0])
		cnts1 := vector.MustFixedColNoTypeCheck[int64](exec.state[x1].vecs[1])
		cnts2 := vector.MustFixedColNoTypeCheck[int64](other.state[x2].vecs[1])

		if exec.overflowCheck {
			var err error
			sums1[y1], err = sums1[y1].Add128(sums2[y2])
			if err != nil {
				return err
			}
		} else {
			sums1[y1] = sums1[y1].Add128Unchecked(sums2[y2])
		}
		cnts1[y1] += cnts2[y2]
	}
	return nil
}

func (exec *sumDecimal128FastExec) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (exec *sumDecimal128FastExec) Flush() (_ []*vector.Vector, retErr error) {
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
			vecs[i] = vector.NewOffHeapVecWithType(resultType)
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
				}
				var sum types.Decimal128
				err := exec.state[i].iter(uint16(j), func(k []byte) error {
					ptr := util.UnsafeFromBytes[types.Decimal128](k[kAggArgPrefixSz:])
					if exec.overflowCheck {
						var addErr error
						sum, addErr = sum.Add128(*ptr)
						if addErr != nil {
							return addErr
						}
					} else {
						sum = sum.Add128Unchecked(*ptr)
					}
					return nil
				})
				if err != nil {
					return nil, err
				}
				if exec.isSum {
					if err := vector.AppendFixed(vecs[i], sum, false, exec.mp); err != nil {
						return nil, err
					}
				} else {
					cnt := int64(exec.state[i].argCnt[j])
					avg := decAvg[types.Decimal128](sum, cnt, exec.aggInfo.argTypes[0].Scale, resultType.Scale)
					if err := vector.AppendFixed(vecs[i], avg, false, exec.mp); err != nil {
						return nil, err
					}
				}
			}
		}
	} else {
		for i := range vecs {
			sumVec := exec.state[i].vecs[0]
			sums := vector.MustFixedColNoTypeCheck[types.Decimal128](sumVec)
			cntVec := exec.state[i].vecs[1]
			cnts := vector.MustFixedColNoTypeCheck[int64](cntVec)

			if exec.isSum {
				for j, cnt := range cnts {
					if cnt == 0 {
						sumVec.SetNull(uint64(j))
					} else {
						sumVec.UnsetNull(uint64(j))
					}
				}
			} else {
				for j, cnt := range cnts {
					if cnt == 0 {
						sumVec.SetNull(uint64(j))
					} else {
						avg := decAvg[types.Decimal128](sums[j], cnt, exec.aggInfo.argTypes[0].Scale, resultType.Scale)
						vector.SetFixedAtNoTypeCheck(sumVec, j, avg)
					}
				}
			}
			cntVec.Free(exec.mp)
			exec.state[i].vecs[1] = nil

			sumVec.GetType().Scale = resultType.Scale
			vecs[i] = sumVec
			exec.state[i].vecs[0] = nil
			exec.state[i].length = 0
			exec.state[i].capacity = 0
		}
	}
	return vecs, nil
}
