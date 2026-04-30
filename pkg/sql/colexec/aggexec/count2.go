// Copyright 2024 Matrix Origin
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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type countStarExec struct {
	aggExec
	extra int64
}

func (exec *countStarExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	x, y := exec.getXY(uint64(groupIndex))
	vals := vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[0])
	vals[y] += 1
	return nil
}

func (exec *countStarExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	x, y := exec.getXY(uint64(groupIndex))
	vals := vector.MustFixedColNoTypeCheck[int64](exec.state[x].vecs[0])
	vals[y] += int64(vectors[0].Length())
	return nil
}

func (exec *countStarExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	n := len(groups)
	if n == 0 {
		return nil
	}

	const slotEmpty = 0xFF
	const maxSlots = 255
	var slotOf [256]uint8
	var localCnts [maxSlots]int64
	var localGrps [maxSlots]uint64
	nSlots := 0

	for i := range slotOf {
		slotOf[i] = slotEmpty
	}

	for i := 0; i < n; i++ {
		grp := groups[i]
		if grp == GroupNotMatched {
			continue
		}
		g := grp - 1
		h := uint8(g) ^ uint8(g>>8)
		for {
			s := slotOf[h]
			if s == slotEmpty {
				if nSlots >= maxSlots {
					x := int(g >> aggBatchSizeShift)
					y := g & aggBatchSizeMask
					vals := (*[AggBatchSize]int64)(exec.chunkPtrs[x])
					vals[y]++
					break
				}
				s = uint8(nSlots)
				slotOf[h] = s
				localGrps[nSlots] = g
				localCnts[nSlots] = 1
				nSlots++
				break
			}
			if localGrps[s] == g {
				localCnts[s]++
				break
			}
			h++
		}
	}

	for s := 0; s < nSlots; s++ {
		g := localGrps[s]
		x := int(g >> aggBatchSizeShift)
		y := g & aggBatchSizeMask
		vals := (*[AggBatchSize]int64)(exec.chunkPtrs[x])
		vals[y] += localCnts[s]
	}
	return nil
}

func (exec *countStarExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *countStarExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*countStarExec)
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
		vals1 := (*[AggBatchSize]int64)(exec.chunkPtrs[x1])
		vals2 := (*[AggBatchSize]int64)(other.chunkPtrs[x2])
		vals1[y1] += vals2[y2]
	}
	return nil
}

func (exec *countStarExec) SetExtraInformation(partialResult any, _ int) error {
	exec.extra = partialResult.(int64)
	return nil
}

func (exec *countStarExec) Flush() ([]*vector.Vector, error) {
	// transfer vector to result
	vecs := make([]*vector.Vector, len(exec.state))
	for i := range vecs {
		vecs[i] = exec.state[i].vecs[0]
		exec.state[i].vecs[0] = nil
		exec.state[i].length = 0
		exec.state[i].capacity = 0

		if exec.extra != 0 {
			vals := vector.MustFixedColNoTypeCheck[int64](vecs[i])
			for j := range vals {
				vals[j] += exec.extra
			}
		}
	}
	return vecs, nil
}

type countColumnExec struct {
	aggExec
	extra int64
}

func (exec *countColumnExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *countColumnExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *countColumnExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if exec.IsDistinct() {
		return exec.batchFillArgs(offset, groups, vectors, true)
	}

	vec := vectors[0]
	n := len(groups)
	if n == 0 {
		return nil
	}

	const slotEmpty = 0xFF
	const maxSlots = 255
	var slotOf [256]uint8
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
		h := uint8(g) ^ uint8(g>>8)
		for {
			s := slotOf[h]
			if s == slotEmpty {
				if nSlots >= maxSlots {
					x := int(g >> aggBatchSizeShift)
					y := g & aggBatchSizeMask
					vals := (*[AggBatchSize]int64)(exec.chunkPtrs[x])
					vals[y]++
					break
				}
				s = uint8(nSlots)
				slotOf[h] = s
				localGrps[nSlots] = g
				localCnts[nSlots] = 1
				nSlots++
				break
			}
			if localGrps[s] == g {
				localCnts[s]++
				break
			}
			h++
		}
	}

	for s := 0; s < nSlots; s++ {
		g := localGrps[s]
		x := int(g >> aggBatchSizeShift)
		y := g & aggBatchSizeMask
		vals := (*[AggBatchSize]int64)(exec.chunkPtrs[x])
		vals[y] += localCnts[s]
	}
	return nil
}

func (exec *countColumnExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *countColumnExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	if exec.IsDistinct() {
		nextExec := next.(*countColumnExec)
		return exec.batchMergeArgs(&nextExec.aggExec, offset, groups, true)
	}

	other := next.(*countColumnExec)
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
		vals1 := (*[AggBatchSize]int64)(exec.chunkPtrs[x1])
		vals2 := (*[AggBatchSize]int64)(other.chunkPtrs[x2])
		vals1[y1] += vals2[y2]
	}
	return nil
}

func (exec *countColumnExec) SetExtraInformation(partialResult any, _ int) error {
	exec.extra = partialResult.(int64)
	return nil
}

func (exec *countColumnExec) Flush() (_ []*vector.Vector, retErr error) {
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
		if exec.extra != 0 {
			panic(moerr.NewInternalErrorNoCtx("extra is not supported for distinct count"))
		}

		for i := range vecs {
			vecs[i] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
			if err := vecs[i].PreExtend(int(exec.state[i].length), exec.mp); err != nil {
				return nil, err
			}
			vecs[i].SetLength(int(exec.state[i].length))
			vals := vector.MustFixedColNoTypeCheck[int64](vecs[i])
			for j := range vals {
				vals[j] += int64(exec.state[i].argCnt[j])
			}
		}
	} else {
		for i := range vecs {
			vecs[i] = exec.state[i].vecs[0]
			exec.state[i].vecs[0] = nil
			exec.state[i].length = 0
			exec.state[i].capacity = 0
			if exec.extra != 0 {
				vals := vector.MustFixedColNoTypeCheck[int64](vecs[i])
				for j := range vals {
					vals[j] += int64(exec.extra)
				}
			}
		}
	}
	return vecs, nil
}

func makeCount(
	mp *mpool.MPool, isStar bool,
	aggID int64, isDistinct bool,
	param types.Type) AggFuncExec {
	if isStar {
		return newCountStarExec(mp, aggID, isDistinct, param)
	}
	return newCountColumnExec(mp, aggID, isDistinct, param)
}

func newCountStarExec(mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type) AggFuncExec {
	var exec countStarExec
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: false, // count(*) is does not need to know anything of distinct
		argTypes:   []types.Type{param},
		retType:    types.T_int64.ToType(),
		stateTypes: []types.Type{types.T_int64.ToType()},
		emptyNull:  false,
		saveArg:    false,
	}
	return &exec
}

func newCountColumnExec(mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type) AggFuncExec {
	var exec countColumnExec
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: isDistinct,
		argTypes:   []types.Type{param},
		retType:    types.T_int64.ToType(),
		stateTypes: []types.Type{types.T_int64.ToType()},
		emptyNull:  false,
		saveArg:    isDistinct,
	}
	return &exec
}
