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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

var (
	CountReturnType = func(_ []types.Type) types.Type {
		return types.T_int64.ToType()
	}
)

// count is a special agg because it can ignore what the value is but only if it was a null.
type countColumnExec struct {
	singleAggInfo
	singleAggExecExtraInformation
	distinctHash

	ret aggResultWithFixedType[int64]
}

func (exec *countColumnExec) GetOptResult() SplitResult {
	return &exec.ret.optSplitResult
}

func (exec *countColumnExec) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, em, err := exec.ret.marshalToBytes()
	if err != nil {
		return nil, err
	}
	encoded := EncodedAgg{
		Info:    d,
		Result:  r,
		Empties: em,
		Groups:  nil,
	}
	return encoded.Marshal()
}

func (exec *countColumnExec) unmarshal(_ *mpool.MPool, result, empties, _ [][]byte) error {
	return exec.ret.unmarshalFromBytes(result, empties)
}

func newCountColumnExecExec(mg AggMemoryManager, info singleAggInfo) AggFuncExec {
	exec := &countColumnExec{
		singleAggInfo: info,
		ret:           initAggResultWithFixedTypeResult[int64](mg, info.retType, false, 0),
	}
	if info.distinct {
		exec.distinctHash = newDistinctHash()
	}
	return exec
}

func (exec *countColumnExec) GroupGrow(more int) error {
	if exec.IsDistinct() {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	return exec.ret.grows(more)
}

func (exec *countColumnExec) PreAllocateGroups(more int) error {
	return exec.ret.preExtend(more)
}

func (exec *countColumnExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	if vectors[0].IsNull(uint64(row)) {
		return nil
	}

	if exec.IsDistinct() {
		if need, err := exec.distinctHash.fill(groupIndex, vectors, row); err != nil || !need {
			return err
		}
	}

	exec.ret.updateNextAccessIdx(groupIndex)
	exec.ret.set(exec.ret.get() + 1)
	return nil
}

func (exec *countColumnExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	if vectors[0].IsConstNull() {
		return nil
	}
	exec.ret.updateNextAccessIdx(groupIndex)

	old := exec.ret.get()
	if exec.IsDistinct() {
		if vectors[0].IsConst() {
			if need, err := exec.distinctHash.fill(groupIndex, vectors, 0); err != nil || !need {
				return err
			}
			old++

		} else {
			needs, err := exec.distinctHash.bulkFill(groupIndex, vectors)
			if err != nil {
				return err
			}
			nsp := vectors[0].GetNulls()
			for i, j := uint64(0), uint64(len(needs)); i < j; i++ {
				if needs[i] && !nsp.Contains(i) {
					old++
				}
			}
		}

	} else {
		old += int64(vectors[0].Length() - vectors[0].GetNulls().Count())
	}
	exec.ret.set(old)
	return nil
}

func (exec *countColumnExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if vectors[0].IsConstNull() {
		return nil
	}

	vs := exec.ret.values
	if vectors[0].IsConst() || vectors[0].GetNulls().IsEmpty() {
		if exec.IsDistinct() {
			needs, err := exec.distinctHash.batchFill(vectors, offset, groups)
			if err != nil {
				return err
			}
			for i, group := range groups {
				if needs[i] && group != GroupNotMatched {
					x, y := exec.ret.updateNextAccessIdx(int(group - 1))

					vs[x][y]++
				}
			}
			return nil
		}

		for _, group := range groups {
			if group != GroupNotMatched {
				x, y := exec.ret.updateNextAccessIdx(int(group - 1))

				vs[x][y]++
			}
		}
		return nil
	}

	if exec.IsDistinct() {
		needs, err := exec.distinctHash.batchFill(vectors, offset, groups)
		if err != nil {
			return err
		}

		if vectors[0].HasNull() {
			nsp := vectors[0].GetNulls()
			u64Offset := uint64(offset)
			for i, j := uint64(0), uint64(len(groups)); i < j; i++ {
				if needs[i] && !nsp.Contains(i+u64Offset) && groups[i] != GroupNotMatched {
					x, y := exec.ret.updateNextAccessIdx(int(groups[i] - 1))

					vs[x][y]++
				}
			}

		} else {
			for i, group := range groups {
				if needs[i] && group != GroupNotMatched {
					x, y := exec.ret.updateNextAccessIdx(int(group - 1))

					vs[x][y]++
				}
			}
			return nil
		}
		return nil
	}

	if vectors[0].HasNull() {
		nsp := vectors[0].GetNulls()
		u64Offset := uint64(offset)
		for i, j := uint64(0), uint64(len(groups)); i < j; i++ {
			if groups[i] != GroupNotMatched {
				if !nsp.Contains(i + u64Offset) {
					x, y := exec.ret.updateNextAccessIdx(int(groups[i] - 1))

					vs[x][y]++
				}
			}
		}

	} else {
		for _, group := range groups {
			if group != GroupNotMatched {
				x, y := exec.ret.updateNextAccessIdx(int(group - 1))
				vs[x][y]++
			}
		}
	}
	return nil
}

func (exec *countColumnExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*countColumnExec)

	exec.ret.updateNextAccessIdx(groupIdx1)
	other.ret.updateNextAccessIdx(groupIdx2)
	exec.ret.set(exec.ret.get() + other.ret.get())
	return exec.distinctHash.merge(&other.distinctHash)
}

func (exec *countColumnExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*countColumnExec)
	vs1 := exec.ret.values
	vs2 := other.ret.values

	for i := range groups {
		if groups[i] == GroupNotMatched {
			continue
		}
		x1, y1 := exec.ret.updateNextAccessIdx(int(groups[i]) - 1)
		x2, y2 := other.ret.updateNextAccessIdx(i + offset)
		vs1[x1][y1] += vs2[x2][y2]
	}
	return exec.distinctHash.merge(&other.distinctHash)
}

func (exec *countColumnExec) Flush() ([]*vector.Vector, error) {
	if exec.partialResult != nil {
		x, y := exec.ret.updateNextAccessIdx(exec.partialGroup)
		exec.ret.values[x][y] += exec.partialResult.(int64)
	}
	return exec.ret.flushAll(), nil
}

func (exec *countColumnExec) Free() {
	exec.ret.free()
	exec.distinctHash.free()
}

type countStarExec struct {
	singleAggInfo
	singleAggExecExtraInformation
	ret aggResultWithFixedType[int64]
}

func (exec *countStarExec) GetOptResult() SplitResult {
	return &exec.ret.optSplitResult
}

func (exec *countStarExec) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, em, err := exec.ret.marshalToBytes()
	if err != nil {
		return nil, err
	}
	encoded := EncodedAgg{
		Info:    d,
		Result:  r,
		Empties: em,
		Groups:  nil,
	}
	return encoded.Marshal()
}

func (exec *countStarExec) unmarshal(_ *mpool.MPool, result, empties, _ [][]byte) error {
	return exec.ret.unmarshalFromBytes(result, empties)
}

func newCountStarExec(mg AggMemoryManager, info singleAggInfo) AggFuncExec {
	return &countStarExec{
		singleAggInfo: info,
		ret:           initAggResultWithFixedTypeResult[int64](mg, info.retType, false, 0),
	}
}

func (exec *countStarExec) GroupGrow(more int) error {
	return exec.ret.grows(more)
}

func (exec *countStarExec) PreAllocateGroups(more int) error {
	return exec.ret.preExtend(more)
}

func (exec *countStarExec) Fill(groupIndex int, _ int, _ []*vector.Vector) error {
	exec.ret.updateNextAccessIdx(groupIndex)
	exec.ret.set(exec.ret.get() + 1)
	return nil
}

func (exec *countStarExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	exec.ret.updateNextAccessIdx(groupIndex)
	exec.ret.set(exec.ret.get() + int64(vectors[0].Length()))
	return nil
}

func (exec *countStarExec) BatchFill(_ int, groups []uint64, _ []*vector.Vector) error {
	vs := exec.ret.values
	for _, group := range groups {
		if group != GroupNotMatched {
			x, y := exec.ret.updateNextAccessIdx(int(group - 1))
			vs[x][y]++
		}
	}
	return nil
}

func (exec *countStarExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*countStarExec)
	exec.ret.updateNextAccessIdx(groupIdx1)
	other.ret.updateNextAccessIdx(groupIdx2)
	exec.ret.set(exec.ret.get() + other.ret.get())
	return nil
}

func (exec *countStarExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*countStarExec)
	vs1 := exec.ret.values
	vs2 := other.ret.values

	for i := range groups {
		if groups[i] == GroupNotMatched {
			continue
		}
		x1, y1 := exec.ret.updateNextAccessIdx(int(groups[i]) - 1)
		x2, y2 := other.ret.updateNextAccessIdx(i + offset)

		vs1[x1][y1] += vs2[x2][y2]
	}
	return nil
}

func (exec *countStarExec) Flush() ([]*vector.Vector, error) {
	if exec.partialResult != nil {
		x, y := exec.ret.updateNextAccessIdx(exec.partialGroup)
		exec.ret.values[x][y] += exec.partialResult.(int64)
	}
	return exec.ret.flushAll(), nil
}

func (exec *countStarExec) Free() {
	exec.ret.free()
}
