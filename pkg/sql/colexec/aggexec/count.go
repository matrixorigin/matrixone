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

	ret aggFuncResult[int64]
}

func newCountColumnExecExec(mg AggMemoryManager, info singleAggInfo) AggFuncExec {
	exec := &countColumnExec{
		singleAggInfo: info,
		ret:           initFixedAggFuncResult[int64](mg, info.retType, false),
	}
	if info.distinct {
		exec.distinctHash = newDistinctHash(mg.Mp(), false)
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

func (exec *countColumnExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	if vectors[0].IsNull(uint64(row)) {
		return nil
	}

	if exec.IsDistinct() {
		if need, err := exec.distinctHash.fill(groupIndex, vectors, row); err != nil || !need {
			return err
		}
	}

	exec.ret.groupToSet = groupIndex
	exec.ret.aggSet(exec.ret.aggGet() + 1)
	return nil
}

func (exec *countColumnExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	if vectors[0].IsConstNull() {
		return nil
	}
	exec.ret.groupToSet = groupIndex

	old := exec.ret.aggGet()
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
	exec.ret.aggSet(old)
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
					vs[group-1]++
				}
			}
			return nil
		}

		for _, group := range groups {
			if group != GroupNotMatched {
				vs[group-1]++
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
					vs[groups[i]-1]++
				}
			}

		} else {
			for i, group := range groups {
				if needs[i] && group != GroupNotMatched {
					vs[group-1]++
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
					vs[groups[i]-1]++
				}
			}
		}

	} else {
		for _, group := range groups {
			if group != GroupNotMatched {
				vs[group-1]++
			}
		}
	}
	return nil
}

func (exec *countColumnExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*countColumnExec)

	exec.ret.groupToSet = groupIdx1
	other.ret.groupToSet = groupIdx2
	exec.ret.aggSet(exec.ret.aggGet() + other.ret.aggGet())
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
		g1, g2 := int(groups[i])-1, i+offset
		exec.ret.mergeEmpty(other.ret.basicResult, g1, g2)
		vs1[g1] += vs2[g2]
	}
	return exec.distinctHash.merge(&other.distinctHash)
}

func (exec *countColumnExec) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.values[exec.ret.groupToSet] += exec.partialResult.(int64)
	}
	return exec.ret.flush(), nil
}

func (exec *countColumnExec) Free() {
	exec.ret.free()
	exec.distinctHash.free()
}

type countStarExec struct {
	singleAggInfo
	singleAggExecExtraInformation
	ret aggFuncResult[int64]
}

func newCountStarExec(mg AggMemoryManager, info singleAggInfo) AggFuncExec {
	// todo: should we check if `distinct` here ?
	return &countStarExec{
		singleAggInfo: info,
		ret:           initFixedAggFuncResult[int64](mg, info.retType, false),
	}
}

func (exec *countStarExec) GroupGrow(more int) error {
	return exec.ret.grows(more)
}

func (exec *countStarExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	exec.ret.groupToSet = groupIndex
	exec.ret.aggSet(exec.ret.aggGet() + 1)
	return nil
}

func (exec *countStarExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	exec.ret.groupToSet = groupIndex
	exec.ret.aggSet(exec.ret.aggGet() + int64(vectors[0].Length()))
	return nil
}

func (exec *countStarExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	vs := exec.ret.values
	for _, group := range groups {
		if group != GroupNotMatched {
			vs[group-1]++
		}
	}
	return nil
}

func (exec *countStarExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	exec.ret.groupToSet = groupIdx1
	exec.ret.aggSet(exec.ret.aggGet() + next.(*countStarExec).ret.aggGet())
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
		g1, g2 := int(groups[i])-1, i+offset
		exec.ret.mergeEmpty(other.ret.basicResult, g1, g2)
		vs1[g1] += vs2[g2]
	}
	return nil
}

func (exec *countStarExec) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.values[exec.ret.groupToSet] += exec.partialResult.(int64)
	}
	return exec.ret.flush(), nil
}

func (exec *countStarExec) Free() {
	exec.ret.free()
}
