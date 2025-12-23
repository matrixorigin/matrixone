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

type countStarExec struct {
	aggExec[int64]
}

func (exec *countStarExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for _, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		pt := exec.GetState(grp - 1)
		*pt += 1
	}
	return nil
}

func (exec *countStarExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*countStarExec)
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		pt1 := exec.GetState(grp - 1)
		pt2 := other.GetState(uint64(offset + i))
		*pt1 += *pt2
	}
	return nil
}

func (exec *countStarExec) SetExtraInformation(partialResult any, groupIndex int) error {
	return nil
}

func (exec *countStarExec) Flush() ([]*vector.Vector, error) {
	vecs := make([]*vector.Vector, len(exec.state))
	for i := range vecs {
		vecs[i] = vector.NewOffHeapVecWithTypeAndData(
			types.T_int64.ToType(),
			exec.state[i].states,
			int(exec.state[i].length),
			int(exec.state[i].capacity))
		exec.state[i].states = nil
		exec.state[i].length = 0
		exec.state[i].capacity = 0
	}
	return vecs, nil
}

type countColumnExec struct {
	aggExec[int64]
}

func (exec *countColumnExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if exec.IsDistinct() {
		return exec.batchFillArgs(offset, groups, vectors, true)
	}
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		idx := uint64(i) + uint64(offset)
		if vectors[0].IsNull(idx) {
			continue
		} else {
			pt := exec.GetState(grp - 1)
			*pt += 1
		}
	}
	return nil
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
		pt1 := exec.GetState(grp - 1)
		pt2 := other.GetState(uint64(offset + i))
		*pt1 += *pt2
	}
	return nil
}

func (exec *countColumnExec) SetExtraInformation(partialResult any, groupIndex int) error {
	return nil
}

func (exec *countColumnExec) Flush() ([]*vector.Vector, error) {
	vecs := make([]*vector.Vector, len(exec.state))
	if exec.IsDistinct() {
		for i := range vecs {
			vecs[i] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
			vecs[i].PreExtend(int(exec.state[i].length), exec.mp)
			ptrs := exec.state[i].getPtrLenSlice()
			vals := vector.MustFixedColNoTypeCheck[int64](vecs[i])
			for j := range ptrs {
				vals[j] = int64(ptrs[j].Len())
			}
			vecs[i].SetLength(int(exec.state[i].length))
		}
	} else {
		for i := range vecs {
			vecs[i] = vector.NewOffHeapVecWithTypeAndData(
				types.T_int64.ToType(),
				exec.state[i].states,
				int(exec.state[i].length),
				int(exec.state[i].capacity))
			exec.state[i].states = nil
			exec.state[i].length = 0
			exec.state[i].capacity = 0
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
		emptyNull:  false,
		usePtrLen:  false,
		entrySize:  8, // int64
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
		emptyNull:  false,
		usePtrLen:  isDistinct, // count (distinct X) needs to store varlen value
		entrySize:  8,          // int64
	}
	return &exec
}
