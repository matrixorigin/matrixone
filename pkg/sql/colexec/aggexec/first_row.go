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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// firstRowExec retains the first input, including NULL. The separate seen
// vector distinguishes a selected NULL from an empty partial aggregate and is
// serialized with the value during spill and partial aggregation.
type firstRowExec struct{ aggExec }

func makeFirstRowExec(mp *mpool.MPool, id int64, param types.Type) AggFuncExec {
	exec := &firstRowExec{}
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId: id, argTypes: []types.Type{param}, retType: param,
		stateTypes: []types.Type{param, types.T_bool.ToType()}, emptyNull: true,
	}
	return exec
}

func (exec *firstRowExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *firstRowExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	if vectors[0].Length() == 0 {
		return nil
	}
	return exec.Fill(groupIndex, 0, vectors)
}

func (exec *firstRowExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	input := vectors[0]
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x, y := exec.getXY(group - 1)
		state := exec.state[x].vecs
		if !state[1].IsNull(uint64(y)) {
			continue
		}
		row := offset + i
		if input.IsConst() {
			row = 0
		}
		if err := exec.selectValue(state, int(y), input, row); err != nil {
			return err
		}
	}
	return nil
}

func (exec *firstRowExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *firstRowExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*firstRowExec)
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x1, y1 := exec.getXY(group - 1)
		x2, y2 := other.getXY(uint64(offset + i))
		dst, src := exec.state[x1].vecs, other.state[x2].vecs
		if !dst[1].IsNull(uint64(y1)) || src[1].IsNull(uint64(y2)) {
			continue
		}
		if err := exec.selectValue(dst, int(y1), src[0], int(y2)); err != nil {
			return err
		}
	}
	return nil
}

func (exec *firstRowExec) selectValue(state []*vector.Vector, dst int, input *vector.Vector, row int) error {
	if !input.IsNull(uint64(row)) {
		if err := state[0].SetRawBytesAt(dst, input.GetRawBytesAt(row), exec.mp); err != nil {
			return err
		}
		state[0].UnsetNull(uint64(dst))
	}
	state[1].UnsetNull(uint64(dst))
	return nil
}

func (exec *firstRowExec) SetExtraInformation(_ any, _ int) error { return nil }

func (exec *firstRowExec) Flush() ([]*vector.Vector, error) {
	result := make([]*vector.Vector, len(exec.state))
	for i := range exec.state {
		state := &exec.state[i]
		result[i] = state.vecs[0]
		state.vecs[0] = nil
		state.vecs[1].Free(exec.mp)
		state.vecs[1] = nil
		state.length, state.capacity = 0, 0
	}
	return result, nil
}
