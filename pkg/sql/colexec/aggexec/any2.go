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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type anyExec struct {
	aggExec
}

func (exec *anyExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *anyExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *anyExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		idx := uint64(i) + uint64(offset)
		if vectors[0].IsNull(idx) {
			continue
		} else {
			x, y := exec.getXY(uint64(grp - 1))
			if exec.state[x].vecs[0].IsNull(uint64(y)) {
				exec.state[x].vecs[0].UnsetNull(uint64(y))
				bs := vectors[0].GetRawBytesAt(int(idx))
				exec.state[x].vecs[0].SetRawBytesAt(int(y), bs, exec.mp)
			}
		}
	}
	return nil
}

func (exec *anyExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *anyExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*anyExec)
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		x1, y1 := exec.getXY(uint64(grp - 1))
		x2, y2 := other.getXY(uint64(offset + i))
		if other.state[x2].vecs[0].IsNull(uint64(y2)) {
			continue
		}
		if exec.state[x1].vecs[0].IsNull(uint64(y1)) {
			exec.state[x1].vecs[0].UnsetNull(uint64(y1))
			bs := other.state[x2].vecs[0].GetRawBytesAt(int(y2))
			exec.state[x1].vecs[0].SetRawBytesAt(int(y1), bs, exec.mp)
		}
	}
	return nil
}

func (exec *anyExec) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (exec *anyExec) Flush() ([]*vector.Vector, error) {
	vecs := make([]*vector.Vector, len(exec.state))
	for i := range vecs {
		vecs[i] = exec.state[i].vecs[0]
		exec.state[i].vecs[0] = nil
		exec.state[i].length = 0
		exec.state[i].capacity = 0
	}
	return vecs, nil
}

func makeAnyValueExec(mp *mpool.MPool, id int64, param types.Type) AggFuncExec {
	var exec anyExec
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:      id,
		isDistinct: false,
		argTypes:   []types.Type{param},
		retType:    param,
		stateTypes: []types.Type{param},
		emptyNull:  true,
		saveArg:    false,
	}
	return &exec
}
