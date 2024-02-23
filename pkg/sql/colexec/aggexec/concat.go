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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// group_concat is a special string aggregation function.
type groupConcatExec struct {
	multiAggInfo
	ret aggFuncBytesResult
}

func newGroupConcatExec(proc *process.Process, info multiAggInfo) AggFuncExec {
	return &groupConcatExec{
		multiAggInfo: info,
		ret:          initBytesAggFuncResult(proc, info.retType),
	}
}

func (exec *groupConcatExec) GroupGrow(more int) error {
	return exec.ret.grows(more)
}

func (exec *groupConcatExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	exec.ret.groupToSet = groupIndex
	for _, v := range vectors {
		if err := exec.ret.aggSet(append(exec.ret.aggGet(), v.GetBytesAt(row)...)); err != nil {
			return err
		}
	}
	return nil
}

func (exec *groupConcatExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	exec.ret.groupToSet = groupIndex
	for _, v := range vectors {
		for row, end := 0, v.Length(); row < end; row++ {
			if err := exec.ret.aggSet(append(exec.ret.aggGet(), v.GetBytesAt(row)...)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (exec *groupConcatExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	return nil
}

func (exec *groupConcatExec) SetPreparedResult(partialResult any, groupIndex int) {
	return
}

func (exec *groupConcatExec) Flush() (*vector.Vector, error) {
	return nil, nil
}

func (exec *groupConcatExec) Free() {
	exec.ret.free()
}
