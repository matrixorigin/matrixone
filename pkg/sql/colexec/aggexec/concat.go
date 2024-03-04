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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"math"
)

// group_concat is a special string aggregation function.
type groupConcatExec struct {
	multiAggInfo
	ret aggFuncBytesResult

	separator []byte
}

func newGroupConcatExec(mg AggMemoryManager, info multiAggInfo, separator string) AggFuncExec {
	return &groupConcatExec{
		multiAggInfo: info,
		ret:          initBytesAggFuncResult(mg, info.retType, info.emptyNull),
		separator:    []byte(separator),
	}
}

func isValidGroupConcatUnit(value []byte) error {
	if len(value) > math.MaxUint16 {
		return moerr.NewInternalErrorNoCtx("group_concat: the length of the value is too long")
	}
	return nil
}

func (exec *groupConcatExec) GroupGrow(more int) error {
	return exec.ret.grows(more)
}

func (exec *groupConcatExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	// if any value was null, there is no need to Fill.
	u64Row := uint64(row)
	for _, v := range vectors {
		if v.IsNull(u64Row) {
			return nil
		}
	}

	exec.ret.groupToSet = groupIndex
	r := exec.ret.aggGet()
	if len(r) > 0 {
		r = append(r, exec.separator...)
	}
	for _, v := range vectors {
		value := v.GetBytesAt(row)
		if err := isValidGroupConcatUnit(value); err != nil {
			return err
		}
		r = append(r, value...)
	}
	if err := exec.ret.aggSet(r); err != nil {
		return err
	}
	return nil
}

func (exec *groupConcatExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	exec.ret.groupToSet = groupIndex
	for _, v := range vectors {
		for row, end := 0, v.Length(); row < end; row++ {
			if err := exec.Fill(groupIndex, row, vectors); err != nil {
				return err
			}
		}
	}
	return nil
}

func (exec *groupConcatExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, j, idx := offset, offset+len(groups), 0; i < j; i++ {
		if groups[idx] != GroupNotMatched {
			if err := exec.Fill(int(groups[idx]-1), i, vectors); err != nil {
				return err
			}
		}
		idx++
	}
	return nil
}

func (exec *groupConcatExec) SetPreparedResult(partialResult any, groupIndex int) {
	panic("partial result is not supported for group_concat")
}

func (exec *groupConcatExec) merge(other *groupConcatExec, idx1, idx2 int) error {
	exec.ret.groupToSet = idx1
	other.ret.groupToSet = idx2

	v1 := exec.ret.aggGet()
	v2 := other.ret.aggGet()
	if len(v1) == 0 && len(v2) == 0 {
		return nil
	}
	if len(v1) > 0 && len(v2) > 0 {
		v1 = append(v1, exec.separator...)
		v1 = append(v1, v2...)
		return exec.ret.aggSet(v1)
	}
	if len(v1) == 0 {
		return exec.ret.aggSet(v2)
	}
	return exec.ret.aggSet(v1)
}

func (exec *groupConcatExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.merge(next.(*groupConcatExec), groupIdx1, groupIdx2)
}

func (exec *groupConcatExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*groupConcatExec)
	for i := range groups {
		if groups[i] == GroupNotMatched {
			continue
		}
		if err := exec.merge(other, i+offset, int(groups[i])-1); err != nil {
			return err
		}
	}
	return nil
}

func (exec *groupConcatExec) Flush() (*vector.Vector, error) {
	return exec.ret.flush(), nil
}

func (exec *groupConcatExec) Free() {
	exec.ret.free()
}
