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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func SingleWindowReturnType(_ []types.Type) types.Type {
	return types.T_int64.ToType()
}

// special structure for a single column window function.
type singleWindowExec struct {
	singleAggInfo
	ret aggFuncResult[int64]

	groups [][]int64
}

func makeRankDenseRankRowNumber(mg AggMemoryManager, info singleAggInfo) AggFuncExec {
	return &singleWindowExec{
		singleAggInfo: info,
		ret:           initFixedAggFuncResult[int64](mg, info.retType, info.emptyNull),
	}
}

func (exec *singleWindowExec) GroupGrow(more int) error {
	exec.groups = append(exec.groups, make([][]int64, more)...)
	return exec.ret.grows(more)
}

func (exec *singleWindowExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	value := vector.MustFixedCol[int64](vectors[0])[row]
	exec.groups[groupIndex] = append(exec.groups[groupIndex], value)
	return nil
}

func (exec *singleWindowExec) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, err := exec.ret.marshal()
	if err != nil {
		return nil, err
	}

	encoded := &EncodedAgg{
		ExecType: EncodedAggExecType_single_window,
		Info:     d,
		Result:   r,
		Groups:   nil,
	}
	if len(exec.groups) > 0 {
		encoded.Groups = make([][]byte, len(exec.groups))
		for i := range encoded.Groups {
			encoded.Groups[i] = types.EncodeSlice[int64](exec.groups[i])
		}
	}
	return encoded.Marshal()
}

func (exec *singleWindowExec) unmarshal(result []byte, groups [][]byte) error {
	if len(exec.groups) > 0 {
		exec.groups = make([][]int64, len(groups))
		for i := range exec.groups {
			if len(groups[i]) > 0 {
				exec.groups[i] = types.DecodeSlice[int64](groups[i])
			}
		}
	}
	return exec.ret.unmarshal(result)
}

func (exec *singleWindowExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	panic("implement me")
}

func (exec *singleWindowExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	panic("implement me")
}

func (exec *singleWindowExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*singleWindowExec)
	exec.groups[groupIdx1] = append(exec.groups[groupIdx1], other.groups[groupIdx2]...)
	return nil
}

func (exec *singleWindowExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*singleWindowExec)
	for i := range groups {
		if groups[i] != GroupNotMatched {
			groupIdx1 := int(groups[i] - 1)
			groupIdx2 := i + offset

			exec.groups[groupIdx1] = append(exec.groups[groupIdx1], other.groups[groupIdx2]...)
		}
	}
	return nil
}

func (exec *singleWindowExec) SetExtraInformation(partialResult any, groupIndex int) {
	panic("window function do not support the extra information")
}

func (exec *singleWindowExec) Flush() (*vector.Vector, error) {
	switch exec.singleAggInfo.aggID {
	case winIdOfRank:
		return exec.flushRank()
	case winIdOfDenseRank:
		return exec.flushDenseRank()
	case winIdOfRowNumber:
		return exec.flushRowNumber()
	}
	return nil, moerr.NewInternalErrorNoCtx("invalid window function")
}

func (exec *singleWindowExec) Free() {
	exec.ret.free()
}

func (exec *singleWindowExec) flushRank() (*vector.Vector, error) {
	values := exec.ret.values

	idx := 0
	for _, group := range exec.groups {
		if len(group) == 0 {
			continue
		}

		sn := int64(1)
		for i := 1; i < len(group); i++ {
			m := int(group[i] - group[i-1])

			for k := idx + m; idx < k; idx++ {
				values[idx] = sn
			}
			sn += int64(m)
		}
	}
	return exec.ret.flush(), nil
}

func (exec *singleWindowExec) flushDenseRank() (*vector.Vector, error) {
	values := exec.ret.values

	idx := 0
	for _, group := range exec.groups {
		if len(group) == 0 {
			continue
		}

		sn := int64(1)
		for i := 1; i < len(group); i++ {
			m := int(group[i] - group[i-1])

			for k := idx + m; idx < k; idx++ {
				values[idx] = sn
			}
			sn++
		}
	}
	return exec.ret.flush(), nil
}

func (exec *singleWindowExec) flushRowNumber() (*vector.Vector, error) {
	values := exec.ret.values

	idx := 0
	for _, group := range exec.groups {
		if len(group) == 0 {
			continue
		}

		for i := 1; i < len(group); i++ {
			m := group[i] - group[i-1]

			for j := int64(1); j <= m; j++ {
				values[idx] = j
				idx++
			}
		}
	}
	return exec.ret.flush(), nil
}
