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
	"bytes"
	io "io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func SingleWindowReturnType(_ []types.Type) types.Type {
	return types.T_int64.ToType()
}

type i64Slice []int64

func (s i64Slice) MarshalBinary() ([]byte, error) {
	return types.EncodeSlice[int64](s), nil
}

// special structure for a single column window function.
type singleWindowExec struct {
	singleAggInfo
	ret aggResultWithFixedType[int64]

	// groups [][]int64
	groups []i64Slice
}

func makeRankDenseRankRowNumber(mp *mpool.MPool, info singleAggInfo) AggFuncExec {
	return &singleWindowExec{
		singleAggInfo: info,
		ret:           initAggResultWithFixedTypeResult[int64](mp, info.retType, info.emptyNull, 0, false),
	}
}

func (exec *singleWindowExec) GroupGrow(more int) error {
	exec.groups = append(exec.groups, make([]i64Slice, more)...)
	return exec.ret.grows(more)
}

func (exec *singleWindowExec) PreAllocateGroups(more int) error {
	return exec.ret.preExtend(more)
}

func (exec *singleWindowExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	value := vector.MustFixedColWithTypeCheck[int64](vectors[0])[row]
	exec.groups[groupIndex] = append(exec.groups[groupIndex], value)
	return nil
}

func (exec *singleWindowExec) GetOptResult() SplitResult {
	return &exec.ret.optSplitResult
}

func (exec *singleWindowExec) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, em, dist, err := exec.ret.marshalToBytes()
	if err != nil {
		return nil, err
	}
	if dist != nil {
		return nil, moerr.NewInternalErrorNoCtx("dist should have been nil")
	}

	encoded := EncodedAgg{
		Info:    d,
		Result:  r,
		Empties: em,
		Groups:  nil,
	}
	if len(exec.groups) > 0 {
		encoded.Groups = make([][]byte, len(exec.groups))
		for i := range encoded.Groups {
			encoded.Groups[i] = types.EncodeSlice[int64](exec.groups[i])
		}
	}
	return encoded.Marshal()
}

func (exec *singleWindowExec) SaveIntermediateResult(cnt int64, flags [][]uint8, buf *bytes.Buffer) error {
	return marshalRetAndGroupsToBuffer(
		cnt, flags, buf,
		&exec.ret.optSplitResult, exec.groups, nil)
}

func (exec *singleWindowExec) SaveIntermediateResultOfChunk(chunk int, buf *bytes.Buffer) error {
	return marshalChunkToBuffer(
		chunk, buf,
		&exec.ret.optSplitResult, exec.groups, nil)
}

func (exec *singleWindowExec) UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) error {
	err := unmarshalFromReaderNoGroup(reader, &exec.ret.optSplitResult)
	if err != nil {
		return err
	}
	exec.ret.setupT()

	ngrp, err := types.ReadInt64(reader)
	if err != nil {
		return err
	}
	if ngrp != 0 {
		exec.groups = make([]i64Slice, ngrp)
		for i := range exec.groups {
			_, bs, err := types.ReadSizeBytes(reader)
			if err != nil {
				return err
			}
			exec.groups[i] = types.DecodeSlice[int64](bs)
		}
	}
	return nil
}

func (exec *singleWindowExec) unmarshal(mp *mpool.MPool, result, empties, groups [][]byte) error {
	if len(exec.groups) > 0 {
		exec.groups = make([]i64Slice, len(groups))
		for i := range exec.groups {
			if len(groups[i]) > 0 {
				exec.groups[i] = types.DecodeSlice[int64](groups[i])
			}
		}
	}
	// group used by above,
	return exec.ret.unmarshalFromBytes(result, empties, nil)
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

func (exec *singleWindowExec) SetExtraInformation(partialResult any, groupIndex int) error {
	panic("window function do not support the extra information")
}

func (exec *singleWindowExec) Flush() ([]*vector.Vector, error) {
	switch exec.singleAggInfo.aggID {
	case WinIdOfRank:
		return exec.flushRank()
	case WinIdOfDenseRank:
		return exec.flushDenseRank()
	case WinIdOfRowNumber:
		return exec.flushRowNumber()
	}
	return nil, moerr.NewInternalErrorNoCtx("invalid window function")
}

func (exec *singleWindowExec) Free() {
	exec.ret.free()
}

func (exec *singleWindowExec) Size() int64 {
	var size int64
	size += exec.ret.Size()
	for _, group := range exec.groups {
		size += int64(cap(group)) * int64(types.T_int64.ToType().TypeSize())
	}
	// 24 is the size of a slice header.
	size += int64(cap(exec.groups)) * 24
	return size
}

func (exec *singleWindowExec) flushRank() ([]*vector.Vector, error) {
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
				x, y := exec.ret.updateNextAccessIdx(idx)

				values[x][y] = sn
			}
			sn += int64(m)
		}
	}
	return exec.ret.flushAll(), nil
}

func (exec *singleWindowExec) flushDenseRank() ([]*vector.Vector, error) {
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
				x, y := exec.ret.updateNextAccessIdx(idx)

				values[x][y] = sn
			}
			sn++
		}
	}
	return exec.ret.flushAll(), nil
}

func (exec *singleWindowExec) flushRowNumber() ([]*vector.Vector, error) {
	values := exec.ret.values

	idx := 0
	for _, group := range exec.groups {
		if len(group) == 0 {
			continue
		}

		n := group[len(group)-1] - group[0]
		for j := int64(1); j <= n; j++ {
			x, y := exec.ret.updateNextAccessIdx(idx)

			values[x][y] = j
			idx++
		}
	}
	return exec.ret.flushAll(), nil
}

// valueWindowExec is a window function executor for LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE
// These functions need to access values from other rows in the window
type valueWindowExec struct {
	singleAggInfo
	mp *mpool.MPool

	// Store the input values for each group
	// Each group contains the values from the window
	values []*vector.Vector

	// Partition boundaries
	partitions []int64

	// Current row index within each partition
	rowIndices []int64

	// Result vector
	resultVec *vector.Vector
}

func (exec *valueWindowExec) GroupGrow(more int) error {
	// For value window functions, we track row indices
	for i := 0; i < more; i++ {
		exec.rowIndices = append(exec.rowIndices, 0)
	}
	return nil
}

func (exec *valueWindowExec) PreAllocateGroups(more int) error {
	return nil
}

func (exec *valueWindowExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	// Store the value for this row
	if len(vectors) == 0 {
		return nil
	}

	// Ensure we have enough space in values slice
	for len(exec.values) <= groupIndex {
		exec.values = append(exec.values, nil)
	}

	// Copy the value from the input vector
	if exec.values[groupIndex] == nil {
		exec.values[groupIndex] = vector.NewVec(*vectors[0].GetType())
	}

	return exec.values[groupIndex].UnionOne(vectors[0], int64(row), exec.mp)
}

func (exec *valueWindowExec) GetOptResult() SplitResult {
	return nil
}

func (exec *valueWindowExec) marshal() ([]byte, error) {
	return nil, moerr.NewInternalErrorNoCtx("value window function does not support marshal")
}

func (exec *valueWindowExec) unmarshal(mp *mpool.MPool, result, empties, groups [][]byte) error {
	return moerr.NewInternalErrorNoCtx("value window function does not support unmarshal")
}

func (exec *valueWindowExec) SaveIntermediateResult(cnt int64, flags [][]uint8, buf *bytes.Buffer) error {
	return moerr.NewInternalErrorNoCtx("value window function does not support SaveIntermediateResult")
}

func (exec *valueWindowExec) SaveIntermediateResultOfChunk(chunk int, buf *bytes.Buffer) error {
	return moerr.NewInternalErrorNoCtx("value window function does not support SaveIntermediateResultOfChunk")
}

func (exec *valueWindowExec) UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) error {
	return moerr.NewInternalErrorNoCtx("value window function does not support UnmarshalFromReader")
}

func (exec *valueWindowExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return moerr.NewInternalErrorNoCtx("value window function does not support BulkFill")
}

func (exec *valueWindowExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	return moerr.NewInternalErrorNoCtx("value window function does not support BatchFill")
}

func (exec *valueWindowExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return moerr.NewInternalErrorNoCtx("value window function does not support Merge")
}

func (exec *valueWindowExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	return moerr.NewInternalErrorNoCtx("value window function does not support BatchMerge")
}

func (exec *valueWindowExec) SetExtraInformation(partialResult any, groupIndex int) error {
	return nil
}

func (exec *valueWindowExec) Flush() ([]*vector.Vector, error) {
	switch exec.singleAggInfo.aggID {
	case WinIdOfLag:
		return exec.flushLag()
	case WinIdOfLead:
		return exec.flushLead()
	case WinIdOfFirstValue:
		return exec.flushFirstValue()
	case WinIdOfLastValue:
		return exec.flushLastValue()
	case WinIdOfNthValue:
		return exec.flushNthValue()
	}
	return nil, moerr.NewInternalErrorNoCtx("invalid value window function")
}

func (exec *valueWindowExec) Free() {
	for _, v := range exec.values {
		if v != nil {
			v.Free(exec.mp)
		}
	}
	if exec.resultVec != nil {
		exec.resultVec.Free(exec.mp)
	}
}

func (exec *valueWindowExec) Size() int64 {
	var size int64
	for _, v := range exec.values {
		if v != nil {
			size += int64(v.Size())
		}
	}
	return size
}

func (exec *valueWindowExec) flushLag() ([]*vector.Vector, error) {
	// LAG returns the value from the previous row (offset 1 by default)
	// For each group, shift values by 1 position
	if len(exec.values) == 0 {
		return []*vector.Vector{vector.NewVec(exec.retType)}, nil
	}

	result := vector.NewVec(exec.retType)
	for _, v := range exec.values {
		if v == nil || v.Length() == 0 {
			continue
		}

		// First row gets NULL (no previous row)
		if err := vector.AppendAny(result, nil, true, exec.mp); err != nil {
			return nil, err
		}

		// Rest of the rows get the previous row's value
		for i := 0; i < v.Length()-1; i++ {
			if err := result.UnionOne(v, int64(i), exec.mp); err != nil {
				return nil, err
			}
		}
	}

	return []*vector.Vector{result}, nil
}

func (exec *valueWindowExec) flushLead() ([]*vector.Vector, error) {
	// LEAD returns the value from the next row (offset 1 by default)
	if len(exec.values) == 0 {
		return []*vector.Vector{vector.NewVec(exec.retType)}, nil
	}

	result := vector.NewVec(exec.retType)
	for _, v := range exec.values {
		if v == nil || v.Length() == 0 {
			continue
		}

		// First n-1 rows get the next row's value
		for i := 1; i < v.Length(); i++ {
			if err := result.UnionOne(v, int64(i), exec.mp); err != nil {
				return nil, err
			}
		}

		// Last row gets NULL (no next row)
		if err := vector.AppendAny(result, nil, true, exec.mp); err != nil {
			return nil, err
		}
	}

	return []*vector.Vector{result}, nil
}

func (exec *valueWindowExec) flushFirstValue() ([]*vector.Vector, error) {
	// FIRST_VALUE returns the first value in the window frame
	if len(exec.values) == 0 {
		return []*vector.Vector{vector.NewVec(exec.retType)}, nil
	}

	result := vector.NewVec(exec.retType)
	for _, v := range exec.values {
		if v == nil || v.Length() == 0 {
			continue
		}

		// All rows get the first value
		for i := 0; i < v.Length(); i++ {
			if err := result.UnionOne(v, 0, exec.mp); err != nil {
				return nil, err
			}
		}
	}

	return []*vector.Vector{result}, nil
}

func (exec *valueWindowExec) flushLastValue() ([]*vector.Vector, error) {
	// LAST_VALUE returns the last value in the window frame
	if len(exec.values) == 0 {
		return []*vector.Vector{vector.NewVec(exec.retType)}, nil
	}

	result := vector.NewVec(exec.retType)
	for _, v := range exec.values {
		if v == nil || v.Length() == 0 {
			continue
		}

		lastIdx := int64(v.Length() - 1)
		// All rows get the last value
		for i := 0; i < v.Length(); i++ {
			if err := result.UnionOne(v, lastIdx, exec.mp); err != nil {
				return nil, err
			}
		}
	}

	return []*vector.Vector{result}, nil
}

func (exec *valueWindowExec) flushNthValue() ([]*vector.Vector, error) {
	// NTH_VALUE returns the nth value in the window frame
	// For now, we'll return the first value (n=1)
	// TODO: properly handle the n parameter
	return exec.flushFirstValue()
}
