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

func CumeDistReturnType(_ []types.Type) types.Type {
	return types.T_float64.ToType()
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

// cumeDistWindowExec is for CUME_DIST window function which returns float64
type cumeDistWindowExec struct {
	singleAggInfo
	ret aggResultWithFixedType[float64]

	groups []i64Slice
}

func makeCumeDist(mp *mpool.MPool, info singleAggInfo) AggFuncExec {
	return &cumeDistWindowExec{
		singleAggInfo: info,
		ret:           initAggResultWithFixedTypeResult[float64](mp, info.retType, info.emptyNull, 0, false),
	}
}

func (exec *cumeDistWindowExec) GroupGrow(more int) error {
	exec.groups = append(exec.groups, make([]i64Slice, more)...)
	return exec.ret.grows(more)
}

func (exec *cumeDistWindowExec) PreAllocateGroups(more int) error {
	return exec.ret.preExtend(more)
}

func (exec *cumeDistWindowExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	value := vector.MustFixedColWithTypeCheck[int64](vectors[0])[row]
	exec.groups[groupIndex] = append(exec.groups[groupIndex], value)
	return nil
}

func (exec *cumeDistWindowExec) GetOptResult() SplitResult {
	return &exec.ret.optSplitResult
}

func (exec *cumeDistWindowExec) marshal() ([]byte, error) {
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

func (exec *cumeDistWindowExec) SaveIntermediateResult(cnt int64, flags [][]uint8, buf *bytes.Buffer) error {
	return marshalRetAndGroupsToBuffer(
		cnt, flags, buf,
		&exec.ret.optSplitResult, exec.groups, nil)
}

func (exec *cumeDistWindowExec) SaveIntermediateResultOfChunk(chunk int, buf *bytes.Buffer) error {
	return marshalChunkToBuffer(
		chunk, buf,
		&exec.ret.optSplitResult, exec.groups, nil)
}

func (exec *cumeDistWindowExec) UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) error {
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

func (exec *cumeDistWindowExec) unmarshal(mp *mpool.MPool, result, empties, groups [][]byte) error {
	if len(exec.groups) > 0 {
		exec.groups = make([]i64Slice, len(groups))
		for i := range exec.groups {
			if len(groups[i]) > 0 {
				exec.groups[i] = types.DecodeSlice[int64](groups[i])
			}
		}
	}
	return exec.ret.unmarshalFromBytes(result, empties, nil)
}

func (exec *cumeDistWindowExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	panic("implement me")
}

func (exec *cumeDistWindowExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	panic("implement me")
}

func (exec *cumeDistWindowExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*cumeDistWindowExec)
	exec.groups[groupIdx1] = append(exec.groups[groupIdx1], other.groups[groupIdx2]...)
	return nil
}

func (exec *cumeDistWindowExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*cumeDistWindowExec)
	for i := range groups {
		if groups[i] != GroupNotMatched {
			groupIdx1 := int(groups[i] - 1)
			groupIdx2 := i + offset

			exec.groups[groupIdx1] = append(exec.groups[groupIdx1], other.groups[groupIdx2]...)
		}
	}
	return nil
}

func (exec *cumeDistWindowExec) SetExtraInformation(partialResult any, groupIndex int) error {
	panic("window function do not support the extra information")
}

func (exec *cumeDistWindowExec) Flush() ([]*vector.Vector, error) {
	return exec.flushCumeDist()
}

func (exec *cumeDistWindowExec) Free() {
	exec.ret.free()
}

func (exec *cumeDistWindowExec) Size() int64 {
	var size int64
	size += exec.ret.Size()
	for _, group := range exec.groups {
		size += int64(cap(group)) * int64(types.T_int64.ToType().TypeSize())
	}
	size += int64(cap(exec.groups)) * 24
	return size
}

func (exec *cumeDistWindowExec) flushCumeDist() ([]*vector.Vector, error) {
	values := exec.ret.values

	idx := 0
	for _, group := range exec.groups {
		if len(group) == 0 {
			continue
		}

		total := float64(group[len(group)-1] - group[0])
		for i := 1; i < len(group); i++ {
			m := int(group[i] - group[i-1])
			cumeDist := float64(group[i]-group[0]) / total

			for k := idx + m; idx < k; idx++ {
				x, y := exec.ret.updateNextAccessIdx(idx)
				values[x][y] = cumeDist
			}
		}
	}
	return exec.ret.flushAll(), nil
}

// valueWindowExec is a window function executor for LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE
// These functions need to access values from other rows in the window
//
// For these window functions, Fill is called for each row j with all values k in the window frame.
// - groupIndex is the current row index (j)
// - row is the index of a value within the window frame (k)
// - vectors contains the expression values
//
// The key insight is that for LAG/LEAD, we need to know the position of the current row
// within the partition, not just within the frame. Since the frame is UNBOUNDED PRECEDING
// to UNBOUNDED FOLLOWING, the frame contains all rows in the partition.
//
// For row j in a partition starting at position 'start':
// - LAG(1) should return the value at position j-1 (if j > start)
// - LEAD(1) should return the value at position j+1 (if j+1 < end)
//
// Since Fill is called with (j, k, vec) where k iterates through the frame,
// we need to track which k corresponds to the current row j.
type valueWindowExec struct {
	singleAggInfo
	mp *mpool.MPool

	// For each output row (groupIndex), store the values from its window frame
	// frameValues[groupIndex] contains all values in the window frame for that row
	frameValues [][]*valueEntry

	// For each output row, store its position within the frame
	// This is the index where the current row's value appears in frameValues[groupIndex]
	currentRowPosition []int

	// Result vector
	resultVec *vector.Vector
}

// valueEntry stores a single value from the window frame
type valueEntry struct {
	isNull bool
	data   []byte
}

func (exec *valueWindowExec) GroupGrow(more int) error {
	// Grow the frameValues slice to accommodate more groups
	for i := 0; i < more; i++ {
		exec.frameValues = append(exec.frameValues, nil)
		exec.currentRowPosition = append(exec.currentRowPosition, -1)
	}
	return nil
}

func (exec *valueWindowExec) PreAllocateGroups(more int) error {
	return nil
}

func (exec *valueWindowExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	// Store the value for this row in the window frame
	if len(vectors) == 0 {
		return nil
	}

	// Ensure we have enough space
	for len(exec.frameValues) <= groupIndex {
		exec.frameValues = append(exec.frameValues, nil)
		exec.currentRowPosition = append(exec.currentRowPosition, -1)
	}

	vec := vectors[0]
	entry := &valueEntry{
		isNull: vec.IsNull(uint64(row)),
	}

	if !entry.isNull {
		// Copy the value data
		if vec.GetType().IsVarlen() {
			bs := vec.GetBytesAt(row)
			entry.data = make([]byte, len(bs))
			copy(entry.data, bs)
		} else {
			// For fixed-size types, get the raw bytes
			entry.data = vec.GetRawBytesAt(row)
		}
	}

	// Track the position of the current row within the frame
	// When row == groupIndex, this is the current row's value
	if row == groupIndex {
		exec.currentRowPosition[groupIndex] = len(exec.frameValues[groupIndex])
	}

	exec.frameValues[groupIndex] = append(exec.frameValues[groupIndex], entry)
	return nil
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
	if exec.resultVec != nil {
		exec.resultVec.Free(exec.mp)
	}
}

func (exec *valueWindowExec) Size() int64 {
	var size int64
	for _, frame := range exec.frameValues {
		for _, entry := range frame {
			if entry != nil {
				size += int64(len(entry.data)) + 8 // data + isNull flag overhead
			}
		}
	}
	return size
}

func (exec *valueWindowExec) flushLag() ([]*vector.Vector, error) {
	// LAG returns the value from the previous row in the partition
	// For LAG with default offset=1, we want the value at position (currentRowPosition - 1)

	result := vector.NewVec(exec.retType)
	for i, frame := range exec.frameValues {
		if len(frame) == 0 {
			// No values in frame, append NULL
			if err := vector.AppendAny(result, nil, true, exec.mp); err != nil {
				return nil, err
			}
			continue
		}

		// Get the position of the current row within the frame
		currentPos := exec.currentRowPosition[i]
		if currentPos < 0 {
			// Current row position not found, this shouldn't happen
			if err := vector.AppendAny(result, nil, true, exec.mp); err != nil {
				return nil, err
			}
			continue
		}

		// LAG(1) returns the value at currentPos - 1
		lagPos := currentPos - 1
		if lagPos < 0 {
			// No previous row, return NULL
			if err := vector.AppendAny(result, nil, true, exec.mp); err != nil {
				return nil, err
			}
		} else {
			entry := frame[lagPos]
			if entry.isNull {
				if err := vector.AppendAny(result, nil, true, exec.mp); err != nil {
					return nil, err
				}
			} else {
				if err := appendValueToVector(result, entry.data, exec.retType, exec.mp); err != nil {
					return nil, err
				}
			}
		}
	}

	return []*vector.Vector{result}, nil
}

func (exec *valueWindowExec) flushLead() ([]*vector.Vector, error) {
	// LEAD returns the value from the next row in the partition
	// For LEAD with default offset=1, we want the value at position (currentRowPosition + 1)

	result := vector.NewVec(exec.retType)
	for i, frame := range exec.frameValues {
		if len(frame) == 0 {
			// No values in frame, append NULL
			if err := vector.AppendAny(result, nil, true, exec.mp); err != nil {
				return nil, err
			}
			continue
		}

		// Get the position of the current row within the frame
		currentPos := exec.currentRowPosition[i]
		if currentPos < 0 {
			// Current row position not found, this shouldn't happen
			if err := vector.AppendAny(result, nil, true, exec.mp); err != nil {
				return nil, err
			}
			continue
		}

		// LEAD(1) returns the value at currentPos + 1
		leadPos := currentPos + 1
		if leadPos >= len(frame) {
			// No next row, return NULL
			if err := vector.AppendAny(result, nil, true, exec.mp); err != nil {
				return nil, err
			}
		} else {
			entry := frame[leadPos]
			if entry.isNull {
				if err := vector.AppendAny(result, nil, true, exec.mp); err != nil {
					return nil, err
				}
			} else {
				if err := appendValueToVector(result, entry.data, exec.retType, exec.mp); err != nil {
					return nil, err
				}
			}
		}
	}

	return []*vector.Vector{result}, nil
}

func (exec *valueWindowExec) flushFirstValue() ([]*vector.Vector, error) {
	// FIRST_VALUE returns the first value in the window frame

	result := vector.NewVec(exec.retType)
	for _, frame := range exec.frameValues {
		if len(frame) == 0 {
			// No values in frame, append NULL
			if err := vector.AppendAny(result, nil, true, exec.mp); err != nil {
				return nil, err
			}
			continue
		}

		// Get the first value in the frame
		entry := frame[0]
		if entry.isNull {
			if err := vector.AppendAny(result, nil, true, exec.mp); err != nil {
				return nil, err
			}
		} else {
			if err := appendValueToVector(result, entry.data, exec.retType, exec.mp); err != nil {
				return nil, err
			}
		}
	}

	return []*vector.Vector{result}, nil
}

func (exec *valueWindowExec) flushLastValue() ([]*vector.Vector, error) {
	// LAST_VALUE returns the last value in the window frame

	result := vector.NewVec(exec.retType)
	for _, frame := range exec.frameValues {
		if len(frame) == 0 {
			// No values in frame, append NULL
			if err := vector.AppendAny(result, nil, true, exec.mp); err != nil {
				return nil, err
			}
			continue
		}

		// Get the last value in the frame
		entry := frame[len(frame)-1]
		if entry.isNull {
			if err := vector.AppendAny(result, nil, true, exec.mp); err != nil {
				return nil, err
			}
		} else {
			if err := appendValueToVector(result, entry.data, exec.retType, exec.mp); err != nil {
				return nil, err
			}
		}
	}

	return []*vector.Vector{result}, nil
}

func (exec *valueWindowExec) flushNthValue() ([]*vector.Vector, error) {
	// NTH_VALUE returns the nth value in the window frame
	// For now, we default to n=1 (same as FIRST_VALUE)
	// TODO: properly handle the n parameter from the function arguments
	return exec.flushFirstValue()
}

// appendValueToVector appends a value to the result vector based on the type
func appendValueToVector(result *vector.Vector, data []byte, typ types.Type, mp *mpool.MPool) error {
	if typ.IsVarlen() {
		return vector.AppendBytes(result, data, false, mp)
	}

	// For fixed-size types, we need to append based on the type
	switch typ.Oid {
	case types.T_bool:
		return vector.AppendFixed(result, types.DecodeFixed[bool](data), false, mp)
	case types.T_bit:
		return vector.AppendFixed(result, types.DecodeFixed[uint64](data), false, mp)
	case types.T_int8:
		return vector.AppendFixed(result, types.DecodeFixed[int8](data), false, mp)
	case types.T_int16:
		return vector.AppendFixed(result, types.DecodeFixed[int16](data), false, mp)
	case types.T_int32:
		return vector.AppendFixed(result, types.DecodeFixed[int32](data), false, mp)
	case types.T_int64:
		return vector.AppendFixed(result, types.DecodeFixed[int64](data), false, mp)
	case types.T_uint8:
		return vector.AppendFixed(result, types.DecodeFixed[uint8](data), false, mp)
	case types.T_uint16:
		return vector.AppendFixed(result, types.DecodeFixed[uint16](data), false, mp)
	case types.T_uint32:
		return vector.AppendFixed(result, types.DecodeFixed[uint32](data), false, mp)
	case types.T_uint64:
		return vector.AppendFixed(result, types.DecodeFixed[uint64](data), false, mp)
	case types.T_float32:
		return vector.AppendFixed(result, types.DecodeFixed[float32](data), false, mp)
	case types.T_float64:
		return vector.AppendFixed(result, types.DecodeFixed[float64](data), false, mp)
	case types.T_decimal64:
		return vector.AppendFixed(result, types.DecodeFixed[types.Decimal64](data), false, mp)
	case types.T_decimal128:
		return vector.AppendFixed(result, types.DecodeFixed[types.Decimal128](data), false, mp)
	case types.T_date:
		return vector.AppendFixed(result, types.DecodeFixed[types.Date](data), false, mp)
	case types.T_datetime:
		return vector.AppendFixed(result, types.DecodeFixed[types.Datetime](data), false, mp)
	case types.T_time:
		return vector.AppendFixed(result, types.DecodeFixed[types.Time](data), false, mp)
	case types.T_timestamp:
		return vector.AppendFixed(result, types.DecodeFixed[types.Timestamp](data), false, mp)
	case types.T_uuid:
		return vector.AppendFixed(result, types.DecodeFixed[types.Uuid](data), false, mp)
	case types.T_TS:
		return vector.AppendFixed(result, types.DecodeFixed[types.TS](data), false, mp)
	case types.T_Rowid:
		return vector.AppendFixed(result, types.DecodeFixed[types.Rowid](data), false, mp)
	case types.T_Blockid:
		return vector.AppendFixed(result, types.DecodeFixed[types.Blockid](data), false, mp)
	case types.T_enum:
		return vector.AppendFixed(result, types.DecodeFixed[types.Enum](data), false, mp)
	default:
		// For other types, try to append as bytes
		return vector.AppendBytes(result, data, false, mp)
	}
}
