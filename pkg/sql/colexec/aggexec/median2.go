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
	"cmp"
	io "io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

var MedianSupportedType = []types.T{
	types.T_bit, types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128,
}

func MedianReturnType(args []types.Type) types.Type {
	if args[0].IsDecimal() {
		return types.New(types.T_decimal128, 38, args[0].Scale+1)
	}
	return types.T_float64.ToType()
}

type medianColumnExecSelf[T numeric | types.Decimal64 | types.Decimal128, R types.FixedSizeTExceptStrType] struct {
	singleAggInfo
	accounted    *aggExec
	distinctHash distinctHash
	ret          aggResultWithFixedType[R]
	groups       []*Vectors[T]
	argType      types.Type
	mp           *mpool.MPool
}

func newMedianColumnExecSelf[T numeric | types.Decimal64 | types.Decimal128, R types.FixedSizeTExceptStrType](mp *mpool.MPool, info singleAggInfo, initial R) medianColumnExecSelf[T, R] {
	self := medianColumnExecSelf[T, R]{
		singleAggInfo: info,
		distinctHash:  newDistinctHash(mp),
		ret:           initAggResultWithFixedTypeResult[R](mp, info.retType, info.emptyNull, initial, false),
		argType:       info.argType,
		mp:            mp,
	}
	return self
}

func (exec *medianColumnExecSelf[T, R]) GetOptResult() SplitResult {
	if exec.accounted != nil {
		return exec.accounted
	}
	return &exec.ret.optSplitResult
}

func (exec *medianColumnExecSelf[T, R]) GroupGrow(more int) error {
	if exec.accounted != nil {
		return exec.accounted.GroupGrow(more)
	}
	if exec.IsDistinct() {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}

	oldLength := len(exec.groups)
	if cap(exec.groups) >= oldLength+more {
		exec.groups = exec.groups[:oldLength+more]
	} else {
		exec.groups = append(exec.groups, make([]*Vectors[T], more)...)
	}
	for i := oldLength; i < len(exec.groups); i++ {
		exec.groups[i] = NewVectors[T](exec.argType)
	}
	return exec.ret.grows(more)
}

func (exec *medianColumnExecSelf[T, R]) PreAllocateGroups(more int) error {
	if exec.accounted != nil {
		return exec.accounted.PreAllocateGroups(more)
	}
	if len(exec.groups) == 0 {
		exec.groups = make([]*Vectors[T], 0, more)
	} else {
		oldLength := len(exec.groups)
		exec.groups = append(exec.groups, make([]*Vectors[T], more)...)
		exec.groups = exec.groups[:oldLength]
	}
	return exec.ret.preExtend(more)
}

func (exec *medianColumnExecSelf[T, R]) SaveIntermediateResult(cnt int64, flags [][]uint8, writer io.Writer) error {
	if exec.accounted != nil {
		return exec.saveAccountedIntermediate(cnt, flags, writer)
	}
	return marshalRetAndGroupsToBuffer(cnt, flags, writer, &exec.ret.optSplitResult, exec.groups, nil)
}

func (exec *medianColumnExecSelf[T, R]) SaveIntermediateResultOfChunk(chunk int, writer io.Writer) error {
	if exec.accounted != nil {
		if chunk < 0 || chunk >= len(exec.accounted.state) {
			return moerr.NewInvalidInputNoCtx("invalid median chunk")
		}
		return exec.saveAccountedIntermediateChunk(chunk, writer)
	}
	return marshalChunkToBuffer(chunk, writer, &exec.ret.optSplitResult, exec.groups, nil)
}

func (exec *medianColumnExecSelf[T, R]) saveAccountedIntermediateChunk(
	chunk int,
	writer io.Writer,
) error {
	if exec.accounted == nil || writer == nil ||
		chunk < 0 || chunk >= len(exec.accounted.state) {
		return moerr.NewInvalidInputNoCtx("invalid accounted median chunk")
	}
	state := &exec.accounted.state[chunk]
	cnt := int(state.length)
	if err := types.WriteInt64(writer, int64(cnt)); err != nil || cnt == 0 {
		return err
	}
	result, err := exec.accounted.allocation.newVector(exec.retType)
	if err != nil {
		return err
	}
	empty, err := exec.accounted.allocation.newVector(types.T_bool.ToType())
	if err != nil {
		result.Free(exec.mp)
		return err
	}
	defer result.Free(exec.mp)
	defer empty.Free(exec.mp)
	if err = result.PreExtend(cnt, exec.mp); err != nil {
		return err
	}
	if err = empty.PreExtend(cnt, exec.mp); err != nil {
		return err
	}
	result.SetLength(cnt)
	empty.SetLength(cnt)
	emptyValues := vector.MustFixedColNoTypeCheck[bool](empty)
	for row := range cnt {
		emptyValues[row] = state.argCnt[row] == 0
	}
	if err = result.MarshalBinaryTo(writer); err != nil {
		return err
	}
	if err = types.WriteInt64(writer, 1); err != nil {
		return err
	}
	if err = empty.MarshalBinaryTo(writer); err != nil {
		return err
	}
	if err = types.WriteInt64(writer, 0); err != nil {
		return err
	}
	if err = types.WriteInt64(writer, int64(cnt)); err != nil {
		return err
	}
	for row := range cnt {
		if err = exec.writeLegacyMedianGroup(
			state, uint16(row), writer); err != nil {
			return err
		}
	}
	return types.WriteInt64(writer, 0)
}

func (exec *medianColumnExecSelf[T, R]) UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) error {
	if exec.accounted != nil {
		return exec.unmarshalAccountedIntermediate(reader, mp)
	}
	replacement := newMedianColumnExecSelf[T, R](
		mp, exec.singleAggInfo, exec.ret.InitialValue)
	if err := replacement.unmarshalFromReader(reader, mp); err != nil {
		replacement.Free()
		return err
	}
	exec.Free()
	*exec = replacement
	return nil
}

func (exec *medianColumnExecSelf[T, R]) unmarshalFromReader(
	reader io.Reader,
	mp *mpool.MPool,
) error {
	decodedGroups, err := unmarshalFromReaderNoGroup(reader, &exec.ret.optSplitResult)
	if err != nil {
		return err
	}
	exec.ret.setupT()
	if decodedGroups == 0 {
		exec.freeGroups()
		exec.distinctHash.free()
		return nil
	}

	ngrp, err := types.ReadInt64(reader)
	if err != nil {
		return err
	}
	if ngrp < 0 || ngrp != int64(decodedGroups) {
		return moerr.NewInternalErrorNoCtxf("median unmarshal: invalid group count %d, expected %d", ngrp, decodedGroups)
	}
	exec.freeGroups()
	exec.distinctHash.free()
	if ngrp != 0 {
		exec.groups = make([]*Vectors[T], ngrp)
		for i := range exec.groups {
			_, bs, err := types.ReadSizeBytes(reader)
			if err != nil {
				return err
			}
			grp := &Vectors[T]{}
			if err = grp.Unmarshal(bs, exec.argType, mp); err != nil {
				return err
			}
			exec.groups[i] = grp
		}
	}
	if exec.IsDistinct() {
		if err = exec.rebuildDistinctHash(); err != nil {
			return err
		}
	}

	return readAggregateExtra(reader)
}

// saveAccountedIntermediate preserves median's stable partial-result wire.
// The allocation-accounted arena is an execution-local representation only;
// changing the cross-pipeline codec would make mixed-version CNs unable to
// exchange partial median state.
func (exec *medianColumnExecSelf[T, R]) saveAccountedIntermediate(
	cnt int64,
	flags [][]uint8,
	writer io.Writer,
) error {
	if exec.accounted == nil || writer == nil {
		return moerr.NewInvalidInputNoCtx("invalid accounted median state")
	}
	if cnt < 0 {
		return moerr.NewInvalidInputNoCtx("invalid median selection count")
	}
	selected := int64(0)
	for chunk, state := range exec.accounted.state {
		selection, err := aggregateChunkSelection(
			flags, chunk, int(state.length))
		if err != nil {
			return err
		}
		for _, flag := range selection {
			selected += int64(flag)
		}
	}
	for chunk := len(exec.accounted.state); chunk < len(flags); chunk++ {
		if len(flags[chunk]) != 0 {
			return moerr.NewInvalidInputNoCtx("median selection chunk out of range")
		}
	}
	if selected != cnt {
		return moerr.NewInvalidInputNoCtxf(
			"median selection count %d does not match %d", selected, cnt)
	}
	if err := types.WriteInt64(writer, cnt); err != nil || cnt == 0 {
		return err
	}

	result, empty, err := exec.accountedIntermediateResult(flags, int(cnt))
	if err != nil {
		return err
	}
	defer result.Free(exec.mp)
	defer empty.Free(exec.mp)
	if err = result.MarshalBinaryTo(writer); err != nil {
		return err
	}
	if err = types.WriteInt64(writer, 1); err != nil {
		return err
	}
	if err = empty.MarshalBinaryTo(writer); err != nil {
		return err
	}
	// Median never used optSplitResult's distinct sidecar; DISTINCT is rebuilt
	// from the serialized retained values by the legacy decoder.
	if err = types.WriteInt64(writer, 0); err != nil {
		return err
	}
	if err = types.WriteInt64(writer, cnt); err != nil {
		return err
	}
	for chunk, state := range exec.accounted.state {
		selection, err := aggregateChunkSelection(
			flags, chunk, int(state.length))
		if err != nil {
			return err
		}
		for row, flag := range selection {
			if flag == 0 {
				continue
			}
			if err = exec.writeLegacyMedianGroup(
				&state, uint16(row), writer); err != nil {
				return err
			}
		}
	}
	return types.WriteInt64(writer, 0)
}

func (exec *medianColumnExecSelf[T, R]) accountedIntermediateResult(
	flags [][]uint8,
	rows int,
) (*vector.Vector, *vector.Vector, error) {
	result, err := exec.accounted.allocation.newVector(exec.retType)
	if err != nil {
		return nil, nil, err
	}
	empty, err := exec.accounted.allocation.newVector(types.T_bool.ToType())
	if err != nil {
		result.Free(exec.mp)
		return nil, nil, err
	}
	if err := result.PreExtend(rows, exec.mp); err != nil {
		result.Free(exec.mp)
		empty.Free(exec.mp)
		return nil, nil, err
	}
	if err := empty.PreExtend(rows, exec.mp); err != nil {
		result.Free(exec.mp)
		empty.Free(exec.mp)
		return nil, nil, err
	}
	result.SetLength(rows)
	empty.SetLength(rows)
	emptyValues := vector.MustFixedColNoTypeCheck[bool](empty)
	selected := 0
	for chunk, state := range exec.accounted.state {
		selection, err := aggregateChunkSelection(
			flags, chunk, int(state.length))
		if err != nil {
			result.Free(exec.mp)
			empty.Free(exec.mp)
			return nil, nil, err
		}
		for row, flag := range selection {
			if flag == 0 {
				continue
			}
			emptyValues[selected] = state.argCnt[row] == 0
			selected++
		}
	}
	return result, empty, nil
}

func (exec *medianColumnExecSelf[T, R]) writeLegacyMedianGroup(
	state *aggState,
	row uint16,
	writer io.Writer,
) error {
	if state == nil || writer == nil {
		return moerr.NewInvalidInputNoCtx("invalid median group state")
	}
	count := int(state.argCnt[row])
	typeSize := exec.argType.TypeSize()
	const nullsSize = 0
	vectorSize := 1 + types.TSize + 4 + 4 + count*typeSize + 4 + 4 + nullsSize + 1
	groupSize := 8 + 4 + vectorSize
	if groupSize > math.MaxInt32 {
		return mpool.ErrAllocationAllocatorLimit
	}
	if err := types.WriteInt32(writer, int32(groupSize)); err != nil {
		return err
	}
	if err := types.WriteInt64(writer, 1); err != nil {
		return err
	}
	if err := types.WriteUint32(writer, uint32(vectorSize)); err != nil {
		return err
	}
	if _, err := writeBytesRaw([]byte{byte(vector.FLAT)}, writer); err != nil {
		return err
	}
	if _, err := writeBytesRaw(types.EncodeType(&exec.argType), writer); err != nil {
		return err
	}
	if err := types.WriteUint32(writer, uint32(count)); err != nil {
		return err
	}
	if err := types.WriteUint32(writer, uint32(count*typeSize)); err != nil {
		return err
	}
	if err := state.iter(row, func(key []byte) error {
		payload := aggPayloadFromKey(&exec.accounted.aggInfo, key)
		if len(payload) != typeSize {
			return moerr.NewInvalidInputNoCtx("invalid median retained argument")
		}
		written, err := writer.Write(payload)
		if err == nil && written != len(payload) {
			return io.ErrShortWrite
		}
		return err
	}); err != nil {
		return err
	}
	if err := types.WriteUint32(writer, 0); err != nil {
		return err
	}
	if err := types.WriteUint32(writer, uint32(nullsSize)); err != nil {
		return err
	}
	_, err := writeBytesRaw([]byte{0}, writer)
	return err
}

func (exec *medianColumnExecSelf[T, R]) unmarshalAccountedIntermediate(
	reader io.Reader,
	mp *mpool.MPool,
) error {
	if reader == nil || mp == nil || exec.accounted == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	rows, err := types.ReadInt64(reader)
	if err != nil {
		return err
	}
	if rows < 0 || rows > int64(math.MaxInt) {
		return moerr.NewInvalidInputNoCtx("median has invalid result row count")
	}
	if rows == 0 {
		exec.accounted.Free()
		return nil
	}
	result, err := exec.accounted.allocation.newVector(exec.retType)
	if err != nil {
		return err
	}
	defer result.Free(mp)
	if err = result.UnmarshalWithReader(reader, mp); err != nil {
		return err
	}
	if !result.GetType().Eq(exec.retType) || result.Length() != int(rows) {
		return moerr.NewInvalidInputNoCtx("median result rows do not match")
	}
	emptyCount, err := types.ReadInt64(reader)
	if err != nil || emptyCount != 1 {
		return moerr.NewInvalidInputNoCtx("median has invalid empty-state count")
	}
	empty, err := exec.accounted.allocation.newVector(types.T_bool.ToType())
	if err != nil {
		return err
	}
	defer empty.Free(mp)
	if err = empty.UnmarshalWithReader(reader, mp); err != nil {
		return err
	}
	if !empty.GetType().Eq(types.T_bool.ToType()) || empty.Length() != int(rows) {
		return moerr.NewInvalidInputNoCtx("median empty-state rows do not match")
	}
	emptyValues := vector.MustFixedColNoTypeCheck[bool](empty)
	distinctCount, err := types.ReadInt64(reader)
	if err != nil || distinctCount != 0 {
		return moerr.NewInvalidInputNoCtx("median has invalid distinct sidecar")
	}
	groupCount, err := types.ReadInt64(reader)
	if err != nil || groupCount != rows {
		return moerr.NewInvalidInputNoCtx("median group rows do not match")
	}

	replacement := *exec.accounted
	replacement.state = nil
	replacement.standby = nil
	committed := false
	defer func() {
		if !committed {
			replacement.Free()
		}
	}()
	if err = replacement.PreAllocateGroups(int(rows)); err != nil {
		return err
	}
	if err = replacement.GroupGrow(int(rows)); err != nil {
		return err
	}
	var mapping [hashmap.UnitLimit]uint64
	for group := 0; group < int(rows); group++ {
		groupSize, err := types.ReadInt32AsInt(reader)
		if err != nil || groupSize < 0 {
			return moerr.NewInvalidInputNoCtx("median has invalid group frame")
		}
		limited := &io.LimitedReader{R: reader, N: int64(groupSize)}
		vectorCount, err := types.ReadInt64(limited)
		if err != nil || vectorCount <= 0 {
			return moerr.NewInvalidInputNoCtx("median has invalid vector count")
		}
		totalRows := 0
		for range vectorCount {
			wireSize, err := types.ReadUint32(limited)
			if err != nil || int64(wireSize) > limited.N {
				return io.ErrUnexpectedEOF
			}
			wire := &io.LimitedReader{R: limited, N: int64(wireSize)}
			source, err := exec.accounted.allocation.newVector(exec.argType)
			if err != nil {
				return err
			}
			if err = source.UnmarshalWithReader(wire, mp); err != nil {
				source.Free(mp)
				return err
			}
			if wire.N != 0 || !source.GetType().Eq(exec.argType) {
				source.Free(mp)
				return moerr.NewInvalidInputNoCtx("median has invalid argument vector")
			}
			if source.IsConst() || source.HasNull() ||
				source.Length() > math.MaxInt-totalRows {
				source.Free(mp)
				return moerr.NewInvalidInputNoCtx(
					"median has invalid argument vector")
			}
			totalRows += source.Length()
			for offset := 0; offset < source.Length(); offset += hashmap.UnitLimit {
				n := min(hashmap.UnitLimit, source.Length()-offset)
				for i := range n {
					mapping[i] = uint64(group + 1)
				}
				if err = replacement.PreflightBatchFill(
					offset, mapping[:n], []*vector.Vector{source}); err == nil {
					err = replacement.batchFillArgs(
						offset, mapping[:n], []*vector.Vector{source}, exec.IsDistinct())
				}
				if err != nil {
					source.Free(mp)
					return err
				}
			}
			source.Free(mp)
		}
		if limited.N != 0 {
			return moerr.NewInvalidInputNoCtx("median group frame was not consumed")
		}
		if emptyValues[group] != (totalRows == 0) {
			return moerr.NewInvalidInputNoCtx(
				"median empty state does not match retained arguments")
		}
	}
	extraCount, err := types.ReadInt64(reader)
	if err != nil {
		return err
	}
	if extraCount != 0 {
		return moerr.NewInvalidInputNoCtx("median has invalid extra state")
	}
	exec.accounted.Free()
	exec.accounted.state = replacement.state
	exec.accounted.standby = replacement.standby
	replacement.state = nil
	replacement.standby = nil
	committed = true
	return nil
}

func (exec *medianColumnExecSelf[T, R]) rebuildDistinctHash() error {
	if len(exec.groups) == 0 {
		return nil
	}
	if err := exec.distinctHash.grows(len(exec.groups)); err != nil {
		return err
	}
	for groupIdx, group := range exec.groups {
		for _, vec := range group.vecs {
			vals := vector.MustFixedColWithTypeCheck[T](vec)
			for row := range vals {
				if _, err := exec.distinctHash.fill(groupIdx, []*vector.Vector{vec}, row); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	if exec.accounted != nil {
		return exec.accounted.batchFillArgs(
			row, []uint64{uint64(groupIndex + 1)}, vectors, exec.IsDistinct())
	}
	if vectors[0].IsNull(uint64(row)) {
		return nil
	}
	if vectors[0].IsConst() {
		row = 0
	}
	if exec.IsDistinct() {
		need, err := exec.distinctHash.fill(groupIndex, vectors, row)
		if err != nil || !need {
			return err
		}
	}
	x, y := exec.ret.updateNextAccessIdx(groupIndex)
	markMedianGroupNotEmpty(&exec.ret, x, y)
	value := vector.MustFixedColWithTypeCheck[T](vectors[0])[row]
	return appendMedianValue(exec.groups[groupIndex], value, exec.mp)
}

func (exec *medianColumnExecSelf[T, R]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	if exec.accounted != nil {
		if len(vectors) == 0 || vectors[0] == nil {
			return mpool.ErrAllocationAccountInvalid
		}
		var groups [hashmap.UnitLimit]uint64
		for i := range groups {
			groups[i] = uint64(groupIndex + 1)
		}
		for offset := 0; offset < vectors[0].Length(); offset += hashmap.UnitLimit {
			n := min(hashmap.UnitLimit, vectors[0].Length()-offset)
			mapping := groups[:n]
			if err := exec.accounted.PreflightBatchFill(
				offset, mapping, vectors); err != nil {
				return err
			}
			if err := exec.accounted.batchFillArgs(
				offset, mapping, vectors, exec.IsDistinct()); err != nil {
				return err
			}
		}
		return nil
	}
	if vectors[0].IsConstNull() {
		return nil
	}
	if exec.IsDistinct() {
		return exec.distinctBulkFill(groupIndex, vectors)
	}

	x, y := exec.ret.updateNextAccessIdx(groupIndex)
	if vectors[0].IsConst() {
		markMedianGroupNotEmpty(&exec.ret, x, y)
		value := vector.MustFixedColWithTypeCheck[T](vectors[0])[0]
		return AppendMultiFixed(exec.groups[groupIndex], value, false, vectors[0].Length(), exec.mp)
	}

	vals := vector.MustFixedColWithTypeCheck[T](vectors[0])
	mustNotEmpty := false
	for i := range vals {
		if vectors[0].IsNull(uint64(i)) {
			continue
		}
		mustNotEmpty = true
		if err := appendMedianValue(exec.groups[groupIndex], vals[i], exec.mp); err != nil {
			return err
		}
	}
	if mustNotEmpty {
		markMedianGroupNotEmpty(&exec.ret, x, y)
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) distinctBulkFill(groupIndex int, vectors []*vector.Vector) error {
	x, y := exec.ret.updateNextAccessIdx(groupIndex)
	if vectors[0].IsConst() {
		need, err := exec.distinctHash.fill(groupIndex, vectors, 0)
		if err != nil || !need {
			return err
		}
		markMedianGroupNotEmpty(&exec.ret, x, y)
		value := vector.MustFixedColWithTypeCheck[T](vectors[0])[0]
		return appendMedianValue(exec.groups[groupIndex], value, exec.mp)
	}

	vals := vector.MustFixedColWithTypeCheck[T](vectors[0])
	mustNotEmpty := false
	for i := range vals {
		if vectors[0].IsNull(uint64(i)) {
			continue
		}
		need, err := exec.distinctHash.fill(groupIndex, vectors, i)
		if err != nil {
			return err
		}
		if !need {
			continue
		}
		mustNotEmpty = true
		if err = appendMedianValue(exec.groups[groupIndex], vals[i], exec.mp); err != nil {
			return err
		}
	}
	if mustNotEmpty {
		markMedianGroupNotEmpty(&exec.ret, x, y)
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if exec.accounted != nil {
		return exec.accounted.batchFillArgs(offset, groups, vectors, exec.IsDistinct())
	}
	if vectors[0].IsConstNull() {
		return nil
	}
	if exec.IsDistinct() {
		return exec.distinctBatchFill(offset, groups, vectors)
	}

	if vectors[0].IsConst() {
		value := vector.MustFixedColWithTypeCheck[T](vectors[0])[0]
		for _, group := range groups {
			if group == GroupNotMatched {
				continue
			}
			groupIndex := int(group - 1)
			x, y := exec.ret.updateNextAccessIdx(groupIndex)
			markMedianGroupNotEmpty(&exec.ret, x, y)
			if err := appendMedianValue(exec.groups[groupIndex], value, exec.mp); err != nil {
				return err
			}
		}
		return nil
	}

	vals := vector.MustFixedColWithTypeCheck[T](vectors[0])
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row := offset + i
		if vectors[0].IsNull(uint64(row)) {
			continue
		}
		groupIndex := int(group - 1)
		x, y := exec.ret.updateNextAccessIdx(groupIndex)
		markMedianGroupNotEmpty(&exec.ret, x, y)
		if err := appendMedianValue(exec.groups[groupIndex], vals[row], exec.mp); err != nil {
			return err
		}
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) distinctBatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if vectors[0].IsConst() {
		value := vector.MustFixedColWithTypeCheck[T](vectors[0])[0]
		for _, group := range groups {
			if group == GroupNotMatched {
				continue
			}
			need, err := exec.distinctHash.fill(int(group-1), vectors, 0)
			if err != nil {
				return err
			}
			if !need {
				continue
			}
			groupIndex := int(group - 1)
			x, y := exec.ret.updateNextAccessIdx(groupIndex)
			markMedianGroupNotEmpty(&exec.ret, x, y)
			if err = appendMedianValue(exec.groups[groupIndex], value, exec.mp); err != nil {
				return err
			}
		}
		return nil
	}

	vals := vector.MustFixedColWithTypeCheck[T](vectors[0])
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row := offset + i
		if vectors[0].IsNull(uint64(row)) {
			continue
		}
		need, err := exec.distinctHash.fill(int(group-1), vectors, row)
		if err != nil {
			return err
		}
		if !need {
			continue
		}
		groupIndex := int(group - 1)
		x, y := exec.ret.updateNextAccessIdx(groupIndex)
		markMedianGroupNotEmpty(&exec.ret, x, y)
		if err = appendMedianValue(exec.groups[groupIndex], vals[row], exec.mp); err != nil {
			return err
		}
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) mergeDistinctGroup(other *medianColumnExecSelf[T, R], groupIdx1, groupIdx2 int) error {
	for _, vec := range other.groups[groupIdx2].vecs {
		vals := vector.MustFixedColWithTypeCheck[T](vec)
		for row := range vals {
			need, err := exec.distinctHash.fill(groupIdx1, []*vector.Vector{vec}, row)
			if err != nil {
				return err
			}
			if !need {
				continue
			}
			x, y := exec.ret.updateNextAccessIdx(groupIdx1)
			markMedianGroupNotEmpty(&exec.ret, x, y)
			if err = appendMedianValue(exec.groups[groupIdx1], vals[row], exec.mp); err != nil {
				return err
			}
		}
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) Merge(other *medianColumnExecSelf[T, R], groupIdx1, groupIdx2 int) error {
	if !exec.mergeCompatible(other) {
		return mpool.ErrAllocationAccountMismatch
	}
	if exec.accounted != nil || other.accounted != nil {
		if exec.accounted == nil || other.accounted == nil {
			return mpool.ErrAllocationAccountMismatch
		}
		return exec.accounted.batchMergeArgs(
			other.accounted, groupIdx2, []uint64{uint64(groupIdx1 + 1)}, exec.IsDistinct())
	}
	if exec.IsDistinct() {
		return exec.mergeDistinctGroup(other, groupIdx1, groupIdx2)
	}
	if other.groups[groupIdx2].Length() == 0 {
		return nil
	}
	x, y := exec.ret.updateNextAccessIdx(groupIdx1)
	markMedianGroupNotEmpty(&exec.ret, x, y)
	return exec.groups[groupIdx1].Union(other.groups[groupIdx2], exec.mp)
}

func (exec *medianColumnExecSelf[T, R]) BatchMerge(next *medianColumnExecSelf[T, R], offset int, groups []uint64) error {
	if !exec.mergeCompatible(next) {
		return mpool.ErrAllocationAccountMismatch
	}
	if exec.accounted != nil || next.accounted != nil {
		if exec.accounted == nil || next.accounted == nil {
			return mpool.ErrAllocationAccountMismatch
		}
		return exec.accounted.batchMergeArgs(
			next.accounted, offset, groups, exec.IsDistinct())
	}
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		if err := exec.Merge(next, int(group)-1, i+offset); err != nil {
			return err
		}
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) mergeCompatible(
	other *medianColumnExecSelf[T, R],
) bool {
	return exec != nil && other != nil &&
		exec.AggID() == other.AggID() &&
		exec.IsDistinct() == other.IsDistinct() &&
		exec.argType.Eq(other.argType) &&
		exec.retType.Eq(other.retType)
}

func (exec *medianColumnExecSelf[T, R]) SetExtraInformation(partialResult any, groupIndex int) error {
	return nil
}

func (exec *medianColumnExecSelf[T, R]) SetAllocationAccount(
	allocation *AllocationAccount,
) error {
	if exec == nil || allocation == nil || allocation.account == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if exec.accounted != nil {
		return exec.accounted.SetAllocationAccount(allocation)
	}
	if len(exec.groups) != 0 || len(exec.ret.resultList) != 1 ||
		exec.ret.resultList[0].Length() != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	base := &aggExec{
		mp: exec.mp,
		aggInfo: aggInfo{
			aggId:      exec.AggID(),
			isDistinct: exec.IsDistinct(),
			argTypes:   []types.Type{exec.argType},
			retType:    exec.retType,
			emptyNull:  true,
			saveArg:    true,
		},
	}
	if err := base.SetAllocationAccount(allocation); err != nil {
		return err
	}
	exec.distinctHash.free()
	exec.accounted = base
	return nil
}

func (exec *medianColumnExecSelf[T, R]) ClearAllocationAccount(
	allocation *AllocationAccount,
) error {
	if exec.accounted != nil {
		return exec.accounted.ClearAllocationAccount(allocation)
	}
	return nil
}

func (*medianColumnExecSelf[T, R]) PrepareParamKindChunkCount() int {
	return 0
}

func (*medianColumnExecSelf[T, R]) PrepareParamKindVectorForChunk(int) *vector.Vector {
	return nil
}

func (*medianColumnExecSelf[T, R]) SetPrepareParamKind(vector.PrepareParamKind) {}

func (exec *medianColumnExecSelf[T, R]) GetNumGroups() int {
	if exec == nil {
		return 0
	}
	if exec.accounted != nil {
		return exec.accounted.GetNumGroups()
	}
	return len(exec.groups)
}

func (*medianColumnExecSelf[T, R]) AdditionalMemorySize() int64 { return 0 }

func (exec *medianColumnExecSelf[T, R]) PreflightBatchFill(
	offset int, groups []uint64, vectors []*vector.Vector,
) error {
	if exec.accounted == nil {
		return nil
	}
	return exec.accounted.PreflightBatchFill(offset, groups, vectors)
}

func (exec *medianColumnExecSelf[T, R]) preflightBatchMerge(
	next *medianColumnExecSelf[T, R], offset int, groups []uint64,
) error {
	if exec.accounted == nil {
		return nil
	}
	if next == nil || !exec.mergeCompatible(next) || next.accounted == nil {
		return mpool.ErrAllocationAccountMismatch
	}
	return exec.accounted.preflightBatchMergeArgs(
		next.accounted, offset, groups)
}

func (exec *medianColumnExecSelf[T, R]) SaveSpillIntermediateRows(
	chunk int, rows []int32, writer io.Writer,
) error {
	if exec.accounted == nil {
		return moerr.NewNotSupportedNoCtx("median has no bounded spill state")
	}
	return exec.accounted.SaveSpillIntermediateRows(chunk, rows, writer)
}

func (exec *medianColumnExecSelf[T, R]) UnmarshalSpillFromReader(
	reader io.Reader, mp *mpool.MPool,
) error {
	if exec.accounted == nil {
		return moerr.NewNotSupportedNoCtx("median has no bounded spill state")
	}
	return exec.accounted.UnmarshalSpillFromReader(reader, mp)
}

func (exec *medianColumnExecSelf[T, R]) Free() {
	if exec.accounted != nil {
		exec.accounted.Free()
	}
	exec.freeGroups()
	exec.ret.free()
	exec.distinctHash.free()
}

func (exec *medianColumnExecSelf[T, R]) freeGroups() {
	for _, group := range exec.groups {
		if group != nil {
			group.Free(exec.mp)
		}
	}
	exec.groups = nil
}

func (exec *medianColumnExecSelf[T, R]) Size() int64 {
	if exec.accounted != nil {
		var size int64
		for _, state := range exec.accounted.state {
			size += int64(cap(state.argCnt))*4 +
				int64(cap(state.argbuf)) + int64(cap(state.argScratch))
		}
		for _, state := range exec.accounted.standby {
			size += int64(cap(state.argCnt))*4 +
				int64(cap(state.argbuf)) + int64(cap(state.argScratch))
		}
		return size
	}
	var size int64
	for _, group := range exec.groups {
		if group != nil {
			size += group.Size()
		}
	}
	size += int64(cap(exec.groups)) * 8
	return exec.ret.Size() + exec.distinctHash.Size() + size
}

type medianColumnNumericExec[T numeric] struct {
	medianColumnExecSelf[T, float64]
}

func (exec *medianColumnNumericExec[T]) PreflightBatchMerge(
	next AggFuncExec, offset int, groups []uint64,
) error {
	other, ok := next.(*medianColumnNumericExec[T])
	if !ok {
		return mpool.ErrAllocationAccountMismatch
	}
	return exec.preflightBatchMerge(
		&other.medianColumnExecSelf, offset, groups)
}

func newMedianColumnNumericExec[T numeric](mp *mpool.MPool, info singleAggInfo) AggFuncExec {
	return &medianColumnNumericExec[T]{
		medianColumnExecSelf: newMedianColumnExecSelf[T, float64](mp, info, 0),
	}
}

type medianColumnDecimalExec[T types.Decimal64 | types.Decimal128] struct {
	medianColumnExecSelf[T, types.Decimal128]
}

func (exec *medianColumnDecimalExec[T]) PreflightBatchMerge(
	next AggFuncExec, offset int, groups []uint64,
) error {
	other, ok := next.(*medianColumnDecimalExec[T])
	if !ok {
		return mpool.ErrAllocationAccountMismatch
	}
	return exec.preflightBatchMerge(
		&other.medianColumnExecSelf, offset, groups)
}

func newMedianColumnDecimalExec[T types.Decimal64 | types.Decimal128](mp *mpool.MPool, info singleAggInfo) AggFuncExec {
	return &medianColumnDecimalExec[T]{
		medianColumnExecSelf: newMedianColumnExecSelf[T, types.Decimal128](mp, info, types.Decimal128{}),
	}
}

func newMedianExec(mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type) (AggFuncExec, error) {
	info := singleAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argType:   param,
		retType:   MedianReturnType([]types.Type{param}),
		emptyNull: true,
	}

	switch param.Oid {
	case types.T_bit:
		return newMedianColumnNumericExec[uint64](mp, info), nil
	case types.T_int8:
		return newMedianColumnNumericExec[int8](mp, info), nil
	case types.T_int16:
		return newMedianColumnNumericExec[int16](mp, info), nil
	case types.T_int32:
		return newMedianColumnNumericExec[int32](mp, info), nil
	case types.T_int64:
		return newMedianColumnNumericExec[int64](mp, info), nil
	case types.T_uint8:
		return newMedianColumnNumericExec[uint8](mp, info), nil
	case types.T_uint16:
		return newMedianColumnNumericExec[uint16](mp, info), nil
	case types.T_uint32:
		return newMedianColumnNumericExec[uint32](mp, info), nil
	case types.T_uint64:
		return newMedianColumnNumericExec[uint64](mp, info), nil
	case types.T_float32:
		return newMedianColumnNumericExec[float32](mp, info), nil
	case types.T_float64:
		return newMedianColumnNumericExec[float64](mp, info), nil
	case types.T_decimal64:
		return newMedianColumnDecimalExec[types.Decimal64](mp, info), nil
	case types.T_decimal128:
		return newMedianColumnDecimalExec[types.Decimal128](mp, info), nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("unsupported type for median()")
	}
}

func (exec *medianColumnNumericExec[T]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*medianColumnNumericExec[T])
	return exec.medianColumnExecSelf.Merge(&other.medianColumnExecSelf, groupIdx1, groupIdx2)
}

func (exec *medianColumnNumericExec[T]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*medianColumnNumericExec[T])
	return exec.medianColumnExecSelf.BatchMerge(&other.medianColumnExecSelf, offset, groups)
}

func (exec *medianColumnNumericExec[T]) Flush() ([]*vector.Vector, error) {
	if exec.accounted != nil {
		return flushAccountedMedianNumeric(exec)
	}
	vs := exec.ret.values
	groups := len(exec.groups)
	lim := exec.ret.getChunkSize()
	for i, x := 0, 0; i < groups; i += lim {
		n := groups - i
		if n > lim {
			n = lim
		}
		s := i
		for j := 0; j < n; j++ {
			rows := exec.groups[s].Length()
			if rows == 0 {
				s++
				continue
			}
			markMedianGroupNotEmpty(&exec.ret, x, j)
			v, err := MedianNumeric(exec.groups[s])
			if err != nil {
				return nil, err
			}
			vs[x][j] = v
			s++
		}
	}
	return exec.ret.flushAll(), nil
}

func (exec *medianColumnDecimalExec[T]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*medianColumnDecimalExec[T])
	return exec.medianColumnExecSelf.Merge(&other.medianColumnExecSelf, groupIdx1, groupIdx2)
}

func (exec *medianColumnDecimalExec[T]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*medianColumnDecimalExec[T])
	return exec.medianColumnExecSelf.BatchMerge(&other.medianColumnExecSelf, offset, groups)
}

func (exec *medianColumnDecimalExec[T]) Flush() ([]*vector.Vector, error) {
	if exec.accounted != nil {
		return flushAccountedMedianDecimal(exec)
	}
	vs := exec.ret.values
	argIsDecimal128 := exec.singleAggInfo.argType.Oid == types.T_decimal128
	groups := len(exec.groups)
	lim := exec.ret.getChunkSize()
	for i, x := 0, 0; i < groups; i += lim {
		n := groups - i
		if n > lim {
			n = lim
		}
		s := i
		for j := 0; j < n; j++ {
			rows := exec.groups[s].Length()
			if rows == 0 {
				s++
				continue
			}
			markMedianGroupNotEmpty(&exec.ret, x, j)
			var (
				v   types.Decimal128
				err error
			)
			if argIsDecimal128 {
				v, err = MedianDecimal128(any(exec.groups[s]).(*Vectors[types.Decimal128]))
			} else {
				v, err = MedianDecimal64(any(exec.groups[s]).(*Vectors[types.Decimal64]))
			}
			if err != nil {
				return nil, err
			}
			vs[x][j] = v
			s++
		}
	}
	return exec.ret.flushAll(), nil
}

func flushAccountedMedianNumeric[T numeric](
	exec *medianColumnNumericExec[T],
) (_ []*vector.Vector, retErr error) {
	results := make([]*vector.Vector, len(exec.accounted.state))
	defer freeAggregateResultsOnError(exec.mp, results, &retErr)
	for chunk := range exec.accounted.state {
		state := &exec.accounted.state[chunk]
		result, err := exec.accounted.allocation.newVector(exec.retType)
		if err != nil {
			return nil, err
		}
		results[chunk] = result
		if err = result.PreExtendNulls(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		if err = result.PreExtend(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		result.SetLength(int(state.length))
		values := vector.MustFixedColNoTypeCheck[float64](result)
		for row := uint16(0); row < uint16(state.length); row++ {
			if state.argCnt[row] == 0 {
				result.SetNull(uint64(row))
				continue
			}
			scratch, err := makeAccountedScratch[T](
				exec.accounted.allocation, exec.mp, int(state.argCnt[row]))
			if err != nil {
				return nil, err
			}
			index := 0
			err = state.iter(row, func(key []byte) error {
				payload := aggPayloadFromKey(&exec.accounted.aggInfo, key)
				if len(payload) != exec.argType.TypeSize() || index >= len(scratch) {
					return moerr.NewInternalErrorNoCtx("median has invalid retained argument")
				}
				scratch[index] = types.DecodeFixed[T](payload)
				index++
				return nil
			})
			if err == nil && index != len(scratch) {
				err = moerr.NewInternalErrorNoCtx("median retained argument count mismatch")
			}
			if err == nil {
				values[row] = medianNumericVals(scratch)
			}
			mpool.FreeSlice(exec.mp, scratch)
			if err != nil {
				return nil, err
			}
		}
	}
	return results, nil
}

func flushAccountedMedianDecimal[T types.Decimal64 | types.Decimal128](
	exec *medianColumnDecimalExec[T],
) (_ []*vector.Vector, retErr error) {
	results := make([]*vector.Vector, len(exec.accounted.state))
	defer freeAggregateResultsOnError(exec.mp, results, &retErr)
	for chunk := range exec.accounted.state {
		state := &exec.accounted.state[chunk]
		result, err := exec.accounted.allocation.newVector(exec.retType)
		if err != nil {
			return nil, err
		}
		results[chunk] = result
		if err = result.PreExtendNulls(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		if err = result.PreExtend(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		result.SetLength(int(state.length))
		values := vector.MustFixedColNoTypeCheck[types.Decimal128](result)
		for row := uint16(0); row < uint16(state.length); row++ {
			if state.argCnt[row] == 0 {
				result.SetNull(uint64(row))
				continue
			}
			scratch, err := makeAccountedScratch[T](
				exec.accounted.allocation, exec.mp, int(state.argCnt[row]))
			if err != nil {
				return nil, err
			}
			index := 0
			err = state.iter(row, func(key []byte) error {
				payload := aggPayloadFromKey(&exec.accounted.aggInfo, key)
				if len(payload) != exec.argType.TypeSize() || index >= len(scratch) {
					return moerr.NewInternalErrorNoCtx("median has invalid retained argument")
				}
				scratch[index] = types.DecodeFixed[T](payload)
				index++
				return nil
			})
			if err == nil && index != len(scratch) {
				err = moerr.NewInternalErrorNoCtx("median retained argument count mismatch")
			}
			if err == nil {
				switch vals := any(scratch).(type) {
				case []types.Decimal64:
					values[row], err = medianDecimal64Vals(vals)
				case []types.Decimal128:
					values[row], err = medianDecimal128Vals(vals)
				default:
					err = moerr.NewInternalErrorNoCtx("median decimal type mismatch")
				}
			}
			mpool.FreeSlice(exec.mp, scratch)
			if err != nil {
				return nil, err
			}
		}
	}
	return results, nil
}

func freeAggregateResultsOnError(
	mp *mpool.MPool, results []*vector.Vector, resultErr *error,
) {
	if resultErr == nil || *resultErr == nil {
		return
	}
	for _, result := range results {
		if result != nil {
			result.Free(mp)
		}
	}
}

func appendMedianValue[T numeric | types.Decimal64 | types.Decimal128](vecs *Vectors[T], value T, mp *mpool.MPool) error {
	vec := vecs.getAppendableVector()
	return vector.AppendFixed(vec, value, false, mp)
}

func markMedianGroupNotEmpty[T types.FixedSizeTExceptStrType](ret *aggResultWithFixedType[T], x, y int) {
	if len(ret.bsFromEmptyList) > x && ret.bsFromEmptyList[x] != nil {
		ret.bsFromEmptyList[x][y] = false
	}
}

func medianDecimal64FromState(st aggState, idx uint16, info *aggInfo) (types.Decimal128, error) {
	vals := make([]types.Decimal64, 0, st.argCnt[idx])
	if err := st.iter(idx, func(k []byte) error {
		vals = append(vals, types.DecodeDecimal64(aggPayloadFromKey(info, k)))
		return nil
	}); err != nil {
		return types.Decimal128{}, err
	}
	return medianDecimal64Vals(vals)
}

func medianDecimal128FromState(st aggState, idx uint16, info *aggInfo) (types.Decimal128, error) {
	vals := make([]types.Decimal128, 0, st.argCnt[idx])
	if err := st.iter(idx, func(k []byte) error {
		vals = append(vals, types.DecodeDecimal128(aggPayloadFromKey(info, k)))
		return nil
	}); err != nil {
		return types.Decimal128{}, err
	}
	return medianDecimal128Vals(vals)
}

func MedianNumeric[T numeric](vs *Vectors[T]) (float64, error) {
	vals := collectMedianValues(vs)
	return medianNumericVals(vals), nil
}

func MedianDecimal64(vs *Vectors[types.Decimal64]) (types.Decimal128, error) {
	vals := collectMedianValues(vs)
	return medianDecimal64Vals(vals)
}

func MedianDecimal128(vs *Vectors[types.Decimal128]) (types.Decimal128, error) {
	vals := collectMedianValues(vs)
	return medianDecimal128Vals(vals)
}

func collectMedianValues[T numeric | types.Decimal64 | types.Decimal128](vs *Vectors[T]) []T {
	vals := make([]T, 0, vs.Length())
	for _, vec := range vs.vecs {
		vals = append(vals, vector.MustFixedColWithTypeCheck[T](vec)...)
	}
	return vals
}

func medianNumericVals[T numeric](vals []T) float64 {
	rows := len(vals)
	if rows&1 == 1 {
		return float64(selectKthNumeric(vals, rows>>1))
	}
	v1 := selectKthNumeric(vals, rows>>1-1)
	v2 := selectKthNumeric(vals, rows>>1)
	return (float64(v1) + float64(v2)) / 2
}

func medianDecimal64Vals(vals []types.Decimal64) (types.Decimal128, error) {
	rows := len(vals)
	if rows&1 == 1 {
		return FromD64ToD128(selectKthFunc(vals, rows>>1, func(a, b types.Decimal64) int {
			return a.Compare(b)
		})).Scale(1)
	}
	v1 := FromD64ToD128(selectKthFunc(vals, rows>>1-1, func(a, b types.Decimal64) int {
		return a.Compare(b)
	}))
	v2 := FromD64ToD128(selectKthFunc(vals, rows>>1, func(a, b types.Decimal64) int {
		return a.Compare(b)
	}))
	ret, err := v1.Add128(v2)
	if err != nil {
		return types.Decimal128{}, err
	}
	if ret.Sign() {
		if ret, err = ret.Minus().Scale(1); err != nil {
			return types.Decimal128{}, err
		}
		return ret.Right(1).Minus(), nil
	}
	if ret, err = ret.Scale(1); err != nil {
		return types.Decimal128{}, err
	}
	return ret.Right(1), nil
}

func medianDecimal128Vals(vals []types.Decimal128) (types.Decimal128, error) {
	rows := len(vals)
	if rows&1 == 1 {
		ret := selectKthFunc(vals, rows>>1, func(a, b types.Decimal128) int {
			return a.Compare(b)
		})
		return ret.Scale(1)
	}
	v1 := selectKthFunc(vals, rows>>1-1, func(a, b types.Decimal128) int {
		return a.Compare(b)
	})
	v2 := selectKthFunc(vals, rows>>1, func(a, b types.Decimal128) int {
		return a.Compare(b)
	})
	ret, err := v1.Add128(v2)
	if err != nil {
		return types.Decimal128{}, err
	}
	if ret.Sign() {
		if ret, err = ret.Minus().Scale(1); err != nil {
			return types.Decimal128{}, err
		}
		return ret.Right(1).Minus(), nil
	}
	if ret, err = ret.Scale(1); err != nil {
		return types.Decimal128{}, err
	}
	return ret.Right(1), nil
}

func selectKthNumeric[T cmp.Ordered](vals []T, k int) T {
	return selectKthFunc(vals, k, func(a, b T) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
}

func selectKthFunc[T any](vals []T, k int, compare func(a, b T) int) T {
	left, right := 0, len(vals)-1
	for {
		if left == right {
			return vals[left]
		}

		lt, gt := partitionAroundPivot(vals, left, right, (left+right)>>1, compare)
		switch {
		case k < lt:
			right = lt - 1
		case k > gt:
			left = gt + 1
		default:
			return vals[k]
		}
	}
}

func partitionAroundPivot[T any](vals []T, left, right, pivot int, compare func(a, b T) int) (int, int) {
	pivotValue := vals[pivot]
	vals[pivot], vals[right] = vals[right], vals[pivot]
	lt, i, gt := left, left, right
	for i <= gt {
		switch cmp := compare(vals[i], pivotValue); {
		case cmp < 0:
			vals[lt], vals[i] = vals[i], vals[lt]
			lt++
			i++
		case cmp > 0:
			vals[i], vals[gt] = vals[gt], vals[i]
			gt--
		default:
			i++
		}
	}
	return lt, gt
}
