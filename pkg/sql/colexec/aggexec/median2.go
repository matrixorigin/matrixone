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

// usesDenseAccountedState is deliberately limited to ordinary, non-DISTINCT
// MEDIAN. Ordered percentile embeds this implementation too, but needs its
// ordered saved-argument representation. DISTINCT median likewise still needs
// the generic dedup index.
func (exec *medianColumnExecSelf[T, R]) usesDenseAccountedState() bool {
	return exec != nil && exec.accounted != nil &&
		exec.AggID() == AggIdOfMedian && !exec.IsDistinct() &&
		!exec.accounted.saveArg
}

func (exec *medianColumnExecSelf[T, R]) denseGroupIndex(chunk, row int) (int, error) {
	if !exec.usesDenseAccountedState() || chunk < 0 || row < 0 ||
		chunk >= len(exec.accounted.state) || row >= int(exec.accounted.state[chunk].length) {
		return 0, mpool.ErrAllocationAccountInvariant
	}
	index := row
	for i := 0; i < chunk; i++ {
		index += int(exec.accounted.state[i].length)
	}
	if index >= len(exec.groups) || exec.groups[index] == nil {
		return 0, mpool.ErrAllocationAccountInvariant
	}
	return index, nil
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
		if exec.usesDenseAccountedState() {
			if more < 0 {
				return mpool.ErrAllocationAccountInvalid
			}
			pending := make([]*Vectors[T], more)
			for i := range pending {
				var err error
				pending[i], err = newAccountedVectors[T](
					exec.argType, exec.accounted.allocation)
				if err != nil {
					for j := 0; j < i; j++ {
						pending[j].Free(exec.mp)
					}
					return err
				}
			}
			if err := exec.accounted.GroupGrow(more); err != nil {
				for _, group := range pending {
					group.Free(exec.mp)
				}
				return err
			}
			exec.groups = append(exec.groups, pending...)
			return nil
		}
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
		if exec.usesDenseAccountedState() && more > 0 {
			oldLength := len(exec.groups)
			exec.groups = append(exec.groups, make([]*Vectors[T], more)...)
			exec.groups = exec.groups[:oldLength]
		}
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
		if exec.usesDenseAccountedState() {
			group, groupErr := exec.denseGroupIndex(chunk, row)
			if groupErr != nil {
				return groupErr
			}
			emptyValues[row] = exec.groups[group].Length() == 0
		} else {
			emptyValues[row] = state.argCnt[row] == 0
		}
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
		var denseGroup *Vectors[T]
		if exec.usesDenseAccountedState() {
			group, groupErr := exec.denseGroupIndex(chunk, row)
			if groupErr != nil {
				return groupErr
			}
			denseGroup = exec.groups[group]
		}
		if err = exec.writeLegacyMedianGroup(
			state, uint16(row), denseGroup, writer); err != nil {
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
	groupBase := 0
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
			var denseGroup *Vectors[T]
			if exec.usesDenseAccountedState() {
				if groupBase+row >= len(exec.groups) {
					return mpool.ErrAllocationAccountInvariant
				}
				denseGroup = exec.groups[groupBase+row]
			}
			if err = exec.writeLegacyMedianGroup(
				&state, uint16(row), denseGroup, writer); err != nil {
				return err
			}
		}
		groupBase += int(state.length)
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
	groupBase := 0
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
			if exec.usesDenseAccountedState() {
				if groupBase+row >= len(exec.groups) {
					result.Free(exec.mp)
					empty.Free(exec.mp)
					return nil, nil, mpool.ErrAllocationAccountInvariant
				}
				emptyValues[selected] = exec.groups[groupBase+row].Length() == 0
			} else {
				emptyValues[selected] = state.argCnt[row] == 0
			}
			selected++
		}
		groupBase += int(state.length)
	}
	return result, empty, nil
}

func (exec *medianColumnExecSelf[T, R]) writeLegacyMedianGroup(
	state *aggState,
	row uint16,
	denseGroup *Vectors[T],
	writer io.Writer,
) error {
	if state == nil || writer == nil {
		return moerr.NewInvalidInputNoCtx("invalid median group state")
	}
	count := 0
	if denseGroup != nil {
		count = denseGroup.Length()
	} else {
		count = int(state.argCnt[row])
	}
	typeSize := exec.argType.TypeSize()
	const nullsSize = 0
	const fixedVectorFrameSize = 1 + types.TSize + 4 + 4 + 4 + 4 + nullsSize + 1
	if typeSize <= 0 || count > (math.MaxInt32-8-4-fixedVectorFrameSize)/typeSize {
		return mpool.ErrAllocationAllocatorLimit
	}
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
	if denseGroup != nil {
		for _, vec := range denseGroup.vecs {
			payload := vec.UnsafeGetRawData()
			written, err := writer.Write(payload)
			if err != nil {
				return err
			}
			if written != len(payload) {
				return io.ErrShortWrite
			}
		}
	} else {
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
		exec.freeGroups()
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
	var replacementGroups []*Vectors[T]
	committed := false
	defer func() {
		if !committed {
			replacement.Free()
			for _, group := range replacementGroups {
				if group != nil {
					group.Free(mp)
				}
			}
		}
	}()
	if err = replacement.PreAllocateGroups(int(rows)); err != nil {
		return err
	}
	if err = replacement.GroupGrow(int(rows)); err != nil {
		return err
	}
	if exec.usesDenseAccountedState() {
		replacementGroups = make([]*Vectors[T], int(rows))
		for group := range replacementGroups {
			replacementGroups[group], err = newAccountedVectors[T](
				exec.argType, replacement.allocation)
			if err != nil {
				return err
			}
		}
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
		denseSawPartial := false
		for range vectorCount {
			if denseSawPartial {
				return moerr.NewInvalidInputNoCtx(
					"median has a non-terminal partial argument vector")
			}
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
			if exec.usesDenseAccountedState() && vectorCount > 1 &&
				source.Length() > MaxVectorLength {
				source.Free(mp)
				return moerr.NewInvalidInputNoCtx(
					"median has an oversized multi-vector argument")
			}
			totalRows += source.Length()
			if exec.usesDenseAccountedState() {
				if source.Length() < MaxVectorLength {
					denseSawPartial = true
				}
				dense := replacementGroups[group]
				if len(dense.vecs) == 1 && dense.vecs[0].Length() == 0 &&
					dense.vecs[0].Allocated() == 0 {
					dense.vecs[0].Free(mp)
					dense.vecs[0] = source
				} else {
					dense.vecs = append(dense.vecs, source)
				}
			} else {
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
		}
		if limited.N != 0 {
			return moerr.NewInvalidInputNoCtx("median group frame was not consumed")
		}
		if emptyValues[group] != (totalRows == 0) {
			return moerr.NewInvalidInputNoCtx(
				"median empty state does not match retained arguments")
		}
		if exec.usesDenseAccountedState() {
			dense := replacementGroups[group]
			dense.appendAt = len(dense.vecs) - 1
			if dense.vecs[dense.appendAt].Length() >= MaxVectorLength {
				dense.appendAt++
			}
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
	if exec.usesDenseAccountedState() {
		exec.freeGroups()
		exec.groups = replacementGroups
		replacementGroups = nil
	}
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

const denseMedianPreflightSlots = 2 * hashmap.UnitLimit

func denseMedianSlot(group uint64) int {
	// Fibonacci hashing keeps adjacent group ids from clustering while the
	// power-of-two table makes probing cheap.
	return int((group * 11400714819323198485) & (denseMedianPreflightSlots - 1))
}

func addDenseMedianNeed(
	keys *[denseMedianPreflightSlots]uint64,
	counts *[denseMedianPreflightSlots]int,
	group uint64,
	rows int,
) error {
	if group == GroupNotMatched || rows == 0 {
		return nil
	}
	for slot := denseMedianSlot(group); ; slot = (slot + 1) & (denseMedianPreflightSlots - 1) {
		if keys[slot] == 0 {
			keys[slot] = group
			counts[slot] = rows
			return nil
		}
		if keys[slot] == group {
			if counts[slot] > math.MaxInt-rows {
				return mpool.ErrAllocationAllocatorLimit
			}
			counts[slot] += rows
			return nil
		}
	}
}

func (exec *medianColumnExecSelf[T, R]) applyDenseMedianNeeds(
	keys *[denseMedianPreflightSlots]uint64,
	counts *[denseMedianPreflightSlots]int,
) error {
	for slot, group := range keys {
		if group == 0 {
			continue
		}
		index := group - 1
		if index >= uint64(len(exec.groups)) || exec.groups[index] == nil {
			return mpool.ErrAllocationAccountInvariant
		}
		if err := exec.groups[index].PreExtend(counts[slot], exec.mp); err != nil {
			return err
		}
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) preflightDenseBatchFill(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if !exec.usesDenseAccountedState() || len(vectors) != 1 || vectors[0] == nil ||
		!exec.argType.Eq(*vectors[0].GetType()) {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := validatePreflightVectors(vectors, offset, len(groups)); err != nil {
		return err
	}
	var firstGroup uint64
	firstCount := 0
	mixedGroups := false
	plainInput := !vectors[0].IsConst() && !vectors[0].HasNull()
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		if group > uint64(len(exec.groups)) {
			return mpool.ErrAllocationAccountInvariant
		}
		if !plainInput {
			row, err := preflightPhysicalRow(vectors[0], offset+i)
			if err != nil {
				return err
			}
			if vectors[0].IsNull(uint64(row)) {
				continue
			}
		}
		if firstGroup == 0 {
			firstGroup = group
		}
		if group == firstGroup {
			firstCount++
		} else {
			mixedGroups = true
		}
	}
	if !mixedGroups {
		if firstGroup == 0 {
			return nil
		}
		return exec.groups[firstGroup-1].PreExtend(firstCount, exec.mp)
	}
	return exec.preflightDenseBatchFillMixed(offset, groups, vectors[0])
}

func (exec *medianColumnExecSelf[T, R]) preflightDenseBatchFillMixed(
	offset int,
	groups []uint64,
	input *vector.Vector,
) error {
	var keys [denseMedianPreflightSlots]uint64
	var counts [denseMedianPreflightSlots]int
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row := offset + i
		if input.IsConst() {
			row = 0
		}
		if input.IsNull(uint64(row)) {
			continue
		}
		if err := addDenseMedianNeed(&keys, &counts, group, 1); err != nil {
			return err
		}
	}
	return exec.applyDenseMedianNeeds(&keys, &counts)
}

func (exec *medianColumnExecSelf[T, R]) denseBatchFill(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if !exec.usesDenseAccountedState() || len(vectors) != 1 || vectors[0] == nil ||
		len(groups) > hashmap.UnitLimit || offset < 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	input := vectors[0]
	values := vector.MustFixedColWithTypeCheck[T](input)
	if len(groups) != 0 && !input.IsConst() && !input.HasNull() {
		if offset > len(values)-len(groups) {
			return mpool.ErrAllocationAccountInvalid
		}
		group := groups[0]
		if group != GroupNotMatched {
			allSame := true
			for _, candidate := range groups[1:] {
				if candidate != group {
					allSame = false
					break
				}
			}
			if allSame {
				index := group - 1
				if index >= uint64(len(exec.groups)) || exec.groups[index] == nil {
					return mpool.ErrAllocationAccountInvariant
				}
				return appendMedianValues(
					exec.groups[index], values[offset:offset+len(groups)], exec.mp)
			}
		}
	}
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row := offset + i
		if vectors[0].IsConst() {
			row = 0
		}
		if vectors[0].IsNull(uint64(row)) {
			continue
		}
		index := group - 1
		if index >= uint64(len(exec.groups)) || exec.groups[index] == nil {
			return mpool.ErrAllocationAccountInvariant
		}
		if err := appendMedianValue(exec.groups[index], values[row], exec.mp); err != nil {
			return err
		}
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) preflightDenseBatchMerge(
	next *medianColumnExecSelf[T, R],
	offset int,
	groups []uint64,
) error {
	if !exec.usesDenseAccountedState() || next == nil ||
		!next.usesDenseAccountedState() || offset < 0 ||
		len(groups) > hashmap.UnitLimit || offset > len(next.groups)-len(groups) {
		return mpool.ErrAllocationAccountInvalid
	}
	var firstGroup uint64
	firstCount := 0
	mixedGroups := false
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		if group > uint64(len(exec.groups)) {
			return mpool.ErrAllocationAccountInvariant
		}
		rows := next.groups[offset+i].Length()
		if rows == 0 {
			continue
		}
		if firstGroup == 0 {
			firstGroup = group
		}
		if group == firstGroup {
			if firstCount > math.MaxInt-rows {
				return mpool.ErrAllocationAllocatorLimit
			}
			firstCount += rows
		} else {
			mixedGroups = true
		}
	}
	if !mixedGroups {
		if firstGroup == 0 {
			return nil
		}
		return exec.groups[firstGroup-1].PreExtend(firstCount, exec.mp)
	}
	return exec.preflightDenseBatchMergeMixed(next, offset, groups)
}

func (exec *medianColumnExecSelf[T, R]) preflightDenseBatchMergeMixed(
	next *medianColumnExecSelf[T, R],
	offset int,
	groups []uint64,
) error {
	var keys [denseMedianPreflightSlots]uint64
	var counts [denseMedianPreflightSlots]int
	for i, group := range groups {
		if err := addDenseMedianNeed(
			&keys, &counts, group, next.groups[offset+i].Length()); err != nil {
			return err
		}
	}
	return exec.applyDenseMedianNeeds(&keys, &counts)
}

func (exec *medianColumnExecSelf[T, R]) denseBatchMerge(
	next *medianColumnExecSelf[T, R],
	offset int,
	groups []uint64,
) error {
	if !exec.usesDenseAccountedState() || next == nil ||
		!next.usesDenseAccountedState() || offset < 0 ||
		len(groups) > hashmap.UnitLimit || offset > len(next.groups)-len(groups) {
		return mpool.ErrAllocationAccountInvalid
	}
	for i, group := range groups {
		if group == GroupNotMatched || next.groups[offset+i].Length() == 0 {
			continue
		}
		index := group - 1
		if index >= uint64(len(exec.groups)) || exec.groups[index] == nil {
			return mpool.ErrAllocationAccountInvariant
		}
		if err := exec.groups[index].Union(next.groups[offset+i], exec.mp); err != nil {
			return err
		}
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	if exec.accounted != nil {
		if exec.usesDenseAccountedState() {
			groups := []uint64{uint64(groupIndex + 1)}
			if err := exec.preflightDenseBatchFill(row, groups, vectors); err != nil {
				return err
			}
			return exec.denseBatchFill(row, groups, vectors)
		}
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
			if err := exec.PreflightBatchFill(offset, mapping, vectors); err != nil {
				return err
			}
			if err := exec.BatchFill(offset, mapping, vectors); err != nil {
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
		if exec.usesDenseAccountedState() {
			return exec.denseBatchFill(offset, groups, vectors)
		}
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
		if exec.usesDenseAccountedState() || other.usesDenseAccountedState() {
			if !exec.usesDenseAccountedState() || !other.usesDenseAccountedState() {
				return mpool.ErrAllocationAccountMismatch
			}
			groups := []uint64{uint64(groupIdx1 + 1)}
			if err := exec.preflightDenseBatchMerge(other, groupIdx2, groups); err != nil {
				return err
			}
			return exec.denseBatchMerge(other, groupIdx2, groups)
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
		if exec.usesDenseAccountedState() || next.usesDenseAccountedState() {
			if !exec.usesDenseAccountedState() || !next.usesDenseAccountedState() {
				return mpool.ErrAllocationAccountMismatch
			}
			return exec.denseBatchMerge(next, offset, groups)
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
			// Ordinary MEDIAN retains fixed values in append-only accounted
			// vectors. Building an ordered skiplist for every row made a 1B-row
			// median spend nearly all of its time rebuilding and reinserting the
			// saved-argument arena. DISTINCT median and ordered percentile keep
			// the generic saved-argument representation.
			saveArg: exec.AggID() != AggIdOfMedian || exec.IsDistinct(),
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
	if exec.usesDenseAccountedState() {
		return exec.preflightDenseBatchFill(offset, groups, vectors)
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
	if exec.usesDenseAccountedState() || next.usesDenseAccountedState() {
		if !exec.usesDenseAccountedState() || !next.usesDenseAccountedState() {
			return mpool.ErrAllocationAccountMismatch
		}
		return exec.preflightDenseBatchMerge(next, offset, groups)
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
	if exec.usesDenseAccountedState() {
		return exec.saveDenseMedianSpillRows(chunk, rows, writer)
	}
	return exec.accounted.SaveSpillIntermediateRows(chunk, rows, writer)
}

func (exec *medianColumnExecSelf[T, R]) UnmarshalSpillFromReader(
	reader io.Reader, mp *mpool.MPool,
) error {
	if exec.accounted == nil {
		return moerr.NewNotSupportedNoCtx("median has no bounded spill state")
	}
	if exec.usesDenseAccountedState() {
		return exec.unmarshalDenseMedianSpill(reader, mp)
	}
	return exec.accounted.UnmarshalSpillFromReader(reader, mp)
}

func (exec *medianColumnExecSelf[T, R]) saveDenseMedianSpillRows(
	chunk int,
	rows []int32,
	writer io.Writer,
) error {
	if !exec.usesDenseAccountedState() || writer == nil || len(rows) == 0 ||
		len(rows) > AggBatchSize || chunk < 0 || chunk >= len(exec.accounted.state) {
		return moerr.NewInvalidInputNoCtx("invalid median spill selection")
	}
	state := &exec.accounted.state[chunk]
	groupBase := 0
	for i := 0; i < chunk; i++ {
		groupBase += int(exec.accounted.state[i].length)
	}
	for _, row := range rows {
		if row < 0 || row >= state.length || groupBase+int(row) >= len(exec.groups) {
			return moerr.NewInvalidInputNoCtx("invalid median spill row")
		}
	}
	if err := types.WriteUint64(writer, spillMagicNumber); err != nil {
		return err
	}
	if err := types.WriteInt32(writer, int32(len(rows))); err != nil {
		return err
	}
	for _, row := range rows {
		group := exec.groups[groupBase+int(row)]
		if err := types.WriteUint64(writer, uint64(group.Length())); err != nil {
			return err
		}
		for _, vec := range group.vecs {
			if _, err := writeBytesRaw(vec.UnsafeGetRawData(), writer); err != nil {
				return err
			}
		}
	}
	return types.WriteUint64(writer, spillMagicNumber)
}

func (exec *medianColumnExecSelf[T, R]) unmarshalDenseMedianSpill(
	reader io.Reader,
	mp *mpool.MPool,
) error {
	if !exec.usesDenseAccountedState() || reader == nil || mp == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	magic, err := types.ReadUint64(reader)
	if err != nil {
		return err
	}
	if magic != spillMagicNumber {
		return moerr.NewInvalidInputNoCtx("invalid median spill header")
	}
	count, err := types.ReadInt32(reader)
	if err != nil {
		return err
	}
	if count <= 0 || count > AggBatchSize {
		return moerr.NewInvalidInputNoCtx("invalid median spill group count")
	}

	replacement := *exec.accounted
	replacement.state = nil
	replacement.standby = nil
	replacementGroups := make([]*Vectors[T], int(count))
	committed := false
	defer func() {
		if committed {
			return
		}
		replacement.Free()
		for _, group := range replacementGroups {
			if group != nil {
				group.Free(mp)
			}
		}
	}()
	if err = replacement.PreAllocateGroups(int(count)); err != nil {
		return err
	}
	if err = replacement.GroupGrow(int(count)); err != nil {
		return err
	}
	for groupIndex := range replacementGroups {
		group, err := newAccountedVectors[T](exec.argType, replacement.allocation)
		if err != nil {
			return err
		}
		replacementGroups[groupIndex] = group
		rows, err := types.ReadUint64(reader)
		if err != nil {
			return err
		}
		if rows > uint64(math.MaxInt) {
			return mpool.ErrAllocationAllocatorLimit
		}
		remaining := int(rows)
		for remaining > 0 {
			rowsInVector := min(remaining, MaxVectorLength)
			vec, err := group.getAppendableVector()
			if err != nil {
				return err
			}
			if err = vec.PreExtend(rowsInVector, mp); err != nil {
				return err
			}
			vec.SetLength(rowsInVector)
			if _, err = io.ReadFull(reader, vec.UnsafeGetRawData()); err != nil {
				return err
			}
			remaining -= rowsInVector
		}
	}
	magic, err = types.ReadUint64(reader)
	if err != nil {
		return err
	}
	if magic != spillMagicNumber {
		return moerr.NewInvalidInputNoCtx("invalid median spill trailer")
	}

	exec.accounted.Free()
	exec.freeGroups()
	exec.accounted.state = replacement.state
	exec.accounted.standby = replacement.standby
	exec.groups = replacementGroups
	replacement.state = nil
	replacement.standby = nil
	replacementGroups = nil
	committed = true
	return nil
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
		if exec.usesDenseAccountedState() {
			for _, group := range exec.groups {
				if group != nil {
					size += group.Size()
				}
			}
			return size + int64(cap(exec.groups))*8
		}
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
	groupBase := 0
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
			if exec.usesDenseAccountedState() {
				group := exec.groups[groupBase+int(row)]
				if group.Length() == 0 {
					result.SetNull(uint64(row))
					continue
				}
				values[row], err = denseMedianNumeric(
					group, exec.accounted.allocation, exec.mp)
				if err != nil {
					return nil, err
				}
				continue
			}
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
		groupBase += int(state.length)
	}
	return results, nil
}

func flushAccountedMedianDecimal[T types.Decimal64 | types.Decimal128](
	exec *medianColumnDecimalExec[T],
) (_ []*vector.Vector, retErr error) {
	results := make([]*vector.Vector, len(exec.accounted.state))
	defer freeAggregateResultsOnError(exec.mp, results, &retErr)
	groupBase := 0
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
			if exec.usesDenseAccountedState() {
				group := exec.groups[groupBase+int(row)]
				if group.Length() == 0 {
					result.SetNull(uint64(row))
					continue
				}
				switch typed := any(group).(type) {
				case *Vectors[types.Decimal64]:
					values[row], err = denseMedianDecimal64(
						typed, exec.accounted.allocation, exec.mp)
				case *Vectors[types.Decimal128]:
					values[row], err = denseMedianDecimal128(
						typed, exec.accounted.allocation, exec.mp)
				default:
					err = moerr.NewInternalErrorNoCtx("median decimal type mismatch")
				}
				if err != nil {
					return nil, err
				}
				continue
			}
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
		groupBase += int(state.length)
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
	vec, err := vecs.getAppendableVector()
	if err != nil {
		return err
	}
	return vector.AppendFixed(vec, value, false, mp)
}

func appendMedianValues[T numeric | types.Decimal64 | types.Decimal128](
	vecs *Vectors[T], values []T, mp *mpool.MPool,
) error {
	for len(values) != 0 {
		vec, err := vecs.getAppendableVector()
		if err != nil {
			return err
		}
		count := min(len(values), MaxVectorLength-vec.Length())
		if err = vector.AppendFixedList(vec, values[:count], nil, mp); err != nil {
			return err
		}
		values = values[count:]
	}
	return nil
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

func denseMedianNumeric[T numeric](
	values *Vectors[T], allocation *AllocationAccount, mp *mpool.MPool,
) (float64, error) {
	if len(values.vecs) == 1 {
		return medianNumericVals(
			vector.MustFixedColNoTypeCheck[T](values.vecs[0])), nil
	}
	selector, err := newDenseMedianSelector(values, allocation, mp)
	if err != nil {
		return 0, err
	}
	defer selector.free()
	rows := values.Length()
	compare := medianNumericComparator[T]()
	if rows&1 == 1 {
		return float64(selector.selectKth(rows>>1, compare)), nil
	}
	v1 := selector.selectKth(rows>>1-1, compare)
	v2 := selector.selectKth(rows>>1, compare)
	return (float64(v1) + float64(v2)) / 2, nil
}

func denseMedianDecimal64(
	values *Vectors[types.Decimal64],
	allocation *AllocationAccount,
	mp *mpool.MPool,
) (types.Decimal128, error) {
	if len(values.vecs) == 1 {
		return medianDecimal64Vals(
			vector.MustFixedColNoTypeCheck[types.Decimal64](values.vecs[0]))
	}
	selector, err := newDenseMedianSelector(values, allocation, mp)
	if err != nil {
		return types.Decimal128{}, err
	}
	defer selector.free()
	rows := values.Length()
	compare := func(a, b types.Decimal64) int { return a.Compare(b) }
	if rows&1 == 1 {
		return FromD64ToD128(
			selector.selectKth(rows>>1, compare)).Scale(1)
	}
	v1 := FromD64ToD128(
		selector.selectKth(rows>>1-1, compare))
	v2 := FromD64ToD128(
		selector.selectKth(rows>>1, compare))
	return averageMedianDecimal(v1, v2)
}

func denseMedianDecimal128(
	values *Vectors[types.Decimal128],
	allocation *AllocationAccount,
	mp *mpool.MPool,
) (types.Decimal128, error) {
	if len(values.vecs) == 1 {
		return medianDecimal128Vals(
			vector.MustFixedColNoTypeCheck[types.Decimal128](values.vecs[0]))
	}
	selector, err := newDenseMedianSelector(values, allocation, mp)
	if err != nil {
		return types.Decimal128{}, err
	}
	defer selector.free()
	rows := values.Length()
	compare := func(a, b types.Decimal128) int { return a.Compare(b) }
	if rows&1 == 1 {
		return selector.selectKth(rows>>1, compare).Scale(1)
	}
	v1 := selector.selectKth(rows>>1-1, compare)
	v2 := selector.selectKth(rows>>1, compare)
	return averageMedianDecimal(v1, v2)
}

func averageMedianDecimal(
	v1, v2 types.Decimal128,
) (types.Decimal128, error) {
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

const denseMedianRangeWidth = 4

type denseMedianSelector[T numeric | types.Decimal64 | types.Decimal128] struct {
	values *Vectors[T]
	bounds []int
	mp     *mpool.MPool
}

func newDenseMedianSelector[T numeric | types.Decimal64 | types.Decimal128](
	values *Vectors[T],
	allocation *AllocationAccount,
	mp *mpool.MPool,
) (denseMedianSelector[T], error) {
	bounds, err := makeAccountedScratch[int](
		allocation, mp, denseMedianRangeWidth*len(values.vecs))
	if err != nil {
		return denseMedianSelector[T]{}, err
	}
	return denseMedianSelector[T]{
		values: values,
		bounds: bounds,
		mp:     mp,
	}, nil
}

func (s *denseMedianSelector[T]) free() {
	mpool.FreeSlice(s.mp, s.bounds)
	s.bounds = nil
}

func (s *denseMedianSelector[T]) reset() int {
	rows := 0
	for i, vec := range s.values.vecs {
		base := i * denseMedianRangeWidth
		length := vec.Length()
		s.bounds[base] = 0
		s.bounds[base+1] = length
		rows += length
	}
	return rows
}

func (s *denseMedianSelector[T]) selectKth(
	k int,
	compare func(a, b T) int,
) T {
	candidates := s.reset()
	for {
		pivotOffset := candidates >> 1
		var pivot T
		for i, vec := range s.values.vecs {
			base := i * denseMedianRangeWidth
			rows := s.bounds[base+1] - s.bounds[base]
			if pivotOffset < rows {
				values := vector.MustFixedColNoTypeCheck[T](vec)
				pivot = values[s.bounds[base]+pivotOffset]
				break
			}
			pivotOffset -= rows
		}

		less, equal := 0, 0
		for i, vec := range s.values.vecs {
			base := i * denseMedianRangeWidth
			lo, hi := s.bounds[base], s.bounds[base+1]
			values := vector.MustFixedColNoTypeCheck[T](vec)
			lt, gt := partitionDenseSegment(
				values, lo, hi, pivot, compare)
			s.bounds[base+2] = lt
			s.bounds[base+3] = gt
			less += lt - lo
			equal += gt - lt
		}
		switch {
		case k < less:
			candidates = less
			for i := range s.values.vecs {
				base := i * denseMedianRangeWidth
				s.bounds[base+1] = s.bounds[base+2]
			}
		case k < less+equal:
			return pivot
		default:
			k -= less + equal
			candidates -= less + equal
			for i := range s.values.vecs {
				base := i * denseMedianRangeWidth
				s.bounds[base] = s.bounds[base+3]
			}
		}
	}
}

func partitionDenseSegment[T numeric | types.Decimal64 | types.Decimal128](
	values []T,
	left, right int,
	pivot T,
	compare func(a, b T) int,
) (int, int) {
	lt, i, gt := left, left, right
	for i < gt {
		switch comparison := compare(values[i], pivot); {
		case comparison < 0:
			values[lt], values[i] = values[i], values[lt]
			lt++
			i++
		case comparison > 0:
			gt--
			values[i], values[gt] = values[gt], values[i]
		default:
			i++
		}
	}
	return lt, gt
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

func medianNumericComparator[T numeric]() func(a, b T) int {
	var zero T
	switch any(zero).(type) {
	case float32:
		return func(a, b T) int {
			return types.Float32OrderAscCompare(
				any(a).(float32), any(b).(float32))
		}
	case float64:
		return func(a, b T) int {
			return types.Float64OrderAscCompare(
				any(a).(float64), any(b).(float64))
		}
	default:
		return func(a, b T) int {
			if a < b {
				return -1
			}
			if a > b {
				return 1
			}
			return 0
		}
	}
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
	return averageMedianDecimal(v1, v2)
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
	return averageMedianDecimal(v1, v2)
}

func selectKthNumeric[T numeric](vals []T, k int) T {
	return selectKthFunc(vals, k, medianNumericComparator[T]())
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
