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
	"fmt"
	io "io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	AggBatchSize = 8192
)

type aggInfo struct {
	aggId      int64
	isDistinct bool
	argTypes   []types.Type
	retType    types.Type
	emptyNull  bool
	usePtrLen  bool
	entrySize  int
}

func (a *aggInfo) String() string {
	return fmt.Sprintf("aggId: %d, isDistinct: %t, argTypes: %v, retType: %v, emptyNull: %t", a.aggId, a.isDistinct, a.argTypes, a.retType, a.emptyNull)
}

func (a *aggInfo) AggID() int64 {
	return a.aggId
}

func (a *aggInfo) IsDistinct() bool {
	return a.isDistinct
}

func (a *aggInfo) TypesInfo() ([]types.Type, types.Type) {
	return a.argTypes, a.retType
}

type aggState[S any] struct {
	length   int64
	capacity int64
	states   []byte
}

func (ag *aggState[S]) init(mp *mpool.MPool, usePtrLen bool, sz int, n int64) error {
	var err error
	if sz > 0 && !usePtrLen {
		ag.states, err = mp.Alloc(sz*int(n), true)
	} else {
		ag.states, err = mp.Alloc(int(n)*mpool.PtrLenSize, true)
	}
	if err != nil {
		return err
	}

	ag.length = 0
	ag.capacity = int64(n)
	return nil
}

func (ag *aggState[S]) grow(mp *mpool.MPool, usePtrLen bool, more int64) int64 {
	canAdd := ag.capacity - ag.length
	if more <= canAdd {
		ag.length += more
		return 0
	} else {
		ag.length = ag.capacity
		return more - canAdd
	}
}

func (ag *aggState[S]) writeStateToBuf(idx, sz int, usePtrLen bool, buf *bytes.Buffer) error {
	if sz > 0 && !usePtrLen {
		buf.Write(ag.states[idx*sz : (idx+1)*sz])
		return nil
	}
	ptr := ag.getPtrLen(idx)
	return types.WriteSizeBytes(ptr.ToByteSlice(), buf)
}

func (ag *aggState[S]) writeAllStatesToBuf(sz int, usePtrLen bool, buf *bytes.Buffer) error {
	if sz > 0 && !usePtrLen {
		buf.Write(ag.states)
		return nil
	}

	ptrs := ag.getPtrLenSlice()
	for _, ptr := range ptrs {
		if err := types.WriteSizeBytes(ptr.ToByteSlice(), buf); err != nil {
			return err
		}
	}
	return nil
}

func (ag *aggState[S]) readState(mp *mpool.MPool,
	cnt int64, offset int64, sz int, usePtrLen bool, reader io.Reader) (int64, error) {
	rn := ag.length - offset
	if rn >= cnt {
		rn = cnt
	}

	if sz > 0 && !usePtrLen {
		bs := ag.states[int(offset)*sz : int(offset+rn)*sz]
		_, err := io.ReadFull(reader, bs)
		if err != nil {
			return 0, err
		}
	} else {
		ptrs := ag.getPtrLenSlice()
		for i := 0; i < int(rn); i++ {
			_, bs, err := types.ReadSizeBytes(reader)
			if err != nil {
				return 0, err
			}
			ptrs[offset+int64(i)].AppendBytes(mp, bs)
		}
	}
	return rn, nil
}

func (ag *aggState[S]) getStateSlice(sz int) []S {
	return util.UnsafeSliceCast[S](ag.states)
}

func (ag *aggState[S]) getState(idx, sz int) *S {
	return &ag.getStateSlice(sz)[idx]
}

func (ag *aggState[S]) getPtrLenSlice() []mpool.PtrLen {
	return util.UnsafeSliceCast[mpool.PtrLen](ag.states)
}

func (ag *aggState[S]) getPtrLen(idx int) *mpool.PtrLen {
	return &ag.getPtrLenSlice()[idx]
}

func (ag *aggState[S]) free(mp *mpool.MPool, usePtrLen bool, sz int) {
	if sz < 0 || usePtrLen {
		pls := ag.getPtrLenSlice()
		for _, pl := range pls {
			mp.Free(pl.ToByteSlice())
		}
	}
	mp.Free(ag.states)
	ag.states = nil
	ag.length = 0
	ag.capacity = 0
}

type aggExec[S any] struct {
	mp *mpool.MPool
	aggInfo
	chunkSize int
	state     []aggState[S]
}

func (ae *aggExec[S]) marshal() ([]byte, error) {
	panic("not implemented")
}

func (ae *aggExec[S]) unmarshal(mp *mpool.MPool, result, empties, groups [][]byte) error {
	panic("not implemented")
}

func (ae *aggExec[S]) getChunkSize() int {
	return ae.chunkSize
}

func (ae *aggExec[S]) modifyChunkSize(n int) {
	if n != 1 && n != AggBatchSize {
		panic(moerr.NewInternalErrorNoCtxf("invalid chunk size: %d", n))
	}
	ae.chunkSize = n
}

func (ae *aggExec[S]) GetOptResult() SplitResult {
	return ae
}

func (ae *aggExec[S]) GetXY(u uint64) (int, int) {
	x := u / AggBatchSize
	y := u % AggBatchSize
	return int(x), int(y)
}

func (ae *aggExec[S]) NextXY(x, y int) (int, int) {
	if y == int(AggBatchSize)-1 {
		return x + 1, 0
	}
	return x, y + 1
}

func (ae *aggExec[S]) GetState(idx uint64) *S {
	x, y := ae.GetXY(idx)
	return ae.state[x].getState(y, ae.aggInfo.entrySize)
}

func (ae *aggExec[S]) GetPtrLen(idx uint64) *mpool.PtrLen {
	x, y := ae.GetXY(idx)
	return ae.state[x].getPtrLen(y)
}

func (ae *aggExec[S]) GroupGrow(more int) error {
	// special case for calling first time with more = 1
	// this is basically used for no group by case.
	if more == 1 && len(ae.state) == 0 {
		ae.state = make([]aggState[S], 1)
		if err := ae.state[0].init(ae.mp, ae.aggInfo.usePtrLen, int(ae.aggInfo.entrySize), 1); err != nil {
			return err
		}
		ae.state[0].grow(ae.mp, ae.aggInfo.usePtrLen, 1)
		return nil
	}

	m64 := int64(more)
	// grow the state until the more groups are added
	for remain := m64; remain > 0; {
		if len(ae.state) != 0 {
			remain = ae.state[len(ae.state)-1].grow(ae.mp, ae.aggInfo.usePtrLen, remain)
		}

		if remain == 0 {
			return nil
		}
		ae.state = append(ae.state, aggState[S]{})
		if err := ae.state[len(ae.state)-1].init(
			ae.mp, ae.aggInfo.usePtrLen, ae.aggInfo.entrySize, AggBatchSize); err != nil {
			return err
		}
	}
	return nil
}

func (ae *aggExec[S]) PreAllocateGroups(more int) error {
	// there is no need to preallocate groups for aggExec
	return nil
}

// Fill, BulkFill, BatchFill, and Flush are implemented by each agg function.
// SetExtraInformation also implemented by each agg.

func (ae *aggExec[S]) SaveIntermediateResult(cnt int64, flags [][]uint8, buf *bytes.Buffer) error {
	// write the number of groups
	types.WriteInt32(buf, int32(ae.aggInfo.entrySize))
	types.WriteInt64(buf, cnt)

	if cnt == 0 {
		return nil
	}

	for i := range flags {
		st := &ae.state[i]
		for j := range flags[i] {
			if flags[i][j] == 1 {
				if err := st.writeStateToBuf(j, ae.aggInfo.entrySize, ae.aggInfo.usePtrLen, buf); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ae *aggExec[S]) SaveIntermediateResultOfChunk(chunk int, buf *bytes.Buffer) error {
	if chunk >= len(ae.state) {
		return moerr.NewInternalErrorNoCtx("chunk index out of range")
	}
	st := &ae.state[chunk]
	types.WriteInt32(buf, int32(ae.aggInfo.entrySize))
	types.WriteInt64(buf, int64(st.length))

	if st.length == 0 {
		return nil
	}

	if err := st.writeAllStatesToBuf(ae.aggInfo.entrySize, ae.aggInfo.usePtrLen, buf); err != nil {
		return err
	}
	return nil
}

func (ae *aggExec[S]) UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) error {
	sz32, err := types.ReadInt32(reader)
	if err != nil {
		return err
	}
	if int(sz32) != ae.aggInfo.entrySize {
		return moerr.NewInternalErrorNoCtxf("entry size mismatch: %d != %d", sz32, ae.aggInfo.entrySize)
	}

	cnt, err := types.ReadInt64(reader)
	if err != nil {
		return err
	}

	if cnt == 0 {
		return nil
	}

	if ae.chunkSize == 1 {
		// this is no group by case, in this case, caller has already called GroupGrow(1)
		if cnt != 1 {
			return moerr.NewInternalErrorNoCtxf("invalid count: %d", cnt)
		}
		if len(ae.state) != 1 || ae.state[0].length != 1 {
			return moerr.NewInternalErrorNoCtx("invalid state, single group case but state is not initialized")
		}
		rc, err := ae.state[0].readState(mp, cnt, 0,
			ae.aggInfo.entrySize, ae.aggInfo.usePtrLen, reader)
		if err != nil {
			return err
		}
		if rc != 1 {
			return moerr.NewInternalErrorNoCtxf("invalid read count: %d", rc)
		}
		return nil
	}

	// compute oldX and oldY before GroupGrow
	oldX := len(ae.state) - 1
	oldY := int64(0)
	if oldX < 0 {
		oldX = 0
	} else {
		oldY = ae.state[oldX].length
	}

	if oldY == AggBatchSize {
		oldX += 1
		oldY = 0
	}

	ae.GroupGrow(int(cnt))

	for cnt > 0 {
		rc, err := ae.state[oldX].readState(mp, cnt, oldY,
			ae.aggInfo.entrySize, ae.aggInfo.usePtrLen, reader)
		if err != nil {
			return err
		}
		cnt -= rc
		oldX += 1
		oldY = 0
	}
	return nil
}

func (ae *aggExec[S]) Size() int64 {
	panic("not implemented")
}

func (ae *aggExec[S]) Free() {
	for _, state := range ae.state {
		state.free(ae.mp, ae.aggInfo.usePtrLen, int(ae.aggInfo.entrySize))
	}
}

func (ae *aggExec[S]) batchFillArgs(offset int, groups []uint64, vectors []*vector.Vector, distinct bool) error {
	if len(vectors) != 1 {
		return moerr.NewInternalErrorNoCtx("batchFillArgs: only one vector is supported")
	}

	switch vectors[0].GetType().Oid {
	case types.T_bool:
		return batchFillFixed[bool](ae, offset, groups, vectors, distinct)
	case types.T_bit:
		return batchFillFixed[uint64](ae, offset, groups, vectors, distinct)
	case types.T_int8:
		return batchFillFixed[int8](ae, offset, groups, vectors, distinct)
	case types.T_int16:
		return batchFillFixed[int16](ae, offset, groups, vectors, distinct)
	case types.T_int32:
		return batchFillFixed[int32](ae, offset, groups, vectors, distinct)
	case types.T_int64:
		return batchFillFixed[int64](ae, offset, groups, vectors, distinct)
	case types.T_uint8:
		return batchFillFixed[uint8](ae, offset, groups, vectors, distinct)
	case types.T_uint16:
		return batchFillFixed[uint16](ae, offset, groups, vectors, distinct)
	case types.T_uint32:
		return batchFillFixed[uint32](ae, offset, groups, vectors, distinct)
	case types.T_uint64:
		return batchFillFixed[uint64](ae, offset, groups, vectors, distinct)
	case types.T_float32:
		return batchFillFixed[float32](ae, offset, groups, vectors, distinct)
	case types.T_float64:
		return batchFillFixed[float64](ae, offset, groups, vectors, distinct)
	case types.T_decimal64:
		return batchFillFixed[types.Decimal64](ae, offset, groups, vectors, distinct)
	case types.T_decimal128:
		return batchFillFixed[types.Decimal128](ae, offset, groups, vectors, distinct)
	case types.T_decimal256:
		return batchFillFixed[types.Decimal256](ae, offset, groups, vectors, distinct)
	case types.T_date:
		return batchFillFixed[types.Date](ae, offset, groups, vectors, distinct)
	case types.T_time:
		return batchFillFixed[types.Time](ae, offset, groups, vectors, distinct)
	case types.T_datetime:
		return batchFillFixed[types.Datetime](ae, offset, groups, vectors, distinct)
	case types.T_timestamp:
		return batchFillFixed[types.Timestamp](ae, offset, groups, vectors, distinct)
	case types.T_interval:
		return batchFillFixed[int64](ae, offset, groups, vectors, distinct)
	case types.T_uuid:
		return batchFillFixed[types.Uuid](ae, offset, groups, vectors, distinct)
	case types.T_TS:
		return batchFillFixed[types.TS](ae, offset, groups, vectors, distinct)
	case types.T_Rowid:
		return batchFillFixed[types.Rowid](ae, offset, groups, vectors, distinct)
	case types.T_Blockid:
		return batchFillFixed[types.Blockid](ae, offset, groups, vectors, distinct)
	case types.T_Objectid:
		return batchFillFixed[types.Objectid](ae, offset, groups, vectors, distinct)
	case types.T_enum:
		return batchFillFixed[types.Enum](ae, offset, groups, vectors, distinct)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_tuple, types.T_array_float32, types.T_array_float64,
		types.T_datalink:
		return batchFillVarLen(ae, offset, groups, vectors, distinct)
	default:
		return moerr.NewInternalErrorNoCtxf("batchFillArgs: type %d is not supported", vectors[0].GetType().Oid)
	}
}

func batchFillFixed[T comparable, S any](ae *aggExec[S], offset int, groups []uint64, vectors []*vector.Vector, distinct bool) error {
	var ts []T
	// No type check!  Our T is not necessarily a compatible type for the vector.
	// We only care T size.
	vector.ToSliceNoTypeCheck(vectors[0], &ts)
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}

		idx := uint64(i) + uint64(offset)
		if vectors[0].IsNull(idx) {
			continue
		} else {
			ptr := ae.GetPtrLen(group - 1)
			if distinct {
				if mpool.PtrLenFindFixed(ptr, ts[offset+i]) != -1 {
					continue
				}
			}
			mpool.AppendFixed(ae.mp, ptr, ts[offset+i])
		}
	}
	return nil
}

func batchFillVarLen[S any](ae *aggExec[S], offset int, groups []uint64, vectors []*vector.Vector, distinct bool) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}

		idx := uint64(i) + uint64(offset)
		if vectors[0].IsNull(idx) {
			continue
		}
		ptr := ae.GetPtrLen(group - 1)
		bs := vectors[0].GetBytesAt(int(offset + i))
		if distinct {
			if ptr.FindBytes(bs) != -1 {
				continue
			}
		}
		ptr.AppendBytes(ae.mp, bs)
	}
	return nil
}

func (ae *aggExec[S]) batchMergeArgs(next *aggExec[S], offset int, groups []uint64, distinct bool) error {
	// switch on oid
	switch ae.aggInfo.argTypes[0].Oid {
	case types.T_bool:
		return batchMergeFixed[bool](ae, next, offset, groups, distinct)
	case types.T_bit:
		return batchMergeFixed[uint64](ae, next, offset, groups, distinct)
	case types.T_int8:
		return batchMergeFixed[int8](ae, next, offset, groups, distinct)
	case types.T_int16:
		return batchMergeFixed[int16](ae, next, offset, groups, distinct)
	case types.T_int32:
		return batchMergeFixed[int32](ae, next, offset, groups, distinct)
	case types.T_int64:
		return batchMergeFixed[int64](ae, next, offset, groups, distinct)
	case types.T_uint8:
		return batchMergeFixed[uint8](ae, next, offset, groups, distinct)
	case types.T_uint16:
		return batchMergeFixed[uint16](ae, next, offset, groups, distinct)
	case types.T_uint32:
		return batchMergeFixed[uint32](ae, next, offset, groups, distinct)
	case types.T_uint64:
		return batchMergeFixed[uint64](ae, next, offset, groups, distinct)
	case types.T_float32:
		return batchMergeFixed[float32](ae, next, offset, groups, distinct)
	case types.T_float64:
		return batchMergeFixed[float64](ae, next, offset, groups, distinct)
	case types.T_decimal64:
		return batchMergeFixed[types.Decimal64](ae, next, offset, groups, distinct)
	case types.T_decimal128:
		return batchMergeFixed[types.Decimal128](ae, next, offset, groups, distinct)
	case types.T_decimal256:
		return batchMergeFixed[types.Decimal256](ae, next, offset, groups, distinct)
	case types.T_date:
		return batchMergeFixed[types.Date](ae, next, offset, groups, distinct)
	case types.T_time:
		return batchMergeFixed[types.Time](ae, next, offset, groups, distinct)
	case types.T_datetime:
		return batchMergeFixed[types.Datetime](ae, next, offset, groups, distinct)
	case types.T_timestamp:
		return batchMergeFixed[types.Timestamp](ae, next, offset, groups, distinct)
	case types.T_interval:
		return batchMergeFixed[int64](ae, next, offset, groups, distinct)
	case types.T_uuid:
		return batchMergeFixed[types.Uuid](ae, next, offset, groups, distinct)
	case types.T_TS:
		return batchMergeFixed[types.TS](ae, next, offset, groups, distinct)
	case types.T_Rowid:
		return batchMergeFixed[types.Rowid](ae, next, offset, groups, distinct)
	case types.T_Blockid:
		return batchMergeFixed[types.Blockid](ae, next, offset, groups, distinct)
	case types.T_Objectid:
		return batchMergeFixed[types.Objectid](ae, next, offset, groups, distinct)
	case types.T_enum:
		return batchMergeFixed[types.Enum](ae, next, offset, groups, distinct)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary,
		types.T_tuple, types.T_array_float32, types.T_array_float64,
		types.T_datalink:
		return batchMergeVarLen(ae, next, offset, groups, distinct)
	default:
		return moerr.NewInternalErrorNoCtxf("batchMergeArgs: type %d is not supported", ae.aggInfo.argTypes[0].Oid)
	}
}

func batchMergeFixed[T comparable, S any](ae *aggExec[S], next *aggExec[S], offset int, groups []uint64, distinct bool) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}

		ptr := ae.GetPtrLen(group - 1)
		otherPtr := next.GetPtrLen(uint64(offset + i))
		otherTs := mpool.PtrLenToSlice[T](otherPtr)

		if err := mpool.AppendFixedList(ae.mp, ptr, otherTs, distinct); err != nil {
			return err
		}
	}
	return nil
}

func batchMergeVarLen[S any](ae *aggExec[S], next *aggExec[S], offset int, groups []uint64, distinct bool) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}

		ptr := ae.GetPtrLen(group - 1)
		otherPtr := next.GetPtrLen(uint64(offset + i))
		if err := ptr.AppendBytesList(ae.mp, otherPtr, distinct); err != nil {
			return err
		}
	}
	return nil
}
