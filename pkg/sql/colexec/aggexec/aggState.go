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
	"encoding/binary"
	"fmt"
	io "io"

	"github.com/matrixorigin/matrixone/pkg/common/arenaskl"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	AggBatchSize     = 8192
	kAggArgArenaSize = 512 * 1024
	kAggArgPrefixSz  = 2
	kAggArgOrdinalSz = 4
	magicNumber      = uint64(0xdeadbeefbeefdead)
)

type MarshalerUnmarshaler interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary([]byte) error
}

type aggInfo struct {
	aggId                    int64
	isDistinct               bool
	argTypes                 []types.Type
	retType                  types.Type
	stateTypes               []types.Type
	emptyNull                bool
	saveArg                  bool
	makeMarshalerUnmarshaler func(mp *mpool.MPool) (MarshalerUnmarshaler, error)
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

type aggState struct {
	length   int32
	capacity int32
	// vecs are for agg state.
	vecs []*vector.Vector
	// MarshalerUnmarshaler, for state entries.
	// Note that using this, means we pretty much give up memory management
	// for the state entries.
	mobs []MarshalerUnmarshaler

	// argbuf is buffer to the arena for skiplist
	argCnt []uint32
	argbuf []byte
	argSkl *arenaskl.Skiplist
}

func (ag *aggState) init(mp *mpool.MPool, l, c int32, info *aggInfo, setNulls bool) error {
	if c <= 0 || c > AggBatchSize {
		return moerr.NewInternalErrorNoCtxf("invalid length or capacity: %d, %d", l, c)
	}
	if l != 0 && l != c {
		return moerr.NewInternalErrorNoCtxf("invalid length or capacity: %d, %d", l, c)
	}
	ag.length = l
	ag.capacity = c

	var err error
	if !info.saveArg {
		ag.vecs = make([]*vector.Vector, len(info.stateTypes))
		for i, typ := range info.stateTypes {
			ag.vecs[i] = vector.NewVec(typ)
			if err = ag.vecs[i].PreExtend(int(c), mp); err != nil {
				return err
			}
			if info.emptyNull && setNulls {
				ag.vecs[i].SetAllNulls(int(c))
			}
		}
		if info.makeMarshalerUnmarshaler != nil {
			ag.mobs = make([]MarshalerUnmarshaler, int(c))
		}
	} else {
		if ag.argCnt, err = mpool.MakeSlice[uint32](int(c), mp, true); err != nil {
			return err
		}

		bufsz := kAggArgArenaSize
		if c < 1024 {
			bufsz = 16 * 1024
		}

		if ag.argbuf, err = mp.Alloc(bufsz, true); err != nil {
			return err
		}
		arena := arenaskl.NewArena(ag.argbuf)
		ag.argSkl = arenaskl.NewSkiplist(arena, bytes.Compare)
	}
	return nil
}

func (ag *aggState) grow(mp *mpool.MPool, more int32, expandLen bool) (int32, int32) {
	canAdd := int32(ag.capacity - ag.length)
	var toAdd int32

	if more <= canAdd {
		canAdd = more
		if expandLen {
			ag.length += more
		}
	} else {
		if expandLen {
			ag.length = ag.capacity
		}
		toAdd = more - canAdd
	}

	if expandLen {
		for _, vec := range ag.vecs {
			vec.SetLength(int(ag.length))
		}
	}

	return canAdd, toAdd
}

func (ag *aggState) writeStateArg(i int32, buf *bytes.Buffer, info *aggInfo) error {
	types.WriteUint32(buf, ag.argCnt[i])
	if ag.argCnt[i] != 0 {
		// open iterator and write to buf
		xcnt := 0
		var lkb, ukb [kAggArgPrefixSz]byte
		lk := lkb[:]
		uk := ukb[:]
		binary.BigEndian.PutUint16(lk, uint16(i))
		binary.BigEndian.PutUint16(uk, uint16(i+1))
		it := ag.argSkl.NewIter(lk, uk)
		if info.argTypes[0].IsFixedLen() {
			for ok, k, _ := it.SeekGE(lk); ok; ok, k, _ = it.Next() {
				/*
					checkI := binary.BigEndian.Uint16(k[:kAggArgPrefixSz])
					if checkI != uint16(i) {
						panic(moerr.NewInternalErrorNoCtxf("writeStateArg: mismatch i: %d != %d", checkI, i))
					}
				*/
				if _, err := buf.Write(k[kAggArgPrefixSz:]); err != nil {
					return err
				}
				xcnt++
			}
		} else {
			for ok, k, _ := it.SeekGE(lk); ok; ok, k, _ = it.Next() {
				/*
					checkI := binary.BigEndian.Uint16(k[:kAggArgPrefixSz])
					if checkI != uint16(i) {
						panic(moerr.NewInternalErrorNoCtxf("writeStateArg: mismatch i: %d != %d", checkI, i))
					}
				*/
				if err := types.WriteSizeBytes(k[kAggArgPrefixSz:], buf); err != nil {
					return err
				}
				xcnt++
			}
		}

		if int(ag.argCnt[i]) != xcnt {
			panic(moerr.NewInternalErrorNoCtxf("writeStateArg: mismatch count: %d != %d", xcnt, ag.argCnt[i]))
		}
		it.Close()
	}
	return nil
}

func (ag *aggState) readStateArg(mp *mpool.MPool, i int32, r io.Reader, info *aggInfo) error {
	var err error
	if ag.argCnt[i], err = types.ReadUint32(r); err != nil {
		return err
	}
	if ag.argCnt[i] == 0 {
		return nil
	}
	// read the state arguments
	var kbuf []byte
	if info.argTypes[0].IsFixedLen() {
		fixedLen := info.argTypes[0].GetSize()
		if info.isDistinct {
			kbuf = make([]byte, kAggArgPrefixSz+fixedLen)
		} else {
			kbuf = make([]byte, kAggArgPrefixSz+kAggArgOrdinalSz+fixedLen)
		}
	} else {
		kbuf = make([]byte, kAggArgPrefixSz)
	}

	for ui := uint32(0); ui < ag.argCnt[i]; ui++ {
		if info.argTypes[0].IsFixedLen() {
			binary.BigEndian.PutUint16(kbuf[:kAggArgPrefixSz], uint16(i))
			if _, err = io.ReadFull(r, kbuf[kAggArgPrefixSz:]); err != nil {
				return err
			}
		} else {
			binary.BigEndian.PutUint16(kbuf[:kAggArgPrefixSz], uint16(i))
			_, kbuf, err = types.ReadSizeBytesToBuf(r, kbuf, kAggArgPrefixSz)
			if err != nil {
				return err
			}
		}

		if err = ag.insertArg(mp, kbuf); err != nil {
			return err
		}
	}

	return nil
}

func (ag *aggState) writeStateToBuf(mp *mpool.MPool, info *aggInfo, flags []uint8, buf *bytes.Buffer) error {
	var cnt int32
	for i := range flags {
		if flags[i] != 0 {
			cnt += 1
		}
	}

	if err := types.WriteUint64(buf, magicNumber); err != nil {
		return err
	}
	defer types.WriteUint64(buf, magicNumber)

	types.WriteInt32(buf, cnt)
	if cnt == 0 {
		return nil
	}

	if !info.saveArg {
		for _, vec := range ag.vecs {
			err := func() error {
				bufVec := vector.NewOffHeapVecWithType(*vec.GetType())
				defer bufVec.Free(mp)
				if err := bufVec.UnionBatch(vec, 0, int(cnt), flags, mp); err != nil {
					return err
				}
				if err := bufVec.MarshalBinaryWithBuffer(buf); err != nil {
					return err
				}
				return nil
			}()
			if err != nil {
				return err
			}
		}

		if info.makeMarshalerUnmarshaler != nil {
			for i := range flags {
				if flags[i] != 0 {
					if bs, err := ag.mobs[i].MarshalBinary(); err != nil {
						return err
					} else {
						types.WriteSizeBytes(bs, buf)
					}
				}
			}
		}
	} else {
		if ag.argSkl == nil {
			return moerr.NewInternalErrorNoCtx("argSkl is not initialized")
		}
		for i := range flags {
			if flags[i] != 0 {
				if err := ag.writeStateArg(int32(i), buf, info); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ag *aggState) writeAllStatesToBuf(buf *bytes.Buffer, info *aggInfo) error {
	if err := types.WriteUint64(buf, magicNumber); err != nil {
		return err
	}
	defer types.WriteUint64(buf, magicNumber)

	types.WriteInt32(buf, ag.length)
	if ag.length == 0 {
		return nil
	}

	if !info.saveArg {
		for _, vec := range ag.vecs {
			if err := vec.MarshalBinaryWithBuffer(buf); err != nil {
				return err
			}
		}
		if info.makeMarshalerUnmarshaler != nil {
			for _, entry := range ag.mobs {
				if bs, err := entry.MarshalBinary(); err != nil {
					return err
				} else {
					types.WriteSizeBytes(bs, buf)
				}
			}
		}
	} else {
		if ag.argSkl == nil {
			return moerr.NewInternalErrorNoCtx("argSkl is not initialized")
		}
		for i := range ag.length {
			if err := ag.writeStateArg(int32(i), buf, info); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ag *aggState) readState(mp *mpool.MPool, reader io.Reader, info *aggInfo) (int32, error) {
	checkAggStateMagic(reader)
	defer checkAggStateMagic(reader)

	cnt, err := types.ReadInt32(reader)
	if err != nil {
		return 0, err
	}
	if cnt == 0 {
		return 0, nil
	}

	if cnt > AggBatchSize {
		return 0, moerr.NewInternalErrorNoCtxf("invalid count: %d", cnt)
	}
	if err := ag.init(mp, cnt, cnt, info, false); err != nil {
		return 0, err
	}

	if !info.saveArg {
		for _, vec := range ag.vecs {
			if err := vec.UnmarshalWithReader(reader, mp); err != nil {
				return 0, err
			}
		}
		if info.makeMarshalerUnmarshaler != nil {
			for i := range cnt {
				if ag.mobs[i], err = info.makeMarshalerUnmarshaler(mp); err != nil {
					return 0, err
				} else {
					if sz, bs, err := types.ReadSizeBytes(reader); err != nil {
						return 0, err
					} else {
						if sz > 0 {
							if err := ag.mobs[i].UnmarshalBinary(bs); err != nil {
								return 0, err
							}
						}
					}
				}
			}
		}
	} else {
		for i := range cnt {
			if err := ag.readStateArg(mp, int32(i), reader, info); err != nil {
				return 0, err
			}
		}
	}

	return cnt, nil
}

// appendFromStateArg appends the state from other aggState (starting from offset)
func (ag *aggState) appendFromStateArg(mp *mpool.MPool, otherOffset int32, other *aggState, info *aggInfo) (int32, error) {
	// first decide how many we can append
	space := int32(ag.capacity - ag.length)
	if space == 0 {
		// no space to append, return the original offset and caller will append to the next group.
		return otherOffset, nil
	}

	start := otherOffset
	end := start + space
	if end > other.length {
		end = other.length
	}

	if !info.saveArg {
		for i := range ag.vecs {
			if err := ag.vecs[i].UnionBatch(other.vecs[i], int64(otherOffset), int(end-start), nil, mp); err != nil {
				return 0, err
			}
		}
		if info.makeMarshalerUnmarshaler != nil {
			for i := int32(0); i < end-start; i++ {
				ag.mobs[ag.length+i] = other.mobs[otherOffset+i]
				other.mobs[otherOffset+i] = nil
			}
		}
		ag.length += end - start
	} else {
		for i := start; i < end; i++ {
			ag.argCnt[ag.length] = other.argCnt[i]
			var lkb, ukb [kAggArgPrefixSz]byte
			lk := lkb[:]
			uk := ukb[:]
			binary.BigEndian.PutUint16(lk, uint16(i))
			binary.BigEndian.PutUint16(uk, uint16(i+1))
			it := other.argSkl.NewIter(lk, uk)
			for ok, k, _ := it.SeekGE(lk); ok; ok, k, _ = it.Next() {
				// copy the key to the new bytes buffer
				kcpy := append([]byte(nil), k...)
				binary.BigEndian.PutUint16(kcpy[:kAggArgPrefixSz], uint16(ag.length))
				if err := ag.insertArg(mp, kcpy); err != nil {
					return 0, err
				}
			}
			ag.length++
		}
	}
	return end, nil
}

func (ag *aggState) insertArg(mp *mpool.MPool, kbuf []byte) error {
	if ag.argSkl == nil {
		return moerr.NewInternalErrorNoCtx("argSkl is not initialized")
	}

	if err := ag.argSkl.Add(kbuf, nil); err != arenaskl.ErrArenaFull {
		return err
	}

	// arena is full, we need to grow the arena.
	argBuf, err := mp.Alloc(len(ag.argbuf)+kAggArgArenaSize, true)
	if err != nil {
		return err
	}
	oldArgBuf := ag.argbuf
	ag.argbuf = argBuf
	defer mp.Free(oldArgBuf)

	newArena := arenaskl.NewArena(ag.argbuf)
	newArgSkl := arenaskl.NewSkiplist(newArena, bytes.Compare)
	// move entries to new arena
	// I am pretty sure a realloc then fix a few pointers in skl should work, but
	// let's not do that for now, until the profiling shows this is a bottleneck.
	it := ag.argSkl.NewIter(nil, nil)
	for ok, k, _ := it.First(); ok; ok, k, _ = it.Next() {
		if err := newArgSkl.Add(k, nil); err != nil {
			// the tree is messed up.
			ag.argSkl = nil
			return err
		}
	}
	it.Close()
	ag.argSkl = newArgSkl

	// Now do it again, this time it should succeed and if it errors again (ErrArenaFull, means
	// we added an arg that is longer than kAggArgArenaSize, too bad, cannot handle such a long arg
	// for agg.
	err = ag.argSkl.Add(kbuf, nil)
	return err
}

func (ag *aggState) fillArg(mp *mpool.MPool, y uint16, val []byte, distinct bool) error {
	if distinct {
		k := make([]byte, len(val)+kAggArgPrefixSz)
		binary.BigEndian.PutUint16(k[:kAggArgPrefixSz], y)
		copy(k[kAggArgPrefixSz:], val)
		if err := ag.insertArg(mp, k); err == nil {
			ag.argCnt[y] += 1
			if ag.argCnt[y] == 0 {
				return moerr.NewInternalErrorNoCtx("agg fillArg: too many distinct arguments")
			}
			return nil
		} else if err == arenaskl.ErrRecordExists {
			return nil
		} else {
			return err
		}
	} else {
		k := make([]byte, len(val)+kAggArgPrefixSz+kAggArgOrdinalSz)
		binary.BigEndian.PutUint16(k[:kAggArgPrefixSz], y)
		binary.BigEndian.PutUint32(k[kAggArgPrefixSz:kAggArgPrefixSz+kAggArgOrdinalSz], ag.argCnt[y])
		ag.argCnt[y] += 1
		if ag.argCnt[y] == 0 {
			return moerr.NewInternalErrorNoCtx("agg fillArg: too many arguments")
		}
		copy(k[kAggArgPrefixSz+kAggArgOrdinalSz:], val)
		if err := ag.insertArg(mp, k); err == nil {
			ag.argCnt[ag.length] += 1
			if ag.argCnt[ag.length] == 0 {
				return moerr.NewInternalErrorNoCtx("agg fillArg: too many arguments")
			}
			return nil
		} else {
			return err
		}
	}
}

func (ag *aggState) mergeArgs(mp *mpool.MPool, y uint16, other *aggState, otherY uint16, info *aggInfo) error {
	err := other.iter(otherY, func(k []byte) error {
		kcpy := append([]byte(nil), k...)
		binary.BigEndian.PutUint16(kcpy[:kAggArgPrefixSz], y)
		if !info.isDistinct {
			binary.BigEndian.PutUint32(kcpy[kAggArgPrefixSz:kAggArgPrefixSz+kAggArgOrdinalSz], ag.argCnt[y])
		}
		fnerr := ag.insertArg(mp, kcpy)
		if fnerr == nil {
			ag.argCnt[y] += 1
			if ag.argCnt[y] == 0 {
				return moerr.NewInternalErrorNoCtx("agg mergeArgs: too many arguments")
			}
			return nil
		} else if fnerr == arenaskl.ErrRecordExists {
			if info.isDistinct {
				return nil
			} else {
				panic(moerr.NewInternalErrorNoCtx("agg mergeArgs: duplicate arguments"))
			}
		} else {
			return fnerr
		}
	})
	return err
}

func (ag *aggState) iter(idx uint16, fn func(k []byte) error) error {
	var lkb, ukb [kAggArgPrefixSz]byte
	lk := lkb[:]
	uk := ukb[:]
	binary.BigEndian.PutUint16(lk, idx)
	binary.BigEndian.PutUint16(uk, idx+1)
	it := ag.argSkl.NewIter(lk, uk)
	defer it.Close()
	for ok, k, _ := it.SeekGE(lk); ok; ok, k, _ = it.Next() {
		if err := fn(k); err != nil {
			return err
		}
	}
	return nil
}

func (ag *aggState) free(mp *mpool.MPool) {
	if ag.argSkl != nil {
		mpool.FreeSlice(mp, ag.argCnt)
		ag.argCnt = nil
		mp.Free(ag.argbuf)
		ag.argSkl = nil
	}
	for _, vec := range ag.vecs {
		vec.Free(mp)
	}
	ag.vecs = nil
}

type aggExec struct {
	mp *mpool.MPool
	aggInfo
	chunkSize int
	state     []aggState
}

func (ae *aggExec) marshal() ([]byte, error) {
	panic("not implemented")
}

func (ae *aggExec) unmarshal(mp *mpool.MPool, result, empties, groups [][]byte) error {
	panic("not implemented")
}

func (ae *aggExec) getChunkSize() int {
	return ae.chunkSize
}

func (ae *aggExec) modifyChunkSize(n int) {
	if n != 1 && n != AggBatchSize {
		panic(moerr.NewInternalErrorNoCtxf("invalid chunk size: %d", n))
	}
	ae.chunkSize = n
}

func (ae *aggExec) GetOptResult() SplitResult {
	return ae
}

func (ae *aggExec) getXY(u uint64) (int, uint16) {
	x := u / AggBatchSize
	y := u % AggBatchSize
	return int(x), uint16(y)
}

func (ae *aggExec) GetNumChunks() int {
	return len(ae.state)
}

func (ae *aggExec) GetNumGroups() int {
	num := 0
	for _, state := range ae.state {
		num += int(state.length)
	}
	return num
}

func (ae *aggExec) GroupGrow(more int) error {
	if ae.chunkSize == 1 {
		// special grow 1
		ae.state = make([]aggState, 1)
		if err := ae.state[0].init(ae.mp, 1, 1, &ae.aggInfo, true); err != nil {
			panic(err)
		}

		ae.state[0].grow(ae.mp, 1, true)
		return nil
	}

	// grow the state until the more groups are added
	for remain := int32(more); remain > 0; {
		if len(ae.state) != 0 {
			_, remain = ae.state[len(ae.state)-1].grow(ae.mp, remain, true)
		}

		if remain == 0 {
			return nil
		}
		ae.state = append(ae.state, aggState{})
		if err := ae.state[len(ae.state)-1].init(ae.mp, 0, AggBatchSize, &ae.aggInfo, true); err != nil {
			return err
		}
	}
	return nil
}

func (ae *aggExec) preAllocateGroupsWithNulls(more int, setNulls bool) error {
	if more < 0 {
		return moerr.NewInternalErrorNoCtxf("invalid more: %d", more)
	}

	// grow the state until the more groups are added
	for remain := int32(more); remain > 0; {
		if len(ae.state) != 0 {
			_, remain = ae.state[len(ae.state)-1].grow(ae.mp, remain, false)
		}

		if remain == 0 {
			return nil
		}
		ae.state = append(ae.state, aggState{})
		if err := ae.state[len(ae.state)-1].init(ae.mp, 0, AggBatchSize, &ae.aggInfo, setNulls); err != nil {
			return err
		}
	}
	return nil
}

func (ae *aggExec) PreAllocateGroups(more int) error {
	return ae.preAllocateGroupsWithNulls(more, true)
}

// Fill, BulkFill, BatchFill, and Flush are implemented by each agg function.
// SetExtraInformation also implemented by each agg.

func (ae *aggExec) SaveIntermediateResult(cnt int64, flags [][]uint8, buf *bytes.Buffer) error {
	magic := magicNumber
	if err := types.WriteUint64(buf, magic); err != nil {
		return err
	}

	// write the number of chunks
	types.WriteInt32(buf, int32(len(flags)))
	for i := range flags {
		if err := ae.state[i].writeStateToBuf(ae.mp, &ae.aggInfo, flags[i], buf); err != nil {
			return err
		}
	}

	if err := types.WriteUint64(buf, magic); err != nil {
		return err
	}
	return nil
}

func (ae *aggExec) SaveIntermediateResultOfChunk(chunk int, buf *bytes.Buffer) error {
	magic := magicNumber
	if err := types.WriteUint64(buf, magic); err != nil {
		return err
	}

	if chunk >= len(ae.state) {
		return moerr.NewInternalErrorNoCtx("chunk index out of range")
	}

	types.WriteInt32(buf, int32(1))
	if err := ae.state[chunk].writeAllStatesToBuf(buf, &ae.aggInfo); err != nil {
		return err
	}

	if err := types.WriteUint64(buf, magic); err != nil {
		return err
	}

	return nil
}

func checkAggStateMagic(reader io.Reader) {
	magic, err := types.ReadUint64(reader)
	if err != nil || magic != magicNumber {
		panic(moerr.NewInternalErrorNoCtxf("invalid magic number, got %d, %v", magic, err))
	}
}

func (ae *aggExec) UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) error {
	checkAggStateMagic(reader)
	defer checkAggStateMagic(reader)

	// Always unmarshal from a clean state.
	ae.Free()

	// read number of chunks
	cnt, err := types.ReadInt32(reader)
	if err != nil {
		return err
	}

	// nothing to read
	if cnt == 0 {
		return nil
	} else if cnt == 1 {
		// easy case, just one chunk and we read it directly.
		ae.state = make([]aggState, 1)
		if _, err := ae.state[0].readState(mp, reader, &ae.aggInfo); err != nil {
			return err
		}
		return nil
	}

	// multi chunks to read, in this case, we will read each chunk and merge them
	// into fully packed chunks.
	for range cnt {
		err = func() error {
			var st aggState
			defer st.free(mp)
			if _, err := st.readState(mp, reader, &ae.aggInfo); err != nil {
				return err
			}
			if st.length == 0 {
				return nil
			}

			oldX := max(0, len(ae.state)-1)
			ae.preAllocateGroupsWithNulls(int(st.length), false)
			offset, err := ae.state[oldX].appendFromStateArg(mp, 0, &st, &ae.aggInfo)
			if err != nil {
				return err
			}
			if offset < st.length {
				oldX += 1
				offset, err = ae.state[oldX].appendFromStateArg(mp, offset, &st, &ae.aggInfo)
				// we should not have any remaining
				if err != nil || offset != st.length {
					return moerr.NewInternalErrorNoCtxf("invalid read count: %d", offset)
				}
			}
			return nil
		}()

		if err != nil {
			return err
		}
	}
	return nil
}

func (ae *aggExec) Size() int64 {
	panic("not implemented")
}

func (ae *aggExec) Free() {
	for _, st := range ae.state {
		st.free(ae.mp)
	}
}

func (ae *aggExec) batchFillArgs(offset int, groups []uint64, vectors []*vector.Vector, distinct bool) error {
	if len(vectors) != 1 {
		return moerr.NewInternalErrorNoCtx("batchFillArgs: only one vector is supported")
	}

	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}

		idx := uint64(i) + uint64(offset)
		if vectors[0].IsNull(idx) {
			continue
		} else {
			x, y := ae.getXY(group - 1)
			bs := vectors[0].GetRawBytesAt(int(idx))
			if err := ae.state[x].fillArg(ae.mp, y, bs, distinct); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ae *aggExec) batchMergeArgs(next *aggExec, offset int, groups []uint64, distinct bool) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}

		x, y := ae.getXY(group - 1)
		otherX, otherY := next.getXY(uint64(offset + i))

		err := ae.state[x].mergeArgs(ae.mp, y, &next.state[otherX], otherY, &ae.aggInfo)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ag *aggState) checkArgsSkl() {
	if ag.argSkl == nil {
		return
	}

	it := ag.argSkl.NewIter(nil, nil)
	xcnt := make([]uint32, ag.length)
	for ok, k, _ := it.First(); ok; ok, k, _ = it.Next() {
		y := binary.BigEndian.Uint16(k[:kAggArgPrefixSz])
		if y >= uint16(len(xcnt)) {
			panic(moerr.NewInternalErrorNoCtxf("invalid y: %d", y))
		}
		xcnt[y]++
	}
	it.Close()

	for i, cnt := range xcnt {
		if cnt != ag.argCnt[i] {
			panic(moerr.NewInternalErrorNoCtxf("invalid count: %d for y: %d, expected: %d", cnt, i, ag.argCnt[i]))
		}
	}
}

func (ae *aggExec) checkArgsSkl() {
	for _, state := range ae.state {
		state.checkArgsSkl()
	}
}
