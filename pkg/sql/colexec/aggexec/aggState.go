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
	AggBatchSize      = 8192
	aggBatchSizeShift = 13 // log2(AggBatchSize)
	aggBatchSizeMask  = AggBatchSize - 1
	kAggArgArenaSize  = 512 * 1024
	kAggArgPrefixSz   = 2
	kAggArgOrdinalSz  = 4
	magicNumber       = uint64(0xdeadbeefbeefdead)
)

const aggBinaryStringTrailerMagic uint64 = 0x4147474253545231

var _ [0]struct{} = [AggBatchSize & aggBatchSizeMask]struct{}{}       // mask == size-1
var _ [1]struct{} = [1 << aggBatchSizeShift / AggBatchSize]struct{}{} // shift matches size

type MarshalerUnmarshaler interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary([]byte) error
	UnmarshalFromReader(io.Reader) error
}

type freeableMarshalerUnmarshaler interface {
	Free()
}

type aggInfo struct {
	aggId                    int64
	isDistinct               bool
	argTypes                 []types.Type
	retType                  types.Type
	stateTypes               []types.Type
	emptyNull                bool
	saveArg                  bool
	opaqueArg                bool
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

func (a *aggInfo) usesOpaqueArgEncoding() bool {
	return a.opaqueArg || len(a.argTypes) != 1 || !a.argTypes[0].IsFixedLen()
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
			ag.vecs[i] = vector.NewOffHeapVecWithType(typ)
			if err = ag.vecs[i].PreExtend(int(c), mp); err != nil {
				for j := 0; j <= i; j++ {
					ag.vecs[j].Free(mp)
				}
				ag.vecs = nil
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
			mpool.FreeSlice(mp, ag.argCnt)
			ag.argCnt = nil
			return err
		}
		arena := arenaskl.NewArena(ag.argbuf)
		ag.argSkl = arenaskl.NewSkiplist(arena, bytes.Compare)
	}
	return nil
}

func (ag *aggState) grow(mp *mpool.MPool, more int32, expandLen bool) (int32, int32, error) {
	canAdd := int32(ag.capacity - ag.length)
	var toAdd int32

	if more <= canAdd {
		canAdd = more
	} else {
		toAdd = more - canAdd
	}

	if !expandLen || canAdd == 0 {
		return canAdd, toAdd, nil
	}

	// Reserve every row-parallel allocation before publishing the group count.
	// Successful capacity growth is reusable if a later vector fails, while all
	// logical lengths remain unchanged and the caller receives the OOM.
	for _, vec := range ag.vecs {
		if err := vec.PreExtend(int(canAdd), mp); err != nil {
			return 0, more, err
		}
	}
	ag.length += canAdd
	for _, vec := range ag.vecs {
		vec.SetLength(int(ag.length))
	}

	return canAdd, toAdd, nil
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
		if !info.usesOpaqueArgEncoding() {
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
	if !info.usesOpaqueArgEncoding() {
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
		if !info.usesOpaqueArgEncoding() && info.argTypes[0].IsFixedLen() {
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
					if ag.mobs[i] == nil {
						if err := types.WriteSizeBytes(nil, buf); err != nil {
							return err
						}
					} else {
						if bs, err := ag.mobs[i].MarshalBinary(); err != nil {
							return err
						} else {
							if err := types.WriteSizeBytes(bs, buf); err != nil {
								return err
							}
						}
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
			for _, entry := range ag.mobs[:ag.length] {
				/*
					for gap between groups like:
					group 0 , group 1(gap), group 2.

					group 0 and group 2 have data.
					group 1 does not have data. there is no marshal for group 1.
				*/
				if entry == nil {
					if err := types.WriteSizeBytes(nil, buf); err != nil {
						return err
					}
				} else {
					if bs, err := entry.MarshalBinary(); err != nil {
						return err
					} else {
						if err := types.WriteSizeBytes(bs, buf); err != nil {
							return err
						}
					}
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
	cnt, err := types.ReadInt32(reader)
	if err != nil {
		return 0, err
	}
	if cnt == 0 {
		if !info.saveArg && info.makeMarshalerUnmarshaler == nil {
			ag.length = 0
			for _, vec := range ag.vecs {
				if vec != nil {
					vec.CleanOnlyData()
				}
			}
		} else {
			ag.free(mp)
			ag.length = 0
			ag.capacity = 0
		}
		return 0, nil
	}

	if cnt < 0 || cnt > AggBatchSize {
		return 0, moerr.NewInternalErrorNoCtxf("invalid count: %d", cnt)
	}
	reuseVectors := !info.saveArg && info.makeMarshalerUnmarshaler == nil &&
		len(ag.vecs) == len(info.stateTypes) && ag.capacity >= cnt
	if reuseVectors {
		ag.length = cnt
		for _, vec := range ag.vecs {
			if vec != nil {
				vec.CleanOnlyData()
			}
		}
	} else {
		ag.free(mp)
		if err := ag.init(mp, cnt, cnt, info, false); err != nil {
			return 0, err
		}
	}

	if !info.saveArg {
		for _, vec := range ag.vecs {
			if err := vec.UnmarshalWithReader(reader, mp); err != nil {
				return 0, err
			}
		}
		if info.makeMarshalerUnmarshaler != nil {
			for i := range cnt {
				sz, err := types.ReadInt32(reader)
				if err != nil {
					return 0, err
				}
				if sz > 0 {
					//only need marshal for size > 0.
					if ag.mobs[i], err = info.makeMarshalerUnmarshaler(mp); err != nil {
						return 0, err
					}
					lr := io.LimitReader(reader, int64(sz))
					if err := ag.mobs[i].UnmarshalFromReader(lr); err != nil {
						return 0, err
					}
					if n, _ := io.Copy(io.Discard, lr); n > 0 {
						return 0, moerr.NewInternalErrorNoCtxf("mob unmarshal did not consume all bytes: %d remaining", n)
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

	// arena is full, we need to grow the arena. Grow by at least kAggArgArenaSize,
	// but if a single key (plus its skiplist node overhead) needs more than that —
	// e.g. a multi-column distinct key concatenating several large string args —
	// grow by enough to fit it, otherwise the retry below would still ErrArenaFull.
	grow := int64(kAggArgArenaSize)
	if need := int64(arenaskl.MaxNodeSize(uint32(len(kbuf)), 0)); need > grow {
		grow = need
	}
	argBuf, err := mp.Alloc(len(ag.argbuf)+int(grow), true)
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

func aggPayloadOffset(info *aggInfo) int {
	if info.isDistinct {
		return kAggArgPrefixSz
	}
	return kAggArgPrefixSz + kAggArgOrdinalSz
}

func aggPayloadFromKey(info *aggInfo, k []byte) []byte {
	return k[aggPayloadOffset(info):]
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
	for _, mob := range ag.mobs {
		if freeable, ok := mob.(freeableMarshalerUnmarshaler); ok {
			freeable.Free()
		}
	}
	ag.mobs = nil
}

type aggExec struct {
	mp *mpool.MPool
	aggInfo
	chunkSize int
	state     []aggState
}

// SetPrepareParamKind restores the scalar compatibility summary on a
// deserialized preserving aggregate state. Row-exact metadata, when present,
// is installed by the aggregate implementation and remains authoritative;
// this method is only the v1 partial-state fallback.
func (ae *aggExec) SetPrepareParamKind(kind vector.PrepareParamKind) {
	for i := range ae.state {
		if len(ae.state[i].vecs) > 0 && ae.state[i].vecs[0] != nil {
			// An exact winner sidecar is authoritative. A scalar trailer can
			// restore only legacy states that did not carry row provenance.
			if len(ae.state[i].vecs[0].GetPrepareParamKinds()) == 0 {
				ae.state[i].vecs[0].SetPrepareParamKind(kind)
			}
		}
	}
}

// prepareParamKindsFromVector returns a compact copy only when at least one
// non-NULL row carries provenance.  The nil result is the ordinary/unobserved
// fast path and keeps spill/partial records byte-for-byte unchanged.
func prepareParamKindsFromVector(vec *vector.Vector) []vector.PrepareParamKind {
	// A uniform scalar is already represented by the aggregate's legacy
	// summary.  Only the heterogeneous sidecar needs an O(rows) payload.
	if vec == nil || len(vec.GetPrepareParamKinds()) == 0 {
		return nil
	}
	kinds := make([]vector.PrepareParamKind, vec.Length())
	hasKind := false
	for i := range kinds {
		if vec.IsNull(uint64(i)) {
			continue
		}
		kind := vec.GetPrepareParamKindAt(i)
		kinds[i] = kind
		if kind != vector.PrepareParamNone {
			hasKind = true
		}
	}
	if !hasKind {
		return nil
	}
	return kinds
}

// PrepareParamKindsForChunk returns winner provenance in the same row order as
// SaveIntermediateResultOfChunk.  Preserving aggregates use vecs[0] as their
// result vector; callers only request this capability for those aggregates.
func (ae *aggExec) PrepareParamKindsForChunk(chunk int) []vector.PrepareParamKind {
	if chunk < 0 || chunk >= len(ae.state) || len(ae.state[chunk].vecs) == 0 {
		return nil
	}
	return prepareParamKindsFromVector(ae.state[chunk].vecs[0])
}

// PrepareParamKindRowCountForChunk returns the number of rows serialized by
// SaveIntermediateResultOfChunk.  It is used only as a validation bound by
// the transient PPK decoder; it must not allocate or inspect provenance.
func (ae *aggExec) PrepareParamKindRowCountForChunk(chunk int) int {
	if chunk < 0 || chunk >= len(ae.state) || len(ae.state[chunk].vecs) == 0 ||
		ae.state[chunk].vecs[0] == nil {
		return -1
	}
	return ae.state[chunk].vecs[0].Length()
}

// PrepareParamKindRowCountFlat returns the packed row count consumed by
// RestorePrepareParamKindsFlat after UnmarshalFromReader has compacted all
// serialized chunks into the aggregate state.
func (ae *aggExec) PrepareParamKindRowCountFlat() int {
	rows := 0
	for i := range ae.state {
		if len(ae.state[i].vecs) == 0 || ae.state[i].vecs[0] == nil {
			continue
		}
		rows += ae.state[i].vecs[0].Length()
	}
	return rows
}

// prepareParamKindSummaryFromVector returns the transport-significant scalar
// summary for a result vector.  Uniform metadata is deliberately kept O(1);
// a heterogeneous sidecar is scanned only when the caller needs to summarize
// a selected subset (the exact sidecar is emitted separately in that case).
func prepareParamKindSummaryFromVector(vec *vector.Vector) (vector.PrepareParamKind, bool) {
	if vec == nil || vec.Length() == 0 || vec.AllNull() {
		return vector.PrepareParamNone, false
	}
	if len(vec.GetPrepareParamKinds()) == 0 {
		kind := vec.GetPrepareParamKind()
		if kind == vector.PrepareParamNone {
			return vector.PrepareParamNone, false
		}
		return kind, true
	}
	var kind vector.PrepareParamKind
	seen := false
	for row := 0; row < vec.Length(); row++ {
		if vec.IsNull(uint64(row)) {
			continue
		}
		current := vec.GetPrepareParamKindAt(row)
		if !seen {
			kind, seen = current, true
		} else if current != kind {
			kind = vector.PrepareParamNone
		}
	}
	if !seen || kind == vector.PrepareParamNone {
		return vector.PrepareParamNone, false
	}
	return kind, true
}

// PrepareParamKindSummaryForChunk returns the scalar source category for the
// rows written by SaveIntermediateResultOfChunk.  It is separate from
// PrepareParamKindsForChunk so uniform vectors do not allocate a row payload.
func (ae *aggExec) PrepareParamKindSummaryForChunk(chunk int) (vector.PrepareParamKind, bool) {
	if chunk < 0 || chunk >= len(ae.state) || len(ae.state[chunk].vecs) == 0 {
		return vector.PrepareParamNone, false
	}
	return prepareParamKindSummaryFromVector(ae.state[chunk].vecs[0])
}

// PrepareParamKindSummaryForSelection summarizes the rows emitted by
// writeStateToBuf without materializing a uniform per-row representation.
func (ae *aggExec) PrepareParamKindSummaryForSelection(flags [][]uint8) (vector.PrepareParamKind, bool) {
	var kind vector.PrepareParamKind
	seen := false
	for chunk, chunkFlags := range flags {
		if len(chunkFlags) == 0 || chunk >= len(ae.state) || len(ae.state[chunk].vecs) == 0 {
			continue
		}
		vec := ae.state[chunk].vecs[0]
		if vec == nil {
			continue
		}
		for row, flag := range chunkFlags {
			if flag == 0 || row >= vec.Length() || vec.IsNull(uint64(row)) {
				continue
			}
			current := vec.GetPrepareParamKindAt(row)
			if current == vector.PrepareParamNone {
				continue
			}
			if !seen {
				kind, seen = current, true
			} else if current != kind {
				kind = vector.PrepareParamNone
			}
		}
	}
	if !seen || kind == vector.PrepareParamNone {
		return vector.PrepareParamNone, false
	}
	return kind, true
}

// PrepareParamKindsForSelection follows writeStateToBuf's packed row order:
// chunks in ascending order, then selected rows within each chunk.
func (ae *aggExec) PrepareParamKindsForSelection(flags [][]uint8) []vector.PrepareParamKind {
	hasMetadata := false
	rowCount := 0
	for chunk, chunkFlags := range flags {
		if len(chunkFlags) == 0 || chunk >= len(ae.state) || len(ae.state[chunk].vecs) == 0 {
			continue
		}
		vec := ae.state[chunk].vecs[0]
		if vec != nil && len(vec.GetPrepareParamKinds()) != 0 {
			hasMetadata = true
		}
		for _, flag := range chunkFlags {
			if flag != 0 {
				rowCount++
			}
		}
	}
	if !hasMetadata || rowCount == 0 {
		return nil
	}
	kinds := make([]vector.PrepareParamKind, 0, rowCount)
	for chunk, chunkFlags := range flags {
		if len(chunkFlags) == 0 || chunk >= len(ae.state) || len(ae.state[chunk].vecs) == 0 {
			continue
		}
		vec := ae.state[chunk].vecs[0]
		for row, flag := range chunkFlags {
			if flag == 0 {
				continue
			}
			if vec == nil {
				kinds = append(kinds, vector.PrepareParamNone)
			} else {
				kinds = append(kinds, vec.GetPrepareParamKindAt(row))
			}
		}
	}
	for _, kind := range kinds {
		if kind != vector.PrepareParamNone {
			return kinds
		}
	}
	return nil
}

func (ae *aggExec) RestorePrepareParamKindsForChunk(
	chunk int,
	kinds []vector.PrepareParamKind,
	mp *mpool.MPool,
) error {
	if chunk < 0 || chunk >= len(ae.state) || len(ae.state[chunk].vecs) == 0 {
		return moerr.NewInternalErrorNoCtxf("aggregate provenance chunk out of range: %d", chunk)
	}
	vec := ae.state[chunk].vecs[0]
	if vec == nil {
		return moerr.NewInternalErrorNoCtx("aggregate provenance vector is nil")
	}
	if len(kinds) != vec.Length() {
		return moerr.NewInternalErrorNoCtxf(
			"aggregate provenance row count %d does not match %d", len(kinds), vec.Length())
	}
	return vec.SetPrepareParamKindsWithMP(kinds, mp)
}

// RestorePrepareParamKindsFlat restores rows packed by UnmarshalFromReader.
// The aggregate reader may repack several serialized chunks into fresh
// AggBatchSize chunks, so the wire metadata is intentionally a flat sequence.
func (ae *aggExec) RestorePrepareParamKindsFlat(
	kinds []vector.PrepareParamKind,
	mp *mpool.MPool,
) error {
	if len(kinds) == 0 {
		return nil
	}
	pos := 0
	for chunk := range ae.state {
		if len(ae.state[chunk].vecs) == 0 || ae.state[chunk].vecs[0] == nil {
			continue
		}
		vec := ae.state[chunk].vecs[0]
		n := vec.Length()
		if pos+n > len(kinds) {
			return moerr.NewInternalErrorNoCtx("aggregate provenance payload is shorter than state")
		}
		if err := vec.SetPrepareParamKindsWithMP(kinds[pos:pos+n], mp); err != nil {
			return err
		}
		pos += n
	}
	if pos != len(kinds) {
		return moerr.NewInternalErrorNoCtx("aggregate provenance payload has trailing rows")
	}
	return nil
}

func binaryStringRowsFromVector(vec *vector.Vector) []bool {
	if vec == nil || !vec.HasBinaryStringRows() {
		return nil
	}
	rows := make([]bool, vec.Length())
	for row := range rows {
		rows[row] = vec.GetBinaryStringMetadataAt(row)
	}
	return rows
}

func (ae *aggExec) BinaryStringRowsForChunk(chunk int) []bool {
	if chunk < 0 || chunk >= len(ae.state) || len(ae.state[chunk].vecs) == 0 {
		return nil
	}
	return binaryStringRowsFromVector(ae.state[chunk].vecs[0])
}

func (ae *aggExec) BinaryStringRowsForSelection(flags [][]uint8) []bool {
	hasRows := false
	for chunk := range flags {
		if chunk < len(ae.state) && len(ae.state[chunk].vecs) > 0 &&
			ae.state[chunk].vecs[0] != nil && ae.state[chunk].vecs[0].HasBinaryStringRows() {
			hasRows = true
			break
		}
	}
	if !hasRows {
		return nil
	}
	rowCount := 0
	for _, chunkFlags := range flags {
		for _, flag := range chunkFlags {
			if flag != 0 {
				rowCount++
			}
		}
	}
	if rowCount == 0 {
		return nil
	}
	rows := make([]bool, 0, rowCount)
	for chunk, chunkFlags := range flags {
		var vec *vector.Vector
		if chunk < len(ae.state) && len(ae.state[chunk].vecs) > 0 {
			vec = ae.state[chunk].vecs[0]
		}
		for row, flag := range chunkFlags {
			if flag == 0 {
				continue
			}
			rows = append(rows, vec != nil && vec.GetBinaryStringMetadataAt(row))
		}
	}
	return rows
}

func (ae *aggExec) BinaryStringSummaryForChunk(chunk int) bool {
	if chunk < 0 || chunk >= len(ae.state) || len(ae.state[chunk].vecs) == 0 ||
		ae.state[chunk].vecs[0] == nil {
		return false
	}
	vec := ae.state[chunk].vecs[0]
	if !vec.HasBinaryStringMetadata() {
		return false
	}
	for row := 0; row < vec.Length(); row++ {
		if !vec.IsNull(uint64(row)) {
			return vec.GetBinaryStringMetadataAt(row)
		}
	}
	return false
}

func (ae *aggExec) BinaryStringSummaryForSelection(flags [][]uint8) bool {
	for chunk, chunkFlags := range flags {
		if chunk >= len(ae.state) || len(ae.state[chunk].vecs) == 0 || ae.state[chunk].vecs[0] == nil {
			continue
		}
		vec := ae.state[chunk].vecs[0]
		if !vec.HasBinaryStringMetadata() {
			continue
		}
		for row, flag := range chunkFlags {
			if flag != 0 && !vec.IsNull(uint64(row)) && vec.GetBinaryStringMetadataAt(row) {
				return true
			}
		}
	}
	return false
}

func (ae *aggExec) RestoreBinaryStringRowsForChunk(chunk int, rows []bool, mp *mpool.MPool) error {
	if chunk < 0 || chunk >= len(ae.state) || len(ae.state[chunk].vecs) == 0 || ae.state[chunk].vecs[0] == nil {
		return moerr.NewInternalErrorNoCtxf("aggregate binary provenance chunk out of range: %d", chunk)
	}
	return ae.state[chunk].vecs[0].SetBinaryStringRowsWithMP(rows, mp)
}

func (ae *aggExec) RestoreBinaryStringRowsFlat(rows []bool, mp *mpool.MPool) error {
	pos := 0
	for chunk := range ae.state {
		if len(ae.state[chunk].vecs) == 0 || ae.state[chunk].vecs[0] == nil {
			continue
		}
		vec := ae.state[chunk].vecs[0]
		n := vec.Length()
		if pos+n > len(rows) {
			return moerr.NewInternalErrorNoCtx("aggregate binary provenance payload is shorter than state")
		}
		if err := vec.SetBinaryStringRowsWithMP(rows[pos:pos+n], mp); err != nil {
			return err
		}
		pos += n
	}
	if pos != len(rows) {
		return moerr.NewInternalErrorNoCtx("aggregate binary provenance payload has trailing rows")
	}
	return nil
}

func (ae *aggExec) SetBinaryStringSummary(binaryString bool) {
	if !binaryString {
		return
	}
	for chunk := range ae.state {
		if len(ae.state[chunk].vecs) > 0 && ae.state[chunk].vecs[0] != nil &&
			!ae.state[chunk].vecs[0].HasBinaryStringRows() {
			ae.state[chunk].vecs[0].SetIsBinaryString(true)
		}
	}
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
	return int(u >> aggBatchSizeShift), uint16(u & aggBatchSizeMask)
}

func chunkArr[T any](v *vector.Vector) *[AggBatchSize]T {
	return (*[AggBatchSize]T)(vector.MustFixedColAsSlice[T](v, AggBatchSize))
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
		ae.state = make([]aggState, 1)
		if err := ae.state[0].init(ae.mp, 0, 1, &ae.aggInfo, true); err != nil {
			ae.state = nil
			return err
		}
		// Ensure vecs have AggBatchSize capacity so chunkArr is safe.
		for _, vec := range ae.state[0].vecs {
			if vec != nil && vec.Capacity() < AggBatchSize {
				if err := vec.PreExtend(AggBatchSize, ae.mp); err != nil {
					ae.state[0].free(ae.mp)
					ae.state = nil
					return err
				}
			}
		}
		if _, _, err := ae.state[0].grow(ae.mp, 1, true); err != nil {
			ae.state[0].free(ae.mp)
			ae.state = nil
			return err
		}
		return nil
	}

	// grow the state until the more groups are added
	for remain := int32(more); remain > 0; {
		if len(ae.state) != 0 {
			var err error
			_, remain, err = ae.state[len(ae.state)-1].grow(ae.mp, remain, true)
			if err != nil {
				return err
			}
		}

		if remain == 0 {
			return nil
		}
		ae.state = append(ae.state, aggState{})
		if err := ae.state[len(ae.state)-1].init(ae.mp, 0, AggBatchSize, &ae.aggInfo, true); err != nil {
			ae.state = ae.state[:len(ae.state)-1]
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
			_, remain, _ = ae.state[len(ae.state)-1].grow(ae.mp, remain, false)
		}

		if remain == 0 {
			return nil
		}
		ae.state = append(ae.state, aggState{})
		if err := ae.state[len(ae.state)-1].init(ae.mp, 0, AggBatchSize, &ae.aggInfo, setNulls); err != nil {
			ae.state = ae.state[:len(ae.state)-1]
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

	// Empty chunks carry no positional meaning in the intermediate format: the
	// reader packs every selected state contiguously. Group spill commonly
	// selects one 8K state chunk out of hundreds, so emitting all of the empty
	// chunk headers bloats millions of small records and adds needless decode work.
	// A nil/empty flag slice is the caller's compact representation of an empty
	// chunk; non-empty all-zero flags remain encoded for compatibility.
	var chunks int32
	for i := range flags {
		if len(flags[i]) > 0 {
			chunks++
		}
	}
	types.WriteInt32(buf, chunks)
	for i := range flags {
		if len(flags[i]) == 0 {
			continue
		}
		if i >= len(ae.state) {
			return moerr.NewInternalErrorNoCtxf("aggregate state chunk out of range: %d >= %d", i, len(ae.state))
		}
		if err := ae.state[i].writeStateToBuf(ae.mp, &ae.aggInfo, flags[i], buf); err != nil {
			return err
		}
	}
	if err := ae.writeBinaryStringTrailerForSelection(flags, buf); err != nil {
		return err
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
	if err := ae.writeBinaryStringTrailerForChunk(chunk, buf); err != nil {
		return err
	}

	if err := types.WriteUint64(buf, magic); err != nil {
		return err
	}

	return nil
}

func (ae *aggExec) writeBinaryStringTrailerForSelection(flags [][]uint8, buf *bytes.Buffer) error {
	if !ae.BinaryStringSummaryForSelection(flags) {
		return nil
	}
	rowCount := int32(0)
	for _, chunkFlags := range flags {
		for _, flag := range chunkFlags {
			if flag != 0 {
				rowCount++
			}
		}
	}
	types.WriteUint64(buf, aggBinaryStringTrailerMagic)
	types.WriteInt32(buf, rowCount)
	for chunk, chunkFlags := range flags {
		if chunk >= len(ae.state) || len(ae.state[chunk].vecs) == 0 {
			continue
		}
		vec := ae.state[chunk].vecs[0]
		for row, flag := range chunkFlags {
			if flag == 0 {
				continue
			}
			if vec != nil && vec.GetBinaryStringMetadataAt(row) {
				buf.WriteByte(1)
			} else {
				buf.WriteByte(0)
			}
		}
	}
	return nil
}

func (ae *aggExec) writeBinaryStringTrailerForChunk(chunk int, buf *bytes.Buffer) error {
	if chunk < 0 || chunk >= len(ae.state) || len(ae.state[chunk].vecs) == 0 ||
		ae.state[chunk].vecs[0] == nil || !ae.state[chunk].vecs[0].HasBinaryStringMetadata() {
		return nil
	}
	vec := ae.state[chunk].vecs[0]
	types.WriteUint64(buf, aggBinaryStringTrailerMagic)
	types.WriteInt32(buf, int32(vec.Length()))
	for row := 0; row < vec.Length(); row++ {
		if vec.GetBinaryStringMetadataAt(row) {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	}
	return nil
}

func checkAggStateMagic(reader io.Reader) {
	magic, err := types.ReadUint64(reader)
	if err != nil || magic != magicNumber {
		panic(moerr.NewInternalErrorNoCtxf("invalid magic number, got %d, %v", magic, err))
	}
}

func (ae *aggExec) UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) (retErr error) {
	checkAggStateMagic(reader)
	defer func() {
		if retErr != nil {
			ae.Free()
			ae.state = nil
		}
	}()

	// read number of chunks
	cnt, err := types.ReadInt32(reader)
	if err != nil {
		return err
	}

	// nothing to read
	if cnt == 0 {
		ae.Free()
		ae.state = nil
		return ae.readBinaryStringTrailerAndMagic(reader, mp)
	} else if cnt == 1 {
		// The compact spill format makes this the common path. Retain one simple
		// fixed-state chunk across records so UnmarshalWithReader can reuse its
		// off-heap vector capacity instead of mmap/munmap for every small record.
		if len(ae.state) != 1 {
			ae.Free()
			ae.state = make([]aggState, 1)
		}
		if _, err := ae.state[0].readState(mp, reader, &ae.aggInfo); err != nil {
			return err
		}
		// Ensure vecs have AggBatchSize capacity so chunkArr is safe.
		for _, vec := range ae.state[0].vecs {
			if vec != nil && vec.Capacity() < AggBatchSize {
				if err := vec.PreExtend(AggBatchSize, mp); err != nil {
					return err
				}
			}
		}
		if !ae.aggInfo.saveArg && ae.aggInfo.makeMarshalerUnmarshaler == nil {
			ae.state[0].capacity = AggBatchSize
		}
		return ae.readBinaryStringTrailerAndMagic(reader, mp)
	}

	// Multi-chunk inputs may need to repack several independently allocated
	// states. Keep their historical cleanup path; only the one-chunk path above
	// has a complete bounded reuse invariant.
	ae.Free()
	ae.state = nil

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
	return ae.readBinaryStringTrailerAndMagic(reader, mp)
}

func (ae *aggExec) readBinaryStringTrailerAndMagic(reader io.Reader, mp *mpool.MPool) error {
	marker, err := types.ReadUint64(reader)
	if err != nil {
		return err
	}
	if marker == magicNumber {
		return nil
	}
	if marker != aggBinaryStringTrailerMagic {
		return moerr.NewInternalErrorNoCtxf("invalid aggregate trailer magic %d", marker)
	}
	rowCount, err := types.ReadInt32(reader)
	if err != nil {
		return err
	}
	if rowCount < 0 || int(rowCount) != ae.GetNumGroups() {
		return moerr.NewInternalErrorNoCtxf(
			"aggregate binary provenance row count %d does not match %d", rowCount, ae.GetNumGroups())
	}
	for chunk := range ae.state {
		if len(ae.state[chunk].vecs) == 0 || ae.state[chunk].vecs[0] == nil {
			continue
		}
		vec := ae.state[chunk].vecs[0]
		for row := 0; row < vec.Length(); row++ {
			binaryString, err := types.ReadByte(reader)
			if err != nil {
				return err
			}
			if binaryString > 1 {
				return moerr.NewInternalErrorNoCtx("invalid aggregate binary provenance row")
			}
			if binaryString == 1 {
				if err := vec.SetIsBinaryStringAt(row, true, mp); err != nil {
					return err
				}
			}
		}
	}
	checkAggStateMagic(reader)
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
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}

		idx := uint64(i) + uint64(offset)

		// For single-vector, use the fast path.
		if len(vectors) == 1 {
			if vectors[0].IsNull(idx) {
				continue
			}
			x, y := ae.getXY(group - 1)
			bs := vectors[0].GetRawBytesAt(int(idx))
			if err := ae.state[x].fillArg(ae.mp, y, bs, distinct); err != nil {
				return err
			}
			continue
		}

		// For multi-vector (e.g. COUNT(DISTINCT col1, col2)):
		// - Skip row if ANY column is NULL (MySQL semantics).
		// - Encode all column values into a combined key:
		//   [len1:4 bytes][raw1][len2:4 bytes][raw2]...
		hasNull := false
		for _, vec := range vectors {
			if vec.IsNull(idx) {
				hasNull = true
				break
			}
		}
		if hasNull {
			continue
		}

		// Calculate total encoded size.
		totalSize := 0
		rawBytes := make([][]byte, len(vectors))
		for j, vec := range vectors {
			rawBytes[j] = vec.GetRawBytesAt(int(idx))
			totalSize += 4 + len(rawBytes[j])
		}

		// Encode all columns into a single key.
		buf := make([]byte, totalSize)
		off := 0
		for _, raw := range rawBytes {
			binary.BigEndian.PutUint32(buf[off:], uint32(len(raw)))
			off += 4
			copy(buf[off:], raw)
			off += len(raw)
		}

		x, y := ae.getXY(group - 1)
		if err := ae.state[x].fillArg(ae.mp, y, buf, distinct); err != nil {
			return err
		}
	}
	return nil
}

func (ae *aggExec) batchFillOpaqueArgs(offset int, groups []uint64, payloads [][]byte, distinct bool) error {
	_ = offset
	if len(groups) != len(payloads) {
		return moerr.NewInternalErrorNoCtx("batchFillOpaqueArgs: groups and payloads length mismatch")
	}
	for i, group := range groups {
		if group == GroupNotMatched || payloads[i] == nil {
			continue
		}
		x, y := ae.getXY(group - 1)
		if err := ae.state[x].fillArg(ae.mp, y, payloads[i], distinct); err != nil {
			return err
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
