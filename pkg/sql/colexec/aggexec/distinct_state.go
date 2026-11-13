// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/arenaskl"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// normalizeDistinctPayload only mutates caller-owned key bytes. NaNs retain
// their payload identity; both signs of zero have the same SQL equality key.
func normalizeDistinctPayload(info *aggInfo, payload []byte) error {
	if info == nil || !info.isDistinct || info.opaqueArg {
		return nil
	}
	normalize := func(typ types.T, data []byte) error {
		switch typ {
		case types.T_float32:
			if len(data) != 4 {
				return moerr.NewInvalidInputNoCtx("invalid float DISTINCT payload")
			}
			if types.DecodeFloat32(data) == 0 {
				clear(data)
			}
		case types.T_float64:
			if len(data) != 8 {
				return moerr.NewInvalidInputNoCtx("invalid double DISTINCT payload")
			}
			if types.DecodeFloat64(data) == 0 {
				clear(data)
			}
		}
		return nil
	}
	if len(info.argTypes) == 1 {
		return normalize(info.argTypes[0].Oid, payload)
	}
	for _, typ := range info.argTypes {
		if len(payload) < 4 {
			return moerr.NewInvalidInputNoCtx("truncated DISTINCT tuple")
		}
		size := uint64(binary.BigEndian.Uint32(payload))
		payload = payload[4:]
		if size > uint64(len(payload)) {
			return moerr.NewInvalidInputNoCtx("truncated DISTINCT tuple value")
		}
		if err := normalize(typ.Oid, payload[:size]); err != nil {
			return err
		}
		payload = payload[size:]
	}
	if len(payload) != 0 {
		return moerr.NewInvalidInputNoCtx("invalid DISTINCT tuple length")
	}
	return nil
}

func (ag *aggState) initLegacyArena(mp *mpool.MPool) error {
	size := kAggArgArenaSize
	if ag.capacity < 1024 {
		size = 16 << 10
	}
	buf, err := mp.Alloc(size, true)
	if err != nil {
		return err
	}
	ag.argbuf = buf
	ag.argSkl = arenaskl.NewSkiplist(arenaskl.NewArena(buf), bytes.Compare)
	return nil
}

func (ag *aggState) insertFixedArg(mp *mpool.MPool, group uint16, value uint64) error {
	if int(group) >= len(ag.argCnt) {
		return moerr.NewInvalidInputNoCtx("invalid DISTINCT group")
	}
	if ag.argCnt[group] == math.MaxUint32 {
		return mpool.ErrAllocationAllocatorLimit
	}
	duplicate, err := ag.distinctIndex.prepare(mp, group, value)
	if duplicate {
		return arenaskl.ErrRecordExists
	}
	if err != nil {
		// A never-published optional index may fall back; a live index cannot.
		if ag.distinctIndex.count == 0 && mpool.IsRetryableAllocationCapacity(err) {
			ag.distinctIndex.free(mp)
			if err := ag.initLegacyArena(mp); err != nil {
				return err
			}
			width := ag.distinctKeyWidth
			ag.distinctKeyWidth = 0
			var key [kAggArgPrefixSz + 8]byte
			binary.BigEndian.PutUint16(key[:], group)
			binary.LittleEndian.PutUint64(key[kAggArgPrefixSz:], value)
			return ag.insertLegacyArg(mp, key[:kAggArgPrefixSz+width], nil)
		}
		return err
	}
	return ag.distinctIndex.insert(group, value)
}

// ensureArgCapacity relocates before Add can poison Arena.Size on failure.
// Aggregate states are exclusively owned by their operator during mutation.
func (ag *aggState) ensureArgCapacity(mp *mpool.MPool, key []byte) (bool, error) {
	if ag.argSkl == nil {
		return false, moerr.NewInternalErrorNoCtx("aggregate skiplist is not initialized")
	}
	if uint64(len(key)) > math.MaxUint32 {
		return false, mpool.ErrAllocationAllocatorLimit
	}
	used := uint64(ag.argSkl.Size())
	required := used + arenaskl.MaxNodeSize(uint32(len(key)), 0)
	if required <= uint64(len(ag.argbuf)) {
		return false, nil
	}
	// A duplicate must not cause allocation or fail just because the pool is full.
	it := ag.argSkl.NewIter(nil, nil)
	ok, found, _ := it.SeekGE(key)
	duplicate := ok && bytes.Equal(found, key)
	it.Close()
	if duplicate {
		return false, arenaskl.ErrRecordExists
	}
	ceiling := min(uint64(mpool.MaxAllocationSize()), uint64(math.MaxUint32))
	if required > ceiling {
		return false, mpool.ErrAllocationAllocatorLimit
	}
	chunked := min(ceiling, max(required, uint64(len(ag.argbuf))+kAggArgArenaSize))
	preferred := min(ceiling, max(chunked, uint64(len(ag.argbuf))*2))
	buf, err := mp.Alloc(int(preferred), true)
	if err != nil && preferred > chunked && mpool.IsRetryableAllocationCapacity(err) {
		buf, err = mp.Alloc(int(chunked), true)
	}
	if err != nil {
		return false, err
	}
	if err := ag.argSkl.GrowArena(buf); err != nil {
		mp.Free(buf)
		return false, err
	}
	old := ag.argbuf
	ag.argbuf = buf
	mp.Free(old)
	return true, nil
}

func (ag *aggState) insertLegacyArg(mp *mpool.MPool, key []byte, inserter *arenaskl.Inserter) error {
	moved, err := ag.ensureArgCapacity(mp, key)
	if err != nil {
		return err
	}
	if inserter != nil {
		if moved {
			*inserter = arenaskl.Inserter{}
		}
		return inserter.Add(ag.argSkl, key, nil)
	}
	return ag.argSkl.Add(key, nil)
}

// An ordinary source iterator remains valid while another state grows. For
// self-merge, close it before every mutation and reseek the immutable source
// group after it: relocation must never leave a live iterator on freed memory.
func (ag *aggState) iterForMerge(target *aggState, group uint16, fn func([]byte) error) error {
	if ag != target || ag.distinctKeyWidth != 0 {
		return ag.iter(group, fn)
	}
	var low, high [kAggArgPrefixSz]byte
	binary.BigEndian.PutUint16(low[:], group)
	binary.BigEndian.PutUint16(high[:], group+1)
	var previous []byte
	for {
		it := ag.argSkl.NewIter(low[:], high[:])
		var ok bool
		var key []byte
		if previous == nil {
			ok, key, _ = it.SeekGE(low[:])
		} else {
			ok, key, _ = it.SeekGE(previous)
			if ok && bytes.Equal(key, previous) {
				ok, key, _ = it.Next()
			}
		}
		if !ok {
			it.Close()
			return nil
		}
		previous = append(previous[:0], key...)
		it.Close()
		if err := fn(previous); err != nil {
			return err
		}
	}
}
