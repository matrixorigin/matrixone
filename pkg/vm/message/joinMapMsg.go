// Copyright 2022 Matrix Origin
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

package message

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

var _ Message = new(JoinMapMsg)

type GroupSels struct {
	// vals holds all row IDs in group order after Finalize.
	// offsets[k] .. offsets[k+1] is the range in vals for group k.
	vals    []int32
	offsets []int32

	// tmp holds (groupID, rowID) pairs during the build phase, before Finalize.
	tmp []int32
}

func freeSlice[T any](mp *mpool.MPool, s []T) {
	mpool.FreeSlice(mp, s[:cap(s)])
}

func (sels *GroupSels) Init(n int, mp *mpool.MPool) error {
	var err error
	sels.tmp, err = mpool.MakeSlice[int32](n*2, mp, false)
	if err != nil {
		return err
	}
	sels.tmp = sels.tmp[:0]
	return nil
}

func (sels *GroupSels) Free(mp *mpool.MPool) {
	if mp != nil {
		freeSlice(mp, sels.vals)
		freeSlice(mp, sels.offsets)
		freeSlice(mp, sels.tmp)
	}
	sels.vals = nil
	sels.offsets = nil
	sels.tmp = nil
}

func (sels *GroupSels) Size() int64 {
	const int32Size = int64(4)
	return (int64(cap(sels.vals)) + int64(cap(sels.offsets)) + int64(cap(sels.tmp))) * int32Size
}

func (sels *GroupSels) Insert(k, v int32) {
	sels.tmp = append(sels.tmp, k, v)
}

func (sels *GroupSels) Finalize(groupCount int, inputRowCount int, mp *mpool.MPool) error {
	if sels.tmp == nil {
		return nil
	}
	n := len(sels.tmp) / 2
	// if every input row got its own group (no nulls, no duplicates), no sels needed
	if n == 0 || groupCount == inputRowCount {
		sels.vals = nil
		sels.offsets = nil
		freeSlice(mp, sels.tmp)
		sels.tmp = nil
		return nil
	}
	// groupCount+2: +1 for sentinel, +1 for 1-based callers (dedup UPDATE uses keys 1..groupCount)
	var err error
	sels.offsets, err = mpool.MakeSlice[int32](groupCount+2, mp, false)
	if err != nil {
		return err
	}

	// count occurrences per group
	for i := 0; i < len(sels.tmp); i += 2 {
		k := sels.tmp[i]
		sels.offsets[k+1]++
	}
	// prefix sum
	for i := int32(1); i < int32(len(sels.offsets)); i++ {
		sels.offsets[i] += sels.offsets[i-1]
	}
	// scatter vals using offsets as write cursors, then recover
	sels.vals, err = mpool.MakeSlice[int32](n, mp, false)
	if err != nil {
		return err
	}
	for i := 0; i < len(sels.tmp); i += 2 {
		k := sels.tmp[i]
		v := sels.tmp[i+1]
		sels.vals[sels.offsets[k]] = v
		sels.offsets[k]++
	}
	// recover offsets: shift right by one
	copy(sels.offsets[1:], sels.offsets[:len(sels.offsets)-1])
	sels.offsets[0] = 0
	freeSlice(mp, sels.tmp)
	sels.tmp = nil
	return nil
}

func (sels *GroupSels) Get(k int32) []int32 {
	if sels.offsets == nil || int(k+1) >= len(sels.offsets) {
		return nil
	}
	return sels.vals[sels.offsets[k]:sels.offsets[k+1]]
}

// JoinMap is used for join
type JoinMap struct {
	runtimeFilter_In bool
	valid            bool
	rowCnt           int64 // for debug purpose
	refCnt           int64
	mpool            *mpool.MPool
	shm              *hashmap.StrHashMap
	ihm              *hashmap.IntHashMap
	sels             GroupSels
	delRows          *bitmap.Bitmap
	batches          []*batch.Batch

	// spill support
	Spilled       bool
	SpillBuildFds []*os.File // anonymous build-side file descriptors
}

func NewJoinMap(sels GroupSels, ihm *hashmap.IntHashMap, shm *hashmap.StrHashMap, delRows *bitmap.Bitmap, batches []*batch.Batch, m *mpool.MPool) *JoinMap {
	return &JoinMap{
		valid:         true,
		mpool:         m,
		shm:           shm,
		ihm:           ihm,
		sels:          sels,
		delRows:       delRows,
		batches:       batches,
		Spilled:       false,
		SpillBuildFds: nil,
	}
}

func (jm *JoinMap) GetBatches() []*batch.Batch {
	if jm == nil {
		return nil
	}
	return jm.batches
}

func (jm *JoinMap) SetRowCount(cnt int64) {
	jm.rowCnt = cnt
}

func (jm *JoinMap) GetRefCount() int64 {
	if jm == nil {
		return 0
	}
	return atomic.LoadInt64(&jm.refCnt)
}

func (jm *JoinMap) GetRowCount() int64 {
	if jm == nil {
		return 0
	}
	return jm.rowCnt
}

func (jm *JoinMap) GetGroupCount() uint64 {
	if jm.ihm != nil {
		return jm.ihm.GroupCount()
	}
	return jm.shm.GroupCount()
}

func (jm *JoinMap) SetPushedRuntimeFilterIn(b bool) {
	jm.runtimeFilter_In = b
}

func (jm *JoinMap) PushedRuntimeFilterIn() bool {
	return jm.runtimeFilter_In
}

func (jm *JoinMap) HashOnUnique() bool {
	return jm.sels.offsets == nil
}

func (jm *JoinMap) GetSels(k uint64) []int32 {
	return jm.sels.Get(int32(k))
}

//func (jm *JoinMap) GetIgnoreRows() *bitmap.Bitmap {
//	return jm.ignoreRows
//}

//func (jm *JoinMap) SetIgnoreRows(ignoreRows *bitmap.Bitmap) {
//	jm.ignoreRows = ignoreRows
//}

func (jm *JoinMap) NewIterator() hashmap.Iterator {
	if jm.shm != nil {
		return jm.shm.NewIterator()
	} else {
		return jm.ihm.NewIterator()
	}
}

func (jm *JoinMap) IncRef(cnt int32) {
	atomic.AddInt64(&jm.refCnt, int64(cnt))
}

func (jm *JoinMap) IsValid() bool {
	return jm.valid
}

func (jm *JoinMap) IsSpilled() bool {
	return jm.Spilled
}

// TakeSpillBuildFds transfers ownership of anonymous build-side file
// descriptors from the JoinMap to the caller. After this call the JoinMap
// no longer owns the fds; FreeMemory will not close them.
func (jm *JoinMap) TakeSpillBuildFds() []*os.File {
	fds := jm.SpillBuildFds
	jm.SpillBuildFds = nil
	return fds
}

func (jm *JoinMap) IsDeleted(row uint64) bool {
	return jm.delRows != nil && jm.delRows.Contains(uint64(row))
}

func (jm *JoinMap) FreeMemory() {
	for i, fd := range jm.SpillBuildFds {
		if fd != nil {
			fd.Close()
			jm.SpillBuildFds[i] = nil
		}
	}
	jm.SpillBuildFds = nil
	jm.sels.Free(jm.mpool)
	if jm.ihm != nil {
		jm.ihm.Free()
		jm.ihm = nil
	} else if jm.shm != nil {
		jm.shm.Free()
		jm.shm = nil
	}
	for i := range jm.batches {
		jm.batches[i].Clean(jm.mpool)
	}
	jm.batches = nil
	jm.valid = false
}

func (jm *JoinMap) Free() {
	if atomic.AddInt64(&jm.refCnt, -1) != 0 {
		return
	}
	jm.FreeMemory()
}

func (jm *JoinMap) Size() int64 {
	// TODO: add the size of the other JoinMap parts
	if jm.ihm == nil && jm.shm == nil {
		return 0
	}
	if jm.ihm != nil {
		return jm.ihm.Size()
	} else {
		return jm.shm.Size()
	}
}

func (jm *JoinMap) PreAlloc(n uint64) error {
	if jm.ihm != nil {
		return jm.ihm.PreAlloc(n)
	}
	return jm.shm.PreAlloc(n)
}

type JoinMapMsg struct {
	JoinMapPtr *JoinMap
	IsShuffle  bool
	ShuffleIdx int32
	Tag        int32
	Spilled    bool
}

func (t JoinMapMsg) Serialize() []byte {
	panic("top value message only broadcasts on current CN, don't need to serialize")
}

func (t JoinMapMsg) Deserialize([]byte) Message {
	panic("top value message only broadcasts on current CN, don't need to deserialize")
}

func (t JoinMapMsg) NeedBlock() bool {
	return true
}

func (t JoinMapMsg) Destroy() {
	if t.JoinMapPtr != nil {
		t.JoinMapPtr.FreeMemory()
	}
}

func (t JoinMapMsg) GetMsgTag() int32 {
	return t.Tag
}

func (t JoinMapMsg) DebugString() string {
	buf := bytes.NewBuffer(make([]byte, 0, 400))
	buf.WriteString("joinmap message, tag:" + strconv.Itoa(int(t.Tag)) + "\n")
	if t.IsShuffle {
		buf.WriteString("shuffle index " + strconv.Itoa(int(t.ShuffleIdx)) + "\n")
	}
	if t.JoinMapPtr != nil {
		buf.WriteString("joinmap rowcnt " + strconv.Itoa(int(t.JoinMapPtr.rowCnt)) + "\n")
		buf.WriteString("joinmap refcnt " + strconv.Itoa(int(t.JoinMapPtr.GetRefCount())) + "\n")
	} else {
		buf.WriteString("joinmapPtr is nil \n")
	}
	return buf.String()
}

func (t JoinMapMsg) GetReceiverAddr() MessageAddress {
	return AddrBroadCastOnCurrentCN()
}

func ReceiveJoinMap(tag int32, isShuffle bool, shuffleIdx int32, mb *MessageBoard, ctx context.Context) (*JoinMap, error) {
	msgReceiver := NewMessageReceiver([]int32{tag}, AddrBroadCastOnCurrentCN(), mb)
	for {
		msgs, ctxDone, err := msgReceiver.ReceiveMessage(true, ctx)
		if err != nil {
			return nil, err
		}
		if ctxDone {
			return nil, nil
		}
		for i := range msgs {
			msg, ok := msgs[i].(JoinMapMsg)
			if !ok {
				panic("expect join map message, receive unknown message!")
			}
			if isShuffle || msg.IsShuffle {
				if shuffleIdx != msg.ShuffleIdx {
					continue
				}
			}
			jm := msg.JoinMapPtr
			if jm == nil {
				return nil, nil
			}
			if !jm.IsValid() {
				panic("join receive a joinmap which has been freed!")
			}
			return jm, nil
		}
	}
}

func FinalizeJoinMapMessage(mb *MessageBoard, tag int32, isShuffle bool, shuffleIdx int32, sendMapSucceed bool) {
	if !sendMapSucceed {
		SendMessage(JoinMapMsg{JoinMapPtr: nil, IsShuffle: isShuffle, ShuffleIdx: shuffleIdx, Tag: tag}, mb)
	}
}
