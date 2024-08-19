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
	"context"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
)

var _ Message = new(JoinMapMsg)

// JoinMap is used for join
type JoinMap struct {
	runtimeFilter_In bool
	valid            bool
	rowcnt           int64 // for debug purpose
	refCnt           int64
	shm              *hashmap.StrHashMap
	ihm              *hashmap.IntHashMap
	mpool            *mpool.MPool
	multiSels        [][]int32
	batches          []*batch.Batch
}

func NewJoinMap(sels [][]int32, ihm *hashmap.IntHashMap, shm *hashmap.StrHashMap, batches []*batch.Batch, m *mpool.MPool) *JoinMap {
	return &JoinMap{
		shm:       shm,
		ihm:       ihm,
		multiSels: sels,
		batches:   batches,
		mpool:     m,
		valid:     true,
	}
}

func (jm *JoinMap) GetBatches() []*batch.Batch {
	if jm == nil {
		return nil
	}
	return jm.batches
}

func (jm *JoinMap) SetRowCount(cnt int64) {
	jm.rowcnt = cnt
}

func (jm *JoinMap) GetRowCount() int64 {
	if jm == nil {
		return 0
	}
	return jm.rowcnt
}

func (jm *JoinMap) SetPushedRuntimeFilterIn(b bool) {
	jm.runtimeFilter_In = b
}

func (jm *JoinMap) PushedRuntimeFilterIn() bool {
	return jm.runtimeFilter_In
}

func (jm *JoinMap) Sels() [][]int32 {
	return jm.multiSels
}

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

func (jm *JoinMap) Free() {
	if atomic.AddInt64(&jm.refCnt, -1) != 0 {
		return
	}
	for i := range jm.multiSels {
		jm.multiSels[i] = nil
	}
	jm.multiSels = nil
	if jm.ihm != nil {
		jm.ihm.Free()
	} else {
		jm.shm.Free()
	}
	for i := range jm.batches {
		jm.batches[i].Clean(jm.mpool)
	}
	jm.batches = nil
	jm.valid = false
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

type JoinMapMsg struct {
	JoinMapPtr *JoinMap
	IsShuffle  bool
	ShuffleIdx int32
	Tag        int32
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

func (t JoinMapMsg) GetMsgTag() int32 {
	return t.Tag
}

func (t JoinMapMsg) GetReceiverAddr() MessageAddress {
	return AddrBroadCastOnCurrentCN()
}

func ReceiveJoinMap(tag int32, isShuffle bool, shuffleIdx int32, mb *MessageBoard, ctx context.Context) *JoinMap {
	msgReceiver := NewMessageReceiver([]int32{tag}, AddrBroadCastOnCurrentCN(), mb)
	for {
		msgs, ctxDone := msgReceiver.ReceiveMessage(true, ctx)
		if ctxDone {
			return nil
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
				return nil
			}
			if !jm.IsValid() {
				panic("join receive a joinmap which has been freed!")
			}
			return jm
		}
	}
}

func FinalizeJoinMapMessage(mb *MessageBoard, tag int32, isShuffle bool, shuffleIdx int32, pipelineFailed bool, err error) {
	if pipelineFailed || err != nil {
		SendMessage(JoinMapMsg{JoinMapPtr: nil, IsShuffle: isShuffle, ShuffleIdx: shuffleIdx, Tag: tag}, mb)
	}
}
