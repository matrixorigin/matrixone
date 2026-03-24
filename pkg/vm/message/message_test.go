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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/stretchr/testify/require"
)

type testMessage struct {
	tag       int32
	destroyed *atomic.Int32
}

func (m testMessage) Serialize() []byte {
	return nil
}

func (m testMessage) Deserialize([]byte) Message {
	return m
}

func (m testMessage) NeedBlock() bool {
	return true
}

func (m testMessage) GetMsgTag() int32 {
	return m.tag
}

func (m testMessage) GetReceiverAddr() MessageAddress {
	return AddrBroadCastOnCurrentCN()
}

func (m testMessage) DebugString() string {
	return "test message"
}

func (m testMessage) Destroy() {
	if m.destroyed != nil {
		m.destroyed.Add(1)
	}
}

func TestJoinMapMsgDestroyReleasesJoinMapMemory(t *testing.T) {
	m := mpool.MustNewZero()
	shm, err := hashmap.NewStrHashMap(false, m)
	require.NoError(t, err)

	jm := &JoinMap{
		valid: true,
		shm:   shm,
	}

	JoinMapMsg{JoinMapPtr: jm, Tag: 1}.Destroy()

	require.Nil(t, jm.shm)
	require.False(t, jm.valid)
	shm.Free()
}

func TestMessageBoardResetDestroysQueuedMessages(t *testing.T) {
	var destroyed atomic.Int32

	mb := NewMessageBoard()
	SendMessage(testMessage{tag: 1, destroyed: &destroyed}, mb)
	SendMessage(testMessage{tag: 2, destroyed: &destroyed}, mb)

	resetBoard := mb.Reset()

	require.Same(t, mb, resetBoard)
	require.Equal(t, int32(2), destroyed.Load())
	require.Empty(t, mb.messages)
	require.Empty(t, mb.waiters)
}

func TestMessageBoardFinalizerDestroysQueuedMessages(t *testing.T) {
	var destroyed atomic.Int32

	func() {
		center := &MessageCenter{
			StmtIDToBoard: make(map[uuid.UUID]*MessageBoard),
			RwMutex:       &sync.Mutex{},
		}
		mb := NewMessageBoard().SetMultiCN(center, uuid.New())
		SendMessage(testMessage{tag: 1, destroyed: &destroyed}, mb)

		newBoard := mb.Reset()
		require.NotSame(t, mb, newBoard)
		require.Empty(t, center.StmtIDToBoard)
	}()

	require.Eventually(t, func() bool {
		runtime.GC()
		debug.FreeOSMemory()
		return destroyed.Load() == 1
	}, 5*time.Second, 20*time.Millisecond)
}
