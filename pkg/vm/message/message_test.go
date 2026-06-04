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
	shm, err := hashmap.NewStrHashMap(false)
	require.NoError(t, err)

	jm := &JoinMap{
		valid: true,
		shm:   shm,
	}

	JoinMapMsg{JoinMapPtr: jm, Tag: 1}.Destroy()

	require.Nil(t, jm.shm)
	require.False(t, jm.valid)
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
		mb := NewMessageBoard()
		SendMessage(testMessage{tag: 1, destroyed: &destroyed}, mb)
	}()

	require.Eventually(t, func() bool {
		runtime.GC()
		debug.FreeOSMemory()
		return destroyed.Load() == 1
	}, 5*time.Second, 20*time.Millisecond)
}

func TestMultiCNMessageBoardResetRetainsBoardForSameStatement(t *testing.T) {
	center := &MessageCenter{
		StmtIDToBoard: make(map[uuid.UUID]*MessageBoard),
		RwMutex:       &sync.Mutex{},
	}
	stmtID := uuid.New()
	mb := NewMessageBoard().SetMultiCN(center, stmtID)

	newBoard := mb.Reset()
	require.NotSame(t, mb, newBoard)
	require.Same(t, mb, center.StmtIDToBoard[stmtID])

	reused := NewMessageBoard().SetMultiCN(center, stmtID)
	require.Same(t, mb, reused)
	require.Equal(t, 1, reused.refCount)
	require.True(t, reused.releasedAt.IsZero())
}

func TestMultiCNMessageBoardResetKeepsQueuedMessagesForLateReceiver(t *testing.T) {
	center := &MessageCenter{
		StmtIDToBoard: make(map[uuid.UUID]*MessageBoard),
		RwMutex:       &sync.Mutex{},
	}
	stmtID := uuid.New()
	mb := NewMessageBoard().SetMultiCN(center, stmtID)
	SendMessage(testMessage{tag: 1}, mb)

	_ = mb.Reset()

	reused := NewMessageBoard().SetMultiCN(center, stmtID)
	receiver := NewMessageReceiver([]int32{1}, AddrBroadCastOnCurrentCN(), reused)
	msgs, ctxDone, err := receiver.ReceiveMessage(false, context.Background())

	require.NoError(t, err)
	require.False(t, ctxDone)
	require.Len(t, msgs, 1)
	require.IsType(t, testMessage{}, msgs[0])
}

func TestMultiCNMessageBoardResetWaitsForAllActiveRefs(t *testing.T) {
	center := &MessageCenter{
		StmtIDToBoard: make(map[uuid.UUID]*MessageBoard),
		RwMutex:       &sync.Mutex{},
	}
	stmtID := uuid.New()
	mb := NewMessageBoard().SetMultiCN(center, stmtID)
	reused := NewMessageBoard().SetMultiCN(center, stmtID)
	require.Same(t, mb, reused)
	require.Equal(t, 2, mb.refCount)

	mb.waiters = append(mb.waiters, make(chan bool, 1))
	_ = mb.Reset()

	require.Same(t, mb, center.StmtIDToBoard[stmtID])
	require.Equal(t, 1, mb.refCount)
	require.True(t, mb.releasedAt.IsZero())
	require.Len(t, mb.waiters, 1)

	_ = reused.Reset()
	require.Same(t, mb, center.StmtIDToBoard[stmtID])
	require.Equal(t, 0, mb.refCount)
	require.False(t, mb.releasedAt.IsZero())
	require.Empty(t, mb.waiters)
}

func TestMultiCNMessageBoardSetMultiCNClearsStaleResetFlag(t *testing.T) {
	center := &MessageCenter{
		StmtIDToBoard: make(map[uuid.UUID]*MessageBoard),
		RwMutex:       &sync.Mutex{},
	}
	mb := NewMessageBoard()
	mb = mb.Reset()
	require.True(t, mb.reset)

	mb = mb.SetMultiCN(center, uuid.New())

	require.True(t, mb.multiCN)
	require.False(t, mb.reset)
}

func TestMultiCNMessageBoardCleanupReleasedBoards(t *testing.T) {
	var destroyed atomic.Int32
	center := &MessageCenter{
		StmtIDToBoard: make(map[uuid.UUID]*MessageBoard),
		RwMutex:       &sync.Mutex{},
	}
	stmtID := uuid.New()
	mb := NewMessageBoard().SetMultiCN(center, stmtID)
	SendMessage(testMessage{tag: 1, destroyed: &destroyed}, mb)

	_ = mb.Reset()
	require.Len(t, center.StmtIDToBoard, 1)
	require.Equal(t, int32(0), destroyed.Load())
	require.Len(t, mb.messages, 1)
	require.Empty(t, mb.waiters)

	center.RwMutex.Lock()
	mb.releasedAt = time.Now().Add(-multiCNMessageBoardRetainDuration - time.Second)
	center.cleanupReleasedBoardsLocked(time.Now())
	center.RwMutex.Unlock()

	require.Empty(t, center.StmtIDToBoard)
	require.Equal(t, int32(1), destroyed.Load())
	require.Empty(t, mb.messages)
}
