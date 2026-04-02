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
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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

func TestTakeSpillBuildFds(t *testing.T) {
	t.Run("transfers_ownership", func(t *testing.T) {
		f1, err := os.CreateTemp("", "test_fd_*")
		require.NoError(t, err)
		defer os.Remove(f1.Name())
		f2, err := os.CreateTemp("", "test_fd_*")
		require.NoError(t, err)
		defer os.Remove(f2.Name())

		jm := &JoinMap{SpillBuildFds: []*os.File{f1, f2}}
		fds := jm.TakeSpillBuildFds()

		require.Len(t, fds, 2)
		require.Same(t, f1, fds[0])
		require.Same(t, f2, fds[1])
		require.Nil(t, jm.SpillBuildFds)

		// Cleanup
		f1.Close()
		f2.Close()
	})

	t.Run("second_call_returns_nil", func(t *testing.T) {
		f, err := os.CreateTemp("", "test_fd_*")
		require.NoError(t, err)
		defer os.Remove(f.Name())
		defer f.Close()

		jm := &JoinMap{SpillBuildFds: []*os.File{f}}
		jm.TakeSpillBuildFds()

		fds := jm.TakeSpillBuildFds()
		require.Nil(t, fds)
	})

	t.Run("nil_fds", func(t *testing.T) {
		jm := &JoinMap{SpillBuildFds: nil}
		fds := jm.TakeSpillBuildFds()
		require.Nil(t, fds)
	})
}

func TestFreeMemoryClosesSpillFds(t *testing.T) {
	t.Run("closes_all_fds", func(t *testing.T) {
		mp := mpool.MustNewZero()
		f1, err := os.CreateTemp("", "test_fd_*")
		require.NoError(t, err)
		defer os.Remove(f1.Name())
		f2, err := os.CreateTemp("", "test_fd_*")
		require.NoError(t, err)
		defer os.Remove(f2.Name())

		jm := &JoinMap{
			valid:         true,
			mpool:         mp,
			SpillBuildFds: []*os.File{f1, f2},
		}
		jm.FreeMemory()

		require.Nil(t, jm.SpillBuildFds)
		require.False(t, jm.valid)

		// Verify fds are closed
		_, err = f1.Stat()
		require.Error(t, err)
		_, err = f2.Stat()
		require.Error(t, err)
	})

	t.Run("handles_nil_in_fd_slice", func(t *testing.T) {
		mp := mpool.MustNewZero()
		f, err := os.CreateTemp("", "test_fd_*")
		require.NoError(t, err)
		defer os.Remove(f.Name())

		jm := &JoinMap{
			valid:         true,
			mpool:         mp,
			SpillBuildFds: []*os.File{f, nil},
		}
		jm.FreeMemory() // must not panic on nil entry
		require.Nil(t, jm.SpillBuildFds)
	})

	t.Run("take_then_free_does_not_double_close", func(t *testing.T) {
		mp := mpool.MustNewZero()
		f, err := os.CreateTemp("", "test_fd_*")
		require.NoError(t, err)
		defer os.Remove(f.Name())

		jm := &JoinMap{
			valid:         true,
			mpool:         mp,
			SpillBuildFds: []*os.File{f},
		}

		fds := jm.TakeSpillBuildFds()
		require.Len(t, fds, 1)

		// FreeMemory after TakeSpillBuildFds should not close the fds
		jm.FreeMemory()

		// fd is still open (caller owns it)
		_, err = f.Stat()
		require.NoError(t, err)
		f.Close()
	})

	t.Run("double_free_safe", func(t *testing.T) {
		mp := mpool.MustNewZero()
		jm := &JoinMap{
			valid: true,
			mpool: mp,
		}
		jm.FreeMemory()
		jm.FreeMemory() // must not panic
	})
}

func TestIsDeleted(t *testing.T) {
	t.Run("nil_bitmap", func(t *testing.T) {
		jm := &JoinMap{delRows: nil}
		require.False(t, jm.IsDeleted(0))
		require.False(t, jm.IsDeleted(100))
	})

	t.Run("with_bitmap", func(t *testing.T) {
		var bm bitmap.Bitmap
		bm.InitWithSize(64)
		bm.Add(5)
		bm.Add(42)

		jm := &JoinMap{delRows: &bm}
		require.False(t, jm.IsDeleted(0))
		require.True(t, jm.IsDeleted(5))
		require.True(t, jm.IsDeleted(42))
		require.False(t, jm.IsDeleted(10))
	})
}

func TestNewJoinMap(t *testing.T) {
	mp := mpool.MustNewZero()
	jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mp)
	require.True(t, jm.IsValid())
	require.False(t, jm.IsSpilled())
	require.Nil(t, jm.SpillBuildFds)
	require.Nil(t, jm.GetBatches())
	require.Equal(t, int64(0), jm.GetRowCount())
}

func TestJoinMapGettersSetters(t *testing.T) {
	mp := mpool.MustNewZero()
	jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mp)

	jm.SetRowCount(100)
	require.Equal(t, int64(100), jm.GetRowCount())

	require.False(t, jm.PushedRuntimeFilterIn())
	jm.SetPushedRuntimeFilterIn(true)
	require.True(t, jm.PushedRuntimeFilterIn())
}

func TestJoinMapRefCount(t *testing.T) {
	mp := mpool.MustNewZero()
	shm, err := hashmap.NewStrHashMap(false, mp)
	require.NoError(t, err)

	jm := NewJoinMap(GroupSels{}, nil, shm, nil, nil, mp)
	require.Equal(t, int64(0), jm.GetRefCount())

	jm.IncRef(2)
	require.Equal(t, int64(2), jm.GetRefCount())

	// First Free decrements but doesn't release
	jm.Free()
	require.Equal(t, int64(1), jm.GetRefCount())
	require.True(t, jm.IsValid())

	// Second Free releases memory
	jm.Free()
	require.False(t, jm.IsValid())
	require.Nil(t, jm.shm)
}
