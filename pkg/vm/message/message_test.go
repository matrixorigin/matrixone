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

type accountedTestMessage struct {
	mp     *mpool.MPool
	buffer []byte
}

func (m *accountedTestMessage) Serialize() []byte { return nil }

func (m *accountedTestMessage) Deserialize([]byte) Message { return m }

func (m *accountedTestMessage) NeedBlock() bool { return true }

func (m *accountedTestMessage) GetMsgTag() int32 { return 1 }

func (m *accountedTestMessage) GetReceiverAddr() MessageAddress {
	return AddrBroadCastOnCurrentCN()
}

func (m *accountedTestMessage) DebugString() string { return "accounted test message" }

func (m *accountedTestMessage) Destroy() {
	if m.buffer != nil {
		m.mp.Free(m.buffer)
		m.buffer = nil
	}
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

func TestMessageBoardCloseAndDrainRejectsLateMessages(t *testing.T) {
	var destroyed atomic.Int32
	mb := NewMessageBoard()
	SendMessage(testMessage{tag: 1, destroyed: &destroyed}, mb)

	receiver := NewMessageReceiver(
		[]int32{2},
		AddrBroadCastOnCurrentCN(),
		mb,
	)
	waiting := make(chan error, 1)
	go func() {
		_, _, err := receiver.ReceiveMessage(true, context.Background())
		waiting <- err
	}()

	require.True(t, mb.CloseAndDrain())
	require.False(t, mb.CloseAndDrain())
	require.ErrorContains(t, <-waiting, "message board is closed")
	require.Equal(t, int32(1), destroyed.Load())
	require.Empty(t, mb.messages)
	require.Empty(t, mb.waiters)

	SendMessage(testMessage{tag: 2, destroyed: &destroyed}, mb)
	require.Equal(t, int32(2), destroyed.Load())
	require.NotSame(t, mb, mb.Reset())
}

func TestMessageBoardCloseAndDrainRemovesMultiCNRegistration(t *testing.T) {
	center := &MessageCenter{
		StmtIDToBoard: make(map[uuid.UUID]*MessageBoard),
		RwMutex:       &sync.Mutex{},
	}
	stmtID := uuid.New()
	mb := NewMessageBoard().SetMultiCN(center, stmtID)
	require.Same(t, mb, center.StmtIDToBoard[stmtID])

	require.True(t, mb.CloseAndDrain())
	_, ok := center.StmtIDToBoard[stmtID]
	require.False(t, ok)
}

func TestClosedMessageBoardLatePayloadDrainsOriginalGeneration(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	buffer, err := mp.AllocAccounted(64, account, 1, 1)
	require.NoError(t, err)

	mb := NewMessageBoard()
	require.True(t, mb.CloseAndDrain())
	terminal, first, err := registry.CompleteTerminal(account)
	require.True(t, first)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
	require.Equal(t, uint64(cap(buffer)), terminal.Used)
	require.True(t, registry.AdmissionSuspended())

	// A producer that already owns a payload cannot republish it after the
	// attempt boundary. Destroy performs the physical Free against the account
	// captured by the allocation, even though that generation is now sealed.
	SendMessage(&accountedTestMessage{mp: mp, buffer: buffer}, mb)
	require.False(t, registry.AdmissionSuspended())
	_, ok := registry.Resolve(account.Handle())
	require.False(t, ok)
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

func TestLegacySpillBuildPayload(t *testing.T) {
	t.Run("transfers_ownership", func(t *testing.T) {
		f1, err := os.CreateTemp("", "test_fd_*")
		require.NoError(t, err)
		defer os.Remove(f1.Name())
		f2, err := os.CreateTemp("", "test_fd_*")
		require.NoError(t, err)
		defer os.Remove(f2.Name())

		jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mpool.MustNewZero())
		jm.IncRef(1)
		require.NoError(t, jm.SetSpillBuildPayload(SpillBuildPayload{
			LegacyFds: []*os.File{f1, f2},
		}))
		payload, err := jm.TakeSpillBuildPayload()
		require.NoError(t, err)

		require.Len(t, payload.LegacyFds, 2)
		require.Same(t, f1, payload.LegacyFds[0])
		require.Same(t, f2, payload.LegacyFds[1])

		require.NoError(t, payload.Close())
	})

	t.Run("second_call_returns_explicit_error", func(t *testing.T) {
		f, err := os.CreateTemp("", "test_fd_*")
		require.NoError(t, err)
		defer os.Remove(f.Name())

		jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mpool.MustNewZero())
		jm.IncRef(1)
		require.NoError(t, jm.SetSpillBuildPayload(SpillBuildPayload{
			LegacyFds: []*os.File{f},
		}))
		payload, err := jm.TakeSpillBuildPayload()
		require.NoError(t, err)

		_, err = jm.TakeSpillBuildPayload()
		require.ErrorIs(t, err, ErrSpillBuildPayloadTaken)
		require.NoError(t, payload.Close())
	})

	t.Run("empty_payload_is_rejected", func(t *testing.T) {
		jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mpool.MustNewZero())
		jm.IncRef(1)
		require.ErrorIs(t, jm.SetSpillBuildPayload(SpillBuildPayload{}), ErrSpillBuildPayloadEmpty)
	})

	t.Run("mixed_payload_is_rejected", func(t *testing.T) {
		jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mpool.MustNewZero())
		jm.IncRef(1)
		require.ErrorIs(t, jm.SetSpillBuildPayload(SpillBuildPayload{
			Files:     []*SpillFile{nil},
			LegacyFds: []*os.File{nil},
			BudgetRef: struct{}{},
		}), ErrSpillBuildPayloadMixed)
	})

	t.Run("legacy_payload_with_budget_is_rejected", func(t *testing.T) {
		jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mpool.MustNewZero())
		jm.IncRef(1)
		require.ErrorIs(t, jm.SetSpillBuildPayload(SpillBuildPayload{
			LegacyFds: []*os.File{nil},
			BudgetRef: struct{}{},
		}), ErrSpillBuildLegacyBudget)
	})

	t.Run("free_before_set_rejects_late_publication", func(t *testing.T) {
		f, err := os.CreateTemp("", "test_fd_*")
		require.NoError(t, err)
		defer os.Remove(f.Name())

		jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mpool.MustNewZero())
		jm.IncRef(1)
		jm.FreeMemory()
		payload := SpillBuildPayload{LegacyFds: []*os.File{f}}
		require.ErrorIs(t, jm.SetSpillBuildPayload(payload), ErrSpillBuildPayloadTaken)
		_, err = f.Stat()
		require.NoError(t, err, "rejected late payload remains caller-owned")
		require.NoError(t, payload.Close())
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

		jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mp)
		jm.IncRef(1)
		require.NoError(t, jm.SetSpillBuildPayload(SpillBuildPayload{
			LegacyFds: []*os.File{f1, f2},
		}))
		jm.FreeMemory()

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

		jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mp)
		jm.IncRef(1)
		require.NoError(t, jm.SetSpillBuildPayload(SpillBuildPayload{
			LegacyFds: []*os.File{f, nil},
		}))
		jm.FreeMemory() // must not panic on nil entry
	})

	t.Run("take_then_free_does_not_double_close", func(t *testing.T) {
		mp := mpool.MustNewZero()
		f, err := os.CreateTemp("", "test_fd_*")
		require.NoError(t, err)
		defer os.Remove(f.Name())

		jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mp)
		jm.IncRef(1)
		require.NoError(t, jm.SetSpillBuildPayload(SpillBuildPayload{
			LegacyFds: []*os.File{f},
		}))
		payload, err := jm.TakeSpillBuildPayload()
		require.NoError(t, err)
		require.Len(t, payload.LegacyFds, 1)

		// FreeMemory after TakeSpillBuildPayload should not close the fds.
		jm.FreeMemory()

		// fd is still open (caller owns it)
		_, err = f.Stat()
		require.NoError(t, err)
		require.NoError(t, payload.Close())
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

func TestAccountedSpillFileOwnership(t *testing.T) {
	var releases atomic.Int32
	newFile := func() (*SpillFile, string) {
		fd, err := os.CreateTemp("", "test_accounted_spill_*")
		require.NoError(t, err)
		return NewSpillFile(fd, 7, 11, func() { releases.Add(1) }), fd.Name()
	}

	t.Run("free_closes_and_releases_once", func(t *testing.T) {
		releases.Store(0)
		file, name := newFile()
		defer os.Remove(name)
		fd := file.File()
		jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mpool.MustNewZero())
		jm.IncRef(1)
		require.NoError(t, jm.SetSpillBuildPayload(SpillBuildPayload{
			Files:     []*SpillFile{file},
			BudgetRef: struct{}{},
		}))
		require.True(t, jm.IsSpilled())
		require.Equal(t, int64(7), file.Rows())
		require.Equal(t, uint64(11), file.Bytes())

		jm.FreeMemory()
		jm.FreeMemory()
		require.Equal(t, int32(1), releases.Load())
		_, err := fd.Stat()
		require.Error(t, err)
	})

	t.Run("take_moves_complete_ownership", func(t *testing.T) {
		releases.Store(0)
		file, name := newFile()
		defer os.Remove(name)
		jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mpool.MustNewZero())
		jm.IncRef(1)
		budgetIdentity := &struct{ generation uint64 }{generation: 9}
		require.NoError(t, jm.SetSpillBuildPayload(SpillBuildPayload{
			Files:     []*SpillFile{file},
			BudgetRef: budgetIdentity,
		}))

		payload, err := jm.TakeSpillBuildPayload()
		require.NoError(t, err)
		require.Len(t, payload.Files, 1)
		require.Same(t, budgetIdentity, payload.BudgetRef)
		_, err = jm.TakeSpillBuildPayload()
		require.ErrorIs(t, err, ErrSpillBuildPayloadTaken)
		jm.FreeMemory()
		require.Zero(t, releases.Load())
		require.NoError(t, payload.Close())
		require.NoError(t, payload.Close())
		require.Equal(t, int32(1), releases.Load())
	})

	t.Run("shared_map_rejected_without_ownership_transfer", func(t *testing.T) {
		releases.Store(0)
		file, name := newFile()
		defer os.Remove(name)
		fd := file.File()
		jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mpool.MustNewZero())
		jm.IncRef(2)
		payload := SpillBuildPayload{Files: []*SpillFile{file}, BudgetRef: struct{}{}}

		err := jm.SetSpillBuildPayload(payload)
		require.ErrorIs(t, err, ErrSpillBuildShared)
		require.False(t, jm.IsSpilled())
		jm.FreeMemory()
		require.Zero(t, releases.Load(), "rejected payload must remain caller-owned")
		_, err = fd.Stat()
		require.NoError(t, err)
		require.NoError(t, payload.Close())
		require.Equal(t, int32(1), releases.Load())
	})

	t.Run("missing_budget_reference_is_rejected_without_transfer", func(t *testing.T) {
		releases.Store(0)
		file, name := newFile()
		defer os.Remove(name)
		fd := file.File()
		jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mpool.MustNewZero())
		jm.IncRef(1)
		payload := SpillBuildPayload{Files: []*SpillFile{file}}

		err := jm.SetSpillBuildPayload(payload)
		require.ErrorIs(t, err, ErrSpillBuildBudgetRef)
		require.False(t, jm.IsSpilled())
		jm.FreeMemory()
		require.Zero(t, releases.Load())
		_, err = fd.Stat()
		require.NoError(t, err)
		require.NoError(t, payload.Close())
		require.Equal(t, int32(1), releases.Load())
	})

	t.Run("concurrent_take_moves_files_and_budget_together_once", func(t *testing.T) {
		releases.Store(0)
		file, name := newFile()
		defer os.Remove(name)
		jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mpool.MustNewZero())
		jm.IncRef(1)
		budgetIdentity := &struct{ generation uint64 }{generation: 11}
		require.NoError(t, jm.SetSpillBuildPayload(SpillBuildPayload{
			Files:     []*SpillFile{file},
			BudgetRef: budgetIdentity,
		}))

		const callers = 16
		type takeResult struct {
			payload SpillBuildPayload
			err     error
		}
		results := make(chan takeResult, callers)
		var wg sync.WaitGroup
		for range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				payload, err := jm.TakeSpillBuildPayload()
				results <- takeResult{payload: payload, err: err}
			}()
		}
		wg.Wait()
		close(results)

		var winner SpillBuildPayload
		successes := 0
		for result := range results {
			if result.err == nil {
				successes++
				winner = result.payload
				continue
			}
			require.ErrorIs(t, result.err, ErrSpillBuildPayloadTaken)
		}
		require.Equal(t, 1, successes)
		require.Len(t, winner.Files, 1)
		require.Same(t, budgetIdentity, winner.BudgetRef)
		require.NoError(t, winner.Close())
		require.Equal(t, int32(1), releases.Load())
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

func BenchmarkJoinMapResidentSpillCheck(b *testing.B) {
	jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mpool.MustNewZero())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if jm.IsSpilled() {
			b.Fatal("resident JoinMap reported spill")
		}
	}
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
