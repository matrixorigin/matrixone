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

package message

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/stretchr/testify/require"
)

func spillTestFile(t *testing.T, data string) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "spill")
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	_, err = f.WriteString(data)
	require.NoError(t, err)
	return f
}

func TestJoinSpillSourceIndependentOffsets(t *testing.T) {
	f := spillTestFile(t, "abcdef")
	source := NewJoinSpillSource([]*os.File{f})
	defer source.Close()
	a, err := source.Reader(0)
	require.NoError(t, err)
	defer a.Close()
	b, err := source.Reader(0)
	require.NoError(t, err)
	defer b.Close()
	_, err = source.TakeFiles()
	require.Error(t, err)
	source.Close()
	buf := make([]byte, 2)
	_, err = io.ReadFull(a, buf)
	require.NoError(t, err)
	require.Equal(t, "ab", string(buf))
	_, err = io.ReadFull(b, buf)
	require.NoError(t, err)
	require.Equal(t, "ab", string(buf))
	require.NoError(t, a.Close())
	_, err = f.Stat()
	require.NoError(t, err)
	require.NoError(t, b.Close())
	_, err = f.Stat()
	require.Error(t, err)
}

func TestJoinSpillGroupOwnership(t *testing.T) {
	t.Run("empty participant and exclusive claim", func(t *testing.T) {
		group := NewJoinSpillGroup()
		epoch := group.Seal(context.Background(), []uint64{9, 2})
		build, probe := spillTestFile(t, "b"), spillTestFile(t, "p")
		require.NoError(t, group.PublishBuild([]*os.File{build}))
		work, err := group.Register(epoch, 9, nil)
		require.NoError(t, err)
		require.Nil(t, work)
		_, err = group.Register(epoch, 9, nil)
		require.Error(t, err)
		work, err = group.Register(epoch, 2, []*os.File{probe})
		require.NoError(t, err)
		require.Len(t, work.Probes, 2)
		require.Empty(t, work.Probes[0])
		require.Equal(t, probe, work.Probes[1][0])
		group.Abort(epoch)
		require.ErrorIs(t, work.Ctx.Err(), context.Canceled)
		_, err = build.Stat()
		require.NoError(t, err, "abort cannot close a claimed file under its reader")
		work.Close()
		_, err = build.Stat()
		require.Error(t, err)
		_, err = probe.Stat()
		require.Error(t, err)
	})
	t.Run("abort collects and stale epoch does not affect next", func(t *testing.T) {
		group := NewJoinSpillGroup()
		epoch := group.Seal(context.Background(), []uint64{0, 1})
		build, probe := spillTestFile(t, "b"), spillTestFile(t, "p")
		require.NoError(t, group.PublishBuild([]*os.File{build}))
		_, err := group.Register(epoch, 0, []*os.File{probe})
		require.NoError(t, err)
		group.Abort(epoch)
		_, err = probe.Stat()
		require.Error(t, err)
		next := group.Seal(context.Background(), []uint64{0})
		require.Greater(t, next, epoch)
		group.Abort(epoch)
		require.NoError(t, group.PublishBuild(nil))
		work, err := group.Register(next, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, work)
		work.Close()
	})
	t.Run("claim abort race has one terminal owner", func(t *testing.T) {
		group := NewJoinSpillGroup()
		epoch := group.Seal(context.Background(), []uint64{0})
		f := spillTestFile(t, "x")
		require.NoError(t, group.PublishBuild([]*os.File{f}))
		start := make(chan struct{})
		var work *JoinSpillWork
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; work, _ = group.Register(epoch, 0, nil) }()
		go func() { defer wg.Done(); <-start; group.Abort(epoch) }()
		close(start)
		wg.Wait()
		if work != nil {
			work.Close()
		}
		_, err := f.Stat()
		require.Error(t, err)
	})
}

func TestJoinSpillRegistryAcrossRemoteScopes(t *testing.T) {
	center := &MessageCenter{RwMutex: &sync.Mutex{}, StmtIDToBoard: make(map[uuid.UUID]*MessageBoard)}
	statement := uuid.New()
	board := NewMessageBoard().SetMultiCN(center, statement)
	group, err := board.BroadcastSpillGroup(12, []uint32{4, 9})
	require.NoError(t, err)
	epoch := group.Epoch()
	require.NoError(t, group.SealScope(epoch, 4, []int32{0}))
	f := spillTestFile(t, "build")
	require.NoError(t, group.PublishBuild([]*os.File{f}))
	work, err := group.Register(epoch, uint64(4)<<32, nil)
	require.NoError(t, err)
	require.Nil(t, work)
	replacement := board.Reset()
	require.NotSame(t, board, replacement)
	require.Same(t, board, replacement.SetMultiCN(center, statement), "late RPC must find the collecting generation")
	require.NoError(t, group.SealScope(epoch, 9, []int32{0, 1}))
	work, err = group.Register(epoch, uint64(9)<<32, nil)
	require.NoError(t, err)
	require.Nil(t, work)
	work, err = group.Register(epoch, uint64(9)<<32|1, nil)
	require.NoError(t, err)
	require.NotNil(t, work)
	work.Close()
	center.RwMutex.Lock()
	_, present := center.StmtIDToBoard[statement]
	center.RwMutex.Unlock()
	require.False(t, present, "last terminal owner releases the delayed board registration")
	_, err = f.Stat()
	require.Error(t, err)
}

func TestJoinSpillMemoryUsesActualReferences(t *testing.T) {
	group := NewJoinSpillGroup()
	epoch := group.BeginScopes(context.Background(), []uint32{1})
	require.NoError(t, group.SealScope(epoch, 1, []int32{0, 1}))
	jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, nil)
	var released atomic.Int64
	jm.SetMemoryRelease(func() { released.Add(1) })
	jm.IncRef(1)
	require.NoError(t, group.PublishMemory(jm))
	require.True(t, group.AcquireMemory(epoch, jm))
	require.True(t, group.AcquireMemory(epoch, jm))
	require.Equal(t, int64(3), jm.GetRefCount())
	group.FinishParticipant(epoch, uint64(1)<<32)
	jm.Free()
	require.Equal(t, int64(2), jm.GetRefCount())
	group.FinishParticipant(epoch, uint64(1)<<32|1)
	require.Equal(t, int64(1), jm.GetRefCount())
	require.Zero(t, released.Load())
	jm.Free()
	require.Equal(t, int64(1), released.Load())
}

func TestJoinSpillGroupAndBoardRetireMapOnce(t *testing.T) {
	board := NewMessageBoard()
	group, err := board.BroadcastSpillGroup(71, []uint32{1})
	require.NoError(t, err)
	epoch := group.Epoch()
	require.NoError(t, group.SealScope(epoch, 1, []int32{0}))
	jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, nil)
	var released atomic.Int64
	jm.SetMemoryRelease(func() { released.Add(1) })
	jm.IncRef(1)
	require.NoError(t, group.PublishMemory(jm))
	SendMessage(JoinMapMsg{JoinMapPtr: jm, Tag: 71}, board)
	require.True(t, group.AcquireMemory(epoch, jm))
	jm.Free()
	group.FinishParticipant(epoch, uint64(1)<<32)
	require.Equal(t, int64(1), released.Load())
	board.Reset()
	board.Reset()
	require.Equal(t, int64(1), released.Load(), "group root, queued message, and repeated board cleanup share one release")
}

func TestJoinSpillInputWindowsExpandConstReferencesBeforeEval(t *testing.T) {
	pool := mpool.MustNewZero()
	value := bytes.Repeat([]byte{'x'}, 32<<10)
	vec, err := vector.NewConstBytes(types.T_varchar.ToType(), value, 4, pool)
	require.NoError(t, err)
	src := batch.NewWithSize(1)
	src.Vecs[0] = vec
	src.SetRowCount(4)
	defer src.Clean(pool)
	require.Less(t, vec.Allocated(), int(vec.WindowSize(0, 4)))
	seen := 0
	require.NoError(t, ForEachJoinSpillWindow(src, 40<<10, func(window *batch.Batch) error {
		require.Equal(t, 1, window.RowCount())
		require.Equal(t, value, window.Vecs[0].GetBytesAt(0))
		seen += window.RowCount()
		return nil
	}))
	require.Equal(t, 4, seen)
	require.Equal(t, value, src.Vecs[0].GetBytesAt(0), "window cleanup cannot free its borrowed constant")
}
