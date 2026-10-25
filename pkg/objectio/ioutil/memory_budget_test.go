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

package ioutil

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

func TestSinkerByteWindowsAndAllocationPrediction(t *testing.T) {
	mp := mpool.MustNew("sink-budget")
	defer mpool.DeleteMPool(mp)
	srcMP := mpool.MustNew("sink-source")
	defer mpool.DeleteMPool(srcMP)
	b := batch.NewWithSize(1)
	b.Attrs = []string{"v"}
	b.Vecs[0] = vector.NewVec(types.T_text.ToType())
	defer b.Clean(srcMP)
	for range 3 {
		require.NoError(t, vector.AppendBytes(b.Vecs[0], []byte(strings.Repeat("x", 32<<10)), false, srcMP))
	}
	b.SetRowCount(3)
	for _, tc := range []struct {
		limit int64
		rows  int
	}{
		{b.Vecs[0].WindowSize(0, 3), 3},
		{b.Vecs[0].WindowSize(0, 2), 2},
		{b.Vecs[0].WindowSize(0, 2) - 1, 1},
		{1, 1},
	} {
		require.Equal(t, tc.rows, byteWindowEnd(b, 0, 3, 0, tc.limit))
	}
	s := NewSinker(-1, b.Attrs, []types.Type{types.T_text.ToType()}, mockFactory(nil, nil), mp, nil, WithBufferBytes(40<<10), WithMemorySizeThreshold(1<<30), WithTailSizeCap(0))
	defer s.Close()
	ask := s.AppendAllocation(b)
	require.NoError(t, s.Write(context.Background(), b))
	require.LessOrEqual(t, mp.AllocatedBytes(), ask)
	require.Len(t, s.staged.inMemory, 3)
	for _, stored := range s.staged.inMemory {
		require.Equal(t, 1, stored.RowCount())
		require.LessOrEqual(t, stored.Size(), 40<<10)
	}
	require.NoError(t, s.trySpill(context.Background()))
	before := mp.AllocatedBytes()
	require.Zero(t, s.AppendAllocation(b), "admission must account for actual idle buffer reuse")
	require.NoError(t, s.Write(context.Background(), b))
	require.Equal(t, before, mp.AllocatedBytes())
	require.NoError(t, s.Close())
	require.Zero(t, mp.AllocatedBytes())
}

type workspaceTracker struct {
	active, max atomic.Int32
	firstSync   chan struct{}
	gate        chan struct{}
	once        sync.Once
}
type trackedFileSinker struct {
	state  *workspaceTracker
	rows   uint32
	closed atomic.Bool
}

func (s *trackedFileSinker) Sink(_ context.Context, b *batch.Batch) error {
	s.rows += uint32(b.RowCount())
	return nil
}
func (s *trackedFileSinker) Sync(ctx context.Context) (*objectio.ObjectStats, error) {
	s.state.once.Do(func() {
		close(s.state.firstSync)
		select {
		case <-s.state.gate:
		case <-ctx.Done():
		}
	})
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	var stats objectio.ObjectStats
	_ = objectio.SetObjectStatsRowCnt(&stats, s.rows)
	return &stats, nil
}
func (s *trackedFileSinker) Reset() {}
func (s *trackedFileSinker) Close() error {
	if s.closed.CompareAndSwap(false, true) {
		s.state.active.Add(-1)
	}
	return nil
}

func TestSinkPoolTransfersSingleWorkspaceThroughSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m := rscthrottler.NewMemThrottler("pool-working-set", 1, rscthrottler.WithConstLimit(8<<30))
	ws, err := m.(rscthrottler.WorkingSetThrottler).ReserveWorkingSetCandidatesUntilFit(ctx, []rscthrottler.WorkingSetSpec{{UnitBytes: 1 << 20, IOBytes: 4 << 20}})
	require.NoError(t, err)
	defer ws.Close()
	pool := NewSinkPool(2, 1)
	defer pool.Close()
	mp := mpool.MustNew("pool-workspace")
	defer mpool.DeleteMPool(mp)
	state := &workspaceTracker{firstSync: make(chan struct{}), gate: make(chan struct{})}
	factory := func(*mpool.MPool, fileservice.FileService) FileSinker {
		active := state.active.Add(1)
		for old := state.max.Load(); active > old; old = state.max.Load() {
			if state.max.CompareAndSwap(old, active) {
				break
			}
		}
		return &trackedFileSinker{state: state}
	}
	rctx, rcancel := context.WithCancel(ctx)
	defer rcancel()
	result := &pipelineResult{ctx: rctx, cancel: rcancel}
	jobs := make([]*poolSinkJob, 6)
	for i := range jobs {
		b := batch.NewWithSize(1)
		b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(b.Vecs[0], int64(i), false, mp))
		b.SetRowCount(1)
		jobs[i] = &poolSinkJob{data: []*batch.Batch{b}, factory: factory, mp: mp, result: result, workspace: ws}
	}
	done := make(chan error, 1)
	go func() {
		for _, job := range jobs {
			if submitErr := pool.Submit(job); submitErr != nil {
				done <- submitErr
				return
			}
		}
		done <- nil
	}()
	select {
	case <-state.firstSync:
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	// With two sink workers and six submissions, a full input queue means a
	// second worker is already waiting for W. Queued input owns no W lease.
	for len(pool.sinkChan) < cap(pool.sinkChan) {
		select {
		case <-ctx.Done():
			t.Fatal(context.Cause(ctx))
		default:
			runtime.Gosched()
		}
	}
	close(state.gate)
	require.NoError(t, <-done)
	result.pending.Wait()
	require.NoError(t, result.getError())
	require.Equal(t, int32(1), state.max.Load(), "serialization and pending Sync share the same W owner")
	require.Zero(t, state.active.Load())
	require.Zero(t, mp.AllocatedBytes())
	lease, err := ws.AcquireIO(ctx, 4<<20)
	require.NoError(t, err)
	lease.Release()
}
