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
	"errors"
	"fmt"
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

func TestWriterBufferFollowsAdmittedEnvelope(t *testing.T) {
	const target = int64(8 << 20)
	schemas := [][]types.Type{{types.T_int64.ToType(), types.T_text.ToType()}}
	for _, source := range []int64{1, 1 << 20} {
		for _, size := range []int64{source, target} {
			t.Run(fmt.Sprint(source, "-", size), func(t *testing.T) {
				bound, err := IOEnvelope(schemas, size, 0)
				require.NoError(t, err)
				m := rscthrottler.NewMemThrottler("writer-object-target", 1, rscthrottler.WithConstLimit(8<<30))
				ws, err := m.(rscthrottler.WorkingSetThrottler).ReserveWorkingSetCandidatesUntilFit(context.Background(),
					[]rscthrottler.WorkingSetSpec{{UnitBytes: source, IOBytes: bound}})
				require.NoError(t, err)
				t.Cleanup(ws.Close)
				minimumRow := int64(types.T_int64.ToType().TypeSize() + types.T_text.ToType().TypeSize())
				require.Equal(t, min(size, source*minimumRow), WriterBufferBytes(ws, target, schemas))
				require.Equal(t, source, ws.UnitBytes(), "reader and metadata units stay unchanged")
			})
		}
	}
}

type workspaceTracker struct {
	active, max atomic.Int32
	firstSync   chan struct{}
	secondSink  chan struct{}
	values      chan []int64
	syncErr     error
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
	if s.state.values != nil {
		s.state.values <- append([]int64(nil), vector.MustFixedColNoTypeCheck[int64](b.Vecs[0])...)
	}
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
	if s.state.syncErr != nil {
		return nil, s.state.syncErr
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

func TestSinkPoolTransfersWeightedWorkspaceThroughSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m := rscthrottler.NewMemThrottler("pool-working-set", 1, rscthrottler.WithConstLimit(8<<30))
	schema := []types.Type{types.T_int64.ToType()}
	peak, err := IOEnvelope([][]types.Type{schema}, 8, 8)
	require.NoError(t, err)
	upload, err := UploadEnvelope(schema, 8, 8)
	require.NoError(t, err)
	ws, err := m.(rscthrottler.WorkingSetThrottler).ReserveWorkingSetCandidatesUntilFit(ctx, []rscthrottler.WorkingSetSpec{{UnitBytes: 1 << 20, IOBytes: peak + upload}})
	require.NoError(t, err)
	defer ws.Close()
	mp := mpool.MustNew("pool-workspace")
	defer mpool.DeleteMPool(mp)
	pool := NewSinkPool(2, 1)
	defer pool.Close()
	state := &workspaceTracker{firstSync: make(chan struct{}), secondSink: make(chan struct{}), gate: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(state.gate) }) }
	defer release()
	factory := func(*mpool.MPool, fileservice.FileService) FileSinker {
		active := state.active.Add(1)
		if active == 2 {
			close(state.secondSink)
		}
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
	jobs := make([]*poolSinkJob, 2)
	for i := range jobs {
		b := batch.NewWithSize(1)
		b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(b.Vecs[0], int64(i), false, mp))
		b.SetRowCount(1)
		jobs[i] = &poolSinkJob{data: []*batch.Batch{b}, factory: factory, mp: mp, result: result, uploadBytes: upload}
	}
	for _, job := range jobs {
		job.lease, err = ws.AcquireIO(ctx, peak)
		require.NoError(t, err)
		job.ioMP = mpool.MustNew("pool-workspace-io")
		require.NoError(t, pool.Submit(job))
	}
	select {
	case <-state.firstSync:
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	select {
	case <-state.secondSink:
	case <-ctx.Done():
		t.Fatal("the next encoder could not use capacity retired by the blocked upload")
	}
	release()
	result.pending.Wait()
	require.NoError(t, result.getError())
	require.Equal(t, int32(2), state.max.Load(), "encoding overlaps upload under the shared byte budget")
	require.Zero(t, state.active.Load())
	require.Zero(t, mp.AllocatedBytes())
	lease, err := ws.AcquireIO(ctx, peak+upload)
	require.NoError(t, err)
	lease.Release()
}

func TestSinkerBoundedSortedUploadOwnership(t *testing.T) {
	for _, mode := range []string{"sorted", "borrowed", "sync-error", "cancel", "reset"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			t.Cleanup(cancel)
			mp := mpool.MustNew("bounded-source")
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			b := batch.NewWithSize(1)
			b.Attrs = []string{"k"}
			b.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
			t.Cleanup(func() { b.Clean(mp) })
			for _, key := range []int64{2, 1} {
				require.NoError(t, vector.AppendFixed(b.Vecs[0], key, false, mp))
			}
			b.SetRowCount(2)
			const unit = int64(1 << 20)
			peak, _, err := BatchIOEnvelope(b, unit)
			require.NoError(t, err)
			m := rscthrottler.NewMemThrottler("sorted-upload", 1, rscthrottler.WithConstLimit(8<<30))
			ws, err := m.(rscthrottler.WorkingSetThrottler).ReserveWorkingSetCandidatesUntilFit(ctx,
				[]rscthrottler.WorkingSetSpec{{UnitBytes: unit, IOBytes: 3 * peak}})
			require.NoError(t, err)
			t.Cleanup(ws.Close)
			state := &workspaceTracker{firstSync: make(chan struct{}), gate: make(chan struct{}), values: make(chan []int64, 2)}
			if mode == "sync-error" {
				state.syncErr = errors.New("upload failed")
			}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(state.gate) }) }
			factory := func(*mpool.MPool, fileservice.FileService) FileSinker {
				state.active.Add(1)
				return &trackedFileSinker{state: state}
			}
			sinkers := make([]*Sinker, 0, 2)
			for range 2 {
				owner := mpool.MustNew("bounded-writer")
				t.Cleanup(func() { mpool.DeleteMPool(owner) })
				s := NewSinker(0, b.Attrs, []types.Type{types.T_int64.ToType()}, factory, owner, nil,
					WithOffHeap(), WithMemoryWorkspace(ws), WithPipelineFlush(), WithBufferBytes(unit), WithMemorySizeThreshold(1<<30), WithTailSizeCap(0))
				t.Cleanup(func() { s.Close(); require.Zero(t, owner.AllocatedBytes()) })
				sinkers = append(sinkers, s)
			}
			t.Cleanup(release)
			t.Cleanup(cancel)
			for i, s := range sinkers {
				if mode == "borrowed" {
					lease, err := ws.AcquireIO(ctx, peak)
					require.NoError(t, err)
					err = s.WriteBorrowedToFile(ctx, b, lease)
					lease.Release()
					require.NoError(t, err)
				} else {
					require.NoError(t, s.Write(ctx, b))
					require.NoError(t, s.Flush(ctx))
				}
				select {
				case keys := <-state.values:
					require.Equal(t, []int64{1, 2}, keys)
				case <-ctx.Done():
					t.Fatal("next encoder blocked behind an earlier upload")
				}
				if i == 0 {
					select {
					case <-state.firstSync:
					case <-ctx.Done():
						t.Fatal(context.Cause(ctx))
					}
				}
			}
			require.Equal(t, []int64{2, 1}, vector.MustFixedColNoTypeCheck[int64](b.Vecs[0]), "source remains borrowed and immutable")
			if mode == "cancel" {
				cancel()
			}
			if mode == "reset" {
				for _, s := range sinkers {
					s.Reset() // Old uploads must stop without opening the gate.
					require.NoError(t, s.Write(ctx, b))
				}
			}
			release()
			for _, s := range sinkers {
				err := s.Sync(ctx)
				if mode == "sync-error" || mode == "cancel" {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					stats, _ := s.GetResult()
					require.Len(t, stats, 1)
					require.EqualValues(t, 2, stats[0].Rows())
				}
				_ = s.Close()
			}
			require.Zero(t, state.active.Load())
			checkCtx, checkCancel := context.WithTimeout(context.Background(), time.Second)
			defer checkCancel()
			lease, err := ws.AcquireIO(checkCtx, ws.IOBytes())
			require.NoError(t, err, "every encoder and uploader returned its bytes")
			lease.Release()
		})
	}
}
