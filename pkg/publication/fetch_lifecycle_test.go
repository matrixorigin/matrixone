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

package publication

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

type rejectingChunkSubmission struct {
	GetChunkWorker
	before func(Job) error
}

func (w *rejectingChunkSubmission) SubmitGetChunk(j Job) error {
	if err := w.before(j); err != nil {
		return err
	}
	return w.GetChunkWorker.SubmitGetChunk(j)
}

func TestFetchRetiresAcceptedJobsBeforeRefund(t *testing.T) {
	original := GetObjectFromUpstreamWithWorker
	t.Cleanup(func() { GetObjectFromUpstreamWithWorker = original })
	GetObjectFromUpstreamWithWorker = func(ctx context.Context, e SQLExecutor, name string, w GetChunkWorker, acc, pub string) ([]byte, error) {
		return fetchObjectChunks(ctx, e, name, w, acc, pub, 1<<20)
	}
	for _, mode := range []string{"chunk_failure", "caller_cancel", "worker_stop", "submit_rejection", "refill_rejection"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			entered, canceled, retire := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(retire) }) }
			worker := NewGetChunkWorker()
			finished := make(chan struct{})
			t.Cleanup(func() { cancel(); unblock(); worker.Stop() })
			const total = int64(8 << 30)
			owner := rscthrottler.NewMemThrottler(t.Name(), 1, rscthrottler.WithConstLimit(total), rscthrottler.WithMemorySampleForTesting(total, 0)).(rscthrottler.WorkingSetThrottler)
			ctx = context.WithValue(ctx, publicationMemoryKey{}, owner)
			count := int64(2)
			if mode == "refill_rejection" {
				count = int64(min(GetGetChunkWorkerThread(), cap(getOrCreateChunkSemaphore())) + 1)
			}
			executor := fetchTestExecutor(count<<20, count, func(ctx context.Context, index int64) ([]byte, error) {
				first := index == 1
				if mode == "refill_rejection" && !first {
					return bytes.Repeat([]byte{'x'}, 1<<20), nil
				}
				if mode == "submit_rejection" || mode == "refill_rejection" || !first {
					close(entered)
					<-ctx.Done()
					close(canceled)
					<-retire // An executor's cleanup can finish after cancellation arrives.
					return nil, ctx.Err()
				}
				<-entered
				if mode != "chunk_failure" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return nil, moerr.NewInternalErrorNoCtx("permission denied")
			})
			submitted := &rejectingChunkSubmission{GetChunkWorker: worker, before: func(j Job) error {
				if chunk, ok := j.(*GetChunkJob); ok && (chunk.chunkIndex == 2 && mode == "submit_rejection" || chunk.chunkIndex == count && mode == "refill_rejection") {
					<-entered
					return moerr.NewInternalErrorNoCtx("submission rejected")
				}
				return nil
			}}
			id := objectio.NewObjectid()
			stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
			require.NoError(t, objectio.SetObjectStatsSize(stats, uint32(count<<20)))
			var result *FilterObjectJobResult
			go func() {
				defer close(finished)
				job := NewFilterObjectJob(ctx, stats.Marshal(), types.TS{}, executor, true, nil, nil, submitted, nil, "acc", "pub", nil, nil, NewAObjectMap(), nil)
				job.Execute()
				result = job.WaitDone().(*FilterObjectJobResult)
			}()
			t.Cleanup(func() { cancel(); unblock(); <-finished })
			wait := func(ch <-chan struct{}) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(10 * time.Second):
					t.Fatal("lifecycle transition did not complete")
				}
			}
			select {
			case <-entered:
			case <-finished:
				t.Fatalf("job finished before download: %v", result.Err)
			case <-time.After(10 * time.Second):
				t.Fatal("download did not start")
			}
			stopped := make(chan struct{})
			switch mode {
			case "caller_cancel":
				cancel()
			case "worker_stop":
				go func() { worker.Stop(); close(stopped) }()
			}
			select {
			case <-canceled:
			case <-finished:
				t.Fatal("fetch returned before accepted chunks retired")
			case <-time.After(10 * time.Second):
				t.Fatal("accepted chunks were not canceled")
			}
			require.Less(t, owner.WorkingSetHeadroom().Available, total, "retain the fetch promise during executor cleanup")
			ticket, ok, err := owner.TryReserveWorkingSet(context.Background(), rscthrottler.WorkingSetSpec{UnitBytes: 1, Inputs: []rscthrottler.InputSlotSpec{{Count: 1, Bytes: total - (256 << 10)}}})
			if ticket != nil {
				ticket.Close()
			}
			require.NoError(t, err)
			require.False(t, ok, "another job cannot reuse credit for a live chunk")
			select {
			case <-finished:
				t.Fatal("published a terminal result before chunk cleanup")
			default:
			}
			unblock()
			wait(finished)
			if mode == "worker_stop" {
				wait(stopped)
			}
			require.Error(t, result.Err)
			if mode == "caller_cancel" || mode == "worker_stop" {
				require.ErrorIs(t, result.Err, context.Canceled)
			}
			require.Equal(t, total, owner.WorkingSetHeadroom().Available)
			require.Equal(t, objectio.ObjectStats{}, result.DownstreamStats)
		})
	}
}

type queuedSubmitProbe struct {
	once   sync.Once
	called chan struct{}
}

func (j *queuedSubmitProbe) GetType() int8 {
	j.once.Do(func() { close(j.called) })
	return JobTypeGetChunk
}
func (j *queuedSubmitProbe) Execute()      {}
func (j *queuedSubmitProbe) WaitDone() any { return nil }

func TestChunkWorkerStopDrainsQueueAndBlockedSubmission(t *testing.T) {
	w := &getChunkWorker{jobChan: make(chan Job, 1)}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.Run()
	t.Cleanup(w.Stop)
	entered := make(chan struct{}, GetGetChunkWorkerThread())
	executor := &mockSQLExecutorForJob{execFn: func(ctx context.Context, _ *ActiveRoutine, _ uint32, _ string, _, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}}
	jobs := make([]*GetChunkJob, GetGetChunkWorkerThread()+1)
	for i := range jobs {
		jobs[i] = NewGetChunkJob(context.Background(), executor, "queue", int64(i+1), "acc", "pub")
		require.NoError(t, w.SubmitGetChunk(jobs[i]))
		if i < GetGetChunkWorkerThread() {
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("worker did not enter executor")
			}
		}
	}
	extra := &queuedSubmitProbe{called: make(chan struct{})}
	submitted := make(chan struct{})
	go func() { defer close(submitted); _ = w.SubmitGetChunk(extra) }()
	select {
	case <-extra.called:
	case <-time.After(10 * time.Second):
		t.Fatal("submission did not enter its gate")
	}
	stopped := make(chan struct{})
	go func() { w.Stop(); close(stopped) }()
	for _, done := range []<-chan struct{}{submitted, stopped} {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("stop/submission deadlock")
		}
	}
	for _, j := range jobs {
		require.ErrorIs(t, j.WaitDone().(*GetChunkJobResult).Err, context.Canceled)
	}
	require.Error(t, w.SubmitGetChunk(&mockJob{}))
}

func TestObjectReplyClosesAllQueryResources(t *testing.T) {
	for _, phase := range []string{"execute_error", "scan_error", "close_error", "success"} {
		t.Run(phase, func(t *testing.T) {
			canceled := false
			closed := 0
			scanner := &mockScanner{nextFn: func() bool { return true }, scanFn: func(dest ...interface{}) error {
				if phase == "scan_error" {
					return moerr.NewInternalErrorNoCtx("bad scan")
				}
				*dest[0].(*[]byte) = []byte("reply")
				*dest[1].(*int64) = 5
				*dest[2].(*int64) = 1
				*dest[3].(*int64) = 1
				*dest[4].(*bool) = true
				return nil
			}, closeFn: func() error {
				require.Equal(t, phase != "success" && phase != "close_error", canceled)
				closed++
				if phase == "close_error" {
					return moerr.NewInternalErrorNoCtx("close failed")
				}
				return nil
			}, errFn: func() error { return nil }}
			executor := &mockSQLExecutorForJob{execFn: func(context.Context, *ActiveRoutine, uint32, string, bool, bool, time.Duration) (*Result, context.CancelFunc, error) {
				var err error
				if phase == "execute_error" {
					err = moerr.NewInternalErrorNoCtx("permission denied")
				}
				return newMockResult(scanner), func() { canceled = true }, err
			}}
			job := NewGetChunkJob(context.Background(), executor, "object", 1, "acc", "pub")
			job.Execute()
			result := job.WaitDone().(*GetChunkJobResult)
			if phase == "success" {
				require.NoError(t, result.Err)
				require.Equal(t, []byte("reply"), result.ChunkData)
			} else {
				require.Error(t, result.Err)
			}
			require.True(t, canceled)
			require.Equal(t, 1, closed)
		})
	}
}

// The real jobs, query cleanup and worker run with small protocol geometry.
func fetchTestExecutor(size, count int64, data func(context.Context, int64) ([]byte, error)) *mockSQLExecutorForJob {
	return &mockSQLExecutorForJob{execFn: func(ctx context.Context, _ *ActiveRoutine, _ uint32, query string, _, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
		index, err := strconv.ParseInt(query[strings.LastIndexByte(query, ' ')+1:], 10, 64)
		if err != nil {
			return nil, nil, err
		}
		var payload []byte
		if index != 0 {
			payload, err = data(ctx, index)
			if err != nil {
				return nil, nil, err
			}
		}
		return newMockResult(&mockScanner{nextFn: func() bool { return true }, scanFn: func(dest ...interface{}) error {
			*dest[0].(*[]byte) = payload
			*dest[1].(*int64) = size
			*dest[2].(*int64) = index
			*dest[3].(*int64) = count
			*dest[4].(*bool) = index == count
			return nil
		}, closeFn: func() error { return nil }, errFn: func() error { return nil }}), noopCancel, nil
	}}
}

func TestFetchChunkOrderAndProgress(t *testing.T) {
	worker := NewGetChunkWorker()
	t.Cleanup(worker.Stop)
	limit := min(GetGetChunkWorkerThread(), cap(getOrCreateChunkSemaphore()))
	count, width := int64(limit+1), int64(3)
	size := count*width - 1
	first, next := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	ctx, cancel := context.WithCancel(fetchTestContext(t, size))
	t.Cleanup(func() { cancel(); unblock() })
	executor := fetchTestExecutor(size, count, func(ctx context.Context, index int64) ([]byte, error) {
		if index == 1 {
			close(first)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if index == count {
			close(next)
		}
		n := min(width, size-(index-1)*width)
		return bytes.Repeat([]byte{byte(index)}, int(n)), nil
	})
	var data []byte
	var err error
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		data, err = fetchObjectChunks(ctx, executor, "obj", worker, "acc", "pub", width)
	}()
	t.Cleanup(func() { cancel(); unblock(); <-finished })
	for _, phase := range []<-chan struct{}{first, next} {
		select {
		case <-phase:
		case <-time.After(10 * time.Second):
			t.Fatal("a slow first chunk prevented later work from starting")
		}
	}
	unblock()
	<-finished
	require.NoError(t, err)
	require.Len(t, data, int(size))
	require.Equal(t, len(data), cap(data), "one final buffer")
	for i, value := range data {
		require.Equal(t, byte(int64(i)/width+1), value)
	}
}

func TestFetchChunkGeometry(t *testing.T) {
	for _, tc := range []struct {
		size, width, count int64
		valid              bool
	}{{0, 3, 1, true}, {1, 3, 1, true}, {3, 3, 1, true}, {4, 3, 2, true},
		{math.MaxUint32, math.MaxUint32, 1, true}, {math.MaxUint32, math.MaxInt64, 1, true},
		{-1, 3, 0, false}, {math.MaxUint32 + 1, 3, 0, false}, {math.MaxInt64, 3, 0, false}, {1, 0, 0, false}} {
		count, err := objectChunkCount(tc.size, tc.width)
		if tc.valid {
			require.NoError(t, err)
			require.Equal(t, tc.count, count)
		} else {
			require.Error(t, err)
		}
	}
	for _, mode := range []string{"empty", "short", "long", "bad_count", "bad_size", "sync"} {
		t.Run(mode, func(t *testing.T) {
			size, count := int64(5), int64(2)
			switch mode {
			case "empty":
				size, count = 0, 1
			case "bad_count":
				count = 3
			case "bad_size":
				size = math.MaxUint32 + 1
			}
			executor := fetchTestExecutor(size, count, func(_ context.Context, index int64) ([]byte, error) {
				require.NotEqual(t, "bad_count", mode, "reject metadata before submitting data")
				require.NotEqual(t, "bad_size", mode, "reject metadata before allocating")
				n := min(int64(3), size-(index-1)*3)
				if mode == "short" {
					n--
				}
				if mode == "long" {
					n++
				}
				return bytes.Repeat([]byte{byte(index)}, int(n)), nil
			})
			data, err := fetchObjectChunks(fetchTestContext(t, 5), executor, "obj", nil, "acc", "pub", 3)
			if mode == "empty" || mode == "sync" {
				require.NoError(t, err)
				require.Len(t, data, int(size))
			} else {
				require.Error(t, err)
				require.Nil(t, data)
			}
		})
	}
}

func TestNonAppendableWriteSubmissionFailureTerminates(t *testing.T) {
	original := GetObjectFromUpstreamWithWorker
	t.Cleanup(func() { GetObjectFromUpstreamWithWorker = original })
	GetObjectFromUpstreamWithWorker = func(context.Context, SQLExecutor, string, GetChunkWorker, string, string) ([]byte, error) {
		return []byte("raw"), nil
	}
	writer := &writeObjectWorker{simpleJobWorker: &simpleJobWorker{name: "closed"}}
	writer.closed.Store(true)
	_, err := filterNonAppendableObject(context.Background(), new(objectio.ObjectStats), types.TS{}, nil, nil, false, nil, nil, writer, "acc", "pub", nil, nil, nil, nil)
	require.ErrorContains(t, err, "closed")
}

func TestFetchSizeUsesExistingAdmission(t *testing.T) {
	const total = int64(64 << 20)
	ctx := context.Background()
	owner := rscthrottler.NewMemThrottler(t.Name(), 1, rscthrottler.WithConstLimit(total), rscthrottler.WithMemorySampleForTesting(total, 0)).(rscthrottler.WorkingSetThrottler)
	ws, err := owner.ReserveWorkingSetCandidatesUntilFit(ctx, []rscthrottler.WorkingSetSpec{{UnitBytes: 1, Inputs: []rscthrottler.InputSlotSpec{{Count: 1, Bytes: 2 << 20}}}})
	require.NoError(t, err)
	t.Cleanup(ws.Close)
	lease, err := ws.AcquireInput(ctx, 0)
	require.NoError(t, err)
	t.Cleanup(lease.Release)
	other, ok, err := owner.TryReserveWorkingSet(ctx, rscthrottler.WorkingSetSpec{UnitBytes: 1, Inputs: []rscthrottler.InputSlotSpec{{Count: 1, Bytes: owner.WorkingSetHeadroom().Available - 1024}}})
	require.NoError(t, err)
	require.True(t, ok)
	t.Cleanup(other.Close)
	ctx = context.WithValue(ctx, publicationFetchMemoryKey{}, lease)
	fetches := 0
	executor := fetchTestExecutor(1<<20, 1, func(context.Context, int64) ([]byte, error) {
		fetches++
		return bytes.Repeat([]byte{'x'}, 1<<20), nil
	})
	data, err := fetchObjectChunks(ctx, executor, "obj", nil, "acc", "pub", 1<<20)
	var growth *objectMemoryGrowth
	require.ErrorAs(t, err, &growth)
	require.Nil(t, data)
	require.Zero(t, fetches, "grow before allocating/fetching the source")
	require.Equal(t, int64(2<<20), lease.Capacity())
	other.Close()
	data, err = fetchObjectChunks(ctx, executor, "obj", nil, "acc", "pub", 1<<20)
	require.NoError(t, err)
	require.Len(t, data, 1<<20)
	require.Greater(t, lease.Capacity(), int64(2<<20))
	require.Equal(t, 1, fetches)
	lease.Release()
	ws.Close()
	require.Equal(t, total, owner.WorkingSetHeadroom().Available)
}

func BenchmarkFetchObjectChunks(b *testing.B) {
	const width, count = int64(1 << 20), int64(10)
	payload := bytes.Repeat([]byte{'x'}, int(width))
	executor := fetchTestExecutor(width*count, count, func(context.Context, int64) ([]byte, error) { return payload, nil })
	ctx := fetchTestContext(b, width*count)
	b.ReportAllocs()
	b.SetBytes(width * count)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := fetchObjectChunks(ctx, executor, "obj", nil, "acc", "pub", width)
		if err != nil || len(data) != int(width*count) {
			b.Fatalf("fetch: bytes=%d err=%v", len(data), err)
		}
	}
}
