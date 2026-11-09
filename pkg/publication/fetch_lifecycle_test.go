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
	"context"
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
	for _, mode := range []string{"chunk_failure", "caller_cancel", "worker_stop", "submit_rejection"} {
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
			executor := &mockSQLExecutorForJob{execFn: func(ctx context.Context, _ *ActiveRoutine, _ uint32, query string, _, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
				if query == PublicationSQLBuilder.GetObjectSQL("acc", "pub", "obj", 0) {
					return newMockResult(&mockScanner{
						nextFn: func() bool { return true },
						scanFn: func(dest ...interface{}) error {
							*dest[0].(*[]byte) = nil
							*dest[1].(*int64) = 2 << 20
							*dest[2].(*int64) = 0
							*dest[3].(*int64) = 2
							*dest[4].(*bool) = false
							return nil
						},
						closeFn: func() error { return nil }, errFn: func() error { return nil },
					}), noopCancel, nil
				}
				first := query == PublicationSQLBuilder.GetObjectSQL("acc", "pub", "obj", 1)
				if mode == "submit_rejection" || !first {
					close(entered)
					<-ctx.Done()
					close(canceled)
					<-retire // An executor's cleanup can finish after cancellation arrives.
					return nil, nil, ctx.Err()
				}
				<-entered
				if mode != "chunk_failure" {
					<-ctx.Done()
					return nil, nil, ctx.Err()
				}
				return nil, nil, moerr.NewInternalErrorNoCtx("permission denied")
			}}
			submitted := &rejectingChunkSubmission{GetChunkWorker: worker, before: func(j Job) error {
				if chunk, ok := j.(*GetChunkJob); ok && chunk.chunkIndex == 2 && mode == "submit_rejection" {
					<-entered
					return moerr.NewInternalErrorNoCtx("submission rejected")
				}
				return nil
			}}
			id := objectio.NewObjectid()
			stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
			// Match the executor's query name while retaining an actual fetch admission.
			name := stats.ObjectName().String()
			original := executor.execFn
			executor.execFn = func(ctx context.Context, ar *ActiveRoutine, account uint32, query string, txn, retry bool, limit time.Duration) (*Result, context.CancelFunc, error) {
				return original(ctx, ar, account, strings.ReplaceAll(query, name, "obj"), txn, retry, limit)
			}
			require.NoError(t, objectio.SetObjectStatsSize(stats, 2<<20))
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
	for _, phase := range []string{"execute_error", "scan_error", "success"} {
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
				require.True(t, canceled, "cancel network work before closing its reader")
				closed++
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

func TestGetObjectChunkWindowsPreserveOrder(t *testing.T) {
	worker := NewGetChunkWorker()
	defer worker.Stop()
	count := int64(GetGetChunkWorkerThread() + 1)
	executor := &mockSQLExecutorForJob{execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, query string, _, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
		var index int64
		for ; index <= count; index++ {
			if query == PublicationSQLBuilder.GetObjectSQL("acc", "pub", "obj", index) {
				break
			}
		}
		require.LessOrEqual(t, index, count)
		return newMockResult(&mockScanner{nextFn: func() bool { return true }, scanFn: func(dest ...interface{}) error {
			if index != 0 {
				*dest[0].(*[]byte) = []byte{byte(index)}
			}
			*dest[1].(*int64) = count
			*dest[2].(*int64) = index
			*dest[3].(*int64) = count
			*dest[4].(*bool) = index == count
			return nil
		}, closeFn: func() error { return nil }, errFn: func() error { return nil }}), noopCancel, nil
	}}
	data, err := GetObjectFromUpstreamWithWorker(context.Background(), executor, "obj", worker, "acc", "pub")
	require.NoError(t, err)
	require.Len(t, data, int(count))
	for i, value := range data {
		require.Equal(t, byte(i+1), value)
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
