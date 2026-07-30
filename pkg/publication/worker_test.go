// Copyright 2021 Matrix Origin
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

package publication

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkerStopImmediatelyAfterConstruction(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)

	w := NewWorker("", nil, nil, nil, nil)
	w.Stop()
	runtime.Gosched()
}

func TestStoppedWorkersCompleteRejectedJobs(t *testing.T) {
	tests := []struct {
		name   string
		job    Job
		submit func(Job) error
	}{
		{
			name: "filter object",
			job:  NewFilterObjectJob(context.Background(), nil, types.TS{}, nil, false, nil, nil, nil, nil, "", "", nil, nil, nil, nil),
			submit: func(job Job) error {
				worker := NewFilterObjectWorker()
				worker.Stop()
				return worker.SubmitFilterObject(job)
			},
		},
		{
			name: "get chunk",
			job:  NewGetChunkJob(context.Background(), nil, "object", 1, "", ""),
			submit: func(job Job) error {
				worker := NewGetChunkWorker()
				worker.Stop()
				return worker.SubmitGetChunk(job)
			},
		},
		{
			name: "write object",
			job:  NewWriteObjectJob(context.Background(), nil, "object", nil, nil, nil),
			submit: func(job Job) error {
				worker := NewWriteObjectWorker()
				worker.Stop()
				return worker.SubmitWriteObject(job)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Error(t, test.submit(test.job))
			assertJobFailedPromptly(t, test.job)
		})
	}
}

func TestWorkerStopCompletesQueuedJobs(t *testing.T) {
	previousStats := globalJobStats
	globalJobStats = &JobStats{}
	defer func() { globalJobStats = previousStats }()

	filterCtx, filterCancel := context.WithCancel(context.Background())
	filterWorker := &filterObjectWorker{
		jobChan: make(chan Job, 1),
		ctx:     filterCtx,
		cancel:  filterCancel,
	}
	chunkCtx, chunkCancel := context.WithCancel(context.Background())
	chunkWorker := &getChunkWorker{
		jobChan: make(chan Job, 1),
		ctx:     chunkCtx,
		cancel:  chunkCancel,
	}
	writeCtx, writeCancel := context.WithCancel(context.Background())
	writeWorker := &simpleJobWorker{
		name:              "WriteObjectWorker",
		jobChan:           make(chan Job, 1),
		ctx:               writeCtx,
		cancel:            writeCancel,
		onPending:         func() {},
		onPendingCanceled: func() {},
	}

	tests := []struct {
		name   string
		job    Job
		submit func(Job) error
		stop   func()
	}{
		{
			name:   "filter object",
			job:    NewFilterObjectJob(context.Background(), nil, types.TS{}, nil, false, nil, nil, nil, nil, "", "", nil, nil, nil, nil),
			submit: filterWorker.SubmitFilterObject,
			stop:   filterWorker.Stop,
		},
		{
			name:   "get chunk",
			job:    NewGetChunkJob(context.Background(), nil, "object", 1, "", ""),
			submit: chunkWorker.SubmitGetChunk,
			stop:   chunkWorker.Stop,
		},
		{
			name:   "write object",
			job:    NewWriteObjectJob(context.Background(), nil, "object", nil, nil, nil),
			submit: writeWorker.submit,
			stop:   writeWorker.stop,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, test.submit(test.job))
			test.stop()
			assertJobFailedPromptly(t, test.job)
		})
	}

	require.Zero(t, globalJobStats.FilterObjectPending.Load())
	require.Zero(t, globalJobStats.GetChunkPending.Load())
}

func TestJobCompletionIsExactlyOnce(t *testing.T) {
	firstErr := errors.New("first")
	job := NewWriteObjectJob(context.Background(), nil, "object", nil, nil, nil)
	job.Fail(firstErr)
	job.Fail(errors.New("second"))

	result := job.WaitDone().(*WriteObjectJobResult)
	require.ErrorIs(t, result.Err, firstErr)
	select {
	case <-job.result:
		t.Fatal("job published more than one terminal result")
	default:
	}
}

func TestGetMetaReturnsWorkerRejection(t *testing.T) {
	worker := NewGetChunkWorker()
	worker.Stop()

	_, err := getMetaWithRetry(context.Background(), nil, "object", worker, "", "")
	require.Error(t, err)
}

func assertJobFailedPromptly(t *testing.T, job Job) {
	t.Helper()
	result := make(chan any, 1)
	go func() { result <- job.WaitDone() }()
	select {
	case value := <-result:
		switch value := value.(type) {
		case *FilterObjectJobResult:
			require.Error(t, value.Err)
		case *GetChunkJobResult:
			require.Error(t, value.Err)
		case *WriteObjectJobResult:
			require.Error(t, value.Err)
		default:
			t.Fatalf("unexpected job result type %T", value)
		}
	case <-time.After(time.Second):
		t.Fatal("job did not reach a terminal result")
	}
}

func TestWorkerStopIsConcurrentAndIdempotent(t *testing.T) {
	w := NewWorker("", nil, nil, nil, nil)
	var stops sync.WaitGroup
	for range 8 {
		stops.Add(1)
		go func() {
			defer stops.Done()
			w.Stop()
		}()
	}
	stops.Wait()
	require.Error(t, w.Submit("task-after-stop", 1, 0))
}

func TestWorkerPoolsStopIdempotently(t *testing.T) {
	workers := []interface{ Stop() }{
		NewFilterObjectWorker(),
		NewGetChunkWorker(),
		NewWriteObjectWorker(),
	}
	for _, w := range workers {
		w.Stop()
		w.Stop()
	}
}

func TestWorkerPoolsDrainPendingJobsOnStop(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)
	previousStats := globalJobStats
	globalJobStats = &JobStats{}
	defer func() { globalJobStats = previousStats }()

	filterWorker := NewFilterObjectWorker()
	require.NoError(t, filterWorker.SubmitFilterObject(&mockJob{}))
	filterWorker.Stop()
	require.Zero(t, globalJobStats.FilterObjectPending.Load())

	getChunkWorker := NewGetChunkWorker()
	require.NoError(t, getChunkWorker.SubmitGetChunk(&mockJob{}))
	getChunkWorker.Stop()
	require.Zero(t, globalJobStats.GetChunkPending.Load())

	writeObjectWorker := NewWriteObjectWorker()
	require.NoError(t, writeObjectWorker.SubmitWriteObject(&mockJob{}))
	writeObjectWorker.Stop()
	require.Zero(t, globalJobStats.WriteObjectPending.Load())
}

func TestSimpleWorkerCancellationUnblocksSubmitAndRollsBackPending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	jobs := make(chan Job, 1)
	jobs <- &mockJob{}
	var pending atomic.Int64
	w := &simpleJobWorker{
		name:              "test worker",
		jobChan:           jobs,
		ctx:               ctx,
		cancel:            cancel,
		onPending:         func() { pending.Add(1) },
		onPendingCanceled: func() { pending.Add(-1) },
	}

	cancel()
	require.Error(t, w.submit(&mockJob{}))
	require.Zero(t, pending.Load())
}

func TestWorkerStopClearsAndSealsSyncProtection(t *testing.T) {
	w := &worker{
		syncProtectionJobs: make(map[string]*syncProtectionEntry),
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.RegisterSyncProtection("before-stop", 1)

	w.Stop()
	require.Zero(t, w.GetSyncProtectionTTL("before-stop"))

	w.RegisterSyncProtection("after-stop", 2)
	require.Zero(t, w.GetSyncProtectionTTL("after-stop"))
}

func TestGetJobStats(t *testing.T) {
	stats := GetJobStats()
	require.NotNil(t, stats)
	// Should return the same global instance
	assert.Equal(t, stats, GetJobStats())
}

func TestJobStats_FilterObjectCounters(t *testing.T) {
	s := &JobStats{}
	s.IncrementFilterObjectPending()
	assert.Equal(t, int64(1), s.FilterObjectPending.Load())

	s.IncrementFilterObjectRunning()
	assert.Equal(t, int64(1), s.FilterObjectRunning.Load())

	s.DecrementFilterObjectRunning()
	assert.Equal(t, int64(0), s.FilterObjectRunning.Load())
}

func TestJobStats_GetChunkCounters(t *testing.T) {
	s := &JobStats{}
	s.IncrementGetChunkPending()
	assert.Equal(t, int64(1), s.GetChunkPending.Load())

	s.IncrementGetChunkRunning()
	assert.Equal(t, int64(1), s.GetChunkRunning.Load())

	s.DecrementGetChunkRunning()
	assert.Equal(t, int64(0), s.GetChunkRunning.Load())
}

func TestJobStats_GetMetaCounters(t *testing.T) {
	s := &JobStats{}
	s.IncrementGetMetaPending()
	assert.Equal(t, int64(1), s.GetMetaPending.Load())

	s.IncrementGetMetaRunning()
	assert.Equal(t, int64(1), s.GetMetaRunning.Load())

	s.DecrementGetMetaRunning()
	assert.Equal(t, int64(0), s.GetMetaRunning.Load())
}

func TestJobStats_WriteObjectCounters(t *testing.T) {
	s := &JobStats{}
	s.IncrementWriteObjectPending()
	assert.Equal(t, int64(1), s.WriteObjectPending.Load())

	s.IncrementWriteObjectRunning()
	assert.Equal(t, int64(1), s.WriteObjectRunning.Load())

	s.DecrementWriteObjectRunning()
	assert.Equal(t, int64(0), s.WriteObjectRunning.Load())
}

func TestJobStats_RecordGetChunkDuration(t *testing.T) {
	s := &JobStats{}

	s.RecordGetChunkDuration("obj1", 0, 100*time.Millisecond)
	s.RecordGetChunkDuration("obj2", 1, 300*time.Millisecond)
	s.RecordGetChunkDuration("obj3", 2, 200*time.Millisecond)
	s.RecordGetChunkDuration("obj4", 3, 50*time.Millisecond)

	top := s.GetTopGetChunkDurations()
	require.Len(t, top, 3)
	// Should be sorted descending
	assert.Equal(t, 300*time.Millisecond, top[0].Duration)
	assert.Equal(t, "obj2", top[0].ObjectName)
	assert.Equal(t, 200*time.Millisecond, top[1].Duration)
	assert.Equal(t, 100*time.Millisecond, top[2].Duration)
}

func TestJobStats_RecordWriteObjectDuration(t *testing.T) {
	s := &JobStats{}

	s.RecordWriteObjectDuration("obj1", 1024, 100*time.Millisecond)
	s.RecordWriteObjectDuration("obj2", 2048, 300*time.Millisecond)
	s.RecordWriteObjectDuration("obj3", 512, 200*time.Millisecond)
	s.RecordWriteObjectDuration("obj4", 4096, 50*time.Millisecond)

	top := s.GetTopWriteObjectDurations()
	require.Len(t, top, 3)
	assert.Equal(t, 300*time.Millisecond, top[0].Duration)
	assert.Equal(t, "obj2", top[0].ObjectName)
	assert.Equal(t, int64(2048), top[0].Size)
}

func TestJobStats_ResetTopGetChunkDurations(t *testing.T) {
	s := &JobStats{}
	s.RecordGetChunkDuration("obj1", 0, 100*time.Millisecond)
	require.Len(t, s.GetTopGetChunkDurations(), 1)

	s.ResetTopGetChunkDurations()
	assert.Empty(t, s.GetTopGetChunkDurations())
}

func TestJobStats_ResetTopWriteObjectDurations(t *testing.T) {
	s := &JobStats{}
	s.RecordWriteObjectDuration("obj1", 1024, 100*time.Millisecond)
	require.Len(t, s.GetTopWriteObjectDurations(), 1)

	s.ResetTopWriteObjectDurations()
	assert.Empty(t, s.GetTopWriteObjectDurations())
}

func TestJobStats_GetTopGetChunkDurations_Empty(t *testing.T) {
	s := &JobStats{}
	assert.Empty(t, s.GetTopGetChunkDurations())
}

func TestJobStats_GetTopWriteObjectDurations_Empty(t *testing.T) {
	s := &JobStats{}
	assert.Empty(t, s.GetTopWriteObjectDurations())
}

// ---- worker.Submit closed error ----

func TestWorker_Submit_Closed(t *testing.T) {
	w := &worker{
		taskChan: make(chan *TaskContext, 10),
	}
	w.closed.Store(true)
	err := w.Submit("task1", 1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// ---- JobStats top durations printing branches ----

func TestJobStats_TopDurations_NonEmpty(t *testing.T) {
	s := &JobStats{}
	s.RecordGetChunkDuration("obj1", 0, 100*time.Millisecond)
	s.RecordGetChunkDuration("obj2", 1, 200*time.Millisecond)
	s.RecordGetChunkDuration("obj3", 2, 300*time.Millisecond)

	top := s.GetTopGetChunkDurations()
	require.Len(t, top, 3)
	assert.Equal(t, "obj3", top[0].ObjectName)

	s.RecordWriteObjectDuration("w1", 1024, 50*time.Millisecond)
	s.RecordWriteObjectDuration("w2", 2048, 150*time.Millisecond)

	topW := s.GetTopWriteObjectDurations()
	require.Len(t, topW, 2)
	assert.Equal(t, "w2", topW[0].ObjectName)
}
