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

package iscp

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJobStatusesForIterationErrorPreservesStages(t *testing.T) {
	iter := &IterationContext{
		jobNames: []string{"initializing", "running"},
		stages:   []int8{JobStage_Init, JobStage_Running},
	}

	statuses := jobStatusesForIterationError(iter, errors.New("iteration failed"))

	require.Len(t, statuses, 2)
	require.Equal(t, int8(JobStage_Init), statuses[0].Stage)
	require.Equal(t, int8(JobStage_Running), statuses[1].Stage)
	require.Equal(t, "iteration failed", statuses[0].ErrorMsg)
	require.Equal(t, "iteration failed", statuses[1].ErrorMsg)
}

func TestSupersededIterationStopsRetry(t *testing.T) {
	attempts := 0
	err := retryISCPTaskIteration(context.Background(), func() error {
		attempts++
		return newISCPStatusCASLostError("test", "job", 1, 0)
	})

	require.NoError(t, err)
	require.Equal(t, 1, attempts)
	require.False(t, isSupersededIteration(errors.New("temporary failure")))
}

func TestWorkerStopImmediatelyAfterConstruction(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)

	for range 32 {
		worker := NewWorker(nil, "", nil, nil, nil)
		worker.Stop()
		runtime.Gosched()
	}
}

func TestWorkerStopIsIdempotent(t *testing.T) {
	worker := NewWorker(nil, "", nil, nil, nil)

	var stops sync.WaitGroup
	for range 8 {
		stops.Add(1)
		go func() {
			defer stops.Done()
			worker.Stop()
		}()
	}
	stops.Wait()
}

func TestWorkerRejectsInvalidOrClosedSubmissions(t *testing.T) {
	worker := NewWorker(nil, "", nil, nil, nil)
	require.Error(t, worker.Submit(nil))

	worker.Stop()
	require.Error(t, worker.Submit(&IterationContext{}))
}

func TestWorkerCancellationUnblocksSubmit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tasks := make(chan *IterationContext, 1)
	tasks <- &IterationContext{}
	w := &worker{
		taskChan: tasks,
		ctx:      ctx,
		cancel:   cancel,
	}

	cancel()
	require.Error(t, w.Submit(&IterationContext{}))
}
