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
	"fmt"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/txn/client"

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
	require.Equal(t, uint64(atomicInitLifecycleVersion), statuses[0].LifecycleVersion)
	require.Zero(t, statuses[1].LifecycleVersion)
	require.Equal(t, "iteration failed", statuses[0].ErrorMsg)
	require.Equal(t, "iteration failed", statuses[1].ErrorMsg)
}

func TestSupersededIterationStopsRetry(t *testing.T) {
	cas := newISCPStatusCASLostError("test", "job", 1, 0)
	cleanup := errors.New("rollback failed")
	for _, tc := range []struct {
		name       string
		err        error
		superseded bool
	}{
		{"direct", cas, true},
		{"wrapped", fmt.Errorf("iteration: %w", cas), true},
		{"joined CAS only", errors.Join(cas, cas), true},
		{"rollback failed", errors.Join(cas, cleanup), false},
		{"nested rollback failed", fmt.Errorf("iteration: %w", errors.Join(cas, cleanup)), false},
		{"multiple wrapped causes", fmt.Errorf("%w; %w", cas, cleanup), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				attempts := 0
				start := time.Now()
				err := retryISCPTaskIteration(context.Background(), func() error {
					attempts++
					return tc.err
				})
				require.Equal(t, 1, attempts)
				require.Zero(t, time.Since(start))
				require.Equal(t, tc.superseded, isSupersededIteration(tc.err))
				require.Equal(t, tc.superseded, shouldReplayISCPLog(tc.err))
				if tc.superseded {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, cleanup)
					require.ErrorIs(t, err, cas)
				}
			})
		})
	}
	require.False(t, isSupersededIteration(nil))
	require.False(t, isSupersededIteration(cleanup))
}

func TestInitCASLossPreservesRollbackFailureThroughWorkerRetry(t *testing.T) {
	rollbackErr := errors.New("rollback cleanup failed")
	txn := &iscpTxnForTest{rollbackErr: rollbackErr}
	attempts := 0
	err := retryISCPTaskIteration(context.Background(), func() error {
		attempts++
		return runInitSQLTransaction(context.Background(), txn,
			func(context.Context, client.TxnOperator) error { return nil },
			func(context.Context, client.TxnOperator) error {
				return newISCPStatusCASLostError("init", "job", 1, 0)
			})
	})
	require.Equal(t, 1, attempts)
	require.True(t, txn.rolledBack)
	require.False(t, txn.committed)
	require.ErrorIs(t, err, rollbackErr)
	require.ErrorIs(t, err, errISCPStatusCASLost)
}

func TestIterationRetryStillRetriesTransientFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempts := 0
		start := time.Now()
		err := retryISCPTaskIteration(context.Background(), func() error {
			attempts++
			if attempts < 3 {
				return errors.New("temporary failure")
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 3, attempts)
		require.Equal(t, 3*DefaultRetryInterval, time.Since(start))
	})
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
