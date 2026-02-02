// Copyright 2024 Matrix Origin
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

package tasks

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParallelJobScheduler_ScheduleAfterStop(t *testing.T) {
	// Test that scheduling a job after Stop() returns ErrSchedulerClosed
	scheduler := NewParallelJobScheduler(4)

	// Create a simple job
	job := &Job{}
	job.Init(context.Background(), "test-job", JTAny, func(ctx context.Context) *JobResult {
		return &JobResult{Res: "done"}
	})

	// Schedule should work before stop
	err := scheduler.Schedule(job)
	require.NoError(t, err)

	// Wait for job to complete
	result := job.WaitDone()
	assert.Equal(t, "done", result.Res)

	// Stop the scheduler
	scheduler.Stop()

	// Create another job
	job2 := &Job{}
	job2.Init(context.Background(), "test-job-2", JTAny, func(ctx context.Context) *JobResult {
		return &JobResult{Res: "should not run"}
	})

	// Schedule should fail after stop
	err = scheduler.Schedule(job2)
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrSchedulerClosed))
}

func TestParallelJobScheduler_ConcurrentScheduleAndStop(t *testing.T) {
	// Test concurrent scheduling and stopping doesn't cause panic
	scheduler := NewParallelJobScheduler(4)

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Start multiple goroutines that continuously schedule jobs
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					job := &Job{}
					job.Init(context.Background(), "concurrent-job", JTAny, func(ctx context.Context) *JobResult {
						time.Sleep(time.Millisecond)
						return &JobResult{Res: "done"}
					})
					err := scheduler.Schedule(job)
					if err != nil {
						// After stop, we expect ErrSchedulerClosed
						if moerr.IsMoErrCode(err, moerr.ErrSchedulerClosed) {
							return
						}
					}
				}
			}
		}()
	}

	// Let some jobs run
	time.Sleep(50 * time.Millisecond)

	// Stop the scheduler while jobs are being scheduled
	scheduler.Stop()

	// Signal goroutines to stop
	close(stopCh)

	// Wait for all goroutines to finish - should not panic
	wg.Wait()
}

func TestParallelJobScheduler_StopIdempotent(t *testing.T) {
	// Test that calling Stop() multiple times doesn't panic
	scheduler := NewParallelJobScheduler(4)

	// Stop multiple times should not panic
	require.NotPanics(t, func() {
		scheduler.Stop()
		scheduler.Stop()
		scheduler.Stop()
	})

	// Schedule after multiple stops should still return error
	job := &Job{}
	job.Init(context.Background(), "test-job", JTAny, func(ctx context.Context) *JobResult {
		return &JobResult{Res: "done"}
	})

	err := scheduler.Schedule(job)
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrSchedulerClosed))
}

func TestJob_BasicOperations(t *testing.T) {
	// Test basic job operations
	executed := false
	job := &Job{}
	job.Init(context.Background(), "basic-job", JTAny, func(ctx context.Context) *JobResult {
		executed = true
		return &JobResult{Res: "success"}
	})

	assert.Equal(t, "basic-job", job.ID())
	assert.Equal(t, JTAny, job.Type())
	assert.Contains(t, job.String(), "basic-job")

	// Run the job
	job.Run()

	assert.True(t, executed)
	result := job.GetResult()
	assert.Equal(t, "success", result.Res)
	assert.Nil(t, result.Err)
}

func TestJob_DoneWithErr(t *testing.T) {
	// Test DoneWithErr
	job := &Job{}
	job.Init(context.Background(), "error-job", JTAny, func(ctx context.Context) *JobResult {
		return &JobResult{Res: "should not be called"}
	})

	testErr := moerr.NewInternalErrorNoCtx("test error")
	job.DoneWithErr(testErr)

	result := job.WaitDone()
	assert.Equal(t, testErr, result.Err)
	assert.Nil(t, result.Res)
}

func TestJob_Reset(t *testing.T) {
	// Test job reset
	job := &Job{}
	job.Init(context.Background(), "reset-job", JTAny, func(ctx context.Context) *JobResult {
		return &JobResult{Res: "done"}
	})

	job.Run()
	result := job.WaitDone()
	assert.Equal(t, "done", result.Res)

	// Reset the job
	job.Reset()

	assert.Equal(t, "", job.id)
	assert.Equal(t, JobType(JTInvalid), job.typ)
	assert.Nil(t, job.result)
	assert.Nil(t, job.ctx)
	assert.Nil(t, job.exec)
}
