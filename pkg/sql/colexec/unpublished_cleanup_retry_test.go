// Copyright 2026 Matrix Origin
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

package colexec

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestUnpublishedS3RetryQueueMetrics(t *testing.T) {
	serviceID := t.Name()
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	server := NewServer(serviceID)
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		require.NoError(t, server.CloseUnpublishedS3Cleanup(context.Background()))
	})
	require.NoError(t, server.RetryUnpublishedS3Cleanup(func(context.Context) error {
		close(entered)
		<-release
		return nil
	}))
	<-entered
	require.Equal(t, float64(1), promtestutil.ToFloat64(
		metricv2.UnpublishedS3PendingTasksGauge.WithLabelValues(serviceID)))
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, server.CloseUnpublishedS3Cleanup(context.Background()))
	require.Zero(t, promtestutil.ToFloat64(
		metricv2.UnpublishedS3PendingTasksGauge.WithLabelValues(serviceID)))
}

func TestUnpublishedS3OldestRetryAgeSurvivesRotation(t *testing.T) {
	serviceID := t.Name()
	q := &unpublishedS3CleanupQueue{
		serviceID: serviceID,
		pending: []func(context.Context) error{
			func(context.Context) error { return nil },
			func(context.Context) error { return nil },
		},
		pendingSince: []time.Time{
			time.Now().Add(-2 * time.Minute),
			time.Now().Add(-time.Minute),
		},
	}
	q.mu.Lock()
	q.updateMetricsLocked()
	q.finishAttemptLocked(errors.New("temporary failure"))
	q.lastAgeSample = time.Time{}
	q.updateMetricsLocked()
	q.mu.Unlock()

	age := promtestutil.ToFloat64(metricv2.UnpublishedS3OldestTaskAgeGauge.WithLabelValues(serviceID))
	require.GreaterOrEqual(t, age, float64(120), "failure rotation must keep the original enqueue age")
	require.Less(t, age, float64(130))

	q.mu.Lock()
	q.finishAttemptLocked(nil)
	q.finishAttemptLocked(nil)
	q.mu.Unlock()
	require.Zero(t, promtestutil.ToFloat64(
		metricv2.UnpublishedS3OldestTaskAgeGauge.WithLabelValues(serviceID)))
}

func TestServerCloseDrainsUnpublishedS3Retry(t *testing.T) {
	server := NewServer("")
	var calls atomic.Int32
	require.NoError(t, server.RetryUnpublishedS3Cleanup(func(context.Context) error {
		calls.Add(1)
		return nil
	}))
	require.NoError(t, server.CloseUnpublishedS3Cleanup(context.Background()))
	require.Equal(t, int32(1), calls.Load())
	require.Error(t, server.RetryUnpublishedS3Cleanup(func(context.Context) error { return nil }))
}

func TestServerCloseReportsUnresolvedS3Cleanup(t *testing.T) {
	server := NewServer("")
	cleanupErr := errors.New("S3 unavailable")
	var recovered atomic.Bool
	finished := make(chan struct{})
	t.Cleanup(func() {
		recovered.Store(true)
		require.NoError(t, server.CloseUnpublishedS3Cleanup(context.Background()))
	})
	require.NoError(t, server.RetryUnpublishedS3Cleanup(func(context.Context) error {
		if recovered.Load() {
			close(finished)
			return nil
		}
		return cleanupErr
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := server.CloseUnpublishedS3Cleanup(ctx)
	require.ErrorContains(t, err, "1 unpublished S3 cleanup tasks remain")
	server.unpublishedS3Cleanup.mu.Lock()
	require.Len(t, server.unpublishedS3Cleanup.pending, 1, "shutdown must not report success or discard the failed owner")
	server.unpublishedS3Cleanup.mu.Unlock()
	recovered.Store(true)
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup lost its retry entry after the shutdown deadline")
	}
	require.NoError(t, server.CloseUnpublishedS3Cleanup(context.Background()))
}

func TestServerCloseRetriesFailureWithoutBlockingOtherTasks(t *testing.T) {
	server := &Server{}
	server.unpublishedS3Cleanup.pending = make([]func(context.Context) error, 0, 8)
	firstEntered := make(chan struct{})
	allowFirstFailure := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(allowFirstFailure) })
		require.NoError(t, server.CloseUnpublishedS3Cleanup(context.Background()))
	})
	var order []int
	require.NoError(t, server.RetryUnpublishedS3Cleanup(func(context.Context) error {
		order = append(order, 1)
		if len(order) == 1 {
			close(firstEntered)
			<-allowFirstFailure
			return errors.New("temporary failure")
		}
		return nil
	}))
	<-firstEntered
	require.NoError(t, server.RetryUnpublishedS3Cleanup(func(context.Context) error {
		order = append(order, 2)
		return nil
	}))
	server.unpublishedS3Cleanup.mu.Lock()
	backing := server.unpublishedS3Cleanup.pending[:8]
	server.unpublishedS3Cleanup.mu.Unlock()
	releaseOnce.Do(func() { close(allowFirstFailure) })
	require.NoError(t, server.CloseUnpublishedS3Cleanup(context.Background()))
	require.Equal(t, []int{1, 2, 1}, order)
	for _, callback := range backing {
		require.Nil(t, callback, "completed cleanup must not remain in the backing array")
	}
	require.Nil(t, server.unpublishedS3Cleanup.pending)
}

func TestServerRetriesAndRotatesFailedS3Cleanup(t *testing.T) {
	server := &Server{}
	// Keep one backing array across rotation so the retention assertion does
	// not pin an abandoned array that the production queue already released.
	server.unpublishedS3Cleanup.pending = make([]func(context.Context) error, 0, 8)
	t.Cleanup(func() { require.NoError(t, server.CloseUnpublishedS3Cleanup(context.Background())) })
	var firstCalls atomic.Int32
	firstDone := make(chan struct{})
	secondDone := make(chan struct{})
	require.NoError(t, server.RetryUnpublishedS3Cleanup(func(context.Context) error {
		if firstCalls.Add(1) == 1 {
			return errors.New("temporary storage failure")
		}
		close(firstDone)
		return nil
	}))
	require.NoError(t, server.RetryUnpublishedS3Cleanup(func(context.Context) error {
		close(secondDone)
		return nil
	}))
	server.unpublishedS3Cleanup.mu.Lock()
	backing := server.unpublishedS3Cleanup.pending[:cap(server.unpublishedS3Cleanup.pending)]
	server.unpublishedS3Cleanup.mu.Unlock()
	select {
	case <-secondDone:
	case <-time.After(5 * time.Second):
		t.Fatal("healthy cleanup was blocked behind a failed task")
	}
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("failed cleanup was not retried")
	}
	require.Equal(t, int32(2), firstCalls.Load())
	require.NoError(t, server.CloseUnpublishedS3Cleanup(context.Background()))
	for _, callback := range backing {
		require.Nil(t, callback)
	}
}

func TestUnpublishedS3RetryConcurrentEnqueueAndClose(t *testing.T) {
	server := NewServer("")
	entered := make(chan struct{})
	release := make(chan struct{})
	var executed atomic.Int32
	require.NoError(t, server.RetryUnpublishedS3Cleanup(func(context.Context) error {
		close(entered)
		<-release
		executed.Add(1)
		return nil
	}))
	<-entered

	const competitors = 32
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for range competitors {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := server.RetryUnpublishedS3Cleanup(func(context.Context) error {
				executed.Add(1)
				return nil
			}); err == nil {
				accepted.Add(1)
			}
		}()
	}
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- server.CloseUnpublishedS3Cleanup(context.Background())
	}()
	wg.Wait()
	close(release)
	select {
	case err := <-closeResult:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("CN close did not join accepted cleanup tasks")
	}
	require.Equal(t, 1+accepted.Load(), executed.Load())
	require.Error(t, server.RetryUnpublishedS3Cleanup(func(context.Context) error { return nil }))
}

// Measures the normal worker after a previously blocked cleanup succeeds.
// Run with -benchtime=1x so the number of queued tasks stays explicit.
func BenchmarkUnpublishedS3RetrySuccessfulDrain(b *testing.B) {
	const taskCount = 8
	for range b.N {
		b.StopTimer()
		server := NewServer("")
		started := make(chan struct{})
		release := make(chan struct{})
		done := make(chan struct{})
		var completed atomic.Int32
		finish := func() {
			if completed.Add(1) == taskCount {
				close(done)
			}
		}
		if err := server.RetryUnpublishedS3Cleanup(func(context.Context) error {
			close(started)
			<-release
			finish()
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		<-started
		for range taskCount - 1 {
			if err := server.RetryUnpublishedS3Cleanup(func(context.Context) error {
				finish()
				return nil
			}); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		close(release)
		<-done
		b.StopTimer()
		if err := server.CloseUnpublishedS3Cleanup(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
