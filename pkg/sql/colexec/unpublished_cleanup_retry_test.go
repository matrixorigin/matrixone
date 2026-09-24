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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
	var order []int
	require.NoError(t, server.RetryUnpublishedS3Cleanup(func(context.Context) error {
		order = append(order, 1)
		if len(order) == 1 {
			return errors.New("temporary failure")
		}
		return nil
	}))
	require.NoError(t, server.RetryUnpublishedS3Cleanup(func(context.Context) error {
		order = append(order, 2)
		return nil
	}))
	server.unpublishedS3Cleanup.mu.Lock()
	backing := server.unpublishedS3Cleanup.pending[:8]
	server.unpublishedS3Cleanup.mu.Unlock()
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
