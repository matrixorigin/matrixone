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
	require.NoError(t, server.RetryUnpublishedS3Cleanup(func(context.Context) error {
		return cleanupErr
	}))
	err := server.CloseUnpublishedS3Cleanup(context.Background())
	require.ErrorContains(t, err, "1 unpublished S3 cleanup tasks remain")
	server.unpublishedS3Cleanup.mu.Lock()
	require.Len(t, server.unpublishedS3Cleanup.pending, 1, "shutdown must not report success or discard the failed owner")
	server.unpublishedS3Cleanup.mu.Unlock()
}

func TestServerRetriesAndRotatesFailedS3Cleanup(t *testing.T) {
	server := &Server{}
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
}
