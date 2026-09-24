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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

const unpublishedS3RetryInterval = time.Second

// Failed remote-stream cleanup outlives the mirror transaction's handler. One
// worker per CN retries the exact workspace cleanup callback until it succeeds
// or the CN is closed. The queue is in-memory; process crashes need durable GC.
type unpublishedS3CleanupQueue struct {
	closeMu sync.Mutex
	mu      sync.Mutex
	pending []func(context.Context) error
	stop    chan struct{}
	done    chan struct{}
	closing bool
}

func (srv *Server) RetryUnpublishedS3Cleanup(cleanup func(context.Context) error) error {
	if srv == nil || cleanup == nil {
		return moerr.NewInvalidStateNoCtx("missing CN owner for unpublished S3 cleanup")
	}
	q := &srv.unpublishedS3Cleanup
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closing {
		return moerr.NewInvalidStateNoCtx("CN is closing with unpublished S3 cleanup pending")
	}
	q.pending = append(q.pending, cleanup)
	q.startLocked()
	return nil
}

func (q *unpublishedS3CleanupQueue) startLocked() {
	if q.stop == nil {
		q.stop = make(chan struct{})
		q.done = make(chan struct{})
		go q.run(q.stop, q.done)
	}
}

// Clear the consumed slot before advancing the slice: the backing array must
// not retain completed callbacks and their captured cleanup resources.
func (q *unpublishedS3CleanupQueue) finishAttemptLocked(err error) {
	if err != nil && len(q.pending) == 1 {
		return
	}
	cleanup := q.pending[0]
	q.pending[0] = nil
	q.pending = q.pending[1:]
	if err != nil {
		q.pending = append(q.pending, cleanup)
	} else if len(q.pending) == 0 {
		q.pending = nil
	}
}

func (q *unpublishedS3CleanupQueue) run(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(unpublishedS3RetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		q.mu.Lock()
		if len(q.pending) == 0 {
			q.stop = nil
			q.done = nil
			q.mu.Unlock()
			return
		}
		cleanup := q.pending[0]
		q.mu.Unlock()

		attemptCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := cleanup(attemptCtx)
		cancel()
		q.mu.Lock()
		q.finishAttemptLocked(err)
		q.mu.Unlock()
		if err != nil {
			logutil.Warn("remote unpublished S3 cleanup will be retried", zap.Error(err))
		}
	}
}

// CloseUnpublishedS3Cleanup joins the retry worker before CN file-service
// dependencies are closed. Failed tasks rotate within one bounded deadline.
// On timeout the CN keeps its dependencies alive (fail-stop), so resume the
// worker to keep those tasks retryable if storage subsequently recovers.
func (srv *Server) CloseUnpublishedS3Cleanup(ctx context.Context) error {
	if srv == nil {
		return nil
	}
	q := &srv.unpublishedS3Cleanup
	q.closeMu.Lock()
	defer q.closeMu.Unlock()
	cleanupCtx, cancel := UnpublishedS3CleanupContext(ctx)
	defer cancel()
	q.mu.Lock()
	q.closing = true
	stop, done := q.stop, q.done
	if stop != nil {
		close(stop)
		q.stop = nil
		q.done = nil
	}
	q.mu.Unlock()
	if done != nil {
		<-done
	}

	var lastErr error
	for {
		q.mu.Lock()
		remaining := len(q.pending)
		if remaining == 0 {
			q.mu.Unlock()
			return nil
		}
		if cleanupCtx.Err() != nil {
			q.startLocked()
			q.mu.Unlock()
			return moerr.NewInternalErrorNoCtxf("%d unpublished S3 cleanup tasks remain at CN shutdown: %v (deadline: %v)", remaining, lastErr, cleanupCtx.Err())
		}
		q.mu.Unlock()
		for range remaining {
			if cleanupCtx.Err() != nil {
				break
			}
			q.mu.Lock()
			cleanup := q.pending[0]
			q.mu.Unlock()
			attemptCtx, attemptCancel := context.WithTimeout(cleanupCtx, 30*time.Second)
			err := cleanup(attemptCtx)
			attemptCancel()
			q.mu.Lock()
			q.finishAttemptLocked(err)
			remainingTasks := len(q.pending)
			q.mu.Unlock()
			if err != nil {
				lastErr = err
			}
			if remainingTasks == 0 {
				return nil
			}
		}
		timer := time.NewTimer(unpublishedS3RetryInterval)
		select {
		case <-cleanupCtx.Done():
		case <-timer.C:
		}
		timer.Stop()
	}
}
