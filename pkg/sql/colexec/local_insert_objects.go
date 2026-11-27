// Copyright 2022 Matrix Origin
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
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"go.uber.org/zap"
)

// LocalInsertObjectRegistry bounds the objects reserved or awaiting cleanup
// across coordinator-local INSERT streams on a CN.
type LocalInsertObjectRegistry struct {
	ctx         context.Context
	fs          fileservice.FileService
	mu          sync.Mutex
	limit, used int
	garbage     list.List
	closed      bool
	wake        chan struct{}
	stop        context.CancelFunc
	done        chan struct{}
}

func NewLocalInsertObjectRegistry(ctx context.Context, fs fileservice.FileService, limit int) *LocalInsertObjectRegistry {
	return &LocalInsertObjectRegistry{ctx: ctx, fs: fs, limit: limit, wake: make(chan struct{}, 1)}
}

// startWorker is called under mu when the first writer abandons an object.
func (r *LocalInsertObjectRegistry) startWorker() {
	ctx, stop := context.WithCancel(r.ctx)
	r.stop = stop
	r.done = make(chan struct{})
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			case <-r.wake:
			}
			attempt, cancel := context.WithTimeout(ctx, time.Second)
			failed := r.collect(attempt)
			cancel()
			if failed != 0 && ctx.Err() == nil {
				logutil.Warn("failed to clean unpublished local INSERT objects", zap.Int("count", failed))
			}
		}
	}()
}

func (r *LocalInsertObjectRegistry) Close(ctx context.Context) error {
	r.mu.Lock()
	r.closed = true
	stop, done := r.stop, r.done
	r.mu.Unlock()
	if stop != nil {
		stop()
		select {
		case <-done:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	for ctx.Err() == nil {
		r.mu.Lock()
		used, queued := r.used, r.garbage.Len()
		r.mu.Unlock()
		if used == 0 {
			return nil
		}
		if queued == 0 {
			select {
			case <-r.wake:
			case <-ctx.Done():
			}
			continue
		}
		if r.collect(ctx) != 0 {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
			}
			timer.Stop()
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return moerr.NewInternalErrorf(ctx, "local INSERT object cleanup unfinished at shutdown: %d objects", r.used)
}

// LocalInsertObjects owns the outputs of one MergeInsert scope until its
// MergeBlock hands them to the workspace. The scope joins all writers before
// retiring the stream. Its next execution starts with an empty set.
type LocalInsertObjects struct {
	registry *LocalInsertObjectRegistry
	active   bool
	pending  map[objectio.ObjectId]struct{}
}

func (r *LocalInsertObjectRegistry) NewStream() *LocalInsertObjects {
	return &LocalInsertObjects{registry: r}
}

func (s *LocalInsertObjects) Begin(ctx context.Context) error {
	r := s.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || s.active {
		return moerr.NewInternalError(ctx, "local INSERT object stream cannot start")
	}
	s.active = true
	return nil
}

func (s *LocalInsertObjects) Reserve(ctx context.Context, id objectio.ObjectId) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	r := s.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	if !s.active || r.closed {
		return moerr.NewInternalError(ctx, "local INSERT object stream is inactive")
	}
	if _, exists := s.pending[id]; exists {
		return moerr.NewInternalError(ctx, "duplicate local INSERT object reservation")
	}
	if r.used >= r.limit {
		return moerr.NewResourceExhaustedf(ctx, "local INSERT object cleanup capacity (%d)", r.limit)
	}
	if s.pending == nil {
		s.pending = make(map[objectio.ObjectId]struct{})
	}
	s.pending[id] = struct{}{}
	r.used++
	return nil
}

// Adopt follows successful workspace registration and must not fail.
func (s *LocalInsertObjects) Adopt(id objectio.ObjectId) {
	r := s.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := s.pending[id]; exists {
		delete(s.pending, id)
		r.used--
	}
}

func (s *LocalInsertObjects) Finish() int {
	r := s.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(s.pending)
	for id := range s.pending {
		r.garbage.PushBack(id)
	}
	s.pending = nil
	s.active = false
	if n != 0 && !r.closed && r.done == nil {
		r.startWorker()
	}
	if n != 0 || r.closed {
		r.signal()
	}
	return n
}

func (r *LocalInsertObjectRegistry) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// collect attempts a bounded batch and rotates failed deletes behind other
// work. Each delete has one key so only that reservation depends on its result.
func (r *LocalInsertObjectRegistry) collect(ctx context.Context) int {
	r.mu.Lock()
	n := min(64, r.garbage.Len())
	if n == 0 {
		r.mu.Unlock()
		return 0
	}
	var ids [64]objectio.ObjectId
	for i := range n {
		elem := r.garbage.Front()
		ids[i] = r.garbage.Remove(elem).(objectio.ObjectId)
	}
	r.mu.Unlock()

	failed := ids[:0]
	remaining := n
	for i, id := range ids[:n] {
		if ctx.Err() != nil {
			remaining = i
			break
		}
		name := objectio.BuildObjectNameWithObjectID(&id).String()
		if err := r.fs.Delete(ctx, name); err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			failed = append(failed, id)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range ids[remaining:n] {
		r.garbage.PushBack(id)
	}
	for _, id := range failed {
		r.garbage.PushBack(id)
	}
	deleted := remaining - len(failed)
	r.used -= deleted
	if deleted != 0 && r.garbage.Len() != 0 {
		r.signal()
	}
	return n - deleted
}
