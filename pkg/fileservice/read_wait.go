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

package fileservice

import (
	"context"
	"io"
	"sync/atomic"
	"time"
)

type readWaitTrackerKey struct{}

// ReadWaitTracker accumulates time spent in the storage calls and waits of
// one synchronous read path. Cache lookup, decoding and caller work run
// outside these intervals.
type ReadWaitTracker struct {
	nanos atomic.Int64
}

// WithReadWaitTracking tracks the blocking reads initiated under ctx.
func WithReadWaitTracking(ctx context.Context) (context.Context, *ReadWaitTracker) {
	tracker := new(ReadWaitTracker)
	return context.WithValue(ctx, readWaitTrackerKey{}, tracker), tracker
}

// ReadWaitTrackerFromContext returns nil when read wait tracking was not enabled.
func ReadWaitTrackerFromContext(ctx context.Context) *ReadWaitTracker {
	tracker, _ := ctx.Value(readWaitTrackerKey{}).(*ReadWaitTracker)
	return tracker
}

func withoutReadWaitTracking(ctx context.Context) context.Context {
	if ReadWaitTrackerFromContext(ctx) == nil {
		return ctx
	}
	return context.WithValue(ctx, readWaitTrackerKey{}, (*ReadWaitTracker)(nil))
}

// Elapsed returns the sum of completed read wait intervals.
func (t *ReadWaitTracker) Elapsed() time.Duration {
	return time.Duration(t.nanos.Load())
}

// Start starts an interval when tracking is enabled. A nil tracker is a no-op.
func (t *ReadWaitTracker) Start() time.Time {
	if t == nil {
		return time.Time{}
	}
	return time.Now()
}

// Stop records the completed interval on this tracker.
func (t *ReadWaitTracker) Stop(start time.Time) {
	if t != nil {
		t.nanos.Add(int64(time.Since(start)))
	}
}

func (t *ReadWaitTracker) wrapReader(r io.Reader) io.Reader {
	if t == nil {
		return r
	}
	return readerFunc(func(p []byte) (int, error) {
		start := t.Start()
		defer t.Stop(start)
		return r.Read(p)
	})
}
