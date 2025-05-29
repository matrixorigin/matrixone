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

package fileservice

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

type eventLogger struct {
	begin    time.Time
	mu       sync.Mutex
	events   []event
	upstream context.Context
}

type event struct {
	time  time.Duration
	ev    stringRef
	args  []any
	_args [16]any
}

type eventLoggerKey struct{}

var EventLoggerKey eventLoggerKey

var eventLoggersPool = sync.Pool{
	New: func() any {
		return &eventLogger{
			events: make([]event, 0, 32),
		}
	},
}

func (e *eventLogger) reset() {
	e.begin = time.Time{}
	e.events = e.events[:0]
	e.upstream = nil
}

func (e *eventLogger) Stop() {
	e.reset()
	eventLoggersPool.Put(e)
}

func WithEventLogger(ctx context.Context) (_ context.Context, logger *eventLogger) {
	v := ctx.Value(EventLoggerKey)
	if v != nil {
		return ctx, v.(*eventLogger)
	}
	ret := eventLoggersPool.Get().(*eventLogger)
	ret.begin = time.Now()
	ret.upstream = ctx
	return ret, ret
}

func LogEvent(ctx context.Context, ev stringRef, args ...any) {
	v := ctx.Value(EventLoggerKey)
	if v == nil {
		return
	}
	logger := v.(*eventLogger)
	logger.emit(ev, args...)
}

func (e *eventLogger) emit(ev stringRef, args ...any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, event{})
	last := &((e.events)[len(e.events)-1])
	last.time = time.Since(e.begin)
	last.ev = ev
	last.args = last._args[:0]
	last.args = append(last.args, args...)
}

func LogSlowEvent(ctx context.Context, threshold time.Duration) {
	v := ctx.Value(EventLoggerKey)
	if v == nil {
		return
	}
	logger := v.(*eventLogger)

	logger.mu.Lock()
	defer logger.mu.Unlock()

	if time.Since(logger.begin) < threshold {
		return
	}

	buf := stringsBuilderPool.Get().(*strings.Builder)
	defer func() {
		buf.Reset()
		stringsBuilderPool.Put(buf)
	}()

	for _, ev := range logger.events {
		buf.WriteString("<")
		buf.WriteString(ev.time.String())
		buf.WriteString(" ")
		buf.WriteString(ev.ev.String())
		for _, arg := range ev.args {
			buf.WriteString(" ")
			fmt.Fprintf(buf, "%+v", arg)
		}
		buf.WriteString("> ")
	}

	logutil.Info("slow event",
		zap.String("events", buf.String()),
	)
}

var stringsBuilderPool = sync.Pool{
	New: func() any {
		return new(strings.Builder)
	},
}

var _ context.Context = new(eventLogger)

func (e *eventLogger) Deadline() (deadline time.Time, ok bool) {
	return e.upstream.Deadline()
}

func (e *eventLogger) Done() <-chan struct{} {
	return e.upstream.Done()
}

func (e *eventLogger) Err() error {
	return e.upstream.Err()
}

func (e *eventLogger) Value(key any) any {
	if key == EventLoggerKey {
		return e
	}
	return e.upstream.Value(key)
}
