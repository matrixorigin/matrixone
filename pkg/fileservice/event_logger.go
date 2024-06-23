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
	begin  time.Time
	mu     sync.Mutex
	buf    *strings.Builder
	closed bool
}

func newEventLogger() *eventLogger {
	return &eventLogger{
		begin: time.Now(),
		buf:   stringsBuilderPool.Get().(*strings.Builder),
	}
}

type eventLoggerKey struct{}

var EventLoggerKey eventLoggerKey

func WithEventLogger(ctx context.Context) context.Context {
	v := ctx.Value(EventLoggerKey)
	if v != nil {
		return ctx
	}
	ctx = context.WithValue(ctx, EventLoggerKey, newEventLogger())
	return ctx
}

func LogEvent(ctx context.Context, ev string, args ...any) {
	v := ctx.Value(EventLoggerKey)
	if v == nil {
		return
	}
	logger := v.(*eventLogger)
	if len(args) > 0 {
		logger.emit(fmt.Sprintf(ev, args...))
	} else {
		logger.emit(ev)
	}
}

func (e *eventLogger) emit(ev string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	e.buf.WriteString("<")
	e.buf.WriteString(time.Since(e.begin).String())
	e.buf.WriteString(" ")
	e.buf.WriteString(ev)
	e.buf.WriteString("> ")
}

func LogSlowEvent(ctx context.Context, threshold time.Duration) {
	v := ctx.Value(EventLoggerKey)
	if v == nil {
		return
	}
	logger := v.(*eventLogger)
	logger.mu.Lock()
	defer func() {
		logger.buf.Reset()
		stringsBuilderPool.Put(logger.buf)
		logger.buf = nil
		logger.closed = true
		logger.mu.Unlock()
	}()
	if time.Since(logger.begin) < threshold {
		return
	}
	logutil.Info("slow event",
		zap.String("events", logger.buf.String()),
	)
}

var stringsBuilderPool = sync.Pool{
	New: func() any {
		return new(strings.Builder)
	},
}
