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

package log

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// mockEnabledTracer is a tracer that reports IsEnable() = true
type mockEnabledTracer struct {
	trace.NoopTracer
}

func (m mockEnabledTracer) IsEnable(opts ...trace.SpanStartOption) bool {
	return true
}

type noAllocCore struct{}

var eventTestSequence atomic.Uint64

func (noAllocCore) Enabled(zapcore.Level) bool { return true }

func (noAllocCore) With([]zap.Field) zapcore.Core { return noAllocCore{} }

func (noAllocCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return checked.AddCore(entry, noAllocCore{})
}

func (noAllocCore) Write(zapcore.Entry, []zap.Field) error { return nil }

func (noAllocCore) Sync() error { return nil }

func TestMOLogger_WithContext(t *testing.T) {
	logger := wrap(zap.NewNop())

	t.Run("nil context should panic", func(t *testing.T) {
		assert.Panics(t, func() {
			logger.WithContext(nil)
		})
	})

	t.Run("trace disabled - TODO and Background allowed", func(t *testing.T) {
		// NoopTracer is the default, IsEnable() returns false
		trace.SetDefaultTracer(trace.NoopTracer{})

		assert.NotPanics(t, func() {
			logger.WithContext(context.TODO())
		})
		assert.NotPanics(t, func() {
			logger.WithContext(context.Background())
		})
	})

	t.Run("trace enabled - TODO and Background should panic", func(t *testing.T) {
		trace.SetDefaultTracer(mockEnabledTracer{})
		defer trace.SetDefaultTracer(trace.NoopTracer{})

		assert.Panics(t, func() {
			logger.WithContext(context.TODO())
		})
		assert.Panics(t, func() {
			logger.WithContext(context.Background())
		})
	})

	t.Run("trace enabled - empty SpanContext should panic", func(t *testing.T) {
		trace.SetDefaultTracer(mockEnabledTracer{})
		defer trace.SetDefaultTracer(trace.NoopTracer{})

		// A custom context without span info
		ctx := context.WithValue(context.Background(), "key", "value")
		assert.Panics(t, func() {
			logger.WithContext(ctx)
		})
	})
}

func TestWrapWithContext(t *testing.T) {
	zapLogger := zap.NewNop()

	t.Run("nil logger should panic", func(t *testing.T) {
		assert.Panics(t, func() {
			wrapWithContext(nil, context.Background())
		})
	})

	t.Run("nil context allowed", func(t *testing.T) {
		assert.NotPanics(t, func() {
			wrapWithContext(zapLogger, nil)
		})
	})

	t.Run("trace disabled - TODO and Background allowed", func(t *testing.T) {
		trace.SetDefaultTracer(trace.NoopTracer{})

		assert.NotPanics(t, func() {
			wrapWithContext(zapLogger, context.TODO())
		})
		assert.NotPanics(t, func() {
			wrapWithContext(zapLogger, context.Background())
		})
	})

	t.Run("trace enabled - TODO and Background should panic", func(t *testing.T) {
		trace.SetDefaultTracer(mockEnabledTracer{})
		defer trace.SetDefaultTracer(trace.NoopTracer{})

		assert.Panics(t, func() {
			wrapWithContext(zapLogger, context.TODO())
		})
		assert.Panics(t, func() {
			wrapWithContext(zapLogger, context.Background())
		})
	})

	t.Run("trace enabled - empty SpanContext should panic", func(t *testing.T) {
		trace.SetDefaultTracer(mockEnabledTracer{})
		defer trace.SetDefaultTracer(trace.NoopTracer{})

		ctx := context.WithValue(context.Background(), "key", "value")
		assert.Panics(t, func() {
			wrapWithContext(zapLogger, ctx)
		})
	})
}

func TestLogOptions_WithContext(t *testing.T) {
	opts := DefaultLogOptions()

	t.Run("nil context should panic", func(t *testing.T) {
		assert.Panics(t, func() {
			opts.WithContext(nil)
		})
	})

	t.Run("trace disabled - TODO and Background allowed", func(t *testing.T) {
		trace.SetDefaultTracer(trace.NoopTracer{})

		assert.NotPanics(t, func() {
			opts.WithContext(context.TODO())
		})
		assert.NotPanics(t, func() {
			opts.WithContext(context.Background())
		})
	})

	t.Run("trace enabled - TODO and Background should panic", func(t *testing.T) {
		trace.SetDefaultTracer(mockEnabledTracer{})
		defer trace.SetDefaultTracer(trace.NoopTracer{})

		assert.Panics(t, func() {
			opts.WithContext(context.TODO())
		})
		assert.Panics(t, func() {
			opts.WithContext(context.Background())
		})
	})

	t.Run("trace enabled - empty SpanContext should panic", func(t *testing.T) {
		trace.SetDefaultTracer(mockEnabledTracer{})
		defer trace.SetDefaultTracer(trace.NoopTracer{})

		ctx := context.WithValue(context.Background(), "key", "value")
		assert.Panics(t, func() {
			opts.WithContext(ctx)
		})
	})
}

func TestMOLoggerEventBudgetIsSharedAndLazy(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	logger := wrap(zap.New(core))
	child := logger.Named("child")
	independentRoot := wrap(zap.New(core))
	// Event budgets are process-wide. -count runs this test repeatedly in one
	// process, so give each invocation an independent population.
	event := logutil.Event{
		Name:    fmt.Sprintf("test.shared-event.%d", eventTestSequence.Add(1)),
		Message: "test event",
	}
	var builds atomic.Int64
	build := func() []zap.Field {
		builds.Add(1)
		return []zap.Field{zap.String("derived", "value")}
	}

	for range 2 {
		assert.True(t, logger.InfoEventLazy(event, build))
	}
	assert.True(t, child.InfoEventLazy(event, build))
	assert.False(t, independentRoot.InfoEventLazy(event, build))
	assert.Len(t, observed.All(), 3)
	assert.Equal(t, int64(3), builds.Load())
}

func TestMOLoggerEventDoesNotAllocateStateWhenDisabled(t *testing.T) {
	logger := wrap(zap.NewNop())
	var builds atomic.Int64
	allocs := testing.AllocsPerRun(1_000, func() {
		logger.DebugEventLazy(logutil.Event{Name: "test.disabled", Message: "disabled"}, func() []zap.Field {
			builds.Add(1)
			return []zap.Field{zap.String("expensive", "field")}
		})
	})
	assert.Zero(t, allocs)
	assert.Zero(t, builds.Load())
	for range 64 {
		assert.False(t, logger.DebugEvent(logutil.Event{Name: "test.disabled", Message: "disabled"}))
	}
}

func TestMOLoggerLegacyDebugDoesNotUseEventBudget(t *testing.T) {
	core, observed := observer.New(zap.DebugLevel)
	logger := wrap(zap.New(core))

	for range 4 {
		assert.True(t, logger.Debug("legacy debug"))
	}
	assert.Len(t, observed.All(), 4)
}

func TestMOLoggerLegacyLogDoesNotCopyFieldsWithoutContext(t *testing.T) {
	logger := wrap(zap.New(noAllocCore{}))
	fields := []zap.Field{zap.String("key", "value")}

	allocs := testing.AllocsPerRun(1_000, func() {
		if !logger.Info("legacy log", fields...) {
			t.Fatal("expected log to be written")
		}
	})
	assert.Zero(t, allocs)
}
