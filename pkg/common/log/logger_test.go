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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// mockEnabledTracer is a tracer that reports IsEnable() = true
type mockEnabledTracer struct {
	trace.NoopTracer
}

func (m mockEnabledTracer) IsEnable(opts ...trace.SpanStartOption) bool {
	return true
}

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
