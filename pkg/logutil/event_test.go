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

package logutil

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func withEventTestState(t *testing.T, logger *zap.Logger) {
	t.Helper()
	originalLogger := GetGlobalLogger()
	originalBudget := globalEventBudget
	replaceGlobalLogger(logger)
	globalEventBudget = NewEventRateLimiter(RateLimitedLoggerConfig{MaxKeys: 1024})
	t.Cleanup(func() {
		replaceGlobalLogger(originalLogger)
		globalEventBudget = originalBudget
	})
}

func TestEventLazySkipsWorkWhenDisabledOrSuppressed(t *testing.T) {
	event := Event{Name: "test.event.lazy", Message: "test event"}
	var builds atomic.Int64
	build := func() []zap.Field {
		builds.Add(1)
		return []zap.Field{zap.String("derived", "value")}
	}

	disabled, _ := observer.New(zap.WarnLevel)
	withEventTestState(t, zap.New(disabled))
	require.False(t, event.InfoLazy(build))
	require.Zero(t, builds.Load())

	core, observed := observer.New(zap.InfoLevel)
	replaceGlobalLogger(zap.New(core))
	for range 4 {
		event.InfoLazy(build)
	}
	require.Len(t, observed.All(), 3)
	require.Equal(t, int64(3), builds.Load())
	fields := observed.All()[2].ContextMap()
	require.Equal(t, event.Name, fields[FieldEvent])
	require.Equal(t, int64(3), fields[FieldOccurrence])
}

func TestEventReportsSuppressedOccurrences(t *testing.T) {
	event := Event{Name: "test.event.suppressed", Message: "test event"}
	core, observed := observer.New(zap.WarnLevel)
	withEventTestState(t, zap.New(core))
	for range 4 {
		event.Warn()
	}
	globalEventBudget.now = func() time.Time { return time.Now().Add(11 * time.Second) }
	event.Warn()
	require.Len(t, observed.All(), 4)
	fields := observed.All()[3].ContextMap()
	require.Equal(t, int64(1), fields[FieldSuppressed])
}

func TestEventPreservesBusinessCaller(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	withEventTestState(t, zap.New(core, zap.AddCaller()))
	Event{Name: "test.event.caller", Message: "test event"}.Info()
	require.Len(t, observed.All(), 1)
	require.True(t, strings.HasSuffix(observed.All()[0].Caller.File, "event_test.go"))
}

func TestEventBudgetUsesOneBoundedOverflowPopulation(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	withEventTestState(t, zap.New(core))
	globalEventBudget = NewEventRateLimiter(RateLimitedLoggerConfig{MaxKeys: 1})
	Event{Name: "test.event.first", Message: "first"}.Info()
	Event{Name: "test.event.second", Message: "second"}.Info()
	require.Len(t, observed.All(), 2)
	fields := observed.All()[1].ContextMap()
	require.Equal(t, "event-budget-overflow", fields[FieldEvent])
	require.Equal(t, true, fields["event-budget-overflow"])
}

func TestEventBudgetRejectsOversizedNameBeforeStateRetention(t *testing.T) {
	limiter := NewEventRateLimiter(RateLimitedLoggerConfig{MaxKeys: 1})
	oversizedName := strings.Repeat("x", 1<<20)

	decision, ok := limiter.Allow(oversizedName, RateLimitConfig{
		Interval:   time.Hour,
		BurstCount: 1,
	})
	require.True(t, ok)
	require.True(t, decision.Overflow)
	require.Equal(t, overflowEvent, decision.Event)
	require.Zero(t, limiter.StateCount())
}

func TestEventBudgetIsBoundedUnderConcurrentCalls(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	withEventTestState(t, zap.New(core))
	event := Event{Name: "test.event.concurrent", Message: "test event"}

	var builds atomic.Int64
	var callers sync.WaitGroup
	for range 64 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			event.InfoLazy(func() []zap.Field {
				builds.Add(1)
				return nil
			})
		}()
	}
	callers.Wait()
	require.Len(t, observed.All(), 3)
	require.Equal(t, int64(3), builds.Load())
}

func TestEventLazySuppressedPathDoesNotAllocateOrBuild(t *testing.T) {
	core, _ := observer.New(zap.InfoLevel)
	withEventTestState(t, zap.New(core))
	event := Event{Name: "test.event.suppressed-no-alloc", Message: "test event"}
	for range 3 {
		event.Info()
	}
	var builds atomic.Int64
	allocs := testing.AllocsPerRun(1_000, func() {
		event.InfoLazy(func() []zap.Field {
			builds.Add(1)
			return []zap.Field{zap.String("expensive", "field")}
		})
	})
	require.Zero(t, allocs)
	require.Zero(t, builds.Load())
}

func TestFingerprintFieldsDoNotWriteRawValues(t *testing.T) {
	const secret = "select * from customer where token='do-not-retain'"
	var output bytes.Buffer
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&output),
		zap.InfoLevel,
	)
	withEventTestState(t, zap.New(core))
	event := Event{Name: "test.event.redaction", Message: "test event"}
	event.ErrorLazy(func() []zap.Field {
		fields := StringFingerprintFields("sql", secret)
		return append(fields, ErrorFingerprintFields("error", errors.New(secret))...)
	})
	require.NotContains(t, output.String(), secret)
	require.Contains(t, output.String(), "sql-sha256")
	require.Contains(t, output.String(), "error-sha256")
	require.NotContains(t, output.String(), "do-not-retain")
	require.True(t, strings.Contains(output.String(), FieldEvent))
}

func TestConnectionCloseEventsKeepOperationsAndOutcomesIndependent(t *testing.T) {
	core, observed := observer.New(zap.DebugLevel)
	withEventTestState(t, zap.New(core))
	readEvents := ConnectionCloseEvents{
		Expected: Event{Name: "test.connection.read.expected", Message: "session read closed during normal lifecycle"},
		Failed:   Event{Name: "test.connection.read.failed", Message: "session read failed"},
	}
	handleEvents := ConnectionCloseEvents{
		Expected: Event{Name: "test.connection.handle.expected", Message: "session handle closed during normal lifecycle"},
		Failed:   Event{Name: "test.connection.handle.failed", Message: "session handle failed"},
	}

	for range 3 {
		LogConnectionCloseEvent(readEvents, errors.New("read failed"))
	}
	LogConnectionCloseEvent(handleEvents, errors.New("handle failed"))
	LogConnectionCloseEvent(readEvents, errors.New("use of closed network connection"))

	entries := observed.All()
	require.Len(t, entries, 5)
	require.Equal(t, handleEvents.Failed.Name, entries[3].ContextMap()[FieldEvent])
	require.Equal(t, readEvents.Expected.Name, entries[4].ContextMap()[FieldEvent])
	require.NotContains(t, entries[3].ContextMap(), "operation-sha256")
}
