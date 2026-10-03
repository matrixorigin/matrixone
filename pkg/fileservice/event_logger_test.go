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
	"testing"
	"time"
)

func TestEventLogger(t *testing.T) {
	ctx := context.Background()
	ctx = WithEventLogger(ctx)
	LogEvent(ctx, str_to_cache_data_begin, 1)
	LogEvent(ctx, str_to_cache_data_end, 2, 3)
	LogSlowEvent(ctx, time.Nanosecond)
}

func TestEventLoggerPoolCleanup(t *testing.T) {
	for _, threshold := range []time.Duration{time.Hour, 0} {
		t.Run(threshold.String(), func(t *testing.T) {
			held := new([1024]byte)
			ctx := WithEventLogger(context.Background())
			logger := ctx.Value(EventLoggerKey).(*eventLogger)
			LogEvent(ctx, str_to_cache_data_begin, held)
			LogEvent(ctx, str_to_cache_data_end, held)
			// Inspect the actual nonempty active backing, without relying on a pool
			// returning the same object or ranging over its reset zero length.
			events := *logger.events
			if len(events) == 0 || events[0].args == nil {
				t.Fatal("no recorded arguments to check")
			}
			LogSlowEvent(ctx, threshold)
			if !logger.closed || logger.events != nil {
				t.Fatal("logger did not release event ownership")
			}
			for i, ev := range events {
				if ev.args != nil {
					t.Errorf("event[%d].args retained arguments", i)
				}
				for j, arg := range ev._args {
					if arg != nil {
						t.Errorf("event[%d]._args[%d] retained argument", i, j)
					}
				}
			}
			LogEvent(ctx, str_to_cache_data_begin, held)
			if logger.events != nil {
				t.Fatal("closed logger accepted new event")
			}
		})
	}
}

func TestEventLoggerPreservesArgumentsAcrossGrowth(t *testing.T) {
	ctx := WithEventLogger(context.Background())
	logger := ctx.Value(EventLoggerKey).(*eventLogger)
	t.Cleanup(func() {
		events := *logger.events
		LogSlowEvent(ctx, time.Hour)
		for i, ev := range events {
			if ev.args != nil {
				t.Errorf("grown event %d retained arguments", i)
			}
			for j, arg := range ev._args {
				if arg != nil {
					t.Errorf("grown event %d inline argument %d retained", i, j)
				}
			}
		}
	})
	args := []any{nil, "second", int64(3), true, time.Second, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, "last"}
	// Cross the original pool capacity, preserving both inline and overflow
	// parameters in every event even when the event array moves.
	for i := 0; i < 33; i++ {
		count := []int{0, 1, 2, 3, len(args)}[i%5]
		LogEvent(ctx, str_to_cache_data_begin, args[:count]...)
	}
	for i, ev := range *logger.events {
		expected := args[:[]int{0, 1, 2, 3, len(args)}[i%5]]
		if len(ev.args) != len(expected) {
			t.Fatalf("event %d lost arguments: %d != %d", i, len(ev.args), len(expected))
		}
		for j, got := range ev.args {
			if got != expected[j] {
				t.Errorf("event %d argument %d: %v != %v", i, j, got, expected[j])
			}
		}
	}
}

func TestEventLoggerDropsAfterLimit(t *testing.T) {
	ctx := WithEventLogger(context.Background())
	logger := ctx.Value(EventLoggerKey).(*eventLogger)

	for i := 0; i < maxEventLoggerEvents+7; i++ {
		LogEvent(ctx, str_to_cache_data_begin, i)
	}

	logger.mu.Lock()
	if n := len(*logger.events); n != maxEventLoggerEvents {
		t.Fatalf("got %d events, want %d", n, maxEventLoggerEvents)
	}
	if logger.dropped != 7 {
		t.Fatalf("got %d dropped events, want 7", logger.dropped)
	}
	logger.mu.Unlock()

	LogSlowEvent(ctx, time.Hour)
}

func TestWithoutEventLogger(t *testing.T) {
	ctx := WithEventLogger(context.Background())
	logger := ctx.Value(EventLoggerKey).(*eventLogger)

	child := withoutEventLogger(ctx)
	LogEvent(child, str_to_cache_data_begin)

	logger.mu.Lock()
	if n := len(*logger.events); n != 0 {
		t.Fatalf("got %d parent events after child log, want 0", n)
	}
	logger.mu.Unlock()

	LogEvent(ctx, str_to_cache_data_begin)
	logger.mu.Lock()
	if n := len(*logger.events); n != 1 {
		t.Fatalf("got %d parent events after parent log, want 1", n)
	}
	logger.mu.Unlock()

	LogSlowEvent(ctx, time.Hour)
}

func BenchmarkEventLogger(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		ctx = WithEventLogger(ctx)
		LogEvent(ctx, str_to_cache_data_begin, 1)
		LogEvent(ctx, str_to_cache_data_end, 2, 3)
		LogSlowEvent(ctx, time.Hour)
	}
}

func BenchmarkEventLoggerArguments(b *testing.B) {
	for _, count := range []int{0, 1, 2, 3, 17} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			args := make([]any, count)
			for i := range args {
				args[i] = i
			}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ctx := WithEventLogger(context.Background())
				for j := 0; j < 33; j++ {
					LogEvent(ctx, str_to_cache_data_begin, args...)
				}
				LogSlowEvent(ctx, time.Hour)
			}
		})
	}
}
