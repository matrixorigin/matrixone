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
	// Use a large arg to verify it gets zeroed before pool return.
	type heavy struct {
		data [1024]byte
	}
	held := &heavy{}

	ctx := context.Background()
	ctx = WithEventLogger(ctx)
	LogEvent(ctx, str_to_cache_data_begin, held)
	LogEvent(ctx, str_to_cache_data_end, held)
	LogSlowEvent(ctx, time.Nanosecond) // returns slice to pool

	_ = held.data

	// Grab the slice back from the pool and check all args are nil.
	events := eventsPool.Get().(*[]event)
	for i, ev := range *events {
		if ev.args != nil {
			t.Errorf("event[%d].args = %v, want nil", i, ev.args)
		}
		for j, arg := range ev._args {
			if arg != nil {
				t.Errorf("event[%d]._args[%d] = %v, want nil", i, j, arg)
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
