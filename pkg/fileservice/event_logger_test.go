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

func BenchmarkEventLogger(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		ctx = WithEventLogger(ctx)
		LogEvent(ctx, str_to_cache_data_begin, 1)
		LogEvent(ctx, str_to_cache_data_end, 2, 3)
		LogSlowEvent(ctx, time.Hour)
	}
}
