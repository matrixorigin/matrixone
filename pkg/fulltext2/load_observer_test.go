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

package fulltext2

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestLoadTraceEmitsExactlyOnce(t *testing.T) {
	var mu sync.Mutex
	var events []LoadEvent
	restore := setLoadObserver(func(event LoadEvent) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	})
	defer restore()

	trace := newLoadTrace("db.store", LoadMissCDCFlush)
	trace.addInternalSQL(2 * time.Millisecond)
	trace.addTempWrite(3 * time.Millisecond)
	trace.addMmap(4 * time.Millisecond)
	trace.addChecksum(5 * time.Millisecond)
	trace.addBaseBytes(11)
	trace.addTailBytes(7)
	trace.setGeneration(12, 34)
	trace.finish(nil, false, 2)
	trace.finish(errors.New("late error"), false, 9)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, events, 1)
	event := events[0]
	require.Equal(t, "db.store", event.Index)
	require.Equal(t, LoadMissCDCFlush, event.MissReason)
	require.Equal(t, int64(12), event.BaseGeneration)
	require.Equal(t, int64(34), event.TailGeneration)
	require.Equal(t, int64(11), event.BaseBytes)
	require.Equal(t, int64(7), event.TailBytes)
	require.Equal(t, int64(2), event.SingleflightWaiters)
	require.True(t, event.LoadSuccess)
	require.False(t, event.LoadError)
	require.False(t, event.LoadCancel)
	require.GreaterOrEqual(t, event.TotalLoadMicros, int64(0))
}

func TestLoadTraceCancellationClassification(t *testing.T) {
	var got LoadEvent
	restore := setLoadObserver(func(event LoadEvent) { got = event })
	defer restore()
	trace := newLoadTrace("store", LoadMissTTLExpired)
	trace.finish(contextCanceledError{}, true, 0)
	require.True(t, got.LoadCancel)
	require.False(t, got.LoadError)
	require.False(t, got.LoadSuccess)
}

func TestLoadCancellationErrorRecognizesProductionErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "context canceled", err: context.Canceled},
		{name: "context deadline", err: context.DeadlineExceeded},
		{name: "query interrupted", err: moerr.NewQueryInterrupted(context.Background())},
		{name: "query timeout", err: moerr.NewQueryTimeout(context.Background())},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.True(t, isLoadCancellationError(tt.err))
		})
	}
	require.False(t, isLoadCancellationError(errors.New("storage failure")))
}

func TestFulltext2SearchFinishesPendingTraceAfterWaiterSample(t *testing.T) {
	var got LoadEvent
	restore := setLoadObserver(func(event LoadEvent) { got = event })
	defer restore()
	s := NewFulltext2Search(TableConfig{DbName: "db", IndexTable: "store"})
	s.pendingTrace = newLoadTrace("store", LoadMissGenerationChange)
	s.pendingLoadErr = nil
	s.SetLoadWaiters(4)
	s.FinishLoadObservation()
	require.Equal(t, int64(4), got.SingleflightWaiters)
	require.True(t, got.LoadSuccess)
	s.FinishLoadObservation()
}

type contextCanceledError struct{}

func (contextCanceledError) Error() string { return "canceled" }
