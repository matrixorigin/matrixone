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

package trace

import (
	"context"
	"encoding/csv"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func TestBoundedTraceBufferDrainsBeyondCapacity(t *testing.T) {
	const capacity = 1024
	const events = capacity + 1
	// Flush exactly once after the final event, without a timer or one file
	// per event. Compute the trigger from the real CSV representation.
	buf := reuse.Alloc[buffer](nil)
	defer buf.close()
	row := make([]string, 8)
	flushBytes := 0
	for i := range events {
		txnEvent{ts: int64(i), eventType: txnCreateEvent}.toCSVRecord("", buf, row)
		for _, field := range row {
			flushBytes += len(field)
		}
		buf.reset()
	}
	records := make(chan []string, events)
	exec := executor.NewMemExecutor(func(string) (executor.Result, error) {
		return executor.Result{}, nil
	})
	svc, err := NewService(t.TempDir(), "", nil, clock.NewHLCClock(func() int64 { return 0 }, 0), exec,
		WithBufferSize(capacity), WithEnable(true, nil), WithFlushBytes(flushBytes-1),
		func(s *service) {
			s.options.writeFunc = func(_ context.Context, action loadAction) error {
				data, err := os.ReadFile(action.file)
				if err != nil {
					return err
				}
				rows, err := csv.NewReader(strings.NewReader(string(data))).ReadAll()
				if err != nil {
					return err
				}
				for _, row := range rows {
					records <- row
				}
				return nil
			}
		})
	require.NoError(t, err)
	t.Cleanup(svc.Close)
	s := svc.(*service)
	for _, queue := range []chan event{s.entryC, s.txnC, s.txnActionC, s.statementC} {
		require.Equal(t, capacity, cap(queue))
	}
	require.True(t, s.Enabled(FeatureTraceTxn))
	s.EnableFlush()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := range events {
		select {
		case s.txnC <- event{csv: txnEvent{ts: int64(i), eventType: txnCreateEvent}}:
		case <-ctx.Done():
			t.Fatal("trace producer did not progress beyond queue capacity")
		}
	}
	for i := range events {
		select {
		case record := <-records:
			require.Equal(t, strconv.Itoa(i), record[0])
			require.Contains(t, record, txnCreateEvent)
		case <-ctx.Done():
			t.Fatal("trace consumer did not persist all accepted events")
		}
	}
}

// The four event queues share handleEvent. Use a buffer-bearing real producer
// here so the FIFO release marker is exercised as well as CSV persistence.
func TestBoundedTraceBufferBackpressure(t *testing.T) {
	for generation := range 2 {
		t.Run(strconv.Itoa(generation), testBoundedTraceBufferBackpressure)
	}
}

func testBoundedTraceBufferBackpressure(t *testing.T) {
	const capacity = 1024
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var producerDone <-chan struct{}
	records := make(chan [][]string, 1)
	exec := executor.NewMemExecutor(func(string) (executor.Result, error) {
		return executor.Result{}, nil
	})
	svc, err := NewService(t.TempDir(), "", nil, clock.NewHLCClock(func() int64 { return 0 }, 0), exec,
		WithBufferSize(capacity), WithEnable(true, nil), WithFlushBytes(1),
		func(s *service) {
			s.options.writeFunc = func(_ context.Context, action loadAction) error {
				data, err := os.ReadFile(action.file)
				if err != nil {
					return err
				}
				rows, err := csv.NewReader(strings.NewReader(string(data))).ReadAll()
				if err == nil {
					records <- rows
				}
				return err
			}
		})
	require.NoError(t, err)
	t.Cleanup(func() {
		unblock()
		if producerDone != nil {
			select {
			case <-producerDone:
			case <-time.After(10 * time.Second):
				t.Error("producer did not quiesce after consumer release")
				return // Do not close channels while a producer still owns them.
			}
		}
		svc.Close()
	})
	s := svc.(*service)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wait := func(signal <-chan struct{}) {
		t.Helper()
		select {
		case <-signal:
		case <-ctx.Done():
			t.Fatal("trace pipeline did not progress")
		}
	}
	s.txnC <- event{csv: traceBarrierEvent{before: func() {
		close(entered)
		<-release
	}}}
	wait(entered)
	for i := range capacity {
		s.txnC <- event{csv: txnEvent{ts: int64(i), eventType: txnCreateEvent}}
	}
	require.Len(t, s.txnC, capacity)
	attempted, done := make(chan struct{}), make(chan struct{})
	producerDone = done
	go func() {
		defer close(done)
		s.TxnConflictChanged(tracePressureTxn{attempted: attempted}, 42, timestamp.Timestamp{})
	}()
	wait(attempted)
	// The only consumer is held at the barrier and the queue is full. The
	// real producer cannot complete its send until that consumer is released.
	select {
	case <-done:
		t.Fatal("producer bypassed full-queue backpressure")
	default:
	}
	unblock()
	wait(done)
	// Enqueued after the producer's buffer-release marker. The common handler
	// must consume that marker before enabling this single final flush.
	s.txnC <- event{csv: traceBarrierEvent{before: s.EnableFlush}}
	select {
	case rows := <-records:
		require.Len(t, rows, capacity+3)
		for i := range capacity {
			require.Equal(t, strconv.Itoa(i), rows[i+1][0])
			require.Contains(t, rows[i+1], txnCreateEvent)
		}
		require.Contains(t, rows[capacity+1], txnConflictChanged)
		require.Contains(t, rows[capacity+1], "table:42, new-min-snapshot-ts: 0-0")
	case <-ctx.Done():
		t.Fatal("trace consumer did not persist all accepted events")
	}
}

type traceBarrierEvent struct{ before func() }

func (e traceBarrierEvent) toCSVRecord(cn string, buf *buffer, records []string) {
	e.before()
	txnEvent{eventType: txnCreateEvent}.toCSVRecord(cn, buf, records)
}

type tracePressureTxn struct {
	client.TxnOperator
	attempted chan struct{}
}

func (op tracePressureTxn) Txn() txn.TxnMeta {
	close(op.attempted)
	return txn.TxnMeta{ID: []byte{1}}
}

func Test_updateState(t *testing.T) {
	exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if strings.HasPrefix(sql, "update trace_features set state = 'running' where name = 'data'") {
			return executor.Result{}, context.DeadlineExceeded
		}
		return executor.Result{}, nil
	})

	serv := &service{
		clock:    clock.NewHLCClock(func() int64 { return 0 }, 0),
		executor: exec,
	}
	err := serv.updateState(FeatureTraceData, "running")
	assert.Error(t, err)
}
