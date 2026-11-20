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

package motrace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/prashantv/gostub"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

type lockReadTestEvent struct {
	fields []byte
	line   []byte
}

func recordLockReadEvents(t *testing.T, threshold float64) func() []lockReadTestEvent {
	t.Helper()
	provider := GetTracerProvider()
	SetTracerProvider(newMOTracerProvider(EnableTracer(true), WithLongQueryTime(threshold)))
	t.Cleanup(func() { SetTracerProvider(provider) })
	stub := gostub.Stub(&ReportStatement, func(context.Context, *StatementInfo) error { return nil })
	t.Cleanup(stub.Reset)
	previous := logutil.GetReportZapFunc()
	t.Cleanup(func() { logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: previous}) })

	var mu sync.Mutex
	var events []lockReadTestEvent
	logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: func(enc zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
		if entry.Message != lockReadDiagnosticMessage {
			return previous(enc, entry, fields)
		}
		payload, err := enc.EncodeEntry(entry, fields)
		if err != nil {
			return nil, err
		}
		config := zapcore.EncoderConfig{
			MessageKey: "msg", LevelKey: "level", TimeKey: "time", CallerKey: "caller", StacktraceKey: "stacktrace",
			EncodeLevel: zapcore.CapitalLevelEncoder, EncodeTime: zapcore.ISO8601TimeEncoder, EncodeCaller: zapcore.ShortCallerEncoder,
		}
		line, err := zapcore.NewJSONEncoder(config).EncodeEntry(entry, fields)
		if err != nil {
			payload.Free()
			return nil, err
		}
		defer line.Free()
		mu.Lock()
		events = append(events, lockReadTestEvent{fields: append([]byte(nil), payload.Bytes()...), line: append([]byte(nil), line.Bytes()...)})
		mu.Unlock()
		return payload, nil
	}})
	return func() []lockReadTestEvent {
		mu.Lock()
		defer mu.Unlock()
		return append([]lockReadTestEvent(nil), events...)
	}
}

func prepareLockReadStatement(limiter *LockReadLimiter, id uuid.UUID, duration time.Duration) (*StatementInfo, *resource.Root) {
	s := &StatementInfo{StatementID: id, ResponseAt: time.Now(), Duration: duration, Statement: []byte("private SQL and primary key")}
	s.DisableAgg()
	root := resource.NewRoot(resource.ConnExternal)
	s.SetResourceRoot(root)
	s.SetLockReadLimiter(limiter)
	root.MergeExecution(resource.ExecutionSummary{AttemptCount: 2})
	return s, root
}

func TestLockReadSubsecondTerminalEligibility(t *testing.T) {
	readEvents := recordLockReadEvents(t, 0)
	ctx := context.Background()
	at := time.Now()
	limiter := newLockReadLimiter(func() time.Time { return at }, 1, 4)
	suppressed := testutil.ToFloat64(v2.SlowLockDiagnosticsSuppressedCounter)
	remote := moerr.NewRemoteLockWaitTimeoutNoCtx()
	for _, tc := range []struct {
		name                  string
		duration              time.Duration
		phaseErr, terminalErr error
		admit                 bool
	}{
		{name: "fast success", duration: 200 * time.Millisecond},
		{name: "floor equality", duration: time.Second},
		{name: "cancel after successful stage", duration: 200 * time.Millisecond, terminalErr: context.Canceled},
		{name: "ordinary error", duration: 200 * time.Millisecond, phaseErr: errors.New("private timeout payload")},
		{name: "fast fail conflict", duration: 200 * time.Millisecond, phaseErr: moerr.NewLockConflictNoCtx()},
		{name: "remote owner", duration: 200 * time.Millisecond, phaseErr: remote, admit: true},
		{name: "wrapped remote owner", duration: 200 * time.Millisecond, phaseErr: fmt.Errorf("private wrapper: %w", remote), admit: true},
		{name: "phase cancel", duration: 200 * time.Millisecond, phaseErr: fmt.Errorf("wrapped: %w", context.Canceled), admit: true},
		{name: "strictly slow success", duration: time.Second + 1, admit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, root := prepareLockReadStatement(limiter, uuid.New(), tc.duration)
			t.Cleanup(s.FreeExecPlan)
			observation := &resource.LockReadObservation{}
			observation.Record(ctx, resource.LockReadAcquisition, at, at.Add(100*time.Millisecond), tc.phaseErr)
			observation.ObserveAcquisitionConflict(false, tc.phaseErr)
			root.MergeLockRead(observation)
			require.Nil(t, root.PreResponseSummary().LockRead)
			count := len(readEvents())
			err := tc.phaseErr
			if tc.terminalErr != nil {
				err = tc.terminalErr
			}
			s.EndStatement(ctx, err, 0, 0, 0)
			s.EndStatement(ctx, err, 0, 0, 0)
			require.NoError(t, ctx.Err())
			require.NoError(t, context.Cause(ctx))
			require.Equal(t, err, s.Error)
			require.Equal(t, suppressed, testutil.ToFloat64(v2.SlowLockDiagnosticsSuppressedCounter))
			events := readEvents()
			if !tc.admit {
				require.Len(t, events, count)
				require.Nil(t, s.resourceSummary.LockRead)
				return
			}
			require.Len(t, events, count+1)
			var event struct {
				LockRead resource.LockReadDiagnostic `json:"lock_read"`
			}
			require.NoError(t, json.Unmarshal(events[count].fields, &event))
			require.Equal(t, *s.resourceSummary.LockRead, event.LockRead)
			require.Equal(t, uuid.UUID(s.StatementID), event.LockRead.StatementID)
			require.Equal(t, uint64(2), event.LockRead.AttemptCount)
			require.Equal(t, uint64(100*time.Millisecond), event.LockRead.AcquisitionNS)
			require.Empty(t, event.LockRead.ConflictTxnID)
			if tc.phaseErr != nil {
				require.Equal(t, "acquisition", event.LockRead.LastIncomplete)
				require.Equal(t, "failed", event.LockRead.Outcome)
			} else {
				require.Empty(t, event.LockRead.LastIncomplete)
				require.Equal(t, "success", event.LockRead.Outcome)
			}
		})
	}
}

func TestLockReadCNBudgetAndEventSize(t *testing.T) {
	readEvents := recordLockReadEvents(t, 0)
	ctx := context.Background()
	at := time.Now()
	now := at.Add(time.Second)
	var clockReads int
	limiter := newLockReadLimiter(func() time.Time {
		clockReads++
		if clockReads == 2 {
			return at
		}
		return now
	}, 1, 4)
	suppressed := testutil.ToFloat64(v2.SlowLockDiagnosticsSuppressedCounter)
	finish := func(limiter *LockReadLimiter, id byte) {
		idValue := uuid.UUID{15: id}
		s, root := prepareLockReadStatement(limiter, idValue, 200*time.Millisecond)
		defer s.FreeExecPlan()
		root.MergeExecution(resource.ExecutionSummary{AttemptCount: math.MaxUint64})
		observation := &resource.LockReadObservation{}
		err := fmt.Errorf("private payload: %w", context.Canceled)
		for _, stage := range []resource.LockReadStage{resource.LockReadAcquisition, resource.LockReadLogtail, resource.LockReadPKReadiness, resource.LockReadPKCheck} {
			for range 3 {
				observation.Record(ctx, stage, at, at.Add(math.MaxInt64), err)
			}
		}
		resource.ObserveLocalLockConflict(resource.ContextWithLockReadAcquisition(ctx, observation), [][]byte{make([]byte, 16)})
		observation.ObserveAcquisitionConflict(false, err)
		root.MergeLockRead(observation)
		s.EndStatement(ctx, nil, 0, 0, 0)
		s.EndStatement(ctx, nil, 0, 0, 0)
	}
	var workers sync.WaitGroup
	release := make(chan struct{})
	for id := byte(1); id <= 6; id++ {
		workers.Go(func() {
			<-release
			finish(limiter, id)
		})
	}
	close(release)
	workers.Wait()
	events := readEvents()
	require.Len(t, events, 4)
	require.Equal(t, suppressed+2, testutil.ToFloat64(v2.SlowLockDiagnosticsSuppressedCounter))
	seen := map[uuid.UUID]bool{}
	for _, event := range events {
		require.LessOrEqual(t, len(event.fields), 1024)
		require.LessOrEqual(t, len(event.line), 1024)
		require.NotContains(t, string(event.line), "private")
		require.NotContains(t, string(event.line), "stacktrace")
		var raw map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(event.fields, &raw))
		require.Len(t, raw, 1)
		var d resource.LockReadDiagnostic
		require.NoError(t, json.Unmarshal(raw["lock_read"], &d))
		require.False(t, seen[d.StatementID])
		seen[d.StatementID] = true
		require.Equal(t, uint64(math.MaxUint64), d.AttemptCount)
		require.Equal(t, uint64(math.MaxUint64), d.PKReadinessNS)
		require.Len(t, d.ConflictTxnID, 32)
		require.Equal(t, "success", d.Outcome, "a successful retry keeps the final outcome")
	}
	now = now.Add(time.Second)
	finish(limiter, 7)
	finish(limiter, 8)
	require.Len(t, readEvents(), 5)
	require.Equal(t, suppressed+3, testutil.ToFloat64(v2.SlowLockDiagnosticsSuppressedCounter))

	otherCN := newLockReadLimiter(func() time.Time { return now }, 1, 4)
	for id := byte(9); id <= 12; id++ {
		finish(otherCN, id)
	}
	require.Len(t, readEvents(), 9)
	require.Equal(t, suppressed+3, testutil.ToFloat64(v2.SlowLockDiagnosticsSuppressedCounter))
}
