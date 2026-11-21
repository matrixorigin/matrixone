// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
)

type traceOwnershipEvent struct {
	session, statement, extra string
	account, user, kind       string
}

// Match only this test's items. Other goroutines keep using the original
// processor, including its acknowledgement and item-ownership behavior.
type traceOwnershipCollector struct {
	delegate BatchProcessor
	marker   string
	mu       sync.Mutex
	events   []traceOwnershipEvent
}

func (c *traceOwnershipCollector) Collect(ctx context.Context, item batchpipe.HasName) error {
	var event traceOwnershipEvent
	switch item := item.(type) {
	case *MOZapLog:
		if item.Message != c.marker {
			return c.delegate.Collect(ctx, item)
		}
		event = traceOwnershipEvent{session: item.SessionID, statement: item.StatementID, extra: item.Extra}
		item.Free()
	case *StatementInfo:
		if string(item.Statement) != c.marker {
			return c.delegate.Collect(ctx, item)
		}
		event = traceOwnershipEvent{account: item.Account, user: item.User, kind: item.StatementType}
		item.freeNoLocked()
	default:
		return c.delegate.Collect(ctx, item)
	}
	c.mu.Lock()
	c.events = append(c.events, event)
	c.mu.Unlock()
	return nil
}

func (c *traceOwnershipCollector) DiscardableCollect(ctx context.Context, item batchpipe.HasName) error {
	if log, ok := item.(*MOZapLog); ok && log.Message == c.marker {
		return c.Collect(ctx, item)
	}
	if stmt, ok := item.(*StatementInfo); ok && string(stmt.Statement) == c.marker {
		return c.Collect(ctx, item)
	}
	if discardable, ok := c.delegate.(DiscardableCollector); ok {
		return discardable.DiscardableCollect(ctx, item)
	}
	return c.delegate.Collect(ctx, item)
}

func (c *traceOwnershipCollector) Start() bool { return c.delegate.Start() }
func (c *traceOwnershipCollector) Stop(graceful bool) error {
	return c.delegate.Stop(graceful)
}
func (c *traceOwnershipCollector) Register(name batchpipe.HasName, impl PipeImpl) {
	c.delegate.Register(name, impl)
}

func (c *traceOwnershipCollector) snapshot() []traceOwnershipEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]traceOwnershipEvent(nil), c.events...)
}

type traceOwnershipNormalDelegate struct {
	mu                   sync.Mutex
	calls, starts, stops int
	registrations        int
}

func (d *traceOwnershipNormalDelegate) Collect(ctx context.Context, item batchpipe.HasName) error {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	if syncer, ok := item.(*ItemSyncer); ok {
		syncer.GetAckHook()(ctx)
	}
	return nil
}
func (d *traceOwnershipNormalDelegate) Start() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.starts++
	return true
}
func (d *traceOwnershipNormalDelegate) Stop(bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stops++
	return nil
}
func (d *traceOwnershipNormalDelegate) Register(batchpipe.HasName, PipeImpl) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.registrations++
}

type traceOwnershipDiscardDelegate struct {
	traceOwnershipNormalDelegate
	discarded int
}

func (d *traceOwnershipDiscardDelegate) DiscardableCollect(_ context.Context, _ batchpipe.HasName) error {
	d.mu.Lock()
	d.discarded++
	d.mu.Unlock()
	return nil
}

func TestTraceOwnershipCollectorDelegatesAndAcknowledges(t *testing.T) {
	ctx := context.Background()
	delegate := &traceOwnershipDiscardDelegate{}
	collector := &traceOwnershipCollector{delegate: delegate, marker: "owned"}
	unrelated := &MOZapLog{Message: "unrelated"}
	require.NoError(t, collector.Collect(ctx, unrelated))
	require.NoError(t, collector.DiscardableCollect(ctx, unrelated))
	syncer := NewItemSyncer(newMOZap())
	t.Cleanup(syncer.Free)
	require.NoError(t, collector.Collect(ctx, syncer))
	select {
	case <-syncer.ch:
	default:
		t.Fatal("the delegated synchronous item was not acknowledged")
	}
	require.Equal(t, 2, delegate.calls)
	require.Equal(t, 1, delegate.discarded)
	require.True(t, collector.Start())
	require.NoError(t, collector.Stop(false))
	collector.Register(unrelated, nil)
	require.Equal(t, 1, delegate.starts)
	require.Equal(t, 1, delegate.stops)
	require.Equal(t, 1, delegate.registrations)

	normalOnly := &traceOwnershipNormalDelegate{}
	collector.delegate = normalOnly
	require.NoError(t, collector.DiscardableCollect(ctx, unrelated))
	require.Equal(t, 1, normalOnly.calls, "a delegate without a discardable path must use Collect")
}

func installTraceOwnershipCollector(t *testing.T) *traceOwnershipCollector {
	t.Helper()
	previous := GetTracerProvider()
	// Fail when the config grows so this test cannot silently install a
	// provider with different behavior for unrelated background reporters.
	wantFields := []string{
		"enable", "service", "idGenerator", "resource", "disableError", "batchProcessor",
		"writerFactory", "disableSqlWriter", "disableStmtAggregation", "enableStmtMerge",
		"aggregationWindow", "selectAggrThreshold", "sqlExecutor", "needInit", "exportInterval",
		"longQueryTime", "compactStatementDiagnostics", "skipRunningStmt", "bufferSizeThreshold",
		"cuConfig", "cuConfigV1", "tcpPacket", "MaxLogMessageSize", "MaxStatementSize", "labels", "mux",
	}
	typ := reflect.TypeOf(tracerProviderConfig{})
	var fields []string
	for i := 0; i < typ.NumField(); i++ {
		fields = append(fields, typ.Field(i).Name)
	}
	require.Equal(t, wantFields, fields, "update the provider snapshot when its config changes")

	clone := new(MOTracerProvider)
	src, dst := &previous.tracerProviderConfig, &clone.tracerProviderConfig
	src.mux.RLock()
	dst.enable = src.enable
	dst.service = src.service
	dst.idGenerator = src.idGenerator
	// Resource and labels are initialized before the provider is published;
	// this test and its delegate only read the shared values.
	dst.resource = src.resource
	dst.disableError = src.disableError
	dst.writerFactory = src.writerFactory
	dst.disableSqlWriter = src.disableSqlWriter
	dst.disableStmtAggregation = src.disableStmtAggregation
	dst.enableStmtMerge = src.enableStmtMerge
	dst.aggregationWindow = src.aggregationWindow
	dst.selectAggrThreshold = src.selectAggrThreshold
	dst.sqlExecutor = src.sqlExecutor
	dst.needInit = src.needInit
	dst.exportInterval = src.exportInterval
	dst.longQueryTime = src.longQueryTime
	dst.compactStatementDiagnostics = src.compactStatementDiagnostics
	dst.skipRunningStmt = src.skipRunningStmt
	dst.bufferSizeThreshold = src.bufferSizeThreshold
	dst.cuConfig = src.cuConfig
	dst.cuConfigV1 = src.cuConfigV1
	dst.tcpPacket = src.tcpPacket
	dst.MaxLogMessageSize = src.MaxLogMessageSize
	dst.MaxStatementSize = src.MaxStatementSize
	dst.labels = src.labels
	delegate := src.batchProcessor
	src.mux.RUnlock()
	require.True(t, dst.enable, "the owner package must already have tracing enabled")
	require.NotNil(t, delegate)
	collector := &traceOwnershipCollector{delegate: delegate, marker: t.Name()}
	dst.batchProcessor = collector
	require.True(t, gTracerProvider.CompareAndSwap(previous, clone), "provider changed before the test acquired it")
	t.Cleanup(func() {
		if !gTracerProvider.CompareAndSwap(clone, previous) {
			t.Error("provider changed while the test owned its collector")
			return
		}
		if !clone.IsEnable() || !previous.IsEnable() {
			t.Error("provider enable state changed during the test")
		}
	})
	return collector
}

func TestReportZapExtractsTraceIDs(t *testing.T) {
	collector := installTraceOwnershipCollector(t)
	cases := []struct {
		name      string
		fields    func() []zapcore.Field
		session   string
		statement string
	}{
		{"none", func() []zapcore.Field { return []zapcore.Field{zap.String("payload", "keep")} }, "", ""},
		{"session first", func() []zapcore.Field {
			return []zapcore.Field{zap.String(sessionId, "session"), zap.String("payload", "keep")}
		}, "session", ""},
		{"session last", func() []zapcore.Field {
			return []zapcore.Field{zap.String("payload", "keep"), zap.String(sessionId, "session")}
		}, "session", ""},
		{"statement first", func() []zapcore.Field {
			return []zapcore.Field{zap.String(statementId, "statement"), zap.String("payload", "keep")}
		}, "", "statement"},
		{"statement last", func() []zapcore.Field {
			return []zapcore.Field{zap.String("payload", "keep"), zap.String(statementId, "statement")}
		}, "", "statement"},
		{"both", func() []zapcore.Field {
			return []zapcore.Field{zap.String(sessionId, "session"), zap.String(statementId, "statement"), zap.String("payload", "keep")}
		}, "session", "statement"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := len(collector.snapshot())
			encoder := zapcore.NewJSONEncoder(zapcore.EncoderConfig{SkipLineEnding: true})
			buf, err := ReportZap(encoder, zapcore.Entry{Level: zapcore.InfoLevel, Message: collector.marker}, tc.fields())
			require.NoError(t, err)
			require.NotNil(t, buf)
			defer buf.Free()
			var returned map[string]any
			require.NoError(t, json.Unmarshal(buf.Bytes(), &returned))
			require.Equal(t, map[string]any{"payload": "keep"}, returned)
			events := collector.snapshot()
			require.Len(t, events, before+1)
			require.Equal(t, tc.session, events[before].session)
			require.Equal(t, tc.statement, events[before].statement)
			var extra map[string]any
			require.NoError(t, json.Unmarshal([]byte(events[before].extra), &extra))
			require.Equal(t, returned, extra)
		})
	}
}

func TestReportStatementInternalAccountFilter(t *testing.T) {
	collector := installTraceOwnershipCollector(t)
	cases := []struct {
		account, user, kind string
		want                bool
	}{
		{"sys", "internal", "Commit", false},
		{"sys", "internal", "Start Transaction", false},
		{"sys", "internal", "Use", false},
		{"sys", "internal", "Select", true},
		{"other", "internal", "Commit", true},
		{"sys", "other", "Commit", true},
	}
	wantCount := 0
	for _, tc := range cases {
		t.Run(tc.account+"/"+tc.user+"/"+tc.kind, func(t *testing.T) {
			stmt := NewStatementInfo()
			if cap(stmt.Statement) < 1024 {
				stmt.Statement = make([]byte, 0, 1024)
			}
			originalCapacity := cap(stmt.Statement)
			longSQL := strings.Repeat("q", 1024)
			stmt.RecordStatementSql(longSQL, longSQL)
			require.Equal(t, longSQL, string(stmt.Statement))
			stmt.Statement = append(stmt.Statement[:0], collector.marker...)
			require.GreaterOrEqual(t, cap(stmt.Statement), originalCapacity)
			stmt.Account, stmt.User, stmt.StatementType = tc.account, tc.user, tc.kind
			stmt.Status = StatementStatusSuccess
			stmt.end = true
			require.NoError(t, ReportStatement(context.Background(), stmt))
			if tc.want {
				wantCount++
			}
			events := collector.snapshot()
			require.Len(t, events, wantCount)
			if tc.want {
				got := events[len(events)-1]
				require.Equal(t, tc.account, got.account)
				require.Equal(t, tc.user, got.user)
				require.Equal(t, tc.kind, got.kind)
			}
		})
	}
}
