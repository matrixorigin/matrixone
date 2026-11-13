// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

const diagnosticDB, diagnosticCold, diagnosticWarm = uint64(1000), uint64(100), uint64(200)

func subscriptionDiagnosticsFixture(t *testing.T) *PushClient {
	t.Helper()
	e := &Engine{service: "subscription-diagnostics", partitions: make(map[[2]uint64]*logtailreplay.Partition), globalStats: &GlobalStats{tailC: make(chan *logtail.TableLogtail, 1)}}
	c := &e.pClient
	c.eng, c.subscriber = e, newLogTailSubscriber()
	c.subscriber.setReady()
	c.receivedLogTailTime.ready.Store(true)
	c.subscribed.eng = e
	c.subscribed.m = map[uint64]*subEntry{diagnosticWarm: {dbID: diagnosticDB, state: Subscribed}}
	c.subscribed.m[diagnosticWarm].lastTs.Store(time.Now().UnixNano())
	e.GetOrCreateLatestPart(t.Context(), 0, diagnosticDB, diagnosticWarm)
	return c
}

type subscriptionDiagnosticEvent struct {
	entry  zapcore.Entry
	fields []zapcore.Field
}

func (e subscriptionDiagnosticEvent) table(dbID, tableID uint64) bool {
	var db, table uint64
	var hasDB, hasTable bool
	for _, field := range e.fields {
		switch field.Key {
		case "db-id":
			db, hasDB = uint64(field.Integer), true
		case "table-id", "tbl-id":
			table, hasTable = uint64(field.Integer), true
		}
	}
	if hasDB || hasTable {
		return hasDB && hasTable && db == dbID && table == tableID
	}
	for _, target := range []string{
		fmt.Sprintf("%d-%d", dbID, tableID),
		fmt.Sprintf("[db: %d, tbl: %d]", dbID, tableID),
		fmt.Sprintf("[dbId: %d, tblId: %d]", dbID, tableID),
	} {
		if strings.Contains(e.entry.Message, target) {
			return true
		}
	}
	return false
}

// A blocked diagnostic must not retain the subscription lock or stop another
// table's real partition lookup. The optional terminal path also checks removal
// of the old send-only diagnostic without using a timeout as an absence oracle.
func checkSubscriptionDiagnosticBackpressure(t *testing.T, c *PushClient, optional bool,
	matches func(subscriptionDiagnosticEvent) bool, action func(context.Context) error,
	check func(subscriptionDiagnosticEvent),
) ([]subscriptionDiagnosticEvent, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	entered := make(chan subscriptionDiagnosticEvent, 1)
	release := make(chan struct{})
	var releaseOnce, blockOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	previous := logutil.GetReportZapFunc()
	var probe atomic.Bool
	var recordsMu sync.Mutex
	var records []subscriptionDiagnosticEvent
	logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: func(enc zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
		if entry.Message == "subscription-diagnostics-probe" {
			probe.Store(true)
		}
		event := subscriptionDiagnosticEvent{entry: entry, fields: fields}
		if matches(event) {
			event.fields = append([]zapcore.Field(nil), fields...)
			recordsMu.Lock()
			records = append(records, event)
			recordsMu.Unlock()
			blockOnce.Do(func() { entered <- event; <-release })
		}
		return previous(enc, entry, fields)
	}})
	done := make(chan error, 1)
	joined := false
	started := false
	t.Cleanup(func() {
		cancel()
		unblock()
		if started && !joined {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("subscription operation did not terminate")
			}
		}
		logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: previous})
	})
	logutil.Info("subscription-diagnostics-probe")
	require.True(t, probe.Load(), "the real synchronous reporter must be enabled")
	started = true
	go func() { done <- action(ctx) }()
	var result error
	select {
	case event := <-entered:
		unlocked := c.subscribed.rw.TryRLock()
		if unlocked {
			c.subscribed.rw.RUnlock()
		}
		require.True(t, unlocked, "diagnostic backpressure retained the cross-table subscription lock")
		want := c.eng.partitions[[2]uint64{diagnosticDB, diagnosticWarm}].Snapshot()
		part, ok, state, pending := c.isSubscribedWithPendingAccessTime(ctx, 0, diagnosticDB, diagnosticWarm, nil)
		require.True(t, ok)
		require.Equal(t, Subscribed, state)
		require.False(t, pending)
		require.Same(t, want, part)
		if check != nil {
			check(event)
		}
		unblock()
		select {
		case result = <-done:
			joined = true
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	case result = <-done:
		joined = true
		require.True(t, optional, "operation returned without exercising its retained diagnostic")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	recordsMu.Lock()
	defer recordsMu.Unlock()
	return append([]subscriptionDiagnosticEvent(nil), records...), result
}

func TestSubscriptionDiagnosticsReleaseStateLock(t *testing.T) {
	cfg := logutil.GetDefaultConfig()
	cfg.Level = "debug"
	cfg.DisableLog = true
	logutil.SetupMOLogger(&cfg)
	t.Cleanup(func() { cfg := logutil.GetDefaultConfig(); logutil.SetupMOLogger(&cfg) })
	require.True(t, logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel))
	for _, scenario := range []string{"not found", "subscribed", "response received", "subscribe action", "unsubscribe cleanup", "unsubscribe action", "unsubscribe absent", "unsubscribe sent", "unsubscribe failed", "GC rollback", "subscribe completed"} {
		t.Run(scenario, func(t *testing.T) {
			c := subscriptionDiagnosticsFixture(t)
			transportErr := errors.New("subscription transport failed")
			var sends atomic.Int32
			var sent api.TableID
			action := func(context.Context) error { return nil }
			message := ""
			var wantErr error
			switch scenario {
			case "not found":
				message = "logtail.consumer.set.table.sub.not.exist"
				action = func(context.Context) error {
					c.subscribed.setTableSubNotExist(diagnosticDB, diagnosticCold)
					return nil
				}
			case "subscribed":
				message = "logtail.consumer.set.table.subscribed"
				action = func(context.Context) error { c.subscribed.setTableSubscribed(diagnosticDB, diagnosticCold); return nil }
			case "response received":
				message = "logtail.consumer.set.table.sub.rsp.received"
				action = func(context.Context) error {
					c.subscribed.setTableSubRspReceived(diagnosticDB, diagnosticCold)
					return nil
				}
			case "subscribe action":
				message = "logtail.consumer.set.table.sub.rsp.received"
				action = func(ctx context.Context) error {
					cmd := &cmdToConsumeSub{log: &logtail.SubscribeResponse{Logtail: logtail.TableLogtail{
						Table: &api.TableID{DbId: diagnosticDB, TbId: diagnosticCold, DbName: "diagnostics"},
					}}, receiveAt: time.Now()}
					return cmd.action(ctx, c.eng, nil)
				}
			case "unsubscribe cleanup", "unsubscribe action":
				message = "logtail.consumer.set.table.unsubscribe"
				c.subscribed.m[diagnosticCold] = &subEntry{dbID: diagnosticDB, state: Subscribed}
				c.eng.GetOrCreateLatestPart(t.Context(), 0, diagnosticDB, diagnosticCold)
				c.eng.globalStats.mu.statsInfoMap = map[statsinfo.StatsInfoKey]*statsinfo.StatsInfo{{DatabaseID: diagnosticDB, TableID: diagnosticCold}: nil, {DatabaseID: diagnosticDB, TableID: diagnosticWarm}: nil}
				action = func(ctx context.Context) error {
					if scenario == "unsubscribe action" {
						cmd := &cmdToConsumeUnSub{log: &logtail.UnSubscribeResponse{Table: &api.TableID{DbId: diagnosticDB, TbId: diagnosticCold}}}
						return cmd.action(ctx, c.eng, nil)
					}
					c.subscribed.setTableUnsubscribe(diagnosticDB, diagnosticCold)
					return nil
				}
			case "unsubscribe absent", "unsubscribe sent", "unsubscribe failed":
				message = "send unsubscribe table"
				if scenario == "unsubscribe absent" {
					message = "is not subscribed yet"
				} else {
					c.subscribed.m[diagnosticCold] = &subEntry{dbID: diagnosticDB, state: Subscribed}
				}
				if scenario == "unsubscribe failed" {
					message = "cannot unsubscribe table"
					wantErr = transportErr
				}
				c.subscriber.sendUnSubscribe = func(_ context.Context, id api.TableID) error { sends.Add(1); sent = id; return wantErr }
				action = func(ctx context.Context) error { return c.UnsubscribeTable(ctx, 0, 9999, diagnosticCold) }
			case "GC rollback":
				message = "reverted tbl"
				for id := diagnosticCold; id < diagnosticCold+3; id++ {
					ent := &subEntry{dbID: diagnosticDB, state: Subscribed}
					ent.lastTs.Store(time.Now().Add(-2 * time.Hour).UnixNano())
					c.subscribed.m[id] = ent
				}
				c.subscriber.sendUnSubscribe = func(_ context.Context, id api.TableID) error {
					sends.Add(1)
					if id.TbId == diagnosticCold+1 {
						c.subscribed.clearTable(id.DbId, id.TbId)
					}
					if id.TbId == diagnosticCold+2 {
						c.subscribed.setTableSubscribed(id.DbId, id.TbId)
					}
					return transportErr
				}
				action = func(ctx context.Context) error { c.doGCUnusedTable(ctx); return nil }
			case "subscribe completed":
				message = "-subscribe-ok"
				sentC := make(chan struct{})
				c.subscriber.sendSubscribe = func(_ context.Context, id api.TableID) error { sends.Add(1); sent = id; close(sentC); return nil }
				action = func(ctx context.Context) error {
					responseDone := make(chan struct{})
					go func() {
						defer close(responseDone)
						select {
						case <-sentC:
							c.subscribed.setTableSubscribed(diagnosticDB, diagnosticCold)
						case <-ctx.Done():
						}
					}()
					defer func() { <-responseDone }()
					return c.TryToSubscribeTable(ctx, 0, diagnosticDB, diagnosticCold, "db", "cold")
				}
			}
			responseReceived := scenario == "response received" || scenario == "subscribe action"
			unsubscribeCleanup := scenario == "unsubscribe cleanup" || scenario == "unsubscribe action"
			matches := func(event subscriptionDiagnosticEvent) bool {
				msg := event.entry.Message
				if !(strings.Contains(msg, message) || scenario == "subscribe completed" && strings.Contains(msg, "send-subscribe-ok") || unsubscribeCleanup && strings.HasPrefix(msg, "clean memory table of tbl")) {
					return false
				}
				dbID := diagnosticDB
				if scenario == "unsubscribe absent" {
					dbID = 9999
				}
				if scenario == "GC rollback" {
					// Include all owned jobs so a spurious rollback log cannot hide.
					return event.table(dbID, diagnosticCold) || event.table(dbID, diagnosticCold+1) || event.table(dbID, diagnosticCold+2)
				}
				return event.table(dbID, diagnosticCold)
			}
			started := time.Now()
			events, err := checkSubscriptionDiagnosticBackpressure(t, c, false, matches, action, func(event subscriptionDiagnosticEvent) {
				for _, field := range event.fields {
					require.NotEqual(t, "subscribed-table", field.Key, "diagnostics must not retain the mutable subscription map")
				}
				level := zapcore.InfoLevel
				if scenario == "not found" || scenario == "unsubscribe failed" {
					level = zapcore.ErrorLevel
				}
				require.Equal(t, level, event.entry.Level)
				if scenario == "not found" || scenario == "subscribed" || responseReceived || unsubscribeCleanup || scenario == "subscribe completed" {
					tableKey := "tbl-id"
					if scenario == "subscribe completed" {
						tableKey = "table-id"
					}
					want := map[string]uint64{"db-id": diagnosticDB, tableKey: diagnosticCold}
					for _, field := range event.fields {
						if value, ok := want[field.Key]; ok {
							require.Equal(t, zapcore.Uint64Type, field.Type)
							require.Equal(t, value, uint64(field.Integer))
							delete(want, field.Key)
						}
					}
					require.Empty(t, want, "retained diagnostics must preserve table identity")
				}
				if responseReceived {
					var service *zapcore.Field
					for i := range event.fields {
						if event.fields[i].Key == "service" {
							service = &event.fields[i]
						}
					}
					require.NotNil(t, service)
					require.Equal(t, c.eng.service, service.String)
				}
				if unsubscribeCleanup {
					require.NotContains(t, c.subscribed.m, diagnosticCold)
					require.NotContains(t, c.eng.partitions, [2]uint64{diagnosticDB, diagnosticCold})
					require.NotContains(t, c.eng.globalStats.mu.statsInfoMap, statsinfo.StatsInfoKey{DatabaseID: diagnosticDB, TableID: diagnosticCold})
					require.Contains(t, c.eng.globalStats.mu.statsInfoMap, statsinfo.StatsInfoKey{DatabaseID: diagnosticDB, TableID: diagnosticWarm})
				}
			})
			require.ErrorIs(t, err, wantErr)
			require.Len(t, events, 1)
			switch scenario {
			case "not found":
				require.Equal(t, SubRspTableNotExist, c.subscribed.m[diagnosticCold].state)
			case "subscribed", "subscribe completed":
				require.Equal(t, Subscribed, c.subscribed.m[diagnosticCold].state)
			case "response received", "subscribe action":
				require.Equal(t, SubRspReceived, c.subscribed.m[diagnosticCold].state)
			case "unsubscribe absent":
				require.Zero(t, sends.Load())
				require.Contains(t, events[0].entry.Message, "9999-100")
			case "unsubscribe sent", "unsubscribe failed":
				require.Equal(t, int32(1), sends.Load())
				require.Equal(t, api.TableID{DbId: diagnosticDB, TbId: diagnosticCold}, sent)
				require.Equal(t, Unsubscribing, c.subscribed.m[diagnosticCold].state)
				require.Contains(t, events[0].entry.Message, "1000-100")
			case "GC rollback":
				require.Equal(t, int32(3), sends.Load())
				require.Equal(t, Subscribed, c.subscribed.m[diagnosticCold].state)
				require.NotContains(t, c.subscribed.m, diagnosticCold+1)
				require.Equal(t, Subscribed, c.subscribed.m[diagnosticCold+2].state)
				require.Contains(t, events[0].entry.Message, "tbl: 100]")
				retryAt := c.subscribed.m[diagnosticCold].lastTs.Load()
				require.GreaterOrEqual(t, retryAt, started.Add(-unsubscribeTimer+unsubscribeProcessTicker).UnixNano())
				require.LessOrEqual(t, retryAt, time.Now().Add(-unsubscribeTimer+unsubscribeProcessTicker).UnixNano())
			}
		})
	}
}

func TestSubscriptionStartDiagnostics(t *testing.T) {
	entries := []struct {
		name  string
		start func(context.Context, *PushClient) (SubscribeState, error)
	}{
		{"initial", func(ctx context.Context, c *PushClient) (SubscribeState, error) {
			return c.toSubIfUnsubscribed(ctx, diagnosticDB, diagnosticCold)
		}},
		{"checkpoint retry", func(ctx context.Context, c *PushClient) (SubscribeState, error) {
			return c.loadAndConsumeLatestCkp(ctx, 0, diagnosticCold, "cold", diagnosticDB, "db")
		}},
		{"subscribe retry", func(ctx context.Context, c *PushClient) (SubscribeState, error) {
			_, state, err := c.isNotSubscribing(ctx, diagnosticDB, diagnosticCold)
			return state, err
		}},
		{"unsubscribe retry", func(ctx context.Context, c *PushClient) (SubscribeState, error) {
			_, state, err := c.isNotUnsubscribing(ctx, diagnosticDB, diagnosticCold)
			return state, err
		}},
	}
	for _, entry := range entries {
		for _, outcome := range []string{"success", "send error", "cancelled"} {
			t.Run(entry.name+"/"+outcome, func(t *testing.T) {
				c := subscriptionDiagnosticsFixture(t)
				failure := errors.New("subscription send failed")
				var calls atomic.Int32
				var sent api.TableID
				c.subscriber.sendSubscribe = func(_ context.Context, id api.TableID) error {
					calls.Add(1)
					sent = id
					if outcome == "send error" {
						return failure
					}
					return nil
				}
				var state SubscribeState
				events, err := checkSubscriptionDiagnosticBackpressure(t, c, true, func(event subscriptionDiagnosticEvent) bool {
					return strings.Contains(event.entry.Message, "send-subscribe-ok") && event.table(diagnosticDB, diagnosticCold)
				}, func(ctx context.Context) error {
					if outcome == "cancelled" {
						cancelled, cancel := context.WithCancel(ctx)
						cancel()
						ctx = cancelled
					}
					var err error
					state, err = entry.start(ctx, c)
					return err
				}, nil)
				require.Empty(t, events)
				if outcome == "success" {
					require.NoError(t, err)
					require.Equal(t, Subscribing, state)
					require.Equal(t, Subscribing, c.subscribed.m[diagnosticCold].state)
				} else {
					require.NotContains(t, c.subscribed.m, diagnosticCold)
					if outcome == "send error" {
						require.ErrorIs(t, err, failure)
						require.Equal(t, Unsubscribed, state)
					} else {
						require.ErrorIs(t, err, context.Canceled)
					}
				}
				if outcome == "cancelled" {
					require.Zero(t, calls.Load())
				} else {
					require.Equal(t, int32(1), calls.Load())
					require.Equal(t, api.TableID{DbId: diagnosticDB, TbId: diagnosticCold}, sent)
				}
				require.True(t, c.subscribed.isSubscribed(diagnosticDB, diagnosticWarm))
			})
		}
	}
}
