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

//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/stretchr/testify/require"
	"github.com/ti-mo/conntrack"
	"github.com/ti-mo/netfilter"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"golang.org/x/sys/unix"
)

type failingConntrackClient struct {
	closed  bool
	workers uint8
}

func (c *failingConntrackClient) Close() error {
	c.closed = true
	return nil
}

func (c *failingConntrackClient) Listen(_ chan<- conntrack.Event, workers uint8, _ []netfilter.NetlinkGroup) (chan error, error) {
	c.workers = workers
	return nil, errors.New("listen failed")
}

func TestStartConnectionTrackingClosesOnListenFailure(t *testing.T) {
	originalDial := dialConntrack
	t.Cleanup(func() {
		dialConntrack = originalDial
	})

	client := new(failingConntrackClient)
	dialConntrack = func() (conntrackClient, error) {
		return client, nil
	}

	err := startConnectionTracking()
	if err == nil {
		t.Fatal("expected Listen failure")
	}
	if !client.closed {
		t.Fatal("expected conntrack client to be closed after Listen failure")
	}
	// Concurrent listeners can deliver DESTROY before NEW and leave the
	// destroyed flow in activeConns. Pin the ordering contract at startup.
	if client.workers != 1 {
		t.Fatalf("expected one ordered conntrack listener, got %d workers", client.workers)
	}
}

func TestDialConntrack(t *testing.T) {
	client, err := dialConntrack()
	if err != nil {
		t.Logf("conntrack unavailable in test environment: %v", err)
		return
	}
	if client == nil {
		t.Fatal("dialConntrack returned a nil client without an error")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close conntrack client: %v", err)
	}
}

func TestConntrack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := observeConntrack(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		events, errs := make(chan conntrack.Event), make(chan error)
		done := make(chan error, 1)
		go func() { done <- logConnTrack(ctx, events, errs) }()
		for id := uint32(0); id < connsThreshold; id++ {
			events <- conntrackEvent(false, id)
		}
		synctest.Wait()
		// Advance the fake clock, not a wall-clock synchronization sleep.
		time.Sleep(conntrackReportInterval)
		synctest.Wait()
		requireConntrackCount(t, logs, connsThreshold)
		require.Len(t, logs.FilterMessage("conntrack: source").All(), 1)
		require.Len(t, logs.FilterMessage("conntrack: destination").All(), 1)
		for id := uint32(0); id < connsThreshold; id++ {
			events <- conntrackEvent(true, id)
		}
		synctest.Wait()
		time.Sleep(conntrackReportInterval)
		synctest.Wait()
		requireConntrackCount(t, logs, 0)
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
	})
}

func conntrackEvent(destroy bool, id uint32) conntrack.Event {
	kind := conntrack.EventNew
	if destroy {
		kind = conntrack.EventDestroy
	}
	return conntrack.Event{Type: kind, Flow: &conntrack.Flow{ID: id, TupleOrig: conntrack.Tuple{
		Proto: conntrack.ProtoTuple{SourcePort: 42, DestinationPort: 41},
	}}}
}

func observeConntrack(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	oldReport, oldContext := logutil.GetReportZapFunc(), logutil.GetContextFieldFunc()
	t.Cleanup(func() { logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: oldReport, ContextField: oldContext}) })
	pool := buffer.NewPool()
	logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: func(_ zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
		return pool.Get(), core.Write(entry, fields)
	}})
	restore := logutil.SetupMOLoggerForTest(&logutil.LogConfig{Level: "info", DisableLog: true})
	t.Cleanup(func() { restore() })
	return logs
}

func requireConntrackCount(t *testing.T, logs *observer.ObservedLogs, count int) {
	t.Helper()
	stats := logs.FilterMessage("conntrack: stats").All()
	require.NotEmpty(t, stats)
	fields := stats[len(stats)-1].ContextMap()
	require.EqualValues(t, count, fields["connections"])
	require.Equal(t, "since_listener_start", fields["scope"])
	require.Equal(t, false, fields["complete"])
	require.Contains(t, fields, "listener_started_at")
}

func TestConntrackListenerTerminal(t *testing.T) {
	for _, terminal := range []string{"ENOBUFS", "decode error", "events closed", "errors closed", "nil error"} {
		t.Run(terminal, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logs := observeConntrack(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				events, errs := make(chan conntrack.Event), make(chan error)
				done := make(chan error, 1)
				go func() { done <- logConnTrack(ctx, events, errs) }()
				events <- conntrackEvent(false, 1)
				synctest.Wait()
				time.Sleep(conntrackReportInterval)
				synctest.Wait()
				requireConntrackCount(t, logs, 1)
				var want error
				switch terminal {
				case "ENOBUFS":
					want = fmt.Errorf("receive: %w", unix.ENOBUFS)
					errs <- want
				case "decode error":
					want = errors.New("decode failed")
					errs <- want
				case "events closed":
					close(events)
				case "errors closed":
					close(errs)
				case "nil error":
					errs <- nil
				}
				err := <-done
				require.Error(t, err)
				if want != nil {
					require.ErrorIs(t, err, want)
				}
				count := logs.FilterMessage("conntrack: stats").Len()
				time.Sleep(3 * conntrackReportInterval)
				synctest.Wait()
				require.Equal(t, count, logs.FilterMessage("conntrack: stats").Len(), "dead listener must not report stale counts")
			})
		})
	}
}

type channelConntrackClient struct {
	events     chan<- conntrack.Event
	errs       chan error
	listenErr  error
	closeErr   error
	closeCount int
	onClose    func()
	workers    uint8
}

func (c *channelConntrackClient) Listen(events chan<- conntrack.Event, workers uint8, _ []netfilter.NetlinkGroup) (chan error, error) {
	c.events, c.workers = events, workers
	return c.errs, c.listenErr
}

func (c *channelConntrackClient) Close() error {
	c.closeCount++
	if c.onClose != nil {
		c.onClose()
	}
	return c.closeErr
}

func TestConnectionTrackingRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := observeConntrack(t)
		first := &channelConntrackClient{errs: make(chan error)}
		failed := &channelConntrackClient{listenErr: errors.New("join group failed")}
		second := &channelConntrackClient{errs: make(chan error)}
		third := &channelConntrackClient{errs: make(chan error)}
		var calls atomic.Int32
		dial := func() (conntrackClient, error) {
			switch calls.Add(1) {
			case 1:
				return first, nil
			case 2:
				return nil, errors.New("dial failed")
			case 3:
				return failed, nil
			case 4:
				return second, nil
			case 5:
				return third, nil
			default:
				return nil, errors.New("unexpected dial")
			}
		}
		listener, err := listenConntrack(dial)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); runConnectionTracking(ctx, listener, dial) }()
		stop := func() { cancel(); <-done }
		t.Cleanup(stop)
		first.events <- conntrackEvent(false, 1)
		synctest.Wait()
		time.Sleep(conntrackReportInterval)
		synctest.Wait()
		requireConntrackCount(t, logs, 1)
		first.errs <- fmt.Errorf("receive: %w", unix.ENOBUFS)
		synctest.Wait()
		require.Equal(t, 1, first.closeCount)
		require.EqualValues(t, 1, calls.Load(), "restart must be rate limited")
		// Queued events from the old listener must never enter the new map.
		first.events <- conntrackEvent(false, 99)
		statsBefore := logs.FilterMessage("conntrack: stats").Len()
		for want := 2; want <= 4; want++ {
			time.Sleep(conntrackRetryInterval)
			synctest.Wait()
			require.EqualValues(t, want, calls.Load())
			require.Equal(t, statsBefore, logs.FilterMessage("conntrack: stats").Len())
		}
		require.Equal(t, 1, failed.closeCount)
		require.Equal(t, uint8(1), second.workers)
		second.events <- conntrackEvent(false, 2)
		synctest.Wait()
		time.Sleep(conntrackReportInterval)
		synctest.Wait()
		requireConntrackCount(t, logs, 1)
		second.events <- conntrackEvent(true, 2)
		synctest.Wait()
		time.Sleep(conntrackReportInterval)
		synctest.Wait()
		requireConntrackCount(t, logs, 0)
		second.errs <- unix.ENOBUFS
		synctest.Wait()
		time.Sleep(conntrackRetryInterval)
		synctest.Wait()
		require.EqualValues(t, 5, calls.Load())
		require.Equal(t, 1, second.closeCount)
		stop()
		stop() // repeated stop has one effective Close owner
		require.Equal(t, 1, third.closeCount)
		time.Sleep(3 * conntrackRetryInterval)
		synctest.Wait()
		require.EqualValues(t, 5, calls.Load())
	})
}

func TestConnectionTrackingStopsDuringRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client := &channelConntrackClient{errs: make(chan error)}
		calls := 0
		dial := func() (conntrackClient, error) { calls++; return client, nil }
		listener, err := listenConntrack(dial)
		require.NoError(t, err)
		done := make(chan struct{})
		go func() { defer close(done); runConnectionTracking(ctx, listener, dial) }()
		client.errs <- unix.ENOBUFS
		synctest.Wait()
		cancel()
		<-done
		time.Sleep(2 * conntrackRetryInterval)
		synctest.Wait()
		require.Equal(t, 1, calls)
		require.Equal(t, 1, client.closeCount)
	})
}

func TestConntrackCloseDrainsPublishers(t *testing.T) {
	for _, pending := range []string{"event", "error", "closed channels"} {
		t.Run(pending, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				events, errs := make(chan conntrack.Event), make(chan error)
				published := make(chan struct{})
				go func() {
					defer close(published)
					switch pending {
					case "event":
						events <- conntrackEvent(false, 1)
					case "error":
						errs <- unix.ENOBUFS
					case "closed channels":
						close(events)
						close(errs)
					}
				}()
				synctest.Wait() // publisher is blocked, as in conntrack.eventWorker
				client := &channelConntrackClient{onClose: func() { <-published }}
				listener := &conntrackListener{client: client, events: events, errors: errs}
				require.NoError(t, listener.close())
				require.Equal(t, 1, client.closeCount)
			})
		})
	}
}

func TestConnectionTrackingCloseFailureDoesNotReopen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &channelConntrackClient{errs: make(chan error), closeErr: errors.New("close failed")}
		calls := 0
		dial := func() (conntrackClient, error) { calls++; return client, nil }
		listener, err := listenConntrack(dial)
		require.NoError(t, err)
		done := make(chan struct{})
		go func() { defer close(done); runConnectionTracking(context.Background(), listener, dial) }()
		client.errs <- unix.ENOBUFS
		<-done
		time.Sleep(2 * conntrackRetryInterval)
		synctest.Wait()
		require.Equal(t, 1, calls)
		require.Equal(t, 1, client.closeCount)
	})
}

func TestConnectionTrackingFailedListenCloseStopsRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := &channelConntrackClient{errs: make(chan error)}
		failed := &channelConntrackClient{listenErr: errors.New("listen failed"), closeErr: errors.New("close failed")}
		calls := 0
		dial := func() (conntrackClient, error) {
			calls++
			if calls == 1 {
				return first, nil
			}
			return failed, nil
		}
		listener, err := listenConntrack(dial)
		require.NoError(t, err)
		done := make(chan struct{})
		go func() { defer close(done); runConnectionTracking(context.Background(), listener, dial) }()
		first.errs <- unix.ENOBUFS
		<-done
		time.Sleep(2 * conntrackRetryInterval)
		synctest.Wait()
		require.Equal(t, 2, calls)
		require.Equal(t, 1, first.closeCount)
		require.Equal(t, 1, failed.closeCount)
	})
}
