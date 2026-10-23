// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pcg "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/util/metric"
)

func TestRoutineCancelOnDisconnect(t *testing.T) {
	for _, order := range []string{"before-publication", "after-publication", "concurrent-publication"} {
		t.Run(order, func(t *testing.T) {
			root, cancelRoot := context.WithCancel(context.Background())
			t.Cleanup(cancelRoot)
			request, cancelRequest := context.WithCancel(root)
			t.Cleanup(cancelRequest)
			routine := &Routine{cancelRoutineCtx: root, cancelRoutineFunc: cancelRoot}
			switch order {
			case "before-publication":
				routine.cancelOnDisconnect()
				routine.setCancelRequestFunc(cancelRequest)
			case "after-publication":
				routine.setCancelRequestFunc(cancelRequest)
				routine.cancelOnDisconnect()
			default:
				start := make(chan struct{})
				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					<-start
					routine.setCancelRequestFunc(cancelRequest)
				}()
				go func() {
					defer wg.Done()
					<-start
					routine.cancelOnDisconnect()
				}()
				close(start)
				wg.Wait()
			}
			require.ErrorIs(t, request.Err(), context.Canceled)
			require.True(t, routine.isCancelled())
			require.NoError(t, root.Err(), "rollback still needs the connection/transaction context")
			// Repeated observation is safe, and a later admitted request cannot escape.
			routine.cancelOnDisconnect()
			next, cancelNext := context.WithCancel(root)
			t.Cleanup(cancelNext)
			routine.setCancelRequestFunc(cancelNext)
			require.ErrorIs(t, next.Err(), context.Canceled)
		})
	}
}

func TestRoutineManagerClosedRemovesTenantRoutine(t *testing.T) {
	for _, state := range []string{"disconnect-before-close", "counted", "never-counted", "unauthenticated"} {
		t.Run(state, func(t *testing.T) {
			server, conn, ses, proto := newTestServerWithRoutine(t, 0)
			rm := server.rm
			rt := NewRoutine(context.Background(), proto, getPu("").SV)
			rt.setSession(ses)
			ses.setRoutine(rt)
			rm.setRoutine(conn, 0, rt)
			t.Cleanup(func() { rm.Closed(conn) })

			const accountID = uint32(42)
			accountName := "disconnect_cleanup_" + state
			if state == "unauthenticated" {
				ses.SetTenantInfo(nil)
			} else {
				ses.SetTenantInfo(&TenantInfo{Tenant: accountName, TenantID: accountID})
				rm.accountRoutine.recordRoutine(int64(accountID), rt, 1)
				require.Contains(t, rm.accountRoutine.deepCopyRoutineMap()[int64(accountID)], rt)
			}

			// A second registered routine must survive closing this connection.
			peer := &Routine{}
			peer.protocol.Store(&holder[MysqlRrWr]{value: proto})
			rm.accountRoutine.recordRoutine(int64(accountID), peer, 1)
			t.Cleanup(func() { rm.accountRoutine.deleteRoutine(int64(accountID), peer) })
			gauge := metric.ConnectionCounter(accountName, accountID)
			gauge.Set(0)
			t.Cleanup(func() { gauge.Set(0) })
			if state == "counted" || state == "disconnect-before-close" {
				rt.increaseCount(gauge.Inc)
			}
			if state == "disconnect-before-close" {
				request, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				rt.setCancelRequestFunc(cancel)
				rt.setInProcessRequest(true)
				rm.cancelDisconnectedRequests(func(*Conn) (bool, error) { return true, nil })
				require.ErrorIs(t, request.Err(), context.Canceled)
				require.True(t, rt.isCancelled())
				// handleRequest's quit branch decrements before Conn.Close calls Closed.
				rt.decreaseCount(gauge.Dec)
				rt.setInProcessRequest(false)
			}

			rm.Closed(conn)
			rm.Closed(conn)
			require.Nil(t, rm.getRoutine(conn))
			require.Nil(t, rm.getRoutineByConnID(0))
			require.Nil(t, rt.getSession(), "the execution owner must release the session")
			require.False(t, rt.connectionBeCounted.Load())
			value := &pcg.Metric{}
			require.NoError(t, gauge.Write(value))
			require.Zero(t, value.GetGauge().GetValue(), "closing must not double-decrement the metric")
			registered := rm.accountRoutine.deepCopyRoutineMap()[int64(accountID)]
			require.NotContains(t, registered, rt, "closed tenant routine must not remain registered")
			require.Contains(t, registered, peer)
			rm.accountRoutine.deleteRoutine(int64(accountID), peer)
			require.Empty(t, rm.accountRoutine.deepCopyRoutineMap(), "the last removal must release the tenant entry")
		})
	}
}

func TestRoutineDisconnectCancelOutsideLock(t *testing.T) {
	for _, publishFirst := range []bool{false, true} {
		routine := &Routine{}
		called := false
		cancel := func() {
			routine.mu.Lock()
			defer routine.mu.Unlock()
			called = true
		}
		if publishFirst {
			routine.setCancelRequestFunc(cancel)
			routine.cancelOnDisconnect()
		} else {
			routine.cancelOnDisconnect()
			routine.setCancelRequestFunc(cancel)
		}
		require.True(t, called)
	}
}

func TestRoutineManagerCancelDisconnectedRequests(t *testing.T) {
	for _, tc := range []struct {
		name       string
		active     bool
		cancelled  bool
		closed     bool
		probeErr   error
		wantProbe  bool
		wantCancel bool
	}{
		{name: "active-disconnected", active: true, closed: true, wantProbe: true, wantCancel: true},
		{name: "active-healthy", active: true, wantProbe: true},
		{name: "idle", closed: true},
		{name: "already-cancelled", active: true, cancelled: true, closed: true},
		{name: "inconclusive", active: true, closed: true, probeErr: errors.New("probe failed"), wantProbe: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			routine := &Routine{}
			routine.setCancelRequestFunc(cancel)
			routine.setInProcessRequest(tc.active)
			routine.setCancelled(tc.cancelled)
			conn := &Conn{}
			rm := &RoutineManager{clients: map[*Conn]*Routine{conn: routine, nil: nil, new(Conn): nil}}
			probed := false
			rm.cancelDisconnectedRequests(func(got *Conn) (bool, error) {
				require.Same(t, conn, got)
				// Probe/cancel must not hold the registry lock.
				rm.mu.Lock()
				rm.mu.Unlock()
				probed = true
				return tc.closed, tc.probeErr
			})
			require.Equal(t, tc.wantProbe, probed)
			require.Equal(t, tc.wantCancel, ctx.Err() != nil)
			require.Empty(t, rm.disconnectProbeScratch)
			for _, entry := range rm.disconnectProbeScratch[:cap(rm.disconnectProbeScratch)] {
				require.Nil(t, entry.conn)
				require.Nil(t, entry.routine)
			}
			rm.cancelDisconnectedRequests(nil)
		})
	}
}

func TestConnectionLivenessMonitorStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	server := &MOServer{rm: &RoutineManager{ctx: ctx}}
	server.startConnectionLivenessMonitor()
	cancel()
	done := make(chan struct{})
	go func() {
		server.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("connection monitor did not stop")
	}
	var absent *MOServer
	absent.startConnectionLivenessMonitor()
	(&MOServer{}).startConnectionLivenessMonitor()
	(&MOServer{rm: &RoutineManager{}}).startConnectionLivenessMonitor()
}
