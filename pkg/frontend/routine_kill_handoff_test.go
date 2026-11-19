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

package frontend

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/stretchr/testify/require"
)

type observedKillProtocol struct {
	*testMysqlWriter
	closed chan struct{}
	once   sync.Once
}

func (p *observedKillProtocol) Close() { p.once.Do(func() { close(p.closed) }) }

func TestKillRequestFinishHandsCloseToConnectionOwner(t *testing.T) {
	for _, priorError := range []bool{false, true} {
		t.Run(map[bool]string{false: "successful response", true: "existing error"}[priorError], func(t *testing.T) {
			protocol := &observedKillProtocol{testMysqlWriter: &testMysqlWriter{}, closed: make(chan struct{})}
			rt := NewRoutine(context.Background(), protocol, &config.FrontendParameters{})
			require.True(t, rt.mc.tryBeginRequest())
			require.True(t, rt.beginClientRequest())
			// The request's last connection check wins just before KILL.
			require.False(t, rt.shouldCloseConnection())
			rt.killConnection(false)
			select {
			case <-protocol.closed:
				t.Fatal("an active request must finish its response before close")
			default:
			}
			var input error
			if priorError {
				input = errors.New("response failed")
			}
			result := rt.endClientRequest(input)
			if input == nil {
				require.ErrorIs(t, result, io.EOF)
			} else {
				require.ErrorIs(t, result, input)
			}
			rt.mc.endRequest()
			require.False(t, rt.beginClientRequest())
			late, cancel := context.WithCancel(context.Background())
			defer cancel()
			rt.setCancelRequestFunc(cancel)
			require.ErrorIs(t, late.Err(), context.Canceled)
		})
	}
	t.Run("self KILL preserves the response", func(t *testing.T) {
		protocol := &observedKillProtocol{testMysqlWriter: &testMysqlWriter{}, closed: make(chan struct{})}
		rt := NewRoutine(context.Background(), protocol, &config.FrontendParameters{})
		require.True(t, rt.beginClientRequest())
		rt.killConnection(true)
		select {
		case <-protocol.closed:
			t.Fatal("self KILL closed before the response was finished")
		default:
		}
		require.ErrorIs(t, rt.endClientRequest(nil), io.EOF)
	})
}

func TestKillIdleDoesNotWaitForBusyController(t *testing.T) {
	protocol := &observedKillProtocol{testMysqlWriter: &testMysqlWriter{}, closed: make(chan struct{})}
	rt := NewRoutine(context.Background(), protocol, &config.FrontendParameters{})
	rt.mc.Lock()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(rt.mc.Unlock) }
	done := make(chan struct{})
	go func() { rt.killConnection(false); close(done) }()
	t.Cleanup(func() {
		release()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("idle KILL did not stop during cleanup")
		}
	})
	select {
	case <-protocol.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("idle KILL waited for the lifecycle controller")
	}
	release()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("idle KILL did not return")
	}
}

func TestKillPreservesActiveControllerOperation(t *testing.T) {
	protocol := &observedKillProtocol{testMysqlWriter: &testMysqlWriter{}, closed: make(chan struct{})}
	rt := NewRoutine(context.Background(), protocol, &config.FrontendParameters{})
	operation, admitted := rt.mc.beginOperationWithContext(context.Background())
	require.True(t, admitted)
	rt.killConnection(false)
	require.NoError(t, operation.Err(), "account KILL must not newly cancel an admitted proxy operation")
	rt.mc.Lock()
	closed := rt.mc.closed
	rt.mc.Unlock()
	require.False(t, closed, "the busy controller must retain the original KILL behavior")
	rt.mc.endOperation()
}

func TestAccountKillAllowsKillQueryWhileRequestFinishes(t *testing.T) {
	rt, _ := newUnitTestRoutine(t, 100)
	owner := killTestIdentity("acme", 10, "owner", 7, "public")
	rt.setSession(&Session{tenant: owner})
	require.True(t, rt.mc.tryBeginRequest())
	require.True(t, rt.beginClientRequest())
	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt.setCancelRequestFunc(cancel)
	rt.killConnection(false)
	require.NoError(t, requestCtx.Err())
	rm := &RoutineManager{routinesByConnID: map[uint32]*Routine{100: rt}}
	found, err := rm.killLocal(context.Background(), killPrincipal(owner), 1, 100, false, "")
	require.True(t, found)
	require.NoError(t, err)
	require.ErrorIs(t, requestCtx.Err(), context.Canceled)
	require.ErrorIs(t, rt.endClientRequest(nil), io.EOF)
	rt.mc.endRequest()
}

func TestKilledRoutineRejectsNewWireCommands(t *testing.T) {
	rt := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	rt.setCancelled(true)
	conn := &Conn{conn: &testConn{}}
	rm := &RoutineManager{ctx: context.Background(), clients: map[*Conn]*Routine{conn: rt}}
	for _, cmd := range []CommandType{COM_PING, COM_RESET_CONNECTION, COM_CHANGE_USER} {
		require.ErrorIs(t, rm.Handler(conn, []byte{byte(cmd)}), io.EOF)
	}
	rt.mc.Lock()
	requestInProgress := rt.mc.requestInProgress
	rt.mc.Unlock()
	require.False(t, requestInProgress, "rejected normal requests must release controller admission")
}

type blockingKillResponse struct {
	MysqlRrWr
	entered   chan struct{}
	release   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func (p *blockingKillResponse) Close() {
	p.closeOnce.Do(func() { close(p.closed) })
	p.MysqlRrWr.Close()
}

func (p *blockingKillResponse) WriteResponse(context.Context, *Response) error {
	close(p.entered)
	<-p.release
	return nil
}

func TestKillSessionCommandClosesAfterResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := newTestSession(t, ctrl)
	parameters := &config.FrontendParameters{}
	parameters.SetDefaultValues()
	protocol := &blockingKillResponse{MysqlRrWr: session.GetResponser().MysqlRrWr(), entered: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(protocol.release) }) }
	rt := NewRoutine(context.Background(), protocol, parameters)
	rt.setSession(session)
	done := make(chan error, 1)
	finished := make(chan struct{})
	t.Cleanup(func() {
		release()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("session command did not stop during cleanup")
		}
		session.Close()
		rt.cancelRoutineFunc()
	})
	go func() {
		defer close(finished)
		done <- rt.handleSessionCommand(context.Background(), &Request{cmd: COM_RESET_CONNECTION, data: []byte{1}})
	}()
	select {
	case <-protocol.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("session command did not reach response")
	}
	rt.killConnection(false)
	select {
	case <-protocol.closed:
		t.Fatal("protocol closed before the response completed")
	default:
	}
	select {
	case <-done:
		t.Fatal("session command returned before completing its response")
	default:
	}
	release()
	select {
	case err := <-done:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(3 * time.Second):
		t.Fatal("session command did not finish after response release")
	}
}

type immediateKillResponse struct {
	MysqlRrWr
	written chan struct{}
}

func (p *immediateKillResponse) WriteResponse(context.Context, *Response) error {
	close(p.written)
	return nil
}

func TestKillResetBeforeLifecycleAdmission(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := newTestSession(t, ctrl)
	parameters := &config.FrontendParameters{}
	parameters.SetDefaultValues()
	protocol := &immediateKillResponse{MysqlRrWr: session.GetResponser().MysqlRrWr(), written: make(chan struct{})}
	rt := NewRoutine(context.Background(), protocol, parameters)
	rt.setSession(session)
	rt.mc.Lock()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(rt.mc.Unlock) }
	done := make(chan error, 1)
	finished := make(chan struct{})
	t.Cleanup(func() {
		rt.cancelRoutineFunc()
		release()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("reset did not stop during cleanup")
		}
		session.Close()
	})
	go func() {
		defer close(finished)
		done <- rt.handleSessionCommand(context.Background(), &Request{cmd: COM_RESET_CONNECTION, data: []byte{}})
	}()
	require.Eventually(t, func() bool {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		return rt.inProcessRequest
	}, 3*time.Second, time.Millisecond, "reset must enter before KILL")
	rt.killConnection(false)
	release()
	select {
	case err := <-done:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(3 * time.Second):
		t.Fatal("reset did not return after controller admission was released")
	}
	select {
	case <-protocol.written:
	default:
		t.Fatal("reset did not attempt its response before handing off close")
	}
}

func TestAccountKillQueuedLifecycleAdmission(t *testing.T) {
	for _, kind := range []string{"migrate", "reset"} {
		for _, killed := range []bool{false, true} {
			name := kind + "/healthy"
			if killed {
				name = kind + "/killed"
			}
			t.Run(name, func(t *testing.T) {
				protocol := &observedKillProtocol{testMysqlWriter: &testMysqlWriter{}, closed: make(chan struct{})}
				rt := NewRoutine(context.Background(), protocol, &config.FrontendParameters{})
				require.True(t, rt.mc.tryBeginRequest())
				require.True(t, rt.beginClientRequest())
				waiting := make(chan struct{})
				if kind == "migrate" {
					rt.mc.operationWaitHook = func() { close(waiting) }
				} else {
					rt.mc.requestWaitHook = func() { close(waiting) }
				}
				ctx, cancel := context.WithCancel(context.Background())
				var finishOnce sync.Once
				var requestErr error
				finishRequest := func() {
					finishOnce.Do(func() {
						requestErr = rt.endClientRequest(nil)
						rt.mc.endRequest()
					})
				}
				admitted := make(chan bool, 1)
				finished := make(chan struct{})
				go func() {
					defer close(finished)
					var ok bool
					if kind == "migrate" {
						_, ok = rt.mc.beginOperationWithContext(ctx)
					} else {
						_, ok = rt.mc.beginOperationAfterRequestWithContext(ctx)
					}
					if ok {
						rt.mc.endOperation()
					}
					admitted <- ok
				}()
				t.Cleanup(func() {
					cancel()
					finishRequest()
					select {
					case <-finished:
					case <-time.After(3 * time.Second):
						t.Error("lifecycle waiter did not stop")
					}
					rt.beginClose()
					rt.cancelRoutineFunc()
				})
				select {
				case <-waiting:
				case <-time.After(3 * time.Second):
					t.Fatal("lifecycle operation did not queue")
				}
				if killed {
					accounts := &AccountRoutineManager{
						killIdQueue:       map[int64]KillRecord{42: {killTime: time.Now(), version: 0}},
						accountId2Routine: map[int64]map[*Routine]uint64{42: {rt: 0}},
					}
					(&RoutineManager{accountRoutine: accounts}).KillRoutineConnections()
					select {
					case <-protocol.closed:
						t.Fatal("KILL closed the protocol before the request completed")
					default:
					}
				}
				finishRequest()
				if killed {
					require.ErrorIs(t, requestErr, io.EOF)
				} else {
					require.NoError(t, requestErr)
				}
				select {
				case ok := <-admitted:
					require.Equal(t, !killed, ok, "lifecycle admission after request completion")
				case <-time.After(3 * time.Second):
					t.Fatal("lifecycle waiter did not complete")
				}
			})
		}
	}
}

func TestAccountKillAfterWireBeforeControllerRelease(t *testing.T) {
	protocol := &observedKillProtocol{testMysqlWriter: &testMysqlWriter{}, closed: make(chan struct{})}
	rt := NewRoutine(context.Background(), protocol, &config.FrontendParameters{})
	require.True(t, rt.mc.tryBeginRequest())
	require.True(t, rt.beginClientRequest())
	t.Cleanup(func() { rt.mc.endRequest(); rt.beginClose(); rt.cancelRoutineFunc() })
	require.NoError(t, rt.endClientRequest(nil))
	// Wire ownership is released, while the controller still owns the request.
	rt.killConnection(false)
	select {
	case <-protocol.closed:
	default:
		t.Fatal("KILL did not close the idle wire")
	}
	rt.mc.endRequest()
	_, admitted := rt.mc.beginOperationWithContext(context.Background())
	if admitted {
		rt.mc.endOperation()
	}
	require.False(t, admitted, "migration admitted after KILL")
	_, admitted = rt.mc.beginOperationAfterRequestWithContext(context.Background())
	if admitted {
		rt.mc.endOperation()
	}
	require.False(t, admitted, "reset admitted after KILL")
}

type blockingDisconnectProtocol struct {
	*testMysqlWriter
	entered chan struct{}
	release chan struct{}
}

func (p *blockingDisconnectProtocol) Close() {
	close(p.entered)
	<-p.release
}

func TestAccountKillBlockedCloseDoesNotBlockDisconnectScan(t *testing.T) {
	protocol := &blockingDisconnectProtocol{testMysqlWriter: &testMysqlWriter{}, entered: make(chan struct{}), release: make(chan struct{})}
	first := NewRoutine(context.Background(), protocol, &config.FrontendParameters{})
	second := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	require.True(t, first.mc.tryBeginRequest())
	require.True(t, first.beginClientRequest())
	require.True(t, second.mc.tryBeginRequest())
	require.True(t, second.beginClientRequest())
	firstConn, secondConn := &Conn{remoteAddr: "first"}, &Conn{remoteAddr: "second"}
	rm := &RoutineManager{clients: map[*Conn]*Routine{firstConn: first}}
	selected, probeReturned := make(chan struct{}), make(chan struct{})
	allowProbe := make(chan struct{})
	firstDone, secondDone, killDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var allowOnce, closeOnce sync.Once
	allow := func() { allowOnce.Do(func() { close(allowProbe) }) }
	unblock := func() { closeOnce.Do(func() { close(protocol.release) }) }
	controllerHeld, killStarted, secondStarted := false, false, false
	t.Cleanup(func() {
		if controllerHeld {
			first.mc.Unlock()
		}
		allow()
		unblock()
		for _, done := range []struct {
			started bool
			ch      <-chan struct{}
		}{{true, firstDone}, {killStarted, killDone}, {secondStarted, secondDone}} {
			if done.started {
				select {
				case <-done.ch:
				case <-time.After(3 * time.Second):
					t.Error("owned goroutine did not stop")
				}
			}
		}
		second.endClientRequest(nil)
		second.mc.endRequest()
		first.beginClose()
		second.beginClose()
		first.cancelRoutineFunc()
		second.cancelRoutineFunc()
	})
	go func() {
		defer close(firstDone)
		rm.cancelDisconnectedRequests(time.Now(), 0, func(*Conn) (bool, error) {
			close(selected)
			<-allowProbe
			close(probeReturned)
			return true, nil
		})
	}()
	select {
	case <-selected:
	case <-time.After(3 * time.Second):
		t.Fatal("first scan did not select the active request")
	}
	require.NoError(t, first.endClientRequest(nil))
	first.mc.endRequest()
	first.mc.Lock()
	controllerHeld = true
	killStarted = true
	go func() { defer close(killDone); first.killConnection(false) }()
	select {
	case <-protocol.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("KILL did not reach protocol Close")
	}
	first.mc.Unlock()
	controllerHeld = false
	rm.mu.Lock()
	rm.clients[secondConn] = second
	rm.mu.Unlock()
	allow()
	select {
	case <-probeReturned:
	case <-time.After(3 * time.Second):
		t.Fatal("first probe did not return")
	}
	secondReached := make(chan struct{})
	secondStarted = true
	go func() {
		defer close(secondDone)
		rm.cancelDisconnectedRequests(time.Now(), 0, func(conn *Conn) (bool, error) {
			if conn == secondConn {
				close(secondReached)
			}
			return false, nil
		})
	}()
	select {
	case <-secondReached:
	case <-time.After(3 * time.Second):
		t.Fatal("blocked protocol Close prevented the next disconnect scan")
	}
}

type countedKillCloseConn struct {
	testConn
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

func (c *countedKillCloseConn) Close() error {
	c.calls.Add(1)
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return nil
}

func TestKillProtocolCloseRacesCleanup(t *testing.T) {
	t.Run("idle", func(t *testing.T) { testKillProtocolCloseRacesCleanup(t, false) })
	t.Run("contended controller", func(t *testing.T) { testKillProtocolCloseRacesCleanup(t, true) })
}

func testKillProtocolCloseRacesCleanup(t *testing.T, contention bool) {
	raw := &countedKillCloseConn{entered: make(chan struct{}), release: make(chan struct{})}
	parameters := &config.FrontendParameters{}
	proto := NewMysqlClientProtocol("", 42, &Conn{conn: raw}, 0, parameters)
	rt := NewRoutine(context.Background(), proto, parameters)
	if contention {
		rt.mc.Lock()
	}
	var controllerOnce sync.Once
	releaseController := func() {
		if contention {
			controllerOnce.Do(rt.mc.Unlock)
		}
	}
	killDone := make(chan struct{})
	cleanupStarted := make(chan struct{})
	cleanupDone := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(raw.release) }) }
	t.Cleanup(func() {
		releaseController()
		release()
		select {
		case <-killDone:
		case <-time.After(3 * time.Second):
			t.Error("kill did not finish")
		}
		select {
		case <-cleanupStarted:
			select {
			case <-cleanupDone:
			case <-time.After(3 * time.Second):
				t.Error("cleanup did not finish")
			}
		default:
		}
	})
	go func() { defer close(killDone); rt.killConnection(false) }()
	select {
	case <-raw.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("protocol close did not start")
	}
	releaseController()
	close(cleanupStarted)
	go func() { defer close(cleanupDone); rt.cleanup() }()
	require.Eventually(t, rt.closing.Load, 3*time.Second, time.Millisecond, "cleanup must reach close while network close is blocked")
	select {
	case <-cleanupDone:
		t.Fatal("cleanup returned before protocol close")
	default:
	}
	release()
	select {
	case <-killDone:
	case <-time.After(3 * time.Second):
		t.Fatal("kill did not finish")
	}
	select {
	case <-cleanupDone:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup did not finish")
	}
	require.EqualValues(t, 1, raw.calls.Load(), "protocol close must close the transport effectively once")
	require.Nil(t, rt.getProtocol())
}
