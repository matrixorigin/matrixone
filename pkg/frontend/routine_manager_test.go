// Copyright 2022 Matrix Origin
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
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
)

func Test_Closed(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	registerConn(clientConn)
	pu, _ := getParameterUnit("test/system_vars_config.toml", nil, nil)
	pu.SV.SkipCheckUser = true
	pu.SV.KillRountinesInterval = 0
	setSessionAlloc("", NewLeakCheckAllocator())
	setPu("", pu)
	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
	temp, _ := NewRoutineManager(ctx, "")
	setRtMgr("", temp)
	mo := createInnerServer()
	wg := sync.WaitGroup{}
	wg.Add(1)
	cf := &CloseFlag{}
	go func() {
		defer wg.Done()
		mo.handleConn(ctx, serverConn)
	}()

	time.Sleep(100 * time.Millisecond)
	db, err := openDbConn(t, 6001)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	cf.Close()

	closeDbConn(t, db)
	wg.Wait()
	err = mo.Stop()
	require.NoError(t, err)
	serverConn.Close()
	clientConn.Close()
	wg.Wait()

}

var _ net.Addr = new(testAddr)

type testAddr struct {
}

func (ta *testAddr) Network() string {
	return "test network"
}

func (ta *testAddr) String() string {
	return "test addr"
}

var _ net.Conn = new(testConn)

const (
	testConnModNormal int = iota
	testConnModCloseReturnErr
	testConnModSetReadDeadlineReturnErr
	testConnModReadReturnErr
	testConnModReadPanic
	testConnModReadBuffer
)

type testConn struct {
	mod    int
	data   []byte
	local  testAddr
	remote testAddr
	rbuf   []byte
}

func (tc *testConn) Read(b []byte) (n int, err error) {
	if tc.mod == testConnModReadReturnErr {
		return 0, moerr.NewInternalErrorNoCtx("test conn read returns error")
	} else if tc.mod == testConnModReadPanic {
		panic("test conn read panic")
	} else if tc.mod == testConnModReadBuffer {
		blen := len(b)
		if blen == 0 {
			return 0, nil
		}
		rlen := len(tc.rbuf)
		readLen := min(rlen, blen)
		if readLen == 0 {
			return 0, io.EOF
		}
		copy(b, tc.rbuf[0:readLen])
		tc.rbuf = tc.rbuf[readLen:]
		return readLen, nil
	}
	blen := len(b)
	if blen == 0 {
		return 0, nil
	}
	dlen := len(tc.data)
	readLen := min(dlen, blen)
	if readLen == 0 {
		return 0, io.EOF
	}
	copy(b, tc.data[0:readLen])
	tc.data = tc.data[readLen:]
	return readLen, nil
}

func (tc *testConn) Write(b []byte) (n int, err error) {
	tc.data = append(tc.data, b...)
	return len(b), nil
}

func (tc *testConn) Close() error {
	if tc.mod == testConnModCloseReturnErr {
		return moerr.NewInternalErrorNoCtx("test close returns error")
	}
	return nil
}

func (tc *testConn) LocalAddr() net.Addr {
	return &tc.local
}

func (tc *testConn) RemoteAddr() net.Addr {
	return &tc.remote
}

func (tc *testConn) SetDeadline(t time.Time) error {

	return nil
}

func (tc *testConn) SetReadDeadline(t time.Time) error {
	if tc.mod == testConnModSetReadDeadlineReturnErr {
		return moerr.NewInternalErrorNoCtx("SetReadDeadline returns err")
	}
	return nil
}

func (tc *testConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func TestRoutineManager_killClients(t *testing.T) {
	type fields struct {
		ctx              context.Context
		clients          map[*Conn]*Routine
		routinesByConnID map[uint32]*Routine
		tlsConfig        *tls.Config
		accountRoutine   *AccountRoutineManager
		baseService      BaseService
		sessionManager   *queryservice.SessionManager
	}

	clients := make(map[*Conn]*Routine)
	for i := 0; i < 3; i++ {
		conn := &Conn{
			id: uint64(i),
		}
		if i == 2 {
			conn.conn = &testConn{}
		}
		clients[conn] = nil

	}

	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "t1",
			fields: fields{
				clients: clients,
			},
		},
	}
	var rm1 *RoutineManager
	rm1.killNetConns()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rm := &RoutineManager{
				ctx:              tt.fields.ctx,
				clients:          tt.fields.clients,
				routinesByConnID: tt.fields.routinesByConnID,
				tlsConfig:        tt.fields.tlsConfig,
				accountRoutine:   tt.fields.accountRoutine,
				baseService:      tt.fields.baseService,
				sessionManager:   tt.fields.sessionManager,
			}
			rm.killNetConns()
		})
	}
}

func TestRoutineManagerCancelDisconnectedLongRunningRequests(t *testing.T) {
	now := time.Now()
	grace := 30 * time.Second
	longServer, longClient := net.Pipe()
	shortServer, shortClient := net.Pipe()
	t.Cleanup(func() {
		_ = longServer.Close()
		_ = longClient.Close()
		_ = shortServer.Close()
		_ = shortClient.Close()
	})

	longRoutine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	shortRoutine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(longRoutine.cancelRoutineFunc)
	t.Cleanup(shortRoutine.cancelRoutineFunc)
	longRoutine.requestStartedAt.Store(clientRequestClockValue(now.Add(-grace - time.Second)))
	shortRoutine.requestStartedAt.Store(clientRequestClockValue(now.Add(-grace + time.Second)))

	longCtx, cancelLong := context.WithCancel(context.Background())
	shortCtx, cancelShort := context.WithCancel(context.Background())
	t.Cleanup(cancelLong)
	t.Cleanup(cancelShort)
	longRoutine.setCancelRequestFunc(cancelLong)
	shortRoutine.setCancelRequestFunc(cancelShort)

	longConn := &Conn{conn: longServer, remoteAddr: "long"}
	shortConn := &Conn{conn: shortServer, remoteAddr: "short"}
	rm := &RoutineManager{clients: map[*Conn]*Routine{
		longConn:  longRoutine,
		shortConn: shortRoutine,
	}}

	probes := 0
	rm.cancelDisconnectedRequests(now, grace, func(conn net.Conn) (bool, error) {
		probes++
		return conn == longServer, nil
	})

	require.Equal(t, 1, probes, "only requests beyond the grace period should be probed")
	select {
	case <-longCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("disconnected long-running request was not canceled")
	}
	select {
	case <-shortCtx.Done():
		t.Fatal("short-running request was canceled")
	default:
	}

	rm.cancelDisconnectedRequests(now, grace, func(net.Conn) (bool, error) {
		probes++
		return true, nil
	})
	require.Equal(t, 1, probes, "a routine already closing should not be probed again")
}

func TestRoutineManagerProbeErrorDoesNotCancelRequest(t *testing.T) {
	now := time.Now()
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})

	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	routine.requestStartedAt.Store(clientRequestClockValue(now.Add(-time.Minute)))
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	t.Cleanup(cancelRequest)
	routine.setCancelRequestFunc(cancelRequest)

	conn := &Conn{conn: serverConn}
	rm := &RoutineManager{clients: map[*Conn]*Routine{conn: routine}}
	rm.cancelDisconnectedRequests(now, 30*time.Second, func(net.Conn) (bool, error) {
		return false, errors.New("probe failed")
	})

	select {
	case <-requestCtx.Done():
		t.Fatal("an inconclusive socket probe canceled the request")
	default:
	}
}

func TestConnectionLivenessMonitorStopsWithRoutineManager(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	server := &MOServer{rm: &RoutineManager{ctx: ctx}}
	server.startConnectionLivenessMonitor()
	cancel()

	stopped := make(chan struct{})
	go func() {
		server.wg.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("connection liveness monitor leaked after routine manager cancellation")
	}
}

func BenchmarkRoutineManagerLongRunningRequests(b *testing.B) {
	const connections = 10_000
	now := time.Now()

	for _, benchmark := range []struct {
		name       string
		activeLong int
	}{
		{name: "idle", activeLong: 0},
		{name: "one-percent-active", activeLong: connections / 100},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			rm := &RoutineManager{clients: make(map[*Conn]*Routine, connections)}
			for i := 0; i < connections; i++ {
				routine := &Routine{}
				if i < benchmark.activeLong {
					routine.requestStartedAt.Store(clientRequestClockValue(now.Add(-time.Minute)))
				}
				rm.clients[&Conn{}] = routine
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = rm.longRunningRequests(now, 30*time.Second)
			}
		})
	}
}

func Test_rm(t *testing.T) {
	sv, err := getSystemVariables("test/system_vars_config.toml")
	if err != nil {
		t.Error(err)
	}
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	pu.SV.SkipCheckUser = true
	pu.SV.KillRountinesInterval = 1
	setPu("", pu)
	rm, err := NewRoutineManager(context.Background(), "")
	assert.NoError(t, err)
	rm.cleanKillQueue()
	setPu("", nil)
	time.Sleep(2 * time.Second)
	rm.cancelCtx()
}
