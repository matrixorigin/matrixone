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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var nextRoutineManagerTestServiceID atomic.Uint64

func newRoutineManagerTestService(t *testing.T) string {
	t.Helper()
	return t.Name() + "/routine-manager-" +
		strconv.FormatUint(nextRoutineManagerTestServiceID.Add(1), 10)
}

func newTestRoutineManager(t *testing.T, ctx context.Context) *RoutineManager {
	t.Helper()
	service := newRoutineManagerTestService(t)
	rm, err := NewRoutineManager(ctx, service)
	require.NoError(t, err)
	t.Cleanup(rm.cancelCtx)
	return rm
}

func TestRoutineManagerGetConnIDUsesConnectTimeout(t *testing.T) {
	const connectTimeout = 17 * time.Second
	var observed time.Duration
	client := newMockHAKeeperClient()
	client.allocateIDByKey = func(ctx context.Context, key string) (uint64, error) {
		require.Equal(t, ConnIDAllocKey, key)
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		observed = time.Until(deadline)
		return 0, context.DeadlineExceeded
	}
	sv := &config.FrontendParameters{}
	sv.SetDefaultValues()
	sv.ConnectTimeout.Duration = connectTimeout
	rm := &RoutineManager{
		ctx: context.Background(),
		pu: &config.ParameterUnit{
			SV:             sv,
			HAKeeperClient: client,
		},
	}

	_, err := rm.getConnID()
	require.Error(t, err)
	require.InDelta(t, connectTimeout, observed, float64(time.Second))
}

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

type blockingCloseConn struct {
	testConn
	closeStarted chan struct{}
	closeRelease chan struct{}
	startOnce    sync.Once
}

type blockingResponseProtocol struct {
	MysqlRrWr
	responseWritten chan struct{}
	releaseResponse chan struct{}
	writtenOnce     sync.Once
}

func (p *blockingResponseProtocol) WriteResponse(ctx context.Context, response *Response) error {
	err := p.MysqlRrWr.WriteResponse(ctx, response)
	p.writtenOnce.Do(func() {
		close(p.responseWritten)
	})
	<-p.releaseResponse
	return err
}

func (tc *blockingCloseConn) Close() error {
	tc.startOnce.Do(func() {
		close(tc.closeStarted)
	})
	<-tc.closeRelease
	return nil
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
	rm.cancelDisconnectedRequests(now, grace, func(conn *Conn) (bool, error) {
		probes++
		return conn.RawConn() == longServer, nil
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

	rm.cancelDisconnectedRequests(now, grace, func(*Conn) (bool, error) {
		probes++
		return true, nil
	})
	require.Equal(t, 1, probes, "a routine already closing should not be probed again")
}

func TestClientDisconnectProbePolicyCoversNewRequests(t *testing.T) {
	now := time.Now()
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})

	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	routine.requestStartedAt.Store(clientRequestClockValue(now))
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	t.Cleanup(cancelRequest)
	routine.setCancelRequestFunc(cancelRequest)

	conn := &Conn{conn: serverConn, remoteAddr: "new-request"}
	rm := &RoutineManager{clients: map[*Conn]*Routine{conn: routine}}
	probes := 0
	rm.cancelDisconnectedRequests(now, clientDisconnectProbeGrace, func(*Conn) (bool, error) {
		probes++
		return true, nil
	})

	require.Equal(t, 1, probes, "a new active request must be probed without an age grace period")
	select {
	case <-requestCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("a disconnected new request was not canceled")
	}
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
	rm.cancelDisconnectedRequests(now, 30*time.Second, func(*Conn) (bool, error) {
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
				requests := rm.appendLongRunningRequests(nil, now, 30*time.Second)
				clear(requests)
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
	ctx := context.WithValue(context.Background(), config.ParameterUnitKey, pu)
	rm := newTestRoutineManager(t, ctx)
	rm.cleanKillQueue()
}

func TestRoutineManagerRoutineMaps(t *testing.T) {
	rm := &RoutineManager{
		ctx:              context.Background(),
		clients:          make(map[*Conn]*Routine),
		routinesByConnID: make(map[uint32]*Routine),
	}
	rt, _ := newUnitTestRoutine(t, 1001)
	conn := &Conn{conn: &testConn{}, remoteAddr: "remote"}

	require.Nil(t, rm.getRoutine(conn))
	require.Nil(t, rm.getRoutineByConnID(1001))

	rm.setRoutine(conn, 1001, rt)
	require.Same(t, rt, rm.getRoutine(conn))
	require.Same(t, rt, rm.getRoutineByConnID(1001))
	require.Equal(t, 1, rm.clientCount())

	require.Same(t, rt, rm.deleteRoutine(conn))
	require.Nil(t, rm.getRoutine(conn))
	require.Nil(t, rm.getRoutineByConnID(1001))
	require.Equal(t, 0, rm.clientCount())
	require.Nil(t, rm.deleteRoutine(conn))
}

func TestAccountRoutineManagerRecordDeleteAndCopies(t *testing.T) {
	ar := &AccountRoutineManager{
		killIdQueue:       make(map[int64]KillRecord),
		accountId2Routine: make(map[int64]map[*Routine]uint64),
	}
	rt, _ := newUnitTestRoutine(t, 1002)

	ar.recordRoutine(sysAccountID, rt, 1)
	require.Empty(t, ar.accountId2Routine)
	ar.recordRoutine(10, nil, 1)
	require.Empty(t, ar.accountId2Routine)

	ar.recordRoutine(10, rt, 7)
	require.Equal(t, uint64(7), ar.accountId2Routine[10][rt])

	routineCopy := ar.deepCopyRoutineMap()
	routineCopy[10][rt] = 8
	require.Equal(t, uint64(7), ar.accountId2Routine[10][rt])

	ar.EnKillQueue(sysAccountID, 1)
	require.Empty(t, ar.killIdQueue)
	ar.EnKillQueue(10, 3)
	require.Equal(t, uint64(3), ar.killIdQueue[10].version)

	killCopy := ar.deepCopyKillQueue()
	killCopy[10] = NewKillRecord(time.Now(), 4)
	require.Equal(t, uint64(3), ar.killIdQueue[10].version)

	ar.AlterRoutineStatue(10, "restricted")
	require.True(t, rt.isRestricted())
	ar.AlterRoutineStatue(10, "normal")
	require.False(t, rt.isRestricted())
	ar.AlterRoutineStatue(sysAccountID, "restricted")
	require.False(t, rt.isRestricted())

	ar.deleteRoutine(sysAccountID, rt)
	require.Contains(t, ar.accountId2Routine, int64(10))
	ar.deleteRoutine(10, rt)
	require.NotContains(t, ar.accountId2Routine, int64(10))
}

func TestRoutineManagerKillAndCleanKillQueue(t *testing.T) {
	rt, _ := newUnitTestRoutine(t, 1003)
	ses := &Session{}
	rt.setSession(ses)
	rm := &RoutineManager{
		ctx:                    context.Background(),
		clients:                make(map[*Conn]*Routine),
		routinesByConnID:       map[uint32]*Routine{1003: rt},
		cleanKillQueueInterval: time.Minute,
		accountRoutine: &AccountRoutineManager{
			killIdQueue:       make(map[int64]KillRecord),
			accountId2Routine: make(map[int64]map[*Routine]uint64),
		},
	}

	require.ErrorContains(t, rm.kill(context.Background(), false, 1, 9999, ""), "Unknown connection id")

	ses.SetQueryInExecute(true)
	reqCtx, cancelReq := context.WithCancel(context.Background())
	rt.setCancelRequestFunc(cancelReq)
	require.NoError(t, rm.kill(context.Background(), false, 1, 1003, "stmt"))
	require.ErrorIs(t, reqCtx.Err(), context.Canceled)
	require.False(t, ses.GetQueryInExecute())

	rt.setCancelled(false)
	require.NoError(t, rm.kill(context.Background(), true, 1, 1003, ""))
	require.True(t, rt.isCancelled())

	rm.accountRoutine.killIdQueue[1] = NewKillRecord(time.Now().Add(-2*time.Minute), 1)
	rm.accountRoutine.killIdQueue[2] = NewKillRecord(time.Now(), 1)
	rm.cleanKillQueue()
	require.NotContains(t, rm.accountRoutine.killIdQueue, int64(1))
	require.Contains(t, rm.accountRoutine.killIdQueue, int64(2))
}

func TestRoutineManagerKillRoutineConnections(t *testing.T) {
	rt, _ := newUnitTestRoutine(t, 1004)
	ar := &AccountRoutineManager{
		killIdQueue: map[int64]KillRecord{
			20: NewKillRecord(time.Now(), 5),
		},
		accountId2Routine: map[int64]map[*Routine]uint64{
			20: {rt: 5},
		},
	}
	rm := &RoutineManager{
		accountRoutine:         ar,
		service:                "",
		cleanKillQueueInterval: time.Minute,
	}

	rm.KillRoutineConnections()
	require.True(t, rt.isCancelled())
	require.NotContains(t, ar.accountId2Routine, int64(20))
}

func TestRoutineManagerConfigSnapshotAndCancel(t *testing.T) {
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()
	pu.SV.KillRountinesInterval = 3600
	pu.SV.CleanKillQueueInterval = 2
	ctx := context.WithValue(context.Background(), config.ParameterUnitKey, pu)

	rm := newTestRoutineManager(t, ctx)
	require.Equal(t, 2*time.Minute, rm.cleanKillQueueInterval)

	pu.SV.CleanKillQueueInterval = 3
	require.Equal(t, 2*time.Minute, rm.cleanKillQueueInterval)

	done := make(chan struct{})
	go func() {
		rm.cancelCtx()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RoutineManager worker did not stop after cancellation")
	}
}

func TestRoutineManagerRejectsMissingFrontendParameters(t *testing.T) {
	pu := config.NewParameterUnit(nil, nil, nil, nil)
	ctx := context.WithValue(context.Background(), config.ParameterUnitKey, pu)

	rm, err := NewRoutineManager(ctx, t.Name())
	require.Nil(t, rm)
	require.ErrorContains(t, err, "invalid parameter unit")
}

func TestRoutineManagerRejectsMissingParameterUnit(t *testing.T) {
	for _, tc := range []struct {
		name              string
		initializeService bool
	}{
		{name: "uninitialized service"},
		{name: "initialized service", initializeService: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := newRoutineManagerTestService(t)
			if tc.initializeService {
				InitServerLevelVars(service)
			}

			rm, err := NewRoutineManager(context.Background(), service)
			require.Nil(t, rm)
			require.ErrorContains(t, err, "invalid parameter unit")
		})
	}
}

func TestRoutineManagerPublishesContextParameterUnit(t *testing.T) {
	service := newRoutineManagerTestService(t)
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()
	pu.SV.KillRountinesInterval = 0
	ctx := context.WithValue(context.Background(), config.ParameterUnitKey, pu)

	rm, err := NewRoutineManager(ctx, service)
	require.NoError(t, err)
	t.Cleanup(rm.cancelCtx)
	require.Same(t, pu, getPuIfPresent(service))

	conn := &Conn{
		conn:       &testConn{},
		remoteAddr: "remote",
		service:    service,
	}
	require.NoError(t, rm.Created(conn))
	require.NotNil(t, rm.getRoutine(conn))
	rm.Closed(conn)
	require.Nil(t, rm.getRoutine(conn))
}

func TestRoutineManagerRejectsParameterUnitMismatch(t *testing.T) {
	service := newRoutineManagerTestService(t)
	servicePU := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	servicePU.SV.SetDefaultValues()
	InitServerLevelVars(service)
	setPu(service, servicePU)

	contextPU := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	contextPU.SV.SetDefaultValues()
	ctx := context.WithValue(context.Background(), config.ParameterUnitKey, contextPU)

	rm, err := NewRoutineManager(ctx, service)
	require.Nil(t, rm)
	require.ErrorContains(t, err, "parameter unit mismatch")
	require.Same(t, servicePU, getPuIfPresent(service))
}

func TestRoutineManagerFailedInitializationDoesNotPublishParameterUnit(t *testing.T) {
	service := newRoutineManagerTestService(t)
	invalidPU := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	invalidPU.SV.SetDefaultValues()
	invalidPU.SV.EnableTls = true
	ctx := context.WithValue(context.Background(), config.ParameterUnitKey, invalidPU)

	rm, err := NewRoutineManager(ctx, service)
	require.Nil(t, rm)
	require.ErrorContains(t, err, "cert file or key file is empty")
	require.Nil(t, getPuIfPresent(service))

	validPU := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	validPU.SV.SetDefaultValues()
	validPU.SV.KillRountinesInterval = 0
	ctx = context.WithValue(context.Background(), config.ParameterUnitKey, validPU)

	rm, err = NewRoutineManager(ctx, service)
	require.NoError(t, err)
	t.Cleanup(rm.cancelCtx)
	require.Same(t, validPU, getPuIfPresent(service))
}

func TestRoutineManagerCancelWaitsForActiveWorker(t *testing.T) {
	rt, proto := newUnitTestRoutine(t, 1007)
	blockingConn := &blockingCloseConn{
		closeStarted: make(chan struct{}),
		closeRelease: make(chan struct{}),
	}
	proto.tcpConn.conn = blockingConn

	const accountID = int64(21)
	ctx, cancel := context.WithCancel(context.Background())
	rm := &RoutineManager{
		ctx:                    ctx,
		cancel:                 cancel,
		cleanKillQueueInterval: time.Hour,
		accountRoutine: &AccountRoutineManager{
			killIdQueue: map[int64]KillRecord{
				accountID: NewKillRecord(time.Now(), 1),
			},
			accountId2Routine: map[int64]map[*Routine]uint64{
				accountID: {rt: 1},
			},
		},
	}
	releaseWorker := sync.OnceFunc(func() {
		close(blockingConn.closeRelease)
	})
	t.Cleanup(func() {
		releaseWorker()
		rm.cancelCtx()
	})

	rm.startKillRoutineWorker(time.Hour)
	select {
	case <-blockingConn.closeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("RoutineManager worker did not enter connection close")
	}

	cancelReturned := make(chan struct{})
	go func() {
		rm.cancelCtx()
		close(cancelReturned)
	}()
	<-ctx.Done()

	select {
	case <-cancelReturned:
		t.Fatal("RoutineManager cancellation returned before active worker exited")
	case <-time.After(100 * time.Millisecond):
	}

	releaseWorker()
	select {
	case <-cancelReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("RoutineManager cancellation did not return after active worker exited")
	}
}

func TestRoutineManagerMigrationAndResetErrorBranches(t *testing.T) {
	rt, _ := newUnitTestRoutine(t, 1005)
	ses := &Session{}
	rt.setSession(ses)
	rm := &RoutineManager{
		ctx:              context.Background(),
		routinesByConnID: map[uint32]*Routine{1005: rt},
	}

	require.ErrorContains(t,
		rm.MigrateConnectionTo(context.Background(), &query.MigrateConnToRequest{ConnID: 404}),
		"cannot get routine to migrate connection")
	require.ErrorContains(t,
		rm.MigrateConnectionFrom(&query.MigrateConnFromRequest{ConnID: 404}, &query.MigrateConnFromResponse{}),
		"cannot get routine to migrate connection")
	require.ErrorContains(t,
		rm.ResetSession(&query.ResetSessionRequest{ConnID: 404}, &query.ResetSessionResponse{}),
		"cannot get routine to clear session")

	ses.userLevelLocksMigrated = false
	require.NoError(t, rm.MigrateConnectionFrom(&query.MigrateConnFromRequest{
		ConnID: 1005,
		Action: query.MigrateConnFromAction_MigrateConnFromSkipUserLevelLockRelease,
	}, &query.MigrateConnFromResponse{}))
	require.True(t, ses.userLevelLocksMigrated)

	ses.userLevelLocksMigrated = true
	require.NoError(t, rm.MigrateConnectionFrom(&query.MigrateConnFromRequest{
		ConnID: 1005,
		Action: query.MigrateConnFromAction_MigrateConnFromEnableUserLevelLockRelease,
	}, &query.MigrateConnFromResponse{}))
	require.False(t, ses.userLevelLocksMigrated)

	proc := testutil.NewProc(t)
	proc.GetSessionInfo().Account = "acc"
	proc.GetSessionInfo().ConnectionID = 1005
	function.RestoreUserLevelLocksFromMigration(proc, []function.UserLevelLockState{
		{Name: "held_lock", Count: 1},
	})
	defer function.DiscardMigratedUserLevelLocks(proc)
	ses.proc = proc
	require.ErrorContains(t, rm.MigrateConnectionFrom(&query.MigrateConnFromRequest{
		ConnID: 1005,
		Action: query.MigrateConnFromAction_MigrateConnFromSkipUserLevelLockRelease,
	}, &query.MigrateConnFromResponse{}), "cannot migrate connection while user-level locks are held")
}

func TestRoutineManagerLegacyMigrationActionsWaitForRequest(t *testing.T) {
	t.Run("skip user lock release rechecks after request", func(t *testing.T) {
		rm, routine, ses := newLegacyMigrationActionTestFixture(t, 1011)
		require.True(t, routine.mc.tryBeginRequest())
		var releaseRequest sync.Once
		t.Cleanup(func() { releaseRequest.Do(routine.mc.endRequest) })

		result := startLegacyMigrationAction(rm, 1011,
			query.MigrateConnFromAction_MigrateConnFromSkipUserLevelLockRelease)
		requireLegacyMigrationActionPending(t, result)

		function.RestoreUserLevelLocksFromMigration(ses.proc, []function.UserLevelLockState{
			{Name: "request_acquired_lock", Count: 1},
		})
		defer function.DiscardMigratedUserLevelLocks(ses.proc)
		releaseRequest.Do(routine.mc.endRequest)

		require.ErrorContains(t, receiveLegacyMigrationActionResult(t, result),
			"cannot migrate connection while user-level locks are held")
		require.False(t, ses.userLevelLocksMigrated)
	})

	t.Run("enable user lock release waits for request", func(t *testing.T) {
		rm, routine, ses := newLegacyMigrationActionTestFixture(t, 1012)
		ses.userLevelLocksMigrated = true
		require.True(t, routine.mc.tryBeginRequest())
		var releaseRequest sync.Once
		t.Cleanup(func() { releaseRequest.Do(routine.mc.endRequest) })

		result := startLegacyMigrationAction(rm, 1012,
			query.MigrateConnFromAction_MigrateConnFromEnableUserLevelLockRelease)
		requireLegacyMigrationActionPending(t, result)
		require.True(t, ses.userLevelLocksMigrated)

		releaseRequest.Do(routine.mc.endRequest)
		require.NoError(t, receiveLegacyMigrationActionResult(t, result))
		require.False(t, ses.userLevelLocksMigrated)
	})
}

func TestRoutineManagerLegacyMigrationActionsWaitForReset(t *testing.T) {
	t.Run("skip user lock release checks replacement session", func(t *testing.T) {
		rm, routine, oldSession := newLegacyMigrationActionTestFixture(t, 1013)
		require.True(t, routine.mc.tryBeginOperation())
		var releaseReset sync.Once
		t.Cleanup(func() { releaseReset.Do(routine.mc.endOperation) })

		result := startLegacyMigrationAction(rm, 1013,
			query.MigrateConnFromAction_MigrateConnFromSkipUserLevelLockRelease)
		requireLegacyMigrationActionPending(t, result)

		newSession := newLegacyMigrationActionTestSession(t, 1013)
		function.RestoreUserLevelLocksFromMigration(newSession.proc, []function.UserLevelLockState{
			{Name: "replacement_session_lock", Count: 1},
		})
		defer function.DiscardMigratedUserLevelLocks(newSession.proc)
		routine.setSession(newSession)
		releaseReset.Do(routine.mc.endOperation)

		require.ErrorContains(t, receiveLegacyMigrationActionResult(t, result),
			"cannot migrate connection while user-level locks are held")
		require.False(t, oldSession.userLevelLocksMigrated)
		require.False(t, newSession.userLevelLocksMigrated)
	})

	t.Run("enable user lock release mutates replacement session", func(t *testing.T) {
		rm, routine, oldSession := newLegacyMigrationActionTestFixture(t, 1014)
		oldSession.userLevelLocksMigrated = true
		require.True(t, routine.mc.tryBeginOperation())
		var releaseReset sync.Once
		t.Cleanup(func() { releaseReset.Do(routine.mc.endOperation) })

		result := startLegacyMigrationAction(rm, 1014,
			query.MigrateConnFromAction_MigrateConnFromEnableUserLevelLockRelease)
		requireLegacyMigrationActionPending(t, result)
		require.True(t, oldSession.userLevelLocksMigrated)

		newSession := newLegacyMigrationActionTestSession(t, 1014)
		newSession.userLevelLocksMigrated = true
		routine.setSession(newSession)
		releaseReset.Do(routine.mc.endOperation)

		require.NoError(t, receiveLegacyMigrationActionResult(t, result))
		require.True(t, oldSession.userLevelLocksMigrated)
		require.False(t, newSession.userLevelLocksMigrated)
	})
}

func newLegacyMigrationActionTestFixture(
	t *testing.T,
	connID uint32,
) (*RoutineManager, *Routine, *Session) {
	t.Helper()
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	ses := newLegacyMigrationActionTestSession(t, connID)
	routine.setSession(ses)
	return &RoutineManager{
		ctx:              context.Background(),
		routinesByConnID: map[uint32]*Routine{connID: routine},
	}, routine, ses
}

func newLegacyMigrationActionTestSession(t *testing.T, connID uint32) *Session {
	t.Helper()
	proc := testutil.NewProc(t)
	proc.GetSessionInfo().Account = "legacy_migration_action"
	proc.GetSessionInfo().ConnectionID = uint64(connID)
	return &Session{proc: proc}
}

func startLegacyMigrationAction(
	rm *RoutineManager,
	connID uint32,
	action query.MigrateConnFromAction,
) <-chan error {
	result := make(chan error, 1)
	go func() {
		result <- rm.MigrateConnectionFromWithContext(context.Background(),
			&query.MigrateConnFromRequest{ConnID: connID, Action: action},
			&query.MigrateConnFromResponse{})
	}()
	return result
}

func requireLegacyMigrationActionPending(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		require.Failf(t, "legacy migration action bypassed lifecycle admission", "err=%v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

func receiveLegacyMigrationActionResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		require.FailNow(t, "legacy migration action did not finish after lifecycle release")
		return nil
	}
}

func TestRoutineManagerResetSessionWaitsForRequestAfterResponseWrite(t *testing.T) {
	const connID = uint32(1009)
	ctrl := gomock.NewController(t)
	oldSession := newTestSession(t, ctrl)
	protocol := &blockingResponseProtocol{
		MysqlRrWr:       oldSession.GetResponser().MysqlRrWr(),
		responseWritten: make(chan struct{}),
		releaseResponse: make(chan struct{}),
	}
	routine := NewRoutine(context.Background(), protocol, getPu("").SV)
	rm, err := NewRoutineManager(context.Background(), "")
	require.NoError(t, err)
	rm.sessionManager = queryservice.NewSessionManager()
	rm.setBaseService(&testMOServerBaseService{id: ""})

	oldSession.respr = NewMysqlResp(protocol)
	oldSession.SetDatabaseName("must_not_leak")
	oldSession.setRoutineManager(rm)
	oldSession.setRoutine(routine)
	routine.setSession(oldSession)
	rm.sessionManager.AddSession(oldSession)
	conn := &Conn{id: uint64(connID), conn: &testConn{}, remoteAddr: "remote"}
	rm.setRoutine(conn, connID, routine)

	var releaseOnce sync.Once
	handlerFinished := make(chan struct{})
	handlerResult := make(chan struct {
		err       error
		recovered any
	}, 1)
	t.Cleanup(func() {
		releaseOnce.Do(func() {
			close(protocol.releaseResponse)
		})
		select {
		case <-handlerFinished:
		case <-time.After(time.Second):
		}
		if current := routine.getSession(); current != nil && current.GetProc() != nil {
			rm.sessionManager.RemoveSession(current)
			current.Close()
		}
		routine.cancelRoutineFunc()
		rm.cancelCtx()
	})

	go func() {
		var result struct {
			err       error
			recovered any
		}
		defer func() {
			result.recovered = recover()
			handlerResult <- result
			close(handlerFinished)
		}()
		result.err = rm.Handler(conn, []byte{byte(COM_PING)})
	}()

	select {
	case <-protocol.responseWritten:
	case <-time.After(time.Second):
		t.Fatal("request did not write its terminal response")
	}
	waitEntered := make(chan struct{})
	routine.mc.requestWaitHook = func() { close(waitEntered) }

	oldProc := oldSession.GetProc()
	oldTxnHandler := oldSession.GetTxnHandler()
	resetCtx, cancelReset := context.WithTimeout(context.Background(), time.Second)
	defer cancelReset()
	resetResult := make(chan error, 1)
	go func() {
		resetResult <- rm.ResetSessionWithContext(
			resetCtx,
			&query.ResetSessionRequest{ConnID: connID},
			&query.ResetSessionResponse{},
		)
	}()
	select {
	case err := <-resetResult:
		t.Fatalf("reset returned before the request finished: %v", err)
	case <-waitEntered:
	case <-time.After(time.Second):
		t.Fatal("reset did not enter the request-only admission wait")
	}
	require.Same(t, oldSession, routine.getSession())
	require.Same(t, oldProc, oldSession.GetProc())
	require.Same(t, oldTxnHandler, oldSession.GetTxnHandler())
	registered := rm.sessionManager.GetAllSessions()
	require.Len(t, registered, 1)
	require.Same(t, oldSession, registered[0])

	releaseOnce.Do(func() {
		close(protocol.releaseResponse)
	})
	select {
	case result := <-handlerResult:
		require.Nil(t, result.recovered)
		require.NoError(t, result.err)
	case <-time.After(time.Second):
		t.Fatal("request handler did not finish after response release")
	}

	select {
	case err := <-resetResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("reset did not finish after request release")
	}
	newSession := routine.getSession()
	require.NotSame(t, oldSession, newSession)
	require.Empty(t, newSession.GetDatabaseName(),
		"QueryService ResetSession must clear the previous client's database")
	require.Nil(t, oldSession.GetProc())
	require.Nil(t, oldSession.GetTxnHandler())
	registered = rm.sessionManager.GetAllSessions()
	require.Len(t, registered, 1)
	require.Same(t, newSession, registered[0])
	require.NoError(t, rm.Handler(conn, []byte{byte(COM_PING)}))
	secondResetCtx, cancelSecondReset := context.WithTimeout(context.Background(), time.Second)
	defer cancelSecondReset()
	require.NoError(t, rm.ResetSessionWithContext(
		secondResetCtx,
		&query.ResetSessionRequest{ConnID: connID},
		&query.ResetSessionResponse{},
	))
	secondSession := routine.getSession()
	require.NotSame(t, newSession, secondSession)
	registered = rm.sessionManager.GetAllSessions()
	require.Len(t, registered, 1)
	require.Same(t, secondSession, registered[0])
	require.NoError(t, rm.Handler(conn, []byte{byte(COM_PING)}))
}

func TestRoutineManagerHandlerRejectsLifecycleConflictBeforeSessionRead(t *testing.T) {
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	require.True(t, routine.mc.tryBeginOperation())
	defer routine.mc.endOperation()

	conn := &Conn{id: 1010, conn: &testConn{}, remoteAddr: "remote"}
	rm := &RoutineManager{
		ctx:              context.Background(),
		clients:          map[*Conn]*Routine{conn: routine},
		routinesByConnID: map[uint32]*Routine{1010: routine},
	}

	require.ErrorContains(t, rm.Handler(conn, nil), "empty MySQL command packet")
	require.ErrorContains(
		t,
		rm.Handler(conn, []byte{byte(COM_PING)}),
		"cannot process request as routine is closed or busy",
	)
}

func TestRoutineMigrateConnectionFromRejectsUserLevelLocks(t *testing.T) {
	rt, proto := newUnitTestRoutine(t, 1006)
	proc := testutil.NewProc(t)
	proc.GetSessionInfo().Account = "acc"
	proc.GetSessionInfo().ConnectionID = 1006
	function.RestoreUserLevelLocksFromMigration(proc, []function.UserLevelLockState{
		{Name: "exported_lock", Count: 2},
	})
	defer function.DiscardMigratedUserLevelLocks(proc)

	ses := &Session{
		feSessionImpl: feSessionImpl{
			respr: NewMysqlResp(proto),
		},
		proc: proc,
		prepareStmts: map[string]*PrepareStmt{
			"p1": {Name: "p1", Sql: "select ?"},
		},
	}
	proto.SetStr(DBNAME, "db1")
	rt.setSession(ses)

	resp := &query.MigrateConnFromResponse{}
	require.ErrorContains(t, rt.migrateConnectionFrom(resp), "cannot migrate connection while user-level locks are held")
	require.Empty(t, resp.UserLevelLocks)
	require.False(t, resp.UserLevelLockReleaseSupported)
}

func TestRoutineMigrateConnectionToClosedRoutine(t *testing.T) {
	rt, _ := newUnitTestRoutine(t, 1008)
	rt.mc.waitAndClose()

	err := rt.migrateConnectionTo(context.Background(), &query.MigrateConnToRequest{})
	require.ErrorContains(t, err, "cannot start migrate as routine has been closed")
}

func TestSessionCloseDiscardsMigratedUserLevelLocks(t *testing.T) {
	proc := testutil.NewProc(t)
	proc.GetSessionInfo().Account = "acc"
	proc.GetSessionInfo().ConnectionID = 1007
	function.RestoreUserLevelLocksFromMigration(proc, []function.UserLevelLockState{
		{Name: "discarded_lock", Count: 1},
	})

	ses := &Session{
		feSessionImpl: feSessionImpl{
			userLevelLocksMigrated: true,
		},
		proc: proc,
	}
	ses.Close()
	require.Empty(t, function.UserLevelLocksForMigration(proc))
}

func TestSessionCloseReleasesUserLevelLocksWhenNotMigrated(t *testing.T) {
	proc := testutil.NewProc(t)
	proc.GetSessionInfo().Account = "acc"
	proc.GetSessionInfo().ConnectionID = 1009
	lockService := &userLockCloseTestLockService{}
	proc.Base.LockService = lockService
	function.RestoreUserLevelLocksFromMigration(proc, []function.UserLevelLockState{
		{Name: "disconnect_cleanup", Count: 1},
	})

	// Reproduce the next-statement SessionInfo refresh after the protocol's
	// connection ID changed. The user-lock identity lives on BaseProcess and
	// must survive this replacement until Session.Close releases the lock.
	sessionID := proc.GetSessionInfo().SessionId
	proc.Base.SessionInfo = process.SessionInfo{
		Account:      "acc",
		ConnectionID: 1010,
		SessionId:    sessionID,
	}

	ses := &Session{
		feSessionImpl: feSessionImpl{
			userLevelLocksMigrated: false,
		},
		proc: proc,
	}
	ses.Close()
	require.Empty(t, function.UserLevelLocksForMigration(proc))
	require.Len(t, lockService.unlockedTxnIDs, 4)
	var currentTxnIDs int
	var legacyTxnIDs int
	for _, txnID := range lockService.unlockedTxnIDs {
		txnIDText := string(txnID)
		parts := strings.Split(txnIDText, "\x00")
		switch len(parts) {
		case 4:
			require.Equal(t, "mo-user-level-lock", parts[0])
			require.Equal(t, "disconnect_cleanup", parts[2])
			connID, err := strconv.ParseUint(parts[3], 10, 64)
			require.NoError(t, err)
			require.Equal(t, uint64(1009), connID)
			currentTxnIDs++
		case 3:
			// Legacy IDs have no connection-ID field. Do not search the full
			// string for "1010": the owner includes a random UUID that may
			// legitimately contain those digits.
			require.Equal(t, "mo-user-level-lock", parts[0])
			require.Equal(t, "disconnect_cleanup", parts[2])
			legacyTxnIDs++
		default:
			require.Failf(t, "unexpected user lock txnID format", "txnID=%q", txnIDText)
		}
	}
	require.Equal(t, 2, currentTxnIDs)
	require.Equal(t, 2, legacyTxnIDs)
	owner, connID := proc.GetUserLevelLockIdentity()
	require.NotEmpty(t, owner)
	require.Equal(t, uint64(1009), connID)
}

type userLockCloseTestLockService struct {
	lockservice.LockService
	unlockedTxnIDs [][]byte
}

func (s *userLockCloseTestLockService) Unlock(
	_ context.Context,
	txnID []byte,
	_ timestamp.Timestamp,
	_ ...lockpb.ExtraMutation,
) error {
	s.unlockedTxnIDs = append(s.unlockedTxnIDs, append([]byte(nil), txnID...))
	return nil
}

func TestRoutineManagerContextAndConnectionInfoHelpers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rm := &RoutineManager{ctx: ctx, cancel: cancel}
	require.Same(t, ctx, rm.getCtx())
	rm.cancelCtx()
	require.ErrorIs(t, ctx.Err(), context.Canceled)

	var nilRM *RoutineManager
	nilRM.cancelCtx()

	require.Equal(t, "connection from remote-only", getConnectionInfo(&Conn{remoteAddr: "remote-only"}))

	conn := &testConn{}
	ioSession := &Conn{conn: conn, remoteAddr: "remote"}
	require.Contains(t, getConnectionInfo(ioSession), "connection from")
	require.Contains(t, getConnectionInfo(ioSession), "to")
}
