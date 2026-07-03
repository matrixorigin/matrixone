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
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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
		ctx:              context.Background(),
		clients:          make(map[*Conn]*Routine),
		routinesByConnID: map[uint32]*Routine{1003: rt},
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

	pu := getPu("")
	pu.SV.CleanKillQueueInterval = 1
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
		accountRoutine: ar,
		service:        "",
	}

	rm.KillRoutineConnections()
	require.True(t, rt.isCancelled())
	require.NotContains(t, ar.accountId2Routine, int64(20))
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
		ConnID:                   1005,
		SkipUserLevelLockRelease: true,
	}, &query.MigrateConnFromResponse{}))
	require.True(t, ses.userLevelLocksMigrated)

	ses.userLevelLocksMigrated = true
	require.NoError(t, rm.MigrateConnectionFrom(&query.MigrateConnFromRequest{
		ConnID:                     1005,
		EnableUserLevelLockRelease: true,
	}, &query.MigrateConnFromResponse{}))
	require.False(t, ses.userLevelLocksMigrated)
}

func TestRoutineMigrateConnectionFromExportsUserLevelLocks(t *testing.T) {
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
	require.NoError(t, rt.migrateConnectionFrom(resp))
	require.Equal(t, "db1", resp.DB)
	require.Equal(t, []*query.UserLevelLock{{Name: "exported_lock", Count: 2}}, resp.UserLevelLocks)
	require.Len(t, resp.PrepareStmts, 1)
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
		feSessionImpl:          feSessionImpl{},
		proc:                   proc,
		userLevelLocksMigrated: true,
	}
	ses.Close()
	require.Empty(t, function.UserLevelLocksForMigration(proc))
}

func TestSessionCloseReleasesUserLevelLocksWhenNotMigrated(t *testing.T) {
	proc := testutil.NewProc(t)
	proc.GetSessionInfo().Account = "acc"
	proc.GetSessionInfo().ConnectionID = 1009

	ses := &Session{
		feSessionImpl:          feSessionImpl{},
		proc:                   proc,
		userLevelLocksMigrated: false,
	}
	ses.Close()
	require.Empty(t, function.UserLevelLocksForMigration(proc))
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
