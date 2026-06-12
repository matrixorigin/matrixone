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

package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/config"
)

type sysVarSet struct {
	name  string
	value interface{}
}

func initTestSessionSysVars(ses *Session) {
	sysVars := make(map[string]interface{}, len(gSysVarsDefs))
	for name, sysVar := range gSysVarsDefs {
		sysVars[name] = sysVar.Default
	}
	ses.gSysVars = &SystemVariables{mp: sysVars}
	ses.sesSysVars = ses.gSysVars.Clone()
}

func stubGlobalSysVarPersistence(t *testing.T, vars ...sysVarSet) *backgroundExecTest {
	t.Helper()

	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result["begin;"] = nil
	bh.sql2result["commit;"] = nil
	bh.sql2result["rollback;"] = nil

	for _, v := range vars {
		bh.sql2result[getSqlForGetSysVarWithAccount(sysAccountID, v.name)] = newMrsForSystemVariableNameOfAccount([][]interface{}{})
		bh.sql2result[getSqlForInsertSysVarWithAccount(sysAccountID, sysAccountName, v.name, getVariableValue(v.value))] = nil
	}

	stub := gostub.StubFunc(&NewBackgroundExec, bh)
	t.Cleanup(stub.Reset)
	return bh
}

func newTestServerWithRoutine(t *testing.T, cap uint32) (*MOServer, *Conn, *Session, *MysqlProtocolImpl) {
	t.Helper()

	ctx := context.Background()
	sv := &config.FrontendParameters{}
	sv.SetDefaultValues()
	sv.KillRountinesInterval = 0
	sv.WaitTimeoutMin = 1
	sv.WaitTimeoutMax = 100
	sv.InteractiveTimeoutMin = 1
	sv.InteractiveTimeoutMax = 100

	pu := config.NewParameterUnit(sv, nil, nil, nil)
	setSessionAlloc("", NewLeakCheckAllocator())
	setPu("", pu)

	rm, err := NewRoutineManager(ctx, "")
	require.NoError(t, err)

	server := &MOServer{rm: rm}
	tConn := &testConn{}
	ioses, err := NewIOSession(tConn, pu, "")
	require.NoError(t, err)

	proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)
	proto.SetCapability(cap)

	ses := NewSession(ctx, "", proto, nil)
	proto.ses = ses
	ses.SetTenantInfo(getDefaultAccount())
	initTestSessionSysVars(ses)

	rt := &Routine{}
	rt.protocol.Store(&holder[MysqlRrWr]{value: proto})
	rt.ses = ses

	rm.setRoutine(ioses, 0, rt)

	return server, ioses, ses, proto
}

func TestInteractiveWaitTimeoutOverrideOnce(t *testing.T) {
	server, rs, ses, proto := newTestServerWithRoutine(t, CLIENT_INTERACTIVE)

	require.NoError(t, ses.SetSessionSysVar(context.Background(), "interactive_timeout", int64(20)))
	require.NoError(t, ses.SetSessionSysVar(context.Background(), "wait_timeout", int64(5)))

	server.applyInteractiveWaitTimeout(context.Background(), ses, proto)

	val, err := ses.GetSessionSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(20), val)

	// Subsequent session-level change should be effective and not overwritten.
	require.NoError(t, ses.SetSessionSysVar(context.Background(), "wait_timeout", int64(7)))
	server.applyIdleTimeout(rs)
	require.Equal(t, 7*time.Second, rs.timeout)
}

func TestNonInteractiveNoOverride(t *testing.T) {
	server, rs, ses, proto := newTestServerWithRoutine(t, 0)

	require.NoError(t, ses.SetSessionSysVar(context.Background(), "interactive_timeout", int64(20)))
	require.NoError(t, ses.SetSessionSysVar(context.Background(), "wait_timeout", int64(5)))

	server.applyInteractiveWaitTimeout(context.Background(), ses, proto)

	val, err := ses.GetSessionSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(5), val)

	server.applyIdleTimeout(rs)
	require.Equal(t, 5*time.Second, rs.timeout)
}

func TestGlobalTimeoutsAllowed(t *testing.T) {
	_, _, ses, _ := newTestServerWithRoutine(t, 0)
	stubGlobalSysVarPersistence(t,
		sysVarSet{"wait_timeout", int64(10)},
		sysVarSet{"interactive_timeout", int64(20)},
	)

	require.NoError(t, ses.SetSessionSysVar(context.Background(), "wait_timeout", int64(5)))
	require.NoError(t, ses.SetSessionSysVar(context.Background(), "interactive_timeout", int64(6)))

	require.NoError(t, ses.SetGlobalSysVar(context.Background(), "wait_timeout", int64(10)))
	require.NoError(t, ses.SetGlobalSysVar(context.Background(), "interactive_timeout", int64(20)))

	val, err := ses.GetGlobalSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(10), val)

	val, err = ses.GetGlobalSysVar("interactive_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(20), val)

	val, err = ses.GetSessionSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(5), val)

	val, err = ses.GetSessionSysVar("interactive_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(6), val)
}

func TestGlobalTimeoutBounds(t *testing.T) {
	_, _, ses, _ := newTestServerWithRoutine(t, 0)
	pu := getPu("")
	pu.SV.WaitTimeoutMin = 5
	pu.SV.WaitTimeoutMax = 10
	pu.SV.InteractiveTimeoutMin = 5
	pu.SV.InteractiveTimeoutMax = 10

	require.Error(t, ses.SetGlobalSysVar(context.Background(), "wait_timeout", int64(4)))
	require.Error(t, ses.SetGlobalSysVar(context.Background(), "wait_timeout", int64(11)))
	require.Error(t, ses.SetGlobalSysVar(context.Background(), "interactive_timeout", int64(4)))
	require.Error(t, ses.SetGlobalSysVar(context.Background(), "interactive_timeout", int64(11)))

	stubGlobalSysVarPersistence(t,
		sysVarSet{"wait_timeout", int64(5)},
		sysVarSet{"wait_timeout", int64(10)},
		sysVarSet{"interactive_timeout", int64(5)},
		sysVarSet{"interactive_timeout", int64(10)},
	)

	require.NoError(t, ses.SetGlobalSysVar(context.Background(), "wait_timeout", int64(5)))
	require.NoError(t, ses.SetGlobalSysVar(context.Background(), "wait_timeout", int64(10)))
	require.NoError(t, ses.SetGlobalSysVar(context.Background(), "interactive_timeout", int64(5)))
	require.NoError(t, ses.SetGlobalSysVar(context.Background(), "interactive_timeout", int64(10)))
}

func TestNewSessionTimeoutsFromGlobal(t *testing.T) {
	server, rs, ses, proto := newTestServerWithRoutine(t, 0)
	ses.gSysVars.Set("wait_timeout", int64(12))
	ses.gSysVars.Set("interactive_timeout", int64(21))
	ses.sesSysVars = ses.gSysVars.Clone()

	server.applyInteractiveWaitTimeout(context.Background(), ses, proto)

	val, err := ses.GetSessionSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(12), val)

	server.applyIdleTimeout(rs)
	require.Equal(t, 12*time.Second, rs.timeout)
}

func TestNewInteractiveSessionWaitTimeoutFromGlobalInteractiveTimeout(t *testing.T) {
	server, rs, ses, proto := newTestServerWithRoutine(t, CLIENT_INTERACTIVE)
	ses.gSysVars.Set("wait_timeout", int64(12))
	ses.gSysVars.Set("interactive_timeout", int64(21))
	ses.sesSysVars = ses.gSysVars.Clone()

	server.applyInteractiveWaitTimeout(context.Background(), ses, proto)

	val, err := ses.GetSessionSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(21), val)

	server.applyIdleTimeout(rs)
	require.Equal(t, 21*time.Second, rs.timeout)

	require.NoError(t, ses.SetSessionSysVar(context.Background(), "wait_timeout", int64(7)))
	server.applyIdleTimeout(rs)
	require.Equal(t, 7*time.Second, rs.timeout)
}

func TestSessionTimeoutBounds(t *testing.T) {
	_, _, ses, _ := newTestServerWithRoutine(t, 0)
	// Adjust bounds to verify validation.
	pu := getPu("")
	pu.SV.WaitTimeoutMin = 5
	pu.SV.WaitTimeoutMax = 10
	pu.SV.InteractiveTimeoutMin = 5
	pu.SV.InteractiveTimeoutMax = 10

	require.Error(t, ses.SetSessionSysVar(context.Background(), "wait_timeout", int64(4)))
	require.Error(t, ses.SetSessionSysVar(context.Background(), "wait_timeout", int64(11)))
	require.NoError(t, ses.SetSessionSysVar(context.Background(), "wait_timeout", int64(6)))

	require.Error(t, ses.SetSessionSysVar(context.Background(), "interactive_timeout", int64(4)))
	require.Error(t, ses.SetSessionSysVar(context.Background(), "interactive_timeout", int64(11)))
	require.NoError(t, ses.SetSessionSysVar(context.Background(), "interactive_timeout", int64(6)))
}

func TestSessionTimeoutBoundsZeroDisablesPUChecks(t *testing.T) {
	_, _, ses, _ := newTestServerWithRoutine(t, 0)
	pu := getPu("")
	oldWaitMin, oldWaitMax := pu.SV.WaitTimeoutMin, pu.SV.WaitTimeoutMax
	oldInteractiveMin, oldInteractiveMax := pu.SV.InteractiveTimeoutMin, pu.SV.InteractiveTimeoutMax
	defer func() {
		pu.SV.WaitTimeoutMin = oldWaitMin
		pu.SV.WaitTimeoutMax = oldWaitMax
		pu.SV.InteractiveTimeoutMin = oldInteractiveMin
		pu.SV.InteractiveTimeoutMax = oldInteractiveMax
	}()

	// 0 means "no additional PU bound", while system var type constraints still apply.
	pu.SV.WaitTimeoutMin = 0
	pu.SV.WaitTimeoutMax = 0
	pu.SV.InteractiveTimeoutMin = 0
	pu.SV.InteractiveTimeoutMax = 0

	// newTestServerWithRoutine sets max=100. Ensure values >100 are accepted when max=0.
	require.NoError(t, ses.SetSessionSysVar(context.Background(), "wait_timeout", int64(1000)))
	require.NoError(t, ses.SetSessionSysVar(context.Background(), "interactive_timeout", int64(1000)))
}

func TestSessionTimeoutNegativeInput(t *testing.T) {
	_, _, ses, _ := newTestServerWithRoutine(t, 0)

	require.Error(t, ses.SetSessionSysVar(context.Background(), "wait_timeout", int64(-1)))
	require.Error(t, ses.SetSessionSysVar(context.Background(), "interactive_timeout", int64(-1)))
}
