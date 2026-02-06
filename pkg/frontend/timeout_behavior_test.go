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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/config"
)

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

func TestGlobalTimeoutsForbidden(t *testing.T) {
	_, _, ses, _ := newTestServerWithRoutine(t, 0)

	err := ses.SetGlobalSysVar(context.Background(), "wait_timeout", int64(10))
	require.Error(t, err)

	err = ses.SetGlobalSysVar(context.Background(), "interactive_timeout", int64(10))
	require.Error(t, err)
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
