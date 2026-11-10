// Copyright 2026 Matrix Origin
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

// newDefaultLimitsTestServerWithRoutine mirrors newTestServerWithRoutine, but
// keeps the real production limits filled by FrontendParameters.SetDefaultValues
// instead of the 1..100 test overrides. Default wait_timeout ceiling changes are
// only observable this way.
func newDefaultLimitsTestServerWithRoutine(t *testing.T, cap uint32) (*MOServer, *Conn, *Session, *MysqlProtocolImpl) {
	t.Helper()

	ctx := context.Background()
	sv := &config.FrontendParameters{}
	sv.SetDefaultValues()
	sv.KillRountinesInterval = 0

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

// newInheritedSession builds a fresh session whose global snapshot comes from the
// current one, mirroring how production sessions inherit the live global values
// at connection time (not the boot-time defaults).
func newInheritedSession(t *testing.T, ses *Session, pu *config.ParameterUnit) *Session {
	t.Helper()

	ctx := context.Background()
	tConn := &testConn{}
	ioses, err := NewIOSession(tConn, pu, "")
	require.NoError(t, err)

	proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)
	fresh := NewSession(ctx, "", proto, nil)
	proto.ses = fresh
	fresh.SetTenantInfo(getDefaultAccount())
	initTestSessionSysVars(fresh)
	fresh.gSysVars = ses.gSysVars
	fresh.sesSysVars = ses.gSysVars.Clone()
	return fresh
}

// MySQL documents wait_timeout as 1..31536000 on non-Windows servers. With nothing
// configured, the default policy admission gate and the system-variable type must
// both accept that range on both setter scopes, while the default value stays
// 86400.
func TestWaitTimeoutDefaultSessionRange(t *testing.T) {
	server, rs, ses, _ := newDefaultLimitsTestServerWithRoutine(t, 0)
	ctx := context.Background()

	// Default value is unchanged.
	val, err := ses.GetSessionSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(86400), val)

	for _, v := range []int64{86401, 1209600, 31536000} {
		require.NoError(t, ses.SetSessionSysVar(ctx, "wait_timeout", v))
		val, err = ses.GetSessionSysVar("wait_timeout")
		require.NoError(t, err)
		require.Equal(t, v, val, "wait_timeout=%d must be accepted by the session setter", v)

		// The idle-deadline consumer receives the full duration without overflow.
		server.applyIdleTimeout(rs)
		require.Equal(t, time.Duration(v)*time.Second, rs.timeout,
			"wait_timeout=%d must reach Conn.SetTimeout untruncated", v)
	}

	// Over the documented ceiling, malformed values and negatives are rejected
	// and leave the previous value intact.
	require.Error(t, ses.SetSessionSysVar(ctx, "wait_timeout", int64(31536001)))
	val, err = ses.GetSessionSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(31536000), val)

	require.Error(t, ses.SetSessionSysVar(ctx, "wait_timeout", int64(-1)))
	require.Error(t, ses.SetSessionSysVar(ctx, "wait_timeout", "not-a-number"))
	val, err = ses.GetSessionSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(31536000), val)

	// The session stays usable after rejected assignments.
	require.NoError(t, ses.SetSessionSysVar(ctx, "wait_timeout", int64(86401)))
	val, err = ses.GetSessionSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(86401), val)
}

func TestWaitTimeoutDefaultGlobalRange(t *testing.T) {
	_, _, ses, _ := newDefaultLimitsTestServerWithRoutine(t, 0)
	ctx := context.Background()
	pu := getPu("")

	stubGlobalSysVarPersistence(t,
		sysVarSet{"wait_timeout", int64(86401)},
		sysVarSet{"wait_timeout", int64(1209600)},
		sysVarSet{"wait_timeout", int64(31536000)},
		sysVarSet{"wait_timeout", int64(86400)},
	)

	// Keep the session value separated so GLOBAL changes cannot hide behind it.
	require.NoError(t, ses.SetSessionSysVar(ctx, "wait_timeout", int64(5)))

	for _, v := range []int64{86401, 1209600, 31536000} {
		require.NoError(t, ses.SetGlobalSysVar(ctx, "wait_timeout", v))
		val, err := ses.GetGlobalSysVar("wait_timeout")
		require.NoError(t, err)
		require.Equal(t, v, val, "wait_timeout=%d must be accepted by the global setter", v)

		// Existing sessions keep their own value: GLOBAL only feeds future sessions.
		val, err = ses.GetSessionSysVar("wait_timeout")
		require.NoError(t, err)
		require.Equal(t, int64(5), val)

		// A session opened after the change inherits the current global.
		fresh := newInheritedSession(t, ses, pu)
		val, err = fresh.GetSessionSysVar("wait_timeout")
		require.NoError(t, err)
		require.Equal(t, v, val)
	}

	// Over-ceiling GLOBAL assignment fails without mutating the stored global.
	require.Error(t, ses.SetGlobalSysVar(ctx, "wait_timeout", int64(31536001)))
	val, err := ses.GetGlobalSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(31536000), val)

	// Restore the global default even though the fixture is throwaway: the same
	// restore discipline is required against a durable catalog.
	require.NoError(t, ses.SetGlobalSysVar(ctx, "wait_timeout", int64(86400)))
	val, err = ses.GetGlobalSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(86400), val)
}

// Explicitly configured WaitTimeoutMin/Max stay hard policy bounds, including
// below the MySQL-documented ceiling the defaults now follow.
func TestWaitTimeoutExplicitConfigLimitStillHard(t *testing.T) {
	_, _, ses, _ := newDefaultLimitsTestServerWithRoutine(t, 0)
	ctx := context.Background()
	pu := getPu("")
	oldWaitMin, oldWaitMax := pu.SV.WaitTimeoutMin, pu.SV.WaitTimeoutMax
	defer func() {
		pu.SV.WaitTimeoutMin = oldWaitMin
		pu.SV.WaitTimeoutMax = oldWaitMax
	}()

	// An operator-configured ceiling of 100 blocks values the default policy
	// accepts, and rejects everything above it.
	pu.SV.WaitTimeoutMax = 100

	require.Error(t, ses.SetSessionSysVar(ctx, "wait_timeout", int64(86401)))
	require.Error(t, ses.SetSessionSysVar(ctx, "wait_timeout", int64(31536000)))
	require.NoError(t, ses.SetSessionSysVar(ctx, "wait_timeout", int64(100)))
	val, err := ses.GetSessionSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(100), val)

	require.Error(t, ses.SetSessionSysVar(ctx, "wait_timeout", int64(101)))
	val, err = ses.GetSessionSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(100), val)
}

// interactive_timeout keeps its own configured ceiling and the CLIENT_INTERACTIVE
// initialization; only wait_timeout's range changed.
func TestWaitTimeoutInteractivePolicySeparate(t *testing.T) {
	server, rs, ses, proto := newDefaultLimitsTestServerWithRoutine(t, CLIENT_INTERACTIVE)
	ctx := context.Background()

	// interactive_timeout policy is untouched: 86401 stays out of range and
	// its higher type bound does not admit 31536001 either.
	require.Error(t, ses.SetSessionSysVar(ctx, "interactive_timeout", int64(86401)))
	require.Error(t, ses.SetSessionSysVar(ctx, "interactive_timeout", int64(31536001)))
	require.NoError(t, ses.SetSessionSysVar(ctx, "interactive_timeout", int64(28800)))
	val, err := ses.GetSessionSysVar("interactive_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(28800), val)

	// Handshake copy from interactive_timeout still overrides wait_timeout,
	// including when wait_timeout is now settable up to the MySQL ceiling.
	require.NoError(t, ses.SetSessionSysVar(ctx, "wait_timeout", int64(31536000)))
	server.applyInteractiveWaitTimeout(ctx, ses, proto)
	val, err = ses.GetSessionSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(28800), val)
	server.applyIdleTimeout(rs)
	require.Equal(t, 28800*time.Second, rs.timeout)

	// A later explicit SET SESSION wait_timeout wins again.
	require.NoError(t, ses.SetSessionSysVar(ctx, "wait_timeout", int64(31536000)))
	server.applyIdleTimeout(rs)
	require.Equal(t, 31536000*time.Second, rs.timeout)
}

// A GLOBAL assignment rejected by the explicitly configured bounds must leave
// the stored global untouched and must never reach persistence; the rejecting
// session keeps its own value as well.
func TestWaitTimeoutGlobalCustomLimitRejectKeepsValue(t *testing.T) {
	_, _, ses, _ := newDefaultLimitsTestServerWithRoutine(t, 0)
	ctx := context.Background()
	pu := getPu("")
	oldWaitMin, oldWaitMax := pu.SV.WaitTimeoutMin, pu.SV.WaitTimeoutMax
	defer func() {
		pu.SV.WaitTimeoutMin = oldWaitMin
		pu.SV.WaitTimeoutMax = oldWaitMax
	}()

	// An operator-configured ceiling of 100 bounds GLOBAL too, including
	// values far below the MySQL-documented ceiling the defaults follow.
	pu.SV.WaitTimeoutMax = 100
	stubGlobalSysVarPersistence(t,
		sysVarSet{"wait_timeout", int64(100)},
	)

	require.NoError(t, ses.SetSessionSysVar(ctx, "wait_timeout", int64(90)))
	require.NoError(t, ses.SetGlobalSysVar(ctx, "wait_timeout", int64(100)))

	require.Error(t, ses.SetGlobalSysVar(ctx, "wait_timeout", int64(101)))
	require.Error(t, ses.SetGlobalSysVar(ctx, "wait_timeout", int64(86401)))
	require.Error(t, ses.SetGlobalSysVar(ctx, "wait_timeout", int64(31536000)))

	val, err := ses.GetGlobalSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(100), val)

	val, err = ses.GetSessionSysVar("wait_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(90), val)
}
