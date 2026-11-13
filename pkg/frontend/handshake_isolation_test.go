// Copyright 2026 Matrix Origin
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
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

type handshakeFaultConn struct {
	testConn
	closed         bool
	writes         int
	panicWrite     bool
	panicLocalAddr bool
}

func (c *handshakeFaultConn) Write(b []byte) (int, error) {
	c.writes++
	if c.panicWrite {
		panic("injected handshake write panic")
	}
	return len(b), nil
}
func (c *handshakeFaultConn) LocalAddr() net.Addr {
	if c.panicLocalAddr {
		panic("injected connection initialization panic")
	}
	return c.testConn.LocalAddr()
}
func (c *handshakeFaultConn) Read([]byte) (int, error) { return 0, context.Canceled }
func (c *handshakeFaultConn) Close() error             { c.closed = true; return nil }

func TestConnectionPanicIsolationAndCleanup(t *testing.T) {
	server, existing, ses, proto := newTestServerWithRoutine(t, 0)
	rt := NewRoutine(context.Background(), proto, getPu("").SV)
	rt.setSession(ses)
	ses.setRoutine(rt)
	server.rm.setRoutine(existing, 0, rt)
	server.pu = getPu("")
	oldRM := getRtMgr("")
	setRtMgr("", server.rm)
	t.Cleanup(func() { setRtMgr("", oldRM) })
	t.Cleanup(func() { server.rm.Closed(existing) })
	baseline := len(server.rm.sessionManager.GetAllSessions())
	for _, stage := range []string{"handshake-panic", "initialization-panic", "read-error"} {
		c := &handshakeFaultConn{panicWrite: stage == "handshake-panic", panicLocalAddr: stage == "initialization-panic"}
		require.NotPanics(t, func() { server.handleConn(context.Background(), c) }, "a handshake panic must be isolated to its connection")
		require.True(t, c.closed, "the failed connection must release its socket")
		if !c.panicLocalAddr {
			require.Positive(t, c.writes, "a later connection must still reach its handshake")
		}
		require.Equal(t, baseline, len(server.rm.sessionManager.GetAllSessions()))
		require.Len(t, server.rm.clients, 1, "the failed routine must be unregistered")
		require.Len(t, server.rm.routinesByConnID, 1, "the failed connection ID must be unregistered")
	}
	require.NotNil(t, ses)
	require.NotNil(t, server.rm.getRoutine(existing), "closing a failed handshake must not remove another connection")
}
