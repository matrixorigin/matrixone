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
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/stretchr/testify/require"
)

func killTestIdentity(account string, aid uint32, user string, uid uint32, role string) *TenantInfo {
	return &TenantInfo{Tenant: account, TenantID: aid, User: user, UserID: uid, DefaultRole: role}
}

func TestKillSessionAuthorization(t *testing.T) {
	ordinary := killTestIdentity("sys", 0, "ordinary", 7, "public")
	owner := killTestIdentity("acme", 10, "owner", 7, "public")
	tests := []struct {
		name           string
		caller, target *TenantInfo
		allow          bool
	}{
		{"sys ordinary cannot kill root", ordinary, killTestIdentity("sys", 0, "root", 0, "moadmin"), false},
		{"sys own user across roles", ordinary, killTestIdentity("sys", 0, "ordinary", 7, "analyst"), true},
		{"same account other user", owner, killTestIdentity("acme", 10, "other", 8, "public"), false},
		{"same user other account", owner, killTestIdentity("other", 11, "owner", 7, "public"), false},
		{"same name different user ID", owner, killTestIdentity("acme", 10, "owner", 8, "public"), false},
		{"same user", owner, owner, true},
		{"account admin own account", killTestIdentity("acme", 10, "admin", 9, "accountadmin"), owner, true},
		{"account admin other account", killTestIdentity("other", 11, "admin", 9, "accountadmin"), owner, false},
		{"sys accountadmin not global", killTestIdentity("sys", 0, "admin", 9, "accountadmin"), owner, false},
		{"non sys moadmin not global", killTestIdentity("other", 11, "admin", 9, "moadmin"), owner, false},
		{"moadmin global", killTestIdentity("sys", 0, "admin", 9, "moadmin"), owner, true},
		{"root name not authority", killTestIdentity("sys", 0, "root", 0, "public"), owner, false},
		{"missing caller", nil, owner, false}, {"missing target", owner, nil, false},
		{"empty role", killTestIdentity("acme", 10, "owner", 7, ""), owner, false},
		{"inconsistent sys ID", killTestIdentity("sys", 10, "admin", 9, "moadmin"), owner, false},
	}
	for _, tt := range tests {
		for _, connection := range []bool{false, true} {
			t.Run(tt.name+map[bool]string{false: "/query", true: "/connection"}[connection], func(t *testing.T) {
				rt, _ := newUnitTestRoutine(t, 100)
				ses := &Session{tenant: tt.target}
				rt.setSession(ses)
				ses.SetQueryInExecute(true)
				reqCtx, cancel := context.WithCancel(context.Background())
				defer cancel()
				rt.setCancelRequestFunc(cancel)
				rm := &RoutineManager{routinesByConnID: map[uint32]*Routine{100: rt}}
				found, err := rm.killLocal(context.Background(), killPrincipal(tt.caller), 1, 100, connection, "statement")
				if tt.allow {
					require.NoError(t, err)
					require.True(t, found)
					if connection {
						require.True(t, rt.isCancelled())
						require.True(t, rt.mc.closed)
					} else {
						require.ErrorIs(t, reqCtx.Err(), context.Canceled)
						require.False(t, ses.GetQueryInExecute())
					}
				} else {
					require.Error(t, err)
					require.NoError(t, reqCtx.Err())
					require.False(t, rt.isCancelled())
					require.False(t, rt.mc.closed)
					require.True(t, ses.GetQueryInExecute())
				}
			})
		}
	}
}

func TestKillSessionGenerationAndBoundaries(t *testing.T) {
	rt, _ := newUnitTestRoutine(t, 100)
	old := killTestIdentity("acme", 10, "old", 7, "public")
	next := killTestIdentity("acme", 10, "next", 8, "public")
	rt.setSession(&Session{tenant: old})
	rm := &RoutineManager{routinesByConnID: map[uint32]*Routine{100: rt}}
	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt.setCancelRequestFunc(cancel)
	// A lifecycle transition owns admission. KILL must reject without waiting for it.
	rt.mc.Lock()
	rt.mc.inProgress = true
	rt.mc.Unlock()
	found, err := rm.killLocal(context.Background(), killPrincipal(old), 1, 100, false, "")
	require.True(t, found)
	require.Error(t, err)
	require.NoError(t, reqCtx.Err())
	rt.mc.Lock()
	rt.setSession(&Session{tenant: next})
	rt.mc.inProgress = false
	rt.mc.Unlock()
	found, err = rm.killLocal(context.Background(), killPrincipal(old), 1, 100, false, "")
	require.True(t, found)
	require.Error(t, err)
	require.NoError(t, reqCtx.Err())
	// Cancellation executes while mc is held: a replacement cannot publish in the gap.
	rt.setCancelRequestFunc(func() { require.False(t, rt.mc.TryLock()); cancel() })
	found, err = rm.killLocal(context.Background(), killPrincipal(next), 1, 100, false, "")
	require.True(t, found)
	require.NoError(t, err)
	require.ErrorIs(t, reqCtx.Err(), context.Canceled)
	found, err = rm.killLocal(context.Background(), killPrincipal(next), 1, uint64(math.MaxUint32)+101, true, "")
	require.False(t, found)
	require.ErrorContains(t, err, "exceeds uint32")
	require.False(t, rt.isCancelled())
	found, err = rm.killLocal(context.Background(), killPrincipal(next), 1, 999, false, "")
	require.False(t, found)
	require.NoError(t, err)
	require.Error(t, func() error { _, e := rm.KillSession(context.Background(), nil); return e }())
	_, err = rm.KillSession(context.Background(), &query.KillSessionRequest{ConnID: 100})
	require.Error(t, err)
	ctx, stop := context.WithCancel(context.Background())
	stop()
	_, err = rm.killLocal(ctx, killPrincipal(next), 1, 100, true, "")
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, rt.isCancelled())
}

func TestKillSessionSelfAndDisconnect(t *testing.T) {
	for _, connection := range []bool{false, true} {
		t.Run(map[bool]string{false: "query", true: "connection"}[connection], func(t *testing.T) {
			rt, _ := newUnitTestRoutine(t, 100)
			owner := killTestIdentity("sys", 0, "ordinary", 7, "public")
			rt.setSession(&Session{tenant: owner})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rt.setCancelRequestFunc(cancel)
			rm := &RoutineManager{routinesByConnID: map[uint32]*Routine{100: rt}}
			found, err := rm.killLocal(context.Background(), killPrincipal(owner), 100, 100, connection, "")
			require.True(t, found)
			require.NoError(t, err)
			require.NoError(t, ctx.Err())
			require.Equal(t, connection, rt.isCancelled())
		})
	}
	rt, _ := newUnitTestRoutine(t, 101)
	rt.setSession(&Session{})
	rm := &RoutineManager{routinesByConnID: map[uint32]*Routine{101: rt}}
	require.NoError(t, rm.DisconnectSession(context.Background(), 101))
	require.True(t, rt.isCancelled())
	require.True(t, rt.mc.closed)
	require.NoError(t, rm.DisconnectSession(context.Background(), 999))
}

type killRoutingClient struct {
	qclient.QueryClient
	send     func(context.Context, string, *query.Request) (*query.Response, error)
	calls    []string
	releases int
}

func (c *killRoutingClient) NewRequest(method query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: method}
}
func (c *killRoutingClient) SendMessage(ctx context.Context, addr string, req *query.Request) (*query.Response, error) {
	c.calls = append(c.calls, addr)
	return c.send(ctx, addr, req)
}
func (c *killRoutingClient) Release(*query.Response) { c.releases++ }

func TestKillRemoteSessionsStopsAtOneEffect(t *testing.T) {
	sentinel := errors.New("denied or uncertain transport")
	for _, mode := range []string{"found", "absent", "error", "nil", "malformed", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			principal := killPrincipal(killTestIdentity("acme", 10, "owner", 7, "public"))
			request := &query.KillSessionRequest{ConnID: 100, Principal: principal, StatementID: "stmt"}
			c := &killRoutingClient{}
			c.send = func(ctx context.Context, addr string, req *query.Request) (*query.Response, error) {
				require.Equal(t, query.CmdMethod_KillSession, req.CmdMethod)
				require.Equal(t, request, req.KillSessionRequest)
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.WithinDuration(t, time.Now().Add(10*time.Second), deadline, time.Second)
				if addr == "a" {
					return &query.Response{KillSessionResponse: &query.KillSessionResponse{}}, nil
				}
				switch mode {
				case "error":
					return &query.Response{}, sentinel
				case "nil":
					return nil, nil
				case "malformed":
					return &query.Response{}, nil
				case "cancel":
					cancel()
					return nil, ctx.Err()
				}
				return &query.Response{KillSessionResponse: &query.KillSessionResponse{Found: mode == "found"}}, nil
			}
			err := killRemoteSessions(ctx, c, []string{"a", "b", "c"}, request)
			if mode == "found" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			expected := []string{"a", "b"}
			if mode == "absent" {
				expected = append(expected, "c")
			}
			require.Equal(t, expected, c.calls)
			releases := len(expected)
			if mode == "nil" || mode == "cancel" {
				releases--
			}
			require.Equal(t, releases, c.releases)
		})
	}
}

func TestKillRoutingLocalAndAddressSnapshot(t *testing.T) {
	cluster := &mockMOCluster{cnServices: []metadata.CNService{
		{ServiceID: "self", QueryAddress: "self-query"}, {ServiceID: "a", QueryAddress: "remote-a"},
		{ServiceID: "b", QueryAddress: "remote-a"}, {ServiceID: "c"}, {ServiceID: "d", QueryAddress: "remote-d"},
	}}
	require.Equal(t, []string{"remote-a", "remote-d"}, remoteKillAddresses(cluster, "self"))
	rt, _ := newUnitTestRoutine(t, 100)
	owner := killTestIdentity("acme", 10, "owner", 7, "public")
	rt.setSession(&Session{tenant: owner})
	rm := &RoutineManager{routinesByConnID: map[uint32]*Routine{100: rt}}
	previous := getRtMgr("")
	setRtMgr("", rm)
	defer setRtMgr("", previous)
	pu := getPu("")
	previousClient := pu.QueryClient
	t.Cleanup(func() { pu.QueryClient = previousClient })
	pu.QueryClient = &killRoutingClient{send: func(context.Context, string, *query.Request) (*query.Response, error) {
		t.Fatal("local action or denial must not route")
		return nil, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt.setCancelRequestFunc(cancel)
	caller := &Session{tenant: killTestIdentity("acme", 10, "other", 8, "public")}
	require.Error(t, killSession(context.Background(), caller, 100, false, ""))
	require.NoError(t, ctx.Err())
	caller.tenant = owner
	require.NoError(t, killSession(context.Background(), caller, 100, false, ""))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	snapshot := killPrincipal(owner)
	owner.mu.Lock()
	owner.DefaultRole = "accountadmin"
	owner.mu.Unlock()
	require.Equal(t, "public", snapshot.Role, "forwarded identity must be an immutable current-session snapshot")
}
