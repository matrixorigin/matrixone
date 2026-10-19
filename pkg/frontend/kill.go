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
	"math"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
)

func killPrincipal(tenant *TenantInfo) *query.KillSessionPrincipal {
	if tenant == nil {
		return nil
	}
	tenant.mu.Lock()
	defer tenant.mu.Unlock()
	return &query.KillSessionPrincipal{
		AccountID: tenant.TenantID, UserID: tenant.UserID,
		Account: tenant.Tenant, User: tenant.User, Role: tenant.DefaultRole,
	}
}

func validKillPrincipal(p *query.KillSessionPrincipal) bool {
	return p != nil && p.Account != "" && p.User != "" && p.Role != "" &&
		(strings.EqualFold(p.Account, sysAccountName) == (p.AccountID == sysAccountID))
}

func canKillSession(caller, target *query.KillSessionPrincipal) bool {
	if !validKillPrincipal(caller) || !validKillPrincipal(target) {
		return false
	}
	sys := strings.EqualFold(caller.Account, sysAccountName)
	if sys && strings.EqualFold(caller.Role, moAdminRoleName) {
		return true
	}
	return caller.AccountID == target.AccountID &&
		((!sys && strings.EqualFold(caller.Role, accountAdminRoleName)) || caller.UserID == target.UserID)
}

// killLocal pins the target generation from authorization through cancellation.
// Absence is separate from denial so only absence can trigger remote routing.
func (rm *RoutineManager) killLocal(ctx context.Context, principal *query.KillSessionPrincipal,
	callerID, targetID uint64, killConnection bool, statementID string,
) (bool, error) {
	if targetID > math.MaxUint32 {
		return false, moerr.NewInvalidInputf(ctx, "connection id %d exceeds uint32", targetID)
	}
	if !validKillPrincipal(principal) {
		return false, moerr.NewInternalError(ctx, "access denied: missing authenticated KILL identity")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	rt := rm.getRoutineByConnID(uint32(targetID))
	if rt == nil {
		return false, nil
	}
	rt.mc.Lock()
	if rt.mc.closed || rt.mc.inProgress {
		rt.mc.Unlock()
		return true, moerr.NewInternalError(ctx, "cannot kill a closed or changing session")
	}
	ses := rt.getSession()
	if ses == nil || !canKillSession(principal, killPrincipal(ses.GetTenantInfo())) {
		rt.mc.Unlock()
		return true, moerr.NewInternalError(ctx, "access denied: KILL requires session ownership or an administrative role")
	}
	if err := ctx.Err(); err != nil {
		rt.mc.Unlock()
		return true, err
	}
	killMyself := callerID == targetID
	if !killConnection {
		rt.killQuery(killMyself, statementID)
		rt.mc.Unlock()
		return true, nil
	}
	// Seal identity-changing admission before releasing the guard. Closing the
	// protocol can enter routine cleanup and must happen outside this lock.
	rt.mc.closed = true
	rt.mc.cond.Broadcast()
	rt.mc.Unlock()
	rt.killConnection(killMyself)
	return true, nil
}

// KillSession is a local-only operation for the trusted CN query service.
// A remote request cannot claim the local self-KILL exception or forward again.
func (rm *RoutineManager) KillSession(ctx context.Context, req *query.KillSessionRequest) (bool, error) {
	if req == nil {
		return false, moerr.NewInvalidInput(ctx, "missing kill session request")
	}
	return rm.killLocal(ctx, req.Principal, math.MaxUint64, req.ConnID, req.KillConnection, req.StatementID)
}

// DisconnectSession is trusted proxy lifecycle cleanup, independent of SQL
// identity. The captured backend cannot be cached/reassigned during this call.
func (rm *RoutineManager) DisconnectSession(ctx context.Context, connID uint32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if rt := rm.getRoutineByConnID(connID); rt != nil {
		rt.beginClose()
		rt.killConnection(false)
	}
	return nil
}

func killSession(ctx context.Context, ses *Session, targetID uint64, connection bool, statementID string) error {
	principal := killPrincipal(ses.GetTenantInfo())
	found, err := getRtMgr(ses.GetService()).killLocal(ctx, principal, uint64(ses.GetConnectionID()), targetID, connection, statementID)
	if found || err != nil {
		return err
	}
	qc := getPu(ses.GetService()).QueryClient
	if qc == nil {
		return moerr.NewInternalErrorf(ctx, "Unknown connection id %d", targetID)
	}
	addresses := remoteKillAddresses(clusterservice.GetMOCluster(qc.ServiceID()), qc.ServiceID())
	return killRemoteSessions(ctx, qc, addresses, &query.KillSessionRequest{
		ConnID: targetID, KillConnection: connection, StatementID: statementID, Principal: principal,
	})
}

func remoteKillAddresses(cluster clusterservice.MOCluster, localID string) []string {
	var addresses []string
	seen := make(map[string]struct{})
	cluster.GetCNService(clusterservice.NewSelectAll(), func(cn metadata.CNService) bool {
		if cn.ServiceID != localID && cn.QueryAddress != "" {
			if _, ok := seen[cn.QueryAddress]; !ok {
				seen[cn.QueryAddress] = struct{}{}
				addresses = append(addresses, cn.QueryAddress)
			}
		}
		return true
	})
	return addresses
}

func killRemoteSessions(ctx context.Context, qc qclient.QueryClient, addresses []string, kill *query.KillSessionRequest) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for _, addr := range addresses {
		if err := ctx.Err(); err != nil {
			return err
		}
		req := qc.NewRequest(query.CmdMethod_KillSession)
		req.KillSessionRequest = kill
		rsp, err := qc.SendMessage(ctx, addr, req)
		found := false
		if rsp != nil {
			if rsp.KillSessionResponse != nil {
				found = rsp.KillSessionResponse.Found
			} else if err == nil {
				err = moerr.NewInternalError(ctx, "missing kill session response")
			}
			qc.Release(rsp)
		} else if err == nil {
			err = moerr.NewInternalError(ctx, "missing kill session response")
		}
		if err != nil {
			return err
		}
		if found {
			return nil
		}
	}
	return moerr.NewInternalErrorf(ctx, "Unknown connection id %d", kill.ConnID)
}
