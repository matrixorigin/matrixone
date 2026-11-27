// Copyright 2021 - 2024 Matrix Origin
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

package proxy

import (
	"context"
	"time"

	"github.com/petermattis/goid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
)

func (c *clientConn) migrateConnFrom(sqlAddr string) (*query.MigrateConnFromResponse, error) {
	return c.migrateConnFromContext(c.ctx, sqlAddr)
}

func (c *clientConn) migrateConnFromContext(
	parent context.Context,
	sqlAddr string,
) (*query.MigrateConnFromResponse, error) {
	if parent == nil {
		parent = context.Background()
	}
	if c.queryClient == nil || c.moCluster == nil {
		return nil, moerr.NewInternalError(parent, "migration source query service is unavailable")
	}
	req := c.queryClient.NewRequest(query.CmdMethod_MigrateConnFrom)
	req.MigrateConnFromRequest = &query.MigrateConnFromRequest{
		ConnID: c.connID,
	}
	ctx, cancel := context.WithTimeoutCause(parent, time.Second*3, moerr.CauseMigrateConnFrom)
	defer cancel()
	addr := getQueryAddress(c.moCluster, sqlAddr)
	if addr == "" {
		return nil, moerr.NewInternalError(parent, "cannot get query service address")
	}
	resp, err := c.queryClient.SendMessage(ctx, addr, req)
	if err != nil {
		return nil, moerr.AttachCause(ctx, err)
	}
	if resp == nil {
		return nil, moerr.NewInternalError(parent, "missing migration source response")
	}
	defer c.queryClient.Release(resp)
	r := resp.MigrateConnFromResponse
	if r == nil {
		return nil, moerr.NewInternalError(parent, "missing migration source response")
	}

	c.log.Info("connection migrate from server", zap.String("server address", addr),
		zap.String("tenant", string(c.clientInfo.Tenant)),
		zap.String("username", c.clientInfo.username),
		zap.Uint32("conn ID", c.connID),
		zap.String("DB", r.DB),
		zap.Int("prepare stmt num", len(r.PrepareStmts)),
		zap.Int64("goId", goid.Get()),
	)

	return r, nil
}

func (c *clientConn) migrateConnTo(sc ServerConn, info *query.MigrateConnFromResponse) error {
	return c.migrateConnToContext(c.ctx, sc, info)
}

func (c *clientConn) migrateConnToContext(
	parent context.Context,
	sc ServerConn,
	info *query.MigrateConnFromResponse,
) error {
	if parent == nil {
		parent = context.Background()
	}
	// Before migrate session info with RPC, we need to execute some
	// SQLs to initialize the session and account in handler.
	// Currently, the session variable transferred is not used anywhere else,
	// and just used here.
	if _, err := execStmtWithContext(parent, sc, internalStmt{
		cmdType: cmdQuery,
		s:       "/* cloud_nonuser */ set transferred=1;",
	}, nil); err != nil {
		return err
	}

	// The control read above re-arms the frontend IOSession's
	// SessionTimeout. Clear that phase-owned deadline before the backend is
	// handed to the tunnel; otherwise an actively used migrated connection can
	// still expire at the end of the session timeout period.
	if err := clearServerConnReadDeadline(sc); err != nil {
		return moerr.AttachCause(parent, err)
	}

	// Then, migrate other info with RPC.
	addr := getQueryAddress(c.moCluster, sc.RawConn().RemoteAddr().String())
	if addr == "" {
		return moerr.NewInternalError(parent, "cannot get query service address")
	}
	c.log.Info("connection migrate to server", zap.String("server address", addr),
		zap.String("tenant", string(c.clientInfo.Tenant)),
		zap.String("username", c.clientInfo.username),
		zap.Uint32("conn ID", c.connID),
		zap.Int64("goId", goid.Get()),
	)
	req := c.queryClient.NewRequest(query.CmdMethod_MigrateConnTo)
	req.MigrateConnToRequest = &query.MigrateConnToRequest{
		ConnID:           c.connID,
		DB:               info.DB,
		SetVarStmts:      c.migration.setVarStmts,
		PrepareStmts:     info.PrepareStmts,
		LastAffectedRows: info.LastAffectedRows,
		RoleState:        info.RoleState,
	}
	ctx, cancel := context.WithTimeoutCause(parent, defaultTransferTimeout, moerr.CauseMigrateConnTo)
	defer cancel()
	resp, err := c.queryClient.SendMessage(ctx, addr, req)
	if err != nil {
		return moerr.AttachCause(ctx, err)
	}
	if resp == nil {
		return moerr.NewInternalError(parent, "missing migration destination response")
	}
	defer c.queryClient.Release(resp)
	ack := resp.GetMigrateConnToResponse()
	if ack == nil || !ack.Success {
		return moerr.NewInternalError(parent, "destination did not acknowledge session migration")
	}
	return nil
}

func (c *clientConn) migrationSourceContext(ctx context.Context, prevAddr string) (*query.MigrateConnFromResponse, error) {
	resp, err := c.migrateConnFromContext(ctx, prevAddr)
	if err != nil {
		return nil, err
	}
	if !resp.UserLevelLockReleaseSupported {
		return nil, moerr.NewInternalError(ctx, "cannot migrate connection from CN without user-level lock release support")
	}
	if len(resp.UserLevelLocks) > 0 {
		return nil, moerr.NewInternalError(ctx, "cannot migrate connection while user-level locks are held")
	}
	if resp.RoleState == nil {
		return nil, moerr.GetOkExpectedNotSafeToStartTransfer()
	}
	return resp, nil
}

func isMigrationRefusal(err error) bool {
	return moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer) || isRetryableMigrationRefusal(err)
}

func isRetryableMigrationRefusal(err error) bool {
	return moerr.IsMoErrCode(err, moerr.OkExpectedMigrationPolicyChanged) ||
		moerr.IsMoErrCode(err, moerr.OkExpectedMigrationPreparedState) ||
		moerr.IsMoErrCode(err, moerr.OkExpectedMigrationRoleChanged)
}
