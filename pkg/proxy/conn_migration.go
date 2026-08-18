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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
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
	r := resp.MigrateConnFromResponse
	if r == nil {
		return nil, moerr.NewInternalError(parent, "bad response")
	}
	if c.tun != nil {
		r.PrepareStmts = c.tun.filterClosedStatementsForMigration(r.PrepareStmts)
	}

	c.log.Info("connection migrate from server", zap.String("server address", addr),
		zap.String("tenant", string(c.clientInfo.Tenant)),
		zap.String("username", c.clientInfo.username),
		zap.Uint32("conn ID", c.connID),
		zap.String("DB", r.DB),
		zap.Int("prepare stmt num", len(r.PrepareStmts)),
		zap.Int64("goId", goid.Get()),
	)

	defer c.queryClient.Release(resp)
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
	ctx, cancel := context.WithTimeoutCause(parent, defaultTransferTimeout, moerr.CauseMigrateConnTo)
	defer cancel()

	typedMigration := info.UserDefinedVarsExported || info.SystemVariablesExported ||
		info.SystemVariablesSnapshotTooLarge || info.UserDefinedVarsSnapshotTooLarge
	typedMigrationSupported := false
	addr := ""
	if typedMigration {
		addr = getQueryAddress(c.moCluster, sc.RawConn().RemoteAddr().String())
		if addr == "" {
			return moerr.NewInternalError(ctx, "cannot get query service address")
		}
		targetProtocol, err := c.getTargetProtocolVersion(ctx, addr)
		if err != nil {
			return err
		}
		typedMigrationSupported = targetProtocol >= defines.MORPCVersion22
		if typedMigrationSupported && info.SystemVariablesSnapshotTooLarge {
			return moerr.NewInternalError(ctx,
				"cannot migrate typed system variables because the snapshot exceeds the connection migration size limit")
		}
		if typedMigrationSupported && info.UserDefinedVarsSnapshotTooLarge {
			return moerr.NewInternalError(ctx,
				"cannot migrate typed user variables because the snapshot exceeds the connection migration size limit")
		}
	}
	if !typedMigrationSupported {
		if info.UserDefinedVarsSnapshotTooLarge && !info.UserDefinedVarsReplayable {
			return moerr.NewInternalError(ctx,
				"cannot migrate oversized user variables to a pre-v22 target without complete raw replay")
		}
		if info.UserDefinedVarsExported && !info.UserDefinedVarsReplayable {
			return moerr.NewInternalError(ctx,
				"cannot migrate typed user variables to a pre-v22 target without complete raw replay")
		}
		if info.SystemVariablesExported && !info.SystemVariablesReplayable {
			return moerr.NewInternalError(ctx,
				"cannot migrate typed system variables to a pre-v22 target without complete raw replay")
		}
	}

	// Before migrate session info with RPC, we need to execute some
	// SQLs to initialize the session and account in handler.
	// Currently, the session variable transferred is not used anywhere else,
	// and just used here.
	if _, err := execStmtWithContext(ctx, sc, internalStmt{
		cmdType: cmdQuery,
		s:       "/* cloud_nonuser */ set transferred=1;",
	}, nil); err != nil {
		return err
	}

	// Preserve raw replay whenever the target cannot consume typed state or the
	// source did not export evaluated user variables. Typed system variables can
	// still be applied by the target after that replay.
	if !typedMigrationSupported || !info.UserDefinedVarsExported {
		for _, stmt := range c.migration.setVarStmts {
			if _, err := execStmtWithContext(ctx, sc, internalStmt{
				cmdType: cmdQuery,
				s:       stmt,
			}, nil); err != nil {
				v2.ProxyConnectCommonFailCounter.Inc()
				return err
			}
		}
	}

	// Then, migrate other info with RPC.
	if addr == "" {
		addr = getQueryAddress(c.moCluster, sc.RawConn().RemoteAddr().String())
	}
	if addr == "" {
		return moerr.NewInternalError(ctx, "cannot get query service address")
	}
	c.log.Info("connection migrate to server", zap.String("server address", addr),
		zap.String("tenant", string(c.clientInfo.Tenant)),
		zap.String("username", c.clientInfo.username),
		zap.Uint32("conn ID", c.connID),
		zap.Int64("goId", goid.Get()),
	)
	req := c.queryClient.NewRequest(query.CmdMethod_MigrateConnTo)
	req.MigrateConnToRequest = &query.MigrateConnToRequest{
		ConnID:                    c.connID,
		DB:                        info.DB,
		PrepareStmts:              info.PrepareStmts,
		LastAffectedRows:          info.LastAffectedRows,
		UserDefinedVars:           nil,
		UserDefinedVarsExported:   false,
		SystemVariables:           nil,
		SystemVariablesExported:   false,
		UserDefinedVarsReplayable: info.UserDefinedVarsReplayable,
		SystemVariablesReplayable: info.SystemVariablesReplayable,
	}
	if typedMigrationSupported {
		req.MigrateConnToRequest.UserDefinedVars = info.UserDefinedVars
		req.MigrateConnToRequest.UserDefinedVarsExported = info.UserDefinedVarsExported
		req.MigrateConnToRequest.SystemVariables = info.SystemVariables
		req.MigrateConnToRequest.SystemVariablesExported = info.SystemVariablesExported
	}
	if typedMigrationSupported && info.UserDefinedVarsExported && !info.SystemVariablesExported {
		req.MigrateConnToRequest.SetVarStmts = append([]string(nil), c.migration.systemSetVarStmts...)
	}
	resp, err := c.queryClient.SendMessage(ctx, addr, req)
	if err != nil {
		return moerr.AttachCause(ctx, err)
	}
	c.queryClient.Release(resp)
	return nil
}

func (c *clientConn) getTargetProtocolVersion(ctx context.Context, addr string) (int64, error) {
	if c.queryClient == nil {
		return 0, moerr.NewInternalError(ctx, "query client is not initialized")
	}
	req := c.queryClient.NewRequest(query.CmdMethod_GetProtocolVersion)
	req.GetProtocolVersion = &query.GetProtocolVersionRequest{}
	resp, err := c.queryClient.SendMessage(ctx, addr, req)
	if err != nil {
		if resp != nil {
			c.queryClient.Release(resp)
		}
		return 0, moerr.AttachCause(ctx, err)
	}
	if resp == nil || resp.GetProtocolVersion == nil {
		if resp != nil {
			c.queryClient.Release(resp)
		}
		return 0, moerr.NewInternalError(ctx, "target query service returned no protocol version")
	}
	version := resp.GetProtocolVersion.Version
	c.queryClient.Release(resp)
	if version <= 0 {
		return 0, moerr.NewInternalErrorf(ctx, "target query service returned invalid protocol version %d", version)
	}
	return version, nil
}

func (c *clientConn) migrateConnContext(
	ctx context.Context,
	prevAddr string,
	sc ServerConn,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	resp, err := c.migrateConnFromContext(ctx, prevAddr)
	if err != nil {
		return err
	}
	if !resp.UserLevelLockReleaseSupported {
		return moerr.NewInternalError(ctx, "cannot migrate connection from CN without user-level lock release support")
	}
	if len(resp.UserLevelLocks) > 0 {
		return moerr.NewInternalError(ctx, "cannot migrate connection while user-level locks are held")
	}
	if err := c.migrateConnToContext(ctx, sc, resp); err != nil {
		return err
	}
	if c.tun != nil {
		c.tun.clearClosedStatements()
	}
	return nil
}
