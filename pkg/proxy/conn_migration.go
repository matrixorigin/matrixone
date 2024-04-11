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

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

func (c *clientConn) getQueryAddress(addr string) string {
	var queryAddr string
	c.moCluster.GetCNService(clusterservice.NewSelectAll(), func(service metadata.CNService) bool {
		if service.SQLAddress == addr {
			queryAddr = service.QueryAddress
			return false
		}
		return true
	})
	return queryAddr
}

func (c *clientConn) migrateConnFrom(sqlAddr string) (*query.MigrateConnFromResponse, error) {
	req := c.queryService.NewRequest(query.CmdMethod_MigrateConnFrom)
	req.MigrateConnFromRequest = &query.MigrateConnFromRequest{
		ConnID: c.connID,
	}
	ctx, cancel := context.WithTimeout(c.ctx, time.Second*3)
	defer cancel()
	addr := c.getQueryAddress(sqlAddr)
	if addr == "" {
		return nil, moerr.NewInternalError(c.ctx, "cannot get query service address")
	}
	resp, err := c.queryService.SendMessage(ctx, addr, req)
	if err != nil {
		return nil, err
	}
	r := resp.MigrateConnFromResponse
	defer c.queryService.Release(resp)
	return r, nil
}

func (c *clientConn) migrateConnTo(sc ServerConn, info *query.MigrateConnFromResponse) error {
	// Before migrate session info with RPC, we need to execute some
	// SQLs to initialize the session and account in handler.
	// Currently, the session variable transferred is not used anywhere else,
	// and just used here.
	if _, err := sc.ExecStmt(internalStmt{
		cmdType: cmdQuery,
		s:       "/* cloud_nonuser */ set transferred=1;",
	}, nil); err != nil {
		return err
	}

	// First, we re-run the set variables statements.
	for _, stmt := range c.migration.setVarStmts {
		if _, err := sc.ExecStmt(internalStmt{
			cmdType: cmdQuery,
			s:       stmt,
		}, nil); err != nil {
			v2.ProxyConnectCommonFailCounter.Inc()
			return err
		}
	}

	// Then, migrate other info with RPC.
	addr := c.getQueryAddress(sc.RawConn().RemoteAddr().String())
	if addr == "" {
		return moerr.NewInternalError(c.ctx, "cannot get query service address")
	}
	req := c.queryService.NewRequest(query.CmdMethod_MigrateConnTo)
	req.MigrateConnToRequest = &query.MigrateConnToRequest{
		ConnID:       c.connID,
		DB:           info.DB,
		PrepareStmts: info.PrepareStmts,
	}
	ctx, cancel := context.WithTimeout(c.ctx, time.Second*3)
	defer cancel()
	resp, err := c.queryService.SendMessage(ctx, addr, req)
	if err != nil {
		return err
	}
	c.queryService.Release(resp)
	return nil
}

func (c *clientConn) migrateConn(prevAddr string, sc ServerConn) error {
	resp, err := c.migrateConnFrom(prevAddr)
	if err != nil {
		return err
	}
	if resp == nil {
		return moerr.NewInternalError(c.ctx, "bad response")
	}
	return c.migrateConnTo(sc, resp)
}
