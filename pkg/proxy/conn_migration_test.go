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
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runTestWithQueryService(
	t *testing.T,
	cn metadata.CNService,
	fn func(cc *clientConn, addr string),
) {
	runTestWithQueryServiceHandler(t, cn, nil, fn)
}

func runTestWithQueryServiceHandler(
	t *testing.T,
	cn metadata.CNService,
	migrateConnToHandler func(context.Context, *pb.Request, *pb.Response, *morpc.Buffer) error,
	fn func(cc *clientConn, addr string),
) {
	runTestWithQueryServiceHandlers(t, cn, migrateConnToHandler, nil, fn)
}

func runTestWithQueryServiceResetHandler(
	t *testing.T,
	cn metadata.CNService,
	resetSessionHandler func(context.Context, *pb.Request, *pb.Response, *morpc.Buffer) error,
	fn func(cc *clientConn, addr string),
) {
	runTestWithQueryServiceHandlers(t, cn, nil, resetSessionHandler, fn)
}

func runTestWithQueryServiceHandlers(
	t *testing.T,
	cn metadata.CNService,
	migrateConnToHandler func(context.Context, *pb.Request, *pb.Response, *morpc.Buffer) error,
	resetSessionHandler func(context.Context, *pb.Request, *pb.Response, *morpc.Buffer) error,
	fn func(cc *clientConn, addr string),
) {
	runTestWithQueryServiceHandlersAndRefresh(
		t, cn, migrateConnToHandler, resetSessionHandler, nil, fn)
}

func runTestWithQueryServiceHandlersAndRefresh(
	t *testing.T,
	cn metadata.CNService,
	migrateConnToHandler func(context.Context, *pb.Request, *pb.Response, *morpc.Buffer) error,
	resetSessionHandler func(context.Context, *pb.Request, *pb.Response, *morpc.Buffer) error,
	refreshSessionAuthHandler func(context.Context, *pb.Request, *pb.Response, *morpc.Buffer) error,
	fn func(cc *clientConn, addr string),
) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			defer leaktest.AfterTest(t)()
			runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
			address := fmt.Sprintf("unix:///tmp/cn-%d-%s.sock",
				time.Now().Nanosecond(), cn.ServiceID)

			if err := os.RemoveAll(address[7:]); err != nil {
				panic(err)
			}
			cluster := clusterservice.NewMOCluster(
				sid,
				nil,
				0,
				clusterservice.WithDisableRefresh(),
				clusterservice.WithServices([]metadata.CNService{{
					ServiceID:    cn.ServiceID,
					SQLAddress:   cn.SQLAddress,
					QueryAddress: address,
				}}, nil))
			defer cluster.Close()
			runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.ClusterService, cluster)
			runtime.SetupServiceBasedRuntime(cn.ServiceID, rt)

			qs, err := queryservice.NewQueryService(cn.ServiceID, address, morpc.Config{})
			assert.NoError(t, err)

			qt, err := qclient.NewQueryClient(cn.ServiceID, morpc.Config{})
			assert.NoError(t, err)

			qs.AddHandleFunc(pb.CmdMethod_MigrateConnFrom, func(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
				if req.MigrateConnFromRequest == nil {
					return moerr.NewInternalError(ctx, "bad request")
				}
				if !req.MigrateConnFromRequest.TempTableMigrationSupported {
					return moerr.NewInternalError(ctx, "missing temporary-table migration capability")
				}
				resp.MigrateConnFromResponse = &pb.MigrateConnFromResponse{
					DB:                            "d1",
					LastAffectedRows:              7,
					FoundRows:                     11,
					UserLevelLockReleaseSupported: true,
					TempTableStateExported:        true,
				}
				return nil
			}, false)
			if migrateConnToHandler == nil {
				migrateConnToHandler = func(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
					if req.MigrateConnToRequest == nil {
						return moerr.NewInternalError(ctx, "bad request")
					}
					if req.MigrateConnToRequest.LastAffectedRows != 7 {
						return moerr.NewInternalErrorf(ctx, "unexpected last affected rows: %d",
							req.MigrateConnToRequest.LastAffectedRows)
					}
					resp.MigrateConnToResponse = &pb.MigrateConnToResponse{
						Success: true,
					}
					return nil
				}
			}
			qs.AddHandleFunc(pb.CmdMethod_MigrateConnTo, migrateConnToHandler, false)
			qs.AddHandleFunc(pb.CmdMethod_ResetSession, func(ctx context.Context, req *pb.Request, resp *pb.Response, buf *morpc.Buffer) error {
				if resetSessionHandler != nil {
					return resetSessionHandler(ctx, req, resp, buf)
				}
				if req.ResetSessionRequest == nil {
					return moerr.NewInternalError(ctx, "bad request")
				}
				resp.ResetSessionResponse = &pb.ResetSessionResponse{
					AuthString: nil,
					Success:    true,
				}
				return nil
			}, false)
			if refreshSessionAuthHandler == nil {
				refreshSessionAuthHandler = func(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
					if req.RefreshSessionAuthRequest == nil {
						return moerr.NewInternalError(ctx, "bad request")
					}
					resp.RefreshSessionAuthResponse = &pb.RefreshSessionAuthResponse{
						AuthString: []byte("auth"),
						Success:    true,
					}
					return nil
				}
			}
			qs.AddHandleFunc(pb.CmdMethod_RefreshSessionAuth, refreshSessionAuthHandler, false)
			err = qs.Start()
			assert.NoError(t, err)

			cc, closeFn := createNewClientConn(t)
			defer closeFn()
			ccc := cc.(*clientConn)
			ccc.queryClient = qt
			ccc.moCluster = cluster
			fn(ccc, cn.SQLAddress)

			err = qs.Close()
			assert.NoError(t, err)
			err = qt.Close()
			assert.NoError(t, err)
		},
	)

}

func TestQueryServiceMigrateFrom(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "127.0.0.1:9000"}
	runTestWithQueryService(t, cn, func(cc *clientConn, addr string) {
		resp, err := cc.migrateConnFrom(addr)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "d1", resp.DB)
		assert.Equal(t, int64(7), resp.LastAffectedRows)
		assert.Equal(t, uint64(11), resp.FoundRows)
	})
}

func TestMigrateConnFromRejectsCNWithoutTempTableSnapshotSupport(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe", QueryAddress: "query"}
	cluster := clusterservice.NewMOCluster(
		"", nil, 0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{cn}, nil),
	)
	defer cluster.Close()

	cc, closeFn := createNewClientConn(t)
	defer closeFn()
	ccc := cc.(*clientConn)
	queryClient := &migrationUserLockQueryClient{omitTempTableState: true}
	ccc.queryClient = queryClient
	ccc.moCluster = cluster

	_, err := ccc.migrateConnFromContext(context.Background(), "pipe")
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer))
	require.Equal(t, 1, queryClient.releaseCount)
}

func TestQueryServiceMigrateTo(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	handler := func(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
		if req.MigrateConnToRequest == nil {
			return moerr.NewInternalError(ctx, "bad request")
		}
		if req.MigrateConnToRequest.LastAffectedRows != 7 {
			return moerr.NewInternalErrorf(ctx, "unexpected last affected rows: %d",
				req.MigrateConnToRequest.LastAffectedRows)
		}
		if req.MigrateConnToRequest.FoundRows != 11 {
			return moerr.NewInternalErrorf(ctx, "unexpected found rows: %d",
				req.MigrateConnToRequest.FoundRows)
		}
		resp.MigrateConnToResponse = &pb.MigrateConnToResponse{Success: true}
		return nil
	}
	runTestWithQueryServiceHandler(t, cn, handler, func(cc *clientConn, addr string) {
		resp, err := cc.migrateConnFrom(addr)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "d1", resp.DB)

		c1, _ := net.Pipe()
		sc := newMockServerConn(c1)
		cc.migration.setVarStmts = append(cc.migration.setVarStmts, "set a=1")
		err = cc.migrateConnTo(sc, resp)
		assert.NoError(t, err)
	})
}

func TestQueryServiceMigrateToClearsReadDeadlineAfterControlReads(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	runTestWithQueryService(t, cn, func(cc *clientConn, _ string) {
		local, remote := net.Pipe()
		defer remote.Close()
		raw := &phaseDeadlineConn{Conn: local}
		statements := make([]string, 0, 2)
		sc := &deadlineRearmingServerConn{
			ServerConn: newMockServerConn(raw),
			raw:        raw,
			statements: &statements,
		}
		defer sc.Close()

		cc.migration.setVarStmts = []string{"set @mode = 'PIPES_AS_CONCAT'"}
		require.NoError(t, cc.migrateConnTo(sc, &pb.MigrateConnFromResponse{LastAffectedRows: 7}))
		assert.Equal(t, []string{
			"/* cloud_nonuser */ set transferred=1;",
			"set @mode = 'PIPES_AS_CONCAT'",
		}, statements)
		assert.True(t, raw.readDeadline().IsZero(),
			"migration must clear the deadline armed by its final control read")
	})
}

func TestQueryServiceMigrateToRejectsReadDeadlineClearFailure(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	runTestWithQueryService(t, cn, func(cc *clientConn, _ string) {
		local, remote := net.Pipe()
		defer remote.Close()
		raw := &phaseDeadlineConn{Conn: local, failClear: true}
		sc := &deadlineRearmingServerConn{
			ServerConn: newMockServerConn(raw),
			raw:        raw,
		}
		defer sc.Close()

		err := cc.migrateConnTo(sc, &pb.MigrateConnFromResponse{})
		assert.ErrorContains(t, err, "read deadline clear failed")
		assert.False(t, raw.readDeadline().IsZero(),
			"a failed clear must not make the backend eligible for handoff")
	})
}

func TestQueryServiceMigrateToRejectsNonZeroFoundRowsForPreV29Target(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	runTestWithQueryService(t, cn, func(cc *clientConn, _ string) {
		targetRuntime := runtime.ServiceRuntime(cn.ServiceID)
		oldVersion, hadVersion := targetRuntime.GetGlobalVariables(runtime.MOProtocolVersion)
		targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion28)
		defer func() {
			if hadVersion {
				targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion)
			} else {
				targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
			}
		}()

		local, remote := net.Pipe()
		defer remote.Close()
		sc := &recordingMigrationServerConn{mockServerConn: newMockServerConn(local)}
		defer sc.Close()

		err := cc.migrateConnTo(sc, &pb.MigrateConnFromResponse{FoundRows: 11})
		assert.ErrorContains(t, err, "cannot migrate non-zero FOUND_ROWS state to a pre-v29 target")
		assert.Empty(t, sc.statements)
	})
}

func TestQueryServiceMigrateToAllowsZeroFoundRowsForPreV22Target(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	runTestWithQueryService(t, cn, func(cc *clientConn, _ string) {
		targetRuntime := runtime.ServiceRuntime(cn.ServiceID)
		oldVersion, hadVersion := targetRuntime.GetGlobalVariables(runtime.MOProtocolVersion)
		targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion20)
		defer func() {
			if hadVersion {
				targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion)
			} else {
				targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
			}
		}()

		local, remote := net.Pipe()
		defer remote.Close()
		sc := &recordingMigrationServerConn{mockServerConn: newMockServerConn(local)}
		defer sc.Close()

		assert.NoError(t, cc.migrateConnTo(sc, &pb.MigrateConnFromResponse{LastAffectedRows: 7}))
		assert.Equal(t, []string{"/* cloud_nonuser */ set transferred=1;"}, sc.statements)
	})
}

func TestQueryServiceMigrateToCarriesTemporaryTables(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	tables := []*pb.MigrateTempTable{{
		Database: "d1", Alias: "tmp", PhysicalName: "__mo_tmp_source_d1_tmp",
	}}
	handler := func(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
		migration := req.MigrateConnToRequest
		if migration == nil {
			return moerr.NewInternalError(ctx, "bad request")
		}
		assert.Equal(t, tables, migration.TempTables)
		resp.MigrateConnToResponse = &pb.MigrateConnToResponse{Success: true}
		return nil
	}
	runTestWithQueryServiceHandler(t, cn, handler, func(cc *clientConn, _ string) {
		local, remote := net.Pipe()
		defer remote.Close()
		sc := &recordingMigrationServerConn{mockServerConn: newMockServerConn(local)}
		defer sc.Close()

		require.NoError(t, cc.migrateConnTo(sc, &pb.MigrateConnFromResponse{
			TempTables: tables,
		}))
		assert.Equal(t, []string{"/* cloud_nonuser */ set transferred=1;"}, sc.statements)
	})
}

func TestQueryServiceMigrateToRejectsTemporaryTablesForPreV38Target(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	runTestWithQueryService(t, cn, func(cc *clientConn, _ string) {
		targetRuntime := runtime.ServiceRuntime(cn.ServiceID)
		oldVersion, hadVersion := targetRuntime.GetGlobalVariables(runtime.MOProtocolVersion)
		targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion36)
		defer func() {
			if hadVersion {
				targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion)
			} else {
				targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
			}
		}()

		local, remote := net.Pipe()
		defer remote.Close()
		sc := &recordingMigrationServerConn{mockServerConn: newMockServerConn(local)}
		defer sc.Close()

		err := cc.migrateConnTo(sc, &pb.MigrateConnFromResponse{
			TempTables: []*pb.MigrateTempTable{{
				Database: "d1", Alias: "tmp", PhysicalName: "__mo_tmp_source_d1_tmp",
			}},
		})
		assert.ErrorContains(t, err, "cannot migrate temporary tables to a pre-v38 target")
		assert.Empty(t, sc.statements)
	})
}

func TestQueryServiceMigrateToCarriesTypedUserVariables(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	handler := func(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
		migration := req.MigrateConnToRequest
		if migration == nil {
			return moerr.NewInternalError(ctx, "bad request")
		}
		assert.True(t, migration.UserDefinedVarsExported)
		assert.True(t, migration.UserDefinedVarsReplayable)
		assert.True(t, migration.SystemVariablesReplayable)
		assert.Len(t, migration.UserDefinedVars, 1)
		assert.Equal(t, "ts0", migration.UserDefinedVars[0].Name)
		assert.Equal(t, []string{"set time_zone = @ts0"}, migration.SetVarStmts)
		resp.MigrateConnToResponse = &pb.MigrateConnToResponse{Success: true}
		return nil
	}
	runTestWithQueryServiceHandler(t, cn, handler, func(cc *clientConn, _ string) {
		c1, _ := net.Pipe()
		sc := newMockServerConn(c1)
		cc.migration.setVarStmts = []string{"set @ts0 = now()"}
		cc.migration.systemSetVarStmts = []string{"set time_zone = @ts0"}
		info := &pb.MigrateConnFromResponse{
			UserDefinedVarsExported:       true,
			UserDefinedVarsReplayable:     true,
			SystemVariablesReplayable:     true,
			UserLevelLockReleaseSupported: true,
			UserDefinedVars: []*pb.MigrateUserDefinedVar{{
				Name:  "ts0",
				Value: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "stable-value"}}}},
			}},
		}
		assert.NoError(t, cc.migrateConnTo(sc, info))
	})
}

func TestQueryServiceMigrateToCarriesTypedSystemVariables(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	handler := func(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
		migration := req.MigrateConnToRequest
		if migration == nil {
			return moerr.NewInternalError(ctx, "bad request")
		}
		assert.True(t, migration.UserDefinedVarsExported)
		assert.True(t, migration.SystemVariablesExported)
		assert.Len(t, migration.SystemVariables, 1)
		assert.Equal(t, "sql_mode", migration.SystemVariables[0].Name)
		assert.Empty(t, migration.SetVarStmts)
		resp.MigrateConnToResponse = &pb.MigrateConnToResponse{Success: true}
		return nil
	}
	runTestWithQueryServiceHandler(t, cn, handler, func(cc *clientConn, _ string) {
		c1, _ := net.Pipe()
		sc := newMockServerConn(c1)
		cc.migration.systemSetVarStmts = []string{"set sql_mode = @mode"}
		info := &pb.MigrateConnFromResponse{
			UserDefinedVarsExported:       true,
			SystemVariablesExported:       true,
			UserLevelLockReleaseSupported: true,
			UserDefinedVars: []*pb.MigrateUserDefinedVar{{
				Name:  "mode",
				Value: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "PIPES_AS_CONCAT"}}}},
			}},
			SystemVariables: []*pb.MigrateSystemVariable{{
				Name:  "sql_mode",
				Value: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "ANSI_QUOTES"}}}},
			}},
		}
		assert.NoError(t, cc.migrateConnTo(sc, info))
	})
}

type recordingMigrationServerConn struct {
	*mockServerConn
	statements []string
}

func (s *recordingMigrationServerConn) ExecStmt(stmt internalStmt, resp chan<- []byte) (bool, error) {
	s.statements = append(s.statements, stmt.s)
	return s.mockServerConn.ExecStmt(stmt, resp)
}

func TestQueryServiceMigrateToFallsBackForPreV22Target(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	handler := func(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
		migration := req.MigrateConnToRequest
		if migration == nil {
			return moerr.NewInternalError(ctx, "bad request")
		}
		assert.False(t, migration.UserDefinedVarsExported)
		assert.False(t, migration.SystemVariablesExported)
		assert.Empty(t, migration.UserDefinedVars)
		assert.Empty(t, migration.SystemVariables)
		resp.MigrateConnToResponse = &pb.MigrateConnToResponse{Success: true}
		return nil
	}
	runTestWithQueryServiceHandler(t, cn, handler, func(cc *clientConn, _ string) {
		targetRuntime := runtime.ServiceRuntime(cn.ServiceID)
		oldVersion, hadVersion := targetRuntime.GetGlobalVariables(runtime.MOProtocolVersion)
		targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion20)
		defer func() {
			if hadVersion {
				targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion)
			} else {
				targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
			}
		}()

		local, remote := net.Pipe()
		defer remote.Close()
		sc := &recordingMigrationServerConn{mockServerConn: newMockServerConn(local)}
		defer sc.Close()
		cc.migration.setVarStmts = []string{"set @mode = 'PIPES_AS_CONCAT'"}
		info := &pb.MigrateConnFromResponse{
			UserDefinedVarsExported:   true,
			SystemVariablesExported:   true,
			UserDefinedVarsReplayable: true,
			SystemVariablesReplayable: true,
		}
		assert.NoError(t, cc.migrateConnTo(sc, info))
		assert.Equal(t, []string{
			"/* cloud_nonuser */ set transferred=1;",
			"set @mode = 'PIPES_AS_CONCAT'",
		}, sc.statements)
	})
}

func TestQueryServiceMigrateToAllowsOversizedSystemSnapshotForPreV22Target(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	runTestWithQueryService(t, cn, func(cc *clientConn, _ string) {
		targetRuntime := runtime.ServiceRuntime(cn.ServiceID)
		oldVersion, hadVersion := targetRuntime.GetGlobalVariables(runtime.MOProtocolVersion)
		targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion20)
		defer func() {
			if hadVersion {
				targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion)
			} else {
				targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
			}
		}()

		local, remote := net.Pipe()
		defer remote.Close()
		sc := &recordingMigrationServerConn{mockServerConn: newMockServerConn(local)}
		defer sc.Close()
		cc.migration.setVarStmts = []string{"set optimizer_hints = 'legacy'"}
		info := &pb.MigrateConnFromResponse{
			LastAffectedRows:                7,
			SystemVariablesSnapshotTooLarge: true,
			SystemVariablesReplayable:       true,
			UserLevelLockReleaseSupported:   true,
		}
		assert.NoError(t, cc.migrateConnTo(sc, info))
		assert.Equal(t, []string{
			"/* cloud_nonuser */ set transferred=1;",
			"set optimizer_hints = 'legacy'",
		}, sc.statements)
	})
}

func TestQueryServiceMigrateToRejectsOversizedSystemSnapshotForV22Target(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	runTestWithQueryService(t, cn, func(cc *clientConn, _ string) {
		local, remote := net.Pipe()
		defer remote.Close()
		sc := &recordingMigrationServerConn{mockServerConn: newMockServerConn(local)}
		defer sc.Close()
		info := &pb.MigrateConnFromResponse{
			LastAffectedRows:                7,
			SystemVariablesSnapshotTooLarge: true,
			SystemVariablesReplayable:       true,
			UserLevelLockReleaseSupported:   true,
		}
		err := cc.migrateConnTo(sc, info)
		assert.ErrorContains(t, err, "snapshot exceeds the connection migration size limit")
		assert.Empty(t, sc.statements)
	})
}

func TestQueryServiceMigrateToRejectsOversizedUserSnapshotForV22Target(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	runTestWithQueryService(t, cn, func(cc *clientConn, _ string) {
		local, remote := net.Pipe()
		defer remote.Close()
		sc := &recordingMigrationServerConn{mockServerConn: newMockServerConn(local)}
		defer sc.Close()
		info := &pb.MigrateConnFromResponse{
			LastAffectedRows:                7,
			UserDefinedVarsSnapshotTooLarge: true,
			UserDefinedVarsReplayable:       true,
			UserLevelLockReleaseSupported:   true,
		}
		err := cc.migrateConnTo(sc, info)
		assert.ErrorContains(t, err, "typed user variables because the snapshot exceeds")
		assert.Empty(t, sc.statements)
	})
}

func TestQueryServiceMigrateToRejectsUnreplayableTypedStateForPreV22Target(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	runTestWithQueryService(t, cn, func(cc *clientConn, _ string) {
		targetRuntime := runtime.ServiceRuntime(cn.ServiceID)
		oldVersion, hadVersion := targetRuntime.GetGlobalVariables(runtime.MOProtocolVersion)
		targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion20)
		defer func() {
			if hadVersion {
				targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion)
			} else {
				targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
			}
		}()

		local, remote := net.Pipe()
		defer remote.Close()
		sc := &recordingMigrationServerConn{mockServerConn: newMockServerConn(local)}
		defer sc.Close()
		info := &pb.MigrateConnFromResponse{
			UserDefinedVarsExported:       true,
			UserDefinedVarsReplayable:     false,
			UserDefinedVars:               []*pb.MigrateUserDefinedVar{{Name: "v"}},
			UserLevelLockReleaseSupported: true,
		}
		err := cc.migrateConnTo(sc, info)
		assert.ErrorContains(t, err, "complete raw replay")
		assert.Empty(t, sc.statements)
	})
}

func TestQueryServiceMigrateToRejectsUnreplayableTypedSystemStateForPreV22Target(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	runTestWithQueryService(t, cn, func(cc *clientConn, _ string) {
		targetRuntime := runtime.ServiceRuntime(cn.ServiceID)
		oldVersion, hadVersion := targetRuntime.GetGlobalVariables(runtime.MOProtocolVersion)
		targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion20)
		defer func() {
			if hadVersion {
				targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion)
			} else {
				targetRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
			}
		}()

		local, remote := net.Pipe()
		defer remote.Close()
		sc := &recordingMigrationServerConn{mockServerConn: newMockServerConn(local)}
		defer sc.Close()
		info := &pb.MigrateConnFromResponse{
			SystemVariablesExported:       true,
			SystemVariablesReplayable:     false,
			SystemVariables:               []*pb.MigrateSystemVariable{{Name: "optimizer_hints"}},
			UserLevelLockReleaseSupported: true,
		}
		err := cc.migrateConnTo(sc, info)
		assert.ErrorContains(t, err, "complete raw replay")
		assert.Empty(t, sc.statements)
	})
}

func TestQueryServiceMigrateToReplaysRawUserStateWhenTypedUserSnapshotMissing(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	handler := func(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
		migration := req.MigrateConnToRequest
		if migration == nil {
			return moerr.NewInternalError(ctx, "bad request")
		}
		assert.False(t, migration.UserDefinedVarsExported)
		assert.True(t, migration.SystemVariablesExported)
		assert.Empty(t, migration.SetVarStmts)
		resp.MigrateConnToResponse = &pb.MigrateConnToResponse{Success: true}
		return nil
	}
	runTestWithQueryServiceHandler(t, cn, handler, func(cc *clientConn, _ string) {
		local, remote := net.Pipe()
		defer remote.Close()
		sc := &recordingMigrationServerConn{mockServerConn: newMockServerConn(local)}
		defer sc.Close()
		cc.migration.setVarStmts = []string{"set @mode = 'PIPES_AS_CONCAT'"}
		info := &pb.MigrateConnFromResponse{
			SystemVariablesExported: true,
		}
		assert.NoError(t, cc.migrateConnTo(sc, info))
		assert.Equal(t, []string{
			"/* cloud_nonuser */ set transferred=1;",
			"set @mode = 'PIPES_AS_CONCAT'",
		}, sc.statements)
	})
}

func TestMigrateConnToUsesTransferDeadline(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	transferDeadline := make(chan time.Time, 1)
	handler := func(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
		if req.MigrateConnToRequest == nil {
			return moerr.NewInternalError(ctx, "bad request")
		}
		expectedDeadline := <-transferDeadline
		actualDeadline, ok := ctx.Deadline()
		if !ok {
			return moerr.NewInternalError(ctx, "missing migration deadline")
		}
		if actualDeadline.Before(expectedDeadline.Add(-time.Millisecond)) {
			return moerr.NewInternalErrorf(ctx,
				"migration deadline %s is earlier than transfer deadline %s",
				actualDeadline, expectedDeadline)
		}
		resp.MigrateConnToResponse = &pb.MigrateConnToResponse{Success: true}
		return nil
	}
	runTestWithQueryServiceHandler(t, cn, handler, func(cc *clientConn, _ string) {
		local, remote := net.Pipe()
		defer remote.Close()
		sc := newMockServerConn(local)
		defer sc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), defaultTransferTimeout)
		defer cancel()
		deadline, ok := ctx.Deadline()
		assert.True(t, ok)
		transferDeadline <- deadline
		err := cc.migrateConnToContext(ctx, sc, &pb.MigrateConnFromResponse{})
		assert.NoError(t, err)
	})
}

func TestMigrateConnToPropagatesCancellation(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	handlerEntered := make(chan struct{})
	handlerRelease := make(chan struct{})
	handlerDone := make(chan struct{})
	handler := func(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
		defer close(handlerDone)
		if req.MigrateConnToRequest == nil {
			return moerr.NewInternalError(ctx, "bad request")
		}
		close(handlerEntered)
		<-handlerRelease
		resp.MigrateConnToResponse = &pb.MigrateConnToResponse{Success: true}
		return nil
	}
	runTestWithQueryServiceHandler(t, cn, handler, func(cc *clientConn, _ string) {
		local, remote := net.Pipe()
		defer remote.Close()
		sc := newMockServerConn(local)
		defer sc.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := make(chan error, 1)
		go func() {
			result <- cc.migrateConnToContext(ctx, sc, &pb.MigrateConnFromResponse{})
		}()

		handlerReleased := false
		defer func() {
			if !handlerReleased {
				close(handlerRelease)
			}
		}()
		select {
		case <-handlerEntered:
		case <-time.After(time.Second):
			t.Fatal("MigrateConnTo handler was not called")
		}

		cancel()
		select {
		case err := <-result:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("MigrateConnTo ignored transfer cancellation")
		}

		close(handlerRelease)
		handlerReleased = true
		select {
		case <-handlerDone:
		case <-time.After(time.Second):
			t.Fatal("MigrateConnTo handler did not finish")
		}
	})
}

func TestMigrateConnToContextCancelsReplay(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	blocked := newBlockingContextServerConn(local)
	defer blocked.Close()
	cc := &clientConn{}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- cc.migrateConnToContext(ctx, blocked, &pb.MigrateConnFromResponse{})
	}()

	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("migration replay did not enter backend ExecStmt")
	}
	cancel()
	select {
	case err := <-result:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("migration replay ignored transfer cancellation")
	}
}

type migrationUserLockQueryClient struct {
	userLevelLocks                []*pb.UserLevelLock
	userLevelLockReleaseSupported bool
	prepareStmts                  []*pb.PrepareStmt
	migrateToPrepareStmts         []*pb.PrepareStmt
	migrateFromErr                error
	preparedStmtLongDataChecked   bool
	omitTempTableState            bool
	releaseCount                  int
}

func (c *migrationUserLockQueryClient) ServiceID() string {
	return "s1"
}

func (c *migrationUserLockQueryClient) SendMessage(ctx context.Context, address string, req *pb.Request) (*pb.Response, error) {
	switch req.CmdMethod {
	case pb.CmdMethod_MigrateConnFrom:
		if c.migrateFromErr != nil {
			return nil, c.migrateFromErr
		}
		return &pb.Response{MigrateConnFromResponse: &pb.MigrateConnFromResponse{
			DB:                            "d1",
			PrepareStmts:                  append([]*pb.PrepareStmt(nil), c.prepareStmts...),
			UserLevelLocks:                c.userLevelLocks,
			UserLevelLockReleaseSupported: c.userLevelLockReleaseSupported,
			PreparedStmtLongDataChecked:   c.preparedStmtLongDataChecked,
			TempTableStateExported:        !c.omitTempTableState,
		}}, nil
	case pb.CmdMethod_MigrateConnTo:
		c.migrateToPrepareStmts = append(
			[]*pb.PrepareStmt(nil), req.MigrateConnToRequest.PrepareStmts...)
		return &pb.Response{MigrateConnToResponse: &pb.MigrateConnToResponse{Success: true}}, nil
	default:
		return nil, moerr.NewInternalError(ctx, "unexpected request")
	}
}

func TestMigrateConnFromReblocksLongDataRejectedByOldCN(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe", QueryAddress: "query"}
	cluster := clusterservice.NewMOCluster(
		"",
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{cn}, nil))
	defer cluster.Close()

	for _, test := range []struct {
		name             string
		queryClient      *migrationUserLockQueryClient
		wantReleaseCount int
	}{
		{
			name: "current CN reports staged data",
			queryClient: &migrationUserLockQueryClient{
				migrateFromErr: moerr.GetOkExpectedNotSafeToStartTransfer(),
			},
		},
		{
			name:             "older CN lacks authoritative check",
			queryClient:      &migrationUserLockQueryClient{},
			wantReleaseCount: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tun := &tunnel{}
			tun.trackClientRequest(makeStmtCommandPacket(
				frontend.COM_STMT_SEND_LONG_DATA, 41, 0, 0, 'x'))
			tun.trackClientRequest(makeSimplePacket("select 1"))
			tun.trackServerResponse(makeOKPacket(8))
			require.False(t, tun.hasUntransferableClientState())

			cc, closeFn := createNewClientConn(t)
			defer closeFn()
			ccc := cc.(*clientConn)
			ccc.tun = tun
			ccc.queryClient = test.queryClient
			ccc.moCluster = cluster

			_, err := ccc.migrateConnFromContext(context.Background(), "pipe")
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer))
			require.True(t, tun.hasUntransferableClientState(),
				"the old CN did not prove staged data absent, so another response fence is required")
			require.Equal(t, test.wantReleaseCount, test.queryClient.releaseCount)
		})
	}
}

func TestMigrateConnFromAcceptsAuthoritativelyReconciledLongData(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe", QueryAddress: "query"}
	cluster := clusterservice.NewMOCluster(
		"",
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{cn}, nil))
	defer cluster.Close()

	tun := &tunnel{}
	tun.trackClientRequest(makeStmtCommandPacket(
		frontend.COM_STMT_SEND_LONG_DATA, 41, 0, 0, 'x'))
	tun.trackClientRequest(makeSimplePacket("deallocate prepare __mo_stmt_id_41"))
	tun.trackServerResponse(makeOKPacket(8))

	cc, closeFn := createNewClientConn(t)
	defer closeFn()
	ccc := cc.(*clientConn)
	ccc.tun = tun
	ccc.queryClient = &migrationUserLockQueryClient{
		preparedStmtLongDataChecked: true,
	}
	ccc.moCluster = cluster

	_, err := ccc.migrateConnFromContext(context.Background(), "pipe")
	require.NoError(t, err)
	require.Equal(t, 1, ccc.queryClient.(*migrationUserLockQueryClient).releaseCount)
	require.True(t, tun.hasUnsafeClientState(),
		"staged-data bookkeeping remains non-cacheable until migration commits")
	tun.clearMigratedStatementState()
	require.False(t, tun.hasUnsafeClientState())
}

func (c *migrationUserLockQueryClient) NewRequest(method pb.CmdMethod) *pb.Request {
	return &pb.Request{CmdMethod: method}
}

func (c *migrationUserLockQueryClient) Release(response *pb.Response) {
	c.releaseCount++
}

func (c *migrationUserLockQueryClient) Close() error {
	return nil
}

func TestMigrateConnFromFiltersCloseThatBackendHasNotDispatched(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe", QueryAddress: "query"}
	cluster := clusterservice.NewMOCluster(
		"",
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{cn}, nil))
	defer cluster.Close()

	const closedID uint32 = 41
	const liveID uint32 = 42
	tun := &tunnel{}
	commit := tun.trackClientRequest(
		makeStmtCommandPacket(frontend.COM_STMT_CLOSE, closedID))
	tun.commitClientRequest(commit)

	cc, closeFn := createNewClientConn(t)
	defer closeFn()
	ccc := cc.(*clientConn)
	ccc.tun = tun
	queryClient := &migrationUserLockQueryClient{
		userLevelLockReleaseSupported: true,
		prepareStmts: []*pb.PrepareStmt{
			{Name: frontend.GetPrepareStmtName(closedID), SQL: "select 41"},
			{Name: frontend.GetPrepareStmtName(liveID), SQL: "select 42"},
		},
	}
	ccc.queryClient = queryClient
	ccc.moCluster = cluster

	// Model the ordering where MigrateConnFrom exports the old session before
	// the backend dispatches the already-forwarded COM_STMT_CLOSE.
	resp, err := ccc.migrateConnFromContext(context.Background(), "pipe")
	assert.NoError(t, err)
	if assert.Len(t, resp.PrepareStmts, 1) {
		assert.Equal(t, frontend.GetPrepareStmtName(liveID), resp.PrepareStmts[0].Name)
	}
	assert.True(t, tun.hasUnsafeClientState(),
		"the old backend must remain non-cacheable until migration completes")

	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()
	assert.NoError(t, ccc.migrateConnContext(
		context.Background(), "pipe", newMockServerConn(local)))
	if assert.Len(t, queryClient.migrateToPrepareStmts, 1) {
		assert.Equal(t, frontend.GetPrepareStmtName(liveID),
			queryClient.migrateToPrepareStmts[0].Name)
	}
	assert.False(t, tun.hasUnsafeClientState(),
		"successful migration carried the CLOSE into the new backend generation")
}

func TestMigrateConnContextRejectsUserLevelLocks(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe", QueryAddress: "query"}
	cluster := clusterservice.NewMOCluster(
		"",
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{cn}, nil))
	defer cluster.Close()

	cc, closeFn := createNewClientConn(t)
	defer closeFn()
	ccc := cc.(*clientConn)
	ccc.queryClient = &migrationUserLockQueryClient{
		userLevelLocks:                []*pb.UserLevelLock{{Name: "migration_lock", Count: 1}},
		userLevelLockReleaseSupported: true,
	}
	ccc.moCluster = cluster

	err := ccc.migrateConnContext(context.Background(), "pipe", nil)
	assert.ErrorContains(t, err, "cannot migrate connection while user-level locks are held")
}

func TestMigrateConnContextRejectsOldUserLevelLockProtocol(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe", QueryAddress: "query"}
	cluster := clusterservice.NewMOCluster(
		"",
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{cn}, nil))
	defer cluster.Close()

	cc, closeFn := createNewClientConn(t)
	defer closeFn()
	ccc := cc.(*clientConn)
	ccc.queryClient = &migrationUserLockQueryClient{}
	ccc.moCluster = cluster

	err := ccc.migrateConnContext(context.Background(), "pipe", nil)
	assert.ErrorContains(t, err, "cannot migrate connection from CN without user-level lock release support")
}
