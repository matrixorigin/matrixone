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
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/stretchr/testify/assert"
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
				resp.MigrateConnFromResponse = &pb.MigrateConnFromResponse{
					DB:                            "d1",
					LastAffectedRows:              7,
					UserLevelLockReleaseSupported: true,
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
			qs.AddHandleFunc(pb.CmdMethod_ResetSession, func(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
				if req.ResetSessionRequest == nil {
					return moerr.NewInternalError(ctx, "bad request")
				}
				resp.ResetSessionResponse = &pb.ResetSessionResponse{
					AuthString: nil,
					Success:    true,
				}
				return nil
			}, false)
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
	})
}

func TestQueryServiceMigrateTo(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	runTestWithQueryService(t, cn, func(cc *clientConn, addr string) {
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

func TestQueryServiceMigrateToCarriesTypedUserVariables(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	handler := func(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
		migration := req.MigrateConnToRequest
		if migration == nil {
			return moerr.NewInternalError(ctx, "bad request")
		}
		assert.True(t, migration.UserDefinedVarsExported)
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
			UserLevelLockReleaseSupported: true,
			UserDefinedVars: []*pb.MigrateUserDefinedVar{{
				Name:  "ts0",
				Value: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "stable-value"}}}},
			}},
		}
		assert.NoError(t, cc.migrateConnTo(sc, info))
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
}

func (c *migrationUserLockQueryClient) ServiceID() string {
	return "s1"
}

func (c *migrationUserLockQueryClient) SendMessage(ctx context.Context, address string, req *pb.Request) (*pb.Response, error) {
	switch req.CmdMethod {
	case pb.CmdMethod_MigrateConnFrom:
		return &pb.Response{MigrateConnFromResponse: &pb.MigrateConnFromResponse{
			DB:                            "d1",
			UserLevelLocks:                c.userLevelLocks,
			UserLevelLockReleaseSupported: c.userLevelLockReleaseSupported,
		}}, nil
	default:
		return nil, moerr.NewInternalError(ctx, "unexpected request")
	}
}

func (c *migrationUserLockQueryClient) NewRequest(method pb.CmdMethod) *pb.Request {
	return &pb.Request{CmdMethod: method}
}

func (c *migrationUserLockQueryClient) Release(response *pb.Response) {}

func (c *migrationUserLockQueryClient) Close() error {
	return nil
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
