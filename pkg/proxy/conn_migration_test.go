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
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/stretchr/testify/assert"
)

func runTestWithQueryService(t *testing.T, cn metadata.CNService, fn func(cc *clientConn, addr string)) {
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
					DB:               "d1",
					LastAffectedRows: 7,
				}
				return nil
			}, false)
			qs.AddHandleFunc(pb.CmdMethod_MigrateConnTo, func(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
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
			}, false)
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
	userLevelLocks []*pb.UserLevelLock
}

func (c *migrationUserLockQueryClient) ServiceID() string {
	return "s1"
}

func (c *migrationUserLockQueryClient) SendMessage(ctx context.Context, address string, req *pb.Request) (*pb.Response, error) {
	switch req.CmdMethod {
	case pb.CmdMethod_MigrateConnFrom:
		return &pb.Response{MigrateConnFromResponse: &pb.MigrateConnFromResponse{
			DB:             "d1",
			UserLevelLocks: c.userLevelLocks,
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
		userLevelLocks: []*pb.UserLevelLock{{Name: "migration_lock", Count: 1}},
	}
	ccc.moCluster = cluster

	err := ccc.migrateConnContext(context.Background(), "pipe", nil)
	assert.ErrorContains(t, err, "cannot migrate connection while user-level locks are held")
}
