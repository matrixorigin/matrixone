// Copyright 2021 - 2023 Matrix Origin
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

package queryservice

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/stretchr/testify/assert"
)

func testCreateQueryService(t *testing.T) QueryService {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	cluster := clusterservice.NewMOCluster(
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{{}}, nil))
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)
	address := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
	err := os.RemoveAll(address[7:])
	assert.NoError(t, err)
	qs, err := NewQueryService("s1", address, morpc.Config{}, nil)
	assert.NoError(t, err)
	return qs
}

func TestNewQueryService(t *testing.T) {
	qs := testCreateQueryService(t)
	assert.NotNil(t, qs)
}

func TestUnwrapResponseError(t *testing.T) {
	qs := testCreateQueryService(t)
	assert.NotNil(t, qs)
	svc, ok := qs.(*queryService)
	assert.True(t, ok)
	resp1 := &pb.Response{Error: nil}
	resp2, err := svc.unwrapResponseError(resp1)
	assert.Nil(t, err)
	assert.Equal(t, resp2, resp1)

	e := moerr.NewInternalErrorNoCtx("test")
	moe, err := e.MarshalBinary()
	assert.NoError(t, err)
	resp1 = &pb.Response{Error: moe}
	resp2, err = svc.unwrapResponseError(resp1)
	assert.Equal(t, "internal error: test", err.Error())
	assert.Nil(t, resp2)
}

func TestQueryService(t *testing.T) {
	cn := metadata.CNService{
		ServiceID: "s1",
	}

	t.Run("sys tenant", func(t *testing.T) {
		runTestWithQueryService(t, cn, func(svc QueryService, addr string, sm *SessionManager) {
			sm.AddSession(&mockSession{id: "s1", tenant: "t1"})
			sm.AddSession(&mockSession{id: "s2", tenant: "t2"})
			sm.AddSession(&mockSession{id: "s3", tenant: "t3"})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := svc.NewRequest(pb.CmdMethod_ShowProcessList)
			req.ShowProcessListRequest = &pb.ShowProcessListRequest{
				Tenant:    "sys",
				SysTenant: true,
			}
			resp, err := svc.SendMessage(ctx, addr, req)
			assert.NoError(t, err)
			defer svc.Release(resp)
			assert.NotNil(t, resp.ShowProcessListResponse)
			assert.Equal(t, 3, len(resp.ShowProcessListResponse.Sessions))
		})
	})

	t.Run("common tenant", func(t *testing.T) {
		runTestWithQueryService(t, cn, func(svc QueryService, addr string, sm *SessionManager) {
			sm.AddSession(&mockSession{id: "s1", tenant: "t1"})
			sm.AddSession(&mockSession{id: "s2", tenant: "t2"})
			sm.AddSession(&mockSession{id: "s3", tenant: "t3"})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := svc.NewRequest(pb.CmdMethod_ShowProcessList)
			req.ShowProcessListRequest = &pb.ShowProcessListRequest{
				Tenant:    "t1",
				SysTenant: false,
			}
			resp, err := svc.SendMessage(ctx, addr, req)
			assert.NoError(t, err)
			defer svc.Release(resp)
			assert.NotNil(t, resp.ShowProcessListResponse)
			assert.Equal(t, 1, len(resp.ShowProcessListResponse.Sessions))
		})
	})

	t.Run("bad request", func(t *testing.T) {
		runTestWithQueryService(t, cn, func(svc QueryService, addr string, sm *SessionManager) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := svc.NewRequest(pb.CmdMethod_ShowProcessList)
			_, err := svc.SendMessage(ctx, addr, req)
			assert.Error(t, err)
			assert.Equal(t, "internal error: bad request", err.Error())
		})
	})

	t.Run("unsupported cmd", func(t *testing.T) {
		runTestWithQueryService(t, cn, func(svc QueryService, addr string, sm *SessionManager) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := svc.NewRequest(pb.CmdMethod(10))
			_, err := svc.SendMessage(ctx, addr, req)
			assert.Error(t, err)
			assert.Equal(t, "not supported: 10 not support in current service", err.Error())
		})
	})
}

func runTestWithQueryService(t *testing.T, cn metadata.CNService,
	fn func(svc QueryService, addr string, sm *SessionManager)) {
	defer leaktest.AfterTest(t)()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	address := fmt.Sprintf("unix:///tmp/cn-%d-%s.sock",
		time.Now().Nanosecond(), cn.ServiceID)

	if err := os.RemoveAll(address[7:]); err != nil {
		panic(err)
	}
	cluster := clusterservice.NewMOCluster(
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{{
			ServiceID:    cn.ServiceID,
			QueryAddress: address,
		}}, nil))
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)

	sm := NewSessionManager()
	qs, err := NewQueryService(cn.ServiceID, address, morpc.Config{}, sm)
	assert.NoError(t, err)
	err = qs.Start()
	assert.NoError(t, err)

	fn(qs, address, sm)

	err = qs.Close()
	assert.NoError(t, err)
}
