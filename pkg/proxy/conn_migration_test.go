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

func runTestWithQueryService(t *testing.T, cn metadata.CNService, fn func(qc qclient.QueryClient, addr string)) {
	defer leaktest.AfterTest(t)()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
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

	qs, err := queryservice.NewQueryService(cn.ServiceID, address, morpc.Config{})
	assert.NoError(t, err)

	qt, err := qclient.NewQueryClient(cn.ServiceID, morpc.Config{})
	assert.NoError(t, err)

	qs.AddHandleFunc(pb.CmdMethod_MigrateConnFrom, func(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
		if req.MigrateConnFromRequest == nil {
			return moerr.NewInternalError(ctx, "bad request")
		}
		resp.MigrateConnFromResponse = &pb.MigrateConnFromResponse{
			DB: "d1",
		}
		return nil
	}, false)
	qs.AddHandleFunc(pb.CmdMethod_MigrateConnTo, func(ctx context.Context, req *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
		if req.MigrateConnToRequest == nil {
			return moerr.NewInternalError(ctx, "bad request")
		}
		resp.MigrateConnToResponse = &pb.MigrateConnToResponse{
			Success: true,
		}
		return nil
	}, false)
	err = qs.Start()
	assert.NoError(t, err)

	fn(qt, address)

	err = qs.Close()
	assert.NoError(t, err)
	err = qt.Close()
	assert.NoError(t, err)
}

func TestQueryServiceMigrateConn(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1"}
	runTestWithQueryService(t, cn, func(qc qclient.QueryClient, addr string) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		req := qc.NewRequest(pb.CmdMethod_MigrateConnFrom)
		req.MigrateConnFromRequest = &pb.MigrateConnFromRequest{
			ConnID: 1,
		}
		resp, err := qc.SendMessage(ctx, addr, req)
		assert.NoError(t, err)
		defer qc.Release(resp)
		assert.NotNil(t, resp.MigrateConnFromResponse)
		assert.Equal(t, "d1", resp.MigrateConnFromResponse.DB)
	})
}

func TestQueryServiceMigrateFrom(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1"}
	runTestWithQueryService(t, cn, func(qc qclient.QueryClient, addr string) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		req := qc.NewRequest(pb.CmdMethod_MigrateConnTo)
		req.MigrateConnToRequest = &pb.MigrateConnToRequest{
			ConnID: 1,
		}
		resp, err := qc.SendMessage(ctx, addr, req)
		assert.NoError(t, err)
		defer qc.Release(resp)
		assert.NotNil(t, resp.MigrateConnToResponse)
		assert.Equal(t, true, resp.MigrateConnToResponse.Success)
	})
}
