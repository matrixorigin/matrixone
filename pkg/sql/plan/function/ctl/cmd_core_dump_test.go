// Copyright 2024 Matrix Origin
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

package ctl

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

var _ qclient.QueryClient = new(testQClient)

type testQClient struct {
}

func (tQCli *testQClient) ServiceID() string {
	return ""
}

func (tQCli *testQClient) SendMessage(ctx context.Context, address string, req *query.Request) (*query.Response, error) {
	return nil, moerr.NewInternalErrorNoCtx("send error")
}

func (tQCli *testQClient) NewRequest(method query.CmdMethod) *query.Request {
	return &query.Request{}
}

func (tQCli *testQClient) Release(response *query.Response) {
	//TODO implement me
	panic("implement me")
}

func (tQCli *testQClient) Close() error {
	//TODO implement me
	panic("implement me")
}

func Test_handleCoreDump(t *testing.T) {

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	mc := clusterservice.NewMOCluster(
		"",
		nil,
		3*time.Second,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices(
			[]metadata.CNService{
				{
					ServiceID: "mock",
				},
			}, nil,
		),
	)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)

	proc := testutil.NewProc(t)
	proc.Base.QueryClient = &testQClient{}
	_, err := handleCoreDump(proc, cn, "do", nil)
	assert.Error(t, err)
}
