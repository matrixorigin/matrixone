// Copyright 2023 Matrix Origin
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
	"fmt"
	"sync"
	"testing"
	"time"

	uuid2 "github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/mohae/deepcopy"
	"github.com/stretchr/testify/require"
)

func TestCanHandleServiceAndCmdWrong(t *testing.T) {
	var a1, a2 struct {
		proc      *process.Process
		service   serviceType
		parameter string
		sender    requestSender
	}

	// testing query with wrong serviceType
	a1.service = serviceType("log")
	ret, err := handleTraceSpan(a1.proc, a1.service, a1.parameter, a1.sender)
	require.Equal(t, ret, Result{})
	require.Equal(t, err, moerr.NewWrongServiceNoCtx("CN or DN", string(a1.service)))

	// testing query with wrong cmd
	a2.service = cn
	a2.parameter = "xxx:open:s3:0"
	ret, err = handleTraceSpan(a2.proc, a2.service, a2.parameter, a2.sender)
	require.Equal(t, ret, Result{})
	require.Equal(t, err, moerr.NewInternalErrorNoCtx("cmd invalid, expected enable or disable"))
}

func initRuntime(uuids []string, queryAddress []string) {
	cns := make([]metadata.CNService, len(uuids))
	for idx := range uuids {
		cns[idx] = metadata.CNService{
			ServiceID:    uuids[idx],
			QueryAddress: queryAddress[idx],
		}
	}

	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	moCluster := clusterservice.NewMOCluster(new(testHAKeeperClient),
		time.Duration(time.Second),
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices(cns, nil))
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, moCluster)
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
}

func TestCanHandleSelfCmd(t *testing.T) {
	var a1 struct {
		proc      *process.Process
		service   serviceType
		parameter string
		sender    requestSender
	}

	trace.InitMOCtledSpan()

	initRuntime(nil, nil)

	uuid := uuid2.New().String()
	cli, err := qclient.NewQueryClient(uuid, morpc.Config{})
	require.Nil(t, err)

	a1.proc = new(process.Process)
	a1.proc.QueryClient = cli
	a1.service = cn
	a1.parameter = fmt.Sprintf("%s:enable:s3,local:10", uuid)

	ret, err := handleTraceSpan(a1.proc, a1.service, a1.parameter, a1.sender)
	require.Nil(t, err)
	require.Equal(t, ret, Result{
		Method: TraceSpanMethod,
		Data:   fmt.Sprintf("%s:[s3 local] enabled, [] failed; ", uuid),
	})

	k1 := trace.MOCtledSpanEnableConfig.NameToKind["s3"]
	k2 := trace.MOCtledSpanEnableConfig.NameToKind["local"]
	require.Equal(t, true, trace.MOCtledSpanEnableConfig.KindToState[k1].Enable)
	require.Equal(t, int64(10), trace.MOCtledSpanEnableConfig.KindToState[k1].Threshold.Milliseconds())
	require.Equal(t, true, trace.MOCtledSpanEnableConfig.KindToState[k2].Enable)
	require.Equal(t, int64(10), trace.MOCtledSpanEnableConfig.KindToState[k2].Threshold.Milliseconds())

}

func TestCanTransferQuery(t *testing.T) {
	var a1 struct {
		proc      *process.Process
		service   serviceType
		parameter string
		sender    requestSender
	}
	uuids := []string{
		uuid2.New().String(),
		uuid2.New().String(),
	}
	addrs := []string{
		"127.0.0.1:7777",
		"127.0.0.1:5555",
	}

	a1.proc = new(process.Process)
	a1.service = cn
	a1.parameter = fmt.Sprintf("%s,%s:enable:s3,local:0", uuids[0], uuids[1])

	initRuntime(uuids, addrs)
	trace.InitMOCtledSpan()

	qs1, err := queryservice.NewQueryService(uuids[0], addrs[0], morpc.Config{})
	require.Nil(t, err)
	qs2, err := queryservice.NewQueryService(uuids[1], addrs[1], morpc.Config{})
	require.Nil(t, err)
	qt1, err := qclient.NewQueryClient(uuids[1], morpc.Config{})
	require.Nil(t, err)

	qs1.AddHandleFunc(query.CmdMethod_TraceSpan, mockHandleTraceSpan, false)
	qs2.AddHandleFunc(query.CmdMethod_TraceSpan, mockHandleTraceSpan, false)

	a1.proc.QueryClient = qt1

	err = qs1.Start()
	require.Nil(t, err)
	err = qs2.Start()
	require.Nil(t, err)

	defer func() {
		qs1.Close()
		qs2.Close()
	}()

	ret, err := handleTraceSpan(a1.proc, a1.service, a1.parameter, a1.sender)
	require.Nil(t, err)

	str1 := fmt.Sprintf("%s:[s3 local] enabled, [] failed; ", uuids[0])
	str2 := fmt.Sprintf("%s:[s3 local] enabled, [] failed; ", uuids[1])

	require.True(t, func() bool {
		if ret.Data == str1+str2 ||
			ret.Data == str2+str1 {
			return true
		}
		return false
	}())
}

type testHAKeeperClient struct {
	sync.RWMutex
	value logpb.ClusterDetails
	err   error
}

func (c *testHAKeeperClient) GetClusterDetails(ctx context.Context) (logpb.ClusterDetails, error) {
	c.RLock()
	defer c.RUnlock()
	// deep copy the cluster details to avoid data race.
	copied := deepcopy.Copy(c.value)
	return copied.(logpb.ClusterDetails), c.err
}

func mockHandleTraceSpan(ctx context.Context, req *query.Request, resp *query.Response) error {
	resp.TraceSpanResponse = new(query.TraceSpanResponse)
	resp.TraceSpanResponse.Resp = SelfProcess(
		req.TraceSpanRequest.Cmd, req.TraceSpanRequest.Spans, req.TraceSpanRequest.Threshold)
	return nil
}
