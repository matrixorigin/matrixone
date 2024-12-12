// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fj

import (
	"context"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/mohae/deepcopy"
)

type serviceType string

const (
	tn  serviceType = "DN"
	cn  serviceType = "CN"
	all serviceType = "ALL"
)

func unmarshalResp(command string, payload []byte, res *PodResponse) {
	switch command {
	case fault.ListFault:
		if err := jsoniter.Unmarshal(payload, &res.ReturnList); err != nil {
			res.ErrorStr = string(payload)
		}
	default:
		str := string(payload)
		if strings.Contains(str, "error") {
			res.ErrorStr = str
		} else {
			res.ReturnStr = string(payload)
		}
	}
}

func mockHandleFaultInject(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	resp.FaultInjectResponse = new(query.FaultInjectResponse)
	resp.FaultInjectResponse.Resp = fault.HandleFaultInject(
		ctx, req.FaultInjectRequest.Method, req.FaultInjectRequest.Parameters,
	)
	return nil
}

func initRuntime(uuids []string, queryAddress []string) {
	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			cns := make([]metadata.CNService, len(uuids))
			for idx := range uuids {
				cns[idx] = metadata.CNService{
					ServiceID:    uuids[idx],
					QueryAddress: queryAddress[idx],
				}
				runtime.SetupServiceBasedRuntime(uuids[idx], rt)
			}

			moCluster := clusterservice.NewMOCluster(
				"",
				new(testHAKeeperClient),
				time.Duration(time.Second),
				clusterservice.WithDisableRefresh(),
				clusterservice.WithServices(cns, nil))
			rt.SetGlobalVariables(runtime.ClusterService, moCluster)
			rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
		},
	)

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
