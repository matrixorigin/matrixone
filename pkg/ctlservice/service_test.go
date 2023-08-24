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

package ctlservice

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCtlRequest(t *testing.T) {
	runCtlServiceTest(
		t,
		[]serviceMeta{
			{serviceType: metadata.ServiceType_CN, serviceID: "s1"},
			{serviceType: metadata.ServiceType_DN, serviceID: "s2"},
		},
		func(services []*service) {
			s1 := services[0]
			s2 := services[1]
			s1.AddHandleFunc(ctl.CmdMethod_GetCommit,
				func(ctx context.Context,
					req *ctl.Request,
					resp *ctl.Response) error {
					resp.GetCommit.CurrentCommitTS = timestamp.Timestamp{PhysicalTime: 1}
					return nil
				},
				false)
			s2.AddHandleFunc(ctl.CmdMethod_GetCommit,
				func(ctx context.Context,
					req *ctl.Request,
					resp *ctl.Response) error {
					resp.GetCommit.CurrentCommitTS = timestamp.Timestamp{PhysicalTime: 2}
					return nil
				},
				false)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			resp, err := s1.SendCtlMessage(
				ctx,
				metadata.ServiceType_CN,
				"s1",
				s1.NewRequest(ctl.CmdMethod_GetCommit))
			require.NoError(t, err)
			assert.Equal(t, timestamp.Timestamp{PhysicalTime: 1}, resp.GetCommit.CurrentCommitTS)
			s1.Release(resp)

			resp, err = s2.SendCtlMessage(
				ctx,
				metadata.ServiceType_CN,
				"s1",
				s2.NewRequest(ctl.CmdMethod_GetCommit))
			require.NoError(t, err)
			assert.Equal(t, timestamp.Timestamp{PhysicalTime: 1}, resp.GetCommit.CurrentCommitTS)
			s2.Release(resp)

			resp, err = s1.SendCtlMessage(
				ctx,
				metadata.ServiceType_DN,
				"s2",
				s1.NewRequest(ctl.CmdMethod_GetCommit))
			require.NoError(t, err)
			assert.Equal(t, timestamp.Timestamp{PhysicalTime: 2}, resp.GetCommit.CurrentCommitTS)
			s1.Release(resp)

			_, err = s1.SendCtlMessage(
				ctx,
				metadata.ServiceType_DN,
				"s3",
				s1.NewRequest(ctl.CmdMethod_GetCommit))
			require.Error(t, err)
		},
	)
}

func runCtlServiceTest(
	t *testing.T,
	serviceMetadatas []serviceMeta,
	fn func([]*service),
) {
	defer leaktest.AfterTest(t)()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())

	cns := make([]metadata.CNService, 0, len(serviceMetadatas))
	dns := make([]metadata.DNService, 0, len(serviceMetadatas))
	addresses := make([]string, 0, len(serviceMetadatas))
	services := make([]*service, 0, len(serviceMetadatas))
	for _, v := range serviceMetadatas {
		address := fmt.Sprintf("unix:///tmp/service-%d-%s.sock",
			time.Now().Nanosecond(), v.serviceID)
		if err := os.RemoveAll(address[7:]); err != nil {
			panic(err)
		}
		if v.serviceType == metadata.ServiceType_CN {
			cns = append(cns, metadata.CNService{
				ServiceID:  v.serviceID,
				CtlAddress: address,
				WorkState:  metadata.WorkState_Working,
			})
		} else if v.serviceType == metadata.ServiceType_DN {
			dns = append(dns, metadata.DNService{
				ServiceID:  v.serviceID,
				CtlAddress: address,
			})
		}
		addresses = append(addresses, address)
	}
	cluster := clusterservice.NewMOCluster(
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices(cns, dns))
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)

	for idx, address := range addresses {
		s, err := NewCtlService(serviceMetadatas[idx].serviceID, address, morpc.Config{})
		require.NoError(t, err)
		if err := s.Start(); err != nil {
			panic(err)
		}
		services = append(services, s.(*service))
	}

	fn(services)

	for _, s := range services {
		if err := s.Close(); err != nil {
			panic(err)
		}
	}
}

type serviceMeta struct {
	serviceType metadata.ServiceType
	serviceID   string
}
