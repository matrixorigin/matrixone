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
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

var _ logservice.CNHAKeeperClient = new(testHAClient)

type testHAClient struct {
}

func (client *testHAClient) Close() error {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) AllocateID(ctx context.Context) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) AllocateIDByKeyWithBatch(ctx context.Context, key string, batch uint64) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) GetClusterState(ctx context.Context) (pb.CheckerState, error) {
	cs := pb.CheckerState{}

	return cs, nil
}

func (client *testHAClient) CheckLogServiceHealth(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) GetBackupData(ctx context.Context) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) SendCNHeartbeat(ctx context.Context, hb pb.CNStoreHeartbeat) (pb.CommandBatch, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) UpdateNonVotingReplicaNum(ctx context.Context, num uint64) error {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) UpdateNonVotingLocality(ctx context.Context, locality pb.Locality) error {
	//TODO implement me
	panic("implement me")
}

func Test_handleTask(t *testing.T) {

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
	proc.Base.Hakeeper = &testHAClient{}
	_, err := handleTask(proc, "", getUser, nil)
	assert.NoError(t, err)
}
