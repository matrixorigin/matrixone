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

package tnservice

import (
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/util"
)

var _ logservice.TNHAKeeperClient = new(testHAClient)

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
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) CheckLogServiceHealth(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) SendTNHeartbeat(ctx context.Context, hb pb.TNStoreHeartbeat) (pb.CommandBatch, error) {
	return pb.CommandBatch{}, context.DeadlineExceeded
}

func Test_heartbeat(t *testing.T) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 0, moerr.NewInternalErrorNoCtx("ut tester"))
	defer cancel()

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)

	cfg := &Config{}

	lstore := &store{
		cfg:            cfg,
		replicas:       &sync.Map{},
		config:         &util.ConfigData{},
		hakeeperClient: &testHAClient{},
		rt:             rt,
	}
	lstore.heartbeat(ctx)
}
