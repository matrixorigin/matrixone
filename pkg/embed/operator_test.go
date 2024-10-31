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

package embed

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

var _ logservice.CNHAKeeperClient = new(testHAKClient)

type testHAKClient struct {
	cfg *cnservice.Config
	mod int
	cnt int
}

func (client *testHAKClient) Close() error {
	//TODO implement me
	panic("implement me")
}

func (client *testHAKClient) AllocateID(ctx context.Context) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAKClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAKClient) AllocateIDByKeyWithBatch(ctx context.Context, key string, batch uint64) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAKClient) GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	cd := pb.ClusterDetails{
		TNStores: []pb.TNStore{
			{
				Shards: []pb.TNShardInfo{
					{},
				},
			},
		},
	}
	if client.mod == 2 {
		client.cnt++
		if client.cnt == 1 {
			return cd, nil
		} else if client.cnt == 2 {
			return cd, context.DeadlineExceeded
		} else if client.cnt == 3 {
			return cd, moerr.NewInternalErrorNoCtx("return_err")
		} else {
			return cd, nil
		}
	}

	return cd, nil
}

func (client *testHAKClient) GetClusterState(ctx context.Context) (pb.CheckerState, error) {
	cs := pb.CheckerState{
		CNState: pb.CNState{
			Stores: make(map[string]pb.CNStoreInfo),
		},
		State: pb.HAKeeperRunning,
	}
	if client.mod == 1 {
		client.cnt++
		if client.cnt == 1 {
			return cs, nil
		} else if client.cnt == 2 {
			return cs, context.DeadlineExceeded
		} else {
			return cs, moerr.NewInternalErrorNoCtx("return_err")
		}
	}

	return cs, nil
}

func (client *testHAKClient) CheckLogServiceHealth(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (client *testHAKClient) GetBackupData(ctx context.Context) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAKClient) SendCNHeartbeat(ctx context.Context, hb pb.CNStoreHeartbeat) (pb.CommandBatch, error) {
	return pb.CommandBatch{}, moerr.NewInternalErrorNoCtx("return_err")
}

func (client *testHAKClient) UpdateNonVotingReplicaNum(ctx context.Context, num uint64) error {
	//TODO implement me
	panic("implement me")
}

func (client *testHAKClient) UpdateNonVotingLocality(ctx context.Context, locality pb.Locality) error {
	//TODO implement me
	panic("implement me")
}

func Test_waitHAKeeperRunningLocked(t *testing.T) {
	conf := &cnservice.Config{}
	client := &testHAKClient{
		cfg: conf,
		mod: 1,
		cnt: 0,
	}

	op := &operator{}

	err := op.waitHAKeeperRunningLocked(client)
	assert.NoError(t, err)

	err = op.waitHAKeeperRunningLocked(client)
	assert.Error(t, err)

	err = op.waitHAKeeperRunningLocked(client)
	assert.Error(t, err)
}

func Test_waitAnyShardReadyLocked(t *testing.T) {
	conf := &cnservice.Config{}
	client := &testHAKClient{
		cfg: conf,
		mod: 2,
		cnt: 0,
	}

	op := &operator{}
	op.reset.logger = logutil.GetPanicLogger()

	err := op.waitAnyShardReadyLocked(client)
	assert.NoError(t, err)

	err = op.waitAnyShardReadyLocked(client)
	assert.Error(t, err)

	err = op.waitAnyShardReadyLocked(client)
	assert.NoError(t, err)
}

//func Test_waitHAKeeperReadyLocked(t *testing.T) {
//	op := &operator{}
//	op.reset.logger = logutil.GetPanicLogger()
//
//	_, err := op.waitHAKeeperReadyLocked()
//	assert.Error(t, err)
//}
