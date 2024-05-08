// Copyright 2021 -2023 Matrix Origin
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

package status

import (
	"context"
	"testing"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
)

type mockHAKeeperClient struct {
	details pb.ClusterDetails
}

func (c *mockHAKeeperClient) Close() error {
	return nil
}

func (c *mockHAKeeperClient) AllocateID(ctx context.Context) (uint64, error) {
	return 0, nil
}

func (c *mockHAKeeperClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	return 0, nil
}

func (c *mockHAKeeperClient) AllocateIDByKeyWithBatch(ctx context.Context, key string, batch uint64) (uint64, error) {
	return 0, nil
}

func (c *mockHAKeeperClient) GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	return c.details, nil
}

func (c *mockHAKeeperClient) GetClusterState(ctx context.Context) (pb.CheckerState, error) {
	return pb.CheckerState{}, nil
}

func (c *mockHAKeeperClient) addCNStore() {
	c.details.CNStores = append(c.details.CNStores, pb.CNStore{
		SQLAddress: "127.0.0.1:8000",
	})
}

func (c *mockHAKeeperClient) addDeletedCNStore() {
	c.details.DeletedStores = append(c.details.DeletedStores, pb.DeletedStore{})
}

func TestFillHAKeeper(t *testing.T) {
	var status Status
	var client mockHAKeeperClient
	for i := 0; i < 10; i++ {
		client.addCNStore()
		client.addDeletedCNStore()
	}
	status.HAKeeperStatus.fill(&client)
	assert.Equal(t, 10, len(status.HAKeeperStatus.Nodes))
	assert.Equal(t, 10, len(status.HAKeeperStatus.DeletedNodes))
}
