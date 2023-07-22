// Copyright 2022 Matrix Origin
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

package util

import (
	"context"
	"sync"
	"testing"

	log "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
)

type testHAKeeperClient struct {
	sync.RWMutex
	value log.ClusterDetails
}

func (c *testHAKeeperClient) addCN(state metadata.WorkState, serviceIDs ...string) {
	c.Lock()
	defer c.Unlock()
	for _, id := range serviceIDs {
		c.value.CNStores = append(c.value.CNStores, log.CNStore{
			UUID:       id,
			SQLAddress: id,
			WorkState:  state,
		})
	}
}

func (c *testHAKeeperClient) GetClusterDetails(ctx context.Context) (log.ClusterDetails, error) {
	c.Lock()
	defer c.Unlock()
	return c.value, nil
}

func TestAddressFunc(t *testing.T) {
	t.Run("no client", func(t *testing.T) {
		getClient := func() HAKeeperClient {
			return nil
		}
		fn := AddressFunc(getClient)
		ctx := context.Background()
		cn, err := fn(ctx, true)
		assert.Error(t, err)
		assert.Equal(t, "", cn)
	})

	t.Run("no cn", func(t *testing.T) {
		getClient := func() HAKeeperClient {
			return &testHAKeeperClient{}
		}
		fn := AddressFunc(getClient)
		ctx := context.Background()
		cn, err := fn(ctx, true)
		assert.Error(t, err)
		assert.Equal(t, "", cn)
	})

	t.Run("one available cn", func(t *testing.T) {
		client := &testHAKeeperClient{}
		client.addCN(metadata.WorkState_Working, "cn1")
		client.addCN(metadata.WorkState_Draining, "cn2")
		getClient := func() HAKeeperClient {
			return client
		}
		fn := AddressFunc(getClient)
		ctx := context.Background()
		cn, err := fn(ctx, false) // set false to return the last one.
		assert.NoError(t, err)
		assert.Equal(t, "cn1", cn)
	})

	t.Run("multi cn", func(t *testing.T) {
		client := &testHAKeeperClient{}
		client.addCN(metadata.WorkState_Working, "cn1")
		client.addCN(metadata.WorkState_Working, "cn2")
		client.addCN(metadata.WorkState_Working, "cn3")
		client.addCN(metadata.WorkState_Working, "cn4")
		getClient := func() HAKeeperClient {
			return client
		}
		fn := AddressFunc(getClient)
		ctx := context.Background()
		_, err := fn(ctx, true)
		assert.NoError(t, err)
	})
}
