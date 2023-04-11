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

package clusterservice

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterReady(t *testing.T) {
	runClusterTest(
		time.Hour,
		func(hc *testHAKeeperClient, c *cluster) {
			cc := make(chan struct{})
			go func() {
				defer close(cc)
				c.GetCNService(NewSelector(), nil)
			}()
			select {
			case <-cc:
			case <-time.After(time.Second * 5):
				assert.Fail(t, "wait ready timeout")
			}
		})
}

func TestClusterForceRefresh(t *testing.T) {
	runClusterTest(
		time.Hour,
		func(hc *testHAKeeperClient, c *cluster) {
			cnt := 0
			apply := func(c metadata.CNService) bool {
				cnt++
				return true
			}
			c.GetCNService(NewServiceIDSelector("cn0"), apply)
			assert.Equal(t, 0, cnt)

			hc.addCN("cn0")
			cnt = 0
			c.ForceRefresh()
			time.Sleep(time.Millisecond * 100)
			c.GetCNService(NewServiceIDSelector("cn0"), apply)
			assert.Equal(t, 1, cnt)
		})
}

func TestClusterRefresh(t *testing.T) {
	runClusterTest(
		time.Millisecond*10,
		func(hc *testHAKeeperClient, c *cluster) {
			cnt := 0
			apply := func(c metadata.DNService) bool {
				cnt++
				return true
			}
			c.GetDNService(NewServiceIDSelector("dn0"), apply)
			assert.Equal(t, 0, cnt)

			hc.addDN("dn0")
			time.Sleep(time.Millisecond * 100)
			c.GetDNService(NewServiceIDSelector("dn0"), apply)
			assert.Equal(t, 1, cnt)
		})
}

func BenchmarkGetService(b *testing.B) {
	runClusterTest(
		time.Hour,
		func(hc *testHAKeeperClient, c *cluster) {
			cnt := 0
			apply := func(c metadata.DNService) bool {
				cnt++
				return true
			}
			c.GetDNService(NewServiceIDSelector("dn0"), apply)

			hc.addDN("dn0")
			c.ForceRefresh()
			time.Sleep(time.Millisecond * 100)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.GetDNService(NewServiceIDSelector("dn0"), apply)
			}
		})
}

func TestCluster_DebugUpdateCNLabel(t *testing.T) {
	runClusterTest(
		time.Hour,
		func(hc *testHAKeeperClient, c *cluster) {
			var cns []metadata.CNService
			apply := func(c metadata.CNService) bool {
				cns = append(cns, c)
				return true
			}
			hc.addCN("cn0")
			err := c.DebugUpdateCNLabel("cn0", map[string][]string{"k1": {"v1"}})
			require.NoError(t, err)
			c.ForceRefresh()
			time.Sleep(time.Millisecond * 100)
			c.GetCNService(NewServiceIDSelector("cn0"), apply)
			require.Equal(t, 1, len(cns))
			require.Equal(t, "cn0", cns[0].ServiceID)
			require.Equal(t, map[string]metadata.LabelList{
				"k1": {Labels: []string{"v1"}},
			}, cns[0].Labels)
		})
}

func runClusterTest(
	refreshInterval time.Duration,
	fn func(*testHAKeeperClient, *cluster)) {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	hc := &testHAKeeperClient{}
	c := NewMOCluster(hc, refreshInterval)
	defer c.Close()
	fn(hc, c.(*cluster))
}

type testHAKeeperClient struct {
	sync.RWMutex
	value logpb.ClusterDetails
	err   error
}

func (c *testHAKeeperClient) addCN(serviceIDs ...string) {
	c.Lock()
	defer c.Unlock()
	for _, id := range serviceIDs {
		c.value.CNStores = append(c.value.CNStores, logpb.CNStore{
			UUID: id,
		})
	}
}

func (c *testHAKeeperClient) addDN(serviceIDs ...string) {
	c.Lock()
	defer c.Unlock()
	for _, id := range serviceIDs {
		c.value.DNStores = append(c.value.DNStores, logpb.DNStore{
			UUID: id,
		})
	}
}

func (c *testHAKeeperClient) Close() error                                   { return nil }
func (c *testHAKeeperClient) AllocateID(ctx context.Context) (uint64, error) { return 0, nil }
func (c *testHAKeeperClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	return 0, nil
}
func (c *testHAKeeperClient) GetClusterDetails(ctx context.Context) (logpb.ClusterDetails, error) {
	c.RLock()
	defer c.RUnlock()
	return c.value, c.err
}
func (c *testHAKeeperClient) GetClusterState(ctx context.Context) (logpb.CheckerState, error) {
	return logpb.CheckerState{}, nil
}
func (c *testHAKeeperClient) GetCNState(ctx context.Context) (logpb.CNState, error) {
	return logpb.CNState{}, nil
}
func (c *testHAKeeperClient) UpdateCNLabel(ctx context.Context, label logpb.CNStoreLabel) error {
	c.Lock()
	defer c.Unlock()
	for i, cn := range c.value.CNStores {
		if cn.UUID == label.UUID {
			c.value.CNStores[i].Labels = label.Labels
		}
	}
	return nil
}
