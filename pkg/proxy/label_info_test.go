// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

type mockHAKeeperClient struct {
	sync.RWMutex
	value logpb.ClusterDetails
	err   error
}

func (c *mockHAKeeperClient) updateCN(uuid string, addr string, labels map[string]metadata.LabelList) {
	c.Lock()
	defer c.Unlock()
	var cs *logpb.CNStore
	for i := range c.value.CNStores {
		if c.value.CNStores[i].UUID == uuid {
			cs = &c.value.CNStores[i]
			break
		}
	}
	if cs != nil {
		cs.Labels = labels
		cs.SQLAddress = addr
		return
	}
	cs = &logpb.CNStore{
		UUID:       uuid,
		SQLAddress: addr,
		Labels:     labels,
	}
	c.value.CNStores = append(c.value.CNStores, *cs)
}

func (c *mockHAKeeperClient) Close() error                                   { return nil }
func (c *mockHAKeeperClient) AllocateID(ctx context.Context) (uint64, error) { return 0, nil }
func (c *mockHAKeeperClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	return uint64(nextClientConnID()), nil
}
func (c *mockHAKeeperClient) AllocateIDByKeyWithBatch(ctx context.Context, key string, batch uint64) (uint64, error) {
	return uint64(nextClientConnID()), nil
}
func (c *mockHAKeeperClient) GetClusterDetails(ctx context.Context) (logpb.ClusterDetails, error) {
	c.RLock()
	defer c.RUnlock()
	return c.value, c.err
}
func (c *mockHAKeeperClient) GetClusterState(ctx context.Context) (logpb.CheckerState, error) {
	return logpb.CheckerState{
		TaskTableUser: logpb.TaskTableUser{
			Username: "u1",
			Password: "p1",
		},
	}, nil
}

func TestLabelInfoReserved(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tenant := Tenant("t1")
	labels := map[string]string{
		"k1":           "v1",
		"k2":           "v2",
		"os_user":      "u1",
		"os_sudouser":  "u2",
		"program_name": "p1",
		"_r1":          "_v1",
		"_r2":          "_v2",
	}
	info := newLabelInfo(tenant, labels)
	require.Equal(t, 2, len(info.Labels))
	_, ok := info.Labels["os_user"]
	require.False(t, ok)
	_, ok = info.Labels["os_sudouser"]
	require.False(t, ok)
	_, ok = info.Labels["program_name"]
	require.False(t, ok)
	_, ok = info.Labels["_r1"]
	require.False(t, ok)
	_, ok = info.Labels["_r2"]
	require.False(t, ok)
	v1, ok := info.Labels["k1"]
	require.True(t, ok)
	require.Equal(t, "v1", v1)
	v2, ok := info.Labels["k2"]
	require.True(t, ok)
	require.Equal(t, "v2", v2)
}

func TestLabelInfoAll(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tenant := Tenant("t1")
	labels := map[string]string{
		"k1": "v1",
		"k2": "v2",
	}
	info := newLabelInfo(tenant, labels)

	t.Run("all", func(t *testing.T) {
		all := info.allLabels()
		require.Equal(t, 3, len(all))

		v0, ok := all[tenantLabelKey]
		require.True(t, ok)
		require.Equal(t, "t1", v0)

		v1, ok := all["k1"]
		require.True(t, ok)
		require.Equal(t, "v1", v1)

		v2, ok := all["k2"]
		require.True(t, ok)
		require.Equal(t, "v2", v2)

		_, ok = all["no"]
		require.False(t, ok)
	})

	t.Run("common", func(t *testing.T) {
		common := info.commonLabels()
		require.Equal(t, 2, len(common))
		v1, ok := common["k1"]
		require.True(t, ok)
		require.Equal(t, "v1", v1)
		v2, ok := common["k2"]
		require.True(t, ok)
		require.Equal(t, "v2", v2)
	})

	t.Run("tenant", func(t *testing.T) {
		tenantLabel := info.tenantLabel()
		require.Equal(t, 1, len(tenantLabel))
		v1, ok := tenantLabel[tenantLabelKey]
		require.True(t, ok)
		require.Equal(t, "t1", v1)
	})
}

func TestLabelSuperTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tenant := Tenant("t1")
	labels := map[string]string{
		"k1": "v1",
		"k2": "v2",
	}
	info := newLabelInfo(tenant, labels)
	require.Equal(t, false, info.isSuperTenant())

	tenant = ""
	info = newLabelInfo(tenant, labels)
	require.Equal(t, true, info.isSuperTenant())

	tenant = "sys"
	info = newLabelInfo(tenant, labels)
	require.Equal(t, true, info.isSuperTenant())
}

func TestLabelSelector(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tenant := Tenant("t1")
	labels := map[string]string{
		"k1": "v1",
		"k2": "v2",
	}
	info := newLabelInfo(tenant, labels)
	se := info.genSelector()

	hc := &mockHAKeeperClient{}
	cnLabels1 := map[string]metadata.LabelList{
		tenantLabelKey: {
			Labels: []string{"t1"},
		},
		"k1": {
			Labels: []string{"v1", "multi1"},
		},
		"k2": {
			Labels: []string{"v2", "multi2"},
		},
		"k3": {
			Labels: []string{"v3", "multi3"},
		},
	}
	cnLabels2 := map[string]metadata.LabelList{
		tenantLabelKey: {
			Labels: []string{"t1"},
		},
		"k1": {
			Labels: []string{"no1", "no2"},
		},
		"k2": {
			Labels: []string{"v2", "multi2"},
		},
		"k3": {
			Labels: []string{"v3", "multi3"},
		},
	}
	hc.updateCN("cn1", "", cnLabels1)
	hc.updateCN("cn2", "", cnLabels2)

	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	mc := clusterservice.NewMOCluster(hc, 3*time.Second)
	defer func() { mc.Close() }()
	mc.ForceRefresh()
	time.Sleep(time.Millisecond * 200)

	var servers []CNServer
	mc.GetCNService(se, func(s metadata.CNService) bool {
		servers = append(servers, CNServer{
			uuid: s.ServiceID,
		})
		return true
	})
	require.Equal(t, 1, len(servers))
}

func TestLabelHash(t *testing.T) {
	defer leaktest.AfterTest(t)()
	label1 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
			"k2": "v2",
		},
	}
	h1, err := label1.getHash()
	require.NoError(t, err)

	label2 := labelInfo{
		Labels: map[string]string{
			"k2": "v2",
			"k1": "v1",
		},
		Tenant: "t1",
	}
	h2, err := label2.getHash()
	require.NoError(t, err)

	label3 := labelInfo{
		Tenant: "t3",
		Labels: map[string]string{
			"k1": "v1",
			"k2": "v2",
		},
	}
	h3, err := label3.getHash()
	require.NoError(t, err)

	label4 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
			"k2": "v3",
		},
	}
	h4, err := label4.getHash()
	require.NoError(t, err)

	require.Equal(t, h1, h2)
	require.NotEqual(t, h1, h3)
	require.NotEqual(t, h1, h4)
}

func TestLabelMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	lb := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
			"k2": "v2",
		},
	}
	lb.merge(nil)
	require.Equal(t, 2, len(lb.Labels))

	lb.merge(map[string]string{"a": "1"})
	require.Equal(t, 3, len(lb.Labels))

	lb.merge(map[string]string{"k1": "v11"})
	require.Equal(t, 3, len(lb.Labels))
	require.Equal(t, "v1", lb.Labels["k1"])
}
