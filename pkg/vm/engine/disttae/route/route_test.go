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

package route

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
	"github.com/stretchr/testify/assert"
)

type mockHAKeeperClient struct {
	sync.RWMutex
	value logpb.ClusterDetails
	err   error
}

func (c *mockHAKeeperClient) updateCN(uuid string, labels map[string]metadata.LabelList) {
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
		return
	}
	cs = &logpb.CNStore{
		UUID:      uuid,
		Labels:    labels,
		WorkState: metadata.WorkState_Working,
	}
	c.value.CNStores = append(c.value.CNStores, *cs)
}

func (c *mockHAKeeperClient) GetClusterDetails(ctx context.Context) (logpb.ClusterDetails, error) {
	c.RLock()
	defer c.RUnlock()
	return c.value, c.err
}

func runTestWithMOCluster(t *testing.T, fn func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster)) {
	defer leaktest.AfterTest(t)()
	rt := runtime.DefaultRuntime()
	runtime.SetupProcessLevelRuntime(rt)
	hc := &mockHAKeeperClient{}
	mc := clusterservice.NewMOCluster(hc, 3*time.Second)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)

	fn(t, hc, mc)
}

func TestRouteForSuperTenant_C0_Contain(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{}
		hc.updateCN("cn2", cnLabels2)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
		}, clusterservice.Contain)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 2, len(cns))
	})
}

func TestRouteForSuperTenant_C0_EQ_Globbing(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{}
		hc.updateCN("cn2", cnLabels2)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
		}, clusterservice.EQ_Globbing)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 2, len(cns))
	})
}

func TestRouteForSuperTenant_C1_Contain(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"sys"},
			},
			"k1": {
				Labels: []string{"v1"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"k2": {
				Labels: []string{"v2"},
			},
		}
		hc.updateCN("cn2", cnLabels2)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
			"k1":      "v1",
		}, clusterservice.Contain)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 1, len(cns))
		assert.Equal(t, "cn1", cns[0].ServiceID)
	})
}

func TestRouteForSuperTenant_C1_EQ_Globbing(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"sys"},
			},
			"k1": {
				Labels: []string{"v1"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"sys"},
			},
			"k1": {
				Labels: []string{"*"},
			},
		}
		hc.updateCN("cn2", cnLabels2)

		cnLabels3 := map[string]metadata.LabelList{
			"k2": {
				Labels: []string{"v2"},
			},
		}
		hc.updateCN("cn3", cnLabels3)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
			"k1":      "v1",
		}, clusterservice.EQ_Globbing)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 2, len(cns))
	})
}

func TestRouteForSuperTenant_C2_Contain(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"k1": {
				Labels: []string{"v1"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"k2": {
				Labels: []string{"v2"},
			},
		}
		hc.updateCN("cn2", cnLabels2)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
			"k2":      "v2",
		}, clusterservice.Contain)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 1, len(cns))
		assert.Equal(t, "cn2", cns[0].ServiceID)
	})
}

func TestRouteForSuperTenant_C2_EQ_Globbing(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"k1": {
				Labels: []string{"v1"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"k2": {
				Labels: []string{"v2"},
			},
		}
		hc.updateCN("cn2", cnLabels2)

		cnLabels3 := map[string]metadata.LabelList{
			"k2": {
				Labels: []string{"*"},
			},
		}
		hc.updateCN("cn3", cnLabels3)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
			"k2":      "v2",
		}, clusterservice.EQ_Globbing)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 2, len(cns))
	})
}

func TestRouteForSuperTenant_C3_Contain(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"sys"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"k2": {
				Labels: []string{"v2"},
			},
		}
		hc.updateCN("cn2", cnLabels2)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
		}, clusterservice.Contain)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 1, len(cns))
		assert.Equal(t, "cn1", cns[0].ServiceID)
	})
}

func TestRouteForSuperTenant_C3_EQ_Globbing(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"sys"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"sy*"},
			},
		}
		hc.updateCN("cn2", cnLabels2)

		cnLabels3 := map[string]metadata.LabelList{
			"k2": {
				Labels: []string{"v2"},
			},
		}
		hc.updateCN("cn3", cnLabels3)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
		}, clusterservice.EQ_Globbing)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 2, len(cns))
	})
}

func TestRouteForSuperTenant_C4_Contain(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"k1": {
				Labels: []string{"v1"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"k2": {
				Labels: []string{"v2"},
			},
		}
		hc.updateCN("cn2", cnLabels2)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
		}, clusterservice.Contain)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 2, len(cns))
	})
}

func TestRouteForSuperTenant_C4_EQ_Globbing(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"k1": {
				Labels: []string{"v1"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"k2": {
				Labels: []string{"v2"},
			},
		}
		hc.updateCN("cn2", cnLabels2)

		cnLabels3 := map[string]metadata.LabelList{
			"k3": {
				Labels: []string{"*"},
			},
		}
		hc.updateCN("cn3", cnLabels3)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
		}, clusterservice.EQ_Globbing)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 3, len(cns))
	})
}

func TestRouteForSuperTenant_C5_Contain(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"k1": {
				Labels: []string{"v1"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{}
		hc.updateCN("cn2", cnLabels2)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
		}, clusterservice.Contain)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 1, len(cns))
		assert.Equal(t, "cn1", cns[0].ServiceID)
	})
}

func TestRouteForSuperTenant_C5_EQ_Globbing(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"k1": {
				Labels: []string{"v1"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"k2": {
				Labels: []string{"*"},
			},
		}
		hc.updateCN("cn2", cnLabels2)

		cnLabels3 := map[string]metadata.LabelList{}
		hc.updateCN("cn3", cnLabels3)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
		}, clusterservice.EQ_Globbing)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 2, len(cns))
	})
}

func TestRouteForSuperTenant_C6_Contain(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"sys"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{}
		hc.updateCN("cn2", cnLabels2)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
		}, clusterservice.Contain)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 1, len(cns))
		assert.Equal(t, "cn1", cns[0].ServiceID)
	})
}

func TestRouteForSuperTenant_C6_EQ_Globbing(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"sys"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"s*"},
			},
		}
		hc.updateCN("cn2", cnLabels2)

		cnLabels3 := map[string]metadata.LabelList{}
		hc.updateCN("cn3", cnLabels3)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
		}, clusterservice.EQ_Globbing)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 2, len(cns))
	})
}

func TestRouteForSuperTenant_C7_Contain(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"sys"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"k2": {
				Labels: []string{"v2"},
			},
			"k3": {
				Labels: []string{"v3"},
			},
		}
		hc.updateCN("cn2", cnLabels2)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
			"k3":      "v3",
		}, clusterservice.Contain)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 1, len(cns))
		assert.Equal(t, "cn2", cns[0].ServiceID)
	})
}

func TestRouteForSuperTenant_C7_EQ_Globbing(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"sys"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"k2": {
				Labels: []string{"v2"},
			},
			"k3": {
				Labels: []string{"v3"},
			},
		}
		hc.updateCN("cn2", cnLabels2)

		cnLabels3 := map[string]metadata.LabelList{
			"k2": {
				Labels: []string{"v2"},
			},
			"k3": {
				Labels: []string{"*"},
			},
		}
		hc.updateCN("cn3", cnLabels3)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
			"k3":      "v3",
		}, clusterservice.EQ_Globbing)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 3, len(cns))
	})
}

func TestRouteForSuperTenant_C8_Contain(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"sys"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"k2": {
				Labels: []string{"v2"},
			},
		}
		hc.updateCN("cn2", cnLabels2)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
			"k1":      "v1",
		}, clusterservice.Contain)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 2, len(cns))
	})
}

func TestRouteForSuperTenant_C8_EQ_Globbing(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"sys"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"k2": {
				Labels: []string{"v2"},
			},
		}
		hc.updateCN("cn2", cnLabels2)

		cnLabels3 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"sys"},
			},
		}
		hc.updateCN("cn3", cnLabels3)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "sys",
			"k1":      "v1",
		}, clusterservice.EQ_Globbing)
		RouteForSuperTenant(s, "dump", nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 3, len(cns))
	})
}

func TestRouteForCommonTenant_C1_Contain(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"k1": {
				Labels: []string{"v1"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{}
		hc.updateCN("cn2", cnLabels2)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "t1",
		}, clusterservice.Contain)
		RouteForCommonTenant(s, nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 1, len(cns))
		assert.Equal(t, "cn2", cns[0].ServiceID)
	})
}

func TestRouteForCommonTenant_C1_EQ_Globbing(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"k1": {
				Labels: []string{"v1"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"k1": {
				Labels: []string{"*"},
			},
		}
		hc.updateCN("cn2", cnLabels2)

		cnLabels3 := map[string]metadata.LabelList{}
		hc.updateCN("cn3", cnLabels3)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "t1",
		}, clusterservice.Contain)
		RouteForCommonTenant(s, nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 1, len(cns))
		assert.Equal(t, "cn3", cns[0].ServiceID)
	})
}

func TestRouteForCommonTenant_C2_Contain(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"t1"},
			},
			"k1": {
				Labels: []string{"v1"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"t2"},
			},
			"k1": {
				Labels: []string{"v1"},
			},
		}
		hc.updateCN("cn2", cnLabels2)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "t1",
		}, clusterservice.Contain)
		RouteForCommonTenant(s, nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 1, len(cns))
		assert.Equal(t, "cn1", cns[0].ServiceID)
	})
}

func TestRouteForCommonTenant_C2_EQ_Globbing(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"t1"},
			},
			"k1": {
				Labels: []string{"v1"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"t2"},
			},
			"k1": {
				Labels: []string{"v1"},
			},
		}
		hc.updateCN("cn2", cnLabels2)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "t1",
		}, clusterservice.EQ_Globbing)
		RouteForCommonTenant(s, nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 0, len(cns))
	})
}

func TestRouteForCommonTenant_C3_EQ_Globbing(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"t1"},
			},
			"k1": {
				Labels: []string{"*"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"t2"},
			},
			"k1": {
				Labels: []string{"v1"},
			},
		}
		hc.updateCN("cn2", cnLabels2)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "t1",
			"k1":      "v1",
		}, clusterservice.EQ_Globbing)
		RouteForCommonTenant(s, nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 1, len(cns))
		assert.Equal(t, "cn1", cns[0].ServiceID)
	})
}

func TestRouteForCommonTenant_C4_EQ_Globbing(t *testing.T) {
	runTestWithMOCluster(t, func(t *testing.T, hc *mockHAKeeperClient, c clusterservice.MOCluster) {
		cnLabels1 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"t1"},
			},
			"k1": {
				Labels: []string{"*"},
			},
		}
		hc.updateCN("cn1", cnLabels1)

		cnLabels2 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"t1"},
			},
			"k1": {
				Labels: []string{"v*"},
			},
		}
		hc.updateCN("cn2", cnLabels2)

		cnLabels3 := map[string]metadata.LabelList{
			"account": {
				Labels: []string{"t2"},
			},
			"k1": {
				Labels: []string{"v1"},
			},
		}
		hc.updateCN("cn3", cnLabels3)
		c.ForceRefresh(true)

		var cns []*metadata.CNService

		s := clusterservice.NewSelector().SelectByLabel(map[string]string{
			"account": "t1",
			"k1":      "v1",
		}, clusterservice.EQ_Globbing)
		RouteForCommonTenant(s, nil, func(s *metadata.CNService) {
			cns = append(cns, s)
		})
		assert.Equal(t, 2, len(cns))
	})
}
