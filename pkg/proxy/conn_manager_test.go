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
	"fmt"
	"sync"
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/stretchr/testify/require"
)

func TestTunnelSet(t *testing.T) {
	ts := make(tunnelSet)
	tu := &tunnel{}

	ts.add(tu)
	require.Equal(t, 1, ts.count())
	ts.add(tu)
	require.Equal(t, 1, ts.count())

	ts.add(&tunnel{})
	require.Equal(t, 2, ts.count())

	require.True(t, ts.exists(tu))
	t1 := &tunnel{}
	require.False(t, ts.exists(t1))

	ts.del(tu)
	require.Equal(t, 1, ts.count())
	require.False(t, ts.exists(tu))
}

func TestCNTunnels(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ct := newCNTunnels()
	require.NotNil(t, ct)

	t1 := &tunnel{}
	ct.add("cn1", t1)
	require.Equal(t, 1, ct.count())

	t2 := &tunnel{}
	ct.add("cn1", t2)
	require.Equal(t, 2, ct.count())

	// same tunnel
	ct.add("cn1", t2)
	require.Equal(t, 2, ct.count())

	ct.add("cn1", nil)
	require.Equal(t, 2, ct.count())

	t3 := &tunnel{}
	ct.add("cn2", t3)
	require.Equal(t, 3, ct.count())

	// no this cn.
	ct.del("no-this-cn", t1)
	require.Equal(t, 3, ct.count())

	// tunnel is not on this cn.
	ct.del("cn2", t1)
	require.Equal(t, 3, ct.count())

	ct.del("cn1", t1)
	require.Equal(t, 2, ct.count())

	ct.del("cn1", t1)
	require.Equal(t, 2, ct.count())
	ct.del("cn1", t2)
	require.Equal(t, 1, ct.count())
	ct.del("cn2", t3)
	require.Equal(t, 0, ct.count())

	ct.del("cn2", t3)
	require.Equal(t, 0, ct.count())
}

func TestConnManagerConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cm := newConnManager()
	require.NotNil(t, cm)

	cn11 := testMakeCNServer("cn11", "", 0, "hash1",
		newLabelInfo("t1", map[string]string{
			"k1": "v1",
		}),
	)
	cn12 := testMakeCNServer("cn12", "", 0, "hash1",
		newLabelInfo("t1", map[string]string{
			"k1": "v1",
		}),
	)
	cn21 := testMakeCNServer("cn21", "", 0, "hash2",
		newLabelInfo("t1", map[string]string{
			"k2": "v2",
		}),
	)

	rt := runtime.DefaultRuntime()
	tu0 := newTunnel(context.TODO(), rt.Logger(), nil)

	tu11 := newTunnel(context.TODO(), rt.Logger(), nil)
	cm.connect(cn11, tu11)
	require.Equal(t, 1, cm.count())
	require.Equal(t, 1, len(cm.getLabelHashes()))
	require.Equal(t, 1, cm.getCNTunnels("hash1").count())
	require.Equal(t, 0, cm.getCNTunnels("hash2").count())

	tu12 := newTunnel(context.TODO(), rt.Logger(), nil)
	cm.connect(cn12, tu12)
	require.Equal(t, 2, cm.count())
	require.Equal(t, 1, len(cm.getLabelHashes()))
	require.Equal(t, 2, cm.getCNTunnels("hash1").count())
	require.Equal(t, 0, cm.getCNTunnels("hash2").count())

	tu21 := newTunnel(context.TODO(), rt.Logger(), nil)
	cm.connect(cn21, tu21)
	require.Equal(t, 3, cm.count())
	require.Equal(t, 2, len(cm.getLabelHashes()))
	require.Equal(t, 2, cm.getCNTunnels("hash1").count())
	require.Equal(t, 1, cm.getCNTunnels("hash2").count())

	cm.disconnect(cn12, tu11)
	require.Equal(t, 3, cm.count())
	require.Equal(t, 2, len(cm.getLabelHashes()))
	require.Equal(t, 2, cm.getCNTunnels("hash1").count())
	require.Equal(t, 1, cm.getCNTunnels("hash2").count())

	cm.disconnect(cn11, tu0)
	require.Equal(t, 3, cm.count())
	require.Equal(t, 2, len(cm.getLabelHashes()))
	require.Equal(t, 2, cm.getCNTunnels("hash1").count())
	require.Equal(t, 1, cm.getCNTunnels("hash2").count())

	cm.disconnect(cn12, tu12)
	require.Equal(t, 2, cm.count())
	require.Equal(t, 2, len(cm.getLabelHashes()))
	require.Equal(t, 1, cm.getCNTunnels("hash1").count())
	require.Equal(t, 1, cm.getCNTunnels("hash2").count())

	cm.disconnect(cn12, tu0)
	require.Equal(t, 2, cm.count())
	require.Equal(t, 2, len(cm.getLabelHashes()))
	require.Equal(t, 1, cm.getCNTunnels("hash1").count())
	require.Equal(t, 1, cm.getCNTunnels("hash2").count())

	cm.disconnect(cn11, tu11)
	require.Equal(t, 1, cm.count())
	require.Equal(t, 1, len(cm.getLabelHashes()))
	require.Equal(t, 0, cm.getCNTunnels("hash1").count())
	require.Equal(t, 1, cm.getCNTunnels("hash2").count())

	cm.disconnect(cn21, tu21)
	require.Equal(t, 0, cm.count())
	require.Equal(t, 0, len(cm.getLabelHashes()))
	require.Equal(t, 0, cm.getCNTunnels("hash1").count())
	require.Equal(t, 0, cm.getCNTunnels("hash2").count())

	cm.disconnect(cn21, tu0)
	require.Equal(t, 0, cm.count())
	require.Equal(t, 0, len(cm.getLabelHashes()))
	require.Equal(t, 0, cm.getCNTunnels("hash1").count())
	require.Equal(t, 0, cm.getCNTunnels("hash2").count())
}

func TestConnManagerConnectionConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rt := runtime.DefaultRuntime()
	cm := newConnManager()
	require.NotNil(t, cm)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(j int) {
			cn11 := testMakeCNServer(fmt.Sprintf("cn1-%d", j), "", 0, "hash1",
				newLabelInfo("t1", map[string]string{
					"k1": "v1",
				}),
			)
			tu11 := newTunnel(context.TODO(), rt.Logger(), nil)
			cm.connect(cn11, tu11)
			wg.Done()
		}(i)
		go func(j int) {
			cn11 := testMakeCNServer(fmt.Sprintf("cn2-%d", j), "", 0, "hash2",
				newLabelInfo("t1", map[string]string{
					"k2": "v2",
				}),
			)
			tu11 := newTunnel(context.TODO(), rt.Logger(), nil)
			cm.connect(cn11, tu11)
			wg.Done()
		}(i)
	}
	wg.Wait()

	require.Equal(t, 200, cm.count())
	require.Equal(t, 2, len(cm.getLabelHashes()))
	require.Equal(t, 100, cm.getCNTunnels("hash1").count())
	require.Equal(t, 100, cm.getCNTunnels("hash2").count())
}

func TestConnManagerLabelInfo(t *testing.T) {
	rt := runtime.DefaultRuntime()
	cm := newConnManager()
	require.NotNil(t, cm)

	cn11 := testMakeCNServer("cn11", "", 0, "hash1",
		newLabelInfo("t1", map[string]string{
			"k1": "v1",
		}),
	)

	tu11 := newTunnel(context.TODO(), rt.Logger(), nil)
	cm.connect(cn11, tu11)
	require.Equal(t, 1, cm.count())
	require.Equal(t, 1, len(cm.getLabelHashes()))
	require.Equal(t, 1, cm.getCNTunnels("hash1").count())
	require.Equal(t, 0, cm.getCNTunnels("hash2").count())

	li := cm.getLabelInfo("hash1")
	require.Equal(t, labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
		},
	}, li)
}

// TestSelectOneLoadBalancing tests that selectOne distributes connections evenly
// across multiple CN servers.
func TestSelectOneLoadBalancing(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cm := newConnManager()
	require.NotNil(t, cm)

	label := newLabelInfo("t1", map[string]string{"k1": "v1"})
	cn1 := testMakeCNServer("cn1", "", 0, "hash1", label)
	cn2 := testMakeCNServer("cn2", "", 0, "hash1", label)
	cn3 := testMakeCNServer("cn3", "", 0, "hash1", label)
	cns := []*CNServer{cn1, cn2, cn3}

	// First selection should select cn1 and add a placeholder
	selected := cm.selectOne("hash1", cns)
	require.NotNil(t, selected)
	require.Equal(t, "cn1", selected.uuid)
	require.Equal(t, 1, cm.getCNTunnels("hash1").count())

	// Second selection should select cn2 (least loaded)
	selected = cm.selectOne("hash1", cns)
	require.NotNil(t, selected)
	require.Equal(t, "cn2", selected.uuid)
	require.Equal(t, 2, cm.getCNTunnels("hash1").count())

	// Third selection should select cn3 (least loaded)
	selected = cm.selectOne("hash1", cns)
	require.NotNil(t, selected)
	require.Equal(t, "cn3", selected.uuid)
	require.Equal(t, 3, cm.getCNTunnels("hash1").count())

	// Fourth selection should select cn1 again (all have 1 placeholder)
	selected = cm.selectOne("hash1", cns)
	require.NotNil(t, selected)
	require.Equal(t, "cn1", selected.uuid)
	require.Equal(t, 4, cm.getCNTunnels("hash1").count())
}

// TestSelectOneWithPlaceholder tests that placeholders are correctly added
// and counted in selectOne.
func TestSelectOneWithPlaceholder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cm := newConnManager()
	require.NotNil(t, cm)

	label := newLabelInfo("t1", map[string]string{"k1": "v1"})
	cn1 := testMakeCNServer("cn1", "", 0, "hash1", label)
	cn2 := testMakeCNServer("cn2", "", 0, "hash1", label)
	cns := []*CNServer{cn1, cn2}

	// Select cn1, should add a placeholder
	selected := cm.selectOne("hash1", cns)
	require.Equal(t, "cn1", selected.uuid)
	cnTunnels := cm.getCNTunnels("hash1")
	require.NotNil(t, cnTunnels)
	require.Equal(t, 1, cnTunnels.count())

	// Check that the placeholder has ctx == nil
	tunnels := cnTunnels["cn1"]
	require.NotNil(t, tunnels)
	require.Equal(t, 1, tunnels.count())
	for tun := range tunnels {
		require.Nil(t, tun.ctx, "Placeholder tunnel should have ctx == nil")
	}
}

// TestSelectOneConcurrentDistribution tests that concurrent selectOne calls
// distribute connections evenly across CN servers.
func TestSelectOneConcurrentDistribution(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cm := newConnManager()
	require.NotNil(t, cm)

	label := newLabelInfo("t1", map[string]string{"k1": "v1"})
	cn1 := testMakeCNServer("cn1", "", 0, "hash1", label)
	cn2 := testMakeCNServer("cn2", "", 0, "hash1", label)
	cn3 := testMakeCNServer("cn3", "", 0, "hash1", label)
	cns := []*CNServer{cn1, cn2, cn3}

	// Concurrently select 30 connections
	numSelections := 30
	var wg sync.WaitGroup
	selections := make([]*CNServer, numSelections)
	var mu sync.Mutex

	for i := 0; i < numSelections; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			selected := cm.selectOne("hash1", cns)
			mu.Lock()
			selections[idx] = selected
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	// Count selections per CN
	cnCounts := make(map[string]int)
	for _, selected := range selections {
		require.NotNil(t, selected)
		cnCounts[selected.uuid]++
	}

	// All CNs should be selected at least once
	require.Equal(t, 3, len(cnCounts))
	require.GreaterOrEqual(t, cnCounts["cn1"], 1)
	require.GreaterOrEqual(t, cnCounts["cn2"], 1)
	require.GreaterOrEqual(t, cnCounts["cn3"], 1)

	// Total count should match number of selections
	require.Equal(t, numSelections, cm.getCNTunnels("hash1").count())

	// Distribution should be relatively even (each CN should have at least 5 selections)
	// Note: Due to concurrent execution, exact distribution may vary, but should be reasonable
	minSelections := numSelections / 6 // At least 1/6 of selections per CN
	require.GreaterOrEqual(t, cnCounts["cn1"], minSelections)
	require.GreaterOrEqual(t, cnCounts["cn2"], minSelections)
	require.GreaterOrEqual(t, cnCounts["cn3"], minSelections)
}

// TestSelectOneFailed tests that selectOneFailed correctly removes placeholders.
func TestSelectOneFailed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cm := newConnManager()
	require.NotNil(t, cm)

	label := newLabelInfo("t1", map[string]string{"k1": "v1"})
	cn1 := testMakeCNServer("cn1", "", 0, "hash1", label)
	cn2 := testMakeCNServer("cn2", "", 0, "hash1", label)
	cns := []*CNServer{cn1, cn2}

	// Select cn1, should add a placeholder
	selected := cm.selectOne("hash1", cns)
	require.Equal(t, "cn1", selected.uuid)
	require.Equal(t, 1, cm.getCNTunnels("hash1").count())

	// Simulate connection failure
	cm.selectOneFailed("hash1", "cn1")
	require.Equal(t, 0, cm.getCNTunnels("hash1").count())

	// Select again, should select cn1 again (now it has 0 connections)
	selected = cm.selectOne("hash1", cns)
	require.Equal(t, "cn1", selected.uuid)
	require.Equal(t, 1, cm.getCNTunnels("hash1").count())
}

// TestSelectOnePlaceholderReplacement tests that connect correctly replaces
// placeholder with real tunnel.
func TestSelectOnePlaceholderReplacement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rt := runtime.DefaultRuntime()
	cm := newConnManager()
	require.NotNil(t, cm)

	label := newLabelInfo("t1", map[string]string{"k1": "v1"})
	cn1 := testMakeCNServer("cn1", "", 0, "hash1", label)
	cn2 := testMakeCNServer("cn2", "", 0, "hash1", label)
	cns := []*CNServer{cn1, cn2}

	// Select cn1, should add a placeholder
	selected := cm.selectOne("hash1", cns)
	require.Equal(t, "cn1", selected.uuid)
	cnTunnels := cm.getCNTunnels("hash1")
	require.Equal(t, 1, cnTunnels.count())

	// Verify placeholder exists (ctx == nil)
	tunnels := cnTunnels["cn1"]
	require.NotNil(t, tunnels)
	require.Equal(t, 1, tunnels.count())
	var placeholder *tunnel
	for tun := range tunnels {
		require.Nil(t, tun.ctx)
		placeholder = tun
	}

	// Connect with real tunnel, should replace placeholder
	realTunnel := newTunnel(context.TODO(), rt.Logger(), nil)
	cm.connect(cn1, realTunnel)
	require.Equal(t, 1, cm.getCNTunnels("hash1").count())

	// Verify placeholder is replaced with real tunnel
	// Note: getCNTunnels returns a reference to the map, so cnTunnels and cnTunnelsAfterConnect
	// point to the same map. We can use either, but re-fetch for clarity.
	cnTunnelsAfterConnect := cm.getCNTunnels("hash1")
	tunnels = cnTunnelsAfterConnect["cn1"]
	require.Equal(t, 1, tunnels.count())
	require.True(t, tunnels.exists(realTunnel))
	require.False(t, tunnels.exists(placeholder))
	require.NotNil(t, realTunnel.ctx)
}

// TestSelectOneMultiplePending tests that multiple pending connections
// to the same CN are correctly counted.
func TestSelectOneMultiplePending(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cm := newConnManager()
	require.NotNil(t, cm)

	label := newLabelInfo("t1", map[string]string{"k1": "v1"})
	cn1 := testMakeCNServer("cn1", "", 0, "hash1", label)
	cn2 := testMakeCNServer("cn2", "", 0, "hash1", label)
	cns := []*CNServer{cn1, cn2}

	// Select cn1 multiple times
	selected1 := cm.selectOne("hash1", cns)
	require.Equal(t, "cn1", selected1.uuid)
	require.Equal(t, 1, cm.getCNTunnels("hash1").count())

	selected2 := cm.selectOne("hash1", cns)
	// cn1 has 1 placeholder (count=1), cn2 has 0 (count=0), so should choose cn2 (least loaded)
	require.Equal(t, "cn2", selected2.uuid)
	require.Equal(t, 2, cm.getCNTunnels("hash1").count())

	selected3 := cm.selectOne("hash1", cns)
	// cn1 has 1 placeholder (count=1), cn2 has 1 placeholder (count=1), so should choose cn1 (first in list)
	require.Equal(t, "cn1", selected3.uuid)
	require.Equal(t, 3, cm.getCNTunnels("hash1").count())
}

// TestSelectOneWithRealConnections tests that selectOne correctly considers
// both real connections and placeholders when selecting.
func TestSelectOneWithRealConnections(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rt := runtime.DefaultRuntime()
	cm := newConnManager()
	require.NotNil(t, cm)

	label := newLabelInfo("t1", map[string]string{"k1": "v1"})
	cn1 := testMakeCNServer("cn1", "", 0, "hash1", label)
	cn2 := testMakeCNServer("cn2", "", 0, "hash1", label)
	cns := []*CNServer{cn1, cn2}

	// Connect a real tunnel to cn1
	realTunnel := newTunnel(context.TODO(), rt.Logger(), nil)
	cm.connect(cn1, realTunnel)
	require.Equal(t, 1, cm.getCNTunnels("hash1").count())

	// Select should choose cn2 (least loaded)
	selected := cm.selectOne("hash1", cns)
	require.Equal(t, "cn2", selected.uuid)
	require.Equal(t, 2, cm.getCNTunnels("hash1").count())

	// Select again, cn1 has 1 real (count=1), cn2 has 1 placeholder (count=1)
	// Both have same count, so should choose the first one (cn1)
	selected = cm.selectOne("hash1", cns)
	require.Equal(t, "cn1", selected.uuid)
	require.Equal(t, 3, cm.getCNTunnels("hash1").count()) // cn1 now has 1 real + 1 placeholder

	// Connect real tunnel to cn2, replacing one placeholder
	realTunnel2 := newTunnel(context.TODO(), rt.Logger(), nil)
	cm.connect(cn2, realTunnel2)
	require.Equal(t, 3, cm.getCNTunnels("hash1").count()) // Still 3: 1 real on cn2, 1 real + 1 placeholder on cn1

	// Now cn1 has 1 real + 1 placeholder (count=2), cn2 has 1 real (count=1), so next selection should choose cn2 (least loaded)
	selected = cm.selectOne("hash1", cns)
	require.Equal(t, "cn2", selected.uuid)
	require.Equal(t, 4, cm.getCNTunnels("hash1").count()) // cn2 now has 1 real + 1 placeholder
}

// TestSelectOneFailedWithMultiplePending tests that selectOneFailed correctly
// removes one placeholder when there are multiple pending connections.
func TestSelectOneFailedWithMultiplePending(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cm := newConnManager()
	require.NotNil(t, cm)

	label := newLabelInfo("t1", map[string]string{"k1": "v1"})
	cn1 := testMakeCNServer("cn1", "", 0, "hash1", label)
	cns := []*CNServer{cn1}

	// Select cn1 three times
	cm.selectOne("hash1", cns)
	cm.selectOne("hash1", cns)
	cm.selectOne("hash1", cns)
	require.Equal(t, 3, cm.getCNTunnels("hash1").count())

	// Fail one connection
	cm.selectOneFailed("hash1", "cn1")
	require.Equal(t, 2, cm.getCNTunnels("hash1").count())

	// Fail another connection
	cm.selectOneFailed("hash1", "cn1")
	require.Equal(t, 1, cm.getCNTunnels("hash1").count())

	// Fail the last connection
	cm.selectOneFailed("hash1", "cn1")
	require.Equal(t, 0, cm.getCNTunnels("hash1").count())
}

// TestSelectOneEmptyCNServers tests that selectOne handles empty CN server list.
func TestSelectOneEmptyCNServers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cm := newConnManager()
	require.NotNil(t, cm)

	// Select with empty CN list
	selected := cm.selectOne("hash1", []*CNServer{})
	require.Nil(t, selected)
	require.Equal(t, 0, cm.getCNTunnels("hash1").count())
}
