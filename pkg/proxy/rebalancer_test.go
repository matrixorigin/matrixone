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
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func testRebalancer(
	t *testing.T, st *stopper.Stopper, logger *log.MOLogger, mc clusterservice.MOCluster,
) *rebalancer {
	var opts []rebalancerOption
	opts = append(opts,
		withRebalancerInterval(200*time.Millisecond),
		withRebalancerTolerance(0.3),
	)
	re, err := newRebalancer(st, logger, mc, opts...)
	require.NoError(t, err)
	return re
}

func TestCollectTunnels(t *testing.T) {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	hc := &mockHAKeeperClient{}
	mc := clusterservice.NewMOCluster(hc, 3*time.Second)
	defer mc.Close()
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	defer st.Stop()
	ha := LabelHash("hash1")

	cn11 := &CNServer{
		hash: ha,
		reqLabel: newLabelInfo("t1", map[string]string{
			"k1": "v1",
			"k2": "v2",
		}),
		uuid: "cn11",
		addr: "127.0.0.1:38001",
	}
	hc.updateCN("cn11", cn11.addr, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k1":           {Labels: []string{"v1"}},
		"k2":           {Labels: []string{"v2"}},
	})

	cn12 := &CNServer{
		hash: ha,
		reqLabel: newLabelInfo("t1", map[string]string{
			"k1": "v1",
			"k2": "v2",
		}),
		uuid: "cn12",
		addr: "127.0.0.1:38002",
	}
	hc.updateCN("cn12", cn12.addr, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k1":           {Labels: []string{"v1"}},
		"k2":           {Labels: []string{"v2"}},
	})

	cn13 := &CNServer{
		hash: ha,
		reqLabel: newLabelInfo("t1", map[string]string{
			"k1": "v1",
			"k2": "v2",
		}),
		uuid: "cn13",
		addr: "127.0.0.1:38003",
	}
	hc.updateCN("cn13", cn13.addr, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k1":           {Labels: []string{"v1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	mc.ForceRefresh()
	time.Sleep(time.Millisecond * 200)

	t.Run("tolerance-0.1", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		re := testRebalancer(t, st, logger, mc)
		re.tolerance = 0.1
		tu1 := newTunnel(ctx, logger, nil)
		re.connManager.connect(cn11, tu1)
		tu2 := newTunnel(ctx, logger, nil)
		re.connManager.connect(cn11, tu2)
		tu3 := newTunnel(ctx, logger, nil)
		re.connManager.connect(cn11, tu3)
		tu4 := newTunnel(ctx, logger, nil)
		re.connManager.connect(cn11, tu4)
		tu5 := newTunnel(ctx, logger, nil)
		re.connManager.connect(cn12, tu5)
		require.Equal(t, 2, len(re.collectTunnels(ha)))
	})

	t.Run("tolerance-0.3", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		re := testRebalancer(t, st, logger, mc)
		re.tolerance = 0.3
		tu1 := newTunnel(ctx, logger, nil)
		re.connManager.connect(cn11, tu1)
		tu2 := newTunnel(ctx, logger, nil)
		re.connManager.connect(cn11, tu2)
		tu3 := newTunnel(ctx, logger, nil)
		re.connManager.connect(cn11, tu3)
		tu4 := newTunnel(ctx, logger, nil)
		re.connManager.connect(cn11, tu4)
		require.Equal(t, 2, len(re.collectTunnels(ha)))
	})

	t.Run("tolerance-0.8", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		re := testRebalancer(t, st, logger, mc)
		re.tolerance = 0.8
		tu1 := newTunnel(ctx, logger, nil)
		re.connManager.connect(cn11, tu1)
		tu2 := newTunnel(ctx, logger, nil)
		re.connManager.connect(cn11, tu2)
		tu3 := newTunnel(ctx, logger, nil)
		re.connManager.connect(cn11, tu3)
		tu4 := newTunnel(ctx, logger, nil)
		re.connManager.connect(cn11, tu4)
		require.Equal(t, 1, len(re.collectTunnels(ha)))
	})
}

func TestDoRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var err error
	tp := newTestProxyHandler(t)
	defer tp.closeFn()

	// Construct backend CN servers.
	cn11 := &CNServer{
		reqLabel: newLabelInfo("t1", map[string]string{
			"k1": "v1",
			"k2": "v2",
		}),
		uuid: "cn11",
		addr: "127.0.0.1:38001",
	}
	li := labelInfo{
		Tenant: "t1",
	}
	cn11.hash, err = li.getHash()
	require.NoError(t, err)
	tp.hc.updateCN("cn11", cn11.addr, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k1":           {Labels: []string{"v1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	stopFn11 := startTestCNServer(t, tp.ctx, cn11.addr)
	defer func() {
		require.NoError(t, stopFn11())
	}()

	cn12 := &CNServer{
		reqLabel: newLabelInfo("t1", map[string]string{
			"k1": "v1",
			"k2": "v2",
		}),
		uuid: "cn12",
		addr: "127.0.0.1:38002",
	}
	cn12.hash, err = li.getHash()
	require.NoError(t, err)
	tp.hc.updateCN("cn12", cn12.addr, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k1":           {Labels: []string{"v1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	stopFn12 := startTestCNServer(t, tp.ctx, cn12.addr)
	defer func() {
		require.NoError(t, stopFn12())
	}()
	tp.mc.ForceRefresh()
	time.Sleep(time.Millisecond * 200)

	ctx, cancel := context.WithTimeout(tp.ctx, 10*time.Second)
	defer cancel()

	// There 2 servers cn11 and cn12. 4 connections are all on cn11, and the
	// toleration is 0.3, so there will be 3 connections on cn11 and 1 connection
	// on cn12 at last.
	cleanup := testStartNClients(t, tp, li, cn11, 4)
	defer cleanup()

	tick := time.NewTicker(time.Millisecond * 200)
	for {
		select {
		case <-ctx.Done():
			require.Fail(t, "rebalance failed")
		case <-tick.C:
			tunnels := tp.re.connManager.getCNTunnels(cn11.hash)
			tp.re.connManager.Lock()
			if tunnels["cn11"].count() == 3 && tunnels["cn12"].count() == 1 {
				tp.re.connManager.Unlock()
				return
			}
			tp.re.connManager.Unlock()
		}
	}
}
