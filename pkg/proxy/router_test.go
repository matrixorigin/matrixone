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
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestCNServer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("error", func(t *testing.T) {
		cn := &CNServer{
			addr: "127.0.0.1:38010",
		}
		c, err := cn.Connect()
		require.Error(t, err)
		require.Nil(t, c)
	})

	t.Run("success", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		addr := "127.0.0.1:38020"
		stopFn := startTestCNServer(t, ctx, addr)
		defer func() {
			require.NoError(t, stopFn())
		}()
		cn := &CNServer{
			addr: "127.0.0.1:38020",
		}
		c, err := cn.Connect()
		require.NoError(t, err)
		require.NotNil(t, c)
	})
}

func TestRouter_SelectEmptyCN(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	defer st.Stop()
	hc := &mockHAKeeperClient{}
	hc.updateCN("cn1", "", map[string]metadata.LabelList{})

	mc := clusterservice.NewMOCluster(hc, 3*time.Second)
	defer mc.Close()
	mc.ForceRefresh()
	time.Sleep(time.Millisecond * 200)
	re := testRebalancer(t, st, logger, mc)

	ru := newRouter(mc, re, true)

	li1 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
		},
	}
	cn, err := ru.SelectByLabel(li1)
	require.NoError(t, err)
	require.NotNil(t, cn)
}

func TestRouter_SelectByLabel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	defer st.Stop()
	hc := &mockHAKeeperClient{}
	// Construct backend CN servers.
	hc.updateCN("cn1", "", map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k1":           {Labels: []string{"v1"}},
		"k2":           {Labels: []string{"v2"}},
	})

	mc := clusterservice.NewMOCluster(hc, 3*time.Second)
	defer mc.Close()
	mc.ForceRefresh()
	time.Sleep(time.Millisecond * 200)
	re := testRebalancer(t, st, logger, mc)

	ru := newRouter(mc, re, true)

	li1 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
		},
	}
	cn, err := ru.SelectByLabel(li1)
	require.NoError(t, err)
	require.NotNil(t, cn)

	li2 := labelInfo{
		Tenant: "t2",
		Labels: map[string]string{
			"k1": "v1",
		},
	}
	cn, err = ru.SelectByLabel(li2)
	require.Error(t, err)
	require.Nil(t, cn)

	li3 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k2": "v1",
		},
	}
	cn, err = ru.SelectByLabel(li3)
	require.Error(t, err)
	require.Nil(t, cn)
}

func TestRouter_SelectByConnID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	defer st.Stop()
	re := testRebalancer(t, st, logger, nil)

	addr1 := "127.0.0.1:38001"
	stopFn1 := startTestCNServer(t, ctx, addr1)
	defer func() {
		require.NoError(t, stopFn1())
	}()
	ru := newRouter(nil, re, true)

	cn1 := &CNServer{
		connID: 10,
		addr:   addr1,
		uuid:   "uuid1",
	}
	_, _, err := ru.Connect(cn1, nil, nil)
	require.NoError(t, err)

	cn2, err := ru.SelectByConnID(10)
	require.NoError(t, err)
	require.NotNil(t, cn2)
	require.Equal(t, cn1.uuid, cn2.uuid)
	require.Equal(t, cn1.addr, cn2.addr)

	cn3, err := ru.SelectByConnID(20)
	require.Error(t, err)
	require.Nil(t, cn3)
}

func TestRouter_ConnectAndSelectBalanced(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	defer st.Stop()
	hc := &mockHAKeeperClient{}
	// Construct backend CN servers.
	addr1 := "127.0.0.1:38001"
	hc.updateCN("cn1", addr1, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k1":           {Labels: []string{"v1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	stopFn1 := startTestCNServer(t, ctx, addr1)
	defer func() {
		require.NoError(t, stopFn1())
	}()

	addr2 := "127.0.0.1:38002"
	hc.updateCN("cn2", addr2, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k1":           {Labels: []string{"v1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	stopFn2 := startTestCNServer(t, ctx, addr2)
	defer func() {
		require.NoError(t, stopFn2())
	}()

	addr3 := "127.0.0.1:38003"
	hc.updateCN("cn3", addr3, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k1":           {Labels: []string{"v1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	stopFn3 := startTestCNServer(t, ctx, addr3)
	defer func() {
		require.NoError(t, stopFn3())
	}()

	mc := clusterservice.NewMOCluster(hc, 3*time.Second)
	defer mc.Close()
	mc.ForceRefresh()
	time.Sleep(time.Millisecond * 200)
	re := testRebalancer(t, st, logger, mc)

	ru := newRouter(mc, re, true)

	connResult := make(map[string]struct{})
	li1 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
		},
	}
	cn, err := ru.SelectByLabel(li1)
	require.NoError(t, err)
	require.NotNil(t, cn)
	tu1 := newTunnel(context.TODO(), nil)
	_, _, err = ru.Connect(cn, nil, tu1)
	require.NoError(t, err)
	connResult[cn.uuid] = struct{}{}

	li2 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
		},
	}
	cn, err = ru.SelectByLabel(li2)
	require.NoError(t, err)
	require.NotNil(t, cn)
	tu2 := newTunnel(context.TODO(), nil)
	_, _, err = ru.Connect(cn, nil, tu2)
	require.NoError(t, err)
	connResult[cn.uuid] = struct{}{}

	li3 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
		},
	}
	cn, err = ru.SelectByLabel(li3)
	require.NoError(t, err)
	require.NotNil(t, cn)
	tu3 := newTunnel(context.TODO(), nil)
	_, _, err = ru.Connect(cn, nil, tu3)
	require.NoError(t, err)
	connResult[cn.uuid] = struct{}{}

	require.Equal(t, 3, len(connResult))
}

func TestRouter_ConnectAndSelectSpecify(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	defer st.Stop()
	hc := &mockHAKeeperClient{}
	// Construct backend CN servers.
	addr1 := "127.0.0.1:38001"
	hc.updateCN("cn1", addr1, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k1":           {Labels: []string{"v1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	stopFn1 := startTestCNServer(t, ctx, addr1)
	defer func() {
		require.NoError(t, stopFn1())
	}()

	addr2 := "127.0.0.1:38002"
	hc.updateCN("cn2", addr2, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	stopFn2 := startTestCNServer(t, ctx, addr2)
	defer func() {
		require.NoError(t, stopFn2())
	}()

	addr3 := "127.0.0.1:38003"
	hc.updateCN("cn3", addr3, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	stopFn3 := startTestCNServer(t, ctx, addr3)
	defer func() {
		require.NoError(t, stopFn3())
	}()

	mc := clusterservice.NewMOCluster(hc, 3*time.Second)
	defer mc.Close()
	mc.ForceRefresh()
	time.Sleep(time.Millisecond * 200)
	re := testRebalancer(t, st, logger, mc)

	ru := newRouter(mc, re, true)

	connResult := make(map[string]struct{})
	li1 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
		},
	}
	cn, err := ru.SelectByLabel(li1)
	require.NoError(t, err)
	require.NotNil(t, cn)
	tu1 := newTunnel(context.TODO(), nil)
	_, _, err = ru.Connect(cn, nil, tu1)
	require.NoError(t, err)
	connResult[cn.uuid] = struct{}{}

	li2 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
		},
	}
	cn, err = ru.SelectByLabel(li2)
	require.NoError(t, err)
	require.NotNil(t, cn)
	tu2 := newTunnel(context.TODO(), nil)
	_, _, err = ru.Connect(cn, nil, tu2)
	require.NoError(t, err)
	connResult[cn.uuid] = struct{}{}

	li3 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
		},
	}
	cn, err = ru.SelectByLabel(li3)
	require.NoError(t, err)
	require.NotNil(t, cn)
	tu3 := newTunnel(context.TODO(), nil)
	_, _, err = ru.Connect(cn, nil, tu3)
	require.NoError(t, err)
	connResult[cn.uuid] = struct{}{}

	require.Equal(t, 1, len(connResult))
}
