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
	"os"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

type mockSQLWorker struct{}

func newMockSQLWorker() *mockSQLWorker {
	return &mockSQLWorker{}
}

func (w *mockSQLWorker) GetCNServerByConnID(_ uint32) (*CNServer, error) {
	return nil, nil
}

func TestCNServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	temp := os.TempDir()

	t.Run("error", func(t *testing.T) {
		addr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
		require.NoError(t, os.RemoveAll(addr))
		cn := testMakeCNServer("", addr, 0, "", labelInfo{})
		c, err := cn.Connect(nil, 0)
		require.Error(t, err)
		require.Nil(t, c)
	})

	t.Run("success", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		addr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
		require.NoError(t, os.RemoveAll(addr))
		stopFn := startTestCNServer(t, ctx, addr, nil)
		defer func() {
			require.NoError(t, stopFn())
		}()
		cn := testMakeCNServer("", addr, 0, "", labelInfo{})
		c, err := cn.Connect(nil, 0)
		require.NoError(t, err)
		require.NotNil(t, c)
	})
}

func TestRouter_SelectEmptyCN(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	defer st.Stop()
	hc := &mockHAKeeperClient{}
	hc.updateCN("cn1", "", map[string]metadata.LabelList{})

	mc := clusterservice.NewMOCluster("", hc, 3*time.Second)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)
	mc.ForceRefresh(true)
	re := testRebalancer(t, st, logger, mc)

	ru := newRouter(mc, re, newMockSQLWorker(), true)

	li1 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
		},
	}
	cn, err := ru.Route(context.TODO(), "", clientInfo{labelInfo: li1}, nil)
	require.NoError(t, err)
	require.NotNil(t, cn)
}

func TestRouter_RouteForCommon(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
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

	mc := clusterservice.NewMOCluster("", hc, 3*time.Second)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)
	mc.ForceRefresh(true)
	re := testRebalancer(t, st, logger, mc)

	ru := newRouter(mc, re, newMockSQLWorker(), true)
	ctx := context.TODO()

	li1 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
			"k2": "v2",
		},
	}
	cn, err := ru.Route(ctx, "", clientInfo{labelInfo: li1}, nil)
	require.NoError(t, err)
	require.NotNil(t, cn)

	li2 := labelInfo{
		Tenant: "t2",
		Labels: map[string]string{
			"k1": "v1",
		},
	}
	cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li2}, nil)
	require.Error(t, err)
	require.Nil(t, cn)

	li3 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k2": "v1",
		},
	}
	cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li3}, nil)
	require.Error(t, err)
	require.Nil(t, cn)

	// empty tenant means sys tenant.
	li4 := labelInfo{
		Tenant: "",
		Labels: map[string]string{
			"k2": "v1",
		},
	}
	cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li4, username: "dump"}, nil)
	require.NoError(t, err)
	require.NotNil(t, cn)

	cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li4}, nil)
	require.Error(t, err)
	require.Nil(t, cn)

	li5 := labelInfo{
		Tenant: "sys",
		Labels: map[string]string{
			"k2": "v1",
		},
	}
	cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li5, username: "dump"}, nil)
	require.NoError(t, err)
	require.NotNil(t, cn)

	cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li5}, nil)
	require.Error(t, err)
	require.Nil(t, cn)
}

func TestRouter_RouteForSys(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	defer st.Stop()
	hc := &mockHAKeeperClient{}
	mc := clusterservice.NewMOCluster("", hc, 3*time.Second)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)
	re := testRebalancer(t, st, logger, mc)
	ru := newRouter(mc, re, newMockSQLWorker(), true)
	li1 := labelInfo{
		Tenant: "sys",
	}

	hc.updateCN("cn1", "", map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
	})
	mc.ForceRefresh(true)
	ctx := context.TODO()
	cn, err := ru.Route(ctx, "", clientInfo{labelInfo: li1, username: "dump"}, nil)
	require.NoError(t, err)
	require.NotNil(t, cn)
	require.Equal(t, "cn1", cn.uuid)

	cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li1}, nil)
	require.Error(t, err)
	require.Nil(t, cn)

	hc.updateCN("cn2", "", map[string]metadata.LabelList{})
	mc.ForceRefresh(true)
	cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li1}, nil)
	require.NoError(t, err)
	require.NotNil(t, cn)
	require.Equal(t, "cn2", cn.uuid)

	hc.updateCN("cn3", "", map[string]metadata.LabelList{
		"k1": {Labels: []string{"v1"}},
	})
	mc.ForceRefresh(true)
	cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li1}, nil)
	require.NoError(t, err)
	require.NotNil(t, cn)
	require.Equal(t, "cn3", cn.uuid)

	hc.updateCN("cn4", "", map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"sys"}},
	})
	mc.ForceRefresh(true)
	cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li1}, nil)
	require.NoError(t, err)
	require.NotNil(t, cn)
	require.Equal(t, "cn4", cn.uuid)
}

func TestRouter_SelectByConnID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	defer st.Stop()
	re := testRebalancer(t, st, logger, nil)

	temp := os.TempDir()
	addr1 := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr1))
	stopFn1 := startTestCNServer(t, ctx, addr1, nil)
	defer func() {
		require.NoError(t, stopFn1())
	}()
	ru := newRouter(nil, re, re.connManager, true)

	cn1 := testMakeCNServer("uuid1", addr1, 10, "", labelInfo{})
	_, _, err := ru.Connect(cn1, testPacket, nil)
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
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	defer st.Stop()
	hc := &mockHAKeeperClient{}
	// Construct backend CN servers.
	temp := os.TempDir()
	addr1 := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr1))
	hc.updateCN("cn1", addr1, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k1":           {Labels: []string{"v1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	stopFn1 := startTestCNServer(t, ctx, addr1, nil)
	defer func() {
		require.NoError(t, stopFn1())
	}()

	addr2 := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr2))
	hc.updateCN("cn2", addr2, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k1":           {Labels: []string{"v1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	stopFn2 := startTestCNServer(t, ctx, addr2, nil)
	defer func() {
		require.NoError(t, stopFn2())
	}()

	addr3 := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr3))
	hc.updateCN("cn3", addr3, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k1":           {Labels: []string{"v1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	stopFn3 := startTestCNServer(t, ctx, addr3, nil)
	defer func() {
		require.NoError(t, stopFn3())
	}()

	mc := clusterservice.NewMOCluster("", hc, 3*time.Second)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)
	mc.ForceRefresh(true)
	re := testRebalancer(t, st, logger, mc)

	ru := newRouter(mc, re, newMockSQLWorker(), true)

	connResult := make(map[string]struct{})
	li1 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
			"k2": "v2",
		},
	}
	cn, err := ru.Route(ctx, "", clientInfo{labelInfo: li1}, nil)
	require.NoError(t, err)
	require.NotNil(t, cn)
	cn.addr = "unix://" + cn.addr
	cn.salt = testSlat
	tu1 := newTunnel(context.TODO(), logger, nil)
	_, _, err = ru.Connect(cn, testPacket, tu1)
	require.NoError(t, err)
	connResult[cn.uuid] = struct{}{}

	li2 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
			"k2": "v2",
		},
	}
	cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li2}, nil)
	require.NoError(t, err)
	require.NotNil(t, cn)
	cn.addr = "unix://" + cn.addr
	cn.salt = testSlat
	tu2 := newTunnel(context.TODO(), logger, nil)
	_, _, err = ru.Connect(cn, testPacket, tu2)
	require.NoError(t, err)
	connResult[cn.uuid] = struct{}{}

	li3 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k1": "v1",
			"k2": "v2",
		},
	}
	cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li3}, nil)
	require.NoError(t, err)
	require.NotNil(t, cn)
	cn.addr = "unix://" + cn.addr
	cn.salt = testSlat
	tu3 := newTunnel(context.TODO(), logger, nil)
	_, _, err = ru.Connect(cn, testPacket, tu3)
	require.NoError(t, err)
	connResult[cn.uuid] = struct{}{}

	require.Equal(t, 3, len(connResult))
}

func TestRouter_ConnectAndSelectSpecify(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	defer st.Stop()
	hc := &mockHAKeeperClient{}
	// Construct backend CN servers.
	temp := os.TempDir()
	addr1 := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr1))
	hc.updateCN("cn1", addr1, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k1":           {Labels: []string{"v1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	stopFn1 := startTestCNServer(t, ctx, addr1, nil)
	defer func() {
		require.NoError(t, stopFn1())
	}()

	addr2 := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr2))
	hc.updateCN("cn2", addr2, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	stopFn2 := startTestCNServer(t, ctx, addr2, nil)
	defer func() {
		require.NoError(t, stopFn2())
	}()

	addr3 := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr3))
	hc.updateCN("cn3", addr3, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	stopFn3 := startTestCNServer(t, ctx, addr3, nil)
	defer func() {
		require.NoError(t, stopFn3())
	}()

	mc := clusterservice.NewMOCluster("", hc, 3*time.Second)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)
	mc.ForceRefresh(true)
	re := testRebalancer(t, st, logger, mc)

	ru := newRouter(mc, re, newMockSQLWorker(), true)

	connResult := make(map[string]struct{})
	li1 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k2": "v2",
		},
	}
	cn, err := ru.Route(ctx, "", clientInfo{labelInfo: li1}, nil)
	require.NoError(t, err)
	require.NotNil(t, cn)
	cn.addr = "unix://" + cn.addr
	cn.salt = testSlat
	tu1 := newTunnel(context.TODO(), logger, nil)
	_, _, err = ru.Connect(cn, testPacket, tu1)
	require.NoError(t, err)
	connResult[cn.uuid] = struct{}{}

	li2 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k2": "v2",
		},
	}
	cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li2}, nil)
	require.NoError(t, err)
	require.NotNil(t, cn)
	cn.addr = "unix://" + cn.addr
	cn.salt = testSlat
	tu2 := newTunnel(context.TODO(), logger, nil)
	_, _, err = ru.Connect(cn, testPacket, tu2)
	require.NoError(t, err)
	connResult[cn.uuid] = struct{}{}

	li3 := labelInfo{
		Tenant: "t1",
		Labels: map[string]string{
			"k2": "v2",
		},
	}
	cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li3}, nil)
	require.NoError(t, err)
	require.NotNil(t, cn)
	cn.addr = "unix://" + cn.addr
	cn.salt = testSlat
	tu3 := newTunnel(context.TODO(), logger, nil)
	_, _, err = ru.Connect(cn, testPacket, tu3)
	require.NoError(t, err)
	connResult[cn.uuid] = struct{}{}

	require.Equal(t, 2, len(connResult))
}

func TestRouter_Filter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	defer st.Stop()
	hc := &mockHAKeeperClient{}
	// Construct backend CN servers.
	temp := os.TempDir()
	addr1 := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr1))
	hc.updateCN("cn1", addr1, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k1":           {Labels: []string{"v1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	stopFn1 := startTestCNServer(t, ctx, addr1, nil)
	defer func() {
		require.NoError(t, stopFn1())
	}()

	addr2 := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr2))
	hc.updateCN("cn2", addr2, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
		"k2":           {Labels: []string{"v2"}},
	})
	stopFn2 := startTestCNServer(t, ctx, addr2, nil)
	defer func() {
		require.NoError(t, stopFn2())
	}()

	mc := clusterservice.NewMOCluster("", hc, 3*time.Second)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)
	mc.ForceRefresh(true)
	re := testRebalancer(t, st, logger, mc)

	ru := newRouter(mc, re, newMockSQLWorker(), true)

	badCNServers := make(map[string]struct{})
	badCNServers[addr2] = struct{}{}
	filterFn := func(str string) bool {
		if _, ok := badCNServers[str]; ok {
			return true
		}
		return false
	}
	cn, err := ru.Route(ctx, "", clientInfo{username: "dump"}, filterFn)
	require.NoError(t, err)
	require.NotNil(t, cn)
	require.Equal(t, cn.uuid, "cn1")
}

func TestRouter_RetryableConnect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	defer st.Stop()
	hc := &mockHAKeeperClient{}
	// Construct backend CN servers.
	temp := os.TempDir()
	addr1 := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr1))
	hc.updateCN("cn1", addr1, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
	})
	stopFn1 := startTestCNServer(t, ctx, addr1, nil)
	defer func() {
		require.NoError(t, stopFn1())
	}()

	addr2 := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr2))
	hc.updateCN("cn2", addr2, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
	})
	// We do NOT start cn2 here, to return a retryable error.

	addr3 := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr3))
	hc.updateCN("cn3", addr3, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
	})
	stopFn3 := startTestCNServer(t, ctx, addr3, nil, withBeforeHandle(func() {
		time.Sleep(defaultAuthTimeout/3 + time.Second)
	}))
	defer func() {
		require.NoError(t, stopFn3())
	}()

	mc := clusterservice.NewMOCluster("", hc, 3*time.Second)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)
	mc.ForceRefresh(true)
	re := testRebalancer(t, st, logger, mc)

	ru := newRouter(mc, re, newMockSQLWorker(), false)

	li1 := labelInfo{
		Tenant: "t1",
	}
	cn, err := ru.Route(ctx, "", clientInfo{labelInfo: li1}, func(s string) bool {
		// choose cn2
		return s == addr1 || s == addr3
	})
	require.NoError(t, err)
	require.NotNil(t, cn)
	cn.addr = "unix://" + cn.addr
	cn.salt = testSlat
	tu1 := newTunnel(context.TODO(), logger, nil)
	_, _, err = ru.Connect(cn, testPacket, tu1)
	require.True(t, isRetryableErr(err))

	cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li1}, func(s string) bool {
		// choose cn1
		return s == addr2 || s == addr3
	})
	require.NoError(t, err)
	require.NotNil(t, cn)
	cn.addr = "unix://" + cn.addr
	cn.salt = testSlat
	_, _, err = ru.Connect(cn, testPacket, tu1)
	require.NoError(t, err)
	require.Equal(t, "cn1", cn.uuid)

	// could not connect to cn3, because of timeout.
	cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li1}, func(s string) bool {
		// choose cn3
		return s == addr1 || s == addr2
	})
	require.NoError(t, err)
	require.NotNil(t, cn)
	cn.addr = "unix://" + cn.addr
	cn.salt = testSlat
	tu3 := newTunnel(context.TODO(), logger, nil)
	_, _, err = ru.Connect(cn, testPacket, tu3)
	require.True(t, isRetryableErr(err))
}
