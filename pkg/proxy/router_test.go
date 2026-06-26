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
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

const (
	workerModeReturnError int = 1
)

type mockSQLWorker struct {
	mod int
}

func newMockSQLWorker() *mockSQLWorker {
	return &mockSQLWorker{}
}

func (w *mockSQLWorker) GetCNServerByConnID(_ uint32) (*CNServer, error) {
	if w.mod == workerModeReturnError {
		return nil, moerr.NewInternalErrorNoCtx("return err")
	}
	return nil, nil
}

func TestCNServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	temp := os.TempDir()

	frontend.InitServerLevelVars("")
	frontend.SetSessionAlloc("", frontend.NewSessionAllocator(newTestPu()))

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
	frontend.InitServerLevelVars("")
	frontend.SetSessionAlloc("", frontend.NewSessionAllocator(newTestPu()))
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
	frontend.InitServerLevelVars(cn1.uuid)
	frontend.SetSessionAlloc(cn1.uuid, frontend.NewSessionAllocator(newTestPu()))
	sc1, _, err := ru.Connect(cn1, testPacket, nil)
	require.NoError(t, err)
	defer sc1.Close()

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
	frontend.InitServerLevelVars("")
	frontend.SetSessionAlloc("", frontend.NewSessionAllocator(newTestPu()))
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
	frontend.InitServerLevelVars(cn.uuid)
	frontend.SetSessionAlloc(cn.uuid, frontend.NewSessionAllocator(newTestPu()))
	tu1 := newTunnel(context.TODO(), logger, nil)
	sc1, _, err := ru.Connect(cn, testPacket, tu1)
	require.NoError(t, err)
	defer sc1.Close()
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
	frontend.InitServerLevelVars(cn.uuid)
	frontend.SetSessionAlloc(cn.uuid, frontend.NewSessionAllocator(newTestPu()))
	tu2 := newTunnel(context.TODO(), logger, nil)
	sc2, _, err := ru.Connect(cn, testPacket, tu2)
	require.NoError(t, err)
	defer sc2.Close()
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
	frontend.InitServerLevelVars(cn.uuid)
	frontend.SetSessionAlloc(cn.uuid, frontend.NewSessionAllocator(newTestPu()))
	tu3 := newTunnel(context.TODO(), logger, nil)
	sc3, _, err := ru.Connect(cn, testPacket, tu3)
	require.NoError(t, err)
	defer sc3.Close()
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
	sc1, _, err := ru.Connect(cn, testPacket, tu1)
	require.NoError(t, err)
	defer sc1.Close()
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
	sc2, _, err := ru.Connect(cn, testPacket, tu2)
	require.NoError(t, err)
	defer sc2.Close()
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
	sc3, _, err := ru.Connect(cn, testPacket, tu3)
	require.NoError(t, err)
	defer sc3.Close()
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
	sc1, _, err := ru.Connect(cn, testPacket, tu1)
	require.NoError(t, err)
	defer sc1.Close()
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

func Test_router(t *testing.T) {
	rt := &router{
		sqlRouter: &mockSQLWorker{
			mod: workerModeReturnError,
		},
	}
	_, err := rt.SelectByConnID(123)
	require.Error(t, err)
}

func TestRouter_CNHealthProbeWindowOption(t *testing.T) {
	ru := newRouter(nil, nil, newMockSQLWorker(), true,
		withCNHealthCheckProbeWindow(time.Second*42),
	).(*router)
	require.NotNil(t, ru.health)
	require.Equal(t, time.Second*42, ru.health.probeWindow)
}

// TestRouteWithCNHealthChecker verifies that the router skips CN servers whose
// health breaker is tripped, and fast-fails with allCNServersBusyErr (rather
// than noCNServerErr) when every candidate CN is temporarily unhealthy.
func TestRouteWithCNHealthChecker(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	defer st.Stop()
	hc := &mockHAKeeperClient{}
	for _, uuid := range []string{"cn1", "cn2", "cn3"} {
		hc.updateCN(uuid, uuid+"-addr", map[string]metadata.LabelList{
			tenantLabelKey: {Labels: []string{"t1"}},
		})
	}

	mc := clusterservice.NewMOCluster("", hc, 3*time.Second)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)
	mc.ForceRefresh(true)
	re := testRebalancer(t, st, logger, mc)

	ru := newRouter(mc, re, newMockSQLWorker(), true).(*router)
	// Inject a controllable clock into the breaker. failThreshold=1 keeps the
	// test focused (a single reported failure trips the CN).
	clock := newFakeClock()
	ru.health = newCNHealthChecker(
		withCNHealthClock(clock.Now),
		withCNHealthFailThreshold(1),
		withCNHealthCooldown(time.Second*5, time.Second*30),
	)

	ci := clientInfo{labelInfo: labelInfo{Tenant: "t1"}}

	// With all CNs healthy, routing succeeds.
	cn, err := ru.Route(context.TODO(), "", ci, nil)
	require.NoError(t, err)
	require.NotNil(t, cn)

	// Trip cn1 and cn2; cn3 must always be the one returned.
	ru.health.reportFailure("cn1", "cn1-addr")
	ru.health.reportFailure("cn2", "cn2-addr")
	for i := 0; i < 10; i++ {
		cn, err = ru.Route(context.TODO(), "", ci, nil)
		require.NoError(t, err)
		require.NotNil(t, cn)
		require.Equal(t, "cn3", cn.uuid, "unhealthy CNs must be skipped")
	}

	// Trip cn3 as well: every candidate is now in active cooldown, so the
	// router fast-fails with the busy error instead of "no available CN".
	ru.health.reportFailure("cn3", "cn3-addr")
	cn, err = ru.Route(context.TODO(), "", ci, nil)
	require.Nil(t, cn)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrAllCNServersBusy),
		"expected ErrAllCNServersBusy, got: %v", err)

	// After the cooldown expires, a half-open probe is allowed through so the
	// cluster can recover automatically.
	clock.advance(time.Second * 5)
	cn, err = ru.Route(context.TODO(), "", ci, nil)
	require.NoError(t, err)
	require.NotNil(t, cn)

	// A successful connect on that probe restores the CN to healthy.
	ru.health.reportSuccess(cn.uuid, cn.addr)
	require.Equal(t, 2, ru.health.unhealthyCount())
}

// TestRouteConnectTripsBreakerEndToEnd verifies the full chain: a real
// router.Connect() failure must feed back into the breaker (reportFailure),
// trip the CN, and cause the next Route() to skip it. This guards against the
// breaker being correct in isolation while the Connect->reportFailure wiring is
// missing or misplaced.
func TestRouteConnectTripsBreakerEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	defer st.Stop()
	hc := &mockHAKeeperClient{}

	temp := os.TempDir()
	// cnBad accepts the TCP connect but hangs in handshake long enough to
	// trigger the auth timeout, which is exactly the busy/overload path the
	// breaker is meant to remember globally.
	badAddr := fmt.Sprintf("%s/%d-bad.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(badAddr))
	hc.updateCN("cnBad", badAddr, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
	})
	stopBad := startTestCNServer(t, ctx, badAddr, nil, withBeforeHandle(func() {
		time.Sleep(time.Second * 2)
	}))
	defer func() { require.NoError(t, stopBad()) }()
	// cnGood is started so it is a healthy alternative.
	goodAddr := fmt.Sprintf("%s/%d-good.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(goodAddr))
	hc.updateCN("cnGood", goodAddr, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
	})
	stopGood := startTestCNServer(t, ctx, goodAddr, nil)
	defer func() { require.NoError(t, stopGood()) }()

	mc := clusterservice.NewMOCluster("", hc, 3*time.Second)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)
	mc.ForceRefresh(true)
	re := testRebalancer(t, st, logger, mc)

	// Real router (test=false) so Connect actually dials the backend. Trip on
	// the first failure to keep the test deterministic.
	ru := newRouter(mc, re, newMockSQLWorker(), false,
		withCNHealthCheckFailThreshold(1),
		withAuthTimeout(time.Second),
	).(*router)

	li := labelInfo{Tenant: "t1"}

	// Force selection of cnBad and connect to it; the real Route-selected
	// connect must fail and trip the breaker.
	cn, err := ru.Route(ctx, "", clientInfo{labelInfo: li}, func(s string) bool {
		return s == goodAddr // exclude cnGood
	})
	require.NoError(t, err)
	require.Equal(t, "cnBad", cn.uuid)
	cn.addr = "unix://" + cn.addr
	cn.salt = testSlat
	tu := newTunnel(context.TODO(), logger, nil)
	rr, ok := any(ru).(routeSelectedConnector)
	require.True(t, ok)
	_, _, err = rr.ConnectRouteSelected(cn, testPacket, tu)
	require.Error(t, err)
	require.True(t, isRetryableErr(err))
	require.True(t, isTimeoutErr(err))

	// The end-to-end feedback must have tripped cnBad's breaker.
	require.Equal(t, 1, ru.health.unhealthyCount(),
		"a real Connect failure must trip the breaker")

	// The next Route (no filter) must skip the tripped cnBad and pick cnGood.
	for i := 0; i < 5; i++ {
		cn, err = ru.Route(ctx, "", clientInfo{labelInfo: li}, nil)
		require.NoError(t, err)
		require.Equal(t, "cnGood", cn.uuid, "tripped CN must be skipped after a real failure")
	}
}

// TestRouteConnectHardFailureDoesNotTripBreaker verifies that hard
// unavailability signals (e.g. ECONNREFUSED / connectErr without timeout) do
// not globally trip the CN health breaker or get reclassified as "busy".
func TestRouteConnectHardFailureDoesNotTripBreaker(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	defer st.Stop()
	hc := &mockHAKeeperClient{}

	temp := os.TempDir()
	badAddr := fmt.Sprintf("%s/%d-hardbad.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(badAddr))
	hc.updateCN("cnBad", badAddr, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
	})
	goodAddr := fmt.Sprintf("%s/%d-hardgood.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(goodAddr))
	hc.updateCN("cnGood", goodAddr, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{"t1"}},
	})
	stopGood := startTestCNServer(t, ctx, goodAddr, nil)
	defer func() { require.NoError(t, stopGood()) }()

	mc := clusterservice.NewMOCluster("", hc, 3*time.Second)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)
	mc.ForceRefresh(true)
	re := testRebalancer(t, st, logger, mc)

	ru := newRouter(mc, re, newMockSQLWorker(), false,
		withCNHealthCheckFailThreshold(1),
	).(*router)

	li := labelInfo{Tenant: "t1"}
	cn, err := ru.Route(ctx, "", clientInfo{labelInfo: li}, func(s string) bool {
		return s == goodAddr // exclude cnGood
	})
	require.NoError(t, err)
	require.Equal(t, "cnBad", cn.uuid)
	cn.addr = "unix://" + cn.addr
	cn.salt = testSlat
	tu := newTunnel(context.TODO(), logger, nil)
	rr, ok := any(ru).(routeSelectedConnector)
	require.True(t, ok)
	_, _, err = rr.ConnectRouteSelected(cn, testPacket, tu)
	require.Error(t, err)
	require.True(t, isRetryableErr(err))
	require.False(t, isTimeoutErr(err))

	// Hard failures must not trip the global breaker.
	require.Equal(t, 0, ru.health.unhealthyCount())
}
