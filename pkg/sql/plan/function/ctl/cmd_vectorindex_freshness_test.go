// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ctl

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// vecCtlQueryClient answers the vector-index-cache ctl commands with fixed per-CN values, and can
// be told to fail a specific address to exercise the best-effort / fail-on-first branches.
type vecCtlQueryClient struct {
	cachedPerCN  int64
	evictedPerCN int64
	keysPerCN    []string
	failAddr     string
}

func (c *vecCtlQueryClient) ServiceID() string { return "" }
func (c *vecCtlQueryClient) NewRequest(m query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: m}
}
func (c *vecCtlQueryClient) SendMessage(_ context.Context, addr string, req *query.Request) (*query.Response, error) {
	if addr == c.failAddr {
		return nil, moerr.NewInternalErrorNoCtx("send failed to " + addr)
	}
	switch req.CmdMethod {
	case query.CmdMethod_SetVectorIndexFreshnessInterval:
		return &query.Response{SetVectorIndexFreshnessInterval: query.SetVectorIndexFreshnessIntervalResponse{
			IntervalNs: req.SetVectorIndexFreshnessInterval.IntervalNs}}, nil
	case query.CmdMethod_GetVectorIndexCacheInfo:
		return &query.Response{GetVectorIndexCacheInfo: query.GetVectorIndexCacheInfoResponse{Count: c.cachedPerCN}}, nil
	case query.CmdMethod_EvictVectorIndexCache:
		return &query.Response{EvictVectorIndexCache: query.EvictVectorIndexCacheResponse{Evicted: c.evictedPerCN}}, nil
	case query.CmdMethod_GetVectorIndexCacheKeys:
		return &query.Response{GetVectorIndexCacheKeys: query.GetVectorIndexCacheKeysResponse{Keys: c.keysPerCN}}, nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unexpected query method")
}
func (c *vecCtlQueryClient) Release(*query.Response) {}
func (c *vecCtlQueryClient) Close() error            { return nil }

func vecCtlProc(t *testing.T, rt runtime.Runtime, qc *vecCtlQueryClient) *process.Process {
	cluster := clusterservice.NewMOCluster("", nil, time.Hour,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{
			{ServiceID: "cn1", QueryAddress: "addr1"},
			{ServiceID: "cn2", QueryAddress: "addr2"},
		}, nil))
	t.Cleanup(cluster.Close)
	rt.SetGlobalVariables(runtime.ClusterService, cluster)
	proc := &process.Process{Base: &process.BaseProcess{QueryClient: qc}}
	proc.Ctx = context.Background()
	return proc
}

func TestHandleSetVectorIndexFreshnessInterval(t *testing.T) {
	runtime.RunTest("", func(rt runtime.Runtime) {
		proc := vecCtlProc(t, rt, &vecCtlQueryClient{})

		// Applies on both CNs.
		ret, err := handleSetVectorIndexFreshnessInterval(proc, cn, "2s", nil)
		require.NoError(t, err)
		require.Equal(t, SetVectorIndexFreshnessIntervalMethod, ret.Method)
		require.Contains(t, ret.Data.(string), "applied=[addr1:2000000000, addr2:2000000000]")

		// 0 restores default; negative normalized to 0.
		ret, err = handleSetVectorIndexFreshnessInterval(proc, cn, "0", nil)
		require.NoError(t, err)
		require.Contains(t, ret.Data.(string), "addr1:0")

		// Best-effort: one CN fails -> still applied on the other, reported in failed=[...].
		proc2 := vecCtlProc(t, rt, &vecCtlQueryClient{failAddr: "addr2"})
		ret, err = handleSetVectorIndexFreshnessInterval(proc2, cn, "1s", nil)
		require.NoError(t, err)
		require.Contains(t, ret.Data.(string), "applied=[addr1:1000000000]")
		require.Contains(t, ret.Data.(string), "failed=[addr2")

		// Errors: bad duration, wrong service.
		_, err = handleSetVectorIndexFreshnessInterval(proc, cn, "bogus", nil)
		require.Error(t, err)
		_, err = handleSetVectorIndexFreshnessInterval(proc, tn, "1s", nil)
		require.Error(t, err)
	})
}

func TestHandleGetVectorIndexCacheInfo(t *testing.T) {
	runtime.RunTest("", func(rt runtime.Runtime) {
		proc := vecCtlProc(t, rt, &vecCtlQueryClient{cachedPerCN: 1})
		ret, err := handleGetVectorIndexCacheInfo(proc, cn, "idx", nil)
		require.NoError(t, err)
		require.Equal(t, GetVectorIndexCacheInfoMethod, ret.Method)
		require.Equal(t, vectorIndexCacheInfo{Cached: 2, CNs: 2}, ret.Data) // 1 per CN x 2 CNs

		_, err = handleGetVectorIndexCacheInfo(proc, tn, "idx", nil)
		require.Error(t, err)
	})
}

func TestHandleGetVectorIndexCacheKeys(t *testing.T) {
	runtime.RunTest("", func(rt runtime.Runtime) {
		// Each CN reports the same two keys; the handler unions them, counts CNs per key, and sorts.
		proc := vecCtlProc(t, rt, &vecCtlQueryClient{keysPerCN: []string{"ivf_tbl:7", "ft2_tbl"}})
		ret, err := handleGetVectorIndexCacheKeys(proc, cn, "", nil)
		require.NoError(t, err)
		require.Equal(t, GetVectorIndexCacheKeysMethod, ret.Method)
		require.Equal(t, vectorIndexCacheKeys{
			Keys: []vectorIndexCacheKeyEntry{{Key: "ft2_tbl", CNs: 2}, {Key: "ivf_tbl:7", CNs: 2}},
			CNs:  2,
		}, ret.Data)

		// Wrong service refused; a failing CN aborts.
		_, err = handleGetVectorIndexCacheKeys(proc, tn, "", nil)
		require.Error(t, err)
		proc2 := vecCtlProc(t, rt, &vecCtlQueryClient{failAddr: "addr1"})
		_, err = handleGetVectorIndexCacheKeys(proc2, cn, "", nil)
		require.Error(t, err)
	})
}

func TestHandleEvictVectorIndexCache(t *testing.T) {
	runtime.RunTest("", func(rt runtime.Runtime) {
		proc := vecCtlProc(t, rt, &vecCtlQueryClient{evictedPerCN: 1})
		ret, err := handleEvictVectorIndexCache(proc, cn, "idx", nil)
		require.NoError(t, err)
		require.Equal(t, EvictVectorIndexCacheMethod, ret.Method)
		require.Equal(t, vectorIndexEvictInfo{Evicted: 2, CNs: 2}, ret.Data)

		// Empty key refused; wrong service refused; a failing CN aborts (fail-on-first).
		_, err = handleEvictVectorIndexCache(proc, cn, "", nil)
		require.Error(t, err)
		_, err = handleEvictVectorIndexCache(proc, tn, "idx", nil)
		require.Error(t, err)
		proc2 := vecCtlProc(t, rt, &vecCtlQueryClient{failAddr: "addr1"})
		_, err = handleEvictVectorIndexCache(proc2, cn, "idx", nil)
		require.Error(t, err)
	})
}

// A positive interval below the floor would peg every CN with HouseKeeping/stale scans, so it is
// rejected; the floor itself and 0 (restore default) are accepted.
func TestHandleSetVectorIndexFreshnessIntervalRejectsTinyInterval(t *testing.T) {
	runtime.RunTest("", func(rt runtime.Runtime) {
		proc := vecCtlProc(t, rt, &vecCtlQueryClient{})
		_, err := handleSetVectorIndexFreshnessInterval(proc, cn, "1ns", nil)
		require.Error(t, err)
		_, err = handleSetVectorIndexFreshnessInterval(proc, cn, "999ms", nil)
		require.Error(t, err)
		_, err = handleSetVectorIndexFreshnessInterval(proc, cn, "1s", nil)
		require.NoError(t, err)
		_, err = handleSetVectorIndexFreshnessInterval(proc, cn, "0", nil)
		require.NoError(t, err)
	})
}
