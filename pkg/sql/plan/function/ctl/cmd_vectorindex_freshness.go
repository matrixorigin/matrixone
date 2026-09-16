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
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// handleSetVectorIndexFreshnessInterval overrides the vector/fulltext2 index cache's cross-CN
// freshness (IsStale) sweep cadence on every CN. It is a sys-admin-only knob (mo_ctl is gated to
// moadmin) intended for tests/ops that need read-your-writes to converge fast in multi-CN; the
// default (~10 min) is unchanged and a value of 0 restores it.
//
//	mo_ctl("cn", "SetVectorIndexFreshnessInterval", "<duration>")
//
// The parameter is a Go duration ("2s", "500ms") or "0" to restore the default. It fans out to
// every CN (mirrors handleSyncCommit) so whichever CN serves the query and whichever hosts the CDC
// consumer both sweep at the new cadence. The change takes effect immediately (the receiving CN
// re-arms its cache ticker); it is process-local and not persisted, so a CN restart reverts.
func handleSetVectorIndexFreshnessInterval(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender,
) (Result, error) {
	if service != cn {
		return Result{}, moerr.NewInternalError(proc.Ctx, "SetVectorIndexFreshnessInterval only supports cn")
	}
	d, err := time.ParseDuration(strings.TrimSpace(parameter))
	if err != nil {
		return Result{}, moerr.NewInternalErrorf(proc.Ctx,
			"invalid duration %q (want a Go duration like 2s, or 0 to restore default): %v", parameter, err)
	}
	if d < 0 {
		d = 0
	}

	qt := proc.GetQueryClient()
	mc := clusterservice.GetMOCluster(proc.GetService())
	var addrs []string
	mc.GetCNService(clusterservice.NewSelector(), func(c metadata.CNService) bool {
		if c.QueryAddress != "" {
			addrs = append(addrs, c.QueryAddress)
		}
		return true
	})
	if len(addrs) == 0 {
		return Result{}, moerr.NewInternalError(proc.Ctx,
			"SetVectorIndexFreshnessInterval: no CN with a query address to apply to")
	}

	// Best-effort broadcast: this knob is meant to reach EVERY CN, so apply it wherever the RPC
	// succeeds and report the rest, rather than aborting after a partial rollout. Aborting on the
	// first error would leave CNs at inconsistent cadences AND hard-fail the whole call for a
	// single transiently-unreachable or old-binary CN (one that lacks the handler) -- surprising
	// for an idempotent, non-persisted override. Only a total failure (no CN applied) is an error.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	applied := make([]string, 0, len(addrs))
	failed := make([]string, 0)
	for _, addr := range addrs {
		req := qt.NewRequest(querypb.CmdMethod_SetVectorIndexFreshnessInterval)
		req.SetVectorIndexFreshnessInterval = querypb.SetVectorIndexFreshnessIntervalRequest{IntervalNs: int64(d)}
		resp, err := qt.SendMessage(ctx, addr, req)
		if err != nil {
			failed = append(failed, fmt.Sprintf("%s(%v)", addr, err))
			continue
		}
		applied = append(applied, fmt.Sprintf("%s:%d", addr, resp.SetVectorIndexFreshnessInterval.IntervalNs))
		qt.Release(resp)
	}
	if len(applied) == 0 {
		return Result{}, moerr.NewInternalErrorf(proc.Ctx,
			"SetVectorIndexFreshnessInterval failed on all CNs: %s", strings.Join(failed, "; "))
	}

	data := "applied=[" + strings.Join(applied, ", ") + "]"
	if len(failed) > 0 {
		data += " failed=[" + strings.Join(failed, "; ") + "]"
	}
	return Result{
		Method: SetVectorIndexFreshnessIntervalMethod,
		Data:   data,
	}, nil
}

// handleGetVectorIndexCacheInfo reports the TOTAL number of vector/fulltext2 index cache entries
// held across all CNs for a given index key, so a test/operator can observe cross-CN eviction
// deterministically (0 = evicted everywhere) instead of guessing a sleep. Read-only.
//
//	mo_ctl("cn", "GetVectorIndexCacheInfo", "<index hidden table name>")   // empty = all entries
//
// The Result's numeric total is JSON-addressable as $.result, so a BVT can poll it with @wait_expect
// (e.g. json_extract(mo_ctl(...), '$.result') until 0).
func handleGetVectorIndexCacheInfo(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender,
) (Result, error) {
	if service != cn {
		return Result{}, moerr.NewInternalError(proc.Ctx, "GetVectorIndexCacheInfo only supports cn")
	}
	key := strings.TrimSpace(parameter)

	qt := proc.GetQueryClient()
	mc := clusterservice.GetMOCluster(proc.GetService())
	var addrs []string
	mc.GetCNService(clusterservice.NewSelector(), func(c metadata.CNService) bool {
		if c.QueryAddress != "" {
			addrs = append(addrs, c.QueryAddress)
		}
		return true
	})
	if len(addrs) == 0 {
		return Result{}, moerr.NewInternalError(proc.Ctx,
			"GetVectorIndexCacheInfo: no CN with a query address to query")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	var total int64
	for _, addr := range addrs {
		req := qt.NewRequest(querypb.CmdMethod_GetVectorIndexCacheInfo)
		req.GetVectorIndexCacheInfo = querypb.GetVectorIndexCacheInfoRequest{Key: key}
		resp, err := qt.SendMessage(ctx, addr, req)
		if err != nil {
			return Result{}, err
		}
		total += resp.GetVectorIndexCacheInfo.Count
		qt.Release(resp)
	}

	// result carries both the cached total and the number of CNs it was summed over, so a caller can
	// tell "cached=0 across N CNs" (evicted everywhere) from a single-CN reading. Poll $.result.cached.
	return Result{
		Method: GetVectorIndexCacheInfoMethod,
		Data: vectorIndexCacheInfo{
			Cached: total,
			CNs:    len(addrs),
		},
	}, nil
}

// vectorIndexCacheInfo is the GetVectorIndexCacheInfo Result payload: Cached is the total cached
// entries for the key summed across CNs; CNs is how many CNs were queried (the denominator).
type vectorIndexCacheInfo struct {
	Cached int64 `json:"cached"`
	CNs    int   `json:"cns"`
}

// handleEvictVectorIndexCache drops a specific index's vector/fulltext2 cache entries on EVERY CN
// (synchronous), so the next query reloads a current generation. Sys-admin/test tool.
//
//	mo_ctl("cn", "EvictVectorIndexCache", "<index hidden table name>")
//
// Unlike SetVectorIndexFreshnessInterval this is fail-on-first-error: an eviction that only reached
// some CNs is not "done", so the caller must see the failure rather than a misleading success.
func handleEvictVectorIndexCache(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender,
) (Result, error) {
	if service != cn {
		return Result{}, moerr.NewInternalError(proc.Ctx, "EvictVectorIndexCache only supports cn")
	}
	key := strings.TrimSpace(parameter)
	if key == "" {
		return Result{}, moerr.NewInternalError(proc.Ctx, "EvictVectorIndexCache: an index key is required")
	}

	qt := proc.GetQueryClient()
	mc := clusterservice.GetMOCluster(proc.GetService())
	var addrs []string
	mc.GetCNService(clusterservice.NewSelector(), func(c metadata.CNService) bool {
		if c.QueryAddress != "" {
			addrs = append(addrs, c.QueryAddress)
		}
		return true
	})
	if len(addrs) == 0 {
		return Result{}, moerr.NewInternalError(proc.Ctx,
			"EvictVectorIndexCache: no CN with a query address to evict on")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	var evicted int64
	for _, addr := range addrs {
		req := qt.NewRequest(querypb.CmdMethod_EvictVectorIndexCache)
		req.EvictVectorIndexCache = querypb.EvictVectorIndexCacheRequest{Key: key}
		resp, err := qt.SendMessage(ctx, addr, req)
		if err != nil {
			return Result{}, err
		}
		evicted += resp.EvictVectorIndexCache.Evicted
		qt.Release(resp)
	}

	return Result{
		Method: EvictVectorIndexCacheMethod,
		Data: vectorIndexEvictInfo{
			Evicted: evicted,
			CNs:     len(addrs),
		},
	}, nil
}

// vectorIndexEvictInfo is the EvictVectorIndexCache Result payload: Evicted is the total entries
// dropped for the key summed across CNs; CNs is how many CNs were evicted on.
type vectorIndexEvictInfo struct {
	Evicted int64 `json:"evicted"`
	CNs     int   `json:"cns"`
}
