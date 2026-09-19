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

package queryservice

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
)

func (s *queryService) handleGetProtocolVersion() func(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	return func(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
		if req.GetProtocolVersion == nil {
			return moerr.NewInternalError(ctx, "bad request")
		}
		version, ok := runtime.ServiceRuntime(s.serviceID).GetGlobalVariables(runtime.MOProtocolVersion)
		if !ok {
			resp.WrapError(moerr.NewInternalError(ctx, "protocol version not found"))
			return nil
		}
		resp.GetProtocolVersion = &query.GetProtocolVersionResponse{
			Version: version.(int64),
		}
		return nil
	}
}

func (s *queryService) handleSetProtocolVersion() func(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	return func(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
		if req.SetProtocolVersion == nil {
			return moerr.NewInternalError(ctx, "bad request")
		}
		runtime.ServiceRuntime(s.serviceID).SetGlobalVariables(runtime.MOProtocolVersion, req.SetProtocolVersion.Version)
		resp.SetProtocolVersion = &query.SetProtocolVersionResponse{
			Version: req.SetProtocolVersion.Version,
		}
		return nil
	}
}

// handleSetVectorIndexFreshnessInterval overrides this CN's vector/fulltext2 index cache cross-CN
// freshness sweep cadence. IntervalNs<=0 restores the default. Sys-admin/test knob (mo_ctl),
// process-local and not persisted.
func (s *queryService) handleSetVectorIndexFreshnessInterval() func(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	return func(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
		ns := req.SetVectorIndexFreshnessInterval.IntervalNs
		runtime.SetVectorIndexStaleCheckInterval(time.Duration(ns))
		resp.SetVectorIndexFreshnessInterval = query.SetVectorIndexFreshnessIntervalResponse{IntervalNs: ns}
		return nil
	}
}

// handleGetVectorIndexCacheInfo reports how many vector/fulltext2 index cache entries this CN holds
// for the requested key (0 = not cached). Read-only introspection (mo_ctl GetVectorIndexCacheInfo).
func (s *queryService) handleGetVectorIndexCacheInfo() func(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	return func(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
		n, _ := runtime.VectorIndexCacheCountKey(req.GetVectorIndexCacheInfo.Key)
		resp.GetVectorIndexCacheInfo = query.GetVectorIndexCacheInfoResponse{Count: n}
		return nil
	}
}

// handleEvictVectorIndexCache drops this CN's vector/fulltext2 index cache entries for the key and
// reports how many were dropped (mo_ctl EvictVectorIndexCache).
func (s *queryService) handleEvictVectorIndexCache() func(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	return func(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
		n, _ := runtime.EvictVectorIndexCache(req.EvictVectorIndexCache.Key)
		resp.EvictVectorIndexCache = query.EvictVectorIndexCacheResponse{Evicted: n}
		return nil
	}
}

// handleGetVectorIndexCacheKeys lists the exact cache keys this CN currently holds (mo_ctl
// GetVectorIndexCacheKeys).
func (s *queryService) handleGetVectorIndexCacheKeys() func(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	return func(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
		keys, _ := runtime.VectorIndexCacheKeys()
		resp.GetVectorIndexCacheKeys = query.GetVectorIndexCacheKeysResponse{Keys: keys}
		return nil
	}
}
