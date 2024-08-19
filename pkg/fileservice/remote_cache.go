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

package fileservice

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

type TargetCacheKeys map[string][]*query.RequestCacheKey

type KeyRouterFactory[T comparable] func() client.KeyRouter[T]

// RemoteCache is the cache for remote read.
type RemoteCache struct {
	// client sends the cache request to query server.
	client client.QueryClient
	// keyRouterFactory is used to set the keyRouter async.
	keyRouterFactory KeyRouterFactory[query.CacheKey]
	// keyRouter is used to find out which node we should send
	// cache request to.
	keyRouter client.KeyRouter[query.CacheKey]
	// We only init the key router for the first time.
	init sync.Once
}

var _ IOVectorCache = new(RemoteCache)

func NewRemoteCache(client client.QueryClient, factory KeyRouterFactory[query.CacheKey]) *RemoteCache {
	return &RemoteCache{
		client:           client,
		keyRouterFactory: factory,
	}
}

func (r *RemoteCache) Read(ctx context.Context, vector *IOVector) error {
	if r.keyRouterFactory == nil {
		return nil
	}
	r.init.Do(func() {
		r.keyRouter = r.keyRouterFactory()
	})
	if r.keyRouter == nil {
		return nil
	}

	path, err := ParsePath(vector.FilePath)
	if err != nil {
		return err
	}

	var numHit, numRead int64
	defer func() {
		metric.FSReadHitRemoteCounter.Add(float64(numHit))
		perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
			c.FileService.Cache.Read.Add(numRead)
			c.FileService.Cache.Hit.Add(numHit)
			c.FileService.Cache.Remote.Read.Add(numRead)
			c.FileService.Cache.Remote.Hit.Add(numHit)
		})
	}()

	targetCacheKeys := make(TargetCacheKeys, len(vector.Entries))
	for i, entry := range vector.Entries {
		if entry.done {
			continue
		}
		if entry.Size < 0 {
			continue
		}

		cacheKey := CacheKey{
			Path:   path.File,
			Offset: entry.Offset,
			Sz:     entry.Size,
		}
		target := r.keyRouter.Target(cacheKey)
		if len(target) == 0 {
			continue
		}

		if _, ok := targetCacheKeys[target]; !ok {
			targetCacheKeys[target] = make([]*query.RequestCacheKey, 0)
		}
		targetCacheKeys[target] = append(targetCacheKeys[target], &query.RequestCacheKey{
			Index:    int32(i),
			CacheKey: &cacheKey,
		})
	}

	for target, key := range targetCacheKeys {
		req := r.client.NewRequest(query.CmdMethod_GetCacheData)
		req.GetCacheDataRequest = &query.GetCacheDataRequest{
			RequestCacheKey: key,
		}

		func(ctx context.Context) {
			ctx, cancel := context.WithTimeout(ctx, time.Second*2)
			defer cancel()
			resp, err := r.client.SendMessage(ctx, target, req)
			if err != nil {
				// Do not return error here to read data from local storage.
				return
			}
			defer r.client.Release(resp)
			if resp.GetCacheDataResponse != nil {
				for _, cacheData := range resp.GetCacheDataResponse.ResponseCacheData {
					numRead++
					idx := int(cacheData.Index)
					if cacheData.Hit {
						vector.Entries[idx].done = true
						vector.Entries[idx].CachedData = Bytes{bytes: cacheData.Data}
						vector.Entries[idx].fromCache = r
						numHit++
					}
				}
			}
		}(ctx)
	}
	return nil
}

func (r *RemoteCache) Update(ctx context.Context, vector *IOVector, async bool) error {
	return nil
}

func (r *RemoteCache) Flush() {}

func (r *RemoteCache) DeletePaths(ctx context.Context, paths []string) error {
	//TODO
	return nil
}

func (r *RemoteCache) Evict(done chan int64) {
}

func HandleRemoteRead(
	ctx context.Context, fs FileService, req *query.Request, resp *query.WrappedResponse,
) error {
	if req.GetCacheDataRequest == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	first := req.GetCacheDataRequest.RequestCacheKey[0].CacheKey
	if first == nil { // We cannot get the first one.
		return nil
	}

	ioVec := &IOVector{
		FilePath: first.Path,
	}
	ioVec.Entries = make([]IOEntry, len(req.GetCacheDataRequest.RequestCacheKey))
	for i, k := range req.GetCacheDataRequest.RequestCacheKey {
		ioVec.Entries[i].Offset = k.CacheKey.Offset
		ioVec.Entries[i].Size = k.CacheKey.Sz
	}
	if err := fs.ReadCache(ctx, ioVec); err != nil {
		return err
	}
	respData := make([]*query.ResponseCacheData, len(req.GetCacheDataRequest.RequestCacheKey))
	for i, k := range req.GetCacheDataRequest.RequestCacheKey {
		var data []byte
		if ioVec.Entries[i].CachedData != nil {
			data = ioVec.Entries[i].CachedData.Bytes()
		}
		respData[i] = &query.ResponseCacheData{
			Index: k.Index,
			Hit:   ioVec.Entries[i].fromCache != nil,
			Data:  data,
		}
	}

	resp.GetCacheDataResponse = &query.GetCacheDataResponse{
		ResponseCacheData: respData,
	}
	resp.ReleaseFunc = func() { ioVec.Release() }
	return nil
}
