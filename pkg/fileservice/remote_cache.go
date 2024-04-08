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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cacheservice/client"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/cache"
	gpb "github.com/matrixorigin/matrixone/pkg/pb/gossip"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

// KeyRouter is an interface manages the remote cache information.
type KeyRouter interface {
	// Target returns the remote cache server service address of
	// the cache key. If the cache do not exist in any node, it
	// returns empty string.
	Target(k CacheKey) string

	// AddItem pushes a cache key item into a queue with a local
	// cache server service address in the item. Gossip will take
	// all the items and send them to other nodes in gossip cluster.
	AddItem(key CacheKey, operation gpb.Operation)
}

type TargetCacheKeys map[string][]*pb.RequestCacheKey

type KeyRouterFactory func() KeyRouter

// RemoteCache is the cache for remote read.
type RemoteCache struct {
	// cacheClient sends the cache request to cache server.
	cacheClient client.CacheClient
	// keyRouterFactory is used to set the keyRouter async.
	keyRouterFactory KeyRouterFactory
	// keyRouter is used to find out which node we should send
	// cache request to.
	keyRouter KeyRouter
	// We only init the key router for the first time.
	init sync.Once
}

var _ IOVectorCache = new(RemoteCache)

func NewRemoteCache(client client.CacheClient, factory KeyRouterFactory) *RemoteCache {
	return &RemoteCache{
		cacheClient:      client,
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
		v2.FSReadHitRemoteCounter.Add(float64(numHit))
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
			targetCacheKeys[target] = make([]*pb.RequestCacheKey, 0)
		}
		targetCacheKeys[target] = append(targetCacheKeys[target], &pb.RequestCacheKey{
			Index:    int32(i),
			CacheKey: &cacheKey,
		})
	}

	for target, key := range targetCacheKeys {
		req := r.cacheClient.NewRequest(pb.CmdMethod_RemoteRead)
		req.RemoteReadRequest = &pb.RemoteReadRequest{
			RequestCacheKey: key,
		}

		func(ctx context.Context) {
			ctx, cancel := context.WithTimeout(ctx, time.Second*2)
			defer cancel()
			resp, err := r.cacheClient.SendMessage(ctx, target, req)
			if err != nil {
				// Do not return error here to read data from local storage.
				return
			}
			defer r.cacheClient.Release(resp)
			if resp.RemoteReadResponse != nil {
				for _, cacheData := range resp.RemoteReadResponse.ResponseCacheData {
					numRead++
					idx := int(cacheData.Index)
					if cacheData.Hit {
						vector.Entries[idx].done = true
						vector.Entries[idx].CachedData = Bytes(cacheData.Data)
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

func HandleRemoteRead(
	ctx context.Context, fs FileService, req *pb.Request, resp *pb.CacheResponse,
) error {
	if req.RemoteReadRequest == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	if len(req.RemoteReadRequest.RequestCacheKey) == 0 {
		return nil
	}
	first := req.RemoteReadRequest.RequestCacheKey[0].CacheKey
	if first == nil { // We cannot get the first one.
		return nil
	}

	ioVec := &IOVector{
		FilePath: first.Path,
	}
	ioVec.Entries = make([]IOEntry, len(req.RemoteReadRequest.RequestCacheKey))
	for i, k := range req.RemoteReadRequest.RequestCacheKey {
		ioVec.Entries[i].Offset = k.CacheKey.Offset
		ioVec.Entries[i].Size = k.CacheKey.Sz
	}
	if err := fs.ReadCache(ctx, ioVec); err != nil {
		return err
	}
	respData := make([]*pb.ResponseCacheData, len(req.RemoteReadRequest.RequestCacheKey))
	for i, k := range req.RemoteReadRequest.RequestCacheKey {
		var data []byte
		if ioVec.Entries[i].CachedData != nil {
			data = ioVec.Entries[i].CachedData.Bytes()
		}
		respData[i] = &pb.ResponseCacheData{
			Index: k.Index,
			Hit:   ioVec.Entries[i].fromCache != nil,
			Data:  data,
		}
	}
	resp.RemoteReadResponse = &pb.RemoteReadResponse{
		ResponseCacheData: respData,
	}
	resp.ReleaseFunc = func() { ioVec.Release() }
	return nil
}
