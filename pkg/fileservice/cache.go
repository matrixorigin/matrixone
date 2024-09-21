// Copyright 2022 Matrix Origin
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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/gossip"
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"go.uber.org/zap"
)

type CacheConfig struct {
	MemoryCapacity       *toml.ByteSize `toml:"memory-capacity" user_setting:"advanced"`
	DiskPath             *string        `toml:"disk-path"`
	DiskCapacity         *toml.ByteSize `toml:"disk-capacity"`
	DiskMinEvictInterval *toml.Duration `toml:"disk-min-evict-interval"`
	DiskEvictTarget      *float64       `toml:"disk-evict-target"`
	RemoteCacheEnabled   bool           `toml:"remote-cache-enabled"`
	RPC                  morpc.Config   `toml:"rpc"`
	CheckOverlaps        bool           `toml:"check-overlaps"`

	QueryClient      client.QueryClient            `json:"-"`
	KeyRouterFactory KeyRouterFactory[pb.CacheKey] `json:"-"`
	KeyRouter        client.KeyRouter[pb.CacheKey] `json:"-"`
	InitKeyRouter    *sync.Once                    `json:"-"`
	CacheCallbacks   `json:"-"`

	enableDiskCacheForLocalFS bool // for testing only
}

type CacheCallbacks struct {
	PostGet   []CacheCallbackFunc
	PostSet   []CacheCallbackFunc
	PostEvict []CacheCallbackFunc
}

type CacheCallbackFunc = func(fscache.CacheKey, fscache.Data)

func (c *CacheConfig) setDefaults() {
	c.RPC.Adjust()
}

func (c *CacheConfig) SetRemoteCacheCallback() {
	if !c.RemoteCacheEnabled || c.KeyRouterFactory == nil {
		return
	}
	c.InitKeyRouter = &sync.Once{}
	c.CacheCallbacks.PostSet = append(c.CacheCallbacks.PostSet,
		func(key fscache.CacheKey, data fscache.Data) {
			c.InitKeyRouter.Do(func() {
				c.KeyRouter = c.KeyRouterFactory()
			})
			if c.KeyRouter == nil {
				return
			}
			c.KeyRouter.AddItem(gossip.CommonItem{
				Operation: gossip.Operation_Set,
				Key: &gossip.CommonItem_CacheKey{
					CacheKey: &key,
				},
			})
		},
	)
	c.CacheCallbacks.PostEvict = append(c.CacheCallbacks.PostEvict,
		func(key fscache.CacheKey, data fscache.Data) {
			c.InitKeyRouter.Do(func() {
				c.KeyRouter = c.KeyRouterFactory()
			})
			if c.KeyRouter == nil {
				return
			}
			c.KeyRouter.AddItem(gossip.CommonItem{
				Operation: gossip.Operation_Delete,
				Key: &gossip.CommonItem_CacheKey{
					CacheKey: &key,
				},
			})
		},
	)
}

var DisabledCacheConfig = CacheConfig{
	MemoryCapacity: ptrTo[toml.ByteSize](DisableCacheCapacity),
	DiskCapacity:   ptrTo[toml.ByteSize](DisableCacheCapacity),
}

const DisableCacheCapacity = 1

var DefaultCacheDataAllocator = sync.OnceValue(func() CacheDataAllocator {
	return &bytesAllocator{
		allocator: memoryCacheAllocator(),
	}
})

// VectorCache caches IOVector
type IOVectorCache interface {
	Read(
		ctx context.Context,
		vector *IOVector,
	) error

	Update(
		ctx context.Context,
		vector *IOVector,
		async bool,
	) error

	Flush()

	//TODO file contents may change in TAE that violates the immutibility assumption
	// before they fix this, we still need this sh**.
	DeletePaths(
		ctx context.Context,
		paths []string,
	) error

	// Evict triggers eviction
	// if done is not nil, when eviction finish, target size will be send to the done chan
	Evict(done chan int64)

	Close()
}

var slowCacheReadThreshold = time.Second * 0

func readCache(ctx context.Context, cache IOVectorCache, vector *IOVector) error {
	if vector.allDone() {
		return nil
	}

	if slowCacheReadThreshold > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, slowCacheReadThreshold)
		defer cancel()
	}

	err := cache.Read(ctx, vector)
	if err != nil {

		if errors.Is(err, context.DeadlineExceeded) {
			logutil.Warn("cache read exceed deadline",
				zap.Any("err", err),
				zap.Any("cache type", fmt.Sprintf("%T", cache)),
				zap.Any("path", vector.FilePath),
				zap.Any("entries", vector.Entries),
			)
			// safe to ignore
			return nil
		}

		return err
	}

	return nil
}

var (
	GlobalMemoryCacheSizeHint atomic.Int64
	GlobalDiskCacheSizeHint   atomic.Int64

	allMemoryCaches sync.Map // *MemCache -> name
	allDiskCaches   sync.Map // *DiskCache -> name
)

func EvictMemoryCaches() map[string]int64 {
	ret := make(map[string]int64)
	ch := make(chan int64, 1)

	allMemoryCaches.Range(func(k, v any) bool {
		cache := k.(*MemCache)
		name := v.(string)
		cache.Evict(ch)
		ret[name] = <-ch

		return true
	})

	return ret
}

func EvictDiskCaches() map[string]int64 {
	ret := make(map[string]int64)
	ch := make(chan int64, 1)

	allDiskCaches.Range(func(k, v any) bool {
		cache := k.(*DiskCache)
		name := v.(string)
		cache.Evict(ch)
		ret[name] = <-ch

		return true
	})

	return ret
}
