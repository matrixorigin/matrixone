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

package gossip

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/pb/cache"
	"github.com/matrixorigin/matrixone/pkg/pb/gossip"
)

const (
	defaultCacheQueueSize = 100
	maxItemCount          = 8192 * 2000 // about 1.5GB
)

// DistKeyCache keeps the key cache of local set/delete items
// and key information on remote nodes.
type DistKeyCache struct {
	// cacheServerAddr is the service address of cache server. The items
	// with it are added to the queue, and then the items are sent to other
	// nodes. Then other nodes will try to send RPC request with this address
	// to this node to get data cache.
	cacheServerAddr string

	queueMu struct {
		sync.Mutex
		// When local file service updates cache item locally, it pushes
		// cache key item to this queue. And gossip worker takes items
		// from the queue and send to other nodes.
		itemQueue []gossip.CacheKeyItem
	}

	cacheMu struct {
		sync.Mutex
		// keyTarget is the map that local file service searches remote keys
		// when the cache does not exist in local file service.
		keyTarget map[cache.CacheKey]string // key => cn address
	}
}

// newDistKeyCache creates a new instance of DistKeyCache. The addr is service
// address of local cache server, to which when other file-service tries to
// send cache request.
func newDistKeyCache(addr string) *DistKeyCache {
	dc := &DistKeyCache{
		cacheServerAddr: addr,
	}
	dc.cacheMu.keyTarget = make(map[cache.CacheKey]string)
	dc.queueMu.itemQueue = make([]gossip.CacheKeyItem, 0, defaultCacheQueueSize)
	return dc
}

// Target implements the fileservice.KeyRouter interface.
func (dc *DistKeyCache) Target(k cache.CacheKey) string {
	dc.cacheMu.Lock()
	defer dc.cacheMu.Unlock()
	if target, ok := dc.cacheMu.keyTarget[k]; ok && target != dc.cacheServerAddr {
		return target
	}
	return ""
}

// AddItem implements the fileservice.KeyRouter interface.
func (dc *DistKeyCache) AddItem(key cache.CacheKey, operation gossip.Operation) {
	if dc.cacheServerAddr == "" {
		return
	}
	dc.queueMu.Lock()
	defer dc.queueMu.Unlock()
	if len(dc.queueMu.itemQueue) >= maxItemCount {
		return
	}
	item := gossip.CacheKeyItem{
		Operation:     operation,
		CacheKey:      key,
		TargetAddress: dc.cacheServerAddr,
	}
	dc.queueMu.itemQueue = append(dc.queueMu.itemQueue, item)
}

// updateCache updates the local data cache with the tk. It is used normally
// when the node is started and joined to the cluster.
func (dc *DistKeyCache) updateCache(tk *cache.TargetCacheKey) {
	dc.cacheMu.Lock()
	defer dc.cacheMu.Unlock()
	for target, cacheKey := range tk.TargetKey {
		for _, key := range cacheKey.Keys {
			dc.cacheMu.keyTarget[key] = target
		}
	}
}
