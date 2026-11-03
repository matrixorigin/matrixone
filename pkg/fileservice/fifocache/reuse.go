// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fifocache

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

// SetReuseHistoryCapacity configures metadata-only observations. They share the
// key namespace but do not consume or shorten ordinary replacement ghosts.
func (d *DataCache) SetReuseHistoryCapacity(capacity fscache.CapacityFunc) {
	c := d.fifo
	c.queueLock.Lock()
	defer c.queueLock.Unlock()
	c.observationMetadataCapacity = capacity
	c.observationMetadataSize = func(key fscache.CacheKey) int64 { return 256 + int64(len(key.Path)) }
}

// ObserveReuse runs once per logical miss, before decoded allocation. A first
// touch stores only its key; a second touch can request normal payload admission.
func (d *DataCache) ObserveReuse(key fscache.CacheKey, size int64) bool {
	c := d.fifo
	c.queueLock.Lock()
	defer c.queueLock.Unlock()
	c.trimObservations()
	if c.observationMetadataCapacity == nil || size <= 0 || size > c.capacity()-c.capacity1() ||
		c.observationMetadataSize(key) > c.observationMetadataCapacity() {
		return false
	}
	shard := &c.shards[c.keyShardFunc(key)%numShards]
	shard.Lock()
	if _, ok := shard.values[key]; ok {
		shard.Unlock()
		return true
	}
	key.Path = strings.Clone(key.Path)
	item := &_CacheItem[fscache.CacheKey, dataCacheValue]{key: key, size: size, queue: cacheItemObservation}
	shard.values[key] = item
	shard.Unlock()
	if c.observationTail == nil {
		c.observationHead = item
	} else {
		c.observationTail.observationNext = item
	}
	c.observationTail = item
	c.observationBytes += size
	c.observationMetadataUsed += c.observationMetadataSize(key)
	c.trimObservations()
	return false
}

// Called under queueLock. Promoted/deleted observations may remain linked until
// trimming; pointer-checked deletion cannot remove their replacement payload.
func (c *Cache[K, V]) popObservation() bool {
	item := c.observationHead
	if item == nil {
		return false
	}
	c.observationHead = item.observationNext
	item.observationNext = nil
	if c.observationHead == nil {
		c.observationTail = nil
	}
	c.observationBytes -= item.size
	c.observationMetadataUsed -= c.observationMetadataSize(item.key)
	c.retireHistoryItem(item)
	return true
}

// queueLock is held by the caller. Rollback examines queue membership under
// the shard lock, so retire the membership and map entry under that same lock.
func (c *Cache[K, V]) retireHistoryItem(item *_CacheItem[K, V]) {
	shard := &c.shards[c.keyShardFunc(item.key)%numShards]
	shard.Lock()
	defer shard.Unlock()
	item.queue = cacheItemNoQueue
	if shard.values[item.key] == item {
		delete(shard.values, item.key)
	}
}

func (c *Cache[K, V]) trimObservations() {
	if c.observationMetadataCapacity == nil {
		return
	}
	representedLimit := max(0, c.capacity()-c.capacity1())
	metadataLimit := max(0, c.observationMetadataCapacity())
	for c.observationBytes > representedLimit || c.observationMetadataUsed > metadataLimit {
		if !c.popObservation() {
			break
		}
	}
}

// Flush clears both kinds of key history after payload eviction. Observation
// quota enforcement above only touches observation entries.
func (d *DataCache) ClearReuseHistory() {
	c := d.fifo
	c.queueLock.Lock()
	defer c.queueLock.Unlock()
	for c.popObservation() {
	}
	for {
		item, ok := c.ghost.dequeue()
		if !ok {
			break
		}
		if item.queue != cacheItemGhost {
			continue
		}
		c.retireHistoryItem(item)
	}
	c.ghostSize = 0
}
