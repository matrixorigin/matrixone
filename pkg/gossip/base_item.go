// Copyright 2021 - 2024 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/pb/gossip"
)

const (
	defaultQueueSize = 100
	maxItemCount     = 8192 * 2000
)

type BaseStore[K comparable] struct {
	// cacheServerAddr is the service address of cache server. The items
	// with it are added to the queue, and then the items are sent to other
	// nodes. Then other nodes will try to send RPC request with this address
	// to this node to get item info.
	cacheServerAddr string

	queueMu struct {
		sync.Mutex
		// When local service updates item, it pushes item to this queue.
		// And gossip worker takes items from the queue and send to other nodes.
		itemQueue []gossip.CommonItem
	}

	mu struct {
		sync.Mutex
		// keyTarget is the map that local service searches remote keys
		// when the item does not exist in local service.
		keyTarget map[K]string // key => cn address
	}
}

func newBaseStore[K comparable](addr string) *BaseStore[K] {
	s := &BaseStore[K]{
		cacheServerAddr: addr,
	}
	s.mu.keyTarget = make(map[K]string)
	s.queueMu.itemQueue = make([]gossip.CommonItem, 0, defaultQueueSize)
	return s
}

// Target implements the client.KeyRouter interface.
func (s *BaseStore[K]) Target(k K) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if target, ok := s.mu.keyTarget[k]; ok && target != s.cacheServerAddr {
		return target
	}
	return ""
}

// AddItem implements the client.KeyRouter interface.
func (s *BaseStore[K]) AddItem(item gossip.CommonItem) {
	if s.cacheServerAddr == "" {
		return
	}
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if len(s.queueMu.itemQueue) >= maxItemCount {
		return
	}
	item.TargetAddress = s.cacheServerAddr
	s.queueMu.itemQueue = append(s.queueMu.itemQueue, item)
}

// Data implements the Module interface.
func (s *BaseStore[K]) Data(limit int) ([]gossip.CommonItem, int) {
	var items []gossip.CommonItem
	s.queueMu.Lock()
	count := len(s.queueMu.itemQueue)
	if count == 0 {
		s.queueMu.Unlock()
		return nil, limit
	}
	sz := s.queueMu.itemQueue[0].Size()
	limitCount := limit / sz
	var leftSize int
	if count < limitCount {
		items = s.queueMu.itemQueue
		s.queueMu.itemQueue = make([]gossip.CommonItem, 0, defaultQueueSize)
		leftSize = limit - count*sz
		s.queueMu.Unlock()
	} else {
		items = s.queueMu.itemQueue[:limitCount]
		s.queueMu.itemQueue = s.queueMu.itemQueue[limitCount:]
		s.queueMu.Unlock()
	}
	return items, leftSize
}

func (s *BaseStore[K]) update(target string, ks []K) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range ks {
		s.mu.keyTarget[k] = target
	}
}
