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

package task

import (
	"container/heap"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"slices"
)

type cnStore struct {
	uuid string
	info pb.CNStoreInfo
}

type cnPool struct {
	freq     map[string]uint32
	sortedCN []cnStore
}

func (p *cnPool) Len() int {
	return len(p.sortedCN)
}

func (p *cnPool) Less(i, j int) bool {
	return p.freq[p.sortedCN[i].uuid] < p.freq[p.sortedCN[j].uuid]
}

func (p *cnPool) Swap(i, j int) {
	p.sortedCN[i], p.sortedCN[j] = p.sortedCN[j], p.sortedCN[i]
}

func (p *cnPool) Push(x any) {
	store := x.(cnStore)
	p.set(store, p.getFreq(store.uuid)+1)
}

func (p *cnPool) Pop() any {
	x := p.sortedCN[len(p.sortedCN)-1]
	p.remove(x.uuid)
	return x
}

func newCNPool() *cnPool {
	pool := &cnPool{
		freq:     make(map[string]uint32),
		sortedCN: make([]cnStore, 0),
	}
	heap.Init(pool)
	return pool
}

func newCNPoolWithCNState(cnState pb.CNState) *cnPool {
	orderedMap := &cnPool{
		freq:     make(map[string]uint32, len(cnState.Stores)),
		sortedCN: make([]cnStore, 0, len(cnState.Stores)),
	}

	for key, info := range cnState.Stores {
		orderedMap.freq[key] = 0
		orderedMap.sortedCN = append(orderedMap.sortedCN, cnStore{key, info})
	}
	heap.Init(orderedMap)
	return orderedMap
}

func (p *cnPool) set(key cnStore, val uint32) {
	if _, ok := p.freq[key.uuid]; !ok {
		p.sortedCN = append(p.sortedCN, key)
	}
	p.freq[key.uuid] = val
	heap.Fix(p, slices.IndexFunc(p.sortedCN,
		func(store cnStore) bool {
			return store.uuid == key.uuid
		}))
}

func (p *cnPool) getFreq(key string) uint32 {
	return p.freq[key]
}

func (p *cnPool) contains(uuid string) bool {
	_, ok := p.freq[uuid]
	return ok
}

func (p *cnPool) min() cnStore {
	if len(p.sortedCN) == 0 {
		return cnStore{}
	}
	return p.sortedCN[0]
}

func (p *cnPool) remove(key string) {
	delete(p.freq, key)
	slices.DeleteFunc(p.sortedCN, func(store cnStore) bool {
		return store.uuid == key
	})
}

func (p *cnPool) getStore(key string) (cnStore, bool) {
	if _, ok := p.freq[key]; !ok {
		return cnStore{}, ok
	}

	for _, store := range p.sortedCN {
		if store.uuid == key {
			return store, true
		}
	}
	return cnStore{}, false
}
