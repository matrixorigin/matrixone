// Copyright 2021 Matrix Origin
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

package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

// CachePins keeps the FileService cache entries that borrowed (read-only)
// vectors alias. The cache is shared, so a borrowed entry is pinned until the
// vectors are no longer read. CachePins belongs to the lender, the producer
// that published the vectors: it releases the pins when it reuses or drops
// them. The zero value is ready to use; it is not safe for concurrent use.
type CachePins struct {
	pins []cachePin
}

// cachePin is one retained cache entry and the reservation of its pinned
// capacity on the borrower's account (nil when the borrower is not accounted).
type cachePin struct {
	data     fscache.Data
	capacity *mpool.CapacityLease
}

// add takes over one retained entry and its capacity reservation.
func (p *CachePins) add(data fscache.Data, capacity *mpool.CapacityLease) {
	p.pins = append(p.pins, cachePin{data: data, capacity: capacity})
}

// Release drops every pin, keeping the slice for the next block. It is safe on
// a nil or zero CachePins.
func (p *CachePins) Release() {
	if p == nil {
		return
	}
	for i := range p.pins {
		p.pins[i].data.Release()
		p.pins[i].capacity.Release()
		p.pins[i] = cachePin{}
	}
	p.pins = p.pins[:0]
}

// Len reports how many cache entries are pinned.
func (p *CachePins) Len() int {
	if p == nil {
		return 0
	}
	return len(p.pins)
}
