// Copyright 2024 Matrix Origin
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

package pSpool

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"sync"
)

type spoolBuffer struct {
	sync.Mutex

	// readyToUse is a stack to record the which memory block is ready to use.
	// the last one should be used first.
	readyToUse []readyToUseItem

	bytesCache []oneBatchMemoryCache
}

type readyToUseItem struct {
	whichCacheToUse   int8
	whichPointerToUse *batch.Batch
}

func initSpoolBuffer(size int8) *spoolBuffer {
	b := new(spoolBuffer)
	b.bytesCache = make([]oneBatchMemoryCache, size)
	b.readyToUse = make([]readyToUseItem, size)

	for i := range b.readyToUse {
		b.readyToUse[i].whichCacheToUse = int8(i)
		b.readyToUse[i].whichPointerToUse = batch.NewOffHeapEmpty()
	}

	return b
}

func (b *spoolBuffer) putCacheID(mp *mpool.MPool, id int8, bat *batch.Batch) {
	// do batch clean and put its memory into cache[id].
	for i, vec := range bat.Vecs {
		if vec == nil {
			continue
		}

		// 1. const vector size was too small,
		// 2. vector doesn't own its data and area,
		// we don't need to cache it.
		if vec.IsConst() || vec.NeedDup() {
			vec.Free(mp)
		}

		data := vector.GetAndClearVecData(vec)
		area := vector.GetAndClearVecArea(vec)

		if data != nil {
			b.bytesCache[id].bs = append(b.bytesCache[id].bs, data)
		}
		if area != nil {
			b.bytesCache[id].bs = append(b.bytesCache[id].bs, area)
		}

		bat.ReplaceVector(vec, nil, i)
	}
	bat.Vecs = bat.Vecs[:0]
	bat.Attrs = bat.Attrs[:0]
	bat.SetRowCount(0)

	// we won't reuse the aggregation's memories now.
	for i := range bat.Aggs {
		bat.Aggs[i].Free()
	}
	bat.Aggs = nil

	// put id into free list.
	b.Lock()
	b.readyToUse = b.readyToUse[:len(b.readyToUse)+1]
	b.readyToUse[len(b.readyToUse)-1].whichCacheToUse = id
	b.readyToUse[len(b.readyToUse)-1].whichPointerToUse = bat
	b.Unlock()
}

func (b *spoolBuffer) getCacheID() (int8, *batch.Batch) {
	b.Lock()
	k := len(b.readyToUse) - 1
	index, bat := b.readyToUse[k].whichCacheToUse, b.readyToUse[k].whichPointerToUse
	b.readyToUse = b.readyToUse[:k]
	b.Unlock()
	return index, bat
}

func (b *spoolBuffer) clean(mp *mpool.MPool) {
	for i := range b.bytesCache {
		for j := range b.bytesCache[i].bs {
			mp.Free(b.bytesCache[i].bs[j])
		}
	}
}
