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
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"math"
)

// cachedBatch is just like the cachedVectorPool in the original code,
//
// it will support
// 1. GetCopiedBatch: generate a copied batch.
// 2. CacheBatch: put the byte slices of batch's vectors into the cache.
// 3. Reset: reset the cachedBatch for reuse.
// 4. Free: free the cached byte slices.
type cachedBatch struct {
	mp *mpool.MPool

	// the channel to cache the batch pointers.
	// the capacity of the channel is the max number of batch pointers that can be copied.
	//
	// only support copy batch once freeBatchPointer is not empty.
	freeBatchPointer chan freeBatchSignal

	// bytesCache stores the cached memory list for each batch.
	bytesCache []oneBatchMemoryCache
}

const (
	noNeedToCache int8 = -1
)

type freeBatchSignal struct {
	pointer         *batch.Batch
	whichCacheToUse int8
}

type oneBatchMemoryCache struct {
	// bytes to copy vector's data and area to.
	bs [][]byte
}

func initCachedBatch(mp *mpool.MPool, capacity int) *cachedBatch {
	if capacity < 1 {
		capacity = 1
	}

	cb := &cachedBatch{
		mp:               mp,
		freeBatchPointer: make(chan freeBatchSignal, capacity),
		bytesCache:       make([]oneBatchMemoryCache, capacity),
	}

	for i := 0; i < capacity; i++ {
		cb.freeBatchPointer <- freeBatchSignal{
			pointer:         batch.NewWithSize(0),
			whichCacheToUse: int8(i),
		}
	}

	return cb
}

func (cb *cachedBatch) CacheBatch(signal freeBatchSignal) {
	if signal.whichCacheToUse == noNeedToCache {
		return
	}
	cb.cacheVectorsInBatch(signal.pointer, signal.whichCacheToUse)
	cb.freeBatchPointer <- signal
}

func (cb *cachedBatch) cacheVectorsInBatch(bat *batch.Batch, whichCacheToUse int8) {
	for i, vec := range bat.Vecs {
		if vec == nil {
			continue
		}

		// 1. const vector size was too small,
		// 2. vector doesn't own its data and area,
		// we don't need to cache it.
		if vec.IsConst() || vec.NeedDup() {
			vec.Free(cb.mp)
		}

		data := vector.GetAndClearVecData(vec)
		area := vector.GetAndClearVecArea(vec)

		if data != nil {
			cb.bytesCache[whichCacheToUse].bs = append(cb.bytesCache[whichCacheToUse].bs, data)
		}
		if area != nil {
			cb.bytesCache[whichCacheToUse].bs = append(cb.bytesCache[whichCacheToUse].bs, area)
		}

		bat.ReplaceVector(vec, nil, i)
	}
	bat.Vecs = bat.Vecs[:0]
	bat.Attrs = bat.Attrs[:0]
	bat.SetRowCount(0)

	// todo: a hack here, because we won't reuse the aggs.
	for i := range bat.Aggs {
		bat.Aggs[i].Free()
	}
	bat.Aggs = nil
}

// GetCopiedBatch get a batch from the batchPointer channel
// and copy the data and area of the src batch to the dst batch.
func (cb *cachedBatch) GetCopiedBatch(
	senderCtx context.Context, src *batch.Batch) (signal freeBatchSignal, senderDone bool, err error) {

	var dst *batch.Batch

	if src == nil || src == batch.EmptyBatch || src == batch.CteEndBatch {
		signal.whichCacheToUse = noNeedToCache
		dst = src

	} else {
		select {
		case signal = <-cb.freeBatchPointer:
			dst = signal.pointer
			dst.Recursive = src.Recursive
			dst.Ro = src.Ro
			dst.ShuffleIDX = src.ShuffleIDX

		case <-senderCtx.Done():
			return signal, true, nil
		}

		if cap(dst.Vecs) >= len(src.Vecs) {
			dst.Vecs = dst.Vecs[:len(src.Vecs)]
			for i := range dst.Vecs {
				dst.Vecs[i] = nil
			}
		} else {
			dst.Vecs = make([]*vector.Vector, len(src.Vecs))
		}

		if cap(dst.Attrs) >= len(src.Attrs) {
			dst.Attrs = dst.Attrs[:len(src.Attrs)]
		} else {
			dst.Attrs = make([]string, len(src.Attrs))
		}
		// copy attrs.
		for i := range dst.Attrs {
			dst.Attrs[i] = src.Attrs[i]
		}

		// copy vectors.
		for i := range dst.Vecs {
			vec := src.Vecs[i]
			if vec == nil || dst.Vecs[i] != nil {
				continue
			}

			typ := *vec.GetType()
			dst.Vecs[i] = vector.NewVec(typ)

			if vec.IsConst() {
				if err = vector.GetConstSetFunction(typ, cb.mp)(dst.Vecs[i], vec, 0, vec.Length()); err != nil {
					dst.Clean(cb.mp)
					return signal, false, err
				}

			} else {
				cb.bytesCache[signal.whichCacheToUse].setSuitableDataAreaToVector(
					len(vec.GetData()), len(vec.GetArea()), dst.Vecs[i])
				dst.Vecs[i].Reset(typ)
				if err = vector.GetUnionAllFunction(typ, cb.mp)(
					dst.Vecs[i],
					vec); err != nil {
					dst.Clean(cb.mp)
					return signal, false, err
				}

				dst.Vecs[i].SetSorted(vec.GetSorted())
			}
			dst.Vecs[i].SetIsBin(vec.GetIsBin())

			// range src and found the same vector.
			for j := i + 1; j < len(src.Vecs); j++ {
				if dst.Vecs[j] == nil && src.Vecs[j] == vec {
					dst.Vecs[j] = dst.Vecs[i]
				}
			}
		}

		dst.Aggs = src.Aggs
		src.Aggs = nil

		// set row count.
		dst.SetRowCount(src.RowCount())

		// set cnt.
		dst.SetCnt(1)
	}

	signal.pointer = dst
	return signal, false, nil
}

// setSuitableDataAreaToVector get two long-enough bytes slices from the cache, and set them to the vector.
// if not found, set the last one to the vector.
func (mc *oneBatchMemoryCache) setSuitableDataAreaToVector(
	dataSize, areaSize int, vec *vector.Vector) {
	// return directly once cache was empty.
	if len(mc.bs) == 0 {
		return
	}

	setDataFirst := dataSize >= areaSize

	first, second := dataSize, areaSize
	if !setDataFirst {
		first, second = areaSize, dataSize
	}

	if first > 0 {
		suitIdx := -1
		suitDifference := math.MaxInt

		for i, bs := range mc.bs {
			if difference := cap(bs) - first; difference > 0 {
				if difference < suitDifference {
					suitIdx = i
					suitDifference = difference
				}
			}
		}

		if suitIdx != -1 {
			mem := mc.removeItemAndArrange(suitIdx)
			if setDataFirst {
				vector.SetVecData(vec, mem)
			} else {
				vector.SetVecArea(vec, mem)
			}
		}
	}

	if second > 0 {
		suitIdx := -1
		suitDifference := math.MaxInt

		for i, bs := range mc.bs {
			if difference := cap(bs) - second; difference > 0 {
				if difference < suitDifference {
					suitIdx = i
					suitDifference = difference
				}
			}
		}

		if suitIdx != -1 {
			mem := mc.removeItemAndArrange(suitIdx)
			if setDataFirst {
				vector.SetVecArea(vec, mem)
			} else {
				vector.SetVecData(vec, mem)
			}
		}
	}

	if len(mc.bs) > 0 && cap(vec.GetData()) == 0 && dataSize > 0 {
		vector.SetVecData(vec, mc.bs[len(mc.bs)-1])
		mc.bs = mc.bs[:len(mc.bs)-1]
	}
	if len(mc.bs) > 0 && cap(vec.GetArea()) == 0 && areaSize > 0 {
		vector.SetVecArea(vec, mc.bs[len(mc.bs)-1])
		mc.bs = mc.bs[:len(mc.bs)-1]
	}
}

// removeItemAndArrange return and remove the idx item of cache.
func (mc *oneBatchMemoryCache) removeItemAndArrange(idx int) []byte {
	last := len(mc.bs) - 1
	dst := mc.bs[idx]

	if idx != last {
		mc.bs[idx] = mc.bs[last]
		mc.bs = mc.bs[:last]
	}
	mc.bs = mc.bs[:last]
	return dst
}

func (cb *cachedBatch) Free() {
	m := cap(cb.freeBatchPointer)
	for i := 0; i < m; i++ {
		b := <-cb.freeBatchPointer
		b.pointer.Clean(cb.mp)
	}

	for i := range cb.bytesCache {
		for j := range cb.bytesCache[i].bs {
			cb.mp.Free(cb.bytesCache[i].bs[j])
		}
	}
}
