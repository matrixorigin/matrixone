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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// cachedBatch is just like the cachedVectorPool in the original code,
//
// it will support
// 1. GetCopiedBatch: generate a copied batch.
// 2. CacheBatch: put the byte slices of batch's vectors into the cache.
// 3. free: free the cached byte slices.
type cachedBatch struct {
	mp *mpool.MPool

	// buffer save all the memory using by this structure.
	buffer *spoolBuffer
}

type oneBatchMemoryCache struct {
	// bytes to copy vector's data and area to.
	bs [][]byte
}

func initCachedBatch(mp *mpool.MPool, capacity uint32) *cachedBatch {
	if capacity < 1 {
		capacity = 1
	}

	cb := &cachedBatch{
		mp:     mp,
		buffer: initSpoolBuffer(capacity),
	}

	return cb
}

func (cb *cachedBatch) CacheBatch(useCache bool, whichCacheDoesThisDataUse uint32, data *batch.Batch) {
	if !useCache {
		return
	}
	cb.buffer.putCacheID(cb.mp, whichCacheDoesThisDataUse, data)
}

// GetCopiedBatch copy the src from a ready memory cache.
//
// if this is a special batch which will never be released, just return it and do not using any cache.
func (cb *cachedBatch) GetCopiedBatch(
	src *batch.Batch) (dst *batch.Batch, useCache bool, cacheID uint32, err error) {

	if src == nil || src == batch.EmptyBatch || src == batch.CteEndBatch {
		return src, false, 0, nil
	}

	cacheID, dst = cb.buffer.getCacheID()
	dst.Recursive = src.Recursive
	dst.ShuffleIDX = src.ShuffleIDX

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
		dst.Vecs[i] = vector.NewOffHeapVecWithType(typ)

		if vec.IsConst() {
			if err = vector.GetConstSetFunction(typ, cb.mp)(dst.Vecs[i], vec, 0, vec.Length()); err != nil {
				dst.Clean(cb.mp)
				return nil, false, 0, err
			}

		} else {
			cb.buffer.bytesCache[cacheID].setSuitableDataAreaToVector(
				len(vec.GetData()), len(vec.GetArea()), dst.Vecs[i])
			dst.Vecs[i].Reset(typ)
			if err = vector.GetUnionAllFunction(typ, cb.mp)(
				dst.Vecs[i],
				vec); err != nil {
				dst.Clean(cb.mp)
				return nil, false, 0, err
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

	dst.ExtraBuf = src.ExtraBuf
	src.ExtraBuf = nil

	// set row count.
	dst.SetRowCount(src.RowCount())

	return dst, true, cacheID, nil
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

func (cb *cachedBatch) free() {
	cb.buffer.clean(cb.mp)
}
