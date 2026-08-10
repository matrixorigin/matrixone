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
	buffers []vector.DetachedBuffer
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
	if sourceSelection := src.AllocationAccountSelection(); !vector.AllocationAccountSelectionsEqual(
		sourceSelection,
		dst.AllocationAccountSelection(),
	) {
		if err = dst.SetAllocationAccount(sourceSelection); err != nil {
			cb.CacheBatch(true, cacheID, dst)
			return nil, false, 0, err
		}
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
		selection := vec.AllocationAccountSelection()
		if selection == nil {
			dst.Vecs[i] = vector.NewOffHeapVecWithType(typ)
		} else {
			dst.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(
				typ,
				selection,
			)
			if err != nil {
				cb.CacheBatch(true, cacheID, dst)
				return nil, false, 0, err
			}
		}

		if vec.IsConst() {
			if err = vector.GetConstSetFunction(typ, cb.mp)(dst.Vecs[i], vec, 0, vec.Length()); err != nil {
				cb.CacheBatch(true, cacheID, dst)
				return nil, false, 0, err
			}

		} else {
			if err = cb.buffer.bytesCache[cacheID].
				setSuitableDataAreaToVector(
					len(vec.GetData()),
					len(vec.GetArea()),
					dst.Vecs[i],
				); err != nil {
				cb.CacheBatch(true, cacheID, dst)
				return nil, false, 0, err
			}
			dst.Vecs[i].Reset(typ)
			if err = vector.GetUnionAllFunction(typ, cb.mp)(
				dst.Vecs[i],
				vec); err != nil {
				cb.CacheBatch(true, cacheID, dst)
				return nil, false, 0, err
			}

			dst.Vecs[i].SetSorted(vec.GetSorted())
		}
		if vec.HasGrouping() {
			groupingRows := vec.GetGrouping().GetBitmap().Len()
			if groupingRows < 0 || groupingRows > int64(math.MaxInt) {
				cb.CacheBatch(true, cacheID, dst)
				return nil, false, 0, mpool.ErrAllocationAccountInvalid
			}
			if err = dst.Vecs[i].PreExtendGrouping(
				int(groupingRows),
				cb.mp,
			); err != nil {
				cb.CacheBatch(true, cacheID, dst)
				return nil, false, 0, err
			}
			dst.Vecs[i].SetGrouping(vec.GetGrouping())
		}
		dst.Vecs[i].SetIsBin(vec.GetIsBin())
		dst.Vecs[i].SetIsBinaryString(vec.GetIsBinaryString())
		if vec.IsConst() {
			// GetUnionAllFunction already propagates row provenance for the
			// non-constant path. Constants still need their scalar metadata
			// copied after the const setter has materialized the value.
			if err = vec.CopyPrepareParamMetadataToWithMP(dst.Vecs[i], cb.mp); err != nil {
				cb.CacheBatch(true, cacheID, dst)
				return nil, false, 0, err
			}
		}

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
	dataSize, areaSize int,
	vec *vector.Vector,
) error {
	// return directly once cache was empty.
	if len(mc.buffers) == 0 {
		return nil
	}

	setDataFirst := dataSize >= areaSize

	first, second := dataSize, areaSize
	firstKind := vector.DetachedDataBuffer
	secondKind := vector.DetachedAreaBuffer
	if !setDataFirst {
		first, second = areaSize, dataSize
		firstKind, secondKind = secondKind, firstKind
	}

	if first > 0 {
		suitIdx := -1
		suitDifference := math.MaxInt

		for i := range mc.buffers {
			if !mc.buffers[i].CanAttachTo(vec, firstKind) {
				continue
			}
			if difference := mc.buffers[i].Capacity() - first; difference > 0 {
				if difference < suitDifference {
					suitIdx = i
					suitDifference = difference
				}
			}
		}

		if suitIdx != -1 {
			mem := mc.removeItemAndArrange(suitIdx)
			if err := mem.AttachTo(vec, firstKind); err != nil {
				mc.buffers = append(mc.buffers, mem)
				return err
			}
		}
	}

	if second > 0 {
		suitIdx := -1
		suitDifference := math.MaxInt

		for i := range mc.buffers {
			if !mc.buffers[i].CanAttachTo(vec, secondKind) {
				continue
			}
			if difference := mc.buffers[i].Capacity() - second; difference > 0 {
				if difference < suitDifference {
					suitIdx = i
					suitDifference = difference
				}
			}
		}

		if suitIdx != -1 {
			mem := mc.removeItemAndArrange(suitIdx)
			if err := mem.AttachTo(vec, secondKind); err != nil {
				mc.buffers = append(mc.buffers, mem)
				return err
			}
		}
	}

	if cap(vec.GetData()) == 0 && dataSize > 0 {
		if idx := mc.lastAttachable(
			vec,
			vector.DetachedDataBuffer,
		); idx >= 0 {
			mem := mc.removeItemAndArrange(idx)
			if err := mem.AttachTo(
				vec,
				vector.DetachedDataBuffer,
			); err != nil {
				mc.buffers = append(mc.buffers, mem)
				return err
			}
		}
	}
	if cap(vec.GetArea()) == 0 && areaSize > 0 {
		if idx := mc.lastAttachable(
			vec,
			vector.DetachedAreaBuffer,
		); idx >= 0 {
			mem := mc.removeItemAndArrange(idx)
			if err := mem.AttachTo(
				vec,
				vector.DetachedAreaBuffer,
			); err != nil {
				mc.buffers = append(mc.buffers, mem)
				return err
			}
		}
	}
	return nil
}

func (mc *oneBatchMemoryCache) lastAttachable(
	vec *vector.Vector,
	kind vector.DetachedBufferKind,
) int {
	for i := len(mc.buffers) - 1; i >= 0; i-- {
		if mc.buffers[i].CanAttachTo(vec, kind) {
			return i
		}
	}
	return -1
}

// removeItemAndArrange return and remove the idx item of cache.
func (mc *oneBatchMemoryCache) removeItemAndArrange(
	idx int,
) vector.DetachedBuffer {
	last := len(mc.buffers) - 1
	dst := mc.buffers[idx]

	if idx != last {
		mc.buffers[idx] = mc.buffers[last]
	}
	mc.buffers[last] = vector.DetachedBuffer{}
	mc.buffers = mc.buffers[:last]
	return dst
}

func (cb *cachedBatch) free() {
	cb.buffer.clean(cb.mp)
}
