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
	"cmp"
	"math"
	"slices"

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
	buffers       []vector.DetachedBuffer
	retainedLimit int64
}

// SetCacheMemoryLimit is configured before sending the first batch. It limits
// only backing returned by the last consumer; an active borrowed batch is never
// evicted. Zero preserves the legacy cache policy.
func (ps *PipelineSpool) SetCacheMemoryLimit(bytes int64) {
	if ps == nil || bytes <= 0 {
		return
	}
	for i := range ps.cache.buffer.bytesCache {
		ps.cache.buffer.bytesCache[i].retainedLimit = bytes
	}
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
	attemptBatch, attemptID := dst, cacheID
	success := false
	defer func() {
		if !success {
			// The caller receives no batch/cache ID on failure. Return the actual
			// ID here after destroying only this attempt's partial vector stores.
			attemptBatch.Clean(cb.mp)
			cb.buffer.putCacheID(cb.mp, attemptID, attemptBatch)
		}
	}()
	dst.Recursive = src.Recursive
	dst.ShuffleIDX = src.ShuffleIDX
	if sourceSelection := src.AllocationAccountSelection(); !vector.AllocationAccountSelectionsEqual(
		sourceSelection,
		dst.AllocationAccountSelection(),
	) {
		if err = dst.SetAllocationAccount(sourceSelection); err != nil {
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

	var localColumns [32]int
	copyColumns := localColumns[:0]
	var localRequests [32]batchMemoryRequest
	requests := localRequests[:0]
	memoryCache := &cb.buffer.bytesCache[cacheID]

	// Prepare unique destinations before assigning the batch's cached backing.
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
				return nil, false, 0, err
			}
		}

		copyColumns = append(copyColumns, i)
		if !vec.IsConst() && len(memoryCache.buffers) > 0 {
			// Reusable data may include historical spare backing. Only live logical
			// descriptors/fixed values are needed by the destination.
			if size := vec.Length() * typ.TypeSize(); size > 0 {
				requests = append(requests, batchMemoryRequest{dst.Vecs[i], size, vector.DetachedDataBuffer})
			}
			if size := len(vec.GetArea()); size > 0 {
				requests = append(requests, batchMemoryRequest{dst.Vecs[i], size, vector.DetachedAreaBuffer})
			}
		}

		// range src and found the same vector.
		for j := i + 1; j < len(src.Vecs); j++ {
			if dst.Vecs[j] == nil && src.Vecs[j] == vec {
				dst.Vecs[j] = dst.Vecs[i]
			}
		}
	}
	if err = memoryCache.setSuitableBuffers(requests); err != nil {
		return nil, false, 0, err
	}

	// Copy payload and metadata in the original source-column order.
	for _, i := range copyColumns {
		vec := src.Vecs[i]
		typ := *vec.GetType()
		if vec.IsConst() {
			if err = vector.GetConstSetFunction(typ, cb.mp)(dst.Vecs[i], vec, 0, vec.Length()); err != nil {
				return nil, false, 0, err
			}

		} else {
			dst.Vecs[i].Reset(typ)
			if err = vector.GetUnionAllFunction(typ, cb.mp)(
				dst.Vecs[i],
				vec); err != nil {
				return nil, false, 0, err
			}

			dst.Vecs[i].SetSorted(vec.GetSorted())
		}
		if vec.HasGrouping() {
			groupingRows := vec.GetGrouping().GetBitmap().Len()
			if groupingRows < 0 || groupingRows > int64(math.MaxInt) {
				return nil, false, 0, mpool.ErrAllocationAccountInvalid
			}
			if err = dst.Vecs[i].PreExtendGrouping(
				int(groupingRows),
				cb.mp,
			); err != nil {
				return nil, false, 0, err
			}
			dst.Vecs[i].SetGrouping(vec.GetGrouping())
		}
		dst.Vecs[i].SetIsBin(vec.GetIsBin())
	}

	dst.ExtraBuf = src.ExtraBuf
	src.ExtraBuf = nil

	// set row count.
	dst.SetRowCount(src.RowCount())
	success = true

	return dst, true, cacheID, nil
}

// batchMemoryRequest names one uniquely owned destination backing. Requests
// from every column participate so a small column cannot consume a later
// column's only suitable cached block.
type batchMemoryRequest struct {
	vec  *vector.Vector
	size int
	kind vector.DetachedBufferKind
}

func (request batchMemoryRequest) capacity() int {
	if request.kind == vector.DetachedDataBuffer {
		return cap(request.vec.GetData())
	}
	return cap(request.vec.GetArea())
}

func (mc *oneBatchMemoryCache) setSuitableBuffers(requests []batchMemoryRequest) error {
	if len(mc.buffers) == 0 {
		return nil
	}
	// Stable shapes normally resolve here without sorting or oversized reuse.
	pending := requests[:0]
	for _, request := range requests {
		if request.size <= 0 {
			continue
		}
		exact := -1
		for i := range mc.buffers {
			if mc.buffers[i].Capacity() == request.size && mc.buffers[i].CanAttachTo(request.vec, request.kind) {
				exact = i
				break
			}
		}
		if exact < 0 {
			pending = append(pending, request)
		} else if err := mc.attachBuffer(exact, request); err != nil {
			return err
		}
	}
	// All exact fits are reserved. Match the largest remaining needs first,
	// including descriptor data as well as varlen areas.
	slices.SortStableFunc(pending, func(left, right batchMemoryRequest) int {
		return cmp.Compare(right.size, left.size)
	})
	for _, request := range pending {
		suitable, difference := -1, math.MaxInt
		for i := range mc.buffers {
			if !mc.buffers[i].CanAttachTo(request.vec, request.kind) {
				continue
			}
			if extra := mc.buffers[i].Capacity() - request.size; extra >= 0 && extra < difference {
				suitable, difference = i, extra
			}
		}
		if suitable >= 0 {
			if err := mc.attachBuffer(suitable, request); err != nil {
				return err
			}
		}
	}
	// Undersized fallback runs only after every sufficient request had its turn.
	for _, request := range pending {
		if request.capacity() != 0 {
			continue
		}
		if i := mc.lastAttachable(request.vec, request.kind); i >= 0 {
			if err := mc.attachBuffer(i, request); err != nil {
				return err
			}
		}
	}
	return nil
}

func (mc *oneBatchMemoryCache) attachBuffer(index int, request batchMemoryRequest) error {
	backing := mc.removeItemAndArrange(index)
	if err := backing.AttachTo(request.vec, request.kind); err != nil {
		mc.buffers = append(mc.buffers, backing)
		return err
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
