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

	memoryCache := &cb.buffer.bytesCache[cacheID]
	planned := false
	hasAliases := false

copyVectors:
	for i := range dst.Vecs {
		vec := src.Vecs[i]
		if vec == nil {
			continue
		}
		if dst.Vecs[i] != nil {
			if !planned {
				continue
			}
			if hasAliases {
				for _, previous := range src.Vecs[:i] {
					if previous == vec {
						continue copyVectors
					}
				}
			}
		}
		if vec.HasBorrowedBacking() {
			if dst.Vecs[i] != nil {
				continue
			}
			dst.Vecs[i], err = vec.RetainedReadonlyViewWithMP(cb.mp)
			if err != nil {
				cb.CacheBatch(true, cacheID, dst)
				return nil, false, 0, err
			}
			for j := i + 1; j < len(src.Vecs); j++ {
				if dst.Vecs[j] == nil && src.Vecs[j] == vec {
					dst.Vecs[j] = dst.Vecs[i]
				}
			}
			continue
		}

		typ := *vec.GetType()
		if dst.Vecs[i] == nil {
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
		}

		destination := dst.Vecs[i]
		if vec.IsConst() {
			if err = vector.GetConstSetFunction(typ, cb.mp)(destination, vec, 0, vec.Length()); err != nil {
				cb.CacheBatch(true, cacheID, dst)
				return nil, false, 0, err
			}

		} else {
			planned, hasAliases, err = memoryCache.prepareCopyBacking(src, dst, i, planned, hasAliases)
			if err != nil {
				cb.CacheBatch(true, cacheID, dst)
				return nil, false, 0, err
			}

			destination.Reset(typ)
			if err = vector.GetUnionAllFunction(typ, cb.mp)(
				destination,
				vec); err != nil {
				cb.CacheBatch(true, cacheID, dst)
				return nil, false, 0, err
			}

			destination.SetSorted(vec.GetSorted())
		}
		if vec.HasGrouping() {
			groupingRows := vec.GetGrouping().GetBitmap().Len()
			if groupingRows < 0 || groupingRows > int64(math.MaxInt) {
				cb.CacheBatch(true, cacheID, dst)
				return nil, false, 0, mpool.ErrAllocationAccountInvalid
			}
			if err = destination.PreExtendGrouping(
				int(groupingRows),
				cb.mp,
			); err != nil {
				cb.CacheBatch(true, cacheID, dst)
				return nil, false, 0, err
			}
			destination.SetGrouping(vec.GetGrouping())
		}
		destination.SetIsBin(vec.GetIsBin())
		if vec.IsConst() {
			// GetUnionAllFunction already propagates row provenance for the
			// non-constant path. Constants still need their scalar metadata
			// copied after the const setter has materialized the value.
			if err = vec.CopyPrepareParamMetadataToWithMP(destination, cb.mp); err != nil {
				cb.CacheBatch(true, cacheID, dst)
				return nil, false, 0, err
			}
			if err = destination.SetRuntimeStringDomainWithMP(vec.GetRuntimeStringDomainAt(0), cb.mp); err != nil {
				cb.CacheBatch(true, cacheID, dst)
				return nil, false, 0, err
			}
		}

		if !planned {
			// range src and found the same vector.
			for j := i + 1; j < len(src.Vecs); j++ {
				if dst.Vecs[j] == nil && src.Vecs[j] == vec {
					dst.Vecs[j] = dst.Vecs[i]
				}
			}
		}
	}

	dst.MoveExtraBufferFrom(src)

	// set row count.
	dst.SetRowCount(src.RowCount())

	return dst, true, cacheID, nil
}

// prepareCopyBacking keeps whole-batch reservation off the copy loop's
// common path. A final column cannot compete with a later request.
func (mc *oneBatchMemoryCache) prepareCopyBacking(src, dst *batch.Batch, column int, planned, aliases bool) (bool, bool, error) {
	if planned || len(mc.buffers) == 0 {
		return planned, aliases, nil
	}
	source := src.Vecs[column]
	dataSize := source.Length() * source.GetType().TypeSize()
	areaSize := len(source.GetArea())
	if column == len(src.Vecs)-1 {
		return false, aliases, mc.setSuitableDataAreaToVector(dataSize, areaSize, dst.Vecs[column])
	}
	exact, err := mc.setExactBuffers(dataSize, areaSize, dst.Vecs[column])
	if err != nil || exact || len(mc.buffers) == 0 {
		return false, aliases, err
	}
	// A current unmet request that dominates every later need is already
	// in the same position as descending batch matching. Reserve its best-fit
	// backing directly, without preparing any future destination shells.
	largestLater := 0
	for i := column + 1; i < len(src.Vecs); i++ {
		future := src.Vecs[i]
		if future == nil || future.IsConst() || future.HasBorrowedBacking() || dst.Vecs[i] != nil {
			continue
		}
		if size := future.Length() * future.GetType().TypeSize(); size > largestLater {
			largestLater = size
		}
		if size := len(future.GetArea()); size > largestLater {
			largestLater = size
		}
		if largestLater > max(dataSize, areaSize) {
			break
		}
	}
	first := batchMemoryRequest{dst.Vecs[column], dataSize, vector.DetachedDataBuffer}
	second := batchMemoryRequest{dst.Vecs[column], areaSize, vector.DetachedAreaBuffer}
	if second.size > first.size {
		first, second = second, first
	}
	if first.size >= largestLater && first.size > 0 && first.capacity() == 0 {
		if err = mc.attachSufficient(first); err != nil {
			return false, aliases, err
		}
	}
	if second.size >= largestLater && second.size > 0 && second.capacity() == 0 {
		if err = mc.attachSufficient(second); err != nil {
			return false, aliases, err
		}
	}
	if (dataSize == 0 || cap(dst.Vecs[column].GetData()) > 0) && (areaSize == 0 || cap(dst.Vecs[column].GetArea()) > 0) {
		return false, aliases, nil
	}
	aliases, err = prepareSpoolSuffix(src, dst, column)
	if err != nil {
		return false, aliases, err
	}
	if err = mc.setSuitableBatchBuffers(src, dst, column); err != nil {
		return false, aliases, err
	}
	return true, aliases, nil
}

// prepareSpoolSuffix creates owned shells only. Payload allocation and
// borrowed snapshots stay in the source-order copy loop.
func prepareSpoolSuffix(src, dst *batch.Batch, first int) (bool, error) {
	aliases := false
	for i := first; i < len(src.Vecs); i++ {
		source := src.Vecs[i]
		if source == nil || source.HasBorrowedBacking() {
			continue
		}
		if dst.Vecs[i] == nil {
			var err error
			if selection := source.AllocationAccountSelection(); selection == nil {
				dst.Vecs[i] = vector.NewOffHeapVecWithType(*source.GetType())
			} else {
				dst.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(*source.GetType(), selection)
				if err != nil {
					return false, err
				}
			}
		} else if i > first {
			aliases = true
		}
		for j := i + 1; j < len(src.Vecs); j++ {
			if dst.Vecs[j] == nil && src.Vecs[j] == source {
				dst.Vecs[j] = dst.Vecs[i]
				aliases = true
			}
		}
	}
	return aliases, nil
}

// Exact matches cannot be improved by considering later requests. Defer the
// whole-batch search until a request cannot reuse an exact compatible block.
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
			if difference := mc.buffers[i].Capacity() - first; difference >= 0 {
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
			if difference := mc.buffers[i].Capacity() - second; difference >= 0 {
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

func (mc *oneBatchMemoryCache) setExactBuffers(dataSize, areaSize int, vec *vector.Vector) (bool, error) {
	matched := true
	if dataSize > 0 {
		found := false
		for i := range mc.buffers {
			if mc.buffers[i].Capacity() != dataSize || !mc.buffers[i].CanAttachTo(vec, vector.DetachedDataBuffer) {
				continue
			}
			mem := mc.removeItemAndArrange(i)
			if err := mem.AttachTo(vec, vector.DetachedDataBuffer); err != nil {
				mc.buffers = append(mc.buffers, mem)
				return false, err
			}
			found = true
			break
		}
		matched = found
	}
	if areaSize > 0 {
		found := false
		for i := range mc.buffers {
			if mc.buffers[i].Capacity() != areaSize || !mc.buffers[i].CanAttachTo(vec, vector.DetachedAreaBuffer) {
				continue
			}
			mem := mc.removeItemAndArrange(i)
			if err := mem.AttachTo(vec, vector.DetachedAreaBuffer); err != nil {
				mc.buffers = append(mc.buffers, mem)
				return false, err
			}
			found = true
			break
		}
		matched = matched && found
	}
	return matched, nil
}

// batchMemoryRequest names one owned destination backing. Matching the uncopied
// suffix prevents a small column from consuming a later column's only suitable
// cached block.
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

// setSuitableBatchBuffers uses destination backing to record assignment.
// It builds no request slice and retains no planning state.
func (mc *oneBatchMemoryCache) setSuitableBatchBuffers(src, dst *batch.Batch, first int) error {
	// The caller already attempted both exact fits for the first column.
	for column := first + 1; column < len(src.Vecs) && len(mc.buffers) > 0; column++ {
		source := src.Vecs[column]
		if source == nil || source.IsConst() || source.HasBorrowedBacking() {
			continue
		}
		target := dst.Vecs[column]
		if cap(target.GetData()) == 0 || cap(target.GetArea()) == 0 {
			// Aliased destinations may already have backing attached.
			for kind := vector.DetachedDataBuffer; kind <= vector.DetachedAreaBuffer; kind++ {
				need := source.Length() * source.GetType().TypeSize()
				if kind == vector.DetachedAreaBuffer {
					need = len(source.GetArea())
				}
				request := batchMemoryRequest{target, need, kind}
				if need <= 0 || request.capacity() != 0 {
					continue
				}
				for i := range mc.buffers {
					if mc.buffers[i].Capacity() == need && mc.buffers[i].CanAttachTo(target, kind) {
						if err := mc.attachBuffer(i, request); err != nil {
							return err
						}
						break
					}
				}
			}
		}
	}
	// Enumerate unmet needs in descending size and source order without
	// materializing/sorting a request list. A cursor skips needs for which no
	// compatible backing was sufficient. Best-fit selection keeps smaller blocks
	// in use when larger blocks are not needed by any remaining column.
	beforeSize, beforeColumn, beforeKind := math.MaxInt, -1, vector.DetachedAreaBuffer
	for len(mc.buffers) > 0 {
		best := batchMemoryRequest{}
		bestColumn := -1
		for column := first; column < len(src.Vecs); column++ {
			source := src.Vecs[column]
			if source == nil || source.IsConst() || source.HasBorrowedBacking() {
				continue
			}
			target := dst.Vecs[column]
			if cap(target.GetData()) == 0 {
				need := source.Length() * source.GetType().TypeSize()
				if need > best.size && (need < beforeSize || need == beforeSize && column > beforeColumn) {
					best = batchMemoryRequest{target, need, vector.DetachedDataBuffer}
					bestColumn = column
				}
			}
			if cap(target.GetArea()) == 0 {
				need := len(source.GetArea())
				if need > best.size && (need < beforeSize || need == beforeSize && (column > beforeColumn || column == beforeColumn && beforeKind == vector.DetachedDataBuffer)) {
					best = batchMemoryRequest{target, need, vector.DetachedAreaBuffer}
					bestColumn = column
				}
			}
		}
		if best.vec == nil {
			break
		}
		beforeSize, beforeColumn, beforeKind = best.size, bestColumn, best.kind
		suitable, difference := -1, math.MaxInt
		for index := range mc.buffers {
			if extra := mc.buffers[index].Capacity() - best.size; extra >= 0 && extra < difference && mc.buffers[index].CanAttachTo(best.vec, best.kind) {
				suitable, difference = index, extra
				if extra == 0 {
					break
				}
			}
		}
		if suitable >= 0 {
			if err := mc.attachBuffer(suitable, best); err != nil {
				return err
			}
		}
	}
	// Old undersized blocks must be used in the order their replacements are
	// copied, so their release is not postponed behind unrelated allocations.
	for column := first; column < len(src.Vecs) && len(mc.buffers) > 0; column++ {
		source := src.Vecs[column]
		if source == nil || source.IsConst() || source.HasBorrowedBacking() {
			continue
		}
		target := dst.Vecs[column]
		for kind := vector.DetachedDataBuffer; kind <= vector.DetachedAreaBuffer; kind++ {
			need := source.Length() * source.GetType().TypeSize()
			if kind == vector.DetachedAreaBuffer {
				need = len(source.GetArea())
			}
			request := batchMemoryRequest{target, need, kind}
			if need <= 0 || request.capacity() != 0 {
				continue
			}
			if index := mc.lastAttachable(target, kind); index >= 0 {
				if err := mc.attachBuffer(index, request); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// attachSufficient transfers a best-fit block without taking undersized
// fallback away from the later source-order pass.
func (mc *oneBatchMemoryCache) attachSufficient(request batchMemoryRequest) error {
	if request.vec == nil || request.capacity() != 0 {
		return nil
	}
	suitable, difference := -1, math.MaxInt
	for index := range mc.buffers {
		if extra := mc.buffers[index].Capacity() - request.size; extra >= 0 && extra < difference && mc.buffers[index].CanAttachTo(request.vec, request.kind) {
			suitable, difference = index, extra
			if extra == 0 {
				break
			}
		}
	}
	if suitable >= 0 {
		return mc.attachBuffer(suitable, request)
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
