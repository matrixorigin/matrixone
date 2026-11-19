// Copyright 2026 Matrix Origin
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

package hashbuild

import (
	"math"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Collected components preserve their original slots, including nil slots.
// Only the hash table decides uniqueness; these batches never deduplicate.
type uniqueKeyBatch struct {
	vecs        []*vector.Vector
	checkpoints []vector.AppendCheckpoint
	metadata    *mpool.CapacityLease
}

type uniqueKeyBatches struct {
	first     uniqueKeyBatch
	rest      []uniqueKeyBatch
	directory *mpool.CapacityLease
	rows      int
}

func (b *uniqueKeyBatch) length() int {
	for _, v := range b.vecs {
		if v != nil {
			return v.Length()
		}
	}
	return 0
}

func (b *uniqueKeyBatch) free(mp *mpool.MPool) {
	for _, v := range b.vecs {
		if v != nil {
			v.Free(mp)
		}
	}
	b.vecs, b.checkpoints = nil, nil
	b.metadata.Release()
	b.metadata = nil
}

func (b *uniqueKeyBatches) count() int {
	if len(b.rest) == 0 {
		return b.first.length()
	}
	return b.rows
}

func (b *uniqueKeyBatches) free(mp *mpool.MPool) {
	if len(b.first.vecs) == 0 && b.first.metadata == nil && len(b.rest) == 0 && b.directory == nil {
		b.rows = 0
		return
	}
	b.first.free(mp)
	for i := range b.rest {
		b.rest[i].free(mp)
	}
	b.rest = nil
	b.directory.Release()
	b.directory = nil
	b.rows = 0
}

func (b *uniqueKeyBatches) visit(fn func([]*vector.Vector) error) error {
	if len(b.first.vecs) != 0 {
		if err := fn(b.first.vecs); err != nil {
			return err
		}
	}
	for i := range b.rest {
		if err := fn(b.rest[i].vecs); err != nil {
			return err
		}
	}
	return nil
}

func (hb *HashmapBuilder) newUniqueKeyBatch(mp *mpool.MPool) (out uniqueKeyBatch, err error) {
	if hb.uniqueKeyAllocation == nil || hb.mapAllocationAccount == nil {
		return out, mpool.ErrAllocationAccountInvalid
	}
	width := len(hb.curVecs)
	// A PK build knows its complete cardinality. Reserve one bounded data
	// chunk once, rather than repeatedly growing it for each hash unit. For
	// non-PK keys the actual uniqueness selection still controls allocation.
	capacityHint := min(hb.uniqueKeyRowsHint, max(0, hb.InputBatchRowCount-hb.uniqueKeys.count()))
	bytes := uint64(width) * uint64(reflect.TypeFor[*vector.Vector]().Size()+reflect.TypeFor[vector.AppendCheckpoint]().Size())
	reservation, err := hb.mapAllocationAccount.ReserveCapacity(bytes, mpool.AllocationOwnerHashBuild, HashBuildAllocationSiteUniqueKeyData)
	if err != nil {
		return out, err
	}
	defer reservation.Abort()
	out.metadata, err = reservation.Commit(bytes)
	if err != nil {
		return out, err
	}
	out.vecs = make([]*vector.Vector, width)
	out.checkpoints = make([]vector.AppendCheckpoint, width)
	for i, src := range hb.curVecs {
		if !hb.collectUniqueKeySlot(i) {
			continue
		}
		out.vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(*src.GetType(), hb.uniqueKeyAllocation)
		if err != nil {
			out.free(mp)
			return uniqueKeyBatch{}, err
		}
		if capacityHint > 0 {
			if err = out.vecs[i].PreExtend(capacityHint, mp); err != nil {
				out.free(mp)
				return uniqueKeyBatch{}, err
			}
		}
	}
	return out, nil
}

// appendUniqueKeyBatch admits directory replacement before Go allocates it.
// Existing entries stay owned by the old directory until replacement succeeds.
func (hb *HashmapBuilder) appendUniqueKeyBatch(next uniqueKeyBatch) error {
	b := &hb.uniqueKeys
	if len(b.first.vecs) == 0 {
		b.first = next
		return nil
	}
	if len(b.rest) == cap(b.rest) {
		capacity := max(1, 2*cap(b.rest))
		bytes := uint64(capacity) * uint64(reflect.TypeFor[uniqueKeyBatch]().Size())
		reservation, err := hb.mapAllocationAccount.ReserveCapacity(bytes, mpool.AllocationOwnerHashBuild, HashBuildAllocationSiteUniqueKeyData)
		if err != nil {
			return err
		}
		defer reservation.Abort()
		lease, err := reservation.Commit(bytes)
		if err != nil {
			return err
		}
		replacement := make([]uniqueKeyBatch, len(b.rest), capacity)
		copy(replacement, b.rest)
		b.rest = replacement
		b.directory.Release()
		b.directory = lease
	}
	b.rest = append(b.rest, next)
	return nil
}

// keyAppendCount checks both row and byte bounds before any vector growth.
// maxBytes includes the wire envelope, not just the area allocation, so a
// collected vector is also independently serializable. Tests inject small
// ceilings without allocating gigabytes.
func keyAppendCount(dst []*vector.Vector, src []*vector.Vector, start, count int, sels []int64, maxBytes int64) (int, error) {
	room := keycodec.KeyChunkRows
	// Signed-zero closure can add one representation at publication. Reserve
	// that row rather than allowing publication to grow a full float chunk.
	for _, v := range dst {
		// typeswitch:partial signed-zero closure is defined only for ordinary float32/64 runtime filters.
		if v != nil && (v.GetType().Oid == types.T_float32 || v.GetType().Oid == types.T_float64) {
			room--
			break
		}
	}
	emptyRoom := room
	for _, v := range dst {
		if v != nil {
			room -= v.Length()
			break
		}
	}
	count = min(count, room)
	for col, v := range dst {
		if v == nil {
			continue
		}
		dataBytes := int64(v.Length()) * int64(v.GetType().TypeSize())
		areaBytes := int64(len(v.GetArea()))
		// Header, null/grouping bitmaps and metadata are bounded by row count.
		const envelope = int64(4096)
		if !v.GetType().IsVarlen() {
			if v.GetType().TypeSize() <= 0 {
				return 0, process.ErrExecutionResourceInvalid
			}
			available := max(int64(0), maxBytes-envelope-dataBytes)
			count = min(count, int(available/int64(v.GetType().TypeSize())))
			continue
		}
		// Every external value is bounded by the source area. When even
		// that conservative bound fits, reuse Union's own sizing pass instead
		// of scanning ordinary variable-width keys a second time here.
		available := maxBytes - envelope - dataBytes - areaBytes
		perRowBound := int64(v.GetType().TypeSize()) + int64(len(src[col].GetArea()))
		if available >= 0 && int64(count) <= available/perRowBound {
			continue
		}
		accepted := 0
		for ; accepted < count; accepted++ {
			row := start + accepted
			if sels != nil {
				row = int(sels[accepted])
			}
			if src[col].IsConst() {
				row = 0
			}
			dataBytes += int64(v.GetType().TypeSize())
			if v.GetType().IsVarlen() && !src[col].IsConstNull() && !src[col].IsNull(uint64(row)) {
				n := len(src[col].GetRawBytesAt(row))
				if n > types.VarlenaInlineSize {
					areaBytes += int64(n)
				}
			}
			if dataBytes+areaBytes+envelope > maxBytes {
				break
			}
		}
		count = min(count, accepted)
	}
	if count == 0 && room == emptyRoom {
		return 0, runtimefilter.ErrOptionalKeySize
	}
	return count, nil
}

// appendUniqueKeys is transactional across columns and chunks. A failed append
// restores the old tail and frees every newly created chunk. Grown old-tail
// capacity stays with its vector until ordinary cleanup, as RollbackAppend
// requires; it is never detached from its allocation account.
func (hb *HashmapBuilder) appendUniqueKeys(proc *process.Process, start, count int, sels []int64) (err error) {
	if limit := hb.optionalRuntimeFilterMaxKeyBytes; limit > 0 {
		// Check the row limit before appending: HashOnPK can append an entire
		// unit before the ordinary cardinality check runs.
		if hb.optionalRuntimeFilterMaxKeys <= 0 || count > hb.optionalRuntimeFilterMaxKeys-hb.uniqueKeys.count() {
			return runtimefilter.ErrOptionalKeySize
		}
		// Check actual bytes, not declared VARCHAR width: external readers can
		// produce values wider than the table declaration. This is before any
		// optional-key copy or serialization; ErrOptionalKeySize abandons only
		// the optional filter and leaves the hash join authoritative.
		for slot, src := range hb.curVecs {
			if src == nil || !hb.collectUniqueKeySlot(slot) || !src.GetType().IsVarlen() || src.IsConstNull() {
				continue
			}
			for i := 0; i < count; i++ {
				row := start + i
				if sels != nil {
					row = int(sels[i])
				}
				if src.IsConst() {
					row = 0
				}
				if !src.IsNull(uint64(row)) && len(src.GetRawBytesAt(row)) > limit {
					return runtimefilter.ErrOptionalKeySize
				}
			}
		}
		if sels == nil {
			// UnionBatch may copy the whole source area, including unreachable
			// bytes left by overwritten values. Union copies only selected rows.
			sels = make([]int64, count)
			for i := range sels {
				sels[i] = int64(start + i)
			}
		}
	}
	return hb.appendUniqueKeysWithin(proc, start, count, sels, mpool.MaxAllocationSize())
}

func (hb *HashmapBuilder) appendUniqueKeysWithin(proc *process.Process, start, count int, sels []int64, maxBytes int64) (err error) {
	if count < 0 || len(hb.curVecs) == 0 && count != 0 {
		return process.ErrExecutionResourceInvalid
	}
	if len(hb.curVecs) == 0 {
		return nil
	}
	b := &hb.uniqueKeys
	oldChunks, oldRows := len(b.rest), b.count()
	if count > math.MaxInt32-oldRows {
		return runtimefilter.ErrOptionalKeySize
	}
	oldFirst := len(b.first.vecs) != 0
	oldTail := &b.first
	if oldChunks != 0 {
		oldTail = &b.rest[oldChunks-1]
	}
	for i, v := range oldTail.vecs {
		if v != nil {
			oldTail.checkpoints[i] = v.MakeAppendCheckpoint()
		}
	}
	// Keep a value copy: growing the directory may move the original tail.
	checkpoint := *oldTail
	oldTailAttemptedRows := 0
	defer func() {
		if err == nil {
			return
		}
		var rollbackErr error
		if oldFirst {
			for i, v := range checkpoint.vecs {
				if v != nil {
					rollbackErr = vector.JoinAppendRollbackError(rollbackErr,
						v.RollbackAppend(checkpoint.checkpoints[i], oldTailAttemptedRows))
				}
			}
		}
		for i := oldChunks; i < len(b.rest); i++ {
			b.rest[i].free(proc.Mp())
		}
		b.rest = b.rest[:oldChunks]
		if !oldFirst {
			b.first.free(proc.Mp())
		}
		if rollbackErr != nil {
			// An invalid checkpoint is terminal. Do not retain a partially
			// restored collection for a capacity retry or later publication.
			b.free(proc.Mp())
			err = vector.JoinAppendRollbackError(err, rollbackErr)
			return
		}
		b.rows = oldRows
	}()
	remaining := count
	// Even a zero-selection append establishes a typed empty collection.
	// Absence of collection state means PASS; a valid empty set means DROP.
	for remaining != 0 || len(b.first.vecs) == 0 {
		if err = proc.Ctx.Err(); err != nil {
			return err
		}
		tail := &b.first
		if len(b.rest) != 0 {
			tail = &b.rest[len(b.rest)-1]
		}
		if len(tail.vecs) == 0 {
			var next uniqueKeyBatch
			next, err = hb.newUniqueKeyBatch(proc.Mp())
			if err != nil {
				return err
			}
			if err = hb.appendUniqueKeyBatch(next); err != nil {
				next.free(proc.Mp())
				return err
			}
			continue
		}
		var n int
		n, err = keyAppendCount(tail.vecs, hb.curVecs, start, remaining, sels, maxBytes)
		if err != nil {
			return err
		}
		if n == 0 {
			var next uniqueKeyBatch
			next, err = hb.newUniqueKeyBatch(proc.Mp())
			if err != nil {
				return err
			}
			if err = hb.appendUniqueKeyBatch(next); err != nil {
				next.free(proc.Mp())
				return err
			}
			continue
		}
		if oldFirst && len(b.rest) == oldChunks {
			oldTailAttemptedRows += n
		}
		for i, v := range tail.vecs {
			if v == nil {
				continue
			}
			if sels == nil {
				err = v.UnionBatch(hb.curVecs[i], int64(start), n, nil, proc.Mp())
			} else {
				err = v.Union(hb.curVecs[i], sels[:n], proc.Mp())
			}
			if err != nil {
				return err
			}
		}
		b.rows += n
		start += n
		remaining -= n
		if sels != nil {
			sels = sels[n:]
		}
	}
	return nil
}
