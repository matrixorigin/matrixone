// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashbuild

import (
	"bytes"
	"math"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type hashMapResizeReservation struct {
	owner *hashMapReservationOwner
	token *process.HashBuildReservation
}

func (r *hashMapResizeReservation) Commit(plan hashtable.ResizePlan) {
	r.owner.commit(r.token, plan.ReuseCurrentBlocks)
	r.token = nil
}

func (r *hashMapResizeReservation) Rollback() {
	if r.token != nil {
		r.token.Release()
		r.token = nil
	}
}

// hashMapReservationOwner follows the physical hash table across producer to
// JoinMap ownership transfer. Full-table replacement swaps the retained token;
// segmented growth keeps the existing tokens and adds one for the appended
// blocks. Resize callbacks retain this owner so consumer growth never stores
// reservations back into a reused producer.
type hashMapReservationOwner struct {
	mu     sync.Mutex
	tokens []*process.HashBuildReservation
}

func (o *hashMapReservationOwner) commit(token *process.HashBuildReservation, reuseCurrent bool) {
	o.mu.Lock()
	if reuseCurrent {
		o.tokens = append(o.tokens, token)
		o.mu.Unlock()
		return
	}
	old := o.tokens
	o.tokens = []*process.HashBuildReservation{token}
	o.mu.Unlock()
	for _, reservation := range old {
		reservation.Release()
	}
}

func (o *hashMapReservationOwner) release() {
	if o == nil {
		return
	}
	o.mu.Lock()
	tokens := o.tokens
	o.tokens = nil
	o.mu.Unlock()
	for _, token := range tokens {
		token.Release()
	}
}

func (hb *HashmapBuilder) setBudget(budget *process.HashBuildBudgetGeneration) {
	hb.budget = budget
}

// SetBudget is the exported boundary used by spill and integration tests.
func (hb *HashmapBuilder) SetBudget(budget *process.HashBuildBudgetGeneration) { hb.setBudget(budget) }

func (hb *HashmapBuilder) reserveInitialMap(size int64) error {
	if hb.budget == nil || size <= 0 {
		return nil
	}
	reservation, err := hb.budget.Reserve(uint64(size))
	if err != nil {
		return err
	}
	hb.mapReservation = &hashMapReservationOwner{tokens: []*process.HashBuildReservation{reservation}}
	return nil
}

func resizeAdmission(budget *process.HashBuildBudgetGeneration, owner *hashMapReservationOwner, plan hashtable.ResizePlan) (hashtable.ResizeReservation, error) {
	if budget == nil || plan.AdditionalBytes == 0 {
		return nil, nil
	}
	token, err := budget.Reserve(plan.AdditionalBytes)
	if err != nil {
		return nil, err
	}
	return &hashMapResizeReservation{owner: owner, token: token}, nil
}

func (hb *HashmapBuilder) attachIntHashMapAdmission(m *hashmap.IntHashMap) error {
	owner := hb.mapReservation
	budget := hb.budget
	m.SetResizeAdmission(func(plan hashtable.ResizePlan) (hashtable.ResizeReservation, error) {
		return resizeAdmission(budget, owner, plan)
	})
	return nil
}

func (hb *HashmapBuilder) attachStrHashMapAdmission(m *hashmap.StrHashMap) error {
	owner := hb.mapReservation
	budget := hb.budget
	m.SetResizeAdmission(func(plan hashtable.ResizePlan) (hashtable.ResizeReservation, error) {
		return resizeAdmission(budget, owner, plan)
	})
	return nil
}

func batchesAllocated(batches []*batch.Batch) uint64 {
	var total uint64
	for _, bat := range batches {
		if bat != nil {
			total += uint64(bat.Allocated())
		}
	}
	return total
}

func (hb *HashmapBuilder) copyBuildBatch(src *batch.Batch, proc *process.Process) error {
	if hb.budget == nil {
		return hb.Batches.CopyIntoBatches(src, proc)
	}
	projected, err := hb.projectedBatchCopyBytes(src)
	if err != nil {
		return err
	}
	reservation, err := hb.budget.Reserve(projected)
	if err != nil {
		return err
	}
	before := batchesAllocated(hb.Batches.Buf)
	if err = hb.Batches.CopyIntoBatches(src, proc); err != nil {
		reservation.Release()
		hb.releaseBatchReservations()
		return err
	}
	after := batchesAllocated(hb.Batches.Buf)
	if after < before {
		hb.Batches.Clean(proc.Mp())
		reservation.Release()
		hb.releaseBatchReservations()
		return process.ErrHashBuildBudgetInvalid
	}
	actual := after - before
	metadata, ok := retainedMetadataAllowance(src)
	if !ok || actual > math.MaxUint64-metadata {
		hb.Batches.Clean(proc.Mp())
		reservation.Release()
		hb.releaseBatchReservations()
		return process.ErrHashBuildBudgetInvalid
	}
	actual += metadata
	if actual > projected {
		// This indicates an incomplete pre-allocation bound. Fail closed after
		// cleaning; never legitimize the excess with post-allocation admission.
		hb.Batches.Clean(proc.Mp())
		reservation.Release()
		hb.releaseBatchReservations()
		return process.ErrHashBuildBudgetInvalid
	}
	if _, err = reservation.ReconcileDown(actual); err != nil {
		hb.Batches.Clean(proc.Mp())
		reservation.Release()
		hb.releaseBatchReservations()
		return err
	}
	hb.batchReservations = append(hb.batchReservations, reservation)
	return nil
}

// CopyBuildBatch is an exported compatibility wrapper.
func (hb *HashmapBuilder) CopyBuildBatch(src *batch.Batch, proc *process.Process) error {
	return hb.copyBuildBatch(src, proc)
}

func retainedMetadataAllowance(src *batch.Batch) (uint64, bool) {
	if src == nil {
		return 0, false
	}
	rows := uint64(src.RowCount())
	columns := uint64(len(src.Vecs))
	if columns > (math.MaxUint64-16)/8 {
		return 0, false
	}
	perRow := uint64(16) + columns*8
	if rows > 0 && perRow > math.MaxUint64/rows {
		return 0, false
	}
	return rows * perRow, true
}

// projectedPartialTailReplacementBytes follows UnionBatch's allocation order.
// The existing tail reservation covers old capacities. At each grow, admission
// needs the complete replacement capacity plus deltas retained by earlier grows.
func projectedPartialTailReplacementBytes(
	tail, src *batch.Batch,
	appendRows int,
) (peak, retained uint64, err error) {
	if tail == nil || src == nil || appendRows < 0 || len(tail.Vecs) != len(src.Vecs) {
		return 0, 0, process.ErrHashBuildBudgetInvalid
	}
	for i, srcVec := range src.Vecs {
		dstVec := tail.Vecs[i]
		if dstVec == nil || srcVec == nil || dstVec.Length() > math.MaxInt-appendRows {
			return 0, 0, process.ErrHashBuildBudgetInvalid
		}

		typeSize := srcVec.GetType().TypeSize()
		requiredRows := dstVec.Length() + appendRows
		if typeSize < 0 || (typeSize > 0 && requiredRows > math.MaxInt/typeSize) {
			return 0, 0, process.ErrHashBuildBudgetInvalid
		}
		oldDataCap := cap(dstVec.GetData())
		if requiredData := requiredRows * typeSize; requiredData > oldDataCap {
			newCap, ok := mpool.GrowCapacity(int64(oldDataCap), int64(requiredData))
			if !ok || retained > math.MaxUint64-uint64(newCap) {
				return 0, 0, process.ErrHashBuildBudgetInvalid
			}
			if candidate := retained + uint64(newCap); candidate > peak {
				peak = candidate
			}
			retained += uint64(newCap) - uint64(oldDataCap)
		}

		areaRows := appendRows
		if srcVec.IsConst() && areaRows > 0 {
			// UnionBatch materializes one varlen payload and broadcasts its header.
			areaRows = 1
		}
		areaBytes := 0
		var areaErr error
		if !srcVec.IsConst() && appendRows == srcVec.Length() && srcVec.GetType().IsVarlen() {
			// The whole-vector fast path copies the complete area with Grow2,
			// including retained bytes beyond the current logical values.
			areaBytes = len(srcVec.GetArea())
		} else {
			areaBytes, areaErr = uniqueAppendAreaBytes(srcVec, 0, areaRows, nil)
		}
		if areaErr != nil || areaBytes > math.MaxInt-len(dstVec.GetArea()) {
			return 0, 0, process.ErrHashBuildBudgetInvalid
		}
		oldAreaCap := cap(dstVec.GetArea())
		requiredArea := len(dstVec.GetArea()) + areaBytes
		if requiredArea > oldAreaCap {
			newCap, ok := mpool.GrowCapacity(int64(oldAreaCap), int64(requiredArea))
			if !ok || retained > math.MaxUint64-uint64(newCap) {
				return 0, 0, process.ErrHashBuildBudgetInvalid
			}
			if candidate := retained + uint64(newCap); candidate > peak {
				peak = candidate
			}
			retained += uint64(newCap) - uint64(oldAreaCap)
		}
	}
	return peak, retained, nil
}

func (hb *HashmapBuilder) projectedBatchCopyBytes(src *batch.Batch) (uint64, error) {
	if src == nil || src.RowCount() < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	projected := uint64(src.Allocated())
	if size := uint64(src.Size()); size > projected {
		projected = size
	}
	rows := uint64(src.RowCount())
	last := len(hb.Batches.Buf) - 1
	if rows != uint64(colexec.DefaultBatchSize) &&
		last >= 0 && hb.Batches.Buf[last] != nil &&
		hb.Batches.Buf[last].RowCount() != colexec.DefaultBatchSize {
		// CopyIntoBatches appends into the partial tail. Derive each replacement
		// from the destination's old capacity and the actual old+append target.
		// A flat 1.25x multiplier is not a bound: GrowCapacity can take repeated
		// 1.25x steps before reaching the required size.
		tail := hb.Batches.Buf[last]
		if tail.RowCount() < 0 || tail.RowCount() >= colexec.DefaultBatchSize {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		appendRows := colexec.DefaultBatchSize - tail.RowCount()
		if appendRows > src.RowCount() {
			appendRows = src.RowCount()
		}
		replacementPeak, retainedDelta, err := projectedPartialTailReplacementBytes(tail, src, appendRows)
		if err != nil {
			return 0, err
		}
		projected = replacementPeak
		if appendRows < src.RowCount() {
			// After the tail grow finishes, its retained delta stays live while
			// CopyIntoBatches materializes the remaining source rows. The source
			// allocation bounds that remainder; when multiple batches already
			// exist, preserve the existing full-batch preallocation adjustment.
			remaining := uint64(src.Allocated())
			if size := uint64(src.Size()); size > remaining {
				remaining = size
			}
			if len(hb.Batches.Buf) > 1 && rows > 0 && rows < uint64(colexec.DefaultBatchSize) {
				if remaining > math.MaxUint64/uint64(colexec.DefaultBatchSize) {
					return 0, process.ErrHashBuildBudgetInvalid
				}
				remaining = (remaining*uint64(colexec.DefaultBatchSize) + rows - 1) / rows
			}
			if retainedDelta > math.MaxUint64-remaining {
				return 0, process.ErrHashBuildBudgetInvalid
			}
			if retained := retainedDelta + remaining; retained > projected {
				projected = retained
			}
		}
	} else if len(hb.Batches.Buf) > 1 && rows > 0 && rows < uint64(colexec.DefaultBatchSize) {
		// Once Batches contains more than one full batch, CopyIntoBatches
		// preallocates a full DefaultBatchSize destination for a tiny tail.
		if projected > math.MaxUint64/uint64(colexec.DefaultBatchSize) {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		projected = (projected*uint64(colexec.DefaultBatchSize) + rows - 1) / rows
	}
	// Vector null bitmaps and batch/vector slice metadata live on the Go heap
	// and are therefore not included in Batch.Allocated. Charge a deliberately
	// conservative per-row allowance that also scales with the column count.
	// Allocated/Size, partial-tail high water, and full-batch scaling above
	// already describe one complete destination allocation. Do not multiply that
	// bound again: the source remains caller-owned, any retained tail already has
	// its own reservation, and CopyIntoBatches reconciles this new reservation to
	// the actual retained delta below.
	metadata, ok := retainedMetadataAllowance(src)
	if !ok {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	columns := uint64(len(src.Vecs))
	// CopyIntoBatches can split one source into multiple fixed-size
	// destinations. Each vector data/area allocation is rounded independently by
	// mpool, so their retained sum can exceed the already-rounded source by a
	// bounded number of allocator pages. Keep that structural slack additive
	// instead of multiplying the complete payload.
	const perColumnAllocationSlack = uint64(16 << 10)
	const batchAllocationSlack = uint64(64 << 10)
	if columns > (math.MaxUint64-batchAllocationSlack)/perColumnAllocationSlack {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	allocationSlack := columns*perColumnAllocationSlack + batchAllocationSlack
	if projected > math.MaxUint64-metadata ||
		projected+metadata > math.MaxUint64-allocationSlack {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return projected + metadata + allocationSlack, nil
}

func (hb *HashmapBuilder) cleanBatches(proc *process.Process) {
	hb.Batches.Clean(proc.Mp())
	hb.releaseBatchReservations()
}

func (hb *HashmapBuilder) reserveBuildAux(needUniqueVec bool) error {
	if hb.budget == nil || hb.auxReservation != nil {
		return nil
	}
	// Covers one persistent join-key copy, its bounded allocator growth, plus
	// O(rows) sels/dedup/bitmap scratch and the cold Int/String iterator's
	// fixed UnitLimit Go slices. Retained
	// build batches are already charged by batchReservations, expression results
	// have their own reservations, and runtime-filter serialization is admitted
	// separately. Charging multiple whole-batch copies here double-counts those
	// owners and can reject a build before any auxiliary allocation occurs.
	bytes := batchesAllocated(hb.Batches.Buf)
	if needUniqueVec {
		growthSlack := bytes / 4
		if bytes%4 != 0 {
			growthSlack++
		}
		if bytes > math.MaxUint64-growthSlack {
			return process.ErrHashBuildBudgetInvalid
		}
		bytes += growthSlack
	}
	rows := uint64(hb.InputBatchRowCount)
	const iteratorScratch = uint64(640 << 10)
	if rows > math.MaxUint64/64 || bytes > math.MaxUint64-rows*64 || bytes+rows*64 > math.MaxUint64-iteratorScratch {
		return process.ErrHashBuildBudgetInvalid
	}
	bytes += rows*64 + iteratorScratch
	token, err := hb.budget.Reserve(bytes)
	if err != nil {
		return err
	}
	hb.auxReservation = token
	return nil
}

func uniqueAppendAreaBytes(src *vector.Vector, start, rows int, sels []int64) (int, error) {
	if src == nil || !src.GetType().IsVarlen() {
		return 0, nil
	}
	if start < 0 || rows < 0 || (sels == nil && (start > src.Length() || rows > src.Length()-start)) {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	values, _ := vector.MustVarlenaRawData(src)
	areaBytes := 0
	for i := 0; i < rows; i++ {
		idx := start + i
		if sels != nil {
			if i >= len(sels) || sels[i] < 0 || sels[i] >= int64(src.Length()) {
				return 0, process.ErrHashBuildBudgetInvalid
			}
			idx = int(sels[i])
		}
		if src.IsConst() {
			idx = 0
		}
		if idx < 0 || idx >= len(values) ||
			(!src.GetNulls().EmptyByFlag() && src.GetNulls().Contains(uint64(idx))) ||
			values[idx].IsSmall() {
			continue
		}
		_, valueLen := values[idx].OffsetLen()
		valueBytes := int(valueLen)
		if areaBytes > math.MaxInt-valueBytes {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		areaBytes += valueBytes
	}
	return areaBytes, nil
}

func (hb *HashmapBuilder) reserveUniqueAppendOverlap(dst *vector.Vector, rows, areaBytes int) (*process.HashBuildReservation, error) {
	if hb.budget == nil {
		return nil, nil
	}
	if dst == nil || rows < 0 || areaBytes < 0 {
		return nil, process.ErrHashBuildBudgetInvalid
	}
	typeSize := dst.GetType().TypeSize()
	if typeSize < 0 || dst.Length() > math.MaxInt-rows ||
		(typeSize > 0 && dst.Length()+rows > math.MaxInt/typeSize) {
		return nil, process.ErrHashBuildBudgetInvalid
	}
	requiredData := (dst.Length() + rows) * typeSize
	var overlap uint64
	if requiredData > cap(dst.GetData()) {
		overlap = uint64(cap(dst.GetData()))
	}
	if len(dst.GetArea()) > math.MaxInt-areaBytes {
		return nil, process.ErrHashBuildBudgetInvalid
	}
	if len(dst.GetArea())+areaBytes > cap(dst.GetArea()) {
		if overlap > math.MaxUint64-uint64(cap(dst.GetArea())) {
			return nil, process.ErrHashBuildBudgetInvalid
		}
		overlap += uint64(cap(dst.GetArea()))
	}
	if overlap == 0 {
		return nil, nil
	}
	return hb.budget.Reserve(overlap)
}

func (hb *HashmapBuilder) marshalRuntimeFilterVector(vec *vector.Vector) ([]byte, func(), error) {
	if vec == nil || hb.budget == nil {
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	// The source vector is already charged by the hash-build auxiliary
	// reservation. Charge one serialized payload plus the temporary null bitmap
	// and pre-grow the output buffer so serialization cannot double its peak.
	payload := uint64(len(vec.GetData())) + uint64(len(vec.GetArea()))
	rows := uint64(vec.Length())
	if rows > math.MaxUint64/16 {
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	metadata := rows * 16
	if metadata > math.MaxUint64-4096 || payload > math.MaxUint64-(metadata+4096) {
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	payload += metadata + 4096
	nullPeak := (rows+7)/8 + 24
	const allocationSlack = uint64(64 << 10)
	if payload > math.MaxUint64-nullPeak-allocationSlack {
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	projected := payload + nullPeak + allocationSlack
	token, err := hb.budget.Reserve(projected)
	if err != nil {
		return nil, nil, err
	}
	if payload > uint64(math.MaxInt) {
		token.Release()
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	var buf bytes.Buffer
	buf.Grow(int(payload))
	if uint64(buf.Cap())+nullPeak > projected {
		token.Release()
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	err = vec.MarshalBinaryWithBuffer(&buf)
	if err != nil {
		token.Release()
		return nil, nil, err
	}
	data := buf.Bytes()
	if uint64(len(data)) > payload {
		token.Release()
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	if _, err = token.ReconcileDown(uint64(cap(data))); err != nil {
		token.Release()
		return nil, nil, err
	}
	return data, func() { token.Release() }, nil
}

func (hb *HashmapBuilder) releaseBatchReservations() {
	for _, reservation := range hb.batchReservations {
		reservation.Release()
	}
	hb.batchReservations = nil
}

func (hb *HashmapBuilder) releaseReservations() {
	hb.releaseMapReservation()
	hb.releaseBatchReservations()
	if hb.auxReservation != nil {
		hb.auxReservation.Release()
		hb.auxReservation = nil
	}
}

func (hb *HashmapBuilder) releaseMapReservation() {
	if hb.mapReservation != nil {
		hb.mapReservation.release()
		hb.mapReservation = nil
	}
}

func (hb *HashmapBuilder) detachReservations() func() {
	mapOwner := hb.mapReservation
	hb.mapReservation = nil
	reservations := make([]*process.HashBuildReservation, 0, 1+len(hb.batchReservations))
	for _, reservation := range hb.batchReservations {
		if token := reservation.Transfer(); token != nil {
			reservations = append(reservations, token)
		}
	}
	hb.batchReservations = nil
	if hb.auxReservation != nil {
		if token := hb.auxReservation.Transfer(); token != nil {
			reservations = append(reservations, token)
		}
		hb.auxReservation = nil
	}
	return func() {
		mapOwner.release()
		for _, reservation := range reservations {
			reservation.Release()
		}
	}
}
