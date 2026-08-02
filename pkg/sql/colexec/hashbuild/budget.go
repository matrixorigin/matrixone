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
	"math"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
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

// NewBudgetedEmptyJoinMap creates an initially empty JoinMap whose complete
// physical hash-table lifetime is charged to budget. The initial allocation is
// admitted before touching the mpool, every later resize uses the same
// generation, and JoinMap.Free releases all retained reservations.
//
// This is used by consumers that must grow a hash table from probe-side keys
// (for example RightDedupJoin after an empty build partition). Such maps cannot
// use the regular HashmapBuilder ownership transfer because there is no build
// batch to publish.
func NewBudgetedEmptyJoinMap(
	keyWidth int,
	budget *process.HashBuildBudgetGeneration,
	mp *mpool.MPool,
) (*message.JoinMap, error) {
	if budget == nil || mp == nil {
		return nil, process.ErrHashBuildBudgetInvalid
	}

	initialBytes := hashtable.Int64HashMapInitialAllocationBytes()
	if keyWidth > 8 {
		initialBytes = hashtable.StringHashMapInitialAllocationBytes()
	}
	initial, err := budget.Reserve(initialBytes)
	if err != nil {
		return nil, err
	}
	owner := &hashMapReservationOwner{
		tokens: []*process.HashBuildReservation{initial},
	}

	var (
		intHashMap *hashmap.IntHashMap
		strHashMap *hashmap.StrHashMap
	)
	if keyWidth <= 8 {
		intHashMap, err = hashmap.NewIntHashMap(false, mp)
		if err == nil {
			intHashMap.SetResizeAdmission(func(plan hashtable.ResizePlan) (hashtable.ResizeReservation, error) {
				return resizeAdmission(budget, owner, plan)
			})
		}
	} else {
		strHashMap, err = hashmap.NewStrHashMap(false, mp)
		if err == nil {
			strHashMap.SetResizeAdmission(func(plan hashtable.ResizePlan) (hashtable.ResizeReservation, error) {
				return resizeAdmission(budget, owner, plan)
			})
		}
	}
	if err != nil {
		owner.release()
		return nil, err
	}

	jm := message.NewJoinMap(message.GroupSels{}, intHashMap, strHashMap, nil, nil, mp)
	jm.SetMemoryRelease(owner.release)
	jm.IncRef(1)
	return jm, nil
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

type batchCopyAllocationSnapshot struct {
	length        int
	tail          *batch.Batch
	tailAllocated uint64
}

func snapshotBatchCopyAllocation(batches []*batch.Batch) (batchCopyAllocationSnapshot, error) {
	snapshot := batchCopyAllocationSnapshot{length: len(batches)}
	if snapshot.length == 0 {
		return snapshot, nil
	}
	snapshot.tail = batches[snapshot.length-1]
	if snapshot.tail == nil {
		return batchCopyAllocationSnapshot{}, process.ErrHashBuildBudgetInvalid
	}
	allocated := snapshot.tail.Allocated()
	if allocated < 0 {
		return batchCopyAllocationSnapshot{}, process.ErrHashBuildBudgetInvalid
	}
	snapshot.tailAllocated = uint64(allocated)
	return snapshot, nil
}

// batchCopyAllocatedDelta relies on CopyIntoBatches' append-only contract: it
// may grow the old partial tail and append destination batches. A full-size
// source can swap one new batch with that partial tail, so inspect the old tail
// plus the appended suffix by identity instead of rescanning every retained
// batch. Across a build this keeps retained-copy accounting linear in the
// number of destination batches rather than quadratic.
func batchCopyAllocatedDelta(
	batches []*batch.Batch,
	snapshot batchCopyAllocationSnapshot,
) (uint64, error) {
	if snapshot.length < 0 || len(batches) < snapshot.length {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	start := 0
	seenTail := snapshot.length == 0
	if snapshot.length > 0 {
		if snapshot.tail == nil {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		start = snapshot.length - 1
	}
	var delta uint64
	for i := start; i < len(batches); i++ {
		bat := batches[i]
		if bat == nil {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		allocated := bat.Allocated()
		if allocated < 0 {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		value := uint64(allocated)
		if bat == snapshot.tail {
			if seenTail || value < snapshot.tailAllocated {
				return 0, process.ErrHashBuildBudgetInvalid
			}
			seenTail = true
			value -= snapshot.tailAllocated
		}
		if delta > math.MaxUint64-value {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		delta += value
	}
	if !seenTail {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return delta, nil
}

func (hb *HashmapBuilder) copyBuildBatch(src *batch.Batch, proc *process.Process) error {
	if hb.budget == nil {
		return hb.Batches.CopyIntoBatches(src, proc)
	}
	projection, err := hb.projectedBatchCopy(src)
	if err != nil {
		return err
	}
	return hb.copyBuildBatchProjected(src, proc, projection)
}

func (hb *HashmapBuilder) copyBuildBatchProjected(
	src *batch.Batch,
	proc *process.Process,
	projection batchCopyProjection,
) error {
	if hb.budget == nil {
		if err := hb.Batches.CopyIntoBatches(src, proc); err != nil {
			// CopyIntoBatches destroys every retained destination on failure.
			// Keep the derived tail state transactional with that owner cleanup.
			hb.retainedSpillTailSelected = 0
			return err
		}
		hb.retainedSpillTailSelected = projection.nextTailSelected
		return nil
	}
	reservation, err := hb.budget.Reserve(projection.admissionBytes)
	if err != nil {
		return err
	}
	snapshot, err := snapshotBatchCopyAllocation(hb.Batches.Buf)
	if err != nil {
		reservation.Release()
		return err
	}
	if err = hb.Batches.CopyIntoBatches(src, proc); err != nil {
		reservation.Release()
		hb.releaseBatchReservations()
		hb.retainedSpillTailSelected = 0
		return err
	}
	actual, err := batchCopyAllocatedDelta(hb.Batches.Buf, snapshot)
	if err != nil {
		hb.Batches.Clean(proc.Mp())
		hb.retainedSpillTailSelected = 0
		reservation.Release()
		hb.releaseBatchReservations()
		return err
	}
	metadata, ok := retainedMetadataAllowance(src)
	if !ok || actual > math.MaxUint64-metadata {
		hb.Batches.Clean(proc.Mp())
		hb.retainedSpillTailSelected = 0
		reservation.Release()
		hb.releaseBatchReservations()
		return process.ErrHashBuildBudgetInvalid
	}
	actual += metadata
	if actual > projection.admissionBytes {
		// This indicates an incomplete pre-allocation bound. Fail closed after
		// cleaning; never legitimize the excess with post-allocation admission.
		hb.Batches.Clean(proc.Mp())
		hb.retainedSpillTailSelected = 0
		reservation.Release()
		hb.releaseBatchReservations()
		return process.ErrHashBuildBudgetInvalid
	}
	if _, err = reservation.ReconcileDown(actual); err != nil {
		hb.Batches.Clean(proc.Mp())
		hb.retainedSpillTailSelected = 0
		reservation.Release()
		hb.releaseBatchReservations()
		return err
	}
	hb.batchReservations = append(hb.batchReservations, reservation)
	hb.retainedSpillTailSelected = projection.nextTailSelected
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

func projectedSpillSelectedAppendBytes(
	src *vector.Vector,
	rows int,
	payloadBytes uint64,
) (uint64, error) {
	if src == nil || rows < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	typeSize := src.GetType().TypeSize()
	if typeSize < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	selected, err := spillCheckedMul(uint64(rows), uint64(typeSize))
	if err != nil {
		return 0, err
	}
	return spillCheckedAdd(selected, payloadBytes)
}

// projectedPartialTailReplacement follows UnionBatch's allocation order. The
// existing tail reservation covers old capacities. At each grow, admission
// needs the complete replacement capacity plus deltas retained by earlier
// grows. appendedSelected is the logical spill materialization contributed by
// this source range, computed from the same varlen scan used for allocation.
func projectedPartialTailReplacement(
	tail, src *batch.Batch,
	appendRows int,
) (peak, retained, appendedSelected uint64, err error) {
	if tail == nil || src == nil || appendRows < 0 || len(tail.Vecs) != len(src.Vecs) {
		return 0, 0, 0, process.ErrHashBuildBudgetInvalid
	}
	for i, srcVec := range src.Vecs {
		dstVec := tail.Vecs[i]
		if dstVec == nil || srcVec == nil || dstVec.Length() > math.MaxInt-appendRows {
			return 0, 0, 0, process.ErrHashBuildBudgetInvalid
		}

		typeSize := srcVec.GetType().TypeSize()
		requiredRows := dstVec.Length() + appendRows
		if typeSize < 0 || (typeSize > 0 && requiredRows > math.MaxInt/typeSize) {
			return 0, 0, 0, process.ErrHashBuildBudgetInvalid
		}
		oldDataCap := cap(dstVec.GetData())
		if requiredData := requiredRows * typeSize; requiredData > oldDataCap {
			newCap, ok := mpool.GrowCapacity(int64(oldDataCap), int64(requiredData))
			if !ok || retained > math.MaxUint64-uint64(newCap) {
				return 0, 0, 0, process.ErrHashBuildBudgetInvalid
			}
			if candidate := retained + uint64(newCap); candidate > peak {
				peak = candidate
			}
			retained += uint64(newCap) - uint64(oldDataCap)
		}

		areaBytes, selectedPayload, areaErr :=
			unionBatchAreaProjection(srcVec, 0, appendRows)
		if areaErr != nil || areaBytes > math.MaxInt-len(dstVec.GetArea()) {
			return 0, 0, 0, process.ErrHashBuildBudgetInvalid
		}
		selected, selectedErr := projectedSpillSelectedAppendBytes(
			srcVec, appendRows, selectedPayload)
		if selectedErr != nil {
			return 0, 0, 0, selectedErr
		}
		appendedSelected, selectedErr = spillCheckedAdd(appendedSelected, selected)
		if selectedErr != nil {
			return 0, 0, 0, selectedErr
		}
		oldAreaCap := cap(dstVec.GetArea())
		requiredArea := len(dstVec.GetArea()) + areaBytes
		if requiredArea > oldAreaCap {
			newCap, ok := mpool.GrowCapacity(int64(oldAreaCap), int64(requiredArea))
			if !ok || retained > math.MaxUint64-uint64(newCap) {
				return 0, 0, 0, process.ErrHashBuildBudgetInvalid
			}
			if candidate := retained + uint64(newCap); candidate > peak {
				peak = candidate
			}
			retained += uint64(newCap) - uint64(oldAreaCap)
		}
	}
	return peak, retained, appendedSelected, nil
}

func projectedPartialTailReplacementBytes(
	tail, src *batch.Batch,
	appendRows int,
) (peak, retained uint64, err error) {
	peak, retained, _, err = projectedPartialTailReplacement(tail, src, appendRows)
	return peak, retained, err
}

type batchCopyProjection struct {
	admissionBytes      uint64
	maxRetainedSelected uint64
	maxRetainedRows     int
	nextTailSelected    uint64
	columns             int
}

// projectedNewDestinationAllocation follows CopyIntoBatches for destinations
// that start empty. In addition to the total pre-allocation charge, it records
// the largest individual destination. HashBuild reuses that already-required
// projection to prove the future spill of every retained destination without
// adding another varlen row scan to the non-spill path.
func projectedNewDestinationAllocation(
	src *batch.Batch,
	start, rows int,
) (total uint64, maxRows int, maxSelected, lastSelected uint64, err error) {
	if src == nil || start < 0 || rows < 0 || start > src.RowCount() || rows > src.RowCount()-start {
		return 0, 0, 0, 0, process.ErrHashBuildBudgetInvalid
	}
	end := start + rows
	add := func(target *uint64, value uint64) error {
		if *target > math.MaxUint64-value {
			return process.ErrHashBuildBudgetInvalid
		}
		*target += value
		return nil
	}
	for offset := start; offset < end; {
		segmentRows := end - offset
		if segmentRows > colexec.DefaultBatchSize {
			segmentRows = colexec.DefaultBatchSize
		}
		var segmentAllocated uint64
		var segmentSelected uint64
		for _, vec := range src.Vecs {
			if vec == nil {
				return 0, 0, 0, 0, process.ErrHashBuildBudgetInvalid
			}
			typeSize := vec.GetType().TypeSize()
			if typeSize < 0 || (typeSize > 0 && segmentRows > math.MaxInt/typeSize) {
				return 0, 0, 0, 0, process.ErrHashBuildBudgetInvalid
			}
			dataCap, ok := mpool.GrowCapacity(0, int64(segmentRows*typeSize))
			if !ok || dataCap < 0 {
				return 0, 0, 0, 0, process.ErrHashBuildBudgetInvalid
			}
			if err = add(&segmentAllocated, uint64(dataCap)); err != nil {
				return 0, 0, 0, 0, err
			}
			areaBytes := 0
			var selectedPayload uint64
			if vec.GetType().IsVarlen() {
				var areaErr error
				areaBytes, selectedPayload, areaErr =
					unionBatchAreaProjection(vec, offset, segmentRows)
				if areaErr != nil {
					return 0, 0, 0, 0, process.ErrHashBuildBudgetInvalid
				}
				areaCap, ok := mpool.GrowCapacity(0, int64(areaBytes))
				if !ok || areaCap < 0 {
					return 0, 0, 0, 0, process.ErrHashBuildBudgetInvalid
				}
				if err = add(&segmentAllocated, uint64(areaCap)); err != nil {
					return 0, 0, 0, 0, err
				}
			}
			selected, selectedErr := projectedSpillSelectedAppendBytes(
				vec, segmentRows, selectedPayload)
			if selectedErr != nil {
				return 0, 0, 0, 0, selectedErr
			}
			if err = add(&segmentSelected, selected); err != nil {
				return 0, 0, 0, 0, err
			}
		}
		if err = add(&total, segmentAllocated); err != nil {
			return 0, 0, 0, 0, err
		}
		if segmentSelected > maxSelected {
			maxSelected = segmentSelected
		}
		lastSelected = segmentSelected
		if segmentRows > maxRows {
			maxRows = segmentRows
		}
		offset += segmentRows
	}
	return total, maxRows, maxSelected, lastSelected, nil
}

func projectedNewDestinationBytes(src *batch.Batch, start, rows int) (uint64, error) {
	total, _, _, _, err := projectedNewDestinationAllocation(src, start, rows)
	return total, err
}

func (hb *HashmapBuilder) projectedBatchCopy(src *batch.Batch) (batchCopyProjection, error) {
	if src == nil || src.RowCount() < 0 {
		return batchCopyProjection{}, process.ErrHashBuildBudgetInvalid
	}
	projection := batchCopyProjection{columns: len(src.Vecs)}
	rows := uint64(src.RowCount())
	last := len(hb.Batches.Buf) - 1
	hadPartialTail := last >= 0 && hb.Batches.Buf[last] != nil &&
		hb.Batches.Buf[last].RowCount() != colexec.DefaultBatchSize
	hasPartialTail := rows != uint64(colexec.DefaultBatchSize) && hadPartialTail
	appendRows := 0
	if hasPartialTail {
		// CopyIntoBatches appends into the partial tail. Derive each replacement
		// from the destination's old capacity and the actual old+append target.
		// A flat 1.25x multiplier is not a bound: GrowCapacity can take repeated
		// 1.25x steps before reaching the required size.
		tail := hb.Batches.Buf[last]
		if tail.RowCount() < 0 || tail.RowCount() >= colexec.DefaultBatchSize {
			return batchCopyProjection{}, process.ErrHashBuildBudgetInvalid
		}
		appendRows = colexec.DefaultBatchSize - tail.RowCount()
		if appendRows > src.RowCount() {
			appendRows = src.RowCount()
		}
		replacementPeak, retainedDelta, appendedSelected, err :=
			projectedPartialTailReplacement(tail, src, appendRows)
		if err != nil {
			return batchCopyProjection{}, err
		}
		projected := replacementPeak
		combinedSelected, err := spillCheckedAdd(
			hb.retainedSpillTailSelected, appendedSelected)
		if err != nil {
			return batchCopyProjection{}, err
		}
		projection.maxRetainedRows = tail.RowCount() + appendRows
		projection.maxRetainedSelected = combinedSelected
		if projection.maxRetainedRows < colexec.DefaultBatchSize {
			projection.nextTailSelected = combinedSelected
		}
		if appendRows < src.RowCount() {
			// After the tail grow finishes, its retained delta stays live while
			// CopyIntoBatches materializes the remaining source rows.
			remaining, maxRows, maxSelected, lastSelected, err := projectedNewDestinationAllocation(
				src, appendRows, src.RowCount()-appendRows,
			)
			if err != nil {
				return batchCopyProjection{}, err
			}
			if retainedDelta > math.MaxUint64-remaining {
				return batchCopyProjection{}, process.ErrHashBuildBudgetInvalid
			}
			if retained := retainedDelta + remaining; retained > projected {
				projected = retained
			}
			if maxSelected > projection.maxRetainedSelected {
				projection.maxRetainedSelected = maxSelected
			}
			if maxRows > projection.maxRetainedRows {
				projection.maxRetainedRows = maxRows
			}
			remainingRows := src.RowCount() - appendRows
			if remainingRows%colexec.DefaultBatchSize != 0 {
				projection.nextTailSelected = lastSelected
			} else {
				projection.nextTailSelected = 0
			}
		}
		projection.admissionBytes, err = projectedBatchCopyWithMetadata(src, projected)
		return projection, err
	}
	projected, maxRows, maxSelected, lastSelected, err := projectedNewDestinationAllocation(
		src, 0, src.RowCount())
	if err != nil {
		return batchCopyProjection{}, err
	}
	projection.admissionBytes, err = projectedBatchCopyWithMetadata(src, projected)
	projection.maxRetainedRows = maxRows
	projection.maxRetainedSelected = maxSelected
	if src.RowCount() == colexec.DefaultBatchSize && hadPartialTail {
		// CopyIntoBatches swaps the new full batch before the old partial tail;
		// the cached tail itself is unchanged.
		projection.nextTailSelected = hb.retainedSpillTailSelected
	} else if src.RowCount()%colexec.DefaultBatchSize != 0 {
		projection.nextTailSelected = lastSelected
	}
	return projection, err
}

func (hb *HashmapBuilder) projectedBatchCopyBytes(src *batch.Batch) (uint64, error) {
	projection, err := hb.projectedBatchCopy(src)
	return projection.admissionBytes, err
}

func projectedBatchCopyWithMetadata(src *batch.Batch, projected uint64) (uint64, error) {
	// Vector null bitmaps and batch/vector slice metadata live on the Go heap
	// and are therefore not included in Batch.Allocated. Charge a deliberately
	// conservative per-row allowance that also scales with the column count.
	// The source remains caller-owned, any retained tail already has its own
	// reservation, and CopyIntoBatches reconciles this reservation to the actual
	// retained delta below.
	metadata, ok := retainedMetadataAllowance(src)
	if !ok {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	const batchAllocationSlack = uint64(64 << 10)
	if projected > math.MaxUint64-metadata ||
		projected+metadata > math.MaxUint64-batchAllocationSlack {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return projected + metadata + batchAllocationSlack, nil
}

func (hb *HashmapBuilder) cleanBatches(proc *process.Process) {
	hb.Batches.Clean(proc.Mp())
	hb.retainedSpillTailSelected = 0
	hb.releaseBatchReservations()
}

func (hb *HashmapBuilder) buildAuxBytes(
	needUniqueVec bool,
) (uint64, error) {
	uniqueBytes, err := hb.uniqueJoinKeyBytes()
	if err != nil {
		return 0, err
	}
	return hb.buildAuxBytesWithUniqueProjection(
		needUniqueVec, uniqueBytes)
}

func (hb *HashmapBuilder) uniqueJoinKeyBytes() (uint64, error) {
	var total uint64
	for _, vec := range hb.UniqueJoinKeys {
		if vec == nil {
			continue
		}
		allocated := vec.Allocated()
		if allocated < 0 ||
			total > math.MaxUint64-uint64(allocated) {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		total += uint64(allocated)
	}
	return total, nil
}

func (hb *HashmapBuilder) buildAuxBytesWithUniqueProjection(
	needUniqueVec bool,
	uniqueBytes uint64,
) (uint64, error) {
	// Covers mandatory hashmap/sels scratch plus the selected runtime-filter
	// key vectors' actual persistent capacities. Before their first append, a
	// bounded source-relative estimate admits the optional owner; every grow is
	// then preflighted against its exact mpool capacity. Retained
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
		if uniqueBytes > growthSlack {
			growthSlack = uniqueBytes
		}
		if bytes > math.MaxUint64-growthSlack {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		bytes += growthSlack
	}
	rowCount := hb.InputBatchRowCount
	if hb.hashMapRowCountSet {
		rowCount = hb.hashMapRowCount
	}
	rows := uint64(rowCount)
	const iteratorScratch = uint64(640 << 10)
	if rows > math.MaxUint64/64 || bytes > math.MaxUint64-rows*64 || bytes+rows*64 > math.MaxUint64-iteratorScratch {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	bytes += rows*64 + iteratorScratch
	return bytes, nil
}

func (hb *HashmapBuilder) reserveBuildAux(needUniqueVec bool) error {
	if hb.budget == nil {
		return nil
	}
	if hb.auxReservation != nil {
		// BuildHashmap can be retried on the same retained batches with a
		// different optional-runtime-filter decision. Reconcile the existing
		// owner instead of silently retaining the previous projection (or,
		// worse, collecting optional keys under a mandatory-only charge).
		return hb.resizeBuildAuxReservation(needUniqueVec)
	}
	bytes, err := hb.buildAuxBytes(needUniqueVec)
	if err != nil {
		return err
	}
	token, err := hb.budget.Reserve(bytes)
	if err != nil {
		return err
	}
	hb.auxReservation = token
	return nil
}

// abandonOptionalRuntimeFilterKeys removes only the exact-filter owner from an
// in-progress mandatory map build. No map or input batch is replayed. The
// persistent auxiliary reservation is reconciled to the same projection used
// by a build which never requested UniqueJoinKeys.
func (hb *HashmapBuilder) abandonOptionalRuntimeFilterKeys(
	proc *process.Process,
) error {
	if err := hb.releaseOptionalRuntimeFilterKeys(proc); err != nil {
		return err
	}
	hb.runtimeFilterCollectionFallback = true
	return nil
}

// fallbackOptionalRuntimeFilterCollection converts only a proven optional
// cause into in-place key abandonment. Fatal causes are returned unchanged,
// leaving the fallback bit untouched and builder ownership with terminal
// cleanup.
func (hb *HashmapBuilder) fallbackOptionalRuntimeFilterCollection(
	proc *process.Process,
	cause error,
) error {
	if runtimefilter.ClassifyOptionalFallback(cause) ==
		runtimefilter.OptionalFallbackNone {
		return cause
	}
	if err := hb.abandonOptionalRuntimeFilterKeys(proc); err != nil {
		return err
	}
	return nil
}

// releaseOptionalRuntimeFilterKeys drops terminal producer-only state without
// marking collection fallback. The JoinMap retains only the mandatory
// auxiliary projection, so its transferred budget owner must be reconciled
// before publication.
func (hb *HashmapBuilder) releaseOptionalRuntimeFilterKeys(
	proc *process.Process,
) error {
	for i := range hb.UniqueJoinKeys {
		if hb.UniqueJoinKeys[i] != nil {
			hb.UniqueJoinKeys[i].Free(proc.Mp())
		}
	}
	hb.UniqueJoinKeys = nil
	hb.uniqueSels = nil
	if hb.auxReservation == nil {
		return nil
	}
	required, err := hb.buildAuxBytes(false)
	if err != nil {
		return err
	}
	if required > hb.auxReservation.Size() {
		return process.ErrHashBuildBudgetInvalid
	}
	_, err = hb.auxReservation.ReconcileDown(required)
	return err
}

func (hb *HashmapBuilder) resizeBuildAuxReservation(
	needUniqueVec bool,
) error {
	if hb.budget == nil {
		return nil
	}
	if hb.auxReservation == nil {
		return process.ErrHashBuildBudgetInvalid
	}
	target, err := hb.buildAuxBytes(needUniqueVec)
	if err != nil {
		return err
	}
	current := hb.auxReservation.Size()
	switch {
	case current < target:
		return hb.auxReservation.Grow(target - current)
	case current > target:
		_, err = hb.auxReservation.ReconcileDown(target)
		return err
	default:
		return nil
	}
}

// prepareCanonicalRuntimeFilterCollection first resizes the mandatory
// auxiliary owner for a Dedup input after its in-place canonical rewrite. It
// then attempts the optional UniqueJoinKeys delta. Failure of only that delta
// disables the runtime filter without failing the canonical map build.
func (hb *HashmapBuilder) prepareCanonicalRuntimeFilterCollection(
	requested bool,
) (bool, error) {
	if err := hb.resizeBuildAuxReservation(false); err != nil {
		return false, err
	}
	if !requested {
		return false, nil
	}
	if err := hb.resizeBuildAuxReservation(true); err != nil {
		if runtimefilter.ClassifyOptionalFallback(err) !=
			runtimefilter.OptionalFallbackBudgetAdmission {
			return false, err
		}
		hb.runtimeFilterCollectionFallback = true
		return false, nil
	}
	hb.runtimeFilterCollectionFallback = false
	return true, nil
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

// logicalAppendAreaBytes sums the payload that UnionInt32 will materialize for
// a contiguous non-const source range. It is intentionally separate from the
// general selected-row helper above: retained-copy projection runs once per
// ingress batch, so its common no-area and no-null paths avoid per-row class,
// bitmap, and selection checks.
func logicalAppendAreaBytes(src *vector.Vector, start, rows int) (uint64, error) {
	if src == nil || !src.GetType().IsVarlen() || start < 0 || rows < 0 ||
		start > src.Length() || rows > src.Length()-start || src.IsConst() {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	if rows == 0 || len(src.GetArea()) == 0 {
		return 0, nil
	}
	values, _ := vector.MustVarlenaRawData(src)
	end := start + rows
	if end > len(values) {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	var payload uint64
	if src.GetNulls().EmptyByFlag() {
		for i := start; i < end; i++ {
			if values[i].IsSmall() {
				continue
			}
			_, length := values[i].OffsetLen()
			if payload > math.MaxUint64-uint64(length) {
				return 0, process.ErrHashBuildBudgetInvalid
			}
			payload += uint64(length)
		}
		return payload, nil
	}
	for i := start; i < end; i++ {
		if src.GetNulls().Contains(uint64(i)) {
			continue
		}
		if values[i].IsSmall() {
			continue
		}
		_, length := values[i].OffsetLen()
		if payload > math.MaxUint64-uint64(length) {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		payload += uint64(length)
	}
	return payload, nil
}

// unionBatchAreaProjection mirrors Vector.UnionBatch's flags=nil varlen paths
// and keeps physical retention separate from logical spill materialization.
// A whole-vector copy retains the complete source area, including stale bytes,
// while shared varlena descriptors can make a later UnionInt32 copy the same
// payload once per logical row. One live-descriptor scan therefore supplies the
// exact selected payload without treating vector class as an ownership proxy.
func unionBatchAreaProjection(
	src *vector.Vector,
	start, rows int,
) (physicalBytes int, selectedPayload uint64, err error) {
	if src == nil || !src.GetType().IsVarlen() {
		return 0, 0, nil
	}
	if start < 0 || rows < 0 ||
		start > src.Length() || rows > src.Length()-start {
		return 0, 0, process.ErrHashBuildBudgetInvalid
	}
	if rows == 0 {
		return 0, 0, nil
	}
	if len(src.GetArea()) == 0 {
		return 0, 0, nil
	}
	if src.IsConst() {
		// UnionBatch materializes the constant payload once and broadcasts its
		// varlena header to the appended logical rows. UnionInt32 later copies
		// that referenced payload once for every selected row.
		physicalBytes, err = uniqueAppendAreaBytes(src, 0, 1, nil)
		if err != nil {
			return 0, 0, err
		}
		selectedPayload, err = spillCheckedMul(
			uint64(physicalBytes), uint64(rows))
		return physicalBytes, selectedPayload, err
	}
	livePayload, err := logicalAppendAreaBytes(src, start, rows)
	if err != nil {
		return 0, 0, err
	}
	if start == 0 && rows == src.Length() {
		return len(src.GetArea()), livePayload, nil
	}
	if livePayload > uint64(math.MaxInt) {
		return 0, 0, process.ErrHashBuildBudgetInvalid
	}
	return int(livePayload), livePayload, nil
}

// unionBatchAreaBytes is kept as the allocation-only projection used by
// focused CopyIntoBatches admission tests.
func unionBatchAreaBytes(src *vector.Vector, start, rows int) (int, error) {
	physical, _, err := unionBatchAreaProjection(src, start, rows)
	return physical, err
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
	dataCapacity, ok := mpool.GrowCapacity(
		int64(cap(dst.GetData())), int64(requiredData))
	if !ok || dataCapacity < 0 {
		return nil, process.ErrHashBuildBudgetInvalid
	}
	var overlap uint64
	if requiredData > cap(dst.GetData()) {
		overlap = uint64(cap(dst.GetData()))
	}
	if len(dst.GetArea()) > math.MaxInt-areaBytes {
		return nil, process.ErrHashBuildBudgetInvalid
	}
	requiredArea := len(dst.GetArea()) + areaBytes
	areaCapacity, ok := mpool.GrowCapacity(
		int64(cap(dst.GetArea())), int64(requiredArea))
	if !ok || areaCapacity < 0 {
		return nil, process.ErrHashBuildBudgetInvalid
	}
	if requiredArea > cap(dst.GetArea()) {
		if overlap > math.MaxUint64-uint64(cap(dst.GetArea())) {
			return nil, process.ErrHashBuildBudgetInvalid
		}
		overlap += uint64(cap(dst.GetArea()))
	}
	if requiredData <= cap(dst.GetData()) &&
		requiredArea <= cap(dst.GetArea()) {
		// The persistent capacities were admitted by their preceding grow.
		// Avoid rescanning every retained batch for each UnitLimit append which
		// stays within those capacities; only allocator growth changes either
		// the retained owner or the temporary replacement overlap.
		return nil, nil
	}

	currentUnique, err := hb.uniqueJoinKeyBytes()
	if err != nil {
		return nil, err
	}
	oldCapacity := uint64(cap(dst.GetData())) +
		uint64(cap(dst.GetArea()))
	newCapacity := uint64(dataCapacity) + uint64(areaCapacity)
	if currentUnique < oldCapacity ||
		currentUnique-oldCapacity > math.MaxUint64-newCapacity {
		return nil, process.ErrHashBuildBudgetInvalid
	}
	projectedUnique := currentUnique - oldCapacity + newCapacity
	target, err := hb.buildAuxBytesWithUniqueProjection(
		true, projectedUnique)
	if err != nil {
		return nil, err
	}
	if hb.auxReservation == nil {
		return nil, process.ErrHashBuildBudgetInvalid
	}
	if current := hb.auxReservation.Size(); current < target {
		if err = hb.auxReservation.Grow(target - current); err != nil {
			return nil, err
		}
	}
	if overlap == 0 {
		return nil, nil
	}
	return hb.budget.Reserve(overlap)
}

func (hb *HashmapBuilder) marshalRuntimeFilterVector(vec *vector.Vector) ([]byte, func(), error) {
	return runtimefilter.MarshalExactFilterVector(vec, hb.budget)
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
