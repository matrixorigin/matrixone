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

func (r *hashMapResizeReservation) Commit(hashtable.ResizePlan) {
	r.owner.replace(r.token)
	r.token = nil
}

func (r *hashMapResizeReservation) Rollback() {
	if r.token != nil {
		r.token.Release()
		r.token = nil
	}
}

// hashMapReservationOwner follows the physical hash table across producer to
// JoinMap ownership transfer. Resize callbacks retain this owner, so a resize
// performed by a consumer replaces the token released by JoinMap.FreeMemory,
// never a token stored back into a reused producer.
type hashMapReservationOwner struct {
	mu    sync.Mutex
	token *process.HashBuildReservation
}

func (o *hashMapReservationOwner) replace(token *process.HashBuildReservation) {
	o.mu.Lock()
	old := o.token
	o.token = token
	o.mu.Unlock()
	if old != nil {
		old.Release()
	}
}

func (o *hashMapReservationOwner) release() {
	if o == nil {
		return
	}
	o.replace(nil)
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
	hb.mapReservation = &hashMapReservationOwner{token: reservation}
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

func (hb *HashmapBuilder) projectedBatchCopyBytes(src *batch.Batch) (uint64, error) {
	projected := uint64(src.Allocated())
	if size := uint64(src.Size()); size > projected {
		projected = size
	}
	rows := uint64(src.RowCount())
	last := len(hb.Batches.Buf) - 1
	if last >= 0 && hb.Batches.Buf[last] != nil && hb.Batches.Buf[last].RowCount() != colexec.DefaultBatchSize {
		// CopyIntoBatches appends into the partial tail. Vector growth briefly
		// keeps the old allocation alive, so the source alone is not a safe
		// bound for small spill records appended to a much larger tail.
		tail := uint64(hb.Batches.Buf[last].Allocated())
		if size := uint64(hb.Batches.Buf[last].Size()); size > tail {
			tail = size
		}
		if tail > projected {
			projected = tail
		}
	} else if len(hb.Batches.Buf) > 1 && rows > 0 && rows < uint64(colexec.DefaultBatchSize) {
		// Once Batches contains more than one full batch, CopyIntoBatches
		// preallocates a full DefaultBatchSize destination for a tiny tail.
		if projected > math.MaxUint64/uint64(colexec.DefaultBatchSize) {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		projected = (projected*uint64(colexec.DefaultBatchSize) + rows - 1) / rows
	}
	// CopyIntoBatches may hold the original-sized destination plus vector/area
	// capacity growth. Four times the scaled source allocation covers that peak.
	if projected > math.MaxUint64/4 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	projected *= 4

	// Vector null bitmaps and batch/vector slice metadata live on the Go heap
	// and are therefore not included in Batch.Allocated. Charge a deliberately
	// conservative per-row allowance that also scales with the column count.
	// This is part of admission, before CopyIntoBatches performs any allocation.
	metadata, ok := retainedMetadataAllowance(src)
	if !ok {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	if projected > math.MaxUint64-metadata {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return projected + metadata, nil
}

func (hb *HashmapBuilder) cleanBatches(proc *process.Process) {
	hb.Batches.Clean(proc.Mp())
	hb.releaseBatchReservations()
}

func (hb *HashmapBuilder) reserveBuildAux() error {
	if hb.budget == nil || hb.auxReservation != nil {
		return nil
	}
	// Covers persistent sels/unique-key copies, O(rows) dedup/bitmap scratch,
	// and the cold Int/String iterator's fixed UnitLimit Go slices. Three extra
	// copies of retained vector capacity cover string-key append growth plus
	// runtime-filter unique-key vectors while both coexist.
	bytes := batchesAllocated(hb.Batches.Buf)
	if bytes > math.MaxUint64/3 {
		return process.ErrHashBuildBudgetInvalid
	}
	bytes *= 3
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

func (hb *HashmapBuilder) marshalRuntimeFilterVector(vec *vector.Vector) ([]byte, func(), error) {
	if vec == nil || hb.budget == nil {
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	// MarshalBinary writes vector data, area, null metadata and headers into a
	// bytes.Buffer. Budget three times the exact payload components plus a
	// per-row null/serialization allowance, covering buffer capacity growth and
	// the temporary null serialization while the source vector remains live.
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
	if payload > math.MaxUint64/3 {
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	projected := payload * 3
	token, err := hb.budget.Reserve(projected)
	if err != nil {
		return nil, nil, err
	}
	data, err := vec.MarshalBinary()
	if err != nil {
		token.Release()
		return nil, nil, err
	}
	if uint64(len(data)) > payload {
		token.Release()
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	// Keep the serialized slice's retained capacity and the source vector's
	// still-live null/metadata footprint charged until the message handoff.
	actual := uint64(cap(data))
	if metadata > math.MaxUint64-4096 || actual > math.MaxUint64-(metadata+4096) {
		token.Release()
		return nil, nil, process.ErrHashBuildBudgetInvalid
	}
	actual += metadata + 4096
	if _, err = token.ReconcileDown(actual); err != nil {
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
