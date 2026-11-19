// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shuffle

import (
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// One pending input owns these references; immutable bases are shared by every
// admitted full partition. Queued outputs own their own references and can
// outlive this workspace. Short fragments never freeze or allocate a selection.
type shufflePartitionViews struct {
	bases    []*vector.DictionaryBase
	rows     *vector.Vector
	disabled bool
}

func (s *shufflePartitionViews) free(mp *mpool.MPool) {
	for i, base := range s.bases {
		if base != nil {
			base.Release()
			s.bases[i] = nil
		}
	}
	if s.rows != nil {
		s.rows.Free(mp)
		s.rows = nil
	}
}

func (sp *ShufflePool) selectPartition(input *batch.Batch, rows []int32, workspace **shufflePartitionViews, proc *process.Process) (_ *batch.Batch, retErr error) {
	if len(input.ExtraBuf) != 0 || len(input.Vecs) == 0 || (*workspace != nil && (*workspace).disabled) {
		return nil, nil
	}
	allocation := sp.outputAllocation
	if allocation == nil {
		allocation = input.Vecs[0].AllocationAccountSelection()
	}
	copyAllocation := sp.outputAllocation
	if copyAllocation == nil {
		copyAllocation = input.AllocationAccountSelection()
	}
	if copyAllocation != nil && !vector.AllocationAccountSelectionsEqual(copyAllocation, allocation) {
		return nil, nil
	}
	var copyBytes, selectors, pinnedBytes int64
	// Ignore constant payload and out-of-line bytes in the owned-copy bound:
	// both make this sufficient admission test conservative.
	sharedSelection := false
	for _, source := range input.Vecs {
		// A single destination policy makes the cost comparison valid for
		// each charged account, rather than trading one column's quota for another.
		if !vector.AllocationAccountSelectionsEqual(source.AllocationAccountSelection(), allocation) {
			return nil, nil
		}
		if !source.IsConst() {
			copyBytes += int64(len(rows)) * int64(source.GetType().TypeSize())
		}
		if !source.IsDictionary() {
			// Queued views can outlive producer reuse. Owned backing then
			// remains charged while the producer allocates its replacement.
			if source.DataBackingKind() == vector.OwnedMPoolUnique {
				pinnedBytes += int64(cap(source.GetData()))
			}
			if source.AreaBackingKind() == vector.OwnedMPoolUnique {
				pinnedBytes += int64(cap(source.GetArea()))
			}
		}
		if source.IsDictionary() {
			selectors++
		} else {
			sharedSelection = true
		}
	}
	if sharedSelection {
		selectors++
	}
	if pinnedBytes+selectors*int64(len(rows))*8 > copyBytes {
		return nil, nil
	}
	if *workspace == nil {
		*workspace = &shufflePartitionViews{bases: make([]*vector.DictionaryBase, len(input.Vecs))}
	}
	s := *workspace
	if s.rows == nil {
		var err error
		s.rows, err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), allocation)
		if err != nil {
			return nil, err
		}
	}
	if err := s.rows.PreExtend(len(rows), proc.Mp()); err != nil {
		if mpool.IsRetryableAllocationCapacity(err) {
			s.free(proc.Mp())
			s.disabled = true
			return nil, nil
		}
		return nil, err
	}
	s.rows.SetLength(len(rows))
	logical := vector.MustFixedColNoTypeCheck[int64](s.rows)
	for i, row := range rows {
		logical[i] = int64(row)
	}
	selection, err := vector.NewRowSelection(logical, input.RowCount(), proc.Mp(), allocation)
	if err != nil {
		if mpool.IsRetryableAllocationCapacity(err) {
			s.free(proc.Mp())
			s.disabled = true
			return nil, nil
		}
		return nil, err
	}
	defer selection.Release()
	output := batch.NewOffHeapWithSize(len(input.Vecs))
	committed := false
	defer func() {
		if !committed {
			output.Clean(proc.Mp())
			// The ordinary destination must get the same remaining capacity
			// it had before this optional attempt, including after a partial
			// freeze. Published partitions keep their independent base leases.
			s.free(proc.Mp())
			s.disabled = true
		}
		if mpool.IsRetryableAllocationCapacity(retErr) || errors.Is(retErr, mpool.ErrAllocationAccountMismatch) {
			retErr = nil
		}
	}()
	batchAllocation := input.AllocationAccountSelection()
	if sp.outputAllocation != nil {
		batchAllocation = sp.outputAllocation
	}
	if err = output.SetAllocationAccount(batchAllocation); err != nil {
		return nil, err
	}
	var retainedBytes int64
	if sharedSelection {
		retainedBytes = selection.RetainedBytes()
	}
	for i, source := range input.Vecs {
		columnAllocation := source.AllocationAccountSelection()
		if sp.outputAllocation != nil {
			columnAllocation = sp.outputAllocation
		}
		if source.IsDictionary() {
			output.Vecs[i], err = source.SelectDictionary(selection, proc.Mp(), columnAllocation)
		} else {
			if s.bases[i] == nil {
				var admitted bool
				s.bases[i], admitted, err = vector.TryFreezeVector(source, proc.Mp(), columnAllocation)
				if err != nil {
					return nil, err
				}
				if !admitted {
					s.disabled = true
					return nil, nil
				}
				// Producer refill can need replacement storage under statement,
				// MPool or global capacity, regardless of allocation provenance.
				retainedBytes += s.bases[i].RetainedBytes()
			}
			output.Vecs[i], err = vector.NewDictionaryVector(s.bases[i], selection, proc.Mp(), columnAllocation)
		}
		if err != nil {
			return nil, err
		}
		// Allocated includes referenced backing; count header metadata separately.
		ownedMetadata := int64(output.Vecs[i].Allocated()) - output.Vecs[i].DictionaryRetainedBytes()
		retainedBytes += ownedMetadata
		if source.IsDictionary() {
			retainedBytes += output.Vecs[i].DictionaryRetainedBytes()
		}
	}
	if retainedBytes > copyBytes {
		return nil, nil
	}
	output.Attrs = append(output.Attrs, input.Attrs...)
	output.Recursive = input.Recursive
	output.SetRowCount(len(rows))
	committed = true
	return output, nil
}

func (shuffle *Shuffle) freePartitionViews(proc *process.Process) {
	if shuffle.ctr.partitionViews != nil {
		shuffle.ctr.partitionViews.free(proc.Mp())
		shuffle.ctr.partitionViews = nil
	}
}
