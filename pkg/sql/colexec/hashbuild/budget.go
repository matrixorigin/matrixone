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
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// setBudget retains the statement generation for non-memory resource ledgers
// such as spill files and disk. Physical memory admission is exclusively
// driven by the allocation account installed through SetAllocationAccount.
func (hb *HashmapBuilder) setBudget(
	budget *process.HashBuildBudgetGeneration,
) {
	hb.budget = budget
}

// SetBudget is the exported boundary used by spill and integration tests.
func (hb *HashmapBuilder) SetBudget(
	budget *process.HashBuildBudgetGeneration,
) {
	hb.setBudget(budget)
}

// NewAccountedEmptyJoinMap creates a consumer-grown map under the statement
// allocation generation. The map and its string-key iterator scratch carry
// the same immutable provenance as producer-built maps.
func NewAccountedEmptyJoinMap(
	keyWidth int,
	account *mpool.AllocationAccount,
	mp *mpool.MPool,
) (*message.JoinMap, error) {
	if account == nil || mp == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	selection, err := hashtable.NewAllocationAccountSelection(
		account,
		HashBuildAllocationOwner,
		HashBuildAllocationSiteHashCell,
		HashBuildAllocationSiteHashDescriptor,
	)
	if err != nil {
		return nil, err
	}
	var (
		intHashMap *hashmap.IntHashMap
		strHashMap *hashmap.StrHashMap
	)
	if keyWidth <= 8 {
		intHashMap, err = hashmap.NewIntHashMapWithAllocation(
			false,
			mp,
			selection,
		)
	} else {
		iteratorAllocation, allocationErr := hashmap.NewIteratorAllocation(
			account,
			HashBuildAllocationOwner,
			HashBuildAllocationSiteHashIterator,
		)
		if allocationErr != nil {
			return nil, allocationErr
		}
		strHashMap, err = hashmap.NewStrHashMapWithAllocations(
			false,
			mp,
			selection,
			iteratorAllocation,
		)
	}
	if err != nil {
		return nil, err
	}

	jm := message.NewJoinMap(
		message.GroupSels{},
		intHashMap,
		strHashMap,
		nil,
		nil,
		mp,
	)
	jm.IncRef(1)
	return jm, nil
}

func (hb *HashmapBuilder) copyBuildBatch(
	src *batch.Batch,
	proc *process.Process,
) error {
	if hb.batchAllocation == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return hb.Batches.CopyIntoBatchesWithAllocation(
		src,
		proc,
		hb.batchAllocation,
	)
}

// CopyBuildBatch is the exported boundary used by spill and integration tests.
func (hb *HashmapBuilder) CopyBuildBatch(
	src *batch.Batch,
	proc *process.Process,
) error {
	return hb.copyBuildBatch(src, proc)
}

func (hb *HashmapBuilder) cleanBatches(proc *process.Process) {
	hb.Batches.Clean(proc.Mp())
}

// abandonOptionalRuntimeFilterKeys removes only the optional exact-filter
// owner from an in-progress mandatory map build. Physical vector frees are the
// single source of truth for releasing the account charge.
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
// allocation failure into in-place key abandonment. Fatal causes keep builder
// ownership with terminal cleanup.
func (hb *HashmapBuilder) fallbackOptionalRuntimeFilterCollection(
	proc *process.Process,
	cause error,
) error {
	if runtimefilter.ClassifyOptionalFallback(cause) ==
		runtimefilter.OptionalFallbackNone {
		return cause
	}
	return hb.abandonOptionalRuntimeFilterKeys(proc)
}

// releaseOptionalRuntimeFilterKeys drops terminal producer-only vectors. The
// backing MPool allocations release their allocation-account charges exactly
// once when each vector is freed.
func (hb *HashmapBuilder) releaseOptionalRuntimeFilterKeys(
	proc *process.Process,
) error {
	if proc == nil {
		return process.ErrHashBuildBudgetInvalid
	}
	for i := range hb.UniqueJoinKeys {
		if hb.UniqueJoinKeys[i] != nil {
			hb.UniqueJoinKeys[i].Free(proc.Mp())
		}
	}
	hb.UniqueJoinKeys = nil
	hb.uniqueSels = nil
	return nil
}

// prepareCanonicalRuntimeFilterCollection restarts optional key collection
// after a destructive Dedup rewrite. The rewritten mandatory map is already
// charged at its physical allocation sites; optional growth may fail open at
// the vector allocation boundary during the rebuild.
func (hb *HashmapBuilder) prepareCanonicalRuntimeFilterCollection(
	requested bool,
) (bool, error) {
	if !requested {
		return false, nil
	}
	hb.runtimeFilterCollectionFallback = false
	return true, nil
}

func (hb *HashmapBuilder) marshalRuntimeFilterVector(
	vec *vector.Vector,
	mp *mpool.MPool,
) ([]byte, func(), error) {
	return runtimefilter.MarshalExactFilterVector(
		vec,
		mp,
		hb.mapAllocationAccount,
		HashBuildAllocationOwner,
		HashBuildAllocationSiteRuntimeFilterPayload,
	)
}
