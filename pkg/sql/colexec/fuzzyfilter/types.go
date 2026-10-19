// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fuzzyfilter

import (
	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(FuzzyFilter)

const (
	Build = iota
	HandleRuntimeFilter
	Probe
	End
)

const (
	fuzzyFilterAllocationSiteRuntimeFilterData mpool.AllocationSite = iota + 1
	fuzzyFilterAllocationSiteRuntimeFilterArea
	fuzzyFilterAllocationSiteRuntimeFilterNulls
	fuzzyFilterAllocationSiteRuntimeFilterGrouping
	fuzzyFilterAllocationSiteRuntimeFilterPayload
)

type container struct {
	state int

	bloomFilter   bloomfilter.BloomFilter
	roaringFilter *roaringFilter

	// buildCnt     int
	// probeCnt     int
	collisionCnt int
	rbat         *batch.Batch

	// about runtime filter
	pass2RuntimeFilter  *vector.Vector
	runtimeFilterUsable bool
	runtimeFilterDone   bool
}

type FuzzyFilter struct {
	ctr                     container
	allocationAccount       *mpool.AllocationAccount
	runtimeFilterAllocation *vector.AllocationAccountSelection
	filterBudget            *process.ExecutionResourceGeneration
	filterCharge            uint64
	keyCharge               uint64

	// Estimates of the number of data items obtained from statistical information
	N                  float64
	PkName             string
	PkTyp              plan.Type
	BuildIdx           int
	Callback           func(bat *batch.Batch) error
	IfInsertFromUnique bool

	RuntimeFilterSpec *plan.RuntimeFilterSpec
	vm.OperatorBase
}

func (fuzzyFilter *FuzzyFilter) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if account == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if fuzzyFilter.allocationAccount != nil {
		if fuzzyFilter.allocationAccount == account {
			return nil
		}
		return mpool.ErrAllocationAccountMismatch
	}
	selection, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerFuzzyFilter,
		fuzzyFilterAllocationSiteRuntimeFilterData,
		fuzzyFilterAllocationSiteRuntimeFilterArea,
		fuzzyFilterAllocationSiteRuntimeFilterNulls,
		fuzzyFilterAllocationSiteRuntimeFilterGrouping,
	)
	if err != nil {
		return err
	}
	fuzzyFilter.allocationAccount = account
	fuzzyFilter.runtimeFilterAllocation = selection
	return nil
}

func (fuzzyFilter *FuzzyFilter) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if fuzzyFilter.allocationAccount == nil {
		return nil
	}
	if fuzzyFilter.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if fuzzyFilter.ctr.pass2RuntimeFilter != nil || fuzzyFilter.filterCharge != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	fuzzyFilter.allocationAccount = nil
	fuzzyFilter.runtimeFilterAllocation = nil
	return nil
}

func (fuzzyFilter *FuzzyFilter) GetOperatorBase() *vm.OperatorBase {
	return &fuzzyFilter.OperatorBase
}

func init() {
	reuse.CreatePool[FuzzyFilter](
		func() *FuzzyFilter {
			return &FuzzyFilter{}
		},
		func(f *FuzzyFilter) {
			*f = FuzzyFilter{}
		},
		reuse.DefaultOptions[FuzzyFilter]().
			WithEnableChecker(),
	)
}

func (fuzzyFilter FuzzyFilter) TypeName() string {
	return opName
}

func NewArgument() *FuzzyFilter {
	return reuse.Alloc[FuzzyFilter](nil)
}

func (fuzzyFilter *FuzzyFilter) Release() {
	if fuzzyFilter != nil {
		reuse.Free[FuzzyFilter](fuzzyFilter, nil)
	}
}

func (fuzzyFilter *FuzzyFilter) ifBuildOnSink() bool {
	return fuzzyFilter.BuildIdx == 1
}

func (fuzzyFilter *FuzzyFilter) getProbeIdx() int {
	return 1 - fuzzyFilter.BuildIdx
}

func (fuzzyFilter *FuzzyFilter) Reset(proc *process.Process, pipelineFailed bool, err error) {
	defer func() {
		if fuzzyFilter.allocationAccount != nil {
			fuzzyFilter.Free(proc, pipelineFailed, err)
		}
	}()
	runtimeSucceed := fuzzyFilter.ctr.state > HandleRuntimeFilter

	if !fuzzyFilter.ctr.runtimeFilterDone {
		if !runtimeSucceed && (pipelineFailed || err != nil) {
			message.FinalizeRuntimeFilterOnBuildError(
				fuzzyFilter.RuntimeFilterSpec, proc.GetMessageBoard())
		} else {
			message.FinalizeRuntimeFilter(
				fuzzyFilter.RuntimeFilterSpec, runtimeSucceed, proc.GetMessageBoard())
		}
		fuzzyFilter.ctr.runtimeFilterDone =
			fuzzyFilter.RuntimeFilterSpec != nil
	}
	ctr := &fuzzyFilter.ctr
	ctr.state = Build
	ctr.runtimeFilterUsable = false
	ctr.collisionCnt = 0
	if ctr.pass2RuntimeFilter != nil {
		ctr.pass2RuntimeFilter.Free(proc.Mp())
		ctr.pass2RuntimeFilter = nil
	}
	if ctr.rbat != nil {
		ctr.rbat.CleanOnlyData()
	}

	if ctr.roaringFilter != nil {
		ctr.roaringFilter.b.Clear()
	}

	ctr.bloomFilter.Reset()
}

func (fuzzyFilter *FuzzyFilter) Free(proc *process.Process, pipelineFailed bool, err error) {
	fuzzyFilter.ctr.bloomFilter.Clean()
	if fuzzyFilter.ctr.roaringFilter != nil {
		fuzzyFilter.ctr.roaringFilter = nil
	}
	if fuzzyFilter.ctr.rbat != nil {
		fuzzyFilter.ctr.rbat.Clean(proc.GetMPool())
		fuzzyFilter.ctr.rbat = nil
	}
	if fuzzyFilter.ctr.pass2RuntimeFilter != nil {
		fuzzyFilter.ctr.pass2RuntimeFilter.Free(proc.GetMPool())
		fuzzyFilter.ctr.pass2RuntimeFilter = nil
	}
	if fuzzyFilter.filterBudget != nil {
		fuzzyFilter.filterBudget.ReleaseAllocationCapacity(fuzzyFilter.filterCharge)
	}
	fuzzyFilter.filterBudget, fuzzyFilter.filterCharge, fuzzyFilter.keyCharge = nil, 0, 0

}

func (fuzzyFilter *FuzzyFilter) reserveFilter(proc *process.Process, size uint64) error {
	if fuzzyFilter.allocationAccount == nil || size == 0 {
		return nil
	}
	budget, err := proc.GetExecutionResourceBudget()
	if err != nil {
		return err
	}
	if err = budget.AcquireAllocationCapacity(size); err != nil {
		return err
	}
	fuzzyFilter.filterBudget = budget
	fuzzyFilter.filterCharge += size
	return nil
}

// admitFilterInput covers the copy peak of dynamic Go backing. Roaring adds
// at most one small array/container per input value; conversion to a bitmap
// replaces a similarly sized array. Reconcile that upper bound after use.
func (fuzzyFilter *FuzzyFilter) admitFilterInput(proc *process.Process, v *vector.Vector) (func(), error) {
	if fuzzyFilter.allocationAccount == nil {
		return func() {}, nil
	}
	if fuzzyFilter.ctr.roaringFilter != nil {
		current := fuzzyFilter.roaringMemoryBound()
		target := current + uint64(v.Length())*256
		if target > fuzzyFilter.filterCharge {
			if err := fuzzyFilter.reserveFilter(proc, target-fuzzyFilter.filterCharge); err != nil {
				return nil, err
			}
		}
		return func() {
			retained := fuzzyFilter.roaringMemoryBound()
			if retained < fuzzyFilter.filterCharge {
				fuzzyFilter.filterBudget.ReleaseAllocationCapacity(fuzzyFilter.filterCharge - retained)
				fuzzyFilter.filterCharge = retained
			}
		}, nil
	}
	width := uint64(v.GetType().TypeSize())
	if !v.GetType().IsFixedLen() {
		width = 0
		for row := 0; row < v.Length(); row++ {
			width = max(width, uint64(len(v.GetBytesAt(row))))
		}
	}
	// Go append can hold the old and new key buffers simultaneously. Include
	// its size-class rounding, the null marker, and the hash padding.
	target := (4*width + 128) * uint64(hashmap.UnitLimit)
	if target > fuzzyFilter.keyCharge {
		if err := fuzzyFilter.reserveFilter(proc, target-fuzzyFilter.keyCharge); err != nil {
			return nil, err
		}
		fuzzyFilter.keyCharge = target
	}
	return func() {}, nil
}

func (fuzzyFilter *FuzzyFilter) roaringMemoryBound() uint64 {
	b := fuzzyFilter.ctr.roaringFilter.b
	// GetSizeInBytes describes live payload lengths; include spare capacity,
	// Go container headers, and the three high-key directory slices as well.
	return 2*b.GetSizeInBytes() + uint64(b.Stats().Containers)*192 + 128
}

func (fuzzyFilter *FuzzyFilter) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (fuzzyFilter *FuzzyFilter) add(pkCol *vector.Vector) {
	ctr := &fuzzyFilter.ctr
	if ctr.roaringFilter != nil {
		ctr.roaringFilter.addFunc(ctr.roaringFilter, pkCol)
	} else {
		ctr.bloomFilter.Add(pkCol)
	}
}

func (fuzzyFilter *FuzzyFilter) test(proc *process.Process, pkCol *vector.Vector) error {
	ctr := &fuzzyFilter.ctr
	var appendErr error
	if ctr.roaringFilter != nil {
		idx, dupVal := ctr.roaringFilter.testFunc(ctr.roaringFilter, pkCol)
		if idx == -1 {
			return nil
		} else {
			return moerr.NewDuplicateEntry(proc.Ctx, valueToString(dupVal), fuzzyFilter.PkName)
		}
	} else {
		ctr.bloomFilter.Test(pkCol, func(exist bool, i int) {
			if exist && appendErr == nil {
				if ctr.collisionCnt < maxCheckDupCount {
					appendErr = fuzzyFilter.appendCollisionKey(proc, i, pkCol)
				}
			}
		})
	}
	return appendErr
}

func (fuzzyFilter *FuzzyFilter) testAndAdd(proc *process.Process, pkCol *vector.Vector) error {
	ctr := &fuzzyFilter.ctr
	var appendErr error
	if ctr.roaringFilter != nil {
		idx, dupVal := ctr.roaringFilter.testAndAddFunc(ctr.roaringFilter, pkCol)
		if idx == -1 {
			return nil
		} else {
			return moerr.NewDuplicateEntry(proc.Ctx, valueToString(dupVal), fuzzyFilter.PkName)
		}
	} else {
		ctr.bloomFilter.TestAndAdd(pkCol, func(exist bool, i int) {
			if exist && appendErr == nil {
				if ctr.collisionCnt < maxCheckDupCount {
					appendErr = fuzzyFilter.appendCollisionKey(proc, i, pkCol)
					return
				}
				logutil.Debugf("too many collision for fuzzy filter")
			}
		})
	}
	return appendErr
}

func IfCanUseRoaringFilter(t types.T) bool {
	switch t {
	case types.T_int8, types.T_int16, types.T_int32:
		return true
	case types.T_uint8, types.T_uint16, types.T_uint32:
		return true
	default:
		return false
	}
}
