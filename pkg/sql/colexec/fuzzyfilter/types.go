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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(FuzzyFilter)

const (
	Build = iota
	HandleRuntimeFilter
	Probe
	End
)

type container struct {
	state int
	colexec.ReceiverOperator

	bloomFilter   *bloomfilter.BloomFilter
	roaringFilter *roaringFilter

	// buildCnt     int
	// probeCnt     int
	collisionCnt int
	rbat         *batch.Batch

	// about runtime filter
	pass2RuntimeFilter *vector.Vector
}

type FuzzyFilter struct {
	ctr *container

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

func (fuzzyFilter *FuzzyFilter) GetOperatorBase() *vm.OperatorBase {
	return &fuzzyFilter.OperatorBase
}

func init() {
	reuse.CreatePool[FuzzyFilter](
		func() *FuzzyFilter {
			return &FuzzyFilter{}
		},
		func(a *FuzzyFilter) {
			*a = FuzzyFilter{}
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
	fuzzyFilter.Free(proc, pipelineFailed, err)
}

func (fuzzyFilter *FuzzyFilter) Free(proc *process.Process, pipelineFailed bool, err error) {
	proc.FinalizeRuntimeFilter(fuzzyFilter.RuntimeFilterSpec)
	if fuzzyFilter.ctr.bloomFilter != nil {
		fuzzyFilter.ctr.bloomFilter.Clean()
		fuzzyFilter.ctr.bloomFilter = nil
	}
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

	fuzzyFilter.ctr.FreeAllReg()
}

func (fuzzyFilter *FuzzyFilter) add(pkCol *vector.Vector) {
	ctr := fuzzyFilter.ctr
	if ctr.roaringFilter != nil {
		ctr.roaringFilter.addFunc(ctr.roaringFilter, pkCol)
	} else {
		ctr.bloomFilter.Add(pkCol)
	}
}

func (fuzzyFilter *FuzzyFilter) test(proc *process.Process, pkCol *vector.Vector) error {
	ctr := fuzzyFilter.ctr
	if ctr.roaringFilter != nil {
		idx, dupVal := ctr.roaringFilter.testFunc(ctr.roaringFilter, pkCol)
		if idx == -1 {
			return nil
		} else {
			return moerr.NewDuplicateEntry(proc.Ctx, valueToString(dupVal), fuzzyFilter.PkName)
		}
	} else {
		ctr.bloomFilter.Test(pkCol, func(exist bool, i int) {
			if exist {
				if ctr.collisionCnt < maxCheckDupCount {
					fuzzyFilter.appendCollisionKey(proc, i, pkCol)
				}
			}
		})
	}
	return nil
}

func (fuzzyFilter *FuzzyFilter) testAndAdd(proc *process.Process, pkCol *vector.Vector) error {
	ctr := fuzzyFilter.ctr
	if ctr.roaringFilter != nil {
		idx, dupVal := ctr.roaringFilter.testAndAddFunc(ctr.roaringFilter, pkCol)
		if idx == -1 {
			return nil
		} else {
			return moerr.NewDuplicateEntry(proc.Ctx, valueToString(dupVal), fuzzyFilter.PkName)
		}
	} else {
		ctr.bloomFilter.TestAndAdd(pkCol, func(exist bool, i int) {
			if exist {
				if ctr.collisionCnt < maxCheckDupCount {
					fuzzyFilter.appendCollisionKey(proc, i, pkCol)
					return
				}
				logutil.Debugf("too many collision for fuzzy filter")
			}
		})
	}
	return nil
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
