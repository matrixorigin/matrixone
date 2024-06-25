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

var _ vm.Operator = new(Argument)

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

type Argument struct {
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

func (arg *Argument) GetOperatorBase() *vm.OperatorBase {
	return &arg.OperatorBase
}

func init() {
	reuse.CreatePool[Argument](
		func() *Argument {
			return &Argument{}
		},
		func(a *Argument) {
			*a = Argument{}
		},
		reuse.DefaultOptions[Argument]().
			WithEnableChecker(),
	)
}

func (arg Argument) TypeName() string {
	return argName
}

func NewArgument() *Argument {
	return reuse.Alloc[Argument](nil)
}

func (arg *Argument) Release() {
	if arg != nil {
		reuse.Free[Argument](arg, nil)
	}
}

func (arg *Argument) ifBuildOnSink() bool {
	return arg.BuildIdx == 1
}

func (arg *Argument) getProbeIdx() int {
	return 1 - arg.BuildIdx
}

func (arg *Argument) Reset(proc *process.Process, pipelineFailed bool, err error) {
	arg.Free(proc, pipelineFailed, err)
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	proc.FinalizeRuntimeFilter(arg.RuntimeFilterSpec)
	if arg.ctr.bloomFilter != nil {
		arg.ctr.bloomFilter.Clean()
		arg.ctr.bloomFilter = nil
	}
	if arg.ctr.roaringFilter != nil {
		arg.ctr.roaringFilter = nil
	}
	if arg.ctr.rbat != nil {
		arg.ctr.rbat.Clean(proc.GetMPool())
		arg.ctr.rbat = nil
	}
	if arg.ctr.pass2RuntimeFilter != nil {
		arg.ctr.pass2RuntimeFilter.Free(proc.GetMPool())
		arg.ctr.pass2RuntimeFilter = nil
	}

	arg.ctr.FreeAllReg()
}

func (arg *Argument) add(pkCol *vector.Vector) {
	ctr := arg.ctr
	if ctr.roaringFilter != nil {
		ctr.roaringFilter.addFunc(ctr.roaringFilter, pkCol)
	} else {
		ctr.bloomFilter.Add(pkCol)
	}
}

func (arg *Argument) test(proc *process.Process, pkCol *vector.Vector) error {
	ctr := arg.ctr
	if ctr.roaringFilter != nil {
		idx, dupVal := ctr.roaringFilter.testFunc(ctr.roaringFilter, pkCol)
		if idx == -1 {
			return nil
		} else {
			return moerr.NewDuplicateEntry(proc.Ctx, valueToString(dupVal), arg.PkName)
		}
	} else {
		ctr.bloomFilter.Test(pkCol, func(exist bool, i int) {
			if exist {
				if ctr.collisionCnt < maxCheckDupCount {
					arg.appendCollisionKey(proc, i, pkCol)
				}
			}
		})
	}
	return nil
}

func (arg *Argument) testAndAdd(proc *process.Process, pkCol *vector.Vector) error {
	ctr := arg.ctr
	if ctr.roaringFilter != nil {
		idx, dupVal := ctr.roaringFilter.testAndAddFunc(ctr.roaringFilter, pkCol)
		if idx == -1 {
			return nil
		} else {
			return moerr.NewDuplicateEntry(proc.Ctx, valueToString(dupVal), arg.PkName)
		}
	} else {
		ctr.bloomFilter.TestAndAdd(pkCol, func(exist bool, i int) {
			if exist {
				if ctr.collisionCnt < maxCheckDupCount {
					arg.appendCollisionKey(proc, i, pkCol)
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
