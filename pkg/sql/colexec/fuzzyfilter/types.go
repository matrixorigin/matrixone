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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Argument)

const (
	Build = iota
	Probe
	End
)

type Argument struct {
	ctr *container

	// Number of items in the filter
	N float64
	T types.T
	PkName string

	useRoaring bool

	bloomFilter   *bloomfilter.BloomFilter
	roaringFilter *roaringFilter

	collisionCnt int
	rbat         *batch.Batch

	info     *vm.OperatorInfo
	children []vm.Operator
}

type container struct {
	colexec.ReceiverOperator
	state int
}

func (arg *Argument) SetInfo(info *vm.OperatorInfo) {
	arg.info = info
}

func (arg *Argument) AppendChild(child vm.Operator) {
	arg.children = append(arg.children, child)
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	if arg.bloomFilter != nil {
		arg.bloomFilter.Clean()
	}
	if arg.roaringFilter != nil {
		arg.roaringFilter = nil
	}
	if arg.rbat != nil {
		arg.rbat.Clean(proc.GetMPool())
		arg.rbat = nil
	}
	ctr := arg.ctr
	if ctr != nil {
		ctr.FreeAllReg()
	}
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
