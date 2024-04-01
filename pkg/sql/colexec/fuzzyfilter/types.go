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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

type Argument struct {
	state int
	colexec.ReceiverOperator

	// Estimates of the number of data items obtained from statistical information
	N      float64
	PkName string
	PkTyp  *plan.Type

	bloomFilter   *bloomfilter.BloomFilter
	roaringFilter *roaringFilter

	// buildCnt     int
	// probeCnt     int
	collisionCnt int
	rbat         *batch.Batch

	// about runtime filter
	inFilterCardLimit    int64
	pass2RuntimeFilter   *vector.Vector
	RuntimeFilterSpecs   []*plan.RuntimeFilterSpec
	RuntimeFilterSenders []*colexec.RuntimeFilterChan

	info     *vm.OperatorInfo
	children []vm.Operator
}

func (arg *Argument) SetRuntimeFilterSenders(rfs []*colexec.RuntimeFilterChan) {
	arg.RuntimeFilterSenders = rfs
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
		arg.bloomFilter = nil
	}
	if arg.roaringFilter != nil {
		arg.roaringFilter = nil
	}
	if arg.rbat != nil {
		arg.rbat.Clean(proc.GetMPool())
		arg.rbat = nil
	}
	if arg.pass2RuntimeFilter != nil {
		proc.PutVector(arg.pass2RuntimeFilter)
	}

	arg.FreeAllReg()
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
