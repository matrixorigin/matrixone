// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package productl2

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Productl2)

const (
	Build = iota
	Probe
	End
)

type container struct {
	state    int
	probeIdx int
	bat      *batch.Batch // build batch
	rbat     *batch.Batch
	inBat    *batch.Batch // probe batch
}

type Productl2 struct {
	ctr        container
	Typs       []types.Type
	Result     []colexec.ResultPos
	OnExpr     *plan.Expr
	JoinMapTag int32

	vm.OperatorBase
	colexec.Projection
}

func (productl2 *Productl2) GetOperatorBase() *vm.OperatorBase {
	return &productl2.OperatorBase
}

func init() {
	reuse.CreatePool[Productl2](
		func() *Productl2 {
			return &Productl2{}
		},
		func(a *Productl2) {
			*a = Productl2{}
		},
		reuse.DefaultOptions[Productl2]().
			WithEnableChecker(),
	)
}

func (productl2 Productl2) TypeName() string {
	return opName
}

func NewArgument() *Productl2 {
	return reuse.Alloc[Productl2](nil)
}

func (productl2 *Productl2) Release() {
	if productl2 != nil {
		reuse.Free[Productl2](productl2, nil)
	}
}

func (productl2 *Productl2) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if productl2.ctr.bat != nil {
		productl2.ctr.bat.CleanOnlyData()
	}
	if productl2.ctr.rbat != nil {
		productl2.ctr.rbat.CleanOnlyData()
	}
	productl2.ctr.inBat = nil
	if productl2.ProjectList != nil {
		//anal := proc.GetAnalyze(productl2.GetIdx(), productl2.GetParallelIdx(), productl2.GetParallelMajor())
		//anal.Alloc(productl2.ProjectAllocSize)
		if productl2.OpAnalyzer != nil {
			productl2.OpAnalyzer.Alloc(productl2.ProjectAllocSize)
		}
		productl2.ResetProjection(proc)
	}
	productl2.ctr.state = Build
	productl2.ctr.probeIdx = 0
}

func (productl2 *Productl2) Free(proc *process.Process, pipelineFailed bool, err error) {
	productl2.ctr.cleanBatch(proc.Mp())
	productl2.FreeProjection(proc)
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
		ctr.bat = nil
	}
	if ctr.rbat != nil {
		ctr.rbat.Clean(mp)
		ctr.rbat = nil
	}
	ctr.inBat = nil
}
