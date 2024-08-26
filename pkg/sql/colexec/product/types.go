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

package product

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Product)

const (
	Build = iota
	Probe
	End
)

type container struct {
	state    int
	probeIdx int
	bat      *batch.Batch
	rbat     *batch.Batch
	inBat    *batch.Batch
}

type Product struct {
	ctr        container
	Typs       []types.Type
	Result     []colexec.ResultPos
	IsShuffle  bool
	JoinMapTag int32

	vm.OperatorBase
	colexec.Projection
}

func (product *Product) GetOperatorBase() *vm.OperatorBase {
	return &product.OperatorBase
}

func init() {
	reuse.CreatePool[Product](
		func() *Product {
			return &Product{}
		},
		func(a *Product) {
			*a = Product{}
		},
		reuse.DefaultOptions[Product]().
			WithEnableChecker(),
	)
}

func (product Product) TypeName() string {
	return opName
}

func NewArgument() *Product {
	return reuse.Alloc[Product](nil)
}

func (product *Product) Release() {
	if product != nil {
		reuse.Free[Product](product, nil)
	}
}

func (product *Product) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if product.ctr.bat != nil {
		product.ctr.bat.CleanOnlyData()
	}
	if product.ctr.rbat != nil {
		product.ctr.rbat.CleanOnlyData()
	}
	product.ctr.inBat = nil
	if product.ProjectList != nil {
		//anal := proc.GetAnalyze(product.GetIdx(), product.GetParallelIdx(), product.GetParallelMajor())
		//anal.Alloc(product.ProjectAllocSize)
		if product.OpAnalyzer != nil {
			product.OpAnalyzer.Alloc(product.ProjectAllocSize)
		}
		product.ResetProjection(proc)
	}
	product.ctr.state = Build
	product.ctr.probeIdx = 0
}

func (product *Product) Free(proc *process.Process, pipelineFailed bool, err error) {
	product.ctr.cleanBatch(proc.Mp())
	product.FreeProjection(proc)
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
