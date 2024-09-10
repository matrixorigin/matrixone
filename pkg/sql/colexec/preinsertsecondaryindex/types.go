// Copyright 2022 Matrix Origin
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

package preinsertsecondaryindex

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/util"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type container struct {
	buf *batch.Batch
}
type PreInsertSecIdx struct {
	ctr          container
	PreInsertCtx *plan.PreInsertUkCtx

	packer util.PackerList

	vm.OperatorBase
}

func (preInsertSecIdx *PreInsertSecIdx) GetOperatorBase() *vm.OperatorBase {
	return &preInsertSecIdx.OperatorBase
}

func init() {
	reuse.CreatePool[PreInsertSecIdx](
		func() *PreInsertSecIdx {
			return &PreInsertSecIdx{}
		},
		func(a *PreInsertSecIdx) {
			*a = PreInsertSecIdx{}
		},
		reuse.DefaultOptions[PreInsertSecIdx]().
			WithEnableChecker(),
	)
}

func (preInsertSecIdx PreInsertSecIdx) TypeName() string {
	return opName
}

func NewArgument() *PreInsertSecIdx {
	return reuse.Alloc[PreInsertSecIdx](nil)
}

func (preInsertSecIdx *PreInsertSecIdx) Release() {
	if preInsertSecIdx != nil {
		reuse.Free[PreInsertSecIdx](preInsertSecIdx, nil)
	}
}

func (preInsertSecIdx *PreInsertSecIdx) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if preInsertSecIdx.ctr.buf != nil {
		preInsertSecIdx.ctr.buf.CleanOnlyData()
	}
	if preInsertSecIdx.packer.PackerCount() > 10 {
		preInsertSecIdx.packer.Free()
	}
}

func (preInsertSecIdx *PreInsertSecIdx) Free(proc *process.Process, pipelineFailed bool, err error) {
	if preInsertSecIdx.ctr.buf != nil {
		preInsertSecIdx.ctr.buf.Clean(proc.Mp())
		preInsertSecIdx.ctr.buf = nil
	}
	preInsertSecIdx.packer.Free()
}
