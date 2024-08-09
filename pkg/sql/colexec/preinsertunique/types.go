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

package preinsertunique

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/util"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(PreInsertUnique)

type container struct {
	buf *batch.Batch
}
type PreInsertUnique struct {
	ctr          container
	PreInsertCtx *plan.PreInsertUkCtx

	packers util.PackerList

	vm.OperatorBase
}

func (preInsertUnique *PreInsertUnique) GetOperatorBase() *vm.OperatorBase {
	return &preInsertUnique.OperatorBase
}

func init() {
	reuse.CreatePool[PreInsertUnique](
		func() *PreInsertUnique {
			return &PreInsertUnique{}
		},
		func(a *PreInsertUnique) {
			*a = PreInsertUnique{}
		},
		reuse.DefaultOptions[PreInsertUnique]().
			WithEnableChecker(),
	)
}

func (preInsertUnique PreInsertUnique) TypeName() string {
	return opName
}

func NewArgument() *PreInsertUnique {
	return reuse.Alloc[PreInsertUnique](nil)
}

func (preInsertUnique *PreInsertUnique) Release() {
	if preInsertUnique != nil {
		reuse.Free[PreInsertUnique](preInsertUnique, nil)
	}
}

func (preInsertUnique *PreInsertUnique) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if preInsertUnique.ctr.buf != nil {
		preInsertUnique.ctr.buf.CleanOnlyData()
	}
}

func (preInsertUnique *PreInsertUnique) Free(proc *process.Process, pipelineFailed bool, err error) {
	if preInsertUnique.ctr.buf != nil {
		preInsertUnique.ctr.buf.Clean(proc.Mp())
		preInsertUnique.ctr.buf = nil
	}

	preInsertUnique.packers.Free()
}
