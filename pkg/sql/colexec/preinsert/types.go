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

package preinsert

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"

	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(PreInsert)

type proc = process.Process

type container struct {
	buf *batch.Batch
}
type PreInsert struct {
	ctr container

	HasAutoCol bool
	IsUpdate   bool
	SchemaName string
	TableDef   *pb.TableDef
	// letter case: origin
	Attrs []string

	EstimatedRowCount int64

	vm.OperatorBase
}

func (preInsert *PreInsert) GetOperatorBase() *vm.OperatorBase {
	return &preInsert.OperatorBase
}

func init() {
	reuse.CreatePool[PreInsert](
		func() *PreInsert {
			return &PreInsert{}
		},
		func(a *PreInsert) {
			*a = PreInsert{}
		},
		reuse.DefaultOptions[PreInsert]().
			WithEnableChecker(),
	)
}

func (preInsert PreInsert) TypeName() string {
	return opName
}

func NewArgument() *PreInsert {
	return reuse.Alloc[PreInsert](nil)
}

func (preInsert *PreInsert) Release() {
	if preInsert != nil {
		reuse.Free[PreInsert](preInsert, nil)
	}
}

func (preInsert *PreInsert) Reset(proc *process.Process, pipelineFailed bool, err error) {
}

func (preInsert *PreInsert) Free(proc *process.Process, pipelineFailed bool, err error) {
	if preInsert.ctr.buf != nil {
		for i, attr := range preInsert.Attrs {
			if idx, ok := preInsert.TableDef.Name2ColIndex[attr]; ok {
				if !preInsert.TableDef.Cols[idx].Typ.AutoIncr {
					preInsert.ctr.buf.SetVector(int32(i), nil)
				}
			}
		}
		preInsert.ctr.buf.Clean(proc.Mp())
		preInsert.ctr.buf = nil
	}
}
