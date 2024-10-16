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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"

	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(PreInsert)

type proc = process.Process

type container struct {
	buf               *batch.Batch
	canFreeVecIdx     map[int]bool //auto incr & expand constant vecotr.need free
	clusterByExecutor colexec.ExpressionExecutor
	compPkExecutor    colexec.ExpressionExecutor
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
	CompPkeyExpr      *plan.Expr
	ClusterByExpr     *plan.Expr

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
	if preInsert.ctr.compPkExecutor != nil {
		preInsert.ctr.compPkExecutor.ResetForNextQuery()
	}
	if preInsert.ctr.clusterByExecutor != nil {
		preInsert.ctr.clusterByExecutor.ResetForNextQuery()
	}
}

func (preInsert *PreInsert) Free(proc *process.Process, pipelineFailed bool, err error) {
	if preInsert.ctr.compPkExecutor != nil {
		preInsert.ctr.compPkExecutor.Free()
		preInsert.ctr.compPkExecutor = nil
	}
	if preInsert.ctr.clusterByExecutor != nil {
		preInsert.ctr.clusterByExecutor.Free()
		preInsert.ctr.clusterByExecutor = nil
	}
	if preInsert.ctr.buf != nil {
		for idx := range preInsert.Attrs {
			if _, ok := preInsert.ctr.canFreeVecIdx[idx]; !ok {
				preInsert.ctr.buf.SetVector(int32(idx), nil)
			}
		}
		idx := len(preInsert.Attrs)
		if preInsert.CompPkeyExpr != nil {
			if idx < len(preInsert.ctr.buf.Vecs) {
				preInsert.ctr.buf.SetVector(int32(idx), nil)
			}
			idx += 1
		}
		if preInsert.ClusterByExpr != nil {
			if idx < len(preInsert.ctr.buf.Vecs) {
				preInsert.ctr.buf.SetVector(int32(idx), nil)
			}
		}
		preInsert.ctr.buf.Clean(proc.Mp())
		preInsert.ctr.buf = nil
	}
}
