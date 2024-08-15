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

package limit

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Limit)

type container struct {
	seen          uint64 // seen is the number of tuples seen so far
	limit         uint64
	limitExecutor colexec.ExpressionExecutor
}
type Limit struct {
	ctr       *container
	LimitExpr *plan.Expr

	vm.OperatorBase
}

func (limit *Limit) GetOperatorBase() *vm.OperatorBase {
	return &limit.OperatorBase
}

func init() {
	reuse.CreatePool[Limit](
		func() *Limit {
			return &Limit{}
		},
		func(a *Limit) {
			*a = Limit{}
		},
		reuse.DefaultOptions[Limit]().
			WithEnableChecker(),
	)
}

func (limit Limit) TypeName() string {
	return opName
}

func NewArgument() *Limit {
	return reuse.Alloc[Limit](nil)
}

func (limit *Limit) WithLimit(limitExpr *plan.Expr) *Limit {
	limit.LimitExpr = limitExpr
	return limit
}

func (limit *Limit) Release() {
	if limit != nil {
		reuse.Free[Limit](limit, nil)
	}
}

func (limit *Limit) Reset(proc *process.Process, pipelineFailed bool, err error) {
	limit.Free(proc, pipelineFailed, err)
}

func (limit *Limit) Free(proc *process.Process, pipelineFailed bool, err error) {
	if limit.ctr != nil {
		if limit.ctr.limitExecutor != nil {
			limit.ctr.limitExecutor.Free()
			limit.ctr.limitExecutor = nil
		}
		limit.ctr = nil
	}

}
