// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package odkueval

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Check represents one compiled assertion. Checks preserve the plan's order;
// only an exact scalar NOT NULL assertion omits an expression executor.
type Check struct {
	nonNullColumn int32
	message       string
	executor      colexec.ExpressionExecutor
}

// FreeChecks releases the executors owned by checks. It is safe to call again.
func FreeChecks(checks []Check) {
	for i := range checks {
		if checks[i].executor != nil {
			checks[i].executor.Free()
			checks[i].executor = nil
		}
	}
}

func validateCheck(e *plan.Expr, leftTypes, rightTypes []types.Type, proc *process.Process) error {
	if e == nil {
		return moerr.NewInternalError(proc.Ctx, "nil ODKU action check")
	}
	if f := e.GetF(); f != nil {
		if f.Func == nil {
			return moerr.NewInternalError(proc.Ctx, "missing ODKU check function")
		}
		id, overload := function.DecodeOverloadID(f.Func.Obj)
		if id == function.CHECK_CONSTRAINT_ASSERT || id == function.ISNOTNULL {
			arity := 1
			if id == function.CHECK_CONSTRAINT_ASSERT {
				arity = 2
			}
			if overload != 0 || len(f.Args) != arity || e.Typ.Id != int32(types.T_bool) {
				return moerr.NewInternalError(proc.Ctx, "invalid ODKU assertion overload or signature")
			}
			if id == function.ISNOTNULL {
				if c := f.Args[0].GetCol(); c != nil {
					width := -1
					if c.RelPos == 0 {
						width = len(leftTypes)
					} else if c.RelPos == 1 {
						width = len(rightTypes)
					}
					if c.ColPos < 0 || int(c.ColPos) >= width {
						return moerr.NewInternalError(proc.Ctx, "invalid ODKU assertion column")
					}
				}
			}
		}
		for _, arg := range f.Args {
			if err := validateCheck(arg, leftTypes, rightTypes, proc); err != nil {
				return err
			}
		}
	}
	if l := e.GetList(); l != nil {
		for _, arg := range l.List {
			if err := validateCheck(arg, leftTypes, rightTypes, proc); err != nil {
				return err
			}
		}
	}
	return nil
}

func directNotNullCheck(e *plan.Expr, leftTypes []types.Type) (Check, bool) {
	f := e.GetF()
	if f == nil || f.Func == nil || len(f.Args) != 2 || len(f.AggConfig) != 0 || e.Typ.Id != int32(types.T_bool) {
		return Check{}, false
	}
	id, index := function.DecodeOverloadID(f.Func.Obj)
	if id != function.CHECK_CONSTRAINT_ASSERT || index != 0 || f.Func.ObjName != "_check_constraint_assert" {
		return Check{}, false
	}
	inner := f.Args[0].GetF()
	if inner == nil || inner.Func == nil || len(inner.Args) != 1 || len(inner.AggConfig) != 0 || f.Args[0].Typ.Id != int32(types.T_bool) {
		return Check{}, false
	}
	id, index = function.DecodeOverloadID(inner.Func.Obj)
	if id != function.ISNOTNULL || index != 0 || inner.Func.ObjName != "isnotnull" {
		return Check{}, false
	}
	c := inner.Args[0].GetCol()
	if c == nil || c.RelPos != 0 || c.ColPos < 0 || int(c.ColPos) >= len(leftTypes) {
		return Check{}, false
	}
	t := leftTypes[c.ColPos]
	declared := inner.Args[0].Typ
	if declared.Id != int32(t.Oid) || declared.Width != t.Width || declared.Scale != t.Scale {
		return Check{}, false
	}
	lit := f.Args[1].GetLit()
	if lit == nil || lit.Isnull || f.Args[1].Typ.Id != int32(types.T_varchar) {
		return Check{}, false
	}
	message, ok := lit.Value.(*plan.Literal_Sval)
	if !ok {
		return Check{}, false
	}
	return Check{nonNullColumn: c.ColPos, message: message.Sval}, true
}

// CompileChecks compiles ordered action checks under the caller's allocation account.
// The caller owns the returned checks and must release them with FreeChecks.
func CompileChecks(exprs []*plan.Expr, leftTypes, rightTypes []types.Type, proc *process.Process, account *mpool.AllocationAccount) ([]Check, error) {
	checks := make([]Check, len(exprs))
	fallback := make([]*plan.Expr, 0, len(exprs))
	positions := make([]int, 0, len(exprs))
	for i, e := range exprs {
		if err := validateCheck(e, leftTypes, rightTypes, proc); err != nil {
			return nil, err
		}
		if check, ok := directNotNullCheck(e, leftTypes); ok {
			checks[i] = check
		} else {
			checks[i].nonNullColumn = -1
			positions = append(positions, i)
			fallback = append(fallback, e)
		}
	}
	if len(fallback) > 0 {
		executors, err := hashbuild.NewExpressionExecutors(proc, fallback, account)
		if err != nil {
			return nil, err
		}
		for i, position := range positions {
			checks[position].executor = executors[i]
		}
	}
	return checks, nil
}

// EvaluateChecks executes assertions in plan order, preserving the first error.
// The image and incoming batches are borrowed for the duration of the call.
func EvaluateChecks(checks []Check, image, incoming *batch.Batch, proc *process.Process) error {
	for _, check := range checks {
		if check.nonNullColumn >= 0 {
			if image.Vecs[check.nonNullColumn].IsNull(0) {
				return moerr.NewConstraintViolation(proc.Ctx, check.message)
			}
		} else if _, err := check.executor.Eval(proc, []*batch.Batch{image, incoming}, nil); err != nil {
			return err
		}
	}
	return nil
}

// EvaluateUpdate applies assignments in order, so each RHS sees earlier
// assignments, then checks the resulting image. Expression result vectors are
// borrowed and remain valid only until their executors are evaluated again.
// The caller validates the layout and owns the stable [image, incoming] input
// frame. Executors borrow it synchronously without retaining or changing it.
func EvaluateUpdate(execs []colexec.ExpressionExecutor, columns []int32, checks []Check, inputs []*batch.Batch, proc *process.Process) error {
	image, incoming := inputs[0], inputs[1]
	for i, e := range execs {
		v, err := e.Eval(proc, inputs, nil)
		if err != nil {
			return err
		}
		image.Vecs[columns[i]] = v
	}
	return EvaluateChecks(checks, image, incoming, proc)
}

// DirectNotNullChecks reports whether all checks can read the same retained
// table image through the validated table-to-source column mapping.
func DirectNotNullChecks(checks []Check, columns []int32) bool {
	for _, check := range checks {
		if check.executor != nil || check.nonNullColumn < 0 || int(check.nonNullColumn) >= len(columns) || columns[check.nonNullColumn] < 0 {
			return false
		}
	}
	return true
}

// EvaluateDirectNotNullChecksAt preserves native check order and error text.
// The caller has validated the checks and projection with DirectNotNullChecks.
func EvaluateDirectNotNullChecksAt(checks []Check, source *batch.Batch, columns []int32, row int, proc *process.Process) error {
	for _, check := range checks {
		if source.Vecs[columns[check.nonNullColumn]].IsNull(uint64(row)) {
			return moerr.NewConstraintViolation(proc.Ctx, check.message)
		}
	}
	return nil
}
