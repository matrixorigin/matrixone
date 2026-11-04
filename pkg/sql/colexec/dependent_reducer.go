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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"unsafe"
)

// DependentReducer owns at most one output tuple plus a three-valued summary.
// Its lhs is borrowed from the caller frame, which outlives the invocation.
// A decisive summary requests producer retirement; it is not proof that all
// started producers succeeded. The region owner publishes only after cleanup.
type DependentReducer struct {
	budget     *process.ExecutionResourceGeneration
	charged    uint64
	proc       *process.Process
	kind       plan.SubqueryRef_Type
	lhs        *batch.Batch
	tuple      *batch.Batch
	result     *batch.Batch
	comparison ExpressionExecutor
	rows       uint8
	unknown    bool
	value      bool
	decided    bool
	finished   bool
	closed     bool
	failure    error
}

func NewDependentReducer(proc *process.Process, call *plan.DependentExpr, output []plan.Type, lhs *batch.Batch, allocation *vector.AllocationAccountSelection, comparisons ...*DependentExpressionProgram) (r *DependentReducer, err error) {
	if proc == nil || call == nil || allocation == nil || len(output) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("invalid dependent reducer owner")
	}
	budget, err := proc.GetExecutionResourceBudget()
	if err != nil {
		return nil, err
	}
	size := uint64(unsafe.Sizeof(DependentReducer{}))
	batchSize := func(width int) uint64 {
		return uint64(unsafe.Sizeof(batch.Batch{})) + uint64(width)*uint64(unsafe.Sizeof(vector.Vector{})+unsafe.Sizeof((*vector.Vector)(nil)))
	}
	if call.Reducer == plan.SubqueryRef_SCALAR || call.Comparison != nil {
		size += batchSize(len(output))
	}
	if call.Reducer != plan.SubqueryRef_SCALAR || call.Comparison != nil {
		size += batchSize(1)
	}
	if err = budget.AcquireAllocationCapacity(size); err != nil {
		return nil, err
	}
	r = &DependentReducer{proc: proc, kind: call.Reducer, lhs: lhs, value: call.Reducer == plan.SubqueryRef_ALL, budget: budget, charged: size}
	defer func() {
		if err != nil {
			r.Close()
			r = nil
		}
	}()
	if call.Reducer < plan.SubqueryRef_SCALAR || call.Reducer > plan.SubqueryRef_ALL {
		return r, moerr.NewInternalErrorNoCtx("invalid dependent reducer")
	}
	if call.Comparison != nil {
		if lhs == nil || lhs.RowCount() != 1 || len(lhs.Vecs) != len(output) {
			return r, moerr.NewInternalErrorNoCtx("invalid dependent comparison operands")
		}
		var comparison *DependentExpressionProgram
		if len(comparisons) > 0 {
			comparison = comparisons[0]
		}
		if comparison == nil {
			comparison, err = CompileDependentExpressions(proc.Ctx, []*plan.Expr{call.Comparison})
			if err != nil {
				return r, err
			}
		}
		r.comparison, err = comparison.NewScalarExecutor(proc, allocation)
		if err != nil {
			return r, err
		}
	} else if call.Reducer != plan.SubqueryRef_SCALAR && call.Reducer != plan.SubqueryRef_EXISTS && call.Reducer != plan.SubqueryRef_NOT_EXISTS {
		return r, moerr.NewInternalErrorNoCtx("dependent reducer requires comparison")
	}
	if call.Reducer == plan.SubqueryRef_SCALAR || r.comparison != nil {
		r.tuple = batch.NewWithSize(len(output))
		for i, typ := range output {
			native, typeErr := types.TypeFromPlan(typ)
			if typeErr != nil {
				return r, typeErr
			}
			r.tuple.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(native, allocation)
			if err != nil {
				return r, err
			}
		}
	}
	if call.Reducer != plan.SubqueryRef_SCALAR || r.comparison != nil {
		r.result = batch.NewWithSize(1)
		r.result.Vecs[0], err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_bool.ToType(), allocation)
		if err != nil {
			return r, err
		}
	}
	return r, nil
}

// Feed preserves cardinality across batch boundaries. It never folds a tuple
// comparison into a single NULL-key summary: the bound overload observes each
// complete lhs/rhs row, including the FALSE-before-NULL row equality case.
func (r *DependentReducer) Feed(input *batch.Batch) (stop bool, err error) {
	if r.closed || r.finished {
		return false, moerr.NewInternalErrorNoCtx("dependent reducer is closed")
	}
	if r.failure != nil {
		return false, r.failure
	}
	defer func() {
		if err != nil {
			r.failure = err
		}
	}()
	if r.decided {
		return true, nil
	}
	if input == nil || input.RowCount() == 0 {
		return false, nil
	}
	if r.kind == plan.SubqueryRef_EXISTS || r.kind == plan.SubqueryRef_NOT_EXISTS {
		r.value = r.kind == plan.SubqueryRef_EXISTS
		r.decided = true
		return true, nil
	}
	if len(input.Vecs) != len(r.tuple.Vecs) {
		return false, moerr.NewInternalErrorNoCtx("dependent output width changed")
	}
	for row := 0; row < input.RowCount(); row++ {
		if r.kind == plan.SubqueryRef_SCALAR && r.rows == 1 {
			return false, moerr.NewErrSubqueryNo1Row(r.proc.Ctx)
		}
		for i, source := range input.Vecs {
			if source == nil || !source.GetType().Eq(*r.tuple.Vecs[i].GetType()) {
				return false, moerr.NewInternalErrorNoCtx("dependent output type changed")
			}
			r.tuple.Vecs[i].CleanOnlyData()
			if err = r.tuple.Vecs[i].UnionOne(source, int64(row), r.proc.Mp()); err != nil {
				return false, err
			}
		}
		r.tuple.SetRowCount(1)
		r.rows = 1
		if r.kind == plan.SubqueryRef_SCALAR {
			continue
		}
		value, null, cmpErr := r.compare()
		if cmpErr != nil {
			return false, cmpErr
		}
		if null {
			r.unknown = true
			continue
		}
		if r.kind == plan.SubqueryRef_ALL {
			if !value {
				r.value = false
				r.unknown = false
				r.decided = true
				return true, nil
			}
		} else if value {
			r.value = true
			r.unknown = false
			r.decided = true
			return true, nil
		}
	}
	return false, nil
}

func (r *DependentReducer) compare() (bool, bool, error) {
	result, err := r.comparison.Eval(r.proc, []*batch.Batch{r.lhs, r.tuple}, nil)
	if err != nil {
		return false, false, err
	}
	if result == nil || !result.GetType().IsBoolean() || result.Length() != 1 {
		return false, false, moerr.NewInternalErrorNoCtx("invalid dependent comparison result")
	}
	if result.IsNull(0) {
		return false, true, nil
	}
	return vector.GetFixedAtNoTypeCheck[bool](result, 0), false, nil
}

// Finish may be called only after EOF or successful early-retirement. The
// returned batch is borrowed until Close; the caller copies it into its frame.
func (r *DependentReducer) Finish() (*batch.Batch, error) {
	if r.closed {
		return nil, moerr.NewInternalErrorNoCtx("dependent reducer is closed")
	}
	if r.failure != nil {
		return nil, r.failure
	}
	if r.finished {
		if r.result != nil {
			return r.result, nil
		}
		return r.tuple, nil
	}
	if r.kind == plan.SubqueryRef_SCALAR {
		if r.rows == 0 {
			for _, vec := range r.tuple.Vecs {
				if err := vector.SetConstNull(vec, 1, r.proc.Mp()); err != nil {
					r.failure = err
					return nil, err
				}
			}
			r.tuple.SetRowCount(1)
		}
		if r.comparison == nil {
			r.finished = true
			return r.tuple, nil
		}
		value, null, err := r.compare()
		if err != nil {
			r.failure = err
			return nil, err
		}
		r.value = value
		r.unknown = null
	} else if r.kind == plan.SubqueryRef_NOT_EXISTS && !r.decided {
		r.value = true
	}
	value := r.value
	if r.kind == plan.SubqueryRef_NOT_IN {
		value = !value
	}
	if err := vector.AppendFixed(r.result.Vecs[0], value, r.unknown, r.proc.Mp()); err != nil {
		r.failure = err
		return nil, err
	}
	r.result.SetRowCount(1)
	r.finished = true
	return r.result, nil
}

func (r *DependentReducer) Close() {
	if r == nil || r.closed {
		return
	}
	r.closed = true
	if r.comparison != nil {
		r.comparison.Free()
		r.comparison = nil
	}
	if r.tuple != nil {
		r.tuple.Clean(r.proc.Mp())
		r.tuple = nil
	}
	if r.result != nil {
		r.result.Clean(r.proc.Mp())
		r.result = nil
	}
	r.lhs = nil
	r.budget.ReleaseAllocationCapacity(r.charged)
	r.charged = 0
}
