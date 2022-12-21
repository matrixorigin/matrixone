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

package colexec

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// EvalExpr a new method to evaluate an expr.
func EvalExpr(bat *batch.Batch, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	batchLen := len(bat.Zs)
	if batchLen == 0 {
		return vector.NewConstNullWithData(types.Type{Oid: types.T(expr.Typ.GetId())}, 1, proc.Mp()), nil
	}

	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_F:
		return evaluateExprF(bat, t, proc)
	default:
		return OldEvalExpr(bat, proc, expr)
	}
}

// evaluateExprF a new compute method for expr_f.
func evaluateExprF(bat *batch.Batch, expr *plan.Expr_F, proc *process.Process) (*vector.Vector, error) {
	var result *vector.Vector

	fid := expr.F.GetFunc().GetObj()
	f, err := function.GetFunctionByID(proc.Ctx, fid)
	if err != nil {
		return nil, err
	}

	args := make([]*vector.Vector, len(expr.F.Args))
	for i := range args {
		args[i], err = EvalExpr(bat, proc, expr.F.Args[i])
		if err != nil {
			break
		}
	}
	if err != nil {
		cleanVectorsExceptList(proc, args, bat.Vecs)
		return nil, err
	}

	result, err = evalFunction(proc, f, args, len(bat.Zs))
	cleanVectorsExceptList(proc, args, bat.Vecs)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func evalFunction(proc *process.Process, f *function.Function, args []*vector.Vector, length int) (*vector.Vector, error) {
	if !f.UseNewFramework {
		return f.Fn(args, proc)
	}
	var resultWrapper vector.FunctionResultWrapper
	var err error

	rTyp := f.ReturnTyp.ToType()
	numScalar := 0
	// If any argument is `NULL`, return NULL.
	// If all arguments are scalar, return scalar.
	for i := range args {
		if args[i].IsScalar() {
			numScalar++
		} else {
			if len(f.ParameterMustScalar) > i && f.ParameterMustScalar[i] {
				return nil, moerr.NewInternalError(proc.Ctx,
					fmt.Sprintf("the %dth parameter of function can only be constant", i+1))
			}
		}
		if !f.AcceptScalarNull && args[i].IsScalarNull() {
			return vector.NewConstNull(rTyp, length), nil
		}
	}
	if !f.ResultMustNotScalar && numScalar == len(args) {
		resultWrapper = newFunctionResultRelated(rTyp, proc, true, length)
		// XXX only evaluate the first row.
		err = f.NewFn(args, resultWrapper, proc, 1)
	} else {
		resultWrapper = newFunctionResultRelated(rTyp, proc, false, length)
		err = f.NewFn(args, resultWrapper, proc, length)
	}

	return resultWrapper.GetResultVector(), err
}

func newFunctionResultRelated(typ types.Type, proc *process.Process, isConst bool, length int) vector.FunctionResultWrapper {
	var v *vector.Vector
	if isConst {
		v = vector.NewConst(typ, length)
	} else {
		v = vector.New(typ)
	}

	switch typ.Oid {
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		// IF STRING type.
		return vector.NewResultFunc[types.Varlena](v, proc.Mp())
	case types.T_json:
		return vector.NewResultFunc[types.Varlena](v, proc.Mp())
	}

	// Pre allocate the memory
	// XXX PreAllocType has BUG. It only shrink the cols.
	//v = vector.PreAllocType(typ, 0, length, proc.Mp())
	//vector.SetLength(v, 0)
	switch typ.Oid {
	case types.T_bool:
		return vector.NewResultFunc[bool](v, proc.Mp())
	case types.T_int8:
		return vector.NewResultFunc[int8](v, proc.Mp())
	case types.T_int16:
		return vector.NewResultFunc[int16](v, proc.Mp())
	case types.T_int32:
		return vector.NewResultFunc[int32](v, proc.Mp())
	case types.T_int64:
		return vector.NewResultFunc[int64](v, proc.Mp())
	case types.T_uint8:
		return vector.NewResultFunc[uint8](v, proc.Mp())
	case types.T_uint16:
		return vector.NewResultFunc[uint16](v, proc.Mp())
	case types.T_uint32:
		return vector.NewResultFunc[uint32](v, proc.Mp())
	case types.T_uint64:
		return vector.NewResultFunc[uint64](v, proc.Mp())
	case types.T_float32:
		return vector.NewResultFunc[float32](v, proc.Mp())
	case types.T_float64:
		return vector.NewResultFunc[float64](v, proc.Mp())
	case types.T_date:
		return vector.NewResultFunc[types.Date](v, proc.Mp())
	case types.T_datetime:
		return vector.NewResultFunc[types.Datetime](v, proc.Mp())
	case types.T_time:
		return vector.NewResultFunc[types.Time](v, proc.Mp())
	case types.T_timestamp:
		return vector.NewResultFunc[types.Timestamp](v, proc.Mp())
	case types.T_decimal64:
		return vector.NewResultFunc[types.Decimal64](v, proc.Mp())
	case types.T_decimal128:
		return vector.NewResultFunc[types.Decimal128](v, proc.Mp())
	case types.T_TS:
		return vector.NewResultFunc[types.TS](v, proc.Mp())
	case types.T_Rowid:
		return vector.NewResultFunc[types.Rowid](v, proc.Mp())
	case types.T_uuid:
		return vector.NewResultFunc[types.Uuid](v, proc.Mp())
	}
	panic(fmt.Sprintf("unexpected type %s for function result", typ))
}

func cleanVectorsExceptList(proc *process.Process, vs []*vector.Vector, excepts []*vector.Vector) {
	mp := proc.Mp()
	for i := range vs {
		if vs[i] == nil {
			continue
		}
		needClean := true
		for j := range excepts {
			if excepts[j] == vs[i] {
				needClean = false
				break
			}
		}
		if needClean {
			vs[i].Free(mp)
		}
	}
}
