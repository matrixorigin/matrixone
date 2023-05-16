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
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	constBType          = types.T_bool.ToType()
	constI8Type         = types.T_int8.ToType()
	constI16Type        = types.T_int16.ToType()
	constI32Type        = types.T_int32.ToType()
	constI64Type        = types.T_int64.ToType()
	constU8Type         = types.T_uint8.ToType()
	constU16Type        = types.T_uint16.ToType()
	constU32Type        = types.T_uint32.ToType()
	constU64Type        = types.T_uint64.ToType()
	constFType          = types.T_float32.ToType()
	constDType          = types.T_float64.ToType()
	constSType          = types.T_varchar.ToType()
	constBinType        = types.T_blob.ToType()
	constDateType       = types.T_date.ToType()
	constTimeType       = types.T_time.ToType()
	constDatetimeType   = types.T_datetime.ToType()
	constTimestampTypes = []types.Type{
		types.New(types.T_timestamp, 0, 0),
		types.New(types.T_timestamp, 0, 1),
		types.New(types.T_timestamp, 0, 2),
		types.New(types.T_timestamp, 0, 3),
		types.New(types.T_timestamp, 0, 4),
		types.New(types.T_timestamp, 0, 5),
		types.New(types.T_timestamp, 0, 6),
	}
)

func EvalFilterByZonemap(
	ctx context.Context,
	meta objectio.ColumnMetaFetcher,
	expr *plan.Expr,
	zms []objectio.ZoneMap,
	vecs []*vector.Vector,
	columnMap map[int]int,
	proc *process.Process,
) objectio.ZoneMap {
	var err error
	switch t := expr.Expr.(type) {
	case *plan.Expr_C:
		if zms[expr.AuxId] == nil {
			if zms[expr.AuxId], err = getConstZM(ctx, expr, proc); err != nil {
				zms[expr.AuxId] = objectio.NewZM(types.T_bool, 0)
			}
		}
	case *plan.Expr_Col:
		zms[expr.AuxId] = meta.MustGetColumn(uint16(columnMap[int(t.Col.ColPos)])).ZoneMap()
	case *plan.Expr_F:
		var (
			err error
			f   *function.Function
		)

		args := t.F.Args
		id := t.F.GetFunc().GetObj()
		if f, err = function.GetFunctionByID(proc.Ctx, id); err != nil {
			zms[expr.AuxId].Reset()
			break
		}

		//if t.F.Func.ObjName == "in" {
		//	rid := args[1].AuxId
		//	if vecs[rid] == nil {
		//		if vecs[args[1].AuxId], err = getConstVecInList(proc.Ctx, proc, args[1].GetList().List); err != nil {
		//			zms[expr.AuxId].Reset()
		//			vecs[args[1].AuxId] = vector.NewConstNull(types.T_any.ToType(), math.MaxInt, proc.Mp())
		//			return zms[expr.AuxId]
		//		}
		//	}
		//
		//	if vecs[rid].IsConstNull() && vecs[rid].Length() == math.MaxInt {
		//		zms[expr.AuxId].Reset()
		//		return zms[expr.AuxId]
		//	}
		//
		//	lhs := EvalFilterByZonemap(ctx, meta, args[0], zms, vecs, columnMap, proc)
		//	if res, ok := lhs.AnyIn(vecs[rid]); !ok {
		//		zms[expr.AuxId].Reset()
		//	} else {
		//		zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
		//	}
		//
		//	return zms[expr.AuxId]
		//}

		for _, arg := range args {
			zms[arg.AuxId] = EvalFilterByZonemap(ctx, meta, arg, zms, vecs, columnMap, proc)
			if !zms[arg.AuxId].IsInited() {
				zms[expr.AuxId].Reset()
				return zms[expr.AuxId]
			}
		}
		var res, ok bool
		switch t.F.Func.ObjName {
		case ">":
			if res, ok = zms[args[0].AuxId].AnyGT(zms[args[1].AuxId]); !ok {
				zms[expr.AuxId].Reset()
			} else {
				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
			}

		case "<":
			if res, ok = zms[args[0].AuxId].AnyLT(zms[args[1].AuxId]); !ok {
				zms[expr.AuxId].Reset()
			} else {
				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
			}

		case ">=":
			if res, ok = zms[args[0].AuxId].AnyGE(zms[args[1].AuxId]); !ok {
				zms[expr.AuxId].Reset()
			} else {
				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
			}

		case "<=":
			if res, ok = zms[args[0].AuxId].AnyLE(zms[args[1].AuxId]); !ok {
				zms[expr.AuxId].Reset()
			} else {
				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
			}

		case "=":
			if res, ok = zms[args[0].AuxId].Intersect(zms[args[1].AuxId]); !ok {
				zms[expr.AuxId].Reset()
			} else {
				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
			}

		case "and":
			if res, ok = zms[args[0].AuxId].And(zms[args[1].AuxId]); !ok {
				zms[expr.AuxId].Reset()
			} else {
				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
			}

		case "or":
			if res, ok = zms[args[0].AuxId].Or(zms[args[1].AuxId]); !ok {
				zms[expr.AuxId].Reset()
			} else {
				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
			}

		case "+":
			zms[expr.AuxId] = index.ZMPlus(zms[args[0].AuxId], zms[args[1].AuxId], zms[expr.AuxId])

		case "-":
			zms[expr.AuxId] = index.ZMMinus(zms[args[0].AuxId], zms[args[1].AuxId], zms[expr.AuxId])

		case "*":
			zms[expr.AuxId] = index.ZMMulti(zms[args[0].AuxId], zms[args[1].AuxId], zms[expr.AuxId])

		default:
			var rvec *vector.Vector
			ivecs := make([]*vector.Vector, len(args))
			for i, arg := range args {
				if vecs[arg.AuxId], err = index.ZMToVector(zms[arg.AuxId], vecs[arg.AuxId], proc.Mp()); err != nil {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}
				ivecs[i] = vecs[arg.AuxId]
			}

			if rvec, err = evalFunction(proc, f, ivecs, 2); err != nil {
				zms[expr.AuxId].Reset()
				return zms[expr.AuxId]
			}
			defer rvec.Free(proc.Mp())
			zms[expr.AuxId] = index.VectorToZM(rvec, zms[expr.AuxId])
		}
	}

	return zms[expr.AuxId]
}

func evalFunction(proc *process.Process, f *function.Function, args []*vector.Vector, length int) (*vector.Vector, error) {
	if !f.UseNewFramework {
		v, err := f.VecFn(args, proc)
		if err != nil {
			return nil, err
		}
		v.SetLength(length)
		return v, nil
	}
	var resultWrapper vector.FunctionResultWrapper
	var err error

	var parameterTypes []types.Type
	if f.FlexibleReturnType != nil {
		parameterTypes = make([]types.Type, len(args))
		for i := range args {
			parameterTypes[i] = *args[i].GetType()
		}
	}
	rTyp, _ := f.ReturnType(parameterTypes)
	numScalar := 0
	// If any argument is `NULL`, return NULL.
	// If all arguments are scalar, return scalar.
	for i := range args {
		if args[i].IsConst() {
			numScalar++
		} else {
			if len(f.ParameterMustScalar) > i && f.ParameterMustScalar[i] {
				return nil, moerr.NewInternalError(proc.Ctx,
					fmt.Sprintf("the %dth parameter of function can only be constant", i+1))
			}
		}
	}

	if !f.Volatile && numScalar == len(args) {
		resultWrapper = vector.NewFunctionResultWrapper(rTyp, proc.Mp())
		// XXX only evaluate the first row.
		err = f.NewFn(args, resultWrapper, proc, 1)
	} else {
		resultWrapper = vector.NewFunctionResultWrapper(rTyp, proc.Mp())
		err = f.NewFn(args, resultWrapper, proc, length)
	}
	if err != nil {
		resultWrapper.Free()
		return nil, err
	}
	rvec := resultWrapper.GetResultVector()
	rvec.SetLength(length)
	return rvec, nil
}

// RewriteFilterExprList will convert an expression list to be an AndExpr
func RewriteFilterExprList(list []*plan.Expr) *plan.Expr {
	l := len(list)
	if l == 0 {
		return nil
	} else if l == 1 {
		return list[0]
	} else {
		left := list[0]
		right := RewriteFilterExprList(list[1:])
		return &plan.Expr{
			Typ:  left.Typ,
			Expr: makeAndExpr(left, right),
		}
	}
}

func SplitAndExprs(list []*plan.Expr) []*plan.Expr {
	exprs := make([]*plan.Expr, 0, len(list))
	for i := range list {
		exprs = append(exprs, splitAndExpr(list[i])...)
	}
	return exprs
}

func splitAndExpr(expr *plan.Expr) []*plan.Expr {
	exprs := make([]*plan.Expr, 0, 1)
	if e, ok := expr.Expr.(*plan.Expr_F); ok {
		fid, _ := function2.DecodeOverloadID(e.F.Func.GetObj())
		if fid == function2.AND {
			exprs = append(exprs, splitAndExpr(e.F.Args[0])...)
			exprs = append(exprs, splitAndExpr(e.F.Args[1])...)
			return exprs
		}
	}
	exprs = append(exprs, expr)
	return exprs
}

func makeAndExpr(left, right *plan.Expr) *plan.Expr_F {
	return &plan.Expr_F{
		F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function2.AndFunctionEncodedID, ObjName: function2.AndFunctionName},
			Args: []*plan.Expr{left, right},
		},
	}
}
