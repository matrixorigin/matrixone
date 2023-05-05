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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ExpressionExecutor
// generated from plan.Expr, can evaluate the result from vectors directly.
type ExpressionExecutor interface {
	// Eval should include memory reuse logic. it's results cannot be used directly.
	// If it needs to be modified or saved, it should be copied by vector.Dup().
	Eval(proc *process.Process, batches []*batch.Batch) (*vector.Vector, error)

	// Free should release all memory of executor.
	// it will be called after query has done.
	Free()
}

func NewExpressionExecutorsFromPlanExpressions(proc *process.Process, planExprs []*plan.Expr) (executors []ExpressionExecutor, err error) {
	executors = make([]ExpressionExecutor, len(planExprs))
	for i := range executors {
		executors[i], err = NewExpressionExecutor(proc, planExprs[i])
		if err != nil {
			return nil, err
		}
	}
	return executors, err
}

func NewExpressionExecutor(proc *process.Process, planExpr *plan.Expr) (ExpressionExecutor, error) {
	expr := planExpr.Expr
	switch t := expr.(type) {
	case *plan.Expr_C:
		typ := types.New(types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale)
		vec, err := generateConstExpressionExecutor(proc, typ, t.C)
		if err != nil {
			return nil, err
		}
		return &FixedVectorExpressionExecutor{
			m:            proc.Mp(),
			resultVector: vec,
		}, nil

	case *plan.Expr_T:
		typ := types.New(types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale)
		vec := vector.NewConstNull(typ, 1, proc.Mp())
		return &FixedVectorExpressionExecutor{
			m:            proc.Mp(),
			resultVector: vec,
		}, nil

	case *plan.Expr_List:
		// a vector like [1, NUll, 2, 3, 4, 5, 6, 7, 9]
		vec, err := generateConstListExpressionExecutor(proc, t.List.List)
		if err != nil {
			return nil, err
		}
		return &FixedVectorExpressionExecutor{
			m:            proc.Mp(),
			fixed:        true,
			resultVector: vec,
		}, nil

	case *plan.Expr_Col:
		typ := types.New(types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale)
		return &ColumnExpressionExecutor{
			relIndex: int(t.Col.RelPos),
			colIndex: int(t.Col.ColPos),
			typ:      typ,
		}, nil

	case *plan.Expr_F:
		overload, err := function2.GetFunctionById(proc.Ctx, t.F.GetFunc().GetObj())
		if err != nil {
			return nil, err
		}

		// TODO: IF all parameters here were constant. and this function can be folded.
		// 	there is a better way to convert it as a FixedVectorExpressionExecutor.

		executor := &FunctionExpressionExecutor{}
		typ := types.New(types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale)
		if err = executor.Init(proc.Mp(), len(t.F.Args), typ, overload.NewOp); err != nil {
			return nil, err
		}

		for i := range executor.parameterExecutor {
			subExecutor, paramErr := NewExpressionExecutor(proc, t.F.Args[i])
			if paramErr != nil {
				return nil, paramErr
			}
			executor.SetParameter(i, subExecutor)
		}
		return executor, nil
	}

	return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("unsupported expression executor for %v now", expr))
}

// EvaluateExprWithoutExecutor can evaluate an expression without make an executor by developer.
//
// XXX I'm not sure if I should support this method.
// There is a big problem that, user cannot get the executor,
// and will be very hard to clean the memory.
// because the result vector maybe just one column from bats.
func EvaluateExprWithoutExecutor(proc *process.Process, planExpr *plan.Expr, bats []*batch.Batch) (*vector.Vector, error) {
	executor, err := NewExpressionExecutor(proc, planExpr)
	if err != nil {
		if v, errEval := executor.Eval(proc, bats); errEval == nil {
			return v, nil
		}
		executor.Free()
	}
	return nil, moerr.NewNotSupported(proc.Ctx, "can not evaluate expression %v", planExpr)
}

// FixedVectorExpressionExecutor
// the content of its vector is fixed.
// e.g.
//
//	ConstVector [1, 1, 1, 1, 1]
//	ConstVector [null, null, null]
//	ListVector  ["1", "2", "3", null, "5"]
type FixedVectorExpressionExecutor struct {
	m *mpool.MPool

	fixed        bool
	resultVector *vector.Vector
}

type FunctionExpressionExecutor struct {
	m *mpool.MPool

	resultVector vector.FunctionResultWrapper
	// parameters related
	parameterResults  []*vector.Vector
	parameterExecutor []ExpressionExecutor

	evalFn func(
		params []*vector.Vector,
		result vector.FunctionResultWrapper,
		proc *process.Process,
		length int) error
}

type ColumnExpressionExecutor struct {
	relIndex int
	colIndex int
	// result type.
	typ types.Type
}

func (expr *FunctionExpressionExecutor) Init(
	m *mpool.MPool,
	parameterNum int,
	retType types.Type,
	fn func(
		params []*vector.Vector,
		result vector.FunctionResultWrapper,
		proc *process.Process,
		length int) error) (err error) {
	expr.m = m
	expr.evalFn = fn
	expr.parameterResults = make([]*vector.Vector, parameterNum)
	expr.parameterExecutor = make([]ExpressionExecutor, parameterNum)
	// pre allocate
	expr.resultVector, err = vector.NewFunctionResultWrapper2(retType, m)
	return err
}

func (expr *FunctionExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch) (*vector.Vector, error) {
	var err error
	for i := range expr.parameterExecutor {
		expr.parameterResults[i], err = expr.parameterExecutor[i].Eval(proc, batches)
		if err != nil {
			return nil, err
		}
	}
	typ := expr.resultVector.GetResultVector().GetType()
	expr.resultVector.GetResultVector().Reset(*typ)

	if err = expr.evalFn(
		expr.parameterResults, expr.resultVector, proc, batches[0].Length()); err != nil {
		return nil, err
	}
	return expr.resultVector.GetResultVector(), nil
}

func (expr *FunctionExpressionExecutor) Free() {
	if expr.resultVector == nil {
		return
	}
	vec := expr.resultVector.GetResultVector()
	vec.Free(expr.m)
	for _, p := range expr.parameterExecutor {
		p.Free()
	}
}

func (expr *FunctionExpressionExecutor) SetParameter(index int, executor ExpressionExecutor) {
	expr.parameterExecutor[index] = executor
}

func (expr *ColumnExpressionExecutor) Eval(_ *process.Process, batches []*batch.Batch) (*vector.Vector, error) {
	// XXX it's a bad hack here. root cause is pipeline set a wrong relation index here.
	if len(batches) == 1 {
		vec := batches[0].Vecs[expr.colIndex]
		if vec.IsConstNull() {
			vec.SetType(expr.typ)
		}
		return vec, nil
	}

	vec := batches[expr.relIndex].Vecs[expr.colIndex]
	if vec.IsConstNull() {
		vec.SetType(expr.typ)
	}
	return vec, nil
}

func (expr *ColumnExpressionExecutor) Free() {
	// Nothing should do.
}

func (expr *FixedVectorExpressionExecutor) Eval(_ *process.Process, batches []*batch.Batch) (*vector.Vector, error) {
	if !expr.fixed {
		expr.resultVector.SetLength(batches[0].Length())
	}
	return expr.resultVector, nil
}

func (expr *FixedVectorExpressionExecutor) Free() {
	if expr.resultVector == nil {
		return
	}
	expr.resultVector.Free(expr.m)
}

func generateConstExpressionExecutor(proc *process.Process, typ types.Type, con *plan.Const) (*vector.Vector, error) {
	var vec *vector.Vector

	if con.GetIsnull() {
		vec = vector.NewConstNull(typ, 1, proc.Mp())
	} else {
		switch con.GetValue().(type) {
		case *plan.Const_Bval:
			vec = vector.NewConstFixed(constBType, con.GetBval(), 1, proc.Mp())
		case *plan.Const_I8Val:
			vec = vector.NewConstFixed(constI8Type, int8(con.GetI8Val()), 1, proc.Mp())
		case *plan.Const_I16Val:
			vec = vector.NewConstFixed(constI16Type, int16(con.GetI16Val()), 1, proc.Mp())
		case *plan.Const_I32Val:
			vec = vector.NewConstFixed(constI32Type, con.GetI32Val(), 1, proc.Mp())
		case *plan.Const_I64Val:
			vec = vector.NewConstFixed(constI64Type, con.GetI64Val(), 1, proc.Mp())
		case *plan.Const_U8Val:
			vec = vector.NewConstFixed(constU8Type, uint8(con.GetU8Val()), 1, proc.Mp())
		case *plan.Const_U16Val:
			vec = vector.NewConstFixed(constU16Type, uint16(con.GetU16Val()), 1, proc.Mp())
		case *plan.Const_U32Val:
			vec = vector.NewConstFixed(constU32Type, con.GetU32Val(), 1, proc.Mp())
		case *plan.Const_U64Val:
			vec = vector.NewConstFixed(constU64Type, con.GetU64Val(), 1, proc.Mp())
		case *plan.Const_Fval:
			vec = vector.NewConstFixed(constFType, con.GetFval(), 1, proc.Mp())
		case *plan.Const_Dval:
			vec = vector.NewConstFixed(constDType, con.GetDval(), 1, proc.Mp())
		case *plan.Const_Dateval:
			vec = vector.NewConstFixed(constDateType, types.Date(con.GetDateval()), 1, proc.Mp())
		case *plan.Const_Timeval:
			vec = vector.NewConstFixed(constTimeType, types.Time(con.GetTimeval()), 1, proc.Mp())
		case *plan.Const_Datetimeval:
			vec = vector.NewConstFixed(constDatetimeType, types.Datetime(con.GetDatetimeval()), 1, proc.Mp())
		case *plan.Const_Decimal64Val:
			cd64 := con.GetDecimal64Val()
			d64 := types.Decimal64(cd64.A)
			vec = vector.NewConstFixed(typ, d64, 1, proc.Mp())
		case *plan.Const_Decimal128Val:
			cd128 := con.GetDecimal128Val()
			d128 := types.Decimal128{B0_63: uint64(cd128.A), B64_127: uint64(cd128.B)}
			vec = vector.NewConstFixed(typ, d128, 1, proc.Mp())
		case *plan.Const_Timestampval:
			scale := typ.Scale
			if scale < 0 || scale > 6 {
				return nil, moerr.NewInternalError(proc.Ctx, "invalid timestamp scale")
			}
			vec = vector.NewConstFixed(constTimestampTypes[scale], types.Timestamp(con.GetTimestampval()), 1, proc.Mp())
		case *plan.Const_Sval:
			sval := con.GetSval()
			// Distinguish binary with non-binary string.
			if typ.Oid == types.T_binary || typ.Oid == types.T_varbinary || typ.Oid == types.T_blob {
				vec = vector.NewConstBytes(constBinType, []byte(sval), 1, proc.Mp())
			} else {
				vec = vector.NewConstBytes(constSType, []byte(sval), 1, proc.Mp())
			}
		case *plan.Const_Defaultval:
			defaultVal := con.GetDefaultval()
			vec = vector.NewConstFixed(constBType, defaultVal, 1, proc.Mp())
		default:
			return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("const expression %v", con.GetValue()))
		}
		vec.SetIsBin(con.IsBin)
	}
	return vec, nil
}

func generateConstListExpressionExecutor(proc *process.Process, exprs []*plan.Expr) (*vector.Vector, error) {
	lenList := len(exprs)
	vec, err := proc.AllocVectorOfRows(types.New(types.T(exprs[0].Typ.Id), exprs[0].Typ.Width, exprs[0].Typ.Scale), lenList, nil)
	if err != nil {
		return nil, err
	}
	for i := 0; i < lenList; i++ {
		expr := exprs[i]
		t, ok := expr.Expr.(*plan.Expr_C)
		if !ok {
			return nil, moerr.NewInternalError(proc.Ctx, "args in list must be constant")
		}
		if t.C.GetIsnull() {
			vec.GetNulls().Set(uint64(i))
		} else {
			switch t.C.GetValue().(type) {
			case *plan.Const_Bval:
				veccol := vector.MustFixedCol[bool](vec)
				veccol[i] = t.C.GetBval()
			case *plan.Const_I8Val:
				veccol := vector.MustFixedCol[int8](vec)
				veccol[i] = int8(t.C.GetI8Val())
			case *plan.Const_I16Val:
				veccol := vector.MustFixedCol[int16](vec)
				veccol[i] = int16(t.C.GetI16Val())
			case *plan.Const_I32Val:
				veccol := vector.MustFixedCol[int32](vec)
				veccol[i] = t.C.GetI32Val()
			case *plan.Const_I64Val:
				veccol := vector.MustFixedCol[int64](vec)
				veccol[i] = t.C.GetI64Val()
			case *plan.Const_U8Val:
				veccol := vector.MustFixedCol[uint8](vec)
				veccol[i] = uint8(t.C.GetU8Val())
			case *plan.Const_U16Val:
				veccol := vector.MustFixedCol[uint16](vec)
				veccol[i] = uint16(t.C.GetU16Val())
			case *plan.Const_U32Val:
				veccol := vector.MustFixedCol[uint32](vec)
				veccol[i] = t.C.GetU32Val()
			case *plan.Const_U64Val:
				veccol := vector.MustFixedCol[uint64](vec)
				veccol[i] = t.C.GetU64Val()
			case *plan.Const_Fval:
				veccol := vector.MustFixedCol[float32](vec)
				veccol[i] = t.C.GetFval()
			case *plan.Const_Dval:
				veccol := vector.MustFixedCol[float64](vec)
				veccol[i] = t.C.GetDval()
			case *plan.Const_Dateval:
				veccol := vector.MustFixedCol[types.Date](vec)
				veccol[i] = types.Date(t.C.GetDateval())
			case *plan.Const_Timeval:
				veccol := vector.MustFixedCol[types.Time](vec)
				veccol[i] = types.Time(t.C.GetTimeval())
			case *plan.Const_Datetimeval:
				veccol := vector.MustFixedCol[types.Datetime](vec)
				veccol[i] = types.Datetime(t.C.GetDatetimeval())
			case *plan.Const_Decimal64Val:
				cd64 := t.C.GetDecimal64Val()
				d64 := types.Decimal64(cd64.A)
				veccol := vector.MustFixedCol[types.Decimal64](vec)
				veccol[i] = d64
			case *plan.Const_Decimal128Val:
				cd128 := t.C.GetDecimal128Val()
				d128 := types.Decimal128{B0_63: uint64(cd128.A), B64_127: uint64(cd128.B)}
				veccol := vector.MustFixedCol[types.Decimal128](vec)
				veccol[i] = d128
			case *plan.Const_Timestampval:
				scale := expr.Typ.Scale
				if scale < 0 || scale > 6 {
					return nil, moerr.NewInternalError(proc.Ctx, "invalid timestamp scale")
				}
				veccol := vector.MustFixedCol[types.Timestamp](vec)
				veccol[i] = types.Timestamp(t.C.GetTimestampval())
			case *plan.Const_Sval:
				sval := t.C.GetSval()
				err = vector.SetStringAt(vec, i, sval, proc.Mp())
				if err != nil {
					return nil, err
				}
			case *plan.Const_Defaultval:
				defaultVal := t.C.GetDefaultval()
				veccol := vector.MustFixedCol[bool](vec)
				veccol[i] = defaultVal
			default:
				return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("const expression %v", t.C.GetValue()))
			}
			vec.SetIsBin(t.C.IsBin)
		}
	}
	return vec, nil
}

// MinDup do dup work for vectors of batch as few time as possible.
func MinDup(proc *process.Process, bat *batch.Batch) (dupSize int, err error) {
	dupSize = 0

	duped := make([]int, len(bat.Vecs))
	for i := range duped {
		duped[i] = -1
	}

	dupedVectors := make([]*vector.Vector, 0, len(bat.Vecs))
	for i, oldVec := range bat.Vecs {
		if duped[i] < 0 {
			newVec := (*vector.Vector)(nil)
			if oldVec.GetType().Oid == types.T_any {
				newVec = vector.NewConstNull(types.T_any.ToType(), oldVec.Length(), proc.Mp())
			} else {
				newVec, err = oldVec.Dup(proc.Mp())
				if err != nil {
					for j := range dupedVectors {
						dupedVectors[j].Free(proc.Mp())
					}
					return 0, err
				}
			}
			dupSize += newVec.Size()

			dupedVectors = append(dupedVectors, newVec)
			indexOfNewVec := len(dupedVectors) - 1
			for j := range bat.Vecs {
				if bat.Vecs[j] == oldVec {
					duped[j] = indexOfNewVec
				}
			}
		}
	}

	// use new vector to replace the old vector.
	for i, index := range duped {
		bat.Vecs[i] = dupedVectors[index]
	}
	return dupSize, nil
}

func NewJoinBatch(bat *batch.Batch, mp *mpool.MPool) (*batch.Batch,
	[]func(*vector.Vector, *vector.Vector, int64, int) error) {
	rbat := batch.NewWithSize(bat.VectorCount())
	cfs := make([]func(*vector.Vector, *vector.Vector, int64, int) error, bat.VectorCount())
	for i, vec := range bat.Vecs {
		typ := *vec.GetType()
		rbat.Vecs[i] = vector.NewConstNull(typ, 0, nil)
		cfs[i] = vector.GetConstSetFunction(typ, mp)
	}
	rbat.Zs = mp.GetSels()
	return rbat, cfs
}

func SetJoinBatchValues(joinBat, bat *batch.Batch, sel int64, length int,
	cfs []func(*vector.Vector, *vector.Vector, int64, int) error) error {
	for i, vec := range bat.Vecs {
		if err := cfs[i](joinBat.Vecs[i], vec, sel, length); err != nil {
			return err
		}
	}
	if n := cap(joinBat.Zs); n < length {
		joinBat.Zs = joinBat.Zs[:n]
		for ; n < length; n++ {
			joinBat.Zs = append(joinBat.Zs, 1)
		}
	}
	joinBat.Zs = joinBat.Zs[:length]
	return nil
}
