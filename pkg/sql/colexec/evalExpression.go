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
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	util2 "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
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
	constBinType        = types.T_varbinary.ToType()
	constDateType       = types.T_date.ToType()
	constTimeType       = types.T_time.ToType()
	constDatetimeType   = types.T_datetime.ToType()
	constEnumType       = types.T_enum.ToType()
	constTimestampTypes = []types.Type{
		types.New(types.T_timestamp, 0, 0),
		types.New(types.T_timestamp, 0, 1),
		types.New(types.T_timestamp, 0, 2),
		types.New(types.T_timestamp, 0, 3),
		types.New(types.T_timestamp, 0, 4),
		types.New(types.T_timestamp, 0, 5),
		types.New(types.T_timestamp, 0, 6),
	}
	//No need to add T_array here, as Array is cast from varchar.
)

// ExpressionExecutor
// generated from plan.Expr, can evaluate the result from vectors directly.
type ExpressionExecutor interface {
	// Eval will return the result vector of expression.
	// the result memory is reused, so it should not be modified or saved.
	// If it needs, it should be copied by vector.Dup().
	Eval(proc *process.Process, batches []*batch.Batch) (*vector.Vector, error)

	// EvalWithoutResultReusing is the same as Eval, but it will not reuse the memory of result vector.
	// so you can save the result vector directly. but should be careful about memory leak.
	// and watch out that maybe the vector is one of the input vectors of batches.
	EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch) (*vector.Vector, error)

	// Free should release all memory of executor.
	// it will be called after query has done.
	Free()

	IsColumnExpr() bool
}

func NewExpressionExecutorsFromPlanExpressions(proc *process.Process, planExprs []*plan.Expr) (executors []ExpressionExecutor, err error) {
	executors = make([]ExpressionExecutor, len(planExprs))
	for i := range executors {
		executors[i], err = NewExpressionExecutor(proc, planExprs[i])
		if err != nil {
			for j := 0; j < i; j++ {
				executors[j].Free()
			}
			return nil, err
		}
	}
	return executors, err
}

func NewExpressionExecutor(proc *process.Process, planExpr *plan.Expr) (ExpressionExecutor, error) {
	switch t := planExpr.Expr.(type) {
	case *plan.Expr_Lit:
		typ := types.New(types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale)
		vec, err := generateConstExpressionExecutor(proc, typ, t.Lit)
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

	case *plan.Expr_Col:
		typ := types.New(types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale)
		return &ColumnExpressionExecutor{
			mp:       proc.Mp(),
			relIndex: int(t.Col.RelPos),
			colIndex: int(t.Col.ColPos),
			typ:      typ,
		}, nil

	case *plan.Expr_P:
		return &ParamExpressionExecutor{
			mp:  proc.Mp(),
			vec: nil,
			pos: int(t.P.Pos),
			typ: types.T_text.ToType(),
		}, nil

	case *plan.Expr_V:
		typ := types.New(types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale)
		return &VarExpressionExecutor{
			mp:     proc.Mp(),
			name:   t.V.Name,
			system: t.V.System,
			global: t.V.Global,
			typ:    typ,
		}, nil

	case *plan.Expr_Vec:
		vec := vector.NewVec(types.T_any.ToType())
		err := vec.UnmarshalBinary(t.Vec.Data)
		if err != nil {
			return nil, err
		}
		return &FixedVectorExpressionExecutor{
			m:            proc.Mp(),
			fixed:        true,
			resultVector: vec,
		}, nil

	case *plan.Expr_F:
		overload, err := function.GetFunctionById(proc.Ctx, t.F.GetFunc().GetObj())
		if err != nil {
			return nil, err
		}

		executor := &FunctionExpressionExecutor{}
		typ := types.New(types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale)
		if err = executor.Init(proc, len(t.F.Args), typ, overload.GetExecuteMethod()); err != nil {
			return nil, err
		}

		for i := range executor.parameterExecutor {
			subExecutor, paramErr := NewExpressionExecutor(proc, t.F.Args[i])
			if paramErr != nil {
				for j := 0; j < i; j++ {
					executor.parameterExecutor[j].Free()
				}
				return nil, paramErr
			}
			executor.SetParameter(i, subExecutor)
		}

		// IF all parameters here were constant. and this function can be folded.
		// 	there is a better way to convert it as a FixedVectorExpressionExecutor.
		if !overload.CannotFold() && !overload.IsRealTimeRelated() && ifAllArgsAreConstant(executor) {
			for i := range executor.parameterExecutor {
				fixExe := executor.parameterExecutor[i].(*FixedVectorExpressionExecutor)
				executor.parameterResults[i] = fixExe.resultVector
				if !fixExe.fixed {
					executor.parameterResults[i].SetLength(1)
				}
			}

			execLen := 1
			if len(executor.parameterResults) > 0 {
				firstParam := executor.parameterResults[0]
				if !firstParam.IsConst() {
					execLen = firstParam.Length()
				}
			}

			if err = executor.resultVector.PreExtendAndReset(execLen); err != nil {
				executor.Free()
				return nil, err
			}

			err = executor.evalFn(executor.parameterResults, executor.resultVector, proc, execLen)
			if err == nil {
				mp := proc.Mp()

				result := executor.resultVector.GetResultVector()
				fixed := &FixedVectorExpressionExecutor{
					m: mp,
				}

				if execLen == 1 {
					// ToConst just returns a new pointer to the same memory.
					// so we need to duplicate it.
					fixed.resultVector, err = result.ToConst(0, 1, mp).Dup(mp)
				} else {
					fixed.fixed = true
					fixed.resultVector = result
				}
				executor.Free()
				if err != nil {
					return nil, err
				}
				return fixed, nil
			}
			executor.Free()
			return nil, err
		}

		return executor, nil
	}

	return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("unsupported expression executor for %v now", planExpr))
}

func EvalExpressionOnce(proc *process.Process, planExpr *plan.Expr, batches []*batch.Batch) (*vector.Vector, error) {
	executor, err := NewExpressionExecutor(proc, planExpr)
	if err != nil {
		return nil, err
	}
	defer executor.Free()

	vec, err := executor.Eval(proc, batches)
	if err != nil {
		return nil, err
	}

	// if memory reuse, we can get it directly because we do only one evaluate.
	if e, ok := executor.(*FunctionExpressionExecutor); ok {
		e.resultVector = nil
		return vec, nil
	}
	if e, ok := executor.(*FixedVectorExpressionExecutor); ok {
		e.resultVector = nil
		return vec, nil
	}

	// I'm not sure if dup is good. but if not.
	// we should check batch's cnt first, get if it's 1, dup if not.
	nv, er := vec.Dup(proc.Mp())
	if er != nil {
		return nil, er
	}
	return nv, nil
}

func ifAllArgsAreConstant(executor *FunctionExpressionExecutor) bool {
	for _, paramE := range executor.parameterExecutor {
		if _, ok := paramE.(*FixedVectorExpressionExecutor); !ok {
			return false
		}
	}
	return true
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
	mp       *mpool.MPool
	relIndex int
	colIndex int

	// result type.
	typ types.Type
	// we should new and cache a null vector here.
	// because we need to change its type when doing the execution for const null vector.
	// but other process may using its type at the same time.
	nullVecCache *vector.Vector
}

func (expr *ColumnExpressionExecutor) GetRelIndex() int {
	return expr.relIndex
}

func (expr *ColumnExpressionExecutor) GetColIndex() int {
	return expr.colIndex
}

type ParamExpressionExecutor struct {
	mp   *mpool.MPool
	null *vector.Vector
	vec  *vector.Vector
	pos  int
	typ  types.Type
}

func (expr *ParamExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch) (*vector.Vector, error) {
	val, err := proc.GetPrepareParamsAt(int(expr.pos))
	if err != nil {
		return nil, err
	}

	if val == nil {
		if expr.null == nil {
			expr.null = vector.NewConstNull(expr.typ, 1, proc.GetMPool())
		}
		return expr.null, nil
	}

	if expr.vec == nil {
		expr.vec = vector.NewConstBytes(expr.typ, val, 1, proc.Mp())
	} else {
		err := vector.SetConstBytes(expr.vec, val, 1, proc.GetMPool())
		if err != nil {
			return nil, err
		}
	}
	return expr.vec, nil
}

func (expr *ParamExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches)
	if err != nil {
		return nil, err
	}
	if vec == expr.null {
		expr.null = nil
		return vec, nil
	}
	expr.vec = nil
	return vec, nil
}

func (expr *ParamExpressionExecutor) Free() {
	if expr.vec != nil {
		expr.vec.Free(expr.mp)
		expr.vec = nil
	}
	if expr.null != nil {
		expr.null.Free(expr.mp)
		expr.null = nil
	}
}

func (expr *ParamExpressionExecutor) IsColumnExpr() bool {
	return false
}

type VarExpressionExecutor struct {
	mp   *mpool.MPool
	null *vector.Vector
	vec  *vector.Vector

	name   string
	system bool
	global bool
	typ    types.Type
}

func (expr *VarExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch) (*vector.Vector, error) {
	val, err := proc.GetResolveVariableFunc()(expr.name, expr.system, expr.global)
	if err != nil {
		return nil, err
	}

	if val == nil {
		if expr.null == nil {
			expr.null, err = util.GenVectorByVarValue(proc, expr.typ, nil)
		}
		return expr.null, err
	}

	if expr.vec == nil {
		expr.vec, err = util.GenVectorByVarValue(proc, expr.typ, val)
	} else {
		switch v := val.(type) {
		case []byte:
			err = vector.SetConstBytes(expr.vec, v, 1, proc.GetMPool())
		case string:
			err = vector.SetConstBytes(expr.vec, util2.UnsafeStringToBytes(v), 1, proc.GetMPool())
		default:
			err = vector.SetConstBytes(expr.vec, util2.UnsafeStringToBytes(fmt.Sprintf("%v", v)), 1, proc.GetMPool())
		}
	}
	return expr.vec, err
}

func (expr *VarExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches)
	if err != nil {
		return nil, err
	}
	if vec == expr.null {
		expr.null = nil
		return vec, nil
	}
	expr.vec = nil
	return vec, nil
}

func (expr *VarExpressionExecutor) Free() {
	if expr.vec != nil {
		expr.vec.Free(expr.mp)
		expr.vec = nil
	}
	if expr.null != nil {
		expr.null.Free(expr.mp)
		expr.null = nil
	}
}

func (expr *VarExpressionExecutor) IsColumnExpr() bool {
	return false
}

func (expr *FunctionExpressionExecutor) Init(
	proc *process.Process,
	parameterNum int,
	retType types.Type,
	fn func(
		params []*vector.Vector,
		result vector.FunctionResultWrapper,
		proc *process.Process,
		length int) error) (err error) {
	m := proc.Mp()

	expr.m = m
	expr.evalFn = fn
	expr.parameterResults = make([]*vector.Vector, parameterNum)
	expr.parameterExecutor = make([]ExpressionExecutor, parameterNum)

	expr.resultVector = vector.NewFunctionResultWrapper(proc.GetVector, proc.PutVector, retType, m)
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

	if err = expr.resultVector.PreExtendAndReset(batches[0].RowCount()); err != nil {
		return nil, err
	}

	if err = expr.evalFn(
		expr.parameterResults, expr.resultVector, proc, batches[0].RowCount()); err != nil {
		return nil, err
	}
	return expr.resultVector.GetResultVector(), nil
}

func (expr *FunctionExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches)
	if err != nil {
		return nil, err
	}
	expr.resultVector.SetResultVector(nil)
	return vec, nil
}

func (expr *FunctionExpressionExecutor) Free() {
	if expr.resultVector != nil {
		expr.resultVector.Free()
		expr.resultVector = nil
	}
	for _, p := range expr.parameterExecutor {
		p.Free()
	}
}

func (expr *FunctionExpressionExecutor) SetParameter(index int, executor ExpressionExecutor) {
	expr.parameterExecutor[index] = executor
}

func (expr *FunctionExpressionExecutor) IsColumnExpr() bool {
	return false
}

func (expr *ColumnExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch) (*vector.Vector, error) {
	relIndex := expr.relIndex
	// XXX it's a bad hack here. root cause is pipeline set a wrong relation index here.
	if len(batches) == 1 {
		relIndex = 0
	}

	// protected code. In fact, we shouldn't receive a wrong index here.
	// if happens, it means it's a bad expression for the input data that we cannot calculate it.
	if len(batches) <= relIndex || len(batches[relIndex].Vecs) <= expr.colIndex {
		return nil, moerr.NewInternalError(proc.Ctx, "unexpected input batch for column expression")
	}

	vec := batches[relIndex].Vecs[expr.colIndex]
	if vec.IsConstNull() {
		vec = expr.getConstNullVec(expr.typ, vec.Length())
	}
	return vec, nil
}

func (expr *ColumnExpressionExecutor) getConstNullVec(typ types.Type, length int) *vector.Vector {
	if expr.nullVecCache != nil {
		expr.nullVecCache.SetType(typ)
		expr.nullVecCache.SetLength(length)
	} else {
		expr.nullVecCache = vector.NewConstNull(typ, length, expr.mp)
	}
	return expr.nullVecCache
}

func (expr *ColumnExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches)
	if vec == expr.nullVecCache {
		expr.nullVecCache = nil
	}
	return vec, err
}

func (expr *ColumnExpressionExecutor) Free() {
	if expr.nullVecCache != nil {
		expr.nullVecCache.Free(expr.mp)
		expr.nullVecCache = nil
	}
}

func (expr *ColumnExpressionExecutor) IsColumnExpr() bool {
	return true
}

func (expr *FixedVectorExpressionExecutor) Eval(_ *process.Process, batches []*batch.Batch) (*vector.Vector, error) {
	if !expr.fixed {
		expr.resultVector.SetLength(batches[0].RowCount())
	}
	return expr.resultVector, nil
}

func (expr *FixedVectorExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches)
	if err != nil {
		return nil, err
	}
	return vec.Dup(proc.Mp())
}

func (expr *FixedVectorExpressionExecutor) Free() {
	if expr.resultVector == nil {
		return
	}
	expr.resultVector.Free(expr.m)
	expr.resultVector = nil
}

func (expr *FixedVectorExpressionExecutor) IsColumnExpr() bool {
	return false
}

func generateConstExpressionExecutor(proc *process.Process, typ types.Type, con *plan.Literal) (*vector.Vector, error) {
	var vec *vector.Vector

	if con.GetIsnull() {
		vec = vector.NewConstNull(typ, 1, proc.Mp())
	} else {
		switch con.GetValue().(type) {
		case *plan.Literal_Bval:
			vec = vector.NewConstFixed(constBType, con.GetBval(), 1, proc.Mp())
		case *plan.Literal_I8Val:
			vec = vector.NewConstFixed(constI8Type, int8(con.GetI8Val()), 1, proc.Mp())
		case *plan.Literal_I16Val:
			vec = vector.NewConstFixed(constI16Type, int16(con.GetI16Val()), 1, proc.Mp())
		case *plan.Literal_I32Val:
			vec = vector.NewConstFixed(constI32Type, con.GetI32Val(), 1, proc.Mp())
		case *plan.Literal_I64Val:
			vec = vector.NewConstFixed(constI64Type, con.GetI64Val(), 1, proc.Mp())
		case *plan.Literal_U8Val:
			vec = vector.NewConstFixed(constU8Type, uint8(con.GetU8Val()), 1, proc.Mp())
		case *plan.Literal_U16Val:
			vec = vector.NewConstFixed(constU16Type, uint16(con.GetU16Val()), 1, proc.Mp())
		case *plan.Literal_U32Val:
			vec = vector.NewConstFixed(constU32Type, con.GetU32Val(), 1, proc.Mp())
		case *plan.Literal_U64Val:
			vec = vector.NewConstFixed(constU64Type, con.GetU64Val(), 1, proc.Mp())
		case *plan.Literal_Fval:
			vec = vector.NewConstFixed(constFType, con.GetFval(), 1, proc.Mp())
		case *plan.Literal_Dval:
			vec = vector.NewConstFixed(constDType, con.GetDval(), 1, proc.Mp())
		case *plan.Literal_Dateval:
			vec = vector.NewConstFixed(constDateType, types.Date(con.GetDateval()), 1, proc.Mp())
		case *plan.Literal_Timeval:
			vec = vector.NewConstFixed(typ, types.Time(con.GetTimeval()), 1, proc.Mp())
		case *plan.Literal_Datetimeval:
			vec = vector.NewConstFixed(typ, types.Datetime(con.GetDatetimeval()), 1, proc.Mp())
		case *plan.Literal_Decimal64Val:
			cd64 := con.GetDecimal64Val()
			d64 := types.Decimal64(cd64.A)
			vec = vector.NewConstFixed(typ, d64, 1, proc.Mp())
		case *plan.Literal_Decimal128Val:
			cd128 := con.GetDecimal128Val()
			d128 := types.Decimal128{B0_63: uint64(cd128.A), B64_127: uint64(cd128.B)}
			vec = vector.NewConstFixed(typ, d128, 1, proc.Mp())
		case *plan.Literal_Timestampval:
			scale := typ.Scale
			if scale < 0 || scale > 6 {
				return nil, moerr.NewInternalError(proc.Ctx, "invalid timestamp scale")
			}
			vec = vector.NewConstFixed(constTimestampTypes[scale], types.Timestamp(con.GetTimestampval()), 1, proc.Mp())
		case *plan.Literal_Sval:
			sval := con.GetSval()
			// Distinguish binary with non-binary string.
			if typ.Oid == types.T_binary || typ.Oid == types.T_varbinary || typ.Oid == types.T_blob {
				vec = vector.NewConstBytes(constBinType, []byte(sval), 1, proc.Mp())
			} else if typ.Oid == types.T_array_float32 {
				array, err := types.StringToArray[float32](sval)
				if err != nil {
					return nil, err
				}
				vec = vector.NewConstArray(typ, array, 1, proc.Mp())
			} else if typ.Oid == types.T_array_float64 {
				array, err := types.StringToArray[float64](sval)
				if err != nil {
					return nil, err
				}
				vec = vector.NewConstArray(typ, array, 1, proc.Mp())
			} else {
				vec = vector.NewConstBytes(constSType, []byte(sval), 1, proc.Mp())
			}
		case *plan.Literal_Defaultval:
			defaultVal := con.GetDefaultval()
			vec = vector.NewConstFixed(constBType, defaultVal, 1, proc.Mp())
		case *plan.Literal_EnumVal:
			vec = vector.NewConstFixed(constEnumType, uint16(con.GetU16Val()), 1, proc.Mp())
		default:
			return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("const expression %v", con.GetValue()))
		}
		vec.SetIsBin(con.IsBin)
	}
	return vec, nil
}

func GenerateConstListExpressionExecutor(proc *process.Process, exprs []*plan.Expr) (*vector.Vector, error) {
	lenList := len(exprs)
	vec, err := proc.AllocVectorOfRows(types.New(types.T(exprs[0].Typ.Id), exprs[0].Typ.Width, exprs[0].Typ.Scale), lenList, nil)
	if err != nil {
		return nil, err
	}
	for i := 0; i < lenList; i++ {
		expr := exprs[i]
		t, ok := expr.Expr.(*plan.Expr_Lit)
		if !ok {
			return nil, moerr.NewInternalError(proc.Ctx, "args in list must be constant")
		}
		if t.Lit.GetIsnull() {
			vec.GetNulls().Set(uint64(i))
		} else {
			switch t.Lit.GetValue().(type) {
			case *plan.Literal_Bval:
				veccol := vector.MustFixedCol[bool](vec)
				veccol[i] = t.Lit.GetBval()
			case *plan.Literal_I8Val:
				veccol := vector.MustFixedCol[int8](vec)
				veccol[i] = int8(t.Lit.GetI8Val())
			case *plan.Literal_I16Val:
				veccol := vector.MustFixedCol[int16](vec)
				veccol[i] = int16(t.Lit.GetI16Val())
			case *plan.Literal_I32Val:
				veccol := vector.MustFixedCol[int32](vec)
				veccol[i] = t.Lit.GetI32Val()
			case *plan.Literal_I64Val:
				veccol := vector.MustFixedCol[int64](vec)
				veccol[i] = t.Lit.GetI64Val()
			case *plan.Literal_U8Val:
				veccol := vector.MustFixedCol[uint8](vec)
				veccol[i] = uint8(t.Lit.GetU8Val())
			case *plan.Literal_U16Val:
				veccol := vector.MustFixedCol[uint16](vec)
				veccol[i] = uint16(t.Lit.GetU16Val())
			case *plan.Literal_U32Val:
				veccol := vector.MustFixedCol[uint32](vec)
				veccol[i] = t.Lit.GetU32Val()
			case *plan.Literal_U64Val:
				veccol := vector.MustFixedCol[uint64](vec)
				veccol[i] = t.Lit.GetU64Val()
			case *plan.Literal_Fval:
				veccol := vector.MustFixedCol[float32](vec)
				veccol[i] = t.Lit.GetFval()
			case *plan.Literal_Dval:
				veccol := vector.MustFixedCol[float64](vec)
				veccol[i] = t.Lit.GetDval()
			case *plan.Literal_Dateval:
				veccol := vector.MustFixedCol[types.Date](vec)
				veccol[i] = types.Date(t.Lit.GetDateval())
			case *plan.Literal_Timeval:
				veccol := vector.MustFixedCol[types.Time](vec)
				veccol[i] = types.Time(t.Lit.GetTimeval())
			case *plan.Literal_Datetimeval:
				veccol := vector.MustFixedCol[types.Datetime](vec)
				veccol[i] = types.Datetime(t.Lit.GetDatetimeval())
			case *plan.Literal_Decimal64Val:
				cd64 := t.Lit.GetDecimal64Val()
				d64 := types.Decimal64(cd64.A)
				veccol := vector.MustFixedCol[types.Decimal64](vec)
				veccol[i] = d64
			case *plan.Literal_Decimal128Val:
				cd128 := t.Lit.GetDecimal128Val()
				d128 := types.Decimal128{B0_63: uint64(cd128.A), B64_127: uint64(cd128.B)}
				veccol := vector.MustFixedCol[types.Decimal128](vec)
				veccol[i] = d128
			case *plan.Literal_Timestampval:
				scale := expr.Typ.Scale
				if scale < 0 || scale > 6 {
					return nil, moerr.NewInternalError(proc.Ctx, "invalid timestamp scale")
				}
				veccol := vector.MustFixedCol[types.Timestamp](vec)
				veccol[i] = types.Timestamp(t.Lit.GetTimestampval())
			case *plan.Literal_Sval:
				sval := t.Lit.GetSval()
				err = vector.SetStringAt(vec, i, sval, proc.Mp())
				if err != nil {
					return nil, err
				}
			case *plan.Literal_Defaultval:
				defaultVal := t.Lit.GetDefaultval()
				veccol := vector.MustFixedCol[bool](vec)
				veccol[i] = defaultVal
			default:
				return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("const expression %v", t.Lit.GetValue()))
			}
			vec.SetIsBin(t.Lit.IsBin)
		}
	}
	return vec, nil
}

// FixProjectionResult set result vector for rbat.
// sbat is the source batch.
func FixProjectionResult(proc *process.Process,
	executors []ExpressionExecutor,
	uafs []func(v, w *vector.Vector) error,
	rbat *batch.Batch, sbat *batch.Batch) (dupSize int, err error) {
	dupSize = 0

	alreadySet := make([]int, len(rbat.Vecs))
	for i := range alreadySet {
		alreadySet[i] = -1
	}

	finalVectors := make([]*vector.Vector, 0, len(rbat.Vecs))
	for i, oldVec := range rbat.Vecs {
		if alreadySet[i] < 0 {
			newVec := (*vector.Vector)(nil)
			if columnExpr, ok := executors[i].(*ColumnExpressionExecutor); ok {
				if sbat.GetCnt() == 1 {
					newVec = oldVec
					if columnExpr.nullVecCache != nil && oldVec == columnExpr.nullVecCache {
						newVec = vector.NewConstNull(columnExpr.typ, oldVec.Length(), proc.Mp())
						dupSize += newVec.Size()
					}
					sbat.ReplaceVector(oldVec, nil)
				} else {
					newVec = proc.GetVector(*oldVec.GetType())
					err = uafs[i](newVec, oldVec)
					if err != nil {
						for j := range finalVectors {
							finalVectors[j].Free(proc.Mp())
						}
						newVec.Free(proc.Mp())
						return 0, err
					}
				}
				dupSize += newVec.Size()
			} else if functionExpr, ok := executors[i].(*FunctionExpressionExecutor); ok {
				// if projection, we can get the result directly
				newVec = functionExpr.resultVector.GetResultVector()
				functionExpr.resultVector.SetResultVector(nil)
			} else {
				if uafs[i] != nil {
					newVec = proc.GetVector(*oldVec.GetType())
					err = uafs[i](newVec, oldVec)
				} else {
					newVec, err = oldVec.Dup(proc.Mp())
				}
				if err != nil {
					for j := range finalVectors {
						finalVectors[j].Free(proc.Mp())
					}
					if newVec != nil {
						newVec.Free(proc.Mp())
					}
					return 0, err
				}
				dupSize += newVec.Size()
			}

			finalVectors = append(finalVectors, newVec)
			indexOfNewVec := len(finalVectors) - 1
			for j := range rbat.Vecs {
				if rbat.Vecs[j] == oldVec {
					alreadySet[j] = indexOfNewVec
				}
			}
		}
	}

	// use new vector to replace the old vector.
	for i, idx := range alreadySet {
		rbat.Vecs[i] = finalVectors[idx]
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
	return rbat, cfs
}

func SetJoinBatchValues(joinBat, bat *batch.Batch, sel int64, length int,
	cfs []func(*vector.Vector, *vector.Vector, int64, int) error) error {
	for i, vec := range bat.Vecs {
		if err := cfs[i](joinBat.Vecs[i], vec, sel, length); err != nil {
			return err
		}
	}
	joinBat.SetRowCount(length)
	return nil
}

var noColumnBatchForZoneMap = []*batch.Batch{batch.NewWithSize(0)}

func getConstZM(
	ctx context.Context,
	expr *plan.Expr,
	proc *process.Process,
) (zm index.ZM, err error) {
	c := expr.Expr.(*plan.Expr_Lit)
	if c.Lit.GetIsnull() {
		zm = index.NewZM(types.T(expr.Typ.Id), expr.Typ.Scale)
		return
	}
	switch c.Lit.GetValue().(type) {
	case *plan.Literal_Bval:
		zm = index.NewZM(constBType.Oid, 0)
		v := c.Lit.GetBval()
		index.UpdateZM(zm, types.EncodeBool(&v))
	case *plan.Literal_I8Val:
		zm = index.NewZM(constI8Type.Oid, 0)
		v := int8(c.Lit.GetI8Val())
		index.UpdateZM(zm, types.EncodeInt8(&v))
	case *plan.Literal_I16Val:
		zm = index.NewZM(constI16Type.Oid, 0)
		v := int16(c.Lit.GetI16Val())
		index.UpdateZM(zm, types.EncodeInt16(&v))
	case *plan.Literal_I32Val:
		zm = index.NewZM(constI32Type.Oid, 0)
		v := c.Lit.GetI32Val()
		index.UpdateZM(zm, types.EncodeInt32(&v))
	case *plan.Literal_I64Val:
		zm = index.NewZM(constI64Type.Oid, 0)
		v := c.Lit.GetI64Val()
		index.UpdateZM(zm, types.EncodeInt64(&v))
	case *plan.Literal_U8Val:
		zm = index.NewZM(constU8Type.Oid, 0)
		v := uint8(c.Lit.GetU8Val())
		index.UpdateZM(zm, types.EncodeUint8(&v))
	case *plan.Literal_U16Val:
		zm = index.NewZM(constU16Type.Oid, 0)
		v := uint16(c.Lit.GetU16Val())
		index.UpdateZM(zm, types.EncodeUint16(&v))
	case *plan.Literal_U32Val:
		zm = index.NewZM(constU32Type.Oid, 0)
		v := c.Lit.GetU32Val()
		index.UpdateZM(zm, types.EncodeUint32(&v))
	case *plan.Literal_U64Val:
		zm = index.NewZM(constU64Type.Oid, 0)
		v := c.Lit.GetU64Val()
		index.UpdateZM(zm, types.EncodeUint64(&v))
	case *plan.Literal_Fval:
		zm = index.NewZM(constFType.Oid, 0)
		v := c.Lit.GetFval()
		index.UpdateZM(zm, types.EncodeFloat32(&v))
	case *plan.Literal_Dval:
		zm = index.NewZM(constDType.Oid, 0)
		v := c.Lit.GetDval()
		index.UpdateZM(zm, types.EncodeFloat64(&v))
	case *plan.Literal_Dateval:
		zm = index.NewZM(constDateType.Oid, 0)
		v := c.Lit.GetDateval()
		index.UpdateZM(zm, types.EncodeInt32(&v))
	case *plan.Literal_Timeval:
		zm = index.NewZM(constTimeType.Oid, 0)
		v := c.Lit.GetTimeval()
		index.UpdateZM(zm, types.EncodeInt64(&v))
	case *plan.Literal_Datetimeval:
		zm = index.NewZM(constDatetimeType.Oid, 0)
		v := c.Lit.GetDatetimeval()
		index.UpdateZM(zm, types.EncodeInt64(&v))
	case *plan.Literal_Decimal64Val:
		v := c.Lit.GetDecimal64Val()
		zm = index.NewZM(types.T_decimal64, expr.Typ.Scale)
		d64 := types.Decimal64(v.A)
		index.UpdateZM(zm, types.EncodeDecimal64(&d64))
	case *plan.Literal_Decimal128Val:
		v := c.Lit.GetDecimal128Val()
		zm = index.NewZM(types.T_decimal128, expr.Typ.Scale)
		d128 := types.Decimal128{B0_63: uint64(v.A), B64_127: uint64(v.B)}
		index.UpdateZM(zm, types.EncodeDecimal128(&d128))
	case *plan.Literal_Timestampval:
		v := c.Lit.GetTimestampval()
		scale := expr.Typ.Scale
		if scale < 0 || scale > 6 {
			err = moerr.NewInternalError(proc.Ctx, "invalid timestamp scale")
			return
		}
		zm = index.NewZM(constTimestampTypes[0].Oid, scale)
		index.UpdateZM(zm, types.EncodeInt64(&v))
	case *plan.Literal_Sval:
		zm = index.NewZM(constSType.Oid, 0)
		v := c.Lit.GetSval()
		index.UpdateZM(zm, []byte(v))
	case *plan.Literal_Defaultval:
		zm = index.NewZM(constBType.Oid, 0)
		v := c.Lit.GetDefaultval()
		index.UpdateZM(zm, types.EncodeBool(&v))
	case *plan.Literal_EnumVal:
		zm = index.NewZM(constEnumType.Oid, 0)
		v := types.Enum(c.Lit.GetU16Val())
		index.UpdateZM(zm, types.EncodeEnum(&v))
	default:
		err = moerr.NewNYI(ctx, fmt.Sprintf("const expression %v", c.Lit.GetValue()))
	}
	return
}

func EvaluateFilterByZoneMap(
	ctx context.Context, // why we need a context here, to escape trace?
	proc *process.Process,
	expr *plan.Expr,
	meta objectio.ColumnMetaFetcher,
	columnMap map[int]int,
	zms []objectio.ZoneMap,
	vecs []*vector.Vector) (selected bool) {
	if expr == nil {
		selected = true
		return
	}

	if len(columnMap) == 0 {
		// XXX should we need to check expr.oid = bool or not ?

		vec, err := EvalExpressionOnce(proc, expr, noColumnBatchForZoneMap)
		if err != nil {
			return true
		}
		cols := vector.MustFixedCol[bool](vec)
		for _, isNeed := range cols {
			if isNeed {
				vec.Free(proc.Mp())
				return true
			}
		}
		vec.Free(proc.Mp())
		return false
	}

	zm := GetExprZoneMap(ctx, proc, expr, meta, columnMap, zms, vecs)
	if !zm.IsInited() || zm.GetType() != types.T_bool {
		selected = false
	} else {
		selected = types.DecodeBool(zm.GetMaxBuf())
	}

	// clean the vector.
	for i := range vecs {
		if vecs[i] != nil {
			vecs[i].Free(proc.Mp())
			vecs[i] = nil
		}
	}
	return
}

func GetExprZoneMap(
	ctx context.Context,
	proc *process.Process,
	expr *plan.Expr,
	meta objectio.ColumnMetaFetcher,
	columnMap map[int]int,
	zms []objectio.ZoneMap,
	vecs []*vector.Vector) (v objectio.ZoneMap) {
	var err error

	switch t := expr.Expr.(type) {
	case *plan.Expr_Lit:
		if zms[expr.AuxId] == nil {
			if zms[expr.AuxId], err = getConstZM(ctx, expr, proc); err != nil {
				zms[expr.AuxId] = objectio.NewZM(types.T_bool, 0)
			}
		}

	case *plan.Expr_Col:
		zms[expr.AuxId] = meta.MustGetColumn(uint16(columnMap[int(t.Col.ColPos)])).ZoneMap()

	case *plan.Expr_F:
		id := t.F.GetFunc().GetObj()
		if overload, errGetFunc := function.GetFunctionById(ctx, id); errGetFunc != nil {
			zms[expr.AuxId].Reset()

		} else {
			args := t.F.Args

			// Some expressions need to be handled specifically
			switch t.F.Func.ObjName {
			case "in":
				rid := args[1].AuxId
				if vecs[rid] == nil {
					if data, ok := args[1].Expr.(*plan.Expr_Vec); ok {
						vec := proc.GetVector(types.T_any.ToType())
						vec.UnmarshalBinary(data.Vec.Data)
						vecs[rid] = vec
					} else {
						zms[expr.AuxId].Reset()
						vecs[rid] = vector.NewConstNull(types.T_any.ToType(), math.MaxInt, proc.Mp())
						return zms[expr.AuxId]
					}
				}

				if vecs[rid].IsConstNull() && vecs[rid].Length() == math.MaxInt {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				lhs := GetExprZoneMap(ctx, proc, args[0], meta, columnMap, zms, vecs)
				if !lhs.IsInited() {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], lhs.AnyIn(vecs[rid]))
				return zms[expr.AuxId]

			case "prefix_eq":
				lhs := GetExprZoneMap(ctx, proc, args[0], meta, columnMap, zms, vecs)
				if !lhs.IsInited() {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				s := []byte(args[1].GetLit().GetSval())

				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], lhs.PrefixEq(s))
				return zms[expr.AuxId]

			case "prefix_between":
				lhs := GetExprZoneMap(ctx, proc, args[0], meta, columnMap, zms, vecs)
				if !lhs.IsInited() {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				lb := []byte(args[1].GetLit().GetSval())
				ub := []byte(args[2].GetLit().GetSval())

				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], lhs.PrefixBetween(lb, ub))
				return zms[expr.AuxId]

			case "prefix_in":
				rid := args[1].AuxId
				if vecs[rid] == nil {
					if data, ok := args[1].Expr.(*plan.Expr_Vec); ok {
						vec := proc.GetVector(types.T_any.ToType())
						vec.UnmarshalBinary(data.Vec.Data)
						vecs[rid] = vec
					} else {
						zms[expr.AuxId].Reset()
						vecs[rid] = vector.NewConstNull(types.T_any.ToType(), math.MaxInt, proc.Mp())
						return zms[expr.AuxId]
					}
				}

				if vecs[rid].IsConstNull() && vecs[rid].Length() == math.MaxInt {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				lhs := GetExprZoneMap(ctx, proc, args[0], meta, columnMap, zms, vecs)
				if !lhs.IsInited() {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], lhs.PrefixIn(vecs[rid]))
				return zms[expr.AuxId]
			}

			f := func() bool {
				for _, arg := range args {
					zms[arg.AuxId] = GetExprZoneMap(ctx, proc, arg, meta, columnMap, zms, vecs)
					if !zms[arg.AuxId].IsInited() {
						zms[expr.AuxId].Reset()
						return true
					}
				}
				return false
			}

			var res, ok bool
			switch t.F.Func.ObjName {
			case ">":
				if f() {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].AnyGT(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "<":
				if f() {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].AnyLT(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case ">=":
				if f() {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].AnyGE(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "<=":
				if f() {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].AnyLE(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "=":
				if f() {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].Intersect(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "between":
				if f() {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].AnyBetween(zms[args[1].AuxId], zms[args[2].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "and":
				if f() {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].And(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "or":
				if f() {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].Or(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "+":
				if f() {
					return zms[expr.AuxId]
				}
				zms[expr.AuxId] = index.ZMPlus(zms[args[0].AuxId], zms[args[1].AuxId], zms[expr.AuxId])

			case "-":
				if f() {
					return zms[expr.AuxId]
				}
				zms[expr.AuxId] = index.ZMMinus(zms[args[0].AuxId], zms[args[1].AuxId], zms[expr.AuxId])

			case "*":
				if f() {
					return zms[expr.AuxId]
				}
				zms[expr.AuxId] = index.ZMMulti(zms[args[0].AuxId], zms[args[1].AuxId], zms[expr.AuxId])

			default:
				ivecs := make([]*vector.Vector, len(args))
				if isAllConst(args) { // constant fold
					for i, arg := range args {
						if vecs[arg.AuxId] != nil {
							vecs[arg.AuxId].Free(proc.Mp())
						}
						if vecs[arg.AuxId], err = EvalExpressionOnce(proc, arg, []*batch.Batch{batch.EmptyForConstFoldBatch}); err != nil {
							zms[expr.AuxId].Reset()
							return zms[expr.AuxId]
						}
						ivecs[i] = vecs[arg.AuxId]
					}
				} else {
					if f() {
						return zms[expr.AuxId]
					}
					for i, arg := range args {
						if vecs[arg.AuxId] != nil {
							vecs[arg.AuxId].Free(proc.Mp())
						}
						if vecs[arg.AuxId], err = index.ZMToVector(zms[arg.AuxId], vecs[arg.AuxId], proc.Mp()); err != nil {
							zms[expr.AuxId].Reset()
							return zms[expr.AuxId]
						}
						ivecs[i] = vecs[arg.AuxId]
					}
				}
				fn := overload.GetExecuteMethod()
				typ := types.New(types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale)

				result := vector.NewFunctionResultWrapper(proc.GetVector, proc.PutVector, typ, proc.Mp())
				if err = result.PreExtendAndReset(2); err != nil {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}
				if err = fn(ivecs, result, proc, 2); err != nil {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}
				zms[expr.AuxId] = index.VectorToZM(result.GetResultVector(), zms[expr.AuxId])
				result.GetResultVector().Free(proc.Mp())
			}
		}

	default:
		zms[expr.AuxId].Reset()
	}

	return zms[expr.AuxId]
}

func SortInFilter(vec *vector.Vector) {
	switch vec.GetType().Oid {
	case types.T_bool:
		col := vector.MustFixedCol[bool](vec)
		sort.Slice(col, func(i, j int) bool {
			return !col[i] && col[j]
		})

	case types.T_int8:
		col := vector.MustFixedCol[int8](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_int16:
		col := vector.MustFixedCol[int16](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_int32:
		col := vector.MustFixedCol[int32](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_int64:
		col := vector.MustFixedCol[int64](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_uint8:
		col := vector.MustFixedCol[uint8](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_uint16:
		col := vector.MustFixedCol[uint16](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_uint32:
		col := vector.MustFixedCol[uint32](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_uint64:
		col := vector.MustFixedCol[uint64](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_float32:
		col := vector.MustFixedCol[float32](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_float64:
		col := vector.MustFixedCol[float64](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_date:
		col := vector.MustFixedCol[types.Date](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_datetime:
		col := vector.MustFixedCol[types.Datetime](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_time:
		col := vector.MustFixedCol[types.Time](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_timestamp:
		col := vector.MustFixedCol[types.Timestamp](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_enum:
		col := vector.MustFixedCol[types.Enum](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i] < col[j]
		})

	case types.T_decimal64:
		col := vector.MustFixedCol[types.Decimal64](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i].Less(col[j])
		})

	case types.T_decimal128:
		col := vector.MustFixedCol[types.Decimal128](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i].Less(col[j])
		})

	case types.T_TS:
		col := vector.MustFixedCol[types.TS](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i].Less(col[j])
		})

	case types.T_uuid:
		col := vector.MustFixedCol[types.Uuid](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i].Lt(col[j])
		})

	case types.T_Rowid:
		col := vector.MustFixedCol[types.Rowid](vec)
		sort.Slice(col, func(i, j int) bool {
			return col[i].Less(col[j])
		})

	case types.T_char, types.T_varchar, types.T_json, types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		col, area := vector.MustVarlenaRawData(vec)
		sort.Slice(col, func(i, j int) bool {
			return bytes.Compare(col[i].GetByteSlice(area), col[j].GetByteSlice(area)) < 0
		})

	case types.T_array_float32:
		col, area := vector.MustVarlenaRawData(vec)
		sort.Slice(col, func(i, j int) bool {
			return moarray.Compare[float32](
				types.GetArray[float32](&col[i], area),
				types.GetArray[float32](&col[j], area),
			) < 0
		})
	case types.T_array_float64:
		col, area := vector.MustVarlenaRawData(vec)
		sort.Slice(col, func(i, j int) bool {
			return moarray.Compare[float64](
				types.GetArray[float64](&col[i], area),
				types.GetArray[float64](&col[j], area),
			) < 0
		})
	}
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
		fid, _ := function.DecodeOverloadID(e.F.Func.GetObj())
		if fid == function.AND {
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
			Func: &plan.ObjectRef{
				Obj:     function.AndFunctionEncodedID,
				ObjName: function.AndFunctionName,
			},
			Args: []*plan.Expr{left, right},
		},
	}
}

func isAllConst(exprs []*plan.Expr) bool {
	for _, expr := range exprs {
		if !isConst(expr) {
			return false
		}
	}
	return true
}

func isConst(expr *plan.Expr) bool {
	switch t := expr.Expr.(type) {
	case *plan.Expr_Col:
		return false
	case *plan.Expr_F:
		return isAllConst(t.F.Args)
	default:
		return true
	}
}
