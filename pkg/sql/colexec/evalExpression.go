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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
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

	ifResultMemoryReuse() bool
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

		executor := &FunctionExpressionExecutor{}
		typ := types.New(types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale)
		if err = executor.Init(proc.Mp(), len(t.F.Args), typ, overload.GetExecuteMethod()); err != nil {
			return nil, err
		}

		for i := range executor.parameterExecutor {
			subExecutor, paramErr := NewExpressionExecutor(proc, t.F.Args[i])
			if paramErr != nil {
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

			if err = executor.resultVector.PreExtendAndReset(1); err != nil {
				return nil, err
			}

			err = executor.evalFn(executor.parameterResults, executor.resultVector, proc, 1)
			if err == nil {
				result := executor.resultVector.GetResultVector()
				fixed := &FixedVectorExpressionExecutor{
					m: proc.Mp(),
				}

				fixed.resultVector = result.ToConst(0, 1, proc.Mp())

				executor.Free()
				return fixed, err
			}
			executor.Free()
			return nil, err
		}

		return executor, nil
	}

	return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("unsupported expression executor for %v now", expr))
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
	if e, ok := executor.(*FunctionExpressionExecutor); ok {
		e.resultVector = nil
		return vec, nil
	} else {
		nv, er := vec.Dup(proc.Mp())
		if er != nil {
			return nil, er
		}
		return nv, nil
	}
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
	expr.resultVector = vector.NewFunctionResultWrapper(retType, m)
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

	if err = expr.resultVector.PreExtendAndReset(batches[0].Length()); err != nil {
		return nil, err
	}

	if err = expr.evalFn(
		expr.parameterResults, expr.resultVector, proc, batches[0].Length()); err != nil {
		return nil, err
	}
	return expr.resultVector.GetResultVector(), nil
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

func (expr *FunctionExpressionExecutor) ifResultMemoryReuse() bool {
	return true
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
		vec.SetType(expr.typ)
	}
	return vec, nil
}

func (expr *ColumnExpressionExecutor) Free() {
	// Nothing should do.
}

func (expr *ColumnExpressionExecutor) ifResultMemoryReuse() bool {
	return false
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
	expr.resultVector = nil
}

func (expr *FixedVectorExpressionExecutor) ifResultMemoryReuse() bool {
	return true
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

// FixProjectionResult set result vector for rbat.
// sbat is the source batch.
func FixProjectionResult(proc *process.Process, executors []ExpressionExecutor,
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
			if oldVec.GetType().Oid == types.T_any {
				newVec = vector.NewConstNull(types.T_any.ToType(), oldVec.Length(), proc.Mp())
			} else {
				if executors[i].ifResultMemoryReuse() {
					if e, ok := executors[i].(*FunctionExpressionExecutor); ok {
						// if projection, we can get the result directly
						newVec = e.resultVector.GetResultVector()
						e.resultVector.SetResultVector(nil)

					} else {
						newVec, err = oldVec.Dup(proc.Mp())
						if err != nil {
							for j := range finalVectors {
								finalVectors[j].Free(proc.Mp())
							}
							return 0, err
						}
						dupSize += newVec.Size()
					}
				} else {
					if sbat.GetCnt() == 1 {
						newVec = oldVec
						sbat.ReplaceVector(oldVec, nil)
					} else {
						newVec = proc.GetVector(*oldVec.GetType())
						err = vector.GetUnionAllFunction(*oldVec.GetType(), proc.Mp())(newVec, oldVec)
						if err != nil {
							for j := range finalVectors {
								finalVectors[j].Free(proc.Mp())
							}
							return 0, err
						}
						dupSize += newVec.Size()
					}
				}
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

// SafeGetResult if executor is function executor, we can get the nulls directly without copied.
// because next reuse, the nsp will reset.
func SafeGetResult(proc *process.Process, vec *vector.Vector, executor ExpressionExecutor) (*vector.Vector, error) {
	if executor.ifResultMemoryReuse() {
		if e, ok := executor.(*FunctionExpressionExecutor); ok {
			nv := e.resultVector.GetResultVector()
			e.resultVector.SetResultVector(nil)
			return nv, nil
		}

		nv, err := vec.Dup(proc.Mp())
		if err != nil {
			return nil, err
		}
		return nv, nil
	} else {
		if vec.IsConst() {
			return vec.Dup(proc.Mp())
		}
		nv := proc.GetVector(*vec.GetType())
		return nv, vector.GetUnionAllFunction(*vec.GetType(), proc.Mp())(nv, vec)
	}
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

var noColumnBatchForZoneMap = []*batch.Batch{batch.NewWithSize(0)}

func EvaluateFilterByZoneMap(proc *process.Process, expr *plan.Expr, meta objectio.ColumnMetaFetcher, columnMap map[int]int) (selected bool) {
	if expr == nil {
		selected = true
		return
	}

	if len(columnMap) == 0 {
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

	zm := evaluateFilterByZoneMap(proc, expr, meta, columnMap)
	if !zm.IsInited() || zm.GetType() != types.T_bool {
		selected = true
	} else {
		selected = types.DecodeBool(zm.GetMaxBuf())
	}
	return
}

func evaluateFilterByZoneMap(
	proc *process.Process,
	expr *plan.Expr,
	meta objectio.ColumnMetaFetcher,
	columnMap map[int]int) (v objectio.ZoneMap) {
	var err error

	switch t := expr.Expr.(type) {
	case *plan.Expr_C:
		if v, err = getConstZM(proc.Ctx, expr, proc); err != nil {
			v = objectio.NewZM(types.T_bool, 0)
		}
		return
	case *plan.Expr_Col:
		v = meta.MustGetColumn(uint16(columnMap[int(t.Col.ColPos)])).ZoneMap()
		return
	case *plan.Expr_F:
		id := t.F.GetFunc().GetObj()
		if overload, errGetFunc := function2.GetFunctionById(proc.Ctx, id); errGetFunc != nil {
			v = objectio.NewZM(types.T_bool, 0)
			return
		} else {
			params := make([]objectio.ZoneMap, len(t.F.Args))
			for i := range params {
				params[i] = evaluateFilterByZoneMap(proc, t.F.Args[i], meta, columnMap)
				if !params[i].IsInited() {
					return params[i]
				}
			}
			var res, ok bool
			switch t.F.Func.ObjName {
			case ">":
				if res, ok = params[0].AnyGT(params[1]); !ok {
					v = objectio.NewZM(types.T_bool, 0)
				} else {
					v = index.BoolToZM(res)
				}
				return
			case "<":
				if res, ok = params[0].AnyLT(params[1]); !ok {
					v = objectio.NewZM(types.T_bool, 0)
				} else {
					v = index.BoolToZM(res)
				}
				return
			case ">=":
				if res, ok = params[0].AnyGE(params[1]); !ok {
					v = objectio.NewZM(types.T_bool, 0)
				} else {
					v = index.BoolToZM(res)
				}
				return
			case "<=":
				if res, ok = params[0].AnyLE(params[1]); !ok {
					v = objectio.NewZM(types.T_bool, 0)
				} else {
					v = index.BoolToZM(res)
				}
				return
			case "=":
				if res, ok = params[0].Intersect(params[1]); !ok {
					v = objectio.NewZM(types.T_bool, 0)
				} else {
					v = index.BoolToZM(res)
				}
				return
			case "and":
				if res, ok = params[0].And(params[1]); !ok {
					v = objectio.NewZM(types.T_bool, 0)
				} else {
					v = index.BoolToZM(res)
				}
				return
			case "or":
				if res, ok = params[0].Or(params[1]); !ok {
					v = objectio.NewZM(types.T_bool, 0)
				} else {
					v = index.BoolToZM(res)
				}
				return
			case "+":
				if v, ok = index.ZMPlus(params[0], params[1]); !ok {
					v = objectio.NewZM(types.T_bool, 0)
				}
				return
			case "-":
				if v, ok = index.ZMMinus(params[0], params[1]); !ok {
					v = objectio.NewZM(types.T_bool, 0)
				}
				return
			case "*":
				if v, ok = index.ZMMulti(params[0], params[1]); !ok {
					v = objectio.NewZM(types.T_bool, 0)
				}
				return
			}

			vecParams := make([]*vector.Vector, len(params))
			defer func() {
				for i := range vecParams {
					if vecParams[i] != nil {
						vecParams[i].Free(proc.Mp())
						vecParams[i] = nil
					}
				}
			}()
			for i := range vecParams {
				if vecParams[i], err = index.ZMToVector(params[i], proc.Mp()); err != nil {
					v = objectio.NewZM(types.T_bool, 0)
					return
				}
			}

			fn := overload.GetExecuteMethod()
			typ := types.New(types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale)
			result := vector.NewFunctionResultWrapper(typ, proc.Mp())
			err = result.PreExtendAndReset(2)
			if err != nil {
				v = objectio.NewZM(types.T_bool, 0)
			}
			if err = fn(vecParams, result, proc, 2); err != nil {
				v = objectio.NewZM(types.T_bool, 0)
				return
			}
			defer result.GetResultVector().Free(proc.Mp())
			v = index.VectorToZM(result.GetResultVector())
		}
	}
	return nil
}
