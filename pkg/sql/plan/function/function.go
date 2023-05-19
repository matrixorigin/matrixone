// Copyright 2021 - 2022 Matrix Origin
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

package function

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var allSupportedFunctions [1000]FuncNew

// register all supported functions.
func initAllSupportedFunctions() {
	for _, fn := range supportedOperators {
		allSupportedFunctions[fn.functionId] = fn
	}
	for _, fn := range supportedStringBuiltIns {
		allSupportedFunctions[fn.functionId] = fn
	}
	for _, fn := range supportedDateAndTimeBuiltIns {
		allSupportedFunctions[fn.functionId] = fn
	}
	for _, fn := range supportedMathBuiltIns {
		allSupportedFunctions[fn.functionId] = fn
	}
	for _, fn := range supportedControlBuiltIns {
		allSupportedFunctions[fn.functionId] = fn
	}
	for _, fn := range supportedOthersBuiltIns {
		allSupportedFunctions[fn.functionId] = fn
	}
	for _, fn := range supportedAggregateFunctions {
		allSupportedFunctions[fn.functionId] = fn
	}
}

func GetFunctionIsAggregateByName(name string) bool {
	fid, exists := getFunctionIdByNameWithoutErr(name)
	if !exists {
		return false
	}
	f := allSupportedFunctions[fid]
	return f.isAggregate()
}

func GetFunctionIsWinFunByName(name string) bool {
	fid, exists := getFunctionIdByNameWithoutErr(name)
	if !exists {
		return false
	}
	f := allSupportedFunctions[fid]
	return len(f.Overloads) > 0 && f.testFlag(plan.Function_WIN)
}

func GetFunctionIsMonotonicById(ctx context.Context, overloadID int64) (bool, error) {
	fid, oIndex := DecodeOverloadID(overloadID)
	if int(fid) >= len(allSupportedFunctions) || int(fid) != allSupportedFunctions[fid].functionId {
		return false, moerr.NewInvalidInput(ctx, "function overload id not found")
	}
	f := allSupportedFunctions[fid]
	if f.Overloads[oIndex].volatile {
		return false, nil
	}
	return f.testFlag(plan.Function_MONOTONIC), nil
}

func GetFunctionById(ctx context.Context, overloadID int64) (f overload, err error) {
	fid, oIndex := DecodeOverloadID(overloadID)
	if int(fid) >= len(allSupportedFunctions) || int(fid) != allSupportedFunctions[fid].functionId {
		return overload{}, moerr.NewInvalidInput(ctx, "function overload id not found")
	}
	return allSupportedFunctions[fid].Overloads[oIndex], nil
}

func GetLayoutById(ctx context.Context, overloadID int64) (FuncExplainLayout, error) {
	fid, _ := DecodeOverloadID(overloadID)
	if int(fid) >= len(allSupportedFunctions) || int(fid) != allSupportedFunctions[fid].functionId {
		return 0, moerr.NewInvalidInput(ctx, "function overload id not found")
	}
	return allSupportedFunctions[fid].layout, nil
}

func GetFunctionByIdWithoutError(overloadID int64) (f overload, exists bool) {
	fid, oIndex := DecodeOverloadID(overloadID)
	if int(fid) >= len(allSupportedFunctions) || int(fid) != allSupportedFunctions[fid].functionId {
		return overload{}, false
	}
	return allSupportedFunctions[fid].Overloads[oIndex], true
}

func GetFunctionByName(ctx context.Context, name string, args []types.Type) (r FuncGetResult, err error) {
	r.fid, err = getFunctionIdByName(ctx, name)
	if err != nil {
		return r, err
	}
	f := allSupportedFunctions[r.fid]
	if len(f.Overloads) == 0 || f.checkFn == nil {
		return r, moerr.NewNYI(ctx, "should implement the function %s", name)
	}

	check := f.checkFn(f.Overloads, args)
	switch check.status {
	case succeedMatched:
		r.overloadId = int32(check.idx)
		r.retType = f.Overloads[r.overloadId].retType(args)
		r.cannotRunInParallel = f.Overloads[r.overloadId].cannotParallel

	case succeedWithCast:
		r.overloadId = int32(check.idx)
		r.needCast = true
		r.targetTypes = check.finalType
		r.retType = f.Overloads[r.overloadId].retType(r.targetTypes)
		r.cannotRunInParallel = f.Overloads[r.overloadId].cannotParallel

	case failedFunctionParametersWrong:
		if f.isFunction() {
			err = moerr.NewInvalidArg(ctx, fmt.Sprintf("function %s", name), args)
		} else {
			err = moerr.NewInvalidArg(ctx, fmt.Sprintf("operator %s", name), args)
		}

	case failedAggParametersWrong:
		err = moerr.NewInvalidArg(ctx, fmt.Sprintf("aggregate function %s", name), args)

	case failedTooManyFunctionMatched:
		err = moerr.NewInvalidArg(ctx, fmt.Sprintf("too many overloads matched %s", name), args)
	}

	return r, err
}

func RunFunctionDirectly(proc *process.Process, overloadID int64, inputs []*vector.Vector, length int) (*vector.Vector, error) {
	f, err := GetFunctionById(proc.Ctx, overloadID)
	if err != nil {
		return nil, err
	}

	inputTypes := make([]types.Type, len(inputs))
	for i := range inputTypes {
		inputTypes[i] = *inputs[i].GetType()
	}
	result := vector.NewFunctionResultWrapper(f.retType(inputTypes), proc.Mp())

	fold := true
	if !f.CannotFold() && !f.IsRealTimeRelated() {
		for _, param := range inputs {
			if !param.IsConst() {
				fold = false
			}
		}
		if fold {
			length = 1
		}
	}

	if err = result.PreExtendAndReset(length); err != nil {
		return nil, err
	}
	exec := f.GetExecuteMethod()
	if err = exec(inputs, result, proc, length); err != nil {
		return nil, err
	}

	vec := result.GetResultVector()
	if fold {
		return vec.ToConst(0, 1, proc.Mp()), nil
	}
	return vec, nil
}

// DeduceNotNullable helps optimization sometimes.
// deduce notNullable for function
// for example, create table t1(c1 int not null, c2 int, c3 int not null ,c4 int);
// sql select c1+1, abs(c2), cast(c3 as varchar(10)) from t1 where c1=c3;
// we can deduce that c1+1, cast c3 and c1=c3 is notNullable, abs(c2) is nullable.
func DeduceNotNullable(overloadID int64, args []*plan.Expr) bool {
	fid, _ := DecodeOverloadID(overloadID)
	if allSupportedFunctions[fid].testFlag(plan.Function_PRODUCE_NO_NULL) {
		return true
	}

	for _, arg := range args {
		if !arg.Typ.NotNullable {
			return false
		}
	}
	return true
}

type FuncGetResult struct {
	fid        int32
	overloadId int32
	retType    types.Type

	cannotRunInParallel bool

	needCast    bool
	targetTypes []types.Type
}

func (fr *FuncGetResult) GetEncodedOverloadID() (overloadID int64) {
	return encodeOverloadID(fr.fid, fr.overloadId)
}

func (fr *FuncGetResult) ShouldDoImplicitTypeCast() (typs []types.Type, should bool) {
	return fr.targetTypes, fr.needCast
}

func (fr *FuncGetResult) GetReturnType() types.Type {
	return fr.retType
}

func (fr *FuncGetResult) CannotRunInParallel() bool {
	return fr.cannotRunInParallel
}

func encodeOverloadID(fid, overloadId int32) (overloadID int64) {
	overloadID = int64(fid)
	overloadID = overloadID << 32
	overloadID |= int64(overloadId)
	return
}

func DecodeOverloadID(overloadID int64) (fid int32, oIndex int32) {
	base := overloadID
	oIndex = int32(overloadID)
	fid = int32(base >> 32)
	return fid, oIndex
}

func getFunctionIdByName(ctx context.Context, name string) (int32, error) {
	if fid, ok := functionIdRegister[name]; ok {
		return fid, nil
	}
	return -1, moerr.NewNotSupported(ctx, "function or operator '%s'", name)
}

func getFunctionIdByNameWithoutErr(name string) (int32, bool) {
	fid, exist := functionIdRegister[name]
	return fid, exist
}

// FuncNew stores all information about a function.
// including the unique id that marks the function, the class which the function belongs to,
// and all overloads of the function.
type FuncNew struct {
	// unique id of function.
	functionId int

	// function type.
	class plan.Function_FuncFlag

	// All overloads of the function.
	Overloads []overload

	// checkFn was used to check whether the input type can match the requirement of the function.
	// if matched, return the corresponding id of overload. If type conversion was required,
	// the required type should be returned at the same time.
	checkFn func(overloads []overload, inputs []types.Type) checkResult

	// layout was used for `explain SQL`.
	layout FuncExplainLayout
}

type executeLogicOfOverload func(parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process, length int) error

// an overload of a function.
// stores all information about execution logic.
type overload struct {
	overloadId int

	// args records some type information about this overload.
	// in most case, it records, in order, which parameter types the overload required.
	// For example,
	//		args can be `{int64, int64}` of one overload of the `pow` function.
	//		this means the overload can accept {int64, int64} as its input.
	// but it was not necessarily the type directly required by the overload.
	// what it is depends on the logic of function's checkFn.
	args []types.T

	// return type of the overload.
	// parameters are the params actually received when the overload is executed.
	retType func(parameters []types.Type) types.Type

	// the execution logic.
	newOp func() executeLogicOfOverload

	// in fact, the function framework does not directly run aggregate functions and window functions.
	// we use two flags to mark whether function is one of them, and if so, we will record the special id of it.
	// This id will be passed to the real execution framework of Agg function and window function.
	//
	// XXX define a special structure to records this information may suitable ?
	isAgg     bool
	isWin     bool
	specialId int

	// if true, overload was unable to run in parallel.
	// For example,
	//		rand(1) cannot run in parallel because it should use the same rand seed.
	//
	// TODO: there is not a good place to use that in plan now. the attribute is not effective.
	cannotParallel bool

	// if true, overload cannot be folded
	volatile bool
	// if realTimeRelated, overload cannot be folded when `Prepare`.
	realTimeRelated bool
}

func (ov *overload) CannotFold() bool {
	return ov.volatile
}

func (ov *overload) IsRealTimeRelated() bool {
	return ov.realTimeRelated
}

func (ov *overload) IsAgg() bool {
	return ov.isAgg
}

func (ov *overload) CannotExecuteInParallel() bool {
	return ov.cannotParallel
}

func (ov *overload) GetSpecialId() int {
	return ov.specialId
}

func (ov *overload) GetExecuteMethod() executeLogicOfOverload {
	f := ov.newOp
	return f()
}

func (ov *overload) IsWin() bool {
	return ov.isWin
}

func (fn *FuncNew) isFunction() bool {
	return fn.layout == STANDARD_FUNCTION || fn.layout >= NOPARAMETER_FUNCTION
}

func (fn *FuncNew) isAggregate() bool {
	return fn.testFlag(plan.Function_AGG)
}

func (fn *FuncNew) testFlag(funcFlag plan.Function_FuncFlag) bool {
	return fn.class&funcFlag != 0
}

type overloadCheckSituation int

const (
	succeedMatched                overloadCheckSituation = 0
	succeedWithCast               overloadCheckSituation = -1
	failedFunctionParametersWrong overloadCheckSituation = -2
	failedAggParametersWrong      overloadCheckSituation = -3
	failedTooManyFunctionMatched  overloadCheckSituation = -4
)

type checkResult struct {
	status overloadCheckSituation

	// if matched
	idx       int
	finalType []types.Type
}

func newCheckResultWithSuccess(overloadId int) checkResult {
	return checkResult{status: succeedMatched, idx: overloadId}
}

func newCheckResultWithFailure(status overloadCheckSituation) checkResult {
	return checkResult{status: status}
}

func newCheckResultWithCast(overloadId int, castType []types.Type) checkResult {
	return checkResult{
		status:    succeedWithCast,
		idx:       overloadId,
		finalType: castType,
	}
}
