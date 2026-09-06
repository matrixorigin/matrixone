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
	for _, fn := range supportedArrayOperations {
		allSupportedFunctions[fn.functionId] = fn
	}
	for _, fn := range supportedControlBuiltIns {
		allSupportedFunctions[fn.functionId] = fn
	}
	for _, fn := range supportedOthersBuiltIns {
		allSupportedFunctions[fn.functionId] = fn
	}

	for _, fn := range supportedWindowInNewFramework {
		allSupportedFunctions[fn.functionId] = fn
	}
	for _, fn := range supportedAggInNewFramework {
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
	return f.isWindow()
}

func GetFunctionIsWinOrderFunByName(name string) bool {
	fid, exists := getFunctionIdByNameWithoutErr(name)
	if !exists {
		return false
	}
	f := allSupportedFunctions[fid]
	return f.isWindowOrder()
}

func GetFunctionIsWinValueFunByName(name string) bool {
	fid, exists := getFunctionIdByNameWithoutErr(name)
	if !exists {
		return false
	}
	f := allSupportedFunctions[fid]
	return f.isWindowValue()
}

// GetFunctionIgnoresWindowFrameByName reports whether a window function
// operates on partition-relative row positions instead of the current frame.
func GetFunctionIgnoresWindowFrameByName(name string) bool {
	fid, exists := getFunctionIdByNameWithoutErr(name)
	return exists && (fid == LAG || fid == LEAD)
}

func GetFunctionIsVolatileOrRealTimeRelatedByName(name string) bool {
	fid, exists := getFunctionIdByNameWithoutErr(name)
	if !exists {
		return false
	}
	for _, ov := range allSupportedFunctions[fid].Overloads {
		if ov.CannotFold() || ov.IsRealTimeRelated() {
			return true
		}
	}
	return false
}

func GetFunctionIsWinOrderFunById(overloadID int64) bool {
	fid, _ := DecodeOverloadID(overloadID)
	return allSupportedFunctions[fid].isWindowOrder()
}

func GetFunctionIsZonemappableById(ctx context.Context, overloadID int64) (bool, error) {
	fid, oIndex := DecodeOverloadID(overloadID)
	if int(fid) >= len(allSupportedFunctions) || int(fid) != allSupportedFunctions[fid].functionId {
		return false, moerr.NewInvalidInput(ctx, "function overload id not found")
	}
	f := allSupportedFunctions[fid]
	if f.Overloads[oIndex].volatile {
		return false, nil
	}
	return f.testFlag(plan.Function_ZONEMAPPABLE), nil
}

func GetFunctionById(ctx context.Context, overloadID int64) (f overload, err error) {
	fid, oIndex := DecodeOverloadID(overloadID)
	if fid < 0 || int(fid) >= len(allSupportedFunctions) || int(fid) != allSupportedFunctions[fid].functionId {
		return overload{}, moerr.NewInvalidInput(ctx, "function overload id not found")
	}
	return allSupportedFunctions[fid].Overloads[oIndex], nil
}

func GetLayoutById(ctx context.Context, overloadID int64) (FuncExplainLayout, error) {
	fid, _ := DecodeOverloadID(overloadID)
	if fid < 0 || int(fid) >= len(allSupportedFunctions) || int(fid) != allSupportedFunctions[fid].functionId {
		return 0, moerr.NewInvalidInput(ctx, "function overload id not found")
	}
	return allSupportedFunctions[fid].layout, nil
}

func GetFunctionByIdWithoutError(overloadID int64) (f overload, exists bool) {
	fid, oIndex := DecodeOverloadID(overloadID)
	if fid < 0 || int(fid) >= len(allSupportedFunctions) || int(fid) != allSupportedFunctions[fid].functionId {
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
		return r, moerr.NewNYIf(ctx, "should implement the function %s", name)
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
		if check.invalidJSONArgumentIndex != 0 {
			err = moerr.NewInvalidTypeForJSON(ctx, check.invalidJSONArgumentIndex, name)
		} else if f.isFunction() {
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

// GetFunctionByNameWithoutError tries to resolve a function overload without
// constructing an error for an expected mismatch. It is intended for
// speculative planner checks where unsupported candidate types are normal and
// the caller only needs the successful resolution metadata.
func GetFunctionByNameWithoutError(name string, args []types.Type) (r FuncGetResult, ok bool) {
	r.fid, ok = getFunctionIdByNameWithoutErr(name)
	if !ok || r.fid < 0 || int(r.fid) >= len(allSupportedFunctions) {
		return FuncGetResult{}, false
	}

	f := allSupportedFunctions[r.fid]
	if len(f.Overloads) == 0 || f.checkFn == nil {
		return FuncGetResult{}, false
	}

	check := f.checkFn(f.Overloads, args)
	switch check.status {
	case succeedMatched:
		r.overloadId = int32(check.idx)
		r.retType = f.Overloads[r.overloadId].retType(args)
		r.cannotRunInParallel = f.Overloads[r.overloadId].cannotParallel
		return r, true

	case succeedWithCast:
		r.overloadId = int32(check.idx)
		r.needCast = true
		r.targetTypes = check.finalType
		r.retType = f.Overloads[r.overloadId].retType(r.targetTypes)
		r.cannotRunInParallel = f.Overloads[r.overloadId].cannotParallel
		return r, true

	default:
		return FuncGetResult{}, false
	}
}

// GetFunctionByNameWithOverload validates the arguments using the function's
// normal type checker, then selects a specific overload. It is intended for
// planner-only variants that must keep the same SQL function name and layout.
func GetFunctionByNameWithOverload(
	ctx context.Context, name string, args []types.Type, overloadID int32,
) (r FuncGetResult, err error) {
	r, err = GetFunctionByName(ctx, name, args)
	if err != nil {
		return r, err
	}
	f := allSupportedFunctions[r.fid]
	if overloadID < 0 || int(overloadID) >= len(f.Overloads) {
		return FuncGetResult{}, moerr.NewInvalidInputf(ctx, "function overload %s.%d not found", name, overloadID)
	}
	r.overloadId = overloadID
	r.retType = f.Overloads[overloadID].retType(args)
	r.cannotRunInParallel = f.Overloads[overloadID].cannotParallel
	return r, nil
}

// RunFunctionDirectly runs a function directly without any protections.
// It is dangerous and should be used only when you are sure that the overloadID is correct and the inputs are valid.
func RunFunctionDirectly(proc *process.Process, overloadID int64, inputs []*vector.Vector, length int) (*vector.Vector, error) {
	f, err := GetFunctionById(proc.Ctx, overloadID)
	if err != nil {
		return nil, err
	}

	mp := proc.Mp()
	inputTypes := make([]types.Type, len(inputs))
	for i := range inputTypes {
		inputTypes[i] = *inputs[i].GetType()
	}

	result := vector.NewFunctionResultWrapper(f.retType(inputTypes), mp)

	fold := !f.CannotFold() && !f.IsRealTimeRelated()
	evaluateLength := length
	if fold {
		for _, param := range inputs {
			if !param.IsConst() {
				fold = false
			}
		}
		if fold {
			evaluateLength = 1
		}
	}

	if err = result.PreExtendAndReset(evaluateLength); err != nil {
		result.Free()
		return nil, err
	}
	exec, _, execFree, _ := f.GetExecuteMethod()
	if err = exec(inputs, result, proc, evaluateLength, nil); err != nil {
		result.Free()
		if execFree != nil {
			// NOTE: execFree is only applicable for serial and serial_full.
			// if execFree is not nil, then make sure to call it after exec() is done.
			_ = execFree()
		}
		return nil, err
	}
	if execFree != nil {
		// NOTE: execFree is only applicable for serial and serial_full.
		// if execFree is not nil, then make sure to call it after exec() is done.
		_ = execFree()
	}

	vec := result.GetResultVector()
	if fold {
		vec.ToConst()
		vec.SetLength(length)
	}
	return vec, nil
}

func GetAggFunctionNameByID(overloadID int64) string {
	f, exist := GetFunctionByIdWithoutError(overloadID)
	if !exist {
		return "unknown function"
	}
	return f.aggName
}

// DeduceNotNullable reports whether a function result is guaranteed to be
// non-NULL. STRICT functions normally preserve an all-non-NULL argument
// guarantee, except for functions that can synthesize NULL from valid values.
func DeduceNotNullable(overloadID int64, args []*plan.Expr) bool {
	fid, _ := DecodeOverloadID(overloadID)
	switch fid {
	case CASE:
		if caseHasTemporalPromotion(args) {
			return false
		}
		for _, arg := range args {
			if !arg.Typ.NotNullable {
				return false
			}
		}
		return true
	case TIMESTAMP:
		if len(args) == 2 {
			return false
		}
	case COALESCE:
		for _, arg := range args {
			if arg.Typ.NotNullable {
				return true
			}
		}
		return false
	case GREATEST, LEAST:
		return false
	case EQUAL, NOT_EQUAL:
		// Direct JSON/BOOL equality preserves the JSON scalar category. A
		// physically non-NULL JSON value can still contain JSON null, which the
		// comparison maps to SQL UNKNOWN. Do not infer a non-NULL result merely
		// from the two vector-level argument declarations.
		if len(args) == 2 && isJSONBooleanComparison(
			types.T(args[0].Typ.Id).ToType(), types.T(args[1].Typ.Id).ToType()) {
			return false
		}
	// Value window functions can synthesize NULLs even when every input is
	// NOT NULL. LAG/LEAD do so outside the partition unless an explicit,
	// non-NULL default is present. FIRST_VALUE/LAST_VALUE can observe an empty
	// frame, and NTH_VALUE can also miss the requested row. The frame is not
	// available here, so keep those contracts conservative.
	case FIRST_VALUE, LAST_VALUE, NTH_VALUE:
		return false
	case LAG, LEAD:
		if len(args) != 3 {
			return false
		}
		for _, arg := range args {
			if !arg.Typ.NotNullable {
				return false
			}
		}
		return true
	// These STRICT functions can synthesize NULL from non-NULL arguments.
	// The UUID extractors do so for non-RFC-4122 variants, and
	// uuid_extract_timestamp also for versions without a time source (e.g. v4).
	case DIV, INTEGER_DIV, MOD,
		JSON_EXTRACT, JSON_EXTRACT_STRING, JSON_EXTRACT_FLOAT64,
		REGEXP_SUBSTR,
		INET6_ATON, ELT, UNHEX, MAKEDATE,
		UUID_EXTRACT_VERSION, UUID_EXTRACT_TIMESTAMP,
		TO_INTERVAL:
		return false
	}
	if ProducesNoNull(overloadID) {
		return true
	}
	for _, arg := range args {
		if !arg.Typ.NotNullable {
			return false
		}
	}
	return true
}

func caseHasTemporalPromotion(args []*plan.Expr) bool {
	for i := 1; i < len(args); i += 2 {
		if isTemporalPromotion(args[i]) {
			return true
		}
	}
	// CASE arguments are condition/value pairs followed by ELSE. The ELSE
	// expression is at the final even index and needs the same check.
	if len(args)%2 == 1 && isTemporalPromotion(args[len(args)-1]) {
		return true
	}
	return false
}

func isTemporalPromotion(arg *plan.Expr) bool {
	fn := arg.GetF()
	if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) == 0 {
		return false
	}
	source := types.T(fn.Args[0].Typ.Id)
	target := types.T(arg.Typ.Id)
	return source.IsDateRelate() && target.IsDateRelate() && source != target
}

// ProducesNoNull reports whether a function's contract guarantees a non-NULL
// result independently of its argument values. This is stronger than
// DeduceNotNullable: STRICT functions such as json_extract can still return
// SQL NULL for non-NULL inputs when a requested value is absent.
func ProducesNoNull(overloadID int64) bool {
	fid, _ := DecodeOverloadID(overloadID)
	return fid >= 0 &&
		int(fid) < len(allSupportedFunctions) &&
		int(fid) == allSupportedFunctions[fid].functionId &&
		allSupportedFunctions[fid].testFlag(plan.Function_PRODUCE_NO_NULL)
}

// HasExecutableCTASTypeDefault reports whether the SQL type default is a valid
// value for a materialized function result when an INSERT omits that column.
// This contract is deliberately independent of ProducesNoNull: domain types
// such as HLL sketches never return NULL, but their zero-value byte string is
// not a valid encoded sketch.
func HasExecutableCTASTypeDefault(overloadID int64) bool {
	fid, _ := DecodeOverloadID(overloadID)
	return fid >= 0 &&
		int(fid) < len(allSupportedFunctions) &&
		int(fid) == allSupportedFunctions[fid].functionId &&
		allSupportedFunctions[fid].hasExecutableCTASTypeDefault
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

func IsUserLevelLockFunctionID(fid int32) bool {
	switch fid {
	case GET_LOCK, RELEASE_LOCK, IS_FREE_LOCK, IS_USED_LOCK, RELEASE_ALL_LOCKS:
		return true
	default:
		return false
	}
}

func getFunctionIdByName(ctx context.Context, name string) (int32, error) {
	if fid, ok := functionIdRegister[name]; ok {
		return fid, nil
	}
	return -1, moerr.NewNotSupportedf(ctx, "function or operator '%s'", name)
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

	// Whether the SQL type default is a valid executable default after CTAS
	// materializes this function's result as a table column.
	hasExecutableCTASTypeDefault bool

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
	proc *process.Process, length int,
	selectList *FunctionSelectList) error

// executeFreeOfOverload is used to free the resources allocated by the execution logic.
// It is mainly used in SERIAL and SERIAL_FULL.
// NOTE: right now, we are not throwing an error when the free logic failed. However, it is still included
// in case we need it in the future.
type executeFreeOfOverload func() error

// executeResetOfOverload is used to reset the resources allocated by the execution logic.
// It is mainly used in SERIAL and SERIAL_FULL.
// NOTE: right now, we are not throwing an error when the reset logic failed. However, it is still included
// in case we need it in the future.
type executeResetOfOverload func() error

// executeRetainedBytesOfOverload reports non-vector backing allocations kept
// alive by a stateful function operator. It is optional: ordinary functions
// whose complete retained state is represented by executor vectors omit it.
type executeRetainedBytesOfOverload func() uint64

// an overload of a function.
// stores all information about execution logic.
type overload struct {
	overloadId int

	// args records some type information about this overload.
	// in most case, it records, in order, which parameter types the overload required.
	// For example,
	//		args can be `{int64, int64}` of one overload for the `pow` function.
	//		this means the overload can accept {int64, int64} as its input.
	// but it was not necessarily the type directly required by the overload.
	// what it is depends on the logic of function's checkFn.
	args []types.T

	// return type of the overload.
	// parameters are the params actually received when the overload is executed.
	retType func(parameters []types.Type) types.Type

	// the execution logic.
	newOp func() executeLogicOfOverload

	// the execution logic and free logic.
	// NOTE: use either newOp or newOpWithFree.
	newOpWithFree func() (
		executeLogicOfOverload,
		executeResetOfOverload,
		executeFreeOfOverload,
		executeRetainedBytesOfOverload,
	)

	// in fact, the function framework does not directly run aggregate functions and window functions.
	// we use two flags to mark whether function is one of them.
	isAgg bool
	isWin bool

	// aggName is used in aggregate-related error messages.
	aggName string

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

func (ov *overload) GetExecuteMethod() (
	executeLogicOfOverload,
	executeResetOfOverload,
	executeFreeOfOverload,
	executeRetainedBytesOfOverload,
) {
	if ov.newOpWithFree != nil {
		return ov.newOpWithFree()
	}

	fn := ov.newOp()
	return fn, nil, nil, nil
}

func (ov *overload) GetReturnTypeMethod() func(parameters []types.Type) types.Type {
	return ov.retType
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

func (fn *FuncNew) isWindow() bool {
	return fn.testFlag(plan.Function_WIN_ORDER) || fn.testFlag(plan.Function_WIN_VALUE) || fn.testFlag(plan.Function_AGG)
}

func (fn *FuncNew) isWindowOrder() bool {
	return fn.testFlag(plan.Function_WIN_ORDER)
}

func (fn *FuncNew) isWindowValue() bool {
	return fn.testFlag(plan.Function_WIN_VALUE)
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
	idx                      int
	finalType                []types.Type
	invalidJSONArgumentIndex int
}

func newCheckResultWithSuccess(overloadId int) checkResult {
	return checkResult{status: succeedMatched, idx: overloadId}
}

func newCheckResultWithFailure(status overloadCheckSituation) checkResult {
	return checkResult{status: status}
}

func newCheckResultWithInvalidJSONArgument(argumentIndex int) checkResult {
	return checkResult{
		status:                   failedFunctionParametersWrong,
		invalidJSONArgumentIndex: argumentIndex,
	}
}

func newCheckResultWithCast(overloadId int, castType []types.Type) checkResult {
	return checkResult{
		status:    succeedWithCast,
		idx:       overloadId,
		finalType: castType,
	}
}

type FunctionSelectList struct {
	AnyNull    bool
	AllNull    bool
	SelectList []bool
}

func (selectList *FunctionSelectList) ShouldEvalAllRow() bool {
	if selectList == nil {
		return true
	}
	return !selectList.AnyNull
}

func (selectList *FunctionSelectList) IgnoreAllRow() bool {
	if selectList == nil {
		return false
	}
	return selectList.AllNull
}

func (selectList *FunctionSelectList) Contains(row uint64) bool {
	if selectList == nil || len(selectList.SelectList) <= int(row) {
		return false
	}
	return !selectList.SelectList[row]
}

var EncodeOverloadID = encodeOverloadID
var GetFunctionIdByName = getFunctionIdByName
