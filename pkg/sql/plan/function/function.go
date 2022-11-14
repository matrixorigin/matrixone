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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	// ScalarNull means of scalar NULL
	// which can meet each required type.
	// e.g.
	// if we input a SQL `select built_in_function(columnA, NULL);`, and columnA is int64 column.
	// it will use [types.T_int64, ScalarNull] to match function when we were building the query plan.
	ScalarNull = types.T_any
)

var (
	// an empty type structure just for return when we couldn't meet any function.
	emptyType = types.Type{}

	// AndFunctionEncodedID is the encoded overload id of And(bool, bool)
	// used to make an AndExpr
	AndFunctionEncodedID = EncodeOverloadID(AND, 0)
	AndFunctionName      = "and"
)

// Functions records all overloads of the same function name
// and its function-id and type-check-function
type Functions struct {
	Id int

	Flag plan.Function_FuncFlag

	// Layout adapt to plan/function.go, used for `explain SQL`.
	Layout FuncExplainLayout

	// TypeCheckFn checks if the input parameters can satisfy one of the overloads
	// and returns its index id.
	// if type convert should happen, return the target-types at the same time.
	TypeCheckFn func(overloads []Function, inputs []types.T) (overloadIndex int32, ts []types.T)

	Overloads []Function
}

// TypeCheck do type check work for a function,
// if the input params matched one of function's overloads.
// returns overload-index-number, target-type
// just set target-type nil if there is no need to do implicit-type-conversion for parameters
func (fs *Functions) TypeCheck(args []types.T) (int32, []types.T) {
	if fs.TypeCheckFn == nil {
		matched := make([]int32, 0, 4)   // function overload which can be matched directly
		byCast := make([]int32, 0, 4)    // function overload which can be matched according to type cast
		convertCost := make([]int, 0, 4) // records the cost of conversion for byCast
		for i, f := range fs.Overloads {
			c, cost := tryToMatch(args, f.Args)
			switch c {
			case matchedDirectly:
				matched = append(matched, int32(i))
			case matchedByConvert:
				byCast = append(byCast, int32(i))
				convertCost = append(convertCost, cost)
			case matchedFailed:
				continue
			}
		}
		if len(matched) == 1 {
			return matched[0], nil
		} else if len(matched) == 0 && len(byCast) > 0 {
			// choose the overload with the least number of conversions
			min, index := math.MaxInt32, 0
			for j := range convertCost {
				if convertCost[j] < min {
					index = j
					min = convertCost[j]
				}
			}
			return byCast[index], fs.Overloads[byCast[index]].Args
		} else if len(matched) > 1 {
			// if contains any scalar null as param, just return the first matched.
			for j := range args {
				if args[j] == ScalarNull {
					return matched[0], nil
				}
			}
			return tooManyFunctionsMatched, nil
		}
		return wrongFunctionParameters, nil
	}
	return fs.TypeCheckFn(fs.Overloads, args)
}

// Function is an overload of
// a built-in function or an aggregate function or an operator
type Function struct {
	// Index is the function's location number of all the overloads with the same functionName.
	Index int32

	// Volatile function cannot be fold
	Volatile bool

	// whether the function needs to append a hidden parameter, such as 'uuid'
	AppendHideArg bool

	Args      []types.T
	ReturnTyp types.T

	// Fn is implementation of built-in function and operator
	// it received vector list, and return result vector.
	Fn func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error)

	// AggregateInfo is related information about aggregate function.
	AggregateInfo int

	// Info records information about the function overload used to print
	Info string

	flag plan.Function_FuncFlag

	// Layout adapt to plan/function.go, used for `explain SQL`.
	layout FuncExplainLayout
}

func (f *Function) GetFlag() plan.Function_FuncFlag {
	return f.flag
}

func (f *Function) GetLayout() FuncExplainLayout {
	return f.layout
}

// ReturnType return result-type of function, and the result is nullable
// if nullable is false, function won't return a vector with null value.
func (f Function) ReturnType() (typ types.T, nullable bool) {
	return f.ReturnTyp, true
}

func (f Function) VecFn(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if f.Fn == nil {
		return nil, moerr.NewInternalError("no function")
	}
	return f.Fn(vs, proc)
}

func (f Function) IsAggregate() bool {
	return f.GetFlag() == plan.Function_AGG
}

func (f Function) isFunction() bool {
	return f.GetLayout() == STANDARD_FUNCTION || f.GetLayout() >= NOPARAMETER_FUNCTION
}

// functionRegister records the information about
// all the operator, built-function and aggregate function.
//
// For use in other packages, see GetFunctionByID and GetFunctionByName
var functionRegister []Functions

// get function id from map functionIdRegister, see functionIds.go
func fromNameToFunctionId(name string) (int32, error) {
	if fid, ok := functionIdRegister[name]; ok {
		return fid, nil
	}
	return -1, moerr.NewNotSupported("function or operator '%s'", name)
}

// EncodeOverloadID convert function-id and overload-index to be an overloadID
// the high 32-bit is function-id, the low 32-bit is overload-index
func EncodeOverloadID(fid int32, index int32) (overloadID int64) {
	overloadID = int64(fid)
	overloadID = overloadID << 32
	overloadID |= int64(index)
	return overloadID
}

// DecodeOverloadID convert overload id to be function-id and overload-index
func DecodeOverloadID(overloadID int64) (fid int32, index int32) {
	base := overloadID
	index = int32(overloadID)
	fid = int32(base >> 32)
	return fid, index
}

// GetFunctionByID get function structure by its index id.
func GetFunctionByID(overloadID int64) (Function, error) {
	fid, overloadIndex := DecodeOverloadID(overloadID)
	fs := functionRegister[fid].Overloads
	return fs[overloadIndex], nil
}

func GetFunctionIsAggregateByName(name string) bool {
	fid, err := fromNameToFunctionId(name)
	if err != nil {
		return false
	}
	fs := functionRegister[fid].Overloads
	return len(fs) > 0 && fs[0].IsAggregate()
}

// Check whether the function needs to append a hidden parameter
func GetFunctionAppendHideArgByID(overloadID int64) bool {
	function, err := GetFunctionByID(overloadID)
	if err != nil {
		return false
	}
	return function.AppendHideArg
}

func GetFunctionIsMonotonicById(overloadID int64) (bool, error) {
	function, err := GetFunctionByID(overloadID)
	if err != nil {
		return false, err
	}
	// if function cann't be fold, we think that will be not monotonic
	if function.Volatile {
		return false, nil
	}
	isMonotonic := (function.GetFlag() & plan.Function_MONOTONIC) != 0
	return isMonotonic, nil
}

// GetFunctionByName check a function exist or not according to input function name and arg types,
// if matches,
// return the encoded overload id and the overload's return type
// and final converted argument types( it will be nil if there's no need to do type level-up work).
func GetFunctionByName(name string, args []types.Type) (int64, types.Type, []types.Type, error) {
	fid, err := fromNameToFunctionId(name)
	if err != nil {
		return -1, emptyType, nil, err
	}
	fs := functionRegister[fid]

	argTs := getOidSlice(args)
	index, targetTs := fs.TypeCheck(argTs)

	// if implicit type conversion happens, set the right precision for target types.
	targetTypes := getTypeSlice(targetTs)
	rewriteTypesIfNecessary(targetTypes, args)

	var finalTypes []types.Type
	if targetTs != nil {
		finalTypes = targetTypes
	} else {
		finalTypes = args
	}

	// deal the failed situations
	switch index {
	case wrongFunctionParameters:
		ArgsToPrint := getOidSlice(finalTypes) // arg information to print for error message
		if len(fs.Overloads) > 0 && fs.Overloads[0].isFunction() {
			return -1, emptyType, nil, moerr.NewInvalidArg("function "+name, ArgsToPrint)
		}
		return -1, emptyType, nil, moerr.NewInvalidArg("operator "+name, ArgsToPrint)
	case tooManyFunctionsMatched:
		return -1, emptyType, nil, moerr.NewInvalidArg("too many overloads matched "+name, args)
	case wrongFuncParamForAgg:
		ArgsToPrint := getOidSlice(finalTypes)
		return -1, emptyType, nil, moerr.NewInvalidArg("aggregate function "+name, ArgsToPrint)
	}

	// make the real return type of function overload.
	rt := getRealReturnType(fid, fs.Overloads[index], finalTypes)

	return EncodeOverloadID(fid, index), rt, targetTypes, nil
}

func ensureBinaryOperatorWithSamePrecision(targets []types.Type, hasSet []bool) {
	if len(targets) == 2 && targets[0].Oid == targets[1].Oid {
		if hasSet[0] && !hasSet[1] { // precision follow the left-part
			copyType(&targets[1], &targets[0])
			hasSet[1] = true
		} else if !hasSet[0] && hasSet[1] { // precision follow the right-part
			copyType(&targets[0], &targets[1])
			hasSet[0] = true
		}
	}
}

func rewriteTypesIfNecessary(targets []types.Type, sources []types.Type) {
	if len(targets) != 0 {
		hasSet := make([]bool, len(sources))

		//ensure that we will not lost the origin scale
		maxScale := int32(0)
		for i := range sources {
			if sources[i].Oid == types.T_decimal64 || sources[i].Oid == types.T_decimal128 {
				if sources[i].Scale > maxScale {
					maxScale = sources[i].Scale
				}
			}
		}
		for i := range sources {
			if targets[i].Oid == types.T_decimal64 || targets[i].Oid == types.T_decimal128 {
				if sources[i].Scale < maxScale {
					sources[i].Scale = maxScale
				}
			}
		}

		for i := range targets {
			oid1, oid2 := sources[i].Oid, targets[i].Oid
			// ensure that we will not lose the original precision.
			if oid2 == types.T_decimal64 || oid2 == types.T_decimal128 || oid2 == types.T_timestamp || oid2 == types.T_time {
				if oid1 == oid2 {
					copyType(&targets[i], &sources[i])
					hasSet[i] = true
				}
			}
		}
		ensureBinaryOperatorWithSamePrecision(targets, hasSet)
		for i := range targets {
			if !hasSet[i] && targets[i].Oid != ScalarNull {
				setDefaultPrecision(&targets[i])
			}
		}
	}
}

// set default precision / scalar / width for a type
func setDefaultPrecision(typ *types.Type) {
	if typ.Oid == types.T_decimal64 {
		typ.Scale = 2
		typ.Width = 6
	} else if typ.Oid == types.T_decimal128 {
		typ.Scale = 10
		typ.Width = 38
	} else if typ.Oid == types.T_timestamp {
		typ.Precision = 6
	} else if typ.Oid == types.T_datetime {
		typ.Precision = 6
	} else if typ.Oid == types.T_time {
		typ.Precision = 6
	}
	typ.Size = int32(typ.Oid.TypeLen())
}

func getRealReturnType(fid int32, f Function, realArgs []types.Type) types.Type {
	if f.IsAggregate() {
		switch fid {
		case MIN, MAX:
			if realArgs[0].Oid != ScalarNull {
				return realArgs[0]
			}
		}
	}
	rt := f.ReturnTyp.ToType()
	for i := range realArgs {
		if realArgs[i].Oid == rt.Oid {
			copyType(&rt, &realArgs[i])
			break
		}
		if types.T(rt.Oid) == types.T_decimal128 && types.T(realArgs[i].Oid) == types.T_decimal64 {
			copyType(&rt, &realArgs[i])
		}
	}
	return rt
}

func copyType(dst, src *types.Type) {
	dst.Width = src.Width
	dst.Scale = src.Scale
	dst.Precision = src.Precision
}

func getOidSlice(ts []types.Type) []types.T {
	ret := make([]types.T, len(ts))
	for i := range ts {
		ret[i] = ts[i].Oid
	}
	return ret
}

func getTypeSlice(ts []types.T) []types.Type {
	ret := make([]types.Type, len(ts))
	for i := range ts {
		ret[i] = ts[i].ToType()
	}
	return ret
}
