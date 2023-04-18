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
		return normalTypeCheck(fs.Overloads, args)
	}
	return fs.TypeCheckFn(fs.Overloads, args)
}

func normalTypeCheck(overloads []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
	matched := make([]int32, 0, 4)   // function overload which can be matched directly
	byCast := make([]int32, 0, 4)    // function overload which can be matched according to type cast
	convertCost := make([]int, 0, 4) // records the cost of conversion for byCast
	for i, f := range overloads {
		c, cost := tryToMatch(inputs, f.Args)
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
		return byCast[index], overloads[byCast[index]].Args
	} else if len(matched) > 1 {
		// if contains any scalar null as param, just return the first matched.
		for j := range inputs {
			if inputs[j] == ScalarNull {
				return matched[0], nil
			}
		}
		return tooManyFunctionsMatched, nil
	}
	return wrongFunctionParameters, nil
}

// Function is an overload of
// a built-in function or an aggregate function or an operator
type Function struct {
	// Index is the function's location number of all the overloads with the same functionName.
	Index int32

	// Volatile function cannot be fold
	Volatile bool

	// RealTimeRelate function cannot be folded when in prepare statement
	RealTimeRelated bool

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

	UseNewFramework     bool
	ResultWillNotNull   bool
	FlexibleReturnType  func(parameters []types.Type) types.Type
	ParameterMustScalar []bool
	NewFn               func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error
}

func (f *Function) TestFlag(funcFlag plan.Function_FuncFlag) bool {
	return f.flag&funcFlag != 0
}

func (f *Function) GetLayout() FuncExplainLayout {
	return f.layout
}

// ReturnType return result-type of function, and the result is nullable
// if nullable is false, function won't return a vector with null value.
func (f Function) ReturnType(args []types.Type) (typ types.Type, nullable bool) {
	if f.FlexibleReturnType != nil {
		return f.FlexibleReturnType(args), !f.ResultWillNotNull
	}
	return f.ReturnTyp.ToType(), !f.ResultWillNotNull
}

func (f Function) VecFn(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if f.Fn == nil {
		return nil, moerr.NewInternalError(proc.Ctx, "no function")
	}
	return f.Fn(vs, proc)
}

func (f Function) IsAggregate() bool {
	return f.TestFlag(plan.Function_AGG)
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
func fromNameToFunctionIdWithoutError(name string) (int32, bool) {
	if fid, ok := functionIdRegister[name]; ok {
		return fid, true
	}
	return -1, false
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
func GetFunctionByID(ctx context.Context, overloadID int64) (*Function, error) {
	fid, overloadIndex := DecodeOverloadID(overloadID)
	if int(fid) < len(functionRegister) {
		fs := functionRegister[fid].Overloads
		return &fs[overloadIndex], nil
	} else {
		return nil, moerr.NewInvalidInput(ctx, "function overload id not found")
	}
}
