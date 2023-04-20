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

func normalTypeCheck(overloads []Function, inputs []types.T) (overloadIndex int32, ts []types.T) {
	panic("old function framework code")
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
	panic("old function framework code")
}

func (f *Function) GetLayout() FuncExplainLayout {
	panic("old function framework code")
}

// ReturnType return result-type of function, and the result is nullable
// if nullable is false, function won't return a vector with null value.
func (f Function) ReturnType(args []types.Type) (typ types.Type, nullable bool) {
	panic("old function framework code")
}

func (f Function) VecFn(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	panic("old function framework code")
}

func (f Function) IsAggregate() bool {
	panic("old function framework code")
}

func (f Function) isFunction() bool {
	panic("old function framework code")
}

// functionRegister records the information about
// all the operator, built-function and aggregate function.
//
// For use in other packages, see GetFunctionByID and GetFunctionByName
var functionRegister []Functions

// get function id from map functionIdRegister, see functionIds.go
func fromNameToFunctionIdWithoutError(name string) (int32, bool) {
	panic("old function framework code")
}

// EncodeOverloadID convert function-id and overload-index to be an overloadID
// the high 32-bit is function-id, the low 32-bit is overload-index
func EncodeOverloadID(fid int32, index int32) (overloadID int64) {
	panic("old function framework code")
}

// DecodeOverloadID convert overload id to be function-id and overload-index
func DecodeOverloadID(overloadID int64) (fid int32, index int32) {
	panic("old function framework code")
}

// GetFunctionByID get function structure by its index id.
func GetFunctionByID(ctx context.Context, overloadID int64) (*Function, error) {
	panic("old function framework code")
}
