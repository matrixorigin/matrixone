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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	process "github.com/matrixorigin/matrixone/pkg/vm/process2"
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
	// an empty function structure just for return when we couldn't meet any function.
	emptyFunction = Function{}
)

// Function is an overload of
// a built-in function or an aggregate function or an operator
type Function struct {
	// Index is the function's location number of all the overloads with the same functionName.
	Index int

	Flag plan.Function_FuncFlag

	// Kind adapt to plan2/function.go, used for explaining.
	// TODO: combine Kind with SQLFn, or just make a map (from function_id to Kind) outside ?
	Kind Kind

	Args      []types.T
	ReturnTyp types.T

	// Fn is implementation of built-in function and operator
	// it received vector list, and return result vector.
	Fn func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error)

	// TypeCheckFn is function's own argument type check function.
	// return true if inputTypes meet the type requirement.
	TypeCheckFn func(inputTypes []types.T, requiredTypes []types.T) (match bool)

	// AggregateInfo is related information about aggregate function.
	AggregateInfo interface{}

	// SQLFn returns the sql string of the function. Maybe useful.
	// TODO(cms): if useless, just remove it.
	SQLFn func(argNames []string) (string, error)

	// Info records information about the function overload used to print
	Info string
}

// TypeCheck returns true if input arguments meets function's type requirement.
func (f Function) TypeCheck(args []types.T) bool {
	return f.TypeCheckFn(args, f.Args)
}

// ReturnType return result-type of function, and the result is nullable
// if nullable is false, function won't return a vector with null value.
func (f Function) ReturnType() (typ types.T, nullable bool) {
	return f.ReturnTyp, true
}

func (f Function) VecFn(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if f.Fn == nil {
		return nil, errors.New(errno.AmbiguousFunction, "function doesn't implement its eval method")
	}
	return f.Fn(vs, proc)
}

func (f Function) IsAggregate() bool {
	return f.Flag == plan.Function_AGG
}

// functionRegister records the information about
// all the operator, built-function and aggregate function.
//
// For use in other packages, see GetFunctionByIndex and GetFunctionByName
var functionRegister = [][]Function{nil}

// get function id from map functionIdRegister, see functionIds.go
func getFunctionId(name string) (int, error) {
	if fid, ok := functionIdRegister[name]; ok {
		return fid, nil
	}
	return -1, errors.New(errno.UndefinedFunction, fmt.Sprintf("function '%s' doesn't register, get id failed", name))
}

// GetFunctionByIndex get function structure by its index id.
func GetFunctionByIndex(functionId int, overloadIndex int) (Function, error) {
	fs := functionRegister[functionId]
	return fs[overloadIndex], nil
}

// GetFunctionByName check a function exist or not by function name and arg types,
// if matches, return its function structure and function id.
func GetFunctionByName(name string, args []types.T) (Function, int, error) {
	matches := make([]Function, 0, 4)
	fid, err := getFunctionId(name)
	if err != nil {
		return emptyFunction, -1, err
	}

	fs := functionRegister[fid]
	for _, f := range fs {
		if f.TypeCheck(args) {
			matches = append(matches, f)
		}
	}

	// must match only 1 function
	if len(matches) == 0 {
		return emptyFunction, -1, errors.New(errno.UndefinedFunction, fmt.Sprintf("undefined function %s%v", name, args))
	}
	if len(matches) > 1 {
		errMessage := "too much function matches:"
		for i := range matches {
			errMessage += "\n"
			errMessage += name
			errMessage += fmt.Sprintf("%v", matches[i].Args)
		}
		return emptyFunction, -1, errors.New(errno.SyntaxError, errMessage)
	}
	return matches[0], fid, nil
}

// strictTypeCheck is a general type check method.
// it returns true only when each input type meets requirement.
// Watch that : ScalarNull can match each requirement at this function.
func strictTypeCheck(args []types.T, require []types.T) bool {
	if len(args) != len(require) {
		return false
	}
	for i := range args {
		if args[i] != require[i] && isNotScalarNull(args[i]) {
			return false
		}
	}
	return true
}

func isNotScalarNull(t types.T) bool {
	return t != ScalarNull
}

var (
	anyNumbers = map[types.T]struct{}{
		types.T_uint8:      {},
		types.T_uint16:     {},
		types.T_uint32:     {},
		types.T_uint64:     {},
		types.T_int8:       {},
		types.T_int16:      {},
		types.T_int32:      {},
		types.T_int64:      {},
		types.T_float32:    {},
		types.T_float64:    {},
		types.T_decimal64:  {},
		types.T_decimal128: {},
	}
)

func isNumberType(t types.T) bool {
	if _, ok := anyNumbers[t]; ok {
		return true
	}
	return false
}
