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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	// NullType is the type of const value `NULL`
	// If input NULL as an argument, its type should be NullValue which can match each type.
	// e.g.
	// built_in_function(100, NULL)
	// its argument type is [types.T_int64, NullType]
	NullType = types.T_any
)

var (
	// an empty function structure just for return when we couldn't meet
	// any function.
	emptyFunction = Function{}

	aggregateEvalError = errors.New(errno.AmbiguousFunction, "aggregate function should not call eval() directly")
)

// Function is an overload of
// a built-in function or an aggregate function or an operator
type Function struct {
	Flag plan.Function_FuncFlag

	Args      []types.T
	ReturnTyp types.T

	// ID is a unique flag for the overload. It is helpful to find the overload quickly.
	ID int64

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

func (f Function) ReturnType() types.T {
	return f.ReturnTyp
}

func (f Function) VecFn(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if f.IsAggregate() {
		return nil, aggregateEvalError
	}
	return f.Fn(vs, proc)
}

func (f Function) IsAggregate() bool {
	if f.Flag == plan.Function_AGG {
		return true
	}
	return false
}

// functionRegister records the information about
// all the operator, built-function and aggregate function.
//
// For use in other packages, see GetFunctionByID and GetFunctionByName
var functionRegister = map[string][]Function{}

// GetFunctionByID get function structure by its function id.
func GetFunctionByID(name string, id int64) (Function, error) {
	fs, ok := functionRegister[name]
	if !ok {
		return emptyFunction, errors.New(errno.UndefinedFunction, fmt.Sprintf("undefined function name '%s'", name))
	}
	for _, f := range fs {
		if f.ID == id {
			return f, nil
		}
	}
	return emptyFunction, errors.New(errno.UndefinedFunction, fmt.Sprintf("undefined function id '%d'", id))
}

// GetFunctionByName check a function exist or not by function name and arg types,
// if matches, return its function structure.
func GetFunctionByName(name string, args []types.T) (Function, error) {
	matches := make([]Function, 0, 4)

	if fs, ok := functionRegister[name]; ok {
		for _, f := range fs {
			if f.TypeCheck(args) {
				matches = append(matches, f)
			}
		}
	}
	// must match only 1 function
	if len(matches) == 0 {
		return emptyFunction, errors.New(errno.UndefinedFunction, fmt.Sprintf("undefined function %s(%v)", name, args))
	}
	if len(matches) > 1 {
		errMessage := "too much function matches:"
		for i := range matches {
			errMessage += "\n"
			errMessage += name
			errMessage += fmt.Sprintf("%s", matches[i].Args)
		}
		return emptyFunction, errors.New(errno.SyntaxError, errMessage)
	}
	return matches[0], nil
}

// strictTypeCheck is a general type check method.
// it returns true only when each input type meets requirement.
// Watch that : NullType can match each requirement at this function.
func strictTypeCheck(args []types.T, require []types.T) bool {
	if len(args) != len(require) {
		return false
	}
	for i := range args {
		if args[i] != require[i] && isNotNull(args[i]) {
			return false
		}
	}
	return true
}

func isNotNull(t types.T) bool {
	if t != NullType {
		return true
	}
	return false
}
