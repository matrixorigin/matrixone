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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/vectorize/add"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"reflect"
	"sync"
)

const (
	// NoLimit is a flag that means no number limit for argument number
	NoLimit = -1

	// NullValueType is the type of const value `NULL`
	// If input NULL as an argument, its type should be NullValue which can match each type.
	// e.g.
	// built_in_function(100, NULL)
	// its argument type is [types.T_int64, NullValueType]
	NullValueType = types.T_any
)

var emptyFunction = Function{Name: "empty function structure"}

// Function is an overload of
// a built-in function or an aggregate function or an operator
type Function struct {
	// Name records info of the function overload used to print
	Name string
	Flag plan.Function_FuncFlag

	Args      ArgList
	ReturnTyp types.T

	// ID is a unique flag for the overload. It is helpful to find the overload quickly.
	ID int64

	// Fn is executed method of built-in function and operator
	Fn func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error)

	// AggregateInfo is related information if it's an aggregate function.
	// TODO(cms): maybe it will record the Ring id
	AggregateInfo interface{}

	// SQLFn returns the sql string of the function. Maybe useful.
	// TODO(cms): if useless, just remove it.
	SQLFn func(vs []*vector.Vector) (string, error)
}

// ArgList is a structure to record argument info of a function overload.
type ArgList struct {
	Limit bool // if true, argument number of function is constant

	// if Limit is true, ArgTypes1 records all argument types
	ArgTypes1 []Arg

	// if Limit is false, ArgTypes2 records all argument types
	ArgTypes2 struct {
		// TypeCheckFn is function's own argument type check function.
		TypeCheckFn func(inputTypes []types.T) bool
	}
}

type Arg struct {
	Name string // for function help, or for developers
	Typ  types.T
}

var registerMutex = sync.RWMutex{} // read/write lock for functionRegister
var functionRegister = map[string][]Function{
	// Operators
	"=": []Function{
		{
			Name: "=(uint8, uint8)",
			Flag: plan.Function_STRICT,
			Args: MakeLimitArgList([]Arg{
				{Name: "left", Typ: types.T_uint8},
				{Name: "right", Typ: types.T_uint8},
			}),
			ReturnTyp: types.T_uint8,
			ID:        operatorEqualUint8Uint8,
			Fn: func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
				lv, rv := vs[0], vs[1]
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				vec, err := process.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vector.SetCol(vec, add.Uint8Add(lvs, rvs, rs))
				if lv.Ref == 0 {
					process.Put(proc, lv)
				}
				if rv.Ref == 0 {
					process.Put(proc, rv)
				}
				return vec, nil
			},
		},
	},

	"case": {
		{
			Name: "case_when_int64",
			Flag: plan.Function_NONE,
			Args: MakeUnLimitArgList(func(ts []types.T) bool {
				l := len(ts)
				if l < 3 {
					return false
				}

				for i := 0; i < l-1; i += 2 { //case and then should be int64
					if ts[i] != types.T_int64 && isNotNull(ts[i]) {
						return false
					}
				}
				if l%2 == 1 { // has else part
					if ts[l-1] != types.T_int64 && isNotNull(ts[l-1]) {
						return false
					}
				}
				return true
			}),
			ReturnTyp: types.T_int64,
			ID:        operatorCaseWhenInt64,
			Fn: func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
				return nil, nil
			},
		},
	},
	// Functions
	// SubQuery
}

func MakeLimitArgList(args []Arg) ArgList {
	return ArgList{
		Limit:     true,
		ArgTypes1: args,
	}
}

func MakeUnLimitArgList(fn func([]types.T) bool) ArgList {
	if fn == nil {
		panic("function with variable-length parameters should have its own type-check function")
	}

	return ArgList{
		Limit: false,
		ArgTypes2: struct {
			TypeCheckFn func(inputTypes []types.T) bool
		}{TypeCheckFn: fn},
	}
}

func AppendFunction(name string, newFunction Function) error {
	if fs, ok := functionRegister[name]; ok {
		for _, f := range fs {
			if reflect.DeepEqual(f, newFunction) {
				return errors.New(errno.DuplicateFunction, fmt.Sprintf("function %s(%v) existed.", name, f.Args))
			}
		}
	}

	registerMutex.Lock()
	defer registerMutex.Unlock()

	functionRegister[name] = append(functionRegister[name], newFunction)
	return nil
}

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

// GetFunctionByName check if a function exist or not by function name and arg types, if matches, return its function structure.
func GetFunctionByName(name string, args []types.T) (Function, error) {
	// TODO(cms): may add some cast rule for function's arguments here
	//		and return the castRule. So we can init the CastExpr for arguments while making the plan-node-tree.
	//		but the logic should be more flexible ?
	if _, ok := SearchCastRule(name, args); ok {
		// ...
		// copy(args, rules)
		// ...
	}

	matches := make([]Function, 0, 4)

	if fs, ok := functionRegister[name]; ok {
		for _, f := range fs {
			if argumentCheck(args, f.Args) {
				matches = append(matches, f)
			}
		}
	}
	// must only match 1 function
	if len(matches) == 0 {
		return emptyFunction, errors.New(errno.UndefinedFunction, fmt.Sprintf("undefined function %s(%v)", name, args))
	}
	if len(matches) > 1 {
		errMessage := "too much function matches:"
		for i := range matches {
			errMessage += "\n"
			errMessage += matches[i].Name
		}
		return emptyFunction, errors.New(errno.SyntaxError, errMessage)
	}
	return matches[0], nil
}

func argumentCheck(args []types.T, fArgs ArgList) bool {
	// if function's argument number is un-limit
	// should call its own special type-check function
	if !fArgs.Limit {
		return fArgs.ArgTypes2.TypeCheckFn(args)
	}

	if len(args) != len(fArgs.ArgTypes1) {
		return false
	}
	for i := range args {
		if args[i] != fArgs.ArgTypes1[i].Typ && isNotNull(args[i]) {
			return false
		}
	}
	return true
}

func isNotNull(t types.T) bool {
	if t != NullValueType {
		return true
	}
	return false
}

func (f Function) IsAggregate() bool {
	if f.Flag == plan.Function_AGG {
		return true
	}
	return false
}

func (f Function) ReturnType() types.T {
	return f.ReturnTyp
}
