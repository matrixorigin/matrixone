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
	"math"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/sql/plan2/function/operator"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	// ScalarNull means of scalar NULL
	// which can meet each required type.
	// e.g.
	// if we input a SQL `select built_in_function(columnA, NULL);`, and columnA is int64 column.
	// it will use [types.T_int64, ScalarNull] to match function when we were building the query plan.
	ScalarNull = types.T_any

	// argument type implicit convert related const
	matchDirectly = 0
	matchFailed   = -1

	upFailed = -1 // it means type1 can not up to type2
)

var (
	// an empty function structure just for return when we couldn't meet any function.
	emptyFunction = Function{}

	// levelUpRules records the implicit convert rule for function's argument.
	// key is the original type, and value is the convertible type
	levelUpRules = map[types.T][]types.T{
		types.T_uint8: {
			types.T_uint16, types.T_uint32, types.T_uint64,
			types.T_int16, types.T_int32, types.T_int64,
			types.T_float64, types.T_decimal64, types.T_decimal128,
		},
		types.T_uint16: {
			types.T_uint32, types.T_uint64,
			types.T_int32, types.T_int64,
			types.T_float64, types.T_decimal64, types.T_decimal128,
		},
		types.T_uint32: {
			types.T_uint64,
			types.T_int64,
			types.T_float64, types.T_decimal64, types.T_decimal128,
		},
		types.T_uint64: {types.T_float64, types.T_decimal64, types.T_decimal128},
		types.T_int8: {
			types.T_int16, types.T_int32, types.T_int64, types.T_float64, types.T_decimal64, types.T_decimal128,
		},
		types.T_int16: {
			types.T_int32, types.T_int64, types.T_float64, types.T_decimal64, types.T_decimal128,
		},
		types.T_int32: {
			types.T_int64, types.T_float64, types.T_decimal64, types.T_decimal128,
		},
		types.T_int64:     {types.T_float64, types.T_decimal64, types.T_decimal128},
		types.T_float32:   {types.T_float64},
		types.T_decimal64: {types.T_decimal128},
		types.T_char:      {types.T_varchar},
		types.T_varchar:   {types.T_char},
	}
)

// Function is an overload of
// a built-in function or an aggregate function or an operator
type Function struct {
	// Index is the function's location number of all the overloads with the same functionName.
	Index int32

	// Volatile function cannot be fold
	Volatile bool

	Flag plan.Function_FuncFlag

	// Layout adapt to plan2/function.go, used for explaining.
	// TODO: combine Layout with SQLFn, or just make a map (from function_id to Layout) outside ?
	Layout FuncExplainLayout

	Args      []types.T
	ReturnTyp types.T

	// Fn is implementation of built-in function and operator
	// it received vector list, and return result vector.
	Fn func(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error)

	// TypeCheckFn is function's own argument type check function.
	// return true if inputTypes meet the type requirement.
	TypeCheckFn func(inputTypes []types.T, requiredTypes []types.T, returnType types.T) (match bool)

	// AggregateInfo is related information about aggregate function.
	AggregateInfo int

	// SQLFn returns the sql string of the function. Maybe useful.
	// TODO(cms): if useless, just remove it.
	SQLFn func(argNames []string) (string, error)

	// Info records information about the function overload used to print
	Info string
}

// TypeCheck returns true if input arguments meets function's type requirement.
func (f Function) TypeCheck(args []types.T) bool {
	return f.TypeCheckFn(args, f.Args, f.ReturnTyp)
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

func (f Function) isFunction() bool {
	return f.Layout == STANDARD_FUNCTION || f.Layout >= NOPARAMETER_FUNCTION
}

// functionRegister records the information about
// all the operator, built-function and aggregate function.
//
// For use in other packages, see GetFunctionByID and GetFunctionByName
var functionRegister = [][]Function{nil}

// levelUp records the convert rule for functions' arguments
//
// it will be filled by initLevelUpRules according to levelUpRules
var levelUp [][]int

// get function id from map functionIdRegister, see functionIds.go
func fromNameToFunctionId(name string) (int32, error) {
	if fid, ok := functionIdRegister[name]; ok {
		return fid, nil
	}
	return -1, errors.New(errno.UndefinedFunction, fmt.Sprintf("Function or operator '%s' will be implemented in future version.", name))
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
	fs := functionRegister[fid]
	return fs[overloadIndex], nil
}

func GetFunctionIsAggregateByName(name string) bool {
	fid, err := fromNameToFunctionId(name)
	if err != nil {
		return false
	}
	fs := functionRegister[fid]
	return len(fs) > 0 && fs[0].IsAggregate()
}

// GetFunctionByName check a function exist or not according to input function name and arg types,
// if matches,
// return function structure and encoded overload id
// and final converted argument types(if it needs to do type level-up work, it will be nil if not).
func GetFunctionByName(name string, args []types.T) (Function, int64, []types.T, error) {
	levelUpFunction, get, minCost := emptyFunction, false, math.MaxInt32 // store the best function which can be matched by type level-up
	matches := make([]Function, 0, 4)                                    // functions can be matched directly
	finalLevelUpTypes := make([]types.T, len(args))                      // store the final argument types of levelUpFunction

	fid, err := fromNameToFunctionId(name)
	if err != nil {
		return emptyFunction, -1, nil, err
	}

	fs := functionRegister[fid]
	b := false
	for _, f := range fs {
		if cost, finalParamTypes := f.typeCheckWithLevelUp(args); cost != matchFailed {
			if cost == matchDirectly {
				matches = append(matches, f)
			} else {
				if cost < minCost {
					levelUpFunction = f
					get = true
					minCost = cost
					copy(finalLevelUpTypes, finalParamTypes)
				} else if cost == minCost {
					levelUpFunction, b = compare(levelUpFunction, f)
					if b {
						copy(finalLevelUpTypes, finalParamTypes)
					}
					get = true
				}
			}
		}
	}

	if len(matches) == 1 {
		return matches[0], EncodeOverloadID(fid, matches[0].Index), nil, nil
	} else if len(matches) > 1 {
		// if contains any ScalarNull as param, just return the first one.
		for i := range args {
			if args[i] == ScalarNull {
				return matches[0], EncodeOverloadID(fid, matches[0].Index), nil, nil
			}
		}
		errMessage := "too many functions matched:"
		for i := range matches {
			errMessage += "\n"
			errMessage += name
			errMessage += fmt.Sprintf("%v", matches[i].Args)
		}
		return emptyFunction, -1, nil, errors.New(errno.SyntaxError, errMessage)
	} else {
		// no function matched directly, but get the best function can be matched by level-up for params
		if get {
			return levelUpFunction, EncodeOverloadID(fid, levelUpFunction.Index), finalLevelUpTypes, nil
		}
	}
	if len(fs) > 0 && fs[0].isFunction() {
		return emptyFunction, -1, nil, errors.New(errno.UndefinedFunction, fmt.Sprintf("Function '%s' with parameters %v will be implemented in future version.", name, args))
	}
	return emptyFunction, -1, nil, errors.New(errno.UndefinedFunction, fmt.Sprintf("Operator '%s' with parameters %v will be implemented in future version.", name, args))
}

// strictTypeCheck is a general type check method.
// it returns true only when each input type meets requirement.
// Watch that : ScalarNull can match each requirement at this function.
func strictTypeCheck(args []types.T, require []types.T, _ types.T) bool {
	if len(args) != len(require) {
		return false
	}
	for i := range args {
		if args[i] != require[i] && IsNotScalarNull(args[i]) {
			return false
		}
	}
	return true
}

func ConcatWsTypeCheck(args []types.T, require []types.T, _ types.T) bool {
	if len(args) <= 1 {
		return false
	}
	for _, arg := range args {
		if arg != types.T_varchar && arg != types.T_char && IsNotScalarNull(arg) {
			return false
		}
	}
	return true
}

func toDateTypeCheck(args []types.T, require []types.T, _ types.T) bool {
	if len(args) != 2 {
		return false
	}
	if args[0] != types.T_char && args[0] != types.T_varchar {
		return false
	}
	if args[0] == ScalarNull {
		return false
	}
	if args[1] != types.T_char && args[1] != types.T_varchar {
		return false
	}
	if args[1] == ScalarNull {
		return false
	}
	return true
}

// returns the cost if t1 can level up to t2
func up(t1, t2 types.T) int {
	return levelUp[t1][t2]
}

var (
	strictTypeCheckPointer   = reflect.ValueOf(strictTypeCheck).Pointer()
	caseWhenTypeCheckPointer = reflect.ValueOf(operator.CwTypeCheckFn).Pointer()
	ifTypeCheckPointer       = reflect.ValueOf(operator.IfTypeCheckFn).Pointer()
)

// typeCheckWithLevelUp check if the input parameters meet the function requirements.
// If the level-up by parameter type can meet successfully, return the cost.
// Else, just return matchDirectly or matchFailed.
func (f *Function) typeCheckWithLevelUp(sources []types.T) (int, []types.T) {
	if f.TypeCheck(sources) {
		return matchDirectly, nil
	}
	switch reflect.ValueOf(f.TypeCheckFn).Pointer() {
	case strictTypeCheckPointer:
		// types of function's arguments are clear and not confused.
		if len(f.Args) != len(sources) {
			return matchFailed, nil
		}
		cost := 0
		for i := range sources {
			if sources[i] == ScalarNull {
				continue
			}
			c := up(sources[i], f.Args[i])
			if c == upFailed {
				return matchFailed, nil
			}
			cost += c
		}
		return cost, f.Args
	case caseWhenTypeCheckPointer:
		// special type up rule for case-when operator
		rt, _ := f.ReturnType()
		l := len(sources)
		cost := 0
		finalTypes := make([]types.T, l)
		if l >= 2 {
			for i := 0; i < l-1; i += 2 {
				if sources[i] != types.T_bool {
					return matchFailed, nil
				}
				finalTypes[i] = types.T_bool
			}

			if l%2 == 1 {
				if sources[l-1] != ScalarNull {
					c := up(sources[l-1], rt)
					if c == upFailed {
						return matchFailed, nil
					}
					cost += c
					finalTypes[l-1] = rt
				} else {
					finalTypes[l-1] = ScalarNull
				}
			}

			for i := 1; i < l; i += 2 {
				if sources[i] != ScalarNull {
					c := up(sources[i], rt)
					if c == upFailed {
						return matchFailed, nil
					}
					cost += c
					finalTypes[i] = rt
				} else {
					finalTypes[i] = ScalarNull
				}
			}
			return cost, finalTypes
		}
	case ifTypeCheckPointer:
		// special type up rule for if operator
		if len(sources) == 3 && sources[0] == types.T_bool {
			cost := 0
			rt, _ := f.ReturnType()
			finalTypes := make([]types.T, 3)
			finalTypes[0] = types.T_bool
			if sources[1] != ScalarNull {
				c := up(sources[1], rt)
				if c == upFailed {
					return matchFailed, nil
				}
				cost += c
				finalTypes[1] = rt
			}
			if sources[2] != ScalarNull {
				c := up(sources[2], rt)
				if c == upFailed {
					return matchFailed, nil
				}
				cost += c
				finalTypes[1] = rt
			}
			return cost, finalTypes
		}
	}
	return matchFailed, nil
}

// choose a function when convert cost is equal
// and just return the small-index one.
// if right is smaller, return true.
func compare(f1, f2 Function) (f Function, change bool) {
	if f1.Index < f2.Index {
		return f1, false
	}
	return f2, true
}

func IsNotScalarNull(t types.T) bool {
	return t != ScalarNull
}

func isScalarNull(t types.T) bool {
	return t == ScalarNull
}

var (
	// AndFunctionEncodedID is the encoded overload id of And(bool, bool)
	// used to make an AndExpr
	AndFunctionEncodedID = EncodeOverloadID(AND, 0)

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
