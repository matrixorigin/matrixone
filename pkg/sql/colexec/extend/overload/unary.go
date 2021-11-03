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

package overload

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	// unaryOpsReturnType contains returnType of a unary expr, likes
	// not (1 + 5), not bool, and so on
	unaryOpsReturnType [][]types.T

	// UnaryOps contains the unary operations indexed by operation type.
	UnaryOps = map[int][]*UnaryOp{}

	// unaryOpsTypeCastRules contains rules of type cast for unary operators.
	// whose argument type can't be resolved directly.
	unaryOpsTypeCastRules [][]castResult

	// variants only used to init and get items from unaryOpsReturnType and unaryOpsTypeCastRules
	unaryOperators = []int{UnaryMinus, Not}
	firstUnaryOp = unaryOperators[0]
)

func init() {
	initSliceForUnaryOps()
	initCastRulesForUnaryOps()
}

func UnaryEval(op int, typ types.T, c bool, v *vector.Vector, p *process.Process) (*vector.Vector, error) {
	// do type cast if it needs.
	if rule, ok := unaryOpsNeedCast(op, typ); ok {
		var err error
		if !v.Typ.Eq(rule.leftCast) {
			v, err = BinaryEval(Typecast, typ, rule.leftCast.Oid, c, false, v, vector.New(rule.leftCast), p)
			if err != nil {
				return nil, err
			}
		}
		typ = rule.leftCast.Oid
	}

	if os, ok := UnaryOps[op]; ok {
		for _, o := range os {
			if unaryCheck(op, o.Typ, typ) {
				return o.Fn(v, p, c)
			}
		}
	}
	return nil, fmt.Errorf("'%s' not yet implemented for %s", OpName[op], typ)
}

func unaryCheck(op int, arg types.T, val types.T) bool {
	return arg == val
}

// GetUnaryOpReturnType returns the returnType of unary op and its arg types.
func GetUnaryOpReturnType(op int, arg types.T) types.T {
	t := unaryOpsReturnType[op-firstUnaryOp][arg]
	if t == noRt {
		t = arg
	}
	return t
}

func initSliceForUnaryOps() {
	unaryOpsReturnType = make([][]types.T, len(unaryOperators))
	for i  := range unaryOperators {
		unaryOpsReturnType[i] = make([]types.T, maxt)
		for j := range unaryOpsReturnType[i] {
			unaryOpsReturnType[i][j] = noRt
		}
	}
}

func initCastRulesForUnaryOps() {
	unaryOpsTypeCastRules = make([][]castResult, len(unaryOperators))
	for i := range unaryOpsTypeCastRules {
		unaryOpsTypeCastRules[i] = make([]castResult, maxt)
	}

	chars := []types.T{types.T_char, types.T_varchar}
	// Not Operator
	{
		index := Not - firstUnaryOp

		// cast to Not Float64
		nl, nret := types.Type{Oid: types.T_float64, Size: 8}, types.T_int8
		for _, argTyp := range chars {
			unaryOpsTypeCastRules[index][argTyp] = castResult{
				has:           true,
				leftCast:      nl,
				newReturnType: types.T(nret),
			}
			unaryOpsReturnType[index][argTyp] = types.T(nret)
		}
	}
}

// unaryOpsNeedCast returns true if a unary operator needs type-cast for its argument.
func unaryOpsNeedCast(op int, argTyp types.T) (castResult, bool) {
	return unaryOpsTypeCastRules[op-firstUnaryOp][argTyp], unaryOpsTypeCastRules[op-firstUnaryOp][argTyp].has
}