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
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
)

var (
	// unaryOpsReturnType contains returnType of a unary expr, likes
	// not (1 + 5), not bool, and so on
	unaryOpsReturnType [][]types.T

	// variants only used to init and get items from unaryOpsReturnType
	unaryOperators = []int{UnaryMinus, Not}
	firstUnaryOp = unaryOperators[0]
)

func UnaryEval(op int, typ types.T, c bool, v *vector.Vector, p *process.Process) (*vector.Vector, error) {
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
	if t == noRt { // just ignore and return any type to make error message normal.
		t = arg
	}
	return t
}
