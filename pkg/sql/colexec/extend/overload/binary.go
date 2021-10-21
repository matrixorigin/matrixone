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
	"errors"
	"fmt"
	"math"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
)

const (
	// noRt signs there is no function and returnType for this operator.
	noRt = math.MaxUint8
	// maxt is max length of binOpsReturnType and unaryOpsReturnType
	maxt = math.MaxUint8
)

var (
	// ErrDivByZero is reported on a division by zero.
	ErrDivByZero = errors.New("division by zero")
	// ErrZeroModulus is reported when computing the rest of a division by zero.
	ErrModByZero = errors.New("zero modulus")

	// BinOps contains the binary operations indexed by operation type.
	BinOps = map[int][]*BinOp{}

	// binOpsReturnType contains returnType of a binary expr, likes
	// int + float32, bigint + double, and so on.
	binOpsReturnType [][][]types.T

	// variants only used to init and get items from binOpsReturnType
	// binOperators does not include comparison operators because their returns are always T_sel.
	binOperators = []int{Or, And, Plus, Minus, Mult, Div, Mod}
	firstBinaryOp = binOperators[0]
)

func BinaryEval(op int, ltyp, rtyp types.T, lc, rc bool, lv, rv *vector.Vector, p *process.Process) (*vector.Vector, error) {
	if os, ok := BinOps[op]; ok {
		for _, o := range os {
			if binaryCheck(op, o.LeftType, o.RightType, ltyp, rtyp) {
				return o.Fn(lv, rv, p, lc, rc)
			}
		}
	}
	return nil, fmt.Errorf("%s not yet implemented for %s, %s", OpName[op], ltyp, rtyp)
}

func binaryCheck(op int, arg0, arg1 types.T, val0, val1 types.T) bool {
	return arg0 == val0 && arg1 == val1
}

// GetBinOpReturnType returns the returnType of binary op and its arg types.
func GetBinOpReturnType(op int, lt, rt types.T) types.T {
	t := binOpsReturnType[op-firstBinaryOp][lt][rt]
	if t == noRt { // just ignore and return any type to make error message normal.
		t = lt
	}
	return t
}
