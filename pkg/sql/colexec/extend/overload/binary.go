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

var (
	// ErrDivByZero is reported on a division by zero.
	ErrDivByZero = errors.New("division by zero")
	// ErrZeroModulus is reported when computing the rest of a division by zero.
	ErrModByZero = errors.New("zero modulus")
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

// BinOps contains the binary operations indexed by operation type.
var BinOps = map[int][]*BinOp{}

// opReturnType contains returnType of a binary expr, likes
// int + float32, bigint + double, and so on.
var opReturnType [][][]types.T

// noRt signs no binOp and returnType for this operator.
const noRt = math.MaxUint8

// GetBinOpReturnType returns returnType from binary op and arg types.
func GetBinOpReturnType(op int, lt, rt types.T) types.T {
	t := opReturnType[op-Plus][lt][rt]
	if t == noRt { // todo: just ignore and return any type to make error message normal.
		t = lt
	}
	return t
}

// init function to init opReturnType from
// plus.go / mult.go / minus.go / div.go / mod.go
func init() {
	ops := []int{Plus, Minus, Mult, Div, Mod}
	opReturnType = make([][][]types.T, len(ops))
	maxt := math.MaxUint8

	for i, op := range ops {
		opReturnType[i] = make([][]types.T, maxt)
		for p := range opReturnType[i] {
			opReturnType[i][p] = make([]types.T, maxt)
		}

		for l := range opReturnType[i] {
			for r := range opReturnType[i][l] {
				opReturnType[i][l][r] = noRt
			}
		}

		for _, bo := range BinOps[op] {
			opReturnType[i][bo.LeftType][bo.RightType] = bo.ReturnType
		}
	}
}