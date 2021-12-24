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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	// ErrDivByZero is reported on a division by zero.
	ErrDivByZero = errors.New("division by zero")
	// ErrModByZero is reported when computing the rest of a division by zero.
	ErrModByZero = errors.New("zero modulus")

	// BinOps contains the binary operations indexed by operation type.
	BinOps = map[int][]*BinOp{}
)

func BinaryEval(op int, ltyp, rtyp types.T, lc, rc bool, lv, rv *vector.Vector, p *process.Process) (*vector.Vector, error) {
	// do type cast if it needs.
	if rule, ok := binaryOpsNeedCast(op, ltyp, rtyp); ok {
		var err error
		leftCast, rightCast := rule.targetTypes[0], rule.targetTypes[1]
		if !lv.Typ.Eq(leftCast) {
			lv, err = BinaryEval(Typecast, ltyp, leftCast.Oid, lc, false, lv, vector.New(leftCast), p)
			if err != nil {
				return nil, err
			}
		}
		if !rv.Typ.Eq(rightCast) {
			rv, err = BinaryEval(Typecast, rtyp, rightCast.Oid, rc, false, rv, vector.New(rightCast), p)
			if err != nil {
				return nil, err
			}
		}
		ltyp, rtyp = leftCast.Oid, rightCast.Oid
	}

	if os, ok := BinOps[op]; ok {
		for _, o := range os {
			if binaryCheck(op, o.LeftType, o.RightType, ltyp, rtyp) {
				return o.Fn(lv, rv, p, lc, rc)
			}
		}
	}
	return nil, fmt.Errorf("%s not yet implemented for %s, %s", OpName[op], ltyp, rtyp)
}

func binaryCheck(_ int, arg0, arg1 types.T, val0, val1 types.T) bool {
	return arg0 == val0 && arg1 == val1
}

// binaryOpsNeedCast returns true if a binary operator needs type-cast for its arguments.
func binaryOpsNeedCast(op int, ltyp types.T, rtyp types.T) (castRule, bool) {
	rules, ok := OperatorCastRules[op]
	if !ok {
		return castRule{}, false
	}
	for _, rule := range rules {
		if rule.NumArgs == 2 && ltyp == rule.sourceTypes[0] && rtyp == rule.sourceTypes[1] {
			return rule, true
		}
	}
	return castRule{}, false
}
