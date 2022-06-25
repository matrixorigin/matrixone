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
	// UnaryOps contains the unary operations indexed by operation type.
	UnaryOps = map[int][]*UnaryOp{}
)

func UnaryEval(op int, typ types.T, c bool, v *vector.Vector, p *process.Process) (*vector.Vector, error) {
	// do type cast if it needs.
	if rule, ok := unaryOpsNeedCast(op, typ); ok {
		var err error
		target := rule.targetTypes[0]
		if !v.Typ.Eq(target) {
			v, err = BinaryEval(Typecast, typ, target.Oid, c, false, v, vector.New(target), p)
			if err != nil {
				return nil, err
			}
		}
		typ = target.Oid
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

func unaryCheck(_ int, arg types.T, val types.T) bool {
	return arg == val
}

// unaryOpsNeedCast returns true if a unary operator needs type-cast for its argument.
func unaryOpsNeedCast(op int, argTyp types.T) (castRule, bool) {
	rules, ok := OperatorCastRules[op]
	if !ok {
		return castRule{}, false
	}
	for _, rule := range rules {
		if rule.NumArgs == 1 && argTyp == rule.sourceTypes[0] {
			return rule, true
		}
	}
	return castRule{}, false
}
