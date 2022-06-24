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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
)

const maxTypeNumber = types.T_tuple + 10

type binaryTargetTypes struct {
	convert     bool
	left, right types.T
}

// binaryTable is a cast rule table for some binary-operators' parameters
// e.g. PLUS, MINUS and so on.
// Format is `binaryTable[LeftInput][RightInput] = {LeftTarget, RightTarget}`
var binaryTable [][]binaryTargetTypes

func init() {
	rules := [][4]types.T{ // left-input, right-input, left-target, right-target
		{types.T_any, types.T_any, types.T_int64, types.T_int64},
		{types.T_any, types.T_int8, types.T_int8, types.T_int8},
		{types.T_int8, types.T_uint8, types.T_int16, types.T_int16},
	}

	binaryTable = make([][]binaryTargetTypes, maxTypeNumber)
	for i := range binaryTable {
		binaryTable[i] = make([]binaryTargetTypes, maxTypeNumber)
	}
	for _, r := range rules {
		binaryTable[r[0]][r[1]] = binaryTargetTypes{
			convert: true,
			left:    r[2],
			right:   r[3],
		}
	}
}

func generalBinaryOperatorTypeCheckFn(overloads []Function, inputs []types.T) (overloadIndex int32, ts []types.T, err error) {
	if len(inputs) == 2 {
		matched := make([]int32, 0, 4)
		inputs[0], inputs[1] = generalBinaryParamsConvert(inputs[0], inputs[1])
		for _, o := range overloads {
			if strictTypeCheck(inputs, o.Args, o.ReturnTyp) {
				matched = append(matched, o.Index)
			}
		}
		if len(matched) == 1 {
			return matched[0], inputs, nil
		} else if len(matched) > 1 {
			return -1, nil, errors.New(errno.AmbiguousFunction, "too many functions matched")
		}
	}
	return -1, nil, errors.New(errno.UndefinedFunction, "type check failed")
}

func generalBinaryParamsConvert(l, r types.T) (types.T, types.T) {
	ts := binaryTable[l][r] // targets
	if ts.convert {
		return ts.left, ts.right
	}
	return l, r
}
