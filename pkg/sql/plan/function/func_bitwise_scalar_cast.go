// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"math/big"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// NewBitwiseScalarDecimalCast is a binder-only conversion for scalar bitwise
// operators. MySQL rounds exact DECIMAL operands and saturates them to the
// signed 64-bit domain before applying scalar bitwise operations.
func NewBitwiseScalarDecimalCast(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	if len(parameters) != 2 || parameters[1].GetType().Oid != types.T_int64 {
		return moerr.NewInternalErrorNoCtx("bitwise scalar decimal cast requires INT64 target")
	}
	from := parameters[0]
	to := vector.MustFunctionResult[int64](result)
	scale := from.GetType().Scale
	switch from.GetType().Oid {
	case types.T_decimal64:
		values := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](from)
		return appendBitwiseScalarDecimal(length, selectList, to, func(row uint64) (string, bool) {
			value, null := values.GetValue(row)
			return decimal64RoundedIntegerString(value, scale), null
		})
	case types.T_decimal128:
		values := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](from)
		return appendBitwiseScalarDecimal(length, selectList, to, func(row uint64) (string, bool) {
			value, null := values.GetValue(row)
			return decimal128RoundedIntegerString(value, scale), null
		})
	case types.T_decimal256:
		values := vector.GenerateFunctionFixedTypeParameter[types.Decimal256](from)
		return appendBitwiseScalarDecimal(length, selectList, to, func(row uint64) (string, bool) {
			value, null := values.GetValue(row)
			return decimal256RoundedIntegerString(value, scale), null
		})
	default:
		return moerr.NewInternalErrorNoCtxf("unsupported scalar bitwise decimal conversion from %s", from.GetType())
	}
}

func appendBitwiseScalarDecimal(
	length int,
	selectList *FunctionSelectList,
	to *vector.FunctionResult[int64],
	valueAt func(uint64) (string, bool),
) error {
	minSigned := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 63))
	maxSigned := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 63), big.NewInt(1))
	for row := uint64(0); row < uint64(length); row++ {
		if !bitwiseAggregateSelectedRow(selectList, row) {
			if err := to.Append(0, true); err != nil {
				return err
			}
			continue
		}
		text, null := valueAt(row)
		if null {
			if err := to.Append(0, true); err != nil {
				return err
			}
			continue
		}
		integer, ok := new(big.Int).SetString(text, 10)
		if !ok {
			return moerr.NewInternalErrorNoCtxf("invalid rounded DECIMAL integer %q", text)
		}
		if integer.Cmp(minSigned) < 0 {
			integer.Set(minSigned)
		} else if integer.Cmp(maxSigned) > 0 {
			integer.Set(maxSigned)
		}
		if err := to.Append(integer.Int64(), false); err != nil {
			return err
		}
	}
	return nil
}
