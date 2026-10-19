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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Keep overload zero executable for historical plans, whose decimal DIV result
// is signed even when an original integer operand was unsigned.
const DecimalIntegerDivUnsignedOverload = 1

// DecimalIntegerDivReturnType derives display precision before execution casts
// erase operand widths and unsigned domains. Arithmetic remains 64 bit; schema
// consumers alone may publish a narrower integer column.
func DecimalIntegerDivReturnType(original []types.Type) (types.Type, bool) {
	if len(original) != 2 ||
		!(original[0].Oid.IsDecimal() || original[1].Oid.IsDecimal()) ||
		!(original[0].Oid.IsDecimal() || original[0].Oid.IsInteger()) ||
		!(original[1].Oid.IsDecimal() || original[1].Oid.IsInteger()) {
		return types.Type{}, false
	}
	p0, s0, _ := exactDivisionOperandType(original[0], original[0])
	_, s1, _ := exactDivisionOperandType(original[1], original[1])
	precision := min(max(p0-s0+s1, 1), 19)
	oid := types.T_int64
	if original[0].Oid.IsUnsignedInt() || original[1].Oid.IsUnsignedInt() {
		oid = types.T_uint64
	} else {
		precision++ // MySQL includes the sign in the display length.
	}
	return types.New(oid, precision, 0), true
}

func decimalIntegerDivUnsignedTypes(original []types.Type) ([]types.Type, bool) {
	result, ok := DecimalIntegerDivReturnType(original)
	if !ok || result.Oid != types.T_uint64 {
		return nil, false
	}
	return decimalIntegerDivOperandTypes(original), true
}

func decimalIntegerDivOperandTypes(original []types.Type) []types.Type {
	return []types.Type{
		decimal256DivisionOperandType(original[0], original[0]),
		decimal256DivisionOperandType(original[1], original[1]),
	}
}

func (fr *FuncGetResult) applyDecimalIntegerDivMetadata(original []types.Type) {
	if fr.fid != INTEGER_DIV {
		return
	}
	if result, ok := DecimalIntegerDivReturnType(original); ok {
		if fr.overloadId == DecimalIntegerDivUnsignedOverload && result.Oid == types.T_int64 {
			result.Oid = types.T_uint64
			result.Width--
		}
		// Explicit selection of historical overload zero preserves its signed domain.
		if fr.overloadId == 0 && result.Oid == types.T_uint64 {
			return
		}
		fr.retType = result
	}
}

func decimalIntegerDivUnsignedFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *FunctionSelectList) error {
	shouldError := checkDivisionByZeroBehavior(proc, selection)
	if err := decimalBatchArith[types.Decimal256, uint64](parameters, result, proc, length,
		func(left, right []types.Decimal256, out []uint64, s0, s1 int32, ns *nulls.Nulls) error {
			return decimalIntegerDivUnsigned(left, right, out, s0, s1, ns, shouldError)
		}, selection); err != nil {
		return err
	}
	appendDivisionByZeroWarnings(parameters, result.GetResultVector(), proc, length, selection)
	return nil
}

// decimalIntegerDivUnsigned borrows the wrapper's selected rows and NULL bitmap.
// The ordinary path uses fixed-width magnitude arithmetic without row allocations.
func decimalIntegerDivUnsigned(left, right []types.Decimal256, out []uint64, s0, s1 int32, ns *nulls.Nulls, shouldError bool) error {
	for i := range out {
		if ns.Contains(uint64(i)) {
			continue
		}
		li, ri := i, i
		if len(left) == 1 {
			li = 0
		}
		if len(right) == 1 {
			ri = 0
		}
		a, b := left[li], right[ri]
		if d256IsZero(b) {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			ns.Add(uint64(i))
			continue
		}
		negative := a.Sign() != b.Sign()
		if a.Sign() {
			a = a.Minus()
		}
		if b.Sign() {
			b = b.Minus()
		}
		adjustment := s1 - s0
		if adjustment > 0 {
			scaled := a
			if !d256MulPow10(&scaled, adjustment) {
				q, err := decimalIntegerDivUnsignedScaleOverflow(a, b, adjustment, negative)
				if err != nil {
					return err
				}
				out[i] = q
				continue
			}
			a = scaled
		} else if adjustment < 0 {
			var err error
			a, err = a.ScaleTruncate(adjustment)
			if err != nil {
				return err
			}
		}
		quotient, err := a.Div256Trunc(b)
		if err != nil {
			return err
		}
		if quotient.B64_127|quotient.B128_191|quotient.B192_255 != 0 || (negative && quotient.B0_63 != 0) {
			return moerr.NewOutOfRangeNoCtx("BIGINT UNSIGNED", "")
		}
		out[i] = quotient.B0_63
	}
	return nil
}

func decimalIntegerDivUnsignedScaleOverflow(a, b types.Decimal256, adjustment int32, negative bool) (uint64, error) {
	if d256IsZero(a) {
		return 0, nil
	}
	// 10^96 / max Decimal256 magnitude already exceeds uint64.
	if adjustment >= 96 {
		return 0, moerr.NewOutOfRangeNoCtx("BIGINT UNSIGNED", "")
	}
	numerator := d256MagnitudeBigInt(a)
	numerator.Mul(numerator, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(adjustment)), nil))
	quotient := new(big.Int).Quo(numerator, d256MagnitudeBigInt(b))
	if !quotient.IsUint64() || (negative && quotient.Sign() != 0) {
		return 0, moerr.NewOutOfRangeNoCtx("BIGINT UNSIGNED", "")
	}
	return quotient.Uint64(), nil
}
