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
	"context"
	"math"
	"math/bits"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type noUnsignedSubtractionKey struct{}

// WithNoUnsignedSubtraction captures the statement's bind-time subtraction
// policy. Only overload selection reads it; execution uses the bound overload.
func WithNoUnsignedSubtraction(ctx context.Context, enabled bool) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if old, _ := ctx.Value(noUnsignedSubtractionKey{}).(bool); old == enabled {
		return ctx
	}
	return context.WithValue(ctx, noUnsignedSubtractionKey{}, enabled)
}

// resolveIntegerAddMulExecution owns only integer +/* signatures. The shared
// mixed predicate below also serves subtraction and must keep its own policy.
func resolveIntegerAddMulExecution(op numericBinaryOp, left, right types.Type) (operands []types.Type, result types.Type, mixed, ok bool) {
	if (op != numericOpAdd && op != numericOpMul) || !left.Oid.IsInteger() || !right.Oid.IsInteger() {
		return nil, types.Type{}, false, false
	}
	// Existing full-width same-sign vectors need no per-row adapter.
	if left.Oid == right.Oid && (left.Oid == types.T_int64 || left.Oid == types.T_uint64) {
		return []types.Type{left, right}, left, false, true
	}
	mixed = left.Oid.IsSignedInt() != right.Oid.IsSignedInt()
	operands = integerDomainOperands([]types.Type{left, right})
	result = operands[0]
	if mixed {
		result = types.New(types.T_uint64, 64, -1)
	}
	return operands, result, mixed, true
}

func isMixedUnsignedInteger(left, right types.Type) bool {
	return left.Oid == types.T_uint64 && right.Oid.IsSignedInt() ||
		right.Oid == types.T_uint64 && left.Oid.IsSignedInt()
}

func signedUnsignedSubtraction(ctx context.Context, args []types.Type) bool {
	if ctx == nil || len(args) != 2 {
		return false
	}
	enabled, _ := ctx.Value(noUnsignedSubtractionKey{}).(bool)
	return enabled && args[0].Oid.IsInteger() && args[1].Oid.IsInteger() &&
		(args[0].Oid.IsUnsignedInt() || args[1].Oid.IsUnsignedInt())
}

func integerDomainOperands(args []types.Type) []types.Type {
	result := make([]types.Type, len(args))
	for i, arg := range args {
		oid := types.T_uint64
		if arg.Oid.IsSignedInt() {
			oid = types.T_int64
		}
		result[i] = types.New(oid, 64, -1)
	}
	return result
}

type integerDomainOperand struct {
	signed   vector.FunctionParameterWrapper[int64]
	unsigned vector.FunctionParameterWrapper[uint64]
}

func newIntegerDomainOperand(v *vector.Vector) integerDomainOperand {
	if v.GetType().Oid == types.T_int64 {
		return integerDomainOperand{signed: vector.GenerateFunctionFixedTypeParameter[int64](v)}
	}
	return integerDomainOperand{unsigned: vector.GenerateFunctionFixedTypeParameter[uint64](v)}
}

func (operand integerDomainOperand) value(row uint64) (types.Decimal128, bool) {
	if operand.signed != nil {
		v, null := operand.signed.GetValue(row)
		return types.Decimal128{B0_63: uint64(v), B64_127: uint64(v >> 63)}, null
	}
	v, null := operand.unsigned.GetValue(row)
	return types.Decimal128{B0_63: v}, null
}

func mixedUnsignedPlusFn(p []*vector.Vector, r vector.FunctionResultWrapper, proc *process.Process, n int, selected *FunctionSelectList) error {
	if isMixedInt64Uint64(p) {
		return mixedUnsignedAddMul(p, r, proc, n, selected, false)
	}
	return checkedIntegerArithmetic[uint64](p, r, proc, n, selected, "+", types.Decimal128.Add128)
}
func mixedUnsignedMinusFn(p []*vector.Vector, r vector.FunctionResultWrapper, proc *process.Process, n int, selected *FunctionSelectList) error {
	return checkedIntegerArithmetic[uint64](p, r, proc, n, selected, "-", types.Decimal128.Sub128)
}
func mixedUnsignedMultiFn(p []*vector.Vector, r vector.FunctionResultWrapper, proc *process.Process, n int, selected *FunctionSelectList) error {
	if isMixedInt64Uint64(p) {
		return mixedUnsignedAddMul(p, r, proc, n, selected, true)
	}
	return checkedIntegerArithmetic[uint64](p, r, proc, n, selected, "*", types.Decimal128.Mul128)
}
func signedUnsignedMinusFn(p []*vector.Vector, r vector.FunctionResultWrapper, proc *process.Process, n int, selected *FunctionSelectList) error {
	return checkedIntegerArithmetic[int64](p, r, proc, n, selected, "-", types.Decimal128.Sub128)
}

func isMixedInt64Uint64(p []*vector.Vector) bool {
	a, b := p[0].GetType().Oid, p[1].GetType().Oid
	return a == types.T_int64 && b == types.T_uint64 || a == types.T_uint64 && b == types.T_int64
}

// Only genuine mixed pairs use this path. Existing same-sign overload-2 inputs
// keep checkedIntegerArithmetic's behavior. Normalize reading roles without
// converting negative signed operands into unsigned mathematical values.
func mixedUnsignedAddMul(p []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, n int, selected *FunctionSelectList, multiply bool) error {
	signed, unsigned := p[0], p[1]
	if signed.GetType().Oid == types.T_uint64 {
		signed, unsigned = unsigned, signed
	}
	result.UseOptFunctionParamFrame(2)
	out := vector.MustFunctionResult[uint64](result)
	sourceSigned := vector.OptGetParamFromWrapper[int64](out, 0, signed)
	sourceUnsigned := vector.OptGetParamFromWrapper[uint64](out, 1, unsigned)
	for i := uint64(0); i < uint64(n); i++ {
		if selected != nil && (selected.IgnoreAllRow() || selected.Contains(i)) {
			if err := out.Append(0, true); err != nil {
				return err
			}
			continue
		}
		s, sn := sourceSigned.GetValue(i)
		u, un := sourceUnsigned.GetValue(i)
		if sn || un {
			if err := out.Append(0, true); err != nil {
				return err
			}
			continue
		}
		var value uint64
		var overflow bool
		if multiply {
			if s == 0 || u == 0 {
				value = 0
			} else if s < 0 {
				overflow = true
			} else {
				var high uint64
				high, value = bits.Mul64(uint64(s), u)
				overflow = high != 0
			}
		} else if s < 0 {
			// Unsigned negation also handles MinInt64 without signed overflow.
			magnitude := uint64(0) - uint64(s)
			overflow = u < magnitude
			value = u - magnitude
		} else {
			var carry uint64
			value, carry = bits.Add64(u, uint64(s), 0)
			overflow = carry != 0
		}
		if overflow {
			return mixedUnsignedAddMulError(proc.Ctx, s, u, multiply)
		}
		if err := out.Append(value, false); err != nil {
			return err
		}
	}
	return nil
}

// Reuse the old mathematical diagnostic only on rejection, never per successful
// row. A genuine mixed pair's product fits signed Decimal128 before domain checks.
func mixedUnsignedAddMulError(ctx context.Context, s int64, u uint64, multiply bool) error {
	a := types.Decimal128{B0_63: uint64(s), B64_127: uint64(s >> 63)}
	b := types.Decimal128{B0_63: u}
	operator, op := "+", types.Decimal128.Add128
	if multiply {
		operator, op = "*", types.Decimal128.Mul128
	}
	value, _ := op(a, b)
	return moerr.NewOutOfRangef(ctx, "BIGINT UNSIGNED", "(mixed signed/unsigned %s result %s)", operator, value.Format(0))
}

// Inputs and the published result stay integer-typed, including after prepared
// rebinding. Only the scalar intermediate is widened to compute the mathematical
// result before checking its signed or unsigned output domain.
func checkedIntegerArithmetic[T int64 | uint64](
	p []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process,
	n int, selected *FunctionSelectList, operator string,
	op func(types.Decimal128, types.Decimal128) (types.Decimal128, error),
) error {
	left := newIntegerDomainOperand(p[0])
	right := newIntegerDomainOperand(p[1])
	out := vector.MustFunctionResult[T](result)
	signed := result.GetResultVector().GetType().Oid == types.T_int64
	lower := types.Decimal128{}
	upper := types.Decimal128{B0_63: math.MaxUint64}
	domain := "BIGINT UNSIGNED"
	if signed {
		lower = types.Decimal128{B0_63: uint64(1) << 63, B64_127: math.MaxUint64}
		upper = types.Decimal128{B0_63: math.MaxInt64}
		domain = "BIGINT"
	}
	for i := uint64(0); i < uint64(n); i++ {
		if selected != nil && (selected.IgnoreAllRow() || selected.Contains(i)) {
			if err := out.Append(0, true); err != nil {
				return err
			}
			continue
		}
		a, an := left.value(i)
		b, bn := right.value(i)
		if an || bn {
			if err := out.Append(0, true); err != nil {
				return err
			}
			continue
		}
		value, err := op(a, b)
		if err != nil || value.Compare(lower) < 0 || value.Compare(upper) > 0 {
			return moerr.NewOutOfRangef(proc.Ctx, domain, "(mixed signed/unsigned %s result %s)", operator, value.Format(0))
		}
		if err := out.Append(T(value.B0_63), false); err != nil {
			return err
		}
	}
	return nil
}
