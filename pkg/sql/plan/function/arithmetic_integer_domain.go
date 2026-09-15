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
	return checkedIntegerArithmetic[uint64](p, r, proc, n, selected, "+", types.Decimal128.Add128)
}
func mixedUnsignedMinusFn(p []*vector.Vector, r vector.FunctionResultWrapper, proc *process.Process, n int, selected *FunctionSelectList) error {
	return checkedIntegerArithmetic[uint64](p, r, proc, n, selected, "-", types.Decimal128.Sub128)
}
func mixedUnsignedMultiFn(p []*vector.Vector, r vector.FunctionResultWrapper, proc *process.Process, n int, selected *FunctionSelectList) error {
	return checkedIntegerArithmetic[uint64](p, r, proc, n, selected, "*", types.Decimal128.Mul128)
}
func signedUnsignedMinusFn(p []*vector.Vector, r vector.FunctionResultWrapper, proc *process.Process, n int, selected *FunctionSelectList) error {
	return checkedIntegerArithmetic[int64](p, r, proc, n, selected, "-", types.Decimal128.Sub128)
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
