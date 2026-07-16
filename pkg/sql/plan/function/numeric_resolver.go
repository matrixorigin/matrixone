// Copyright 2026 Matrix Origin
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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type numericBinaryOp uint8

const (
	numericOpAdd numericBinaryOp = iota
	numericOpSub
	numericOpMul
	numericOpDiv
	numericOpIntegerDiv
	numericOpMod
)

type numericTypeResolution struct {
	left   types.Type
	right  types.Type
	result types.Type
}

// InferNumericParameterType selects the computation type for unresolved
// numeric parameters. known may contain operands from any depth in one
// arithmetic subtree. Approximate operands dominate exact operands, followed
// by the outer context and finally the default DOUBLE domain.
func InferNumericParameterType(known []types.Type, outer *types.Type) (types.Type, bool) {
	for _, typ := range known {
		if typ.Oid == types.T_float32 || typ.Oid == types.T_float64 {
			return types.T_float64.ToType(), true
		}
	}
	if decimal, found, ok := commonDecimalType(known, outer); found {
		return decimal, ok
	}
	if outer != nil && outer.IsNumeric() {
		return *outer, true
	}
	if integer, ok := commonIntegerType(known); ok {
		return integer, true
	}
	return types.T_float64.ToType(), true
}

func commonDecimalType(known []types.Type, outer *types.Type) (types.Type, bool, bool) {
	found := false
	maxIntegerDigits := int32(0)
	maxScale := int32(0)
	minStorage := types.T_decimal64
	for _, typ := range known {
		if typ.Oid.IsInteger() {
			maxIntegerDigits = max32(maxIntegerDigits, integerDecimalDigits(typ.Oid))
			continue
		}
		if !typ.Oid.IsDecimal() {
			continue
		}
		found = true
		width := typ.Width
		if width <= 0 {
			width = decimalStorageWidth(typ.Oid)
		}
		maxIntegerDigits = max32(maxIntegerDigits, max32(0, width-typ.Scale))
		maxScale = max32(maxScale, typ.Scale)
		if decimalStorageWidth(typ.Oid) > decimalStorageWidth(minStorage) {
			minStorage = typ.Oid
		}
	}
	if !found {
		return types.Type{}, false, false
	}
	if outer != nil {
		if outer.Oid.IsInteger() {
			maxIntegerDigits = max32(maxIntegerDigits, integerDecimalDigits(outer.Oid))
		} else if outer.Oid.IsDecimal() {
			width := outer.Width
			if width <= 0 {
				width = decimalStorageWidth(outer.Oid)
			}
			maxIntegerDigits = max32(maxIntegerDigits, max32(0, width-outer.Scale))
			maxScale = max32(maxScale, outer.Scale)
			if decimalStorageWidth(outer.Oid) > decimalStorageWidth(minStorage) {
				minStorage = outer.Oid
			}
		}
	}

	width := maxIntegerDigits + maxScale
	oid := minStorage
	if width > decimalStorageWidth(oid) {
		switch {
		case width <= 38:
			oid = types.T_decimal128
		case width <= 76:
			oid = types.T_decimal256
		default:
			return types.Type{}, true, false
		}
	}
	return types.New(oid, width, maxScale), true, true
}

func integerDecimalDigits(oid types.T) int32 {
	switch oid {
	case types.T_int8, types.T_uint8:
		return 3
	case types.T_int16, types.T_uint16:
		return 5
	case types.T_int32, types.T_uint32:
		return 10
	case types.T_int64:
		return 19
	case types.T_uint64:
		return 20
	default:
		return 0
	}
}

func decimalStorageWidth(oid types.T) int32 {
	switch oid {
	case types.T_decimal64:
		return 18
	case types.T_decimal128:
		return 38
	case types.T_decimal256:
		return 76
	default:
		return 0
	}
}

func commonIntegerType(known []types.Type) (types.Type, bool) {
	maxSignedBits := 0
	maxUnsignedBits := 0
	for _, typ := range known {
		bits, signed := integerTypeBits(typ.Oid)
		if bits == 0 {
			continue
		}
		if signed {
			if bits > maxSignedBits {
				maxSignedBits = bits
			}
		} else if bits > maxUnsignedBits {
			maxUnsignedBits = bits
		}
	}
	if maxSignedBits == 0 && maxUnsignedBits == 0 {
		return types.Type{}, false
	}
	if maxSignedBits == 0 {
		return unsignedIntegerType(maxUnsignedBits), true
	}
	if maxUnsignedBits == 0 {
		return signedIntegerType(maxSignedBits), true
	}

	requiredSignedBits := maxSignedBits
	if maxUnsignedBits >= requiredSignedBits {
		requiredSignedBits = maxUnsignedBits + 1
	}
	if requiredSignedBits <= 64 {
		return signedIntegerType(requiredSignedBits), true
	}
	return types.New(types.T_decimal128, 38, 0), true
}

func integerTypeBits(oid types.T) (int, bool) {
	switch oid {
	case types.T_int8:
		return 8, true
	case types.T_int16:
		return 16, true
	case types.T_int32:
		return 32, true
	case types.T_int64:
		return 64, true
	case types.T_uint8:
		return 8, false
	case types.T_uint16:
		return 16, false
	case types.T_uint32:
		return 32, false
	case types.T_uint64:
		return 64, false
	default:
		return 0, false
	}
}

func signedIntegerType(minBits int) types.Type {
	switch {
	case minBits <= 8:
		return types.T_int8.ToType()
	case minBits <= 16:
		return types.T_int16.ToType()
	case minBits <= 32:
		return types.T_int32.ToType()
	default:
		return types.T_int64.ToType()
	}
}

func unsignedIntegerType(minBits int) types.Type {
	switch {
	case minBits <= 8:
		return types.T_uint8.ToType()
	case minBits <= 16:
		return types.T_uint16.ToType()
	case minBits <= 32:
		return types.T_uint32.ToType()
	default:
		return types.T_uint64.ToType()
	}
}

// ResolveNumericBinaryTypes resolves the physical operand and result types for
// an arithmetic operator. The binder uses it only after it has established a
// prepared numeric context; ordinary coercion keeps using the existing
// overload path.
func ResolveNumericBinaryTypes(
	name string,
	left types.Type,
	right types.Type,
	outer *types.Type,
) (types.Type, types.Type, types.Type, bool) {
	op, ok := numericBinaryOpByName(name)
	if !ok {
		return types.Type{}, types.Type{}, types.Type{}, false
	}
	resolved, ok := resolveNumericBinaryTypes(op, left, right, outer)
	return resolved.left, resolved.right, resolved.result, ok
}

func numericBinaryOpByName(name string) (numericBinaryOp, bool) {
	switch strings.ToLower(name) {
	case "+":
		return numericOpAdd, true
	case "-":
		return numericOpSub, true
	case "*":
		return numericOpMul, true
	case "/":
		return numericOpDiv, true
	case "div":
		return numericOpIntegerDiv, true
	case "%", "mod":
		return numericOpMod, true
	default:
		return 0, false
	}
}

// resolveNumericBinaryTypes resolves parameter markers (T_any) before asking
// the existing overload machinery for physical coercion types. outer is only
// a fallback for unresolved operands; a typed sibling always takes precedence.
func resolveNumericBinaryTypes(
	op numericBinaryOp,
	left types.Type,
	right types.Type,
	outer *types.Type,
) (numericTypeResolution, bool) {
	left, right, ok := resolveUnknownNumericOperands(left, right, outer)
	if !ok {
		return numericTypeResolution{}, false
	}

	var cast bool
	var castLeft, castRight types.Type
	switch op {
	case numericOpDiv, numericOpIntegerDiv:
		cast, castLeft, castRight = fixedTypeCastRule2(left, right)
	default:
		cast, castLeft, castRight = fixedTypeCastRule1(left, right)
	}
	if cast {
		left, right = castLeft, castRight
	}
	if !numericOperatorSupports(op, left, right) {
		return numericTypeResolution{}, false
	}

	return numericTypeResolution{
		left:   left,
		right:  right,
		result: numericBinaryResultType(op, left, right),
	}, true
}

func resolveUnknownNumericOperands(left, right types.Type, outer *types.Type) (types.Type, types.Type, bool) {
	leftUnknown := left.Oid == types.T_any
	rightUnknown := right.Oid == types.T_any
	if (!leftUnknown && !left.IsNumeric()) || (!rightUnknown && !right.IsNumeric()) {
		return types.Type{}, types.Type{}, false
	}
	if !leftUnknown && !rightUnknown {
		return left, right, true
	}

	hint, ok := numericTypeHint(left, right, outer)
	if !ok {
		return types.Type{}, types.Type{}, false
	}
	if leftUnknown {
		left = hint
	}
	if rightUnknown {
		right = hint
	}
	return left, right, true
}

func numericTypeHint(left, right types.Type, outer *types.Type) (types.Type, bool) {
	known := make([]types.Type, 0, 2)
	if left.Oid != types.T_any {
		known = append(known, left)
	}
	if right.Oid != types.T_any {
		known = append(known, right)
	}
	return InferNumericParameterType(known, outer)
}

func numericOperatorSupports(op numericBinaryOp, left, right types.Type) bool {
	switch op {
	case numericOpAdd:
		return plusOperatorSupports(left, right)
	case numericOpSub:
		return minusOperatorSupports(left, right)
	case numericOpMul:
		return multiOperatorSupports(left, right)
	case numericOpDiv:
		return divOperatorSupports(left, right)
	case numericOpIntegerDiv:
		return integerDivOperatorSupports(left, right)
	case numericOpMod:
		return modOperatorSupports(left, right)
	default:
		return false
	}
}

func numericBinaryResultType(op numericBinaryOp, left, right types.Type) types.Type {
	switch op {
	case numericOpIntegerDiv:
		return types.T_int64.ToType()
	case numericOpAdd, numericOpSub:
		if left.Oid.IsDecimal() || right.Oid.IsDecimal() {
			return decimalAddSubResultType(left, right)
		}
	case numericOpMul:
		if left.Oid.IsDecimal() || right.Oid.IsDecimal() {
			return decimalMulResultType(left, right)
		}
	case numericOpDiv:
		if left.Oid.IsDecimal() || right.Oid.IsDecimal() {
			return decimalDivResultType(left, right)
		}
	case numericOpMod:
		if left.Oid.IsDecimal() || right.Oid.IsDecimal() {
			left.Scale = max32(left.Scale, right.Scale)
		}
	}
	return left
}

func decimalAddSubResultType(left, right types.Type) types.Type {
	scale := max32(left.Scale, right.Scale)
	switch {
	case left.Oid == types.T_decimal256 || right.Oid == types.T_decimal256:
		return types.New(types.T_decimal256, 65, scale)
	case left.Oid == types.T_decimal128 || right.Oid == types.T_decimal128:
		return types.New(types.T_decimal128, 38, scale)
	default:
		return types.New(types.T_decimal64, 18, scale)
	}
}

func decimalMulResultType(left, right types.Type) types.Type {
	scale := int32(12)
	scale = max32(scale, left.Scale)
	scale = max32(scale, right.Scale)
	if left.Scale+right.Scale < scale {
		scale = left.Scale + right.Scale
	}
	if left.Oid == types.T_decimal256 || right.Oid == types.T_decimal256 {
		return types.New(types.T_decimal256, 65, scale)
	}
	return types.New(types.T_decimal128, 38, scale)
}

func decimalDivResultType(left, right types.Type) types.Type {
	scale := int32(12)
	if scale > left.Scale+6 {
		scale = left.Scale + 6
	}
	if scale < left.Scale {
		scale = left.Scale
	}
	if left.Oid == types.T_decimal256 || right.Oid == types.T_decimal256 {
		return types.New(types.T_decimal256, 65, scale)
	}
	return types.New(types.T_decimal128, 38, scale)
}

func max32(left, right int32) int32 {
	if left > right {
		return left
	}
	return right
}
