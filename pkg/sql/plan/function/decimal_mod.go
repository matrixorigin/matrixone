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

import "github.com/matrixorigin/matrixone/pkg/container/types"

// DecimalModReturnType derives exact-value MOD metadata from the original
// operands. Implicit casts describe storage, not the operands' SQL precision.
// Keep the selected physical type: modFn's result kernel must match its inputs.
func DecimalModReturnType(original []types.Type, selected types.Type) types.Type {
	if len(original) != 2 || !selected.Oid.IsDecimal() ||
		(!original[0].Oid.IsDecimal() && !original[1].Oid.IsDecimal()) {
		return selected
	}
	var precision, scale int32
	for _, operand := range original {
		var width, operandScale int32
		switch {
		case operand.Oid.IsDecimal():
			width = operand.Width
			if width <= 0 {
				width = decimalStorageWidth(operand.Oid)
			}
			operandScale = max32(operand.Scale, 0)
		case operand.Oid.IsInteger():
			width = integerDecimalDigits(operand.Oid)
			if operand.IsMediumInt() {
				width = 7
				if operand.Oid == types.T_uint32 {
					width = 8
				}
			}
		default:
			// Preserve existing coercion semantics for temporal, approximate,
			// string, BIT and unknown operands.
			return selected
		}
		precision = max32(precision, width)
		scale = max32(scale, operandScale)
	}
	selected.Width, selected.Scale = precision, scale
	return selected
}

func (fr *FuncGetResult) applyDecimalModMetadata(original []types.Type) {
	if fr.fid == MOD && fr.overloadId == 0 {
		fr.retType = DecimalModReturnType(original, fr.retType)
	}
}
