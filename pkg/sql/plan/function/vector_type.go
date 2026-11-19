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

import "github.com/matrixorigin/matrixone/pkg/container/types"

// commonVectorType returns the type that holds every vector argument exactly.
// Every vector argument must have the same dimension. Arguments of one vector
// type keep it; mixed vector types resolve to vecf64 when any argument is
// vecf64 and to vecf32 otherwise. Untyped NULL arguments and string arguments
// with a cast to the result are cast to it; any other argument type fails.
// hasVector reports whether any argument is a vector.
func commonVectorType(inputs []types.Type) (target types.Type, hasVector, ok bool) {
	mixed, wide := false, false
	for _, input := range inputs {
		if !input.Oid.IsArray() {
			continue
		}
		wide = wide || input.Oid == types.T_array_float64
		if !hasVector {
			target, hasVector = input, true
			continue
		}
		if input.Width != target.Width {
			return target, true, false
		}
		mixed = mixed || input.Oid != target.Oid
	}
	if !hasVector {
		return target, false, true
	}
	if mixed {
		oid := types.T_array_float32
		if wide {
			oid = types.T_array_float64
		}
		target = types.New(oid, target.Width, 0)
	}
	for _, input := range inputs {
		if input.Oid.IsArray() || input.Oid == types.T_any {
			continue
		}
		if !input.Oid.IsMySQLString() || !IfTypeCastSupported(input.Oid, target.Oid) {
			return target, true, false
		}
	}
	return target, true, true
}

func coalesceVectorReturnType(parameters []types.Type) types.Type {
	target, _, _ := commonVectorType(parameters)
	return target
}

// sameLowPrecisionFloatType returns the bf16/float16/float8/float4 type shared by every
// branch; untyped NULL branches take that type.
// widenMixedLowPrecisionFloats replaces the bf16/float16/float8/float4 value arguments
// with float32 when the values mix them with another type; isValue selects the value
// positions. A conditional then resolves them as it resolves float32.
func widenMixedLowPrecisionFloats(inputs []types.Type, isValue func(int) bool) ([]types.Type, bool) {
	values := make([]types.Type, 0, len(inputs))
	hasLowPrecision := false
	for i, input := range inputs {
		if isValue(i) {
			values = append(values, input)
			hasLowPrecision = hasLowPrecision || input.Oid.IsLowPrecisionFloat()
		}
	}
	if !hasLowPrecision {
		return nil, false
	}
	if _, same := sameLowPrecisionFloatType(values); same {
		return nil, false
	}
	widened := append([]types.Type(nil), inputs...)
	for i, input := range widened {
		if isValue(i) && input.Oid.IsLowPrecisionFloat() {
			widened[i] = types.T_float32.ToType()
		}
	}
	return widened, true
}

// castFromWidened returns a resolution of widened arguments as a cast of the original ones.
func castFromWidened(result checkResult, widened []types.Type) checkResult {
	if result.status == succeedMatched {
		return newCheckResultWithCast(result.idx, widened)
	}
	return result
}

func sameLowPrecisionFloatType(inputs []types.Type) (types.Type, bool) {
	var target types.Type
	found := false
	for _, input := range inputs {
		if input.Oid == types.T_any {
			continue
		}
		if !input.Oid.IsLowPrecisionFloat() || (found && input.Oid != target.Oid) {
			return types.Type{}, false
		}
		target, found = input, true
	}
	return target, found
}
