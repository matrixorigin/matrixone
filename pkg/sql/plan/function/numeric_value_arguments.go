// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

// HasNumericValueArguments reports whether a registered function has at least
// one argument that participates in its numeric value domain.
func HasNumericValueArguments(name string) bool {
	id, ok := getFunctionIdByNameWithoutErr(name)
	return ok && len(allSupportedFunctions[id].valueArgs) > 0
}

// IsNumericValueArgument reports whether position is a registered numeric
// value operand. It deliberately excludes control parameters such as the
// precision argument to ROUND and TRUNCATE.
func IsNumericValueArgument(name string, position int) bool {
	id, ok := getFunctionIdByNameWithoutErr(name)
	if !ok || position < 0 {
		return false
	}
	for _, valuePosition := range allSupportedFunctions[id].valueArgs {
		if valuePosition == position {
			return true
		}
	}
	return false
}

// NumericValueKindArgumentPositions returns the registered string operands
// whose owner consumes selected numeric-literal provenance as an explicit
// BOOL input. The returned slice is a copy so callers cannot mutate registry
// metadata.
func NumericValueKindArgumentPositions(name string) ([]int, int) {
	id, ok := getFunctionIdByNameWithoutErr(name)
	if !ok {
		return nil, 0
	}
	metadata := allSupportedFunctions[id]
	return append([]int(nil), metadata.sourceKindArgs...), metadata.sourceKindArity
}

// PreservesDecimalValueType reports whether a registered function keeps a
// DECIMAL value in the decimal result domain. The overload resolver still
// determines the result's precision and scale.
func PreservesDecimalValueType(name string) bool {
	id, ok := getFunctionIdByNameWithoutErr(name)
	return ok && allSupportedFunctions[id].preserveDecimalType
}

// HasGenericNumericValueContext reports whether a registered function binds
// its value operands in the generic numeric context.
func HasGenericNumericValueContext(name string) bool {
	id, ok := getFunctionIdByNameWithoutErr(name)
	return ok && allSupportedFunctions[id].numContext
}
