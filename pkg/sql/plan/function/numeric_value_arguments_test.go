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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegisteredNumericValueArguments(t *testing.T) {
	for _, name := range []string{
		"abs", "ceil", "ceiling", "floor", "round", "truncate", "sign",
		"sqrt", "power", "pow", "exp", "ln", "log", "log2", "log10",
		"acos", "asin", "atan", "atan2", "cos", "cot", "degrees", "radians",
		"sin", "sinh", "tan", "tanh", "cosh", "asinh", "acosh", "atanh", "mod",
	} {
		require.True(t, HasNumericValueArguments(name), "%s must declare its numeric value operands", name)
		if name != "mod" {
			require.True(t, HasGenericNumericValueContext(name), "%s uses the generic numeric value context", name)
		}
	}
	require.False(t, HasGenericNumericValueContext("mod"), "MOD retains its operator-specific numeric context")

	for _, name := range []string{"ceil", "ceiling", "floor", "abs", "sign", "sqrt", "exp", "ln", "log2", "sin", "sinh"} {
		require.True(t, IsNumericValueArgument(name, 0), "%s argument 0 is a value operand", name)
	}
	for _, name := range []string{"power", "pow", "log", "atan", "atan2", "mod"} {
		require.True(t, IsNumericValueArgument(name, 0), "%s argument 0 is a value operand", name)
		require.True(t, IsNumericValueArgument(name, 1), "%s argument 1 is a value operand", name)
	}
	for _, name := range []string{"round", "truncate"} {
		require.False(t, IsNumericValueArgument(name, 1), "%s precision is a control argument", name)
	}
	for _, name := range []string{"length", "field", "pi", "does_not_exist"} {
		require.False(t, HasNumericValueArguments(name), "%s must not inherit a numeric role from its result type", name)
	}
}

func TestNumericValueTypePreservationMetadata(t *testing.T) {
	for _, name := range []string{"abs", "ceil", "ceiling", "floor", "round", "truncate"} {
		require.True(t, PreservesDecimalValueType(name), "%s returns in the decimal value domain", name)
	}
	for _, name := range []string{"sqrt", "sign", "power", "sin", "length"} {
		require.False(t, PreservesDecimalValueType(name), "%s does not preserve its first argument's decimal type", name)
	}
}
