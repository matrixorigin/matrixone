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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestDecimal128ArithmeticWidensToDecimal256(t *testing.T) {
	for _, test := range []struct {
		name     string
		operator string
		input    types.Type
		want     types.Type
	}{
		{name: "add 38 digits", operator: "+", input: types.New(types.T_decimal128, 38, 0), want: types.New(types.T_decimal256, 39, 0)},
		{name: "subtract scaled 38 digits", operator: "-", input: types.New(types.T_decimal128, 38, 18), want: types.New(types.T_decimal256, 39, 18)},
		{name: "multiply 20 digits", operator: "*", input: types.New(types.T_decimal128, 20, 0), want: types.New(types.T_decimal256, 40, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), test.operator, []types.Type{test.input, test.input})
			require.NoError(t, err)
			require.True(t, resolved.needCast)
			require.Equal(t, []types.Type{
				types.New(types.T_decimal256, test.input.Width, test.input.Scale),
				types.New(types.T_decimal256, test.input.Width, test.input.Scale),
			}, resolved.targetTypes)
			require.Equal(t, test.want, resolved.retType)
		})
	}
}

func TestDecimal128ArithmeticKeepsExistingFastPathWhenResultFits(t *testing.T) {
	input := types.New(types.T_decimal128, 19, 0)
	resolved, err := GetFunctionByName(context.Background(), "*", []types.Type{input, input})
	require.NoError(t, err)
	require.NotEqual(t, types.T_decimal256, resolved.targetTypes[0].Oid)
	require.Equal(t, types.New(types.T_decimal128, 38, 0), resolved.retType)
}
