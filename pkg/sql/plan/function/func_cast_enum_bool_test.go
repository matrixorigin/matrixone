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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// ENUM ordinals follow MySQL numeric truth in boolean contexts
// (AND/OR/NOT/XOR/CASE WHEN/WHERE): a nonzero ordinal is TRUE, ordinal 0 —
// including the invalid-member sentinel — is FALSE, and SQL NULL stays NULL.
// The display label must never be re-parsed as text for this contract.
func TestCastEnumToBool(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	enumType := types.T_enum.ToType()
	values := []types.Enum{0, 1, 2, 1846}
	nulls := []bool{false, false, false, true}
	expected := []bool{false, true, true, false}

	require.True(t, IfTypeCastSupported(enumType.Oid, types.T_bool))
	_, err := GetFunctionByName(context.Background(), "cast", []types.Type{enumType, types.T_bool.ToType()})
	require.NoError(t, err)

	inputs := []FunctionTestInput{
		NewFunctionTestInput(enumType, values, nulls),
		NewFunctionTestInput(types.T_bool.ToType(), []bool{}, []bool{}),
	}
	tc := NewFunctionTestCase(proc, inputs,
		NewFunctionTestResult(types.T_bool.ToType(), false, expected, nulls), NewCast)
	succeed, info := tc.RunAndFree()
	require.True(t, succeed, info)
}
