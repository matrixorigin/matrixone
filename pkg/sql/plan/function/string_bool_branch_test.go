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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// MySQL: IF(1, 'x', TRUE) is 'x' and IF(0, 'x', TRUE) is '1'. A text branch
// next to a BOOL branch makes the result a string; the text must not be cast
// to BOOL ("'x' is not a valid bool expression").
func TestStringBoolValueBranchesResolveToVarchar(t *testing.T) {
	ctx := context.Background()
	boolType := types.T_bool.ToType()
	for _, str := range []types.Type{types.T_varchar.ToType(), types.T_char.ToType(), types.T_text.ToType()} {
		for _, tc := range []struct {
			name      string
			args      []types.Type
			valueArgs []int
		}{
			{"iff", []types.Type{boolType, str, boolType}, []int{1, 2}},
			{"iff", []types.Type{boolType, boolType, str}, []int{1, 2}},
			{"case", []types.Type{boolType, str, boolType}, []int{1, 2}},
			{"case", []types.Type{boolType, boolType, boolType, str}, []int{1, 3}},
			{"coalesce", []types.Type{boolType, str}, []int{0, 1}},
			{"coalesce", []types.Type{str, boolType}, []int{0, 1}},
		} {
			resolved, err := GetFunctionByName(ctx, tc.name, tc.args)
			require.NoError(t, err, "%s %v", tc.name, tc.args)
			require.True(t, resolved.GetReturnType().Oid.IsMySQLString(), "%s %v -> %s", tc.name, tc.args, resolved.GetReturnType().Oid)
			require.True(t, resolved.needCast, "%s %v", tc.name, tc.args)
			for _, i := range tc.valueArgs {
				require.True(t, resolved.targetTypes[i].Oid.IsMySQLString(), "%s %v arg %d", tc.name, tc.args, i)
			}
		}
	}
}

// BLOB/BINARY next to BOOL keeps its previous (binary) resolution.
func TestMixedStringNumericToVarcharSkipsBinaryBool(t *testing.T) {
	boolType := types.T_bool.ToType()
	for _, bin := range []types.T{types.T_blob, types.T_binary, types.T_varbinary} {
		_, ok := mixedStringNumericToVarchar([]types.Type{bin.ToType(), boolType})
		require.False(t, ok, "%s", bin)
	}
	ret, ok := mixedStringNumericToVarchar([]types.Type{types.T_varchar.ToType(), boolType})
	require.True(t, ok)
	require.Equal(t, types.T_varchar, ret.Oid)
}

// A TEXT-family branch next to BOOL resolves to TEXT, not VARCHAR(65535), so
// a MEDIUMTEXT/LONGTEXT value longer than 65,535 characters is not truncated.
func TestBoolTextBranchesKeepTextDomain(t *testing.T) {
	ctx := context.Background()
	boolType := types.T_bool.ToType()
	for _, width := range []int32{0, types.MaxMediumTextLen, types.MaxLongTextLen} {
		text := types.New(types.T_text, width, 0)
		for _, tc := range []struct {
			name      string
			args      []types.Type
			valueArgs []int
		}{
			{"iff", []types.Type{boolType, text, boolType}, []int{1, 2}},
			{"case", []types.Type{boolType, boolType, boolType, text}, []int{1, 3}},
			{"coalesce", []types.Type{text, boolType}, []int{0, 1}},
		} {
			resolved, err := GetFunctionByName(ctx, tc.name, tc.args)
			require.NoError(t, err, "%s %v", tc.name, tc.args)
			require.Equal(t, types.T_text, resolved.GetReturnType().Oid, "%s %v", tc.name, tc.args)
			for _, i := range tc.valueArgs {
				require.Equal(t, types.T_text, resolved.targetTypes[i].Oid, "%s %v arg %d", tc.name, tc.args, i)
			}
		}
	}
}
