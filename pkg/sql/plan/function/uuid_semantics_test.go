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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestUUIDCommonDomains(t *testing.T) {
	proc := testutil.NewProcess(t)
	u := types.T_uuid.ToType()
	for _, text := range []types.Type{
		types.NewWithCharset(types.T_varchar, 8, 0, types.CharsetUTF8MB4Bin),
		types.NewWithCharset(types.T_char, 36, 0, types.CharsetUTF8),
		types.NewWithCharset(types.T_char, 8, 0, types.CharsetUTF8),
		types.NewWithCharset(types.T_varchar, 37, 0, types.CharsetUTF8),
		types.New(types.T_text, 0, 0), types.New(types.T_text, types.MaxTinyTextLen, 0),
	} {
		wantWidth := text.Width
		if text.Oid != types.T_text && wantWidth < 36 {
			wantWidth = 36
		}
		for _, values := range [][]types.Type{{u, text}, {text, u}, {u, text, types.T_any.ToType()}} {
			for _, name := range []string{"case", "if", "coalesce", "greatest", "least"} {
				if name == "if" && len(values) == 3 {
					continue
				}
				inputs := values
				if name == "case" {
					inputs = []types.Type{types.T_bool.ToType(), values[0], values[1]}
					if len(values) == 3 {
						inputs = []types.Type{types.T_bool.ToType(), values[0], types.T_bool.ToType(), values[1], values[2]}
					}
				}
				if name == "if" {
					inputs = []types.Type{types.T_bool.ToType(), values[0], values[1]}
				}
				result, err := GetFunctionByName(proc.Ctx, name, inputs)
				require.NoError(t, err, "%s %v", name, inputs)
				require.Equal(t, wantWidth, result.GetReturnType().Width, "%s %v", name, inputs)
				require.Equal(t, text.Charset, result.GetReturnType().Charset, "%s %v", name, inputs)
				casts, _ := result.ShouldDoImplicitTypeCast()
				for i, cast := range casts {
					if name == "case" && i%2 == 0 && i < len(casts)-1 || name == "if" && i == 0 {
						continue
					}
					require.Equal(t, wantWidth, cast.Width, name)
				}
			}
		}
	}
	for _, name := range []string{"coalesce", "greatest", "least", "=", "<=>", "<", ">="} {
		result, err := GetFunctionByName(proc.Ctx, name, []types.Type{u, types.T_any.ToType()})
		require.NoError(t, err, name)
		if name == "coalesce" || name == "greatest" || name == "least" {
			require.Equal(t, types.T_uuid, result.GetReturnType().Oid, name)
		}
		casts, _ := result.ShouldDoImplicitTypeCast()
		for _, cast := range casts {
			require.Equal(t, types.T_uuid, cast.Oid, name)
		}
	}
	for _, name := range []string{"case", "if"} {
		for _, values := range [][]types.Type{{u, types.T_any.ToType()}, {types.T_any.ToType(), u}, {u, u}} {
			inputs := append([]types.Type{types.T_bool.ToType()}, values...)
			result, err := GetFunctionByName(proc.Ctx, name, inputs)
			require.NoError(t, err)
			require.Equal(t, types.T_uuid, result.GetReturnType().Oid, name)
		}
	}
	_, err := GetFunctionByName(proc.Ctx, "+", []types.Type{u, types.T_any.ToType()})
	require.Error(t, err)
	for _, inputs := range [][]types.Type{{u, types.T_varchar.ToType()}, {types.T_any.ToType(), u}} {
		result, err := GetFunctionByName(proc.Ctx, "=", inputs)
		require.NoError(t, err)
		casts, _ := result.ShouldDoImplicitTypeCast()
		for _, cast := range casts {
			require.Equal(t, types.T_uuid, cast.Oid)
		}
	}
}
