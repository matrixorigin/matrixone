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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCRC32AcceptsScalarArgumentsWithoutChangingStringDomains(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, tc := range []struct {
		name string
		typ  types.Type
	}{
		{name: "bool", typ: types.T_bool.ToType()},
		{name: "signed integer", typ: types.T_int64.ToType()},
		{name: "unsigned integer", typ: types.T_uint64.ToType()},
		{name: "decimal", typ: types.New(types.T_decimal64, 8, 2)},
		{name: "double", typ: types.T_float64.ToType()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(proc.Ctx, "crc32", []types.Type{tc.typ})
			require.NoError(t, err)
			require.True(t, resolved.needCast)
			require.Len(t, resolved.targetTypes, 1)
			require.Equal(t, types.T_varchar, resolved.targetTypes[0].Oid)
		})
	}

	for _, typ := range []types.T{
		types.T_char,
		types.T_varchar,
		types.T_text,
		types.T_binary,
		types.T_varbinary,
		types.T_blob,
		types.T_json,
		types.T_array_float32,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			resolved, err := GetFunctionByName(proc.Ctx, "crc32", []types.Type{typ.ToType()})
			require.NoError(t, err)
			require.False(t, resolved.needCast)
		})
	}
}
