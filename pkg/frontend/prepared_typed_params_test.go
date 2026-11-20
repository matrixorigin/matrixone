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

package frontend

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestTypedPrepareParamParsing(t *testing.T) {
	proc := testutil.NewProcess(t)
	parse := func(typ types.Type, text string) (*vector.Vector, bool) {
		t.Helper()
		vec, ok, err := typedPrepareParam(proc, typ, text)
		require.NoError(t, err)
		if ok {
			t.Cleanup(func() { vec.Free(proc.Mp()) })
			require.True(t, vec.IsConst())
			require.Equal(t, 1, vec.Length())
			require.Equal(t, typ.Oid, vec.GetType().Oid)
		}
		return vec, ok
	}

	t.Run("integers", func(t *testing.T) {
		vec, ok := parse(types.T_int8.ToType(), "-128")
		require.True(t, ok)
		require.Equal(t, int8(-128), vector.GetFixedAtNoTypeCheck[int8](vec, 0))
		vec, ok = parse(types.T_uint64.ToType(), "18446744073709551615")
		require.True(t, ok)
		require.Equal(t, uint64(math.MaxUint64), vector.GetFixedAtNoTypeCheck[uint64](vec, 0))
		for _, bad := range []string{"128", "1.5", " 1", "", "0x1"} {
			_, ok = parse(types.T_int8.ToType(), bad)
			require.False(t, ok, bad)
		}
		_, ok = parse(types.T_uint32.ToType(), "-1")
		require.False(t, ok)
	})

	t.Run("floats keep non-finite values", func(t *testing.T) {
		vec, ok := parse(types.T_float64.ToType(), "NaN")
		require.True(t, ok)
		require.True(t, math.IsNaN(vector.GetFixedAtNoTypeCheck[float64](vec, 0)))
		vec, ok = parse(types.T_float64.ToType(), "-Inf")
		require.True(t, ok)
		require.True(t, math.IsInf(vector.GetFixedAtNoTypeCheck[float64](vec, 0), -1))
		vec, ok = parse(types.T_float32.ToType(), "1.5")
		require.True(t, ok)
		require.Equal(t, float32(1.5), vector.GetFixedAtNoTypeCheck[float32](vec, 0))
		_, ok = parse(types.T_float64.ToType(), "1.5abc")
		require.False(t, ok)
	})

	t.Run("bool", func(t *testing.T) {
		for text, want := range map[string]bool{"true": true, "1": true, "false": false, "0": false} {
			vec, ok := parse(types.T_bool.ToType(), text)
			require.True(t, ok, text)
			require.Equal(t, want, vector.GetFixedAtNoTypeCheck[bool](vec, 0))
		}
		_, ok := parse(types.T_bool.ToType(), "2")
		require.False(t, ok)
	})

	t.Run("decimals keep their declared scale", func(t *testing.T) {
		typ := types.New(types.T_decimal128, 20, 3)
		vec, ok := parse(typ, "-1.250")
		require.True(t, ok)
		require.Equal(t, int32(3), vec.GetType().Scale)
		want, err := types.ParseDecimal128("-1.250", 20, 3)
		require.NoError(t, err)
		require.Equal(t, want, vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, 0))
		vec, ok = parse(types.New(types.T_decimal64, 10, 2), "3.10")
		require.True(t, ok)
		require.Equal(t, int32(2), vec.GetType().Scale)
	})

	t.Run("strings and unsupported types stay on TEXT", func(t *testing.T) {
		for _, typ := range []types.Type{
			types.T_varchar.ToType(), types.T_text.ToType(), types.T_datetime.ToType(), types.T_json.ToType(),
		} {
			_, ok := parse(typ, "2026-01-01")
			require.False(t, ok, typ.String())
		}
	})
}

func TestInstallTypedPrepareParams(t *testing.T) {
	proc := testutil.NewProcess(t)
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("7"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(params, []byte("abc"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(params, nil, true, proc.Mp()))
	require.NoError(t, vector.AppendBytes(params, []byte("2.5"), false, proc.Mp()))
	proc.SetOwnedPrepareParamsWithIsBin(params, nil)
	t.Cleanup(func() { proc.SetPrepareParams(nil) })

	bindings := []plan2.PreparedSourceBinding{
		{Position: 0, Type: types.T_int64.ToType()},
		{Position: 1, Type: types.T_varchar.ToType()},
		{Position: 2, Type: types.T_int64.ToType()},
		{Position: 3, Type: types.T_float64.ToType()},
	}
	require.NoError(t, installTypedPrepareParams(proc, bindings))
	require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](proc.GetTypedPrepareParam(0), 0))
	require.Nil(t, proc.GetTypedPrepareParam(1), "string binding keeps the TEXT transport")
	require.Nil(t, proc.GetTypedPrepareParam(2), "NULL keeps the TEXT transport")
	require.Equal(t, 2.5, vector.GetFixedAtNoTypeCheck[float64](proc.GetTypedPrepareParam(3), 0))

	// Nothing typed: no batch is installed.
	proc.SetOwnedPrepareParamsWithIsBin(params, nil)
	require.NoError(t, installTypedPrepareParams(proc, bindings[1:3]))
	require.Nil(t, proc.GetTypedPrepareParams())
}
