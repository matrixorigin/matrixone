// Copyright 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// An exact JSON INT64/UINT64 casts to DECIMAL through its digits, not through
// float64 (issue #31669): 9007199254740993 must not become 9007199254740992.
func TestJSONIntegerToDecimalIsExact(t *testing.T) {
	proc := testutil.NewProcess(t)
	encode := func(s string) []byte {
		bj, err := types.ParseStringToByteJson(s)
		require.NoError(t, err)
		enc, err := types.EncodeJson(bj)
		require.NoError(t, err)
		return enc
	}
	run := func(t *testing.T, doc string, typ types.Type) (string, error) {
		vec := vector.NewVec(types.T_json.ToType())
		defer vec.Free(proc.Mp())
		require.NoError(t, vector.AppendBytes(vec, encode(doc), false, proc.Mp()))
		res := vector.NewFunctionResultWrapper(typ, proc.Mp())
		defer res.Free()
		require.NoError(t, res.PreExtendAndReset(1))
		if err := jsonToNumeric(proc.Ctx, vector.GenerateFunctionStrParameter(vec), res, 1, typ); err != nil {
			return "", err
		}
		out := res.GetResultVector()
		if typ.Oid == types.T_decimal64 {
			return vector.GetFixedAtWithTypeCheck[types.Decimal64](out, 0).Format(typ.Scale), nil
		}
		return vector.GetFixedAtWithTypeCheck[types.Decimal128](out, 0).Format(typ.Scale), nil
	}
	d38 := types.New(types.T_decimal128, 38, 0)
	d18 := types.New(types.T_decimal64, 18, 2)
	for _, tc := range []struct {
		doc  string
		typ  types.Type
		want string
	}{
		{"9007199254740993", d38, "9007199254740993"},
		{"9007199254740995", d38, "9007199254740995"},
		{"-9007199254740993", d38, "-9007199254740993"},
		{"9223372036854775807", d38, "9223372036854775807"},
		{"18446744073709551615", d38, "18446744073709551615"},
		{"9007199254740993", types.New(types.T_decimal128, 30, 4), "9007199254740993.0000"},
		{"9007199254740993", types.New(types.T_decimal64, 18, 0), "9007199254740993"},
		{"12", d18, "12.00"},
		{"1.25", d18, "1.25"},
	} {
		got, err := run(t, tc.doc, tc.typ)
		require.NoError(t, err, tc.doc)
		require.Equal(t, tc.want, got, "%s -> %s", tc.doc, tc.typ)
	}
	// An integer the target cannot hold still fails like before.
	_, err := run(t, "9007199254740993", types.New(types.T_decimal64, 5, 0))
	require.Error(t, err)
}
