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

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestJsonParseExact(t *testing.T) {
	for _, typ := range []types.T{types.T_varchar, types.T_text, types.T_char, types.T_blob, types.T_any} {
		f, err := GetFunctionByName(context.Background(), "json_parse_exact", []types.Type{typ.ToType()})
		require.NoError(t, err)
		require.Equal(t, int32(JSON_PARSE_EXACT), f.fid)
		require.Equal(t, types.T_json, f.retType.Oid)
	}
	for _, args := range [][]types.Type{nil, {types.T_float64.ToType()}, {types.T_json.ToType()}, {types.T_varchar.ToType(), types.T_varchar.ToType()}} {
		_, err := GetFunctionByName(context.Background(), "json_parse_exact", args)
		require.Error(t, err)
	}
	proc := testutil.NewProcess(t)
	for _, constant := range []bool{false, true} {
		input := NewFunctionTestInput(types.T_varchar.ToType(), []string{"9007199254740993.001", "9007199254740993.001"}, nil)
		if constant {
			input = NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"9007199254740993.001", "9007199254740993.001"}, nil)
		}
		bj, err := bytejson.ParseFromStringExact("9007199254740993.001")
		require.NoError(t, err)
		raw, err := types.EncodeJson(bj)
		require.NoError(t, err)
		fc := NewFunctionTestCase(proc, []FunctionTestInput{input},
			NewFunctionTestResult(types.T_json.ToType(), false, []string{string(raw), string(raw)}, nil), JsonParseExact)
		ok, info := fc.RunAndFree()
		require.True(t, ok, info)
	}
	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{"bad", "1.000000000000000001", "bad"}, []bool{false, false, true})},
		types.T_json.ToType(), JsonParseExact,
		&FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, true}})
	require.True(t, vec.IsNull(0))
	require.True(t, vec.IsNull(2))
	require.Equal(t, "1.000000000000000001", types.DecodeJson(vec.GetBytesAt(1)).String())
	fc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{"bad"}, nil)},
		NewFunctionTestResult(types.T_json.ToType(), true, nil, nil), JsonParseExact)
	defer fc.Free()
	ok, _ := fc.Run()
	require.True(t, ok)
}

// Pin the opt-in parsing boundary against the public explicit CAST executor.
// Existing CAST callers keep their float semantics; exact parsing must not be
// implemented as an alias, nor should it silently change those callers.
func TestJsonParseExactDiffersFromExplicitCast(t *testing.T) {
	for _, tc := range []struct {
		input, castText     string
		exactType, castType bytejson.TpCode
	}{
		{"1.000000000000000001", "1", bytejson.TpCodeDecimal, bytejson.TpCodeFloat64},
		{"9007199254740993.001", "9007199254740994", bytejson.TpCodeDecimal, bytejson.TpCodeFloat64},
		{"1e3", "1000", bytejson.TpCodeDecimal, bytejson.TpCodeFloat64},
		// Fitting integers already remain exact under CAST, even above 2^53.
		{"9007199254740993", "9007199254740993", bytejson.TpCodeInt64, bytejson.TpCodeInt64},
	} {
		t.Run(tc.input, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			input := NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.input}, nil)
			exact := runJsonFunctionWithSelectList(t, proc, []FunctionTestInput{input},
				types.T_json.ToType(), JsonParseExact, nil)
			cast := runJsonFunctionWithSelectList(t, proc, []FunctionTestInput{input,
				NewFunctionTestInput(types.T_json.ToType(), []string{}, nil)},
				types.T_json.ToType(), NewExplicitCast, nil)
			exactJSON, castJSON := types.DecodeJson(exact.GetBytesAt(0)), types.DecodeJson(cast.GetBytesAt(0))
			require.Equal(t, tc.exactType, exactJSON.Type)
			require.Equal(t, tc.input, exactJSON.String())
			require.Equal(t, tc.castType, castJSON.Type)
			require.Equal(t, tc.castText, castJSON.String())
		})
	}
}
