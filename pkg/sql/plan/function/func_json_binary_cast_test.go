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
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestJSONBinaryCastPreservesOpaqueSubtype(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, existed := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if existed {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
	for _, tc := range []struct {
		oid     types.T
		subtype int
	}{{types.T_binary, 254}, {types.T_varbinary, 15}, {types.T_blob, 252}} {
		for _, cast := range []struct {
			name string
			fn   executeLogicOfOverload
		}{{"implicit", NewCast}, {"explicit", NewExplicitCast}, {"assignment", NewAssignCast}} {
			t.Run(fmt.Sprintf("%s/%s", tc.oid, cast.name), func(t *testing.T) {
				// Fixed bytes and independent visible MySQL subtype oracle; do not use
				// the production FromBinary helper to construct expected values.
				expected := make([]string, 0, 4)
				for _, payload := range []string{"e30=", "AP8=", "", ""} {
					expected = append(expected, encodeJSONCastValue(t, newTypedByteJson(bytejson.TpCodeBlob, fmt.Sprintf("base64:type%d:%s", tc.subtype, payload))))
				}
				nulls := []bool{false, false, false, true}
				test := NewFunctionTestCase(proc, []FunctionTestInput{
					NewFunctionTestInput(tc.oid.ToType(), []string{"{}", "\x00\xff", "", ""}, nulls),
					NewFunctionTestInput(types.T_json.ToType(), []string{}, nil),
				}, NewFunctionTestResult(types.T_json.ToType(), false, expected, nulls), cast.fn)
				ok, info := test.RunAndFree()
				require.True(t, ok, info)
			})
		}
	}
}

func TestJSONOpaqueObjectKeyUsesJSONVisibleText(t *testing.T) {
	proc := testutil.NewProcess(t)
	// Existing stored compact tagged JSON, independent of the CAST producer.
	compact := "base64:type252:" + strings.Repeat("YWFh", 19) + "YQ=="
	input := encodeJSONCastValue(t, newTypedByteJson(bytejson.TpCodeBlob, compact))
	vec := runJsonFunctionWithSelectList(t, proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_json.ToType(), []string{input}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
	}, types.T_json.ToType(), newOpBuiltInJsonObject().jsonObject, nil)
	got := jsonVectorRowString(t, vec, 0)
	var object map[string]int
	require.NoError(t, json.Unmarshal([]byte(got), &object))
	require.Equal(t, map[string]int{`"base64:type252:` + strings.Repeat("YWFh", 19) + `\nYQ=="`: 1}, object)
}

func TestJSONBinaryCastRepresentationAdmissionAndMaskedNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion51)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion51)
	inputs := []FunctionTestInput{NewFunctionTestInput(types.T_blob.ToType(), []string{"bad"}, nil), NewFunctionTestInput(types.T_json.ToType(), []string{}, nil)}
	test := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(types.T_json.ToType(), true, nil, nil), NewCast)
	ok, info := test.RunAndFree()
	require.True(t, ok, info)
	for _, allNull := range []bool{false, true} {
		input := []FunctionTestInput{NewFunctionTestInput(types.T_blob.ToType(), []string{"bad"}, []bool{allNull}), NewFunctionTestInput(types.T_json.ToType(), []string{}, nil)}
		selection := (*FunctionSelectList)(nil)
		if !allNull {
			selection = &FunctionSelectList{AnyNull: true, AllNull: true}
		}
		vec := runJsonFunctionWithSelectList(t, proc, input, types.T_json.ToType(), NewCast, selection)
		require.True(t, vec.IsNull(0))
	}
}
