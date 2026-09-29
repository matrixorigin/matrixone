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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestParseJSONValueDateRejectsTimeComponent(t *testing.T) {
	for _, input := range []string{
		"2024-01-02 12:34:56",
		"2024-01-02T12:34:56",
	} {
		_, err := parseJSONValueDate(jsonValueExtracted{text: input}, types.T_date.ToType())
		require.Error(t, err, input)
	}

	got, err := parseJSONValueDate(jsonValueExtracted{text: "2024-01-02"}, types.T_date.ToType())
	require.NoError(t, err)
	require.Equal(t, "2024-01-02", got.String())
}

// Keep complete validation, admitted extraction and vector execution distinct:
// the production executor uses the admitted path but still validates descendants.
func BenchmarkJSONValueStoredLargeDocumentSmallPath(b *testing.B) {
	for _, count := range []int{16, 256, 4096} {
		b.Run(fmt.Sprintf("elements=%d", count), func(b *testing.B) {
			document, err := types.ParseStringToByteJson(`{"keep":1,"large":[` + strings.Repeat("1,", count-1) + "1]}")
			require.NoError(b, err)
			stored, err := document.Marshal()
			require.NoError(b, err)
			// Admission is setup, outside every measured loop.
			_, err = decodeJSONValueStored(stored)
			require.NoError(b, err)
			path := []byte("$.keep")
			for _, mode := range []string{"validated", "admitted", "vector"} {
				b.Run(mode, func(b *testing.B) {
					var fc FunctionTestCase
					if mode == "vector" {
						proc := testutil.NewProcess(b)
						fc = NewFunctionTestCase(proc, []FunctionTestInput{
							NewFunctionTestInput(types.T_json.ToType(), []string{string(stored)}, nil),
							NewFunctionTestConstInput(types.T_varchar.ToType(), []string{string(path)}, nil),
							NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{true}),
							NewFunctionTestConstInput(types.T_int64.ToType(), []int64{jsonValueErrorResponse}, nil),
							NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{true}),
							NewFunctionTestConstInput(types.T_int64.ToType(), []int64{jsonValueErrorResponse}, nil),
							NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{true}),
						}, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil), JsonValue)
						b.Cleanup(func() {
							fc.result.Free()
							for _, v := range fc.parameters {
								v.Free(proc.Mp())
							}
						})
						require.NoError(b, fc.result.PreExtendAndReset(1))
					}
					b.ReportAllocs()
					b.ResetTimer()
					b.ReportMetric(float64(len(stored)), "bytes/document")
					for i := 0; i < b.N; i++ {
						if mode == "vector" {
							if err := fc.result.PreExtendAndReset(1); err != nil {
								b.Fatal(err)
							}
							if err := JsonValue(fc.parameters, fc.result, fc.proc, 1, nil); err != nil {
								b.Fatal(err)
							}
							v := fc.result.GetResultVector()
							if v.IsNull(0) || vector.MustFixedColWithTypeCheck[int64](v)[0] != 1 {
								b.Fatal("wrong vector result")
							}
							continue
						}
						var extracted jsonValueExtracted
						if mode == "validated" {
							extracted = jsonValueExtract(stored, path, types.T_json)
						} else {
							extracted = jsonValueExtractAdmitted(stored, path, types.T_json)
						}
						if extracted.state != jsonValueOneValue || extracted.text != "1" {
							b.Fatalf("wrong extraction: %v", extracted.state)
						}
					}
				})
			}
		})
	}
}
