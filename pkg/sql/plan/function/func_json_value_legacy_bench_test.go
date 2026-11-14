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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Unlike the clause-bearing benchmark, this calls the public executor with
// exactly two arguments. JSON input is an admitted column; VARCHAR is the
// text parsing control. Admission is timed separately by the vector benchmark.
func BenchmarkJSONValueLegacySmallPath(b *testing.B) {
	for _, count := range []int{16, 4096} {
		b.Run(fmt.Sprintf("elements=%d", count), func(b *testing.B) {
			text := `{"keep":1,"large":[` + strings.Repeat("1,", count-1) + "1]}"
			document, err := types.ParseStringToByteJson(text)
			require.NoError(b, err)
			stored, err := document.Marshal()
			require.NoError(b, err)
			for _, inputType := range []types.T{types.T_json, types.T_varchar} {
				b.Run(inputType.String(), func(b *testing.B) {
					input := text
					if inputType == types.T_json {
						input = string(stored)
					}
					proc := testutil.NewProcess(b)
					fc := NewFunctionTestCase(proc, []FunctionTestInput{
						NewFunctionTestInput(inputType.ToType(), []string{input}, nil),
						NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"$.keep"}, nil),
					}, NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"1"}, nil), JsonValue)
					b.Cleanup(func() {
						fc.result.Free()
						for _, v := range fc.parameters {
							v.Free(proc.Mp())
						}
					})
					step := func() error {
						if err := fc.result.PreExtendAndReset(1); err != nil {
							return err
						}
						return JsonValue(fc.parameters, fc.result, proc, 1, nil)
					}
					check := func() {
						v := fc.result.GetResultVector()
						require.Equal(b, 1, v.Length())
						require.False(b, v.IsNull(0))
						require.Equal(b, "1", string(v.GetBytesAt(0)))
					}
					require.NoError(b, step())
					check()
					b.ReportAllocs()
					b.ResetTimer()
					b.ReportMetric(float64(len(input)), "bytes/document")
					for range b.N {
						if err := step(); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					check()
				})
			}
		})
	}
}
