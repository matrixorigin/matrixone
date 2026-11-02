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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func BenchmarkBuiltInConcat(b *testing.B) {
	const rows = 8192
	for _, scenario := range []struct {
		name  string
		parts []string
	}{
		{name: "SingleArg", parts: []string{"Brand#12"}},
		{name: "TinyTwoArgs", parts: []string{"a", "b"}},
		{name: "AllEmpty", parts: []string{"", ""}},
		{name: "ShortTwoArgs", parts: []string{"Brand#12", "SM BOX"}},
		{name: "ThreeArgsLong", parts: []string{strings.Repeat("brand", 26), "#", strings.Repeat("container", 24)}},
		{name: "ManyArgs8", parts: []string{
			"0123456789abcdef", "fedcba9876543210", "0123456789abcdef", "fedcba9876543210",
			"0123456789abcdef", "fedcba9876543210", "0123456789abcdef", "fedcba9876543210",
		}},
	} {
		for _, withNulls := range []bool{false, true} {
			name := scenario.name + "/NoNulls"
			if withNulls {
				name = scenario.name + "/Nulls"
			}
			b.Run(name, func(b *testing.B) {
				inputs := make([]FunctionTestInput, len(scenario.parts))
				for arg, part := range scenario.parts {
					values := make([]string, rows)
					var nulls []bool
					if withNulls && arg == len(scenario.parts)-1 {
						nulls = make([]bool, rows)
					}
					for row := range values {
						values[row] = part
						if nulls != nil {
							nulls[row] = row%8 == 0
						}
					}
					inputs[arg] = NewFunctionTestInput(types.T_varchar.ToType(), values, nulls)
				}
				want := strings.Join(scenario.parts, "")
				wanted := make([]string, rows)
				var nulls []bool
				if withNulls {
					nulls = make([]bool, rows)
				}
				var outputBytes int64
				for row := range wanted {
					if withNulls && row%8 == 0 {
						nulls[row] = true
					} else {
						wanted[row] = want
						outputBytes += int64(len(want))
					}
				}
				proc := testutil.NewProcess(b)
				fc := NewFunctionTestCase(proc, inputs,
					NewFunctionTestResult(types.T_varchar.ToType(), false, wanted, nulls), builtInConcat)
				defer fc.Free()
				b.SetBytes(outputBytes)
				fc.Benchmark(b)
			})
		}
	}
}
