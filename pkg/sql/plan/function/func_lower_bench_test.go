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
	"bytes"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func BenchmarkBuiltInToLower(b *testing.B) {
	original := func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selected *FunctionSelectList) error {
		return opUnaryBytesToBytesByStringDomain(parameters, result, proc, length, bytes.ToLower, func(value []byte) []byte { return value }, selected)
	}
	boundary := strings.Repeat("AbC ", (64<<10)/4+1)
	for _, scenario := range []struct {
		name     string
		rows     int
		values   []string
		alsoCold bool
	}{
		{"Lower64", 8192, []string{strings.Repeat("abc ", 16)}, true},
		{"Mixed64", 8192, []string{strings.Repeat("AbC ", 16)}, true},
		{"Upper64", 8192, []string{strings.Repeat("ABC ", 16)}, true},
		{"Lower1KiB", 8192, []string{strings.Repeat("abc ", 256)}, true},
		{"Mixed1KiB", 8192, []string{strings.Repeat("AbC ", 256)}, true},
		{"Upper1KiB", 8192, []string{strings.Repeat("ABC ", 256)}, true},
		{"Unicode", 8192, []string{strings.Repeat("ȺİKÄ中😀", 16)}, false},
		{"CapMinus1", 4, []string{boundary[:(64<<10)-1]}, false},
		{"Cap", 4, []string{boundary[:64<<10]}, false},
		{"OverCap", 4, []string{boundary[:(64<<10)+1]}, false},
		{"Mixed64Rows1", 1, []string{strings.Repeat("AbC ", 16)}, true},
		{"Mixed64Rows4", 4, []string{strings.Repeat("AbC ", 16)}, true},
		{"Alternating", 8, []string{"A", boundary[:64<<10], "already lower", "Ä中", boundary[:(64<<10)+1], ""}, true},
	} {
		for _, implementation := range []struct {
			name string
			fn   executeLogicOfOverload
		}{{"Original", original}, {"Candidate", builtInToLower}} {
			modes := []bool{false}
			if scenario.alsoCold {
				modes = append(modes, true)
			}
			for _, cold := range modes {
				mode := "WarmResult"
				if cold {
					mode = "ColdResult"
				}
				b.Run(scenario.name+"/"+mode+"/"+implementation.name, func(b *testing.B) {
					values, wanted := make([]string, scenario.rows), make([]string, scenario.rows)
					var outputBytes int64
					for row := range values {
						values[row] = scenario.values[row%len(scenario.values)]
						wanted[row] = string(bytes.ToLower([]byte(values[row])))
						outputBytes += int64(len(wanted[row]))
					}
					proc := testutil.NewProcess(b)
					typ := types.T_text.ToType()
					fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(typ, values, nil)},
						NewFunctionTestResult(typ, false, wanted, nil), implementation.fn)
					defer fc.Free()
					b.SetBytes(outputBytes)
					if !cold {
						fc.Benchmark(b)
						return
					}
					if ok, info := fc.Run(); !ok {
						b.Fatal(info)
					}
					b.ReportAllocs()
					// Cold mode times freeing the previous output and constructing a fresh
					// result wrapper/storage each iteration; input/expected rows stay fixed.
					for b.Loop() {
						fc.result.Free()
						fc.result = vector.NewFunctionResultWrapper(typ, proc.Mp())
						if err := fc.result.PreExtendAndReset(scenario.rows); err != nil {
							b.Fatal(err)
						}
						if err := implementation.fn(fc.parameters, fc.result, proc, scenario.rows, nil); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
