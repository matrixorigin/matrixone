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
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// Exercise the actual four-argument cast at both indexing thresholds.
func BenchmarkCastCollatedEnum(b *testing.B) {
	for _, count := range []int{3, 7, 8} {
		members := make([]string, count)
		for i := range members {
			members[i] = "value" + strconv.Itoa(i)
		}
		definition := strings.Join(members, ",")
		for _, rows := range []int{63, 64} {
			for _, domain := range []struct {
				name string
				id   uint32
			}{
				{"bin", types.EnumCollationUTF8Bin},
				{"general", types.EnumCollationGeneralCI},
			} {
				b.Run(strconv.Itoa(count)+"-labels/"+strconv.Itoa(rows)+"-rows/"+domain.name, func(b *testing.B) {
					proc := testutil.NewProcess(b)
					definitions := make([]string, rows)
					values := make([]string, rows)
					encodings := make([]int64, rows)
					identities := make([]int64, rows)
					want := make([]types.Enum, rows)
					for i := range rows {
						definitions[i] = definition
						values[i] = members[count-1]
						if domain.id == types.EnumCollationGeneralCI {
							values[i] = strings.ToUpper(values[i])
						}
						identities[i] = int64(domain.id)
						want[i] = types.Enum(count)
					}
					fc := NewFunctionTestCase(proc, []FunctionTestInput{
						NewFunctionTestConstInput(types.T_varchar.ToType(), definitions, nil),
						NewFunctionTestInput(types.T_varchar.ToType(), values, nil),
						NewFunctionTestConstInput(types.T_int64.ToType(), encodings, nil),
						NewFunctionTestConstInput(types.T_int64.ToType(), identities, nil),
					}, NewFunctionTestResult(types.T_enum.ToType(), false, want, nil), CastValueToIndex)
					b.Cleanup(fc.Free)
					fc.Benchmark(b)
				})
			}
		}
	}
}
