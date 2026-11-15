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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func BenchmarkEnumDomainConversions(b *testing.B) {
	benchmarkEnumDomainConversions(b, false)
}

func BenchmarkFramedEnumDomainConversions(b *testing.B) {
	benchmarkEnumDomainConversions(b, true)
}

func benchmarkEnumDomainConversions(b *testing.B, framed bool) {
	for _, labels := range []int{3, 8, 64} {
		members := make([]string, labels)
		for i := range members {
			members[i] = fmt.Sprintf("value_%d", i)
		}
		if framed {
			members[0] = "value,0"
		}
		for _, rows := range []int{1, 64, 1024} {
			for _, constant := range []bool{true, false} {
				for _, op := range []string{"display", "label", "numeric"} {
					b.Run(fmt.Sprintf("%d-labels/%d-rows/const-%t/%s", labels, rows, constant, op), func(b *testing.B) {
						proc := testutil.NewProcess(b)
						payload := strings.Join(members, ",")
						if framed {
							var text strings.Builder
							fmt.Fprintf(&text, "%d#", len(members))
							for _, member := range members {
								fmt.Fprintf(&text, "%d:%s", len(member), member)
							}
							payload = text.String()
						}
						var domain *vector.Vector
						if constant {
							var err error
							domain, err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(payload), rows, proc.Mp())
							if err != nil {
								b.Fatal(err)
							}
						} else {
							domain = vector.NewVec(types.T_varchar.ToType())
							for range rows {
								if err := vector.AppendBytes(domain, []byte(payload), false, proc.Mp()); err != nil {
									b.Fatal(err)
								}
							}
						}
						defer domain.Free(proc.Mp())
						inputType, outputType, fn := types.T_varchar, types.T_enum, fEvalFn(CastValueToIndex)
						if op == "display" {
							inputType, outputType, fn = types.T_enum, types.T_varchar, CastIndexToValue
						}
						if op == "numeric" {
							inputType, fn = types.T_uint16, CastIndexValueToIndex
						}
						input := vector.NewVec(inputType.ToType())
						defer input.Free(proc.Mp())
						for row := range rows {
							ordinal := row%labels + 1
							var err error
							switch op {
							case "display":
								err = vector.AppendFixed(input, types.Enum(ordinal), false, proc.Mp())
							case "numeric":
								err = vector.AppendFixed(input, uint16(ordinal), false, proc.Mp())
							default:
								err = vector.AppendBytes(input, []byte(members[ordinal-1]), false, proc.Mp())
							}
							if err != nil {
								b.Fatal(err)
							}
						}
						result := vector.NewFunctionResultWrapper(outputType.ToType(), proc.Mp())
						defer result.Free()
						parameters := []*vector.Vector{domain, input}
						if framed {
							encoding, err := vector.NewConstFixed(types.T_int64.ToType(), int64(1), rows, proc.Mp())
							if err != nil {
								b.Fatal(err)
							}
							defer encoding.Free(proc.Mp())
							parameters = append(parameters, encoding)
						}
						b.ReportAllocs()
						b.ResetTimer()
						for range b.N {
							if err := result.PreExtendAndReset(rows); err != nil {
								b.Fatal(err)
							}
							if err := fn(parameters, result, proc, rows, nil); err != nil {
								b.Fatal(err)
							}
						}
					})
				}
			}
		}
	}
}
