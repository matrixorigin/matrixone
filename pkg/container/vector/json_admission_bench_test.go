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

package vector

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// Measure the existing public admission paths, including validation in the
// timed loop. Reuse this exact fixture on the target base and candidate.
func BenchmarkJSONAdmissionExistingPaths(b *testing.B) {
	for _, count := range []int{16, 4096} {
		b.Run(fmt.Sprintf("elements=%d", count), func(b *testing.B) {
			raw := jsonAdmissionValue(b, `{"keep":1,"large":[`+strings.Repeat("1,", count-1)+"1]}")
			for _, operation := range []string{"append", "unmarshal", "unmarshal-copy"} {
				b.Run(operation, func(b *testing.B) {
					mp := mpool.MustNewZero()
					v := NewVec(types.T_json.ToType())
					b.Cleanup(func() { v.Free(mp); require.Zero(b, mp.CurrNB()) })
					require.NoError(b, AppendBytes(v, raw, false, mp))
					wire, err := v.MarshalBinary()
					require.NoError(b, err)
					v.Free(mp)
					step := func() error {
						switch operation {
						case "append":
							v.ResetWithSameType()
							return AppendBytes(v, raw, false, mp)
						case "unmarshal":
							return v.UnmarshalBinary(wire)
						default:
							v.Free(mp)
							return v.UnmarshalBinaryWithCopy(wire, mp)
						}
					}
					// Warm up and independently check publication before timing.
					require.NoError(b, step())
					require.Equal(b, 1, v.Length())
					require.True(b, bytes.Equal(raw, v.GetBytesAt(0)))
					b.ReportAllocs()
					b.ResetTimer()
					b.ReportMetric(float64(len(raw)), "bytes/document")
					for range b.N {
						if err := step(); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					require.Equal(b, 1, v.Length())
					require.True(b, bytes.Equal(raw, v.GetBytesAt(0)))
				})
			}
		})
	}
}
