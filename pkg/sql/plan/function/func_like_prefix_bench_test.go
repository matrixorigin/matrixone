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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func BenchmarkGeneralCIPrefixLike(b *testing.B) {
	for _, tc := range []struct {
		name, pattern, value string
		nullable, dynamic    bool
	}{
		{"ASCII", "forest%", "forest sienna antique steel sandy", false, false},
		{"mismatch", "forest%", "coral sienna antique steel sandy", false, false},
		{"Unicode", "forest%", "fórêst sienna antique steel sandy", false, false},
		{"nullable", "forest%", "forest sienna antique steel sandy", true, false},
		{"contains-control", "%forest%", "antique forest steel sandy", false, false},
		{"dynamic-control", "forest%", "forest sienna antique steel sandy", false, true},
	} {
		for _, rows := range []int{1, 256} {
			b.Run(fmt.Sprintf("%s/rows=%d", tc.name, rows), func(b *testing.B) {
				proc := newLikeTestProcess(b)
				typ := types.NewWithCharset(types.T_varchar, 128, 0, types.CharsetUTF8)
				values := vector.NewVec(typ)
				b.Cleanup(func() { values.Free(proc.Mp()) })
				for row := 0; row < rows; row++ {
					require.NoError(b, vector.AppendBytes(values, []byte(tc.value), tc.nullable && row%4 == 0, proc.Mp()))
				}
				var pattern *vector.Vector
				if tc.dynamic {
					pattern = vector.NewVec(typ)
					for row := 0; row < rows; row++ {
						require.NoError(b, vector.AppendBytes(pattern, []byte(tc.pattern), false, proc.Mp()))
					}
				} else {
					var err error
					pattern, err = vector.NewConstBytes(typ, []byte(tc.pattern), rows, proc.Mp())
					require.NoError(b, err)
				}
				b.Cleanup(func() { pattern.Free(proc.Mp()) })
				result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
				b.Cleanup(result.Free)
				op := newOpBuiltInRegexp()
				parameters := []*vector.Vector{values, pattern}
				b.ReportAllocs()
				b.SetBytes(int64(rows * len(tc.value)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := result.PreExtendAndReset(rows); err != nil {
						b.Fatal(err)
					}
					if err := op.likeFn(parameters, result, proc, rows, nil); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
