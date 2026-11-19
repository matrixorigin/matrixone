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
	"github.com/stretchr/testify/require"
)

// Include registry dispatch, result reuse, and optionally the lifetime of a
// fresh expression. Sparse/unsorted/NULL controls exercise the map fallback.
func BenchmarkIntegerMembership(b *testing.B) {
	for _, shape := range []string{"dense", "sparse", "unsorted", "null_set", "null_probe", "sparse_null_probe", "all_null_probe", "sparse_all_null_probe", "constant", "miss"} {
		sizes := []int{256}
		if shape == "dense" {
			sizes = []int{0, 1, 4, 16, 256, 4096}
		}
		for _, n := range sizes {
			for _, rows := range []int{1, 8192} {
				for _, build := range []bool{false, true} {
					b.Run(fmt.Sprintf("%s/set%d/rows%d/build%t", shape, n, rows, build), func(b *testing.B) {
						proc := testutil.NewProcess(b)
						list := vector.NewVec(types.T_int32.ToType())
						defer list.Free(proc.Mp())
						input := vector.NewVec(types.T_int32.ToType())
						defer input.Free(proc.Mp())
						stride := 2
						if strings.HasPrefix(shape, "sparse") {
							stride = 100000
						}
						for i := 0; i < n; i++ {
							require.NoError(b, vector.AppendFixed(list, int32(i*stride), shape == "null_set" && i == 0, proc.Mp()))
						}
						list.SetSorted(shape != "unsorted")
						for i := 0; i < rows; i++ {
							v := int32((i * 7919) % max(n*stride, 1))
							if shape == "miss" {
								v = -1
							}
							require.NoError(b, vector.AppendFixed(input, v, strings.Contains(shape, "null_probe") && (strings.Contains(shape, "all_null") || i%4 == 0), proc.Mp()))
						}
						if shape == "constant" {
							input.ToConst()
						}
						out := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
						defer out.Free()
						resolved, err := GetFunctionByName(proc.Ctx, "in", []types.Type{types.T_int32.ToType(), types.T_int32.ToType()})
						require.NoError(b, err)
						ov, err := GetFunctionById(proc.Ctx, resolved.GetEncodedOverloadID())
						require.NoError(b, err)
						fn, _, _, _ := ov.GetExecuteMethod()
						params := []*vector.Vector{input, list}
						require.NoError(b, out.PreExtendAndReset(rows))
						require.NoError(b, fn(params, out, proc, rows, nil))
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if build {
								fn, _, _, _ = ov.GetExecuteMethod()
							}
							if err := out.PreExtendAndReset(rows); err != nil {
								b.Fatal(err)
							}
							if err := fn(params, out, proc, rows, nil); err != nil {
								b.Fatal(err)
							}
						}
					})
				}
			}
		}
	}
}
