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
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// Exercise the vector entry point, including pattern preparation and the
// existing constant-pattern fast paths, rather than timing only a matcher.
func BenchmarkLikePatterns(b *testing.B) {
	benchmarkLikePatterns(b, false)
}

func BenchmarkLikePatternsCancelableContext(b *testing.B) {
	benchmarkLikePatterns(b, true)
}

func benchmarkLikePatterns(b *testing.B, cancelable bool) {
	const rows = 256
	for _, tc := range []struct{ name, pattern, value string }{
		{"exact", "catalog", "catalog"},
		{"prefix", "catalog%", "catalog_columns"},
		{"suffix", "%columns", "catalog_columns"},
		{"contains", "%catalog%", "mo_catalog_columns"},
		{"segments", "catalog%column%name", "catalog_database_column_table_name"},
		{"wildcards", "%catalog_%column_%", "mo_catalog_x_column_y"},
		{"unicode", "%目录_%字段_%", "mo_目录中_字段🙂"},
		{"long_segments_miss", "a%" + strings.Repeat("a", 63) + "b%c", "a" + strings.Repeat("a", 8192) + "c"},
		{"long_miss", "%catalog_%missing%", strings.Repeat("catalog_x_", 1024)},
	} {
		for _, constant := range []bool{true, false} {
			mode := "dynamic"
			if constant {
				mode = "constant"
			}
			b.Run(tc.name+"/"+mode, func(b *testing.B) {
				proc := newLikeTestProcess(b)
				if cancelable {
					ctx, cancel := context.WithCancel(proc.Ctx)
					defer cancel()
					proc.Ctx = ctx
					// Allocate Done before timing, as a running pipeline does.
					require.NotNil(b, ctx.Done())
				}
				values := vector.NewVec(types.T_varchar.ToType())
				defer values.Free(proc.Mp())
				var patterns *vector.Vector
				if constant {
					var err error
					patterns, err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.pattern), rows, proc.Mp())
					require.NoError(b, err)
				} else {
					patterns = vector.NewVec(types.T_varchar.ToType())
					for row := 0; row < rows; row++ {
						pattern := tc.pattern
						if row%2 != 0 {
							pattern = "%" + pattern
						}
						require.NoError(b, vector.AppendBytes(patterns, []byte(pattern), false, proc.Mp()))
					}
				}
				defer patterns.Free(proc.Mp())
				for row := 0; row < rows; row++ {
					require.NoError(b, vector.AppendBytes(values, []byte(tc.value), false, proc.Mp()))
				}
				result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
				defer result.Free()
				op := newOpBuiltInRegexp()
				parameters := []*vector.Vector{values, patterns}
				b.ReportAllocs()
				b.SetBytes(int64(rows * len(tc.value)))
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
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
