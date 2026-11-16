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

package external

import (
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

var benchmarkParquetFloat32Sink float32

func BenchmarkParquetFloat32Conversion(b *testing.B) {
	for _, tc := range []struct {
		name  string
		value float64
	}{{"finite", 1.25}, {"NaN", math.NaN()}} {
		b.Run("scalar/"+tc.name, func(b *testing.B) {
			ctx := context.Background()
			var result float32
			b.ReportAllocs()
			for b.Loop() {
				var err error
				result, err = parquetFloat64ToFloat32(ctx, tc.value)
				if err != nil {
					b.Fatal(err)
				}
			}
			benchmarkParquetFloat32Sink = result
		})
	}

	for _, dictionary := range []bool{false, true} {
		name := "plain"
		node := parquet.Leaf(parquet.DoubleType)
		if dictionary {
			name = "dictionary"
			node = parquet.Encoded(node, &parquet.RLEDictionary)
		}
		b.Run("page/"+name, func(b *testing.B) {
			const count = 4096
			values := []float64{0, math.Copysign(0, -1), 1.25, -1.25,
				math.SmallestNonzeroFloat32, math.MaxFloat32, -math.MaxFloat32, math.NaN()}
			rows := make([]parquet.Row, count)
			for i := range rows {
				rows[i] = parquet.Row{parquet.DoubleValue(values[i%len(values)]).Level(0, 0, 0)}
			}
			file, page := writeColumnAndGetPage(b, node, rows)
			require.EqualValues(b, count, page.NumValues())
			require.EqualValues(b, count, page.NumRows())
			require.Equal(b, dictionary, page.Dictionary() != nil)
			proc := testutil.NewProcess(b)
			vec := vector.NewVec(types.T_float32.ToType())
			b.Cleanup(func() { vec.Free(proc.Mp()) })
			var handler ParquetHandler
			mapper := handler.getMapper(file.Root().Column("c"),
				plan.Type{Id: int32(types.T_float32), NotNullable: true})
			require.NotNil(b, mapper)
			require.NoError(b, mapper.mapping(page, proc, vec))
			got := vector.MustFixedColWithTypeCheck[float32](vec)
			require.Len(b, got, count)
			for i, value := range got {
				want := float32(values[i%len(values)])
				if math.IsNaN(float64(want)) {
					require.True(b, math.IsNaN(float64(value)))
				} else {
					require.Equal(b, math.Float32bits(want), math.Float32bits(value))
				}
			}
			b.ReportAllocs()
			b.SetBytes(count * 8)
			for b.Loop() {
				vec.SetLength(0)
				if err := mapper.mapping(page, proc, vec); err != nil {
					b.Fatal(err)
				}
			}
			require.Equal(b, count, vec.Length())
			benchmarkParquetFloat32Sink = vector.MustFixedColWithTypeCheck[float32](vec)[0]
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*count), "ns/value")
		})
	}
}
