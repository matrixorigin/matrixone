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

package external

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// Parquet LIST -> vector loads must reject NaN/±Inf elements the same way
// the vector SQL input path does (issue #31508).
func TestParquetListVectorRejectsNonFiniteElements(t *testing.T) {
	proc := testutil.NewProc(t)
	nan32 := float32(math.NaN())
	inf32 := float32(math.Inf(1))
	tests := []struct {
		name   string
		elem   parquet.Node
		bad    parquet.Value
		target types.T
	}{
		{"float32-nan", parquet.Leaf(parquet.FloatType), parquet.FloatValue(nan32), types.T_array_float32},
		{"float32-inf", parquet.Leaf(parquet.FloatType), parquet.FloatValue(inf32), types.T_array_float32},
		{"float32-from-double-nan", parquet.Leaf(parquet.DoubleType), parquet.DoubleValue(math.NaN()), types.T_array_float32},
		{"float32-from-double-ninf", parquet.Leaf(parquet.DoubleType), parquet.DoubleValue(math.Inf(-1)), types.T_array_float32},
		{"float64-from-float-nan", parquet.Leaf(parquet.FloatType), parquet.FloatValue(nan32), types.T_array_float64},
		{"float64-nan", parquet.Leaf(parquet.DoubleType), parquet.DoubleValue(math.NaN()), types.T_array_float64},
		{"float64-inf", parquet.Leaf(parquet.DoubleType), parquet.DoubleValue(math.Inf(1)), types.T_array_float64},
		{"bf16-nan", parquet.Leaf(parquet.FloatType), parquet.FloatValue(nan32), types.T_array_bf16},
		{"bf16-inf", parquet.Leaf(parquet.FloatType), parquet.FloatValue(inf32), types.T_array_bf16},
		{"bf16-overflow", parquet.Leaf(parquet.FloatType), parquet.FloatValue(3.4e38), types.T_array_bf16},
		{"float16-nan", parquet.Leaf(parquet.FloatType), parquet.FloatValue(nan32), types.T_array_float16},
		{"float16-overflow", parquet.Leaf(parquet.FloatType), parquet.FloatValue(70000), types.T_array_float16},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var good parquet.Value
			if test.elem.Type().Kind() == parquet.Double {
				good = parquet.DoubleValue(1)
			} else {
				good = parquet.FloatValue(1)
			}
			row := parquet.Row{
				good.Level(0, 1, 0),
				test.bad.Level(1, 1, 0),
			}
			file, page := writeListAndGetPage(t, test.elem, []parquet.Row{row})
			_, mapper := new(ParquetHandler).getNestedListMapper(file.Root().Column("c"), plan.Type{
				Id:    int32(test.target),
				Width: 2,
			})
			require.NotNil(t, mapper)
			require.NotNil(t, mapper.listValuesMapper)
			values, err := readParquetPageAllValues(proc.Ctx, page)
			require.NoError(t, err)
			vec := vector.NewVec(types.New(test.target, 2, 0))
			t.Cleanup(func() { vec.Free(proc.Mp()) })
			err = mapper.listValuesMapper(mapper, values, 1, proc, vec)
			require.Error(t, err)
			require.Contains(t, err.Error(), "vector element cannot be NaN or Inf")
		})
	}
}

func TestParquetListVectorAcceptsFiniteElements(t *testing.T) {
	proc := testutil.NewProc(t)
	row := parquet.Row{
		parquet.FloatValue(-1.5).Level(0, 1, 0),
		parquet.FloatValue(65504).Level(1, 1, 0),
	}
	for _, target := range []types.T{types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16} {
		file, page := writeListAndGetPage(t, parquet.Leaf(parquet.FloatType), []parquet.Row{row})
		_, mapper := new(ParquetHandler).getNestedListMapper(file.Root().Column("c"), plan.Type{Id: int32(target), Width: 2})
		require.NotNil(t, mapper)
		values, err := readParquetPageAllValues(proc.Ctx, page)
		require.NoError(t, err)
		vec := vector.NewVec(types.New(target, 2, 0))
		require.NoError(t, mapper.listValuesMapper(mapper, values, 1, proc, vec), target.String())
		require.Equal(t, 1, vec.Length())
		vec.Free(proc.Mp())
	}
}
