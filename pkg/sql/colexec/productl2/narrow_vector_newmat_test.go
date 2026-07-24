// Copyright 2021 Matrix Origin
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

package productl2

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// newMat decodes the probe (base table) column into the CENTROID element type.
// That distinction is the whole point of the function: under QUANTIZATION the
// entries column can be bf16/f16/int8/uint8 while the centroids stay f32, so a
// probe row must be widened to f32 before it is ever compared. Reading the raw
// storage bytes as f32 instead would silently produce garbage distances — the
// query still returns rows, just the wrong ones, which is exactly the kind of
// bug a coverage hole here hides.
//
// onExprWithProbeCol builds the minimal OnExpr newMat inspects: it only reads
// args[1]'s column position to find the probe column.
func onExprWithProbeCol(pos int32) *plan.Expr {
	col := func(p int32) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: p}}}
	}
	return &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Args: []*plan.Expr{col(0), col(pos)},
		}},
	}
}

func TestNewMatDecodesNarrowProbeColumns(t *testing.T) {
	mp := mpool.MustNewZero()

	// The same logical vector [1, -2, 3] in each storage type. int8/uint8 cannot
	// hold -2 as the others do, so uint8 carries its own expectation.
	bf16 := []types.BF16{types.BF16FromFloat32(1), types.BF16FromFloat32(-2), types.BF16FromFloat32(3)}
	f16 := []types.Float16{types.Float16FromFloat32(1), types.Float16FromFloat32(-2), types.Float16FromFloat32(3)}

	for _, tc := range []struct {
		name string
		typ  types.Type
		fill func(*vector.Vector)
		want []float32
	}{
		{
			name: "float32 passthrough",
			typ:  types.T_array_float32.ToType(),
			fill: func(v *vector.Vector) {
				require.NoError(t, vector.AppendArrayList[float32](v, [][]float32{{1, -2, 3}}, nil, mp))
			},
			want: []float32{1, -2, 3},
		},
		{
			name: "float64 narrows to f32 centroids",
			typ:  types.T_array_float64.ToType(),
			fill: func(v *vector.Vector) {
				require.NoError(t, vector.AppendArrayList[float64](v, [][]float64{{1, -2, 3}}, nil, mp))
			},
			want: []float32{1, -2, 3},
		},
		{
			name: "bf16 widens through ToFloat32",
			typ:  types.T_array_bf16.ToType(),
			fill: func(v *vector.Vector) {
				require.NoError(t, vector.AppendArrayList[types.BF16](v, [][]types.BF16{bf16}, nil, mp))
			},
			want: []float32{1, -2, 3},
		},
		{
			name: "f16 widens through ToFloat32",
			typ:  types.T_array_float16.ToType(),
			fill: func(v *vector.Vector) {
				require.NoError(t, vector.AppendArrayList[types.Float16](v, [][]types.Float16{f16}, nil, mp))
			},
			want: []float32{1, -2, 3},
		},
		{
			name: "int8 keeps the sign",
			typ:  types.T_array_int8.ToType(),
			fill: func(v *vector.Vector) {
				require.NoError(t, vector.AppendArrayList[int8](v, [][]int8{{1, -2, 3}}, nil, mp))
			},
			want: []float32{1, -2, 3},
		},
		{
			// 254 is -2 read as int8. If uint8 shared the int8 arm this would
			// come back as -2 and the distance would be wildly wrong, so the
			// value is chosen to make that specific confusion fail loudly.
			name: "uint8 does not wrap past 127",
			typ:  types.T_array_uint8.ToType(),
			fill: func(v *vector.Vector) {
				require.NoError(t, vector.AppendArrayList[uint8](v, [][]uint8{{1, 254, 3}}, nil, mp))
			},
			want: []float32{1, 254, 3},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vec := vector.NewVec(tc.typ)
			defer vec.Free(mp)
			tc.fill(vec)

			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vec
			bat.SetRowCount(1)

			ctr := &container{inBat: bat}
			ap := &Productl2{OnExpr: onExprWithProbeCol(0)}

			probes := make([][]float32, 1)
			got, err := newMat[float32](ctr, ap, probes, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, got[0])
		})
	}

	// A NULL probe row must take the shared nullvec, not a decoded value —
	// and nullvec is stamped [1,0,0,...] so a distance against it is defined.
	t.Run("null row uses nullvec", func(t *testing.T) {
		vec := vector.NewVec(types.T_array_int8.ToType())
		defer vec.Free(mp)
		require.NoError(t, vector.AppendArrayList[int8](vec, [][]int8{{1, 2, 3}}, []bool{true}, mp))

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vec
		bat.SetRowCount(1)

		ctr := &container{inBat: bat}
		ap := &Productl2{OnExpr: onExprWithProbeCol(0)}

		nullvec := make([]float32, 3)
		probes := make([][]float32, 1)
		got, err := newMat[float32](ctr, ap, probes, nullvec)
		require.NoError(t, err)
		require.Equal(t, []float32{1, 0, 0}, got[0])
	})

	// f64 centroids reinterpret the base directly rather than widening.
	t.Run("float64 centroids take the direct path", func(t *testing.T) {
		vec := vector.NewVec(types.T_array_float64.ToType())
		defer vec.Free(mp)
		require.NoError(t, vector.AppendArrayList[float64](vec, [][]float64{{1, -2, 3}}, nil, mp))

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vec
		bat.SetRowCount(1)

		ctr := &container{inBat: bat}
		ap := &Productl2{OnExpr: onExprWithProbeCol(0)}

		probes := make([][]float64, 1)
		got, err := newMat[float64](ctr, ap, probes, nil)
		require.NoError(t, err)
		require.Equal(t, []float64{1, -2, 3}, got[0])
	})
}
