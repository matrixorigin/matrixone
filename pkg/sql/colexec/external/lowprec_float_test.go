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
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/stretchr/testify/require"
)

// TestParquetLoadLowPrecisionFloat checks that a FLOAT or DOUBLE parquet column loads into
// bf16, float16, float8 and float4 columns with the checks of a SQL cast, storing -0 as +0.
func TestParquetLoadLowPrecisionFloat(t *testing.T) {
	proc := testutil.NewProc(t)
	for _, st := range []parquet.Type{parquet.FloatType, parquet.DoubleType} {
		load := func(oid types.T, values []float64) (*vector.Vector, error) {
			var page parquet.Page
			if st == parquet.FloatType {
				f32 := make([]float32, len(values))
				for i, v := range values {
					f32[i] = float32(v)
				}
				page = st.NewPage(0, len(values), encoding.FloatValues(f32))
			} else {
				page = st.NewPage(0, len(values), encoding.DoubleValues(values))
			}
			var buf bytes.Buffer
			w := parquet.NewWriter(&buf, parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)}))
			vals := make([]parquet.Value, page.NumRows())
			_, _ = page.Values().ReadValues(vals)
			_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
			require.NoError(t, err)
			require.NoError(t, w.Close())
			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)
			vec := vector.NewVec(oid.ToType())
			var h ParquetHandler
			mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(oid), NotNullable: true})
			require.NotNil(t, mp, oid.String())
			return vec, mp.mapping(page, proc, vec)
		}
		for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
			vec, err := load(oid, []float64{1.5, -2, -0.0})
			require.NoError(t, err, oid.String())
			for i, want := range []float32{1.5, -2, 0} {
				got, _ := vector.GetLowPrecisionFloatAt(vec, i)
				require.Equal(t, want, got, "%s row %d", oid, i)
			}
			require.Equal(t, byte(0), vec.GetRawBytesAt(2)[0], "-0 is stored as +0")
			vec.Free(proc.Mp())
			_, err = load(oid, []float64{1e39})
			require.Error(t, err, oid.String())
		}
	}
}

// TestLoadNumericPredicatesLowPrecisionFloat checks that non-strict LOAD treats bf16,
// float16, float8 and float4 like float32: an empty field is zero and a numeric prefix is
// kept.
func TestLoadNumericPredicatesLowPrecisionFloat(t *testing.T) {
	for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
		require.True(t, isLoadNumericZeroFillType(oid), oid.String())
		require.True(t, isLoadNumericAdjustedValueType(oid), oid.String())
	}
}

// TestParquetLoadLowPrecisionFloatExactSource checks that INT64, UINT64 and DECIMAL parquet
// values round once from their exact value into bf16, as the SQL cast does: 18084767253659649
// lies above the bf16 midpoint 18084767253659648, which a float64 conversion rounds onto.
func TestParquetLoadLowPrecisionFloatExactSource(t *testing.T) {
	proc := testutil.NewProc(t)
	load := func(st parquet.Type, values encoding.Values, n int) *vector.Vector {
		page := st.NewPage(0, n, values)
		var buf bytes.Buffer
		w := parquet.NewWriter(&buf, parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)}))
		vals := make([]parquet.Value, page.NumRows())
		_, _ = page.Values().ReadValues(vals)
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(vals)})
		require.NoError(t, err)
		require.NoError(t, w.Close())
		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)
		vec := vector.NewVec(types.T_bf16.ToType())
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bf16), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		return vec
	}
	const above, midpoint = int64(18084767253659649), int64(18084767253659648)
	signed := []int64{above, -above, midpoint, -midpoint, 42}
	want := []types.BF16{0x5a81, 0xda81, 0x5a80, 0xda80, types.BF16FromFloat32(42)}
	for _, st := range []parquet.Type{parquet.Int64Type, parquet.Decimal(0, 18, parquet.Int64Type).Type()} {
		vec := load(st, encoding.Int64Values(signed), len(signed))
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[types.BF16](vec), st.String())
		vec.Free(proc.Mp())
	}
	unsigned := []uint64{uint64(above), uint64(midpoint)}
	vec := load(parquet.Uint(64).Type(), encoding.Uint64Values(unsigned), len(unsigned))
	require.Equal(t, []types.BF16{0x5a81, 0x5a80}, vector.MustFixedColWithTypeCheck[types.BF16](vec))
	vec.Free(proc.Mp())
}
