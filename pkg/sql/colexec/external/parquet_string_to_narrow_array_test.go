// Copyright 2025 Matrix Origin
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
	"github.com/stretchr/testify/require"
)

// TestParquet_StringToNarrowArray covers the plain-string -> narrow vector
// (bf16/f16/int8/uint8) leaf mappers added to getMapper. Before the fix these
// target types fell through the switch and getMapper returned nil (NYI on
// import), unlike the equivalent vecf32/vecf64 columns.
func TestParquet_StringToNarrowArray(t *testing.T) {
	proc := testutil.NewProc(t)

	tests := []struct {
		name      string
		dt        types.T
		width     int32
		strValues []string
	}{
		{"STRING → VECINT8", types.T_array_int8, 3, []string{"[1,2,3]", "[-128,0,127]"}},
		{"STRING → VECUINT8", types.T_array_uint8, 3, []string{"[0,1,2]", "[255,128,0]"}},
		{"STRING → VECBF16", types.T_array_bf16, 3, []string{"[1,2,3]", "[-1.5,0,2.5]"}},
		{"STRING → VECFLOAT16", types.T_array_float16, 3, []string{"[1,2,3]", "[-1.5,0,2.5]"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Build a plain-string leaf parquet column holding the array text.
			st := parquet.String().Type()
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
			w := parquet.NewWriter(&buf, schema)
			for _, s := range tc.strValues {
				_, err := w.WriteRows([]parquet.Row{{parquet.ByteArrayValue([]byte(s))}})
				require.NoError(t, err)
			}
			require.NoError(t, w.Close())

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)
			col := f.Root().Column("c")
			page, err := col.Pages().ReadPage()
			require.NoError(t, err)

			vec := vector.NewVec(types.New(tc.dt, tc.width, 0))
			var h ParquetHandler
			mp := h.getMapper(col, plan.Type{Id: int32(tc.dt), Width: tc.width, NotNullable: true})
			require.NotNil(t, mp, "STRING → %s conversion should be supported", tc.dt)
			require.NoError(t, mp.mapping(page, proc, vec))
			require.Equal(t, len(tc.strValues), vec.Length())

			// The stored bytes must match parsing the same text directly.
			for i, s := range tc.strValues {
				switch tc.dt {
				case types.T_array_int8:
					want, err := types.StringToArray[int8](s)
					require.NoError(t, err)
					require.Equal(t, want, vector.GetArrayAt[int8](vec, i))
				case types.T_array_uint8:
					want, err := types.StringToArray[uint8](s)
					require.NoError(t, err)
					require.Equal(t, want, vector.GetArrayAt[uint8](vec, i))
				case types.T_array_bf16:
					want, err := types.StringToArray[types.BF16](s)
					require.NoError(t, err)
					require.Equal(t, want, vector.GetArrayAt[types.BF16](vec, i))
				case types.T_array_float16:
					want, err := types.StringToArray[types.Float16](s)
					require.NoError(t, err)
					require.Equal(t, want, vector.GetArrayAt[types.Float16](vec, i))
				}
			}
		})
	}
}

// TestParquet_StringToNarrowArray_DimMismatch covers the width-check branch:
// a value whose element count differs from the column dimension must error.
func TestParquet_StringToNarrowArray_DimMismatch(t *testing.T) {
	proc := testutil.NewProc(t)

	st := parquet.String().Type()
	var buf bytes.Buffer
	schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
	w := parquet.NewWriter(&buf, schema)
	_, err := w.WriteRows([]parquet.Row{{parquet.ByteArrayValue([]byte("[1,2,3,4]"))}})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	col := f.Root().Column("c")
	page, err := col.Pages().ReadPage()
	require.NoError(t, err)

	vec := vector.NewVec(types.New(types.T_array_int8, 3, 0))
	var h ParquetHandler
	mp := h.getMapper(col, plan.Type{Id: int32(types.T_array_int8), Width: 3, NotNullable: true})
	require.NotNil(t, mp)
	require.Error(t, mp.mapping(page, proc, vec), "dimension mismatch (4 != 3) should error")
}

// TestParquet_StringToNarrowArray_Branches covers the two per-case sub-branches
// for every narrow type: width<=0 defaulting to MaxArrayDimension, and a
// non-plain-string physical source being unsupported (getMapper returns nil).
func TestParquet_StringToNarrowArray_Branches(t *testing.T) {
	proc := testutil.NewProc(t)
	narrowTypes := []types.T{
		types.T_array_int8, types.T_array_uint8, types.T_array_bf16, types.T_array_float16,
	}

	// (a) Width == 0 in the plan type → falls back to MaxArrayDimension (no dim check).
	for _, dt := range narrowTypes {
		st := parquet.String().Type()
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(st)})
		w := parquet.NewWriter(&buf, schema)
		_, err := w.WriteRows([]parquet.Row{{parquet.ByteArrayValue([]byte("[1,2,3]"))}})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)
		col := f.Root().Column("c")
		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(dt, 3, 0))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(dt) /* Width: 0 */, NotNullable: true})
		require.NotNil(t, mp, "%s with width 0 should still map", dt)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 1, vec.Length())
	}

	// (b) Non-plain-string physical source (INT64 leaf) → unsupported, nil mapper.
	for _, dt := range narrowTypes {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{"c": parquet.Leaf(parquet.Int64Type)})
		w := parquet.NewWriter(&buf, schema)
		_, err := w.WriteRows([]parquet.Row{{parquet.Int64Value(1)}})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)
		col := f.Root().Column("c")
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(dt), Width: 3, NotNullable: true})
		require.Nil(t, mp, "non-string source → %s should be unsupported", dt)
	}
}
