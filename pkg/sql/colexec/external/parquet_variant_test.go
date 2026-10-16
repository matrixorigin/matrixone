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

package external

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestParquetVariantFileImport(t *testing.T) {
	for _, target := range []types.T{types.T_json, types.T_text} {
		t.Run(target.String(), func(t *testing.T) {
			schema := parquet.NewSchema("variant-test", parquet.Group{
				"a_unused_000": parquet.Leaf(parquet.Int32Type),
				"z_nested":     parquet.Optional(parquet.Variant()),
			})
			decimal := binary.LittleEndian.AppendUint64([]byte{10 << 2, 3}, uint64(9007199254740993001))
			decimal = binary.LittleEndian.AppendUint64(decimal, 0)
			var buf bytes.Buffer
			w := parquet.NewWriter(&buf, schema, parquet.MaxRowsPerRowGroup(2))
			_, err := w.WriteRows([]parquet.Row{
				{
					parquet.Int32Value(10).Level(0, 0, 0),
					parquet.ByteArrayValue([]byte{1, 0, 0}).Level(0, 1, 1),
					parquet.ByteArrayValue(decimal).Level(0, 1, 2),
				},
				{
					parquet.Int32Value(20).Level(0, 0, 0),
					parquet.ByteArrayValue([]byte{1, 0, 0}).Level(0, 1, 1),
					parquet.ByteArrayValue([]byte{0}).Level(0, 1, 2),
				},
				{
					parquet.Int32Value(30).Level(0, 0, 0),
					parquet.NullValue().Level(0, 0, 1),
					parquet.NullValue().Level(0, 0, 2),
				},
			})
			require.NoError(t, err)
			require.NoError(t, w.Close())
			param := nestedAndScalarProjectionParam(buf.Bytes())
			param.Cols[1].Typ.Id = int32(target)
			proc := testutil.NewProc(t)
			r := NewParquetReader(param, proc)
			empty, err := r.Open(param, proc)
			require.NoError(t, err)
			require.False(t, empty)
			defer r.Close()
			require.Equal(t, [][]string{{"z_nested", "metadata"}, {"z_nested", "value"}}, r.h.rowReader.Schema().Columns())
			require.Equal(t, []int{0}, r.h.dataColIndices)
			var seen int
			finished := false
			for !finished {
				bat := vectorBatch([]types.Type{types.T_int32.ToType(), target.ToType()})
				finished, err = r.ReadBatch(context.Background(), bat, proc, nil)
				require.NoError(t, err)
				for row := 0; row < bat.RowCount(); row++ {
					require.Equal(t, int32((seen+1)*10), vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])[row])
					vec := bat.Vecs[1]
					if seen == 2 {
						require.True(t, vec.IsNull(uint64(row)))
					} else {
						require.False(t, vec.IsNull(uint64(row)))
						want := "9007199254740993.001"
						if seen == 1 {
							want = "null"
						}
						if target == types.T_json {
							bj := types.DecodeJson(vec.GetBytesAt(row))
							require.Equal(t, want, bj.String())
							if seen == 0 {
								require.Equal(t, bytejson.TpCodeDecimal, bj.Type)
							}
						} else {
							require.Equal(t, want, vec.GetStringAt(row))
						}
					}
					seen++
				}
				bat.Clean(proc.Mp())
			}
			require.Equal(t, 3, seen)
		})
	}
}

func TestParquetVariantNestedAndMalformed(t *testing.T) {
	schema := parquet.NewSchema("test", parquet.Group{"v": parquet.Variant()})
	fileSchema := parquetFileColumns(t, schema)
	col := fileSchema.Columns()[0]
	valid := []parquet.Value{
		parquet.ByteArrayValue([]byte{1, 0, 0}).Level(0, 0, 0),
		parquet.ByteArrayValue([]byte{0}).Level(0, 0, 1),
	}
	for _, fn := range []func(context.Context, *parquet.Column, []parquet.Value) (any, error){reconstructNestedValue, reconstructNestedByType} {
		value, err := fn(context.Background(), col, valid)
		require.NoError(t, err)
		require.Equal(t, bytejson.Null, value)
	}
	for _, values := range [][]parquet.Value{nil, valid[:1], {valid[0], valid[0], valid[1]}, {valid[0], parquet.ByteArrayValue([]byte{0, 0}).Level(0, 0, 1)}} {
		_, err := reconstructParquetVariant(context.Background(), col, values)
		require.Error(t, err)
	}
}

type parquetVariantTestNode struct{ parquet.Group }

func (parquetVariantTestNode) Type() parquet.Type { return parquet.Variant().Type() }

func TestParquetVariantRejectsInvalidSchemas(t *testing.T) {
	for _, children := range []parquet.Group{
		{"metadata": parquet.Leaf(parquet.ByteArrayType), "value": parquet.Optional(parquet.Leaf(parquet.ByteArrayType)), "typed_value": parquet.Leaf(parquet.Int64Type)},
		{"metadata": parquet.Optional(parquet.Leaf(parquet.ByteArrayType)), "value": parquet.Leaf(parquet.ByteArrayType)},
		{"metadata": parquet.String(), "value": parquet.Leaf(parquet.ByteArrayType)},
		{"metadata": parquet.Leaf(parquet.ByteArrayType), "other": parquet.Leaf(parquet.ByteArrayType)},
	} {
		schema := parquet.NewSchema("test", parquet.Group{"v": parquetVariantTestNode{children}})
		root := parquetFileColumns(t, schema)
		require.Error(t, validateParquetVariantSchema(context.Background(), root))
	}
}

// Obtain Column objects by opening a real schema rather than depending on
// parquet-go's internal Column constructors.
func parquetFileColumns(t *testing.T, schema *parquet.Schema) *parquet.Column {
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	return f.Root()
}
