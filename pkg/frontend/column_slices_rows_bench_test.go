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

package frontend

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const columnSlicesBenchRows = 1024

// columnSlicesBenchBatch returns a batch of columnSlicesBenchRows rows of the
// given column types, every 13th row NULL, and a result set describing them.
func columnSlicesBenchBatch(tb testing.TB, colTypes []types.Type, mp *mpool.MPool) (*batch.Batch, *MysqlResultSet) {
	bat := batch.NewWithSize(len(colTypes))
	mrs := &MysqlResultSet{}
	for c, typ := range colTypes {
		col := new(MysqlColumn)
		if err := setMysqlColumnTypeInfo(context.Background(), typ, col); err != nil {
			tb.Fatal(err)
		}
		mrs.AddColumn(col)
		v := vector.NewVec(typ, mp)
		for r := 0; r < columnSlicesBenchRows; r++ {
			null := (r+c)%13 == 0
			var err error
			switch typ.Oid {
			case types.T_int64:
				err = vector.AppendFixed(v, int64(r*1000+c), null, mp)
			case types.T_int32:
				err = vector.AppendFixed(v, int32(r), null, mp)
			case types.T_decimal64:
				err = vector.AppendFixed(v, types.Decimal64(r*100+7), null, mp)
			case types.T_date:
				err = vector.AppendFixed(v, types.Date(700000+r), null, mp)
			case types.T_datetime:
				err = vector.AppendFixed(v, types.Datetime(int64(r)<<20+63000000000000000), null, mp)
			case types.T_varchar:
				err = vector.AppendBytes(v, []byte(fmt.Sprintf("name-%06d", r)), null, mp)
			}
			if err != nil {
				tb.Fatal(err)
			}
		}
		bat.Vecs[c] = v
	}
	bat.SetRowCount(columnSlicesBenchRows)
	return bat, mrs
}

// BenchmarkColumnSlicesRows encodes a 1024-row batch through the result-row
// path used for query results (appendResultSetTextRow2 / BinaryRow2).
func BenchmarkColumnSlicesRows(b *testing.B) {
	int64s := make([]types.Type, 8)
	for i := range int64s {
		int64s[i] = types.T_int64.ToType()
	}
	mixed := []types.Type{
		types.T_int64.ToType(), types.T_int32.ToType(), types.New(types.T_decimal64, 12, 2),
		types.T_varchar.ToType(), types.T_date.ToType(), types.New(types.T_datetime, 0, 0),
		types.T_int64.ToType(), types.T_varchar.ToType(),
	}
	for _, shape := range []struct {
		name  string
		types []types.Type
	}{{"int64x8", int64s}, {"mixed8", mixed}} {
		for _, binaryRow := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/binary=%v", shape.name, binaryRow), func(b *testing.B) {
				proto := wireBenchProtocol(b)
				ctx := context.Background()
				mp := mpool.MustNewZero()
				bat, mrs := columnSlicesBenchBatch(b, shape.types, mp)
				defer bat.Clean(mp)
				slices := &ColumnSlices{ctx: ctx, colIdx2SliceIdx: make([]int, len(bat.Vecs)), dataSet: bat}
				defer slices.Close()
				if err := convertBatchToSlices(ctx, proto.GetSession(), bat, slices); err != nil {
					b.Fatal(err)
				}
				write := func() error {
					for r := uint64(0); r < columnSlicesBenchRows; r++ {
						var err error
						if binaryRow {
							err = proto.appendResultSetBinaryRow2(mrs, slices, r)
						} else {
							err = proto.appendResultSetTextRow2(mrs, slices, r)
						}
						if err != nil {
							return err
						}
					}
					return proto.flush()
				}
				if err := write(); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := write(); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*columnSlicesBenchRows*len(shape.types)), "ns/cell")
			})
		}
	}
}
