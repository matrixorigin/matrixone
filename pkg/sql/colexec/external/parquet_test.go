// Copyright 2024 Matrix Origin
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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/smartystreets/goconvey/convey"
)

func Test_getMapper(t *testing.T) {
	convey.Convey("dataFn", t, func() {
		proc := testutil.NewProc()

		tests := []struct {
			typ       parquet.Type
			numValues int
			values    encoding.Values
			vType     types.T
			expected  string
		}{
			{
				typ:       parquet.BooleanType,
				numValues: 2,
				values:    encoding.BooleanValues([]byte{2}),
				vType:     types.T_bool,
			},
			{
				typ:       parquet.Int32Type,
				numValues: 2,
				values:    encoding.Int32Values([]int32{1, 5}),
				vType:     types.T_int32,
			},
			{
				typ:       parquet.Int64Type,
				numValues: 2,
				values:    encoding.Int64Values([]int64{2, 7}),
				vType:     types.T_int64,
			},
			// {
			// 	typ: parquet.Int96Type,
			// },
			{
				typ:       parquet.FloatType,
				numValues: 2,
				values:    encoding.FloatValues([]float32{7.5, 3.2}),
				vType:     types.T_float32,
			},
			{
				typ:       parquet.DoubleType,
				numValues: 2,
				values:    encoding.DoubleValues([]float64{77.9, 0}),
				vType:     types.T_float64,
			},
			{
				typ:       parquet.ByteArrayType,
				numValues: 2,
				values:    encoding.ByteArrayValues([]byte("abcdefg"), []uint32{0, 3, 7}),
				vType:     types.T_varchar,
			},
			{
				typ:       parquet.FixedLenByteArrayType(3),
				numValues: 2,
				values:    encoding.FixedLenByteArrayValues([]byte("abcdef"), 3),
				vType:     types.T_char,
			},
			{
				typ:       parquet.Date().Type(),
				numValues: 2,
				values:    encoding.Int32Values([]int32{357, 1245}),
				vType:     types.T_date,
				expected:  "[0001-12-24 0004-05-30]",
			},
			{
				typ:       parquet.Time(parquet.Nanosecond).Type(),
				numValues: 2,
				values:    encoding.Int64Values([]int64{18783_111111_111, 25783_222222_222}),
				vType:     types.T_time,
				expected:  "[05:13:03 07:09:43]",
			},
			{
				typ:       parquet.Time(parquet.Microsecond).Type(),
				numValues: 2,
				values:    encoding.Int64Values([]int64{18783_111111, 25783_222222}),
				vType:     types.T_time,
				expected:  "[05:13:03 07:09:43]",
			},
			{
				typ:       parquet.Time(parquet.Millisecond).Type(),
				numValues: 2,
				values:    encoding.Int32Values([]int32{18783_111, 25783_222}),
				vType:     types.T_time,
				expected:  "[05:13:03 07:09:43]",
			},
			{
				typ:       parquet.Timestamp(parquet.Nanosecond).Type(),
				numValues: 2,
				values:    encoding.Int64Values([]int64{1713419514_111111_111, 1713429514_222222_222}),
				vType:     types.T_timestamp,
				expected:  "[2024-04-18 05:51:54.111111 UTC 2024-04-18 08:38:34.222222 UTC]",
			},
			{
				typ:       parquet.Timestamp(parquet.Microsecond).Type(),
				numValues: 2,
				values:    encoding.Int64Values([]int64{1713419514_111111, 1713429514_222222}),
				vType:     types.T_timestamp,
				expected:  "[2024-04-18 05:51:54.111111 UTC 2024-04-18 08:38:34.222222 UTC]",
			},
			{
				typ:       parquet.Timestamp(parquet.Millisecond).Type(),
				numValues: 2,
				values:    encoding.Int64Values([]int64{1713419514_111, 1713429514_222}),
				vType:     types.T_timestamp,
				expected:  "[2024-04-18 05:51:54.111000 UTC 2024-04-18 08:38:34.222000 UTC]",
			},
		}
		for _, tc := range tests {
			page := tc.typ.NewPage(0, tc.numValues, tc.values)
			vec := proc.GetVector(types.New(tc.vType, 0, 0))

			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{
				"c": parquet.Leaf(tc.typ),
			})
			w := parquet.NewWriter(&buf, schema)

			values := make([]parquet.Value, page.NumRows())
			page.Values().ReadValues(values)
			_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(values)})
			convey.So(err, convey.ShouldBeNil)
			err = w.Close()
			convey.So(err, convey.ShouldBeNil)

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			convey.So(err, convey.ShouldBeNil)

			var h ParquetHandler
			err = h.getMapper(f.Root().Column("c"), plan.Type{
				Id: int32(tc.vType),
			}).mapping(page, proc, vec)
			convey.So(err, convey.ShouldBeNil)
			if tc.expected != "" {
				convey.So(vec.String(), convey.ShouldEqual, tc.expected)
			} else {
				convey.So(vec.String(), convey.ShouldEqual, fmt.Sprint(values))
			}
		}
	})
}
