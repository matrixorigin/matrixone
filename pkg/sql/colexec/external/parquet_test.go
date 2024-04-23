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
	convey.Convey("getMapper", t, func() {
		proc := testutil.NewProc()

		tests := []struct {
			typ         parquet.Type
			numValues   int
			values      encoding.Values
			vType       types.T
			expected    string
			expectedOpt string
		}{
			{
				typ:         parquet.BooleanType,
				numValues:   2,
				values:      encoding.BooleanValues([]byte{2}),
				vType:       types.T_bool,
				expectedOpt: "[false false true false]-[0 3]",
			},
			{
				typ:         parquet.Int32Type,
				numValues:   2,
				values:      encoding.Int32Values([]int32{1, 5}),
				vType:       types.T_int32,
				expectedOpt: "[0 1 5 0]-[0 3]",
			},
			{
				typ:         parquet.Int64Type,
				numValues:   2,
				values:      encoding.Int64Values([]int64{2, 7}),
				vType:       types.T_int64,
				expectedOpt: "[0 2 7 0]-[0 3]",
			},
			// {
			// 	typ: parquet.Int96Type,
			// },
			{
				typ:         parquet.FloatType,
				numValues:   2,
				values:      encoding.FloatValues([]float32{7.5, 3.2}),
				vType:       types.T_float32,
				expectedOpt: "[0 7.5 3.2 0]-[0 3]",
			},
			{
				typ:         parquet.DoubleType,
				numValues:   2,
				values:      encoding.DoubleValues([]float64{77.9, 0}),
				vType:       types.T_float64,
				expectedOpt: "[0 77.9 0 0]-[0 3]",
			},
			{
				typ:         parquet.ByteArrayType,
				numValues:   2,
				values:      encoding.ByteArrayValues([]byte("abcdefg"), []uint32{0, 3, 7}),
				vType:       types.T_varchar,
				expectedOpt: "[ abc defg ]-[0 3]",
			},
			{
				typ:         parquet.FixedLenByteArrayType(3),
				numValues:   2,
				values:      encoding.FixedLenByteArrayValues([]byte("abcdef"), 3),
				vType:       types.T_char,
				expectedOpt: "[ abc def ]-[0 3]",
			},
			{
				typ:         parquet.Date().Type(),
				numValues:   2,
				values:      encoding.Int32Values([]int32{357, 1245}),
				vType:       types.T_date,
				expected:    "[0001-12-24 0004-05-30]",
				expectedOpt: "[0001-01-01 0001-12-24 0004-05-30 0001-01-01]-[0 3]",
			},
			{
				typ:         parquet.Time(parquet.Nanosecond).Type(),
				numValues:   2,
				values:      encoding.Int64Values([]int64{18783_111111_111, 25783_222222_222}),
				vType:       types.T_time,
				expected:    "[05:13:03 07:09:43]",
				expectedOpt: "[00:00:00 05:13:03 07:09:43 00:00:00]-[0 3]",
			},
			{
				typ:         parquet.Time(parquet.Microsecond).Type(),
				numValues:   2,
				values:      encoding.Int64Values([]int64{18783_111111, 25783_222222}),
				vType:       types.T_time,
				expected:    "[05:13:03 07:09:43]",
				expectedOpt: "[00:00:00 05:13:03 07:09:43 00:00:00]-[0 3]",
			},
			{
				typ:         parquet.Time(parquet.Millisecond).Type(),
				numValues:   2,
				values:      encoding.Int32Values([]int32{18783_111, 25783_222}),
				vType:       types.T_time,
				expected:    "[05:13:03 07:09:43]",
				expectedOpt: "[00:00:00 05:13:03 07:09:43 00:00:00]-[0 3]",
			},
			{
				typ:         parquet.Timestamp(parquet.Nanosecond).Type(),
				numValues:   2,
				values:      encoding.Int64Values([]int64{1713419514_111111_111, 1713429514_222222_222}),
				vType:       types.T_timestamp,
				expected:    "[2024-04-18 05:51:54.111111 UTC 2024-04-18 08:38:34.222222 UTC]",
				expectedOpt: "[0001-01-01 00:00:00.000000 UTC 2024-04-18 05:51:54.111111 UTC 2024-04-18 08:38:34.222222 UTC 0001-01-01 00:00:00.000000 UTC]-[0 3]",
			},
			{
				typ:         parquet.Timestamp(parquet.Microsecond).Type(),
				numValues:   2,
				values:      encoding.Int64Values([]int64{1713419514_111111, 1713429514_222222}),
				vType:       types.T_timestamp,
				expected:    "[2024-04-18 05:51:54.111111 UTC 2024-04-18 08:38:34.222222 UTC]",
				expectedOpt: "[0001-01-01 00:00:00.000000 UTC 2024-04-18 05:51:54.111111 UTC 2024-04-18 08:38:34.222222 UTC 0001-01-01 00:00:00.000000 UTC]-[0 3]",
			},
			{
				typ:         parquet.Timestamp(parquet.Millisecond).Type(),
				numValues:   2,
				values:      encoding.Int64Values([]int64{1713419514_111, 1713429514_222}),
				vType:       types.T_timestamp,
				expected:    "[2024-04-18 05:51:54.111000 UTC 2024-04-18 08:38:34.222000 UTC]",
				expectedOpt: "[0001-01-01 00:00:00.000000 UTC 2024-04-18 05:51:54.111000 UTC 2024-04-18 08:38:34.222000 UTC 0001-01-01 00:00:00.000000 UTC]-[0 3]",
			},
		}
		for _, tc := range tests {
			page := tc.typ.NewPage(0, tc.numValues, tc.values)

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

			vec := proc.GetVector(types.New(tc.vType, 0, 0))
			var h ParquetHandler
			err = h.getMapper(f.Root().Column("c"), plan.Type{
				Id:          int32(tc.vType),
				NotNullable: true,
			}).mapping(page, proc, vec)
			convey.So(err, convey.ShouldBeNil)
			if tc.expected != "" {
				convey.So(vec.String(), convey.ShouldEqual, tc.expected)
			} else {
				convey.So(vec.String(), convey.ShouldEqual, fmt.Sprint(values))
			}
		}

		for _, tc := range tests {
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{
				"c": parquet.Optional(parquet.Leaf(tc.typ)),
			})
			w := parquet.NewWriter(&buf, schema)

			err := w.Write(nil)
			convey.So(err, convey.ShouldBeNil)

			page := tc.typ.NewPage(0, tc.numValues, tc.values)
			values := make([]parquet.Value, page.NumRows())
			page.Values().ReadValues(values)
			for i := range values {
				v := &values[i]
				*v = v.Level(v.RepetitionLevel(), 1, v.Column())
			}

			_, err = w.WriteRows([]parquet.Row{parquet.MakeRow(values)})
			convey.So(err, convey.ShouldBeNil)

			err = w.Write(nil)
			convey.So(err, convey.ShouldBeNil)

			err = w.Close()
			convey.So(err, convey.ShouldBeNil)

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			convey.So(err, convey.ShouldBeNil)

			vec := proc.GetVector(types.New(tc.vType, 0, 0))
			var h ParquetHandler
			mp := h.getMapper(f.Root().Column("c"), plan.Type{
				Id: int32(tc.vType),
			})

			pages := f.Root().Column("c").Pages()
			page, _ = pages.ReadPage()
			err = mp.mapping(page, proc, vec)
			convey.So(err, convey.ShouldBeNil)
			if tc.expectedOpt != "" {
				convey.So(vec.String(), convey.ShouldEqual, tc.expectedOpt)
			} else {
				convey.So(vec.String(), convey.ShouldEqual, fmt.Sprint(values))
			}
		}
	})
}
