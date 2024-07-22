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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/stretchr/testify/require"
)

func Test_getMapper(t *testing.T) {
	proc := testutil.NewProc()

	t.Run("indexed string", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			// TODO: check why parquet.PlainDictionary not work
			"c": parquet.Compressed(parquet.Optional(parquet.Encoded(parquet.String(), &parquet.RLEDictionary)), &parquet.Gzip),
		})
		w := parquet.NewWriter(&buf, schema)

		long1 := strings.Repeat("xyzABC", 10)
		long2 := strings.Repeat("789$&@", 10)
		values := []parquet.Value{
			parquet.ValueOf(nil),
			parquet.ValueOf("aa"),
			parquet.ValueOf(nil),
			parquet.ValueOf("bb"),
			parquet.ValueOf("aa"),
			parquet.ValueOf(long2),
			parquet.ValueOf(long2),
			parquet.ValueOf("aa"),
			parquet.ValueOf("bb"),
			parquet.ValueOf(long1),
			parquet.ValueOf(nil),
			parquet.ValueOf(nil),
			parquet.ValueOf(long1),
		}
		for i := range values {
			v := &values[i]
			if v.IsNull() {
				values[i] = v.Level(0, 0, 0)
			} else {
				values[i] = v.Level(0, 1, 0)
			}
		}
		_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(values)})
		require.NoError(t, err)

		err = w.Close()
		require.NoError(t, err)

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := proc.GetVector(types.New(types.T_varchar, 0, 0))
		var h ParquetHandler
		err = h.getMapper(col, plan.Type{
			Id: int32(types.T_varchar),
		}).mapping(page, proc, vec)
		require.NoError(t, err)

		require.Equal(t, len(values), vec.Length())
		for i, v := range values {
			if v.IsNull() {
				require.True(t, vec.IsNull(uint64(i)))
			} else {
				require.Equal(t, v.String(), vec.GetStringAt(i))
			}
		}
	})

	tests := []struct {
		st          parquet.Type
		numValues   int
		values      encoding.Values
		dt          types.T
		expected    string
		expectedOpt string
	}{
		{
			st:          parquet.BooleanType,
			numValues:   2,
			values:      encoding.BooleanValues([]byte{2}),
			dt:          types.T_bool,
			expectedOpt: "[false false true false]-[0 3]",
		},
		{
			st:          parquet.Int32Type,
			numValues:   2,
			values:      encoding.Int32Values([]int32{1, 5}),
			dt:          types.T_int32,
			expectedOpt: "[0 1 5 0]-[0 3]",
		},
		{
			st:          parquet.Int64Type,
			numValues:   2,
			values:      encoding.Int64Values([]int64{2, 7}),
			dt:          types.T_int64,
			expectedOpt: "[0 2 7 0]-[0 3]",
		},
		{
			st:          parquet.Uint(32).Type(),
			numValues:   2,
			values:      encoding.Uint32Values([]uint32{5, 3}),
			dt:          types.T_uint32,
			expectedOpt: "[0 5 3 0]-[0 3]",
		},
		{
			st:          parquet.Uint(64).Type(),
			numValues:   2,
			values:      encoding.Uint64Values([]uint64{8, 10}),
			dt:          types.T_uint64,
			expectedOpt: "[0 8 10 0]-[0 3]",
		},
		{
			st:          parquet.Int64Type,
			numValues:   2,
			values:      encoding.Int64Values([]int64{2, 7}),
			dt:          types.T_int64,
			expectedOpt: "[0 2 7 0]-[0 3]",
		},
		// {
		// 	typ: parquet.Int96Type,
		// },
		{
			st:          parquet.FloatType,
			numValues:   2,
			values:      encoding.FloatValues([]float32{7.5, 3.2}),
			dt:          types.T_float32,
			expectedOpt: "[0 7.5 3.2 0]-[0 3]",
		},
		{
			st:          parquet.DoubleType,
			numValues:   2,
			values:      encoding.DoubleValues([]float64{77.9, 0}),
			dt:          types.T_float64,
			expectedOpt: "[0 77.9 0 0]-[0 3]",
		},
		{
			st:          parquet.String().Type(),
			numValues:   2,
			values:      encoding.ByteArrayValues([]byte("abcdefg"), []uint32{0, 3, 7}),
			dt:          types.T_varchar,
			expectedOpt: "[ abc defg ]-[0 3]",
		},
		{
			st:          parquet.FixedLenByteArrayType(3),
			numValues:   2,
			values:      encoding.FixedLenByteArrayValues([]byte("abcdef"), 3),
			dt:          types.T_char,
			expectedOpt: "[ abc def ]-[0 3]",
		},
		{
			st:          parquet.Date().Type(),
			numValues:   2,
			values:      encoding.Int32Values([]int32{357, 1245}),
			dt:          types.T_date,
			expected:    "[0001-12-24 0004-05-30]",
			expectedOpt: "[0001-01-01 0001-12-24 0004-05-30 0001-01-01]-[0 3]",
		},
		{
			st:          parquet.Time(parquet.Nanosecond).Type(),
			numValues:   2,
			values:      encoding.Int64Values([]int64{18783_111111_111, 25783_222222_222}),
			dt:          types.T_time,
			expected:    "[05:13:03 07:09:43]",
			expectedOpt: "[00:00:00 05:13:03 07:09:43 00:00:00]-[0 3]",
		},
		{
			st:          parquet.Time(parquet.Microsecond).Type(),
			numValues:   2,
			values:      encoding.Int64Values([]int64{18783_111111, 25783_222222}),
			dt:          types.T_time,
			expected:    "[05:13:03 07:09:43]",
			expectedOpt: "[00:00:00 05:13:03 07:09:43 00:00:00]-[0 3]",
		},
		{
			st:          parquet.Time(parquet.Millisecond).Type(),
			numValues:   2,
			values:      encoding.Int32Values([]int32{18783_111, 25783_222}),
			dt:          types.T_time,
			expected:    "[05:13:03 07:09:43]",
			expectedOpt: "[00:00:00 05:13:03 07:09:43 00:00:00]-[0 3]",
		},
		{
			st:          parquet.Timestamp(parquet.Nanosecond).Type(),
			numValues:   2,
			values:      encoding.Int64Values([]int64{1713419514_111111_111, 1713429514_222222_222}),
			dt:          types.T_timestamp,
			expected:    "[2024-04-18 05:51:54.111111 UTC 2024-04-18 08:38:34.222222 UTC]",
			expectedOpt: "[0001-01-01 00:00:00.000000 UTC 2024-04-18 05:51:54.111111 UTC 2024-04-18 08:38:34.222222 UTC 0001-01-01 00:00:00.000000 UTC]-[0 3]",
		},
		{
			st:          parquet.Timestamp(parquet.Microsecond).Type(),
			numValues:   2,
			values:      encoding.Int64Values([]int64{1713419514_111111, 1713429514_222222}),
			dt:          types.T_timestamp,
			expected:    "[2024-04-18 05:51:54.111111 UTC 2024-04-18 08:38:34.222222 UTC]",
			expectedOpt: "[0001-01-01 00:00:00.000000 UTC 2024-04-18 05:51:54.111111 UTC 2024-04-18 08:38:34.222222 UTC 0001-01-01 00:00:00.000000 UTC]-[0 3]",
		},
		{
			st:          parquet.Timestamp(parquet.Millisecond).Type(),
			numValues:   2,
			values:      encoding.Int64Values([]int64{1713419514_111, 1713429514_222}),
			dt:          types.T_timestamp,
			expected:    "[2024-04-18 05:51:54.111000 UTC 2024-04-18 08:38:34.222000 UTC]",
			expectedOpt: "[0001-01-01 00:00:00.000000 UTC 2024-04-18 05:51:54.111000 UTC 2024-04-18 08:38:34.222000 UTC 0001-01-01 00:00:00.000000 UTC]-[0 3]",
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s to %s not null", tc.st, tc.dt), func(t *testing.T) {
			page := tc.st.NewPage(0, tc.numValues, tc.values)

			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{
				"c": parquet.Leaf(tc.st),
			})
			w := parquet.NewWriter(&buf, schema)

			values := make([]parquet.Value, page.NumRows())
			page.Values().ReadValues(values)
			_, err := w.WriteRows([]parquet.Row{parquet.MakeRow(values)})
			require.NoError(t, err)
			err = w.Close()
			require.NoError(t, err)

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)

			vec := proc.GetVector(types.New(tc.dt, 0, 0))
			var h ParquetHandler
			err = h.getMapper(f.Root().Column("c"), plan.Type{
				Id:          int32(tc.dt),
				NotNullable: true,
			}).mapping(page, proc, vec)
			require.NoError(t, err)
			if tc.expected != "" {
				require.Equal(t, tc.expected, vec.String())
			} else {
				require.Equal(t, fmt.Sprint(values), vec.String())
			}
		})
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s to %s null", tc.st, tc.dt), func(t *testing.T) {
			var buf bytes.Buffer
			schema := parquet.NewSchema("x", parquet.Group{
				"c": parquet.Optional(parquet.Leaf(tc.st)),
			})
			w := parquet.NewWriter(&buf, schema)

			err := w.Write(nil)
			require.NoError(t, err)

			page := tc.st.NewPage(0, tc.numValues, tc.values)
			values := make([]parquet.Value, page.NumRows())
			page.Values().ReadValues(values)
			for i := range values {
				v := &values[i]
				*v = v.Level(v.RepetitionLevel(), 1, v.Column())
			}

			_, err = w.WriteRows([]parquet.Row{parquet.MakeRow(values)})
			require.NoError(t, err)

			err = w.Write(nil)
			require.NoError(t, err)

			err = w.Close()
			require.NoError(t, err)

			f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)

			vec := proc.GetVector(types.New(tc.dt, 0, 0))
			var h ParquetHandler
			mp := h.getMapper(f.Root().Column("c"), plan.Type{
				Id: int32(tc.dt),
			})

			pages := f.Root().Column("c").Pages()
			page, _ = pages.ReadPage()
			err = mp.mapping(page, proc, vec)
			require.NoError(t, err)
			if tc.expectedOpt != "" {
				require.Equal(t, tc.expectedOpt, vec.String())
			} else {
				require.Equal(t, fmt.Sprint(values), vec.String())
			}
		})
	}
}
