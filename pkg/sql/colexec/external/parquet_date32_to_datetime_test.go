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

func TestGetMapper_Date32ToDatetime(t *testing.T) {
	proc := testutil.NewProc(t)

	t.Run("plain_date32_to_datetime", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Date(),
		})
		w := parquet.NewWriter(&buf, schema)

		// 2024-01-01 = 19723 days from Unix epoch (1970-01-01)
		// 2024-06-15 = 19889 days from Unix epoch
		// 1970-01-01 = 0 days from Unix epoch
		rows := []parquet.Row{
			{parquet.Int32Value(19723).Level(0, 0, 0)},
			{parquet.Int32Value(19889).Level(0, 0, 0)},
			{parquet.Int32Value(0).Level(0, 0, 0)},
		}
		_, err := w.WriteRows(rows)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		lt := col.Type().LogicalType()
		require.NotNil(t, lt)
		require.NotNil(t, lt.Date)

		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_datetime, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_datetime), NotNullable: true})
		require.NotNil(t, mp, "DATE32 → DATETIME should be supported")
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 3, vec.Length())

		got := vector.MustFixedColWithTypeCheck[types.Datetime](vec)
		// Verify by converting back to date and checking
		for i, days := range []int32{19723, 19889, 0} {
			expectedDatetime := types.DaysFromUnixEpochToDate(days).ToDatetime()
			require.Equal(t, expectedDatetime, got[i], "row %d mismatch", i)
		}
	})

	t.Run("dict_date32_to_datetime", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Encoded(parquet.Date(), &parquet.RLEDictionary),
		})
		w := parquet.NewWriter(&buf, schema)

		// Write with repeated values to trigger dictionary encoding
		rows := []parquet.Row{
			{parquet.Int32Value(19723).Level(0, 0, 0)},
			{parquet.Int32Value(19889).Level(0, 0, 0)},
			{parquet.Int32Value(19723).Level(0, 0, 0)},
		}
		_, err := w.WriteRows(rows)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_datetime, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_datetime), NotNullable: true})
		require.NotNil(t, mp, "DATE32 → DATETIME with dictionary should be supported")
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 3, vec.Length())

		got := vector.MustFixedColWithTypeCheck[types.Datetime](vec)
		expected0 := types.DaysFromUnixEpochToDate(19723).ToDatetime()
		expected1 := types.DaysFromUnixEpochToDate(19889).ToDatetime()
		require.Equal(t, expected0, got[0])
		require.Equal(t, expected1, got[1])
		require.Equal(t, expected0, got[2])
	})

	t.Run("date32_to_datetime_with_nulls", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Optional(parquet.Date()),
		})
		w := parquet.NewWriter(&buf, schema)

		rows := []parquet.Row{
			{parquet.Int32Value(19723).Level(0, 1, 0)},
			{parquet.NullValue().Level(0, 0, 0)},
			{parquet.Int32Value(0).Level(0, 1, 0)},
		}
		_, err := w.WriteRows(rows)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_datetime, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_datetime)})
		require.NotNil(t, mp, "Optional DATE32 → DATETIME should be supported")
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 3, vec.Length())

		require.True(t, vec.GetNulls().Contains(1), "index 1 should be NULL")
		require.False(t, vec.GetNulls().Contains(0))
		require.False(t, vec.GetNulls().Contains(2))

		got := vector.MustFixedColWithTypeCheck[types.Datetime](vec)
		require.Equal(t, types.DaysFromUnixEpochToDate(19723).ToDatetime(), got[0])
		require.Equal(t, types.DaysFromUnixEpochToDate(0).ToDatetime(), got[2])
	})

	t.Run("date32_to_datetime_negative_days", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Date(),
		})
		w := parquet.NewWriter(&buf, schema)

		// Negative days = dates before 1970-01-01
		// -1 = 1969-12-31
		rows := []parquet.Row{
			{parquet.Int32Value(-1).Level(0, 0, 0)},
			{parquet.Int32Value(-365).Level(0, 0, 0)},
		}
		_, err := w.WriteRows(rows)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_datetime, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_datetime), NotNullable: true})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 2, vec.Length())

		got := vector.MustFixedColWithTypeCheck[types.Datetime](vec)
		require.Equal(t, types.DaysFromUnixEpochToDate(-1).ToDatetime(), got[0])
		require.Equal(t, types.DaysFromUnixEpochToDate(-365).ToDatetime(), got[1])
	})

	t.Run("date32_to_datetime_null_to_not_null_error", func(t *testing.T) {
		var buf bytes.Buffer
		schema := parquet.NewSchema("x", parquet.Group{
			"c": parquet.Optional(parquet.Date()),
		})
		w := parquet.NewWriter(&buf, schema)

		rows := []parquet.Row{
			{parquet.Int32Value(19723).Level(0, 1, 0)},
			{parquet.NullValue().Level(0, 0, 0)},
		}
		_, err := w.WriteRows(rows)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		col := f.Root().Column("c")
		page, err := col.Pages().ReadPage()
		require.NoError(t, err)

		vec := vector.NewVec(types.New(types.T_datetime, 0, 0))
		var h ParquetHandler
		mp := h.getMapper(col, plan.Type{Id: int32(types.T_datetime), NotNullable: true})
		require.NotNil(t, mp)
		err = mp.mapping(page, proc, vec)
		require.Error(t, err, "should fail when loading NULL into NOT NULL column")
	})
}
