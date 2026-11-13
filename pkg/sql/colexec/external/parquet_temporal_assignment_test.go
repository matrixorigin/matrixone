// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// loadTemporalProc builds a process wired to the session timezone and SQL mode
// used by the temporal assignment tests.
func loadTemporalProc(t *testing.T, tz *time.Location, sqlMode string) *process.Process {
	t.Helper()
	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.TimeZone = tz
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return sqlMode, nil
	})
	return proc
}

// localTimestampMicrosModel writes an int64 TIMESTAMP(MICROS) leaf with
// isAdjustedToUTC=false through the documented schema tag, so the local
// wall-clock branch of the timestamp mappers can be exercised end to end.
type localTimestampMicrosModel struct {
	Value int64 `parquet:"local_ts,timestamp(microsecond:local)"`
}

func writeLocalTimestampPage(t *testing.T, micros []int64) (*parquet.File, parquet.Page) {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.SchemaOf(localTimestampMicrosModel{})
	w := parquet.NewWriter(&buf, schema)
	rows := make([]parquet.Row, 0, len(micros))
	for _, m := range micros {
		rows = append(rows, parquet.Row{parquet.Int64Value(m).Level(0, 0, 0)})
	}
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	col := f.Root().Column("local_ts")
	require.NotNil(t, col)
	lt := col.Type().LogicalType()
	require.NotNil(t, lt)
	require.NotNil(t, lt.Timestamp)
	require.False(t, lt.Timestamp.IsAdjustedToUTC, "fixture must be a local wall-clock timestamp")
	pages := col.Pages()
	defer func() { require.NoError(t, pages.Close()) }()
	page, err := pages.ReadPage()
	require.NoError(t, err)
	require.NotNil(t, page)
	return f, page
}

// TestParquetLoadTemporalBounds pins the SQL calendar upper bound for numeric
// Parquet DATE/TIMESTAMP sources under LOAD, mirroring the rejection the SQL
// parser applies to year 10000 spellings. Lower-bound and zero-date policies
// belong to the insert layer and are intentionally not re-enforced here.
func TestParquetLoadTemporalBounds(t *testing.T) {
	const (
		// 9999-12-31 23:59:59.999999 UTC
		microsMaxSqlCalendar = 253402300799999999
		// 10000-01-01 00:00:00 UTC
		microsYear10000 = 253402300800000000
		// 9999-12-31 15:00:00 UTC: at +14:00 this is local 10000-01-01 05:00.
		microsUtcTailPlus14 = 253402268000000000
		// 10000-01-01 05:00:00 UTC: at -12:00 this is local 9999-12-31 17:00.
		microsUtcHeadMinus12 = 253402322800000000
	)
	daysYear10000 := int32(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / int64(86400))
	daysMaxSqlCalendar := int32(time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC).Unix() / int64(86400))
	days2024 := int32(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / int64(86400))

	utc := time.UTC
	plus14 := time.FixedZone("+14:00", 14*3600)
	minus12 := time.FixedZone("-12:00", -12*3600)

	rowOfInt64 := func(values ...int64) []parquet.Row {
		rows := make([]parquet.Row, 0, len(values))
		for _, v := range values {
			rows = append(rows, parquet.Row{parquet.Int64Value(v).Level(0, 0, 0)})
		}
		return rows
	}
	rowOfInt32 := func(values ...int32) []parquet.Row {
		rows := make([]parquet.Row, 0, len(values))
		for _, v := range values {
			rows = append(rows, parquet.Row{parquet.Int32Value(v).Level(0, 0, 0)})
		}
		return rows
	}

	t.Run("adjusted micros max passes at utc", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		f, page := writeColumnAndGetPage(t, parquet.Timestamp(parquet.Microsecond), rowOfInt64(microsMaxSqlCalendar))
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), Scale: 6, NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 6))
		defer vec.Free(proc.Mp())
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, []types.Timestamp{types.UnixMicroToTimestamp(microsMaxSqlCalendar)},
			vector.MustFixedColWithTypeCheck[types.Timestamp](vec))
	})

	t.Run("adjusted micros year10000 rejected", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		f, page := writeColumnAndGetPage(t, parquet.Timestamp(parquet.Microsecond), rowOfInt64(microsYear10000))
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), Scale: 6, NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 6))
		defer vec.Free(proc.Mp())
		err := mp.mapping(page, proc, vec)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Equal(t, 0, vec.Length(), "rejected page must not leave rows in the target vector")
	})

	t.Run("utc tail becomes local year10000 at plus14", func(t *testing.T) {
		proc := loadTemporalProc(t, plus14, "")
		f, page := writeColumnAndGetPage(t, parquet.Timestamp(parquet.Microsecond), rowOfInt64(microsUtcTailPlus14))
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), Scale: 6, NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 6))
		defer vec.Free(proc.Mp())
		err := mp.mapping(page, proc, vec)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Equal(t, 0, vec.Length())
	})

	t.Run("utc head stays local year9999 at minus12", func(t *testing.T) {
		proc := loadTemporalProc(t, minus12, "")
		f, page := writeColumnAndGetPage(t, parquet.Timestamp(parquet.Microsecond), rowOfInt64(microsUtcHeadMinus12))
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), Scale: 6, NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 6))
		defer vec.Free(proc.Mp())
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, []types.Timestamp{types.UnixMicroToTimestamp(microsUtcHeadMinus12)},
			vector.MustFixedColWithTypeCheck[types.Timestamp](vec))
	})

	t.Run("local wall clock year10000 rejected", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		f, page := writeLocalTimestampPage(t, []int64{microsYear10000})
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("local_ts"), plan.Type{Id: int32(types.T_timestamp), Scale: 6, NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 6))
		defer vec.Free(proc.Mp())
		err := mp.mapping(page, proc, vec)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Equal(t, 0, vec.Length())
	})

	t.Run("local wall clock max accepted", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		f, page := writeLocalTimestampPage(t, []int64{microsMaxSqlCalendar})
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("local_ts"), plan.Type{Id: int32(types.T_timestamp), Scale: 6, NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 6))
		defer vec.Free(proc.Mp())
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t,
			[]types.Timestamp{parquetLocalTimestampMicrosToTimestamp(microsMaxSqlCalendar, time.UTC)},
			vector.MustFixedColWithTypeCheck[types.Timestamp](vec))
	})

	t.Run("date32 bounds per date datetime timestamp target", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			days   int32
			reject bool
		}{
			{name: "year10000", days: daysYear10000, reject: true},
			{name: "maxSqlCalendar", days: daysMaxSqlCalendar, reject: false},
			{name: "int32 overflow", days: math.MaxInt32, reject: true},
			{name: "int32 underflow", days: math.MinInt32, reject: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				proc := loadTemporalProc(t, utc, "")
				f, page := writeColumnAndGetPage(t, parquet.Date(), rowOfInt32(tc.days))
				for _, target := range []plan.Type{
					{Id: int32(types.T_date), NotNullable: true},
					{Id: int32(types.T_datetime), Scale: 6, NotNullable: true},
					{Id: int32(types.T_timestamp), Scale: 6, NotNullable: true},
				} {
					var h ParquetHandler
					h.timeAssignment = true
					mp := h.getMapper(f.Root().Column("c"), target)
					require.NotNil(t, mp, "target %s", types.T(target.Id))
					vec := vector.NewVec(types.New(types.T(target.Id), 0, target.Scale))
					err := mp.mapping(page, proc, vec)
					if tc.reject {
						require.Error(t, err, "target %s", types.T(target.Id))
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput),
							"target %s: %v", types.T(target.Id), err)
						require.Equal(t, 0, vec.Length())
					} else {
						require.NoError(t, err, "target %s", types.T(target.Id))
						d := types.DaysFromUnixEpochToDate(tc.days)
						switch types.T(target.Id) {
						case types.T_date:
							require.Equal(t, []types.Date{d}, vector.MustFixedColWithTypeCheck[types.Date](vec))
						case types.T_datetime:
							require.Equal(t, []types.Datetime{d.ToDatetime()},
								vector.MustFixedColWithTypeCheck[types.Datetime](vec))
						case types.T_timestamp:
							require.Equal(t, []types.Timestamp{d.ToTimestamp(utc)},
								vector.MustFixedColWithTypeCheck[types.Timestamp](vec))
						}
					}
					vec.Free(proc.Mp())
				}
			})
		}
	})

	t.Run("plain date32 multirow pass", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		f, page := writeColumnAndGetPage(t, parquet.Date(), rowOfInt32(days2024, daysMaxSqlCalendar))
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_date), NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.T_date.ToType())
		defer vec.Free(proc.Mp())
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, []types.Date{
			types.DaysFromUnixEpochToDate(days2024),
			types.DaysFromUnixEpochToDate(daysMaxSqlCalendar),
		}, vector.MustFixedColWithTypeCheck[types.Date](vec))
	})

	t.Run("timestamp to date year10000 rejected", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		f, page := writeColumnAndGetPage(t, parquet.Timestamp(parquet.Microsecond), rowOfInt64(microsYear10000))
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_date), NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.T_date.ToType())
		defer vec.Free(proc.Mp())
		err := mp.mapping(page, proc, vec)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Equal(t, 0, vec.Length())
	})

	t.Run("timestamp to date in range", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		micros := time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC).UnixMicro()
		f, page := writeColumnAndGetPage(t, parquet.Timestamp(parquet.Microsecond), rowOfInt64(micros))
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_date), NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.T_date.ToType())
		defer vec.Free(proc.Mp())
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, []types.Date{types.DaysFromUnixEpochToDate(days2024)},
			vector.MustFixedColWithTypeCheck[types.Date](vec))
	})

	t.Run("datetime target year10000 rejected", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		f, page := writeColumnAndGetPage(t, parquet.Timestamp(parquet.Microsecond),
			rowOfInt64(microsMaxSqlCalendar, microsYear10000))
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_datetime), Scale: 6, NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.New(types.T_datetime, 0, 6))
		defer vec.Free(proc.Mp())
		err := mp.mapping(page, proc, vec)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Equal(t, 0, vec.Length())
	})

	t.Run("raw micros overflow rejected", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		f, page := writeColumnAndGetPage(t, parquet.Timestamp(parquet.Microsecond), rowOfInt64(math.MaxInt64))
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), Scale: 6, NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 6))
		defer vec.Free(proc.Mp())
		err := mp.mapping(page, proc, vec)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Equal(t, 0, vec.Length())
	})

	t.Run("millis overflow stays rejected", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		f, page := writeColumnAndGetPage(t, parquet.Timestamp(parquet.Millisecond), rowOfInt64(math.MaxInt64))
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), Scale: 6, NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 6))
		defer vec.Free(proc.Mp())
		err := mp.mapping(page, proc, vec)
		require.Error(t, err)
		require.Equal(t, 0, vec.Length())
	})

	t.Run("nanos max remains representable", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		// int64 nanoseconds span less than 300 years: the max raw value maps
		// to a valid 2262 datetime and must not be rejected by the year bound.
		f, page := writeColumnAndGetPage(t, parquet.Timestamp(parquet.Nanosecond), rowOfInt64(math.MaxInt64))
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), Scale: 6, NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 6))
		defer vec.Free(proc.Mp())
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, []types.Timestamp{types.UnixMicroToTimestamp(math.MaxInt64 / 1000)},
			vector.MustFixedColWithTypeCheck[types.Timestamp](vec))
	})

	t.Run("dict page slices validate only consumed rows", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		node := parquet.Encoded(parquet.Timestamp(parquet.Microsecond), &parquet.RLEDictionary)
		good := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
		f, page := writeColumnAndGetPage(t, node, rowOfInt64(good, microsYear10000, good))
		require.NotNil(t, page.Dictionary(), "fixture must use dictionary encoding")

		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), Scale: 6, NotNullable: true})
		require.NotNil(t, mp)

		// The dictionary holds an out-of-range entry, but the sliced page
		// never references it: only consumed logical rows may fail.
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 6))
		require.NoError(t, mp.mapping(page.Slice(0, 1), proc, vec))
		require.Equal(t, []types.Timestamp{types.UnixMicroToTimestamp(good)},
			vector.MustFixedColWithTypeCheck[types.Timestamp](vec))
		vec.Free(proc.Mp())

		vec2 := vector.NewVec(types.New(types.T_timestamp, 0, 6))
		err := mp.mapping(page, proc, vec2)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Equal(t, 0, vec2.Length())
		vec2.Free(proc.Mp())
	})

	t.Run("null only page passes", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		node := parquet.Optional(parquet.Timestamp(parquet.Microsecond))
		rows := []parquet.Row{
			{parquet.NullValue().Level(0, 0, 0)},
			{parquet.NullValue().Level(0, 0, 0)},
		}
		f, page := writeColumnAndGetPage(t, node, rows)
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), Scale: 6})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 6))
		defer vec.Free(proc.Mp())
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 2, vec.Length())
		require.True(t, vec.GetNulls().Contains(0))
		require.True(t, vec.GetNulls().Contains(1))
	})

	t.Run("scale0 rounding carry to year10000 rejected", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		f, page := writeColumnAndGetPage(t, parquet.Timestamp(parquet.Microsecond), rowOfInt64(microsMaxSqlCalendar))
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), Scale: 0, NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 0))
		defer vec.Free(proc.Mp())
		err := mp.mapping(page, proc, vec)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Equal(t, 0, vec.Length())
	})

	t.Run("scale0 truncation stays in calendar", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "TIME_TRUNCATE_FRACTIONAL")
		f, page := writeColumnAndGetPage(t, parquet.Timestamp(parquet.Microsecond), rowOfInt64(microsMaxSqlCalendar))
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), Scale: 0, NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 0))
		defer vec.Free(proc.Mp())
		require.NoError(t, mp.mapping(page, proc, vec))
		want := types.UnixMicroToTimestamp(microsMaxSqlCalendar).TruncateToScaleWithoutRounding(0)
		require.Equal(t, []types.Timestamp{want},
			vector.MustFixedColWithTypeCheck[types.Timestamp](vec))
	})

	t.Run("foreign read keeps raw mapping", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		good := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
		f, page := writeColumnAndGetPage(t, parquet.Timestamp(parquet.Microsecond), rowOfInt64(good, microsYear10000))
		var h ParquetHandler
		h.timeAssignment = false
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), Scale: 0, NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 0))
		defer vec.Free(proc.Mp())
		require.NoError(t, mp.mapping(page, proc, vec))
		// External-table scans keep the existing raw mapping: no bounds and
		// no target-scale normalization outside LOAD.
		require.Equal(t, []types.Timestamp{
			types.UnixMicroToTimestamp(good),
			types.UnixMicroToTimestamp(microsYear10000),
		}, vector.MustFixedColWithTypeCheck[types.Timestamp](vec))
	})
}

// TestParquetLoadTemporalScaleAssignment pins target-scale assignment parity
// for numeric TIMESTAMP sources under LOAD: default SQL mode rounds half-step
// fractional seconds and carries into the next second, while
// TIME_TRUNCATE_FRACTIONAL discards the excess digits.
func TestParquetLoadTemporalScaleAssignment(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	offsets := []int64{0, 499999, 500000, 123499, 123500}
	utc := time.UTC

	for _, target := range []struct {
		oid   types.T
		scale int32
	}{
		{types.T_timestamp, 0}, {types.T_timestamp, 3}, {types.T_timestamp, 6},
		{types.T_datetime, 0}, {types.T_datetime, 3}, {types.T_datetime, 6},
	} {
		for _, mode := range []struct {
			name     string
			sqlMode  string
			truncate bool
		}{{"default", "", false}, {"truncate", "TIME_TRUNCATE_FRACTIONAL", true}} {
			t.Run(fmt.Sprintf("%s%d/%s", target.oid, target.scale, mode.name), func(t *testing.T) {
				proc := loadTemporalProc(t, utc, mode.sqlMode)
				node := parquet.Timestamp(parquet.Microsecond)
				rows := make([]parquet.Row, 0, len(offsets))
				for _, off := range offsets {
					rows = append(rows, parquet.Row{parquet.Int64Value(base+off).Level(0, 0, 0)})
				}
				f, page := writeColumnAndGetPage(t, node, rows)
				var h ParquetHandler
				h.timeAssignment = true
				mp := h.getMapper(f.Root().Column("c"),
					plan.Type{Id: int32(target.oid), Scale: target.scale, NotNullable: true})
				require.NotNil(t, mp)
				vec := vector.NewVec(types.New(target.oid, 0, target.scale))
				defer vec.Free(proc.Mp())
				require.NoError(t, mp.mapping(page, proc, vec))
				require.Equal(t, len(offsets), vec.Length())
				switch target.oid {
				case types.T_timestamp:
					want := make([]types.Timestamp, 0, len(offsets))
					for _, off := range offsets {
						ts := types.UnixMicroToTimestamp(base + off)
						if mode.truncate {
							want = append(want, ts.TruncateToScaleWithoutRounding(target.scale))
						} else {
							want = append(want, ts.TruncateToScale(target.scale))
						}
					}
					require.Equal(t, want, vector.MustFixedColWithTypeCheck[types.Timestamp](vec))
				case types.T_datetime:
					want := make([]types.Datetime, 0, len(offsets))
					for _, off := range offsets {
						dt := types.Datetime(types.UnixMicroToTimestamp(base + off))
						if mode.truncate {
							want = append(want, dt.TruncateToScaleWithoutRounding(target.scale))
						} else {
							want = append(want, dt.TruncateToScale(target.scale))
						}
					}
					require.Equal(t, want, vector.MustFixedColWithTypeCheck[types.Datetime](vec))
				}
			})
		}
	}

	t.Run("dict encoded scale0 default mode", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		node := parquet.Encoded(parquet.Timestamp(parquet.Microsecond), &parquet.RLEDictionary)
		rows := make([]parquet.Row, 0, len(offsets))
		for _, off := range offsets {
			rows = append(rows, parquet.Row{parquet.Int64Value(base+off).Level(0, 0, 0)})
		}
		f, page := writeColumnAndGetPage(t, node, rows)
		require.NotNil(t, page.Dictionary(), "fixture must use dictionary encoding")
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), Scale: 0, NotNullable: true})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 0))
		defer vec.Free(proc.Mp())
		require.NoError(t, mp.mapping(page, proc, vec))
		want := make([]types.Timestamp, 0, len(offsets))
		for _, off := range offsets {
			want = append(want, types.UnixMicroToTimestamp(base+off).TruncateToScale(0))
		}
		require.Equal(t, want, vector.MustFixedColWithTypeCheck[types.Timestamp](vec))
	})

	t.Run("null rows keep assignment alignment", func(t *testing.T) {
		proc := loadTemporalProc(t, utc, "")
		node := parquet.Optional(parquet.Timestamp(parquet.Microsecond))
		rows := []parquet.Row{
			{parquet.Int64Value(base+500000).Level(0, 1, 0)},
			{parquet.NullValue().Level(0, 0, 0)},
			{parquet.Int64Value(base+123499).Level(0, 1, 0)},
		}
		f, page := writeColumnAndGetPage(t, node, rows)
		var h ParquetHandler
		h.timeAssignment = true
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_timestamp), Scale: 0})
		require.NotNil(t, mp)
		vec := vector.NewVec(types.New(types.T_timestamp, 0, 0))
		defer vec.Free(proc.Mp())
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 3, vec.Length())
		require.True(t, vec.GetNulls().Contains(1))
		require.False(t, vec.GetNulls().Contains(0))
		require.False(t, vec.GetNulls().Contains(2))
		require.Equal(t, []types.Timestamp{
			types.UnixMicroToTimestamp(base + 500000).TruncateToScale(0),
			0,
			types.UnixMicroToTimestamp(base + 123499).TruncateToScale(0),
		}, vector.MustFixedColWithTypeCheck[types.Timestamp](vec))
	})
}
