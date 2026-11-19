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

package issues

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// issue31913UnionDateAndTime covers #31913: a set-operation column that
// combines DATE and TIME must be typed DATETIME (MySQL semantics) so the
// time-of-day survives, keep the widest fractional-second precision,
// deduplicate correctly under UNION DISTINCT, and persist as datetime
// through CTAS. The assertions extract the time-of-day with
// HOUR/MINUTE/SECOND/MICROSECOND so they do not depend on the current
// execution date that a TIME value takes when widened.
func issue31913UnionDateAndTime(t *testing.T, conn *sql.Conn) {
	t.Helper()
	const dbName = "issue_31913_union_date_time"
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()

	_, err := conn.ExecContext(ctx, "create database if not exists "+dbName)
	require.NoError(t, err)
	defer dropOwnedDatabase(t, conn, dbName)
	_, err = conn.ExecContext(ctx, "use "+dbName)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx,
		"drop table if exists a_31913, b_31913, c_31913, c3_31913, ct_31913, cd_31913")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "create table a_31913 (d date)")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "create table b_31913 (t time(6))")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "insert into a_31913 values ('2024-01-01')")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx,
		"insert into b_31913 values ('10:30:00'), ('23:00:00.123456'), ('10:30:00')")
	require.NoError(t, err)

	// UNION ALL keeps every row and the time-of-day of every TIME value.
	hmsu := func(query string) [][4]int64 {
		rows, err := conn.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()
		var out [][4]int64
		for rows.Next() {
			var h, m, s, us int64
			require.NoError(t, rows.Scan(&h, &m, &s, &us))
			out = append(out, [4]int64{h, m, s, us})
		}
		require.NoError(t, rows.Err())
		return out
	}
	unionAll := "select d as x from a_31913 union all select t from b_31913"
	require.Equal(t, [][4]int64{
		{0, 0, 0, 0},
		{10, 30, 0, 0},
		{10, 30, 0, 0},
		{23, 0, 0, 123456},
	}, hmsu("select hour(x), minute(x), second(x), microsecond(x) from ("+unionAll+") u order by 1, 2, 3, 4"),
		"UNION ALL must keep all rows and the time-of-day")

	// UNION DISTINCT deduplicates in the widened domain, exactly like the
	// two equal TIME values and never across the date part.
	var distinctCount int64
	require.NoError(t, conn.QueryRowContext(ctx,
		"select count(*) from (select d as x from a_31913 union select t from b_31913) u").Scan(&distinctCount))
	require.Equal(t, int64(3), distinctCount, "UNION DISTINCT must keep the distinct widened values")
	require.Equal(t, [][4]int64{
		{0, 0, 0, 0},
		{10, 30, 0, 0},
		{23, 0, 0, 123456},
	}, hmsu("select hour(x), minute(x), second(x), microsecond(x) from (select d as x from a_31913 union select t from b_31913) u order by 1, 2, 3, 4"))

	// Reversed branch order produces the same column contract.
	require.Equal(t, [][4]int64{
		{0, 0, 0, 0},
		{10, 30, 0, 0},
		{10, 30, 0, 0},
		{23, 0, 0, 123456},
	}, hmsu("select hour(x), minute(x), second(x), microsecond(x) from (select t as x from b_31913 union all select d from a_31913) u order by 1, 2, 3, 4"))

	// CTAS persists the widened datetime type with the widest FSP and the
	// values keep their time-of-day.
	_, err = conn.ExecContext(ctx, "create table c_31913 as "+unionAll)
	require.NoError(t, err)
	var dataType string
	var datetimePrecision int64
	require.NoError(t, conn.QueryRowContext(ctx,
		"select lower(data_type), datetime_precision from information_schema.columns where table_schema = database() and table_name = 'c_31913' and column_name = 'x'").
		Scan(&dataType, &datetimePrecision))
	require.Equal(t, "datetime", dataType)
	require.Equal(t, int64(6), datetimePrecision, "the widest fractional-second precision must persist")
	var tableName, showCreate string
	require.NoError(t, conn.QueryRowContext(ctx, "show create table c_31913").Scan(&tableName, &showCreate))
	require.Equal(t, "c_31913", tableName)
	require.Contains(t, showCreate, "datetime")
	require.Equal(t, [][4]int64{
		{0, 0, 0, 0},
		{10, 30, 0, 0},
		{10, 30, 0, 0},
		{23, 0, 0, 123456},
	}, hmsu("select hour(x), minute(x), second(x), microsecond(x) from c_31913 order by 1, 2, 3, 4"))

	// A three-branch date/time/datetime union and the TIME(6) widening.
	threeBranch := "select cast('2024-01-01' as date) as x union all select cast('10:30:00' as time) as x union all select cast('2024-01-02 10:11:12.123' as datetime(3)) as x"
	require.Equal(t, [][4]int64{
		{0, 0, 0, 0},
		{10, 11, 12, 123000},
		{10, 30, 0, 0},
	}, hmsu("select hour(x), minute(x), second(x), microsecond(x) from ("+threeBranch+") u order by 1, 2, 3, 4"))
	_, err = conn.ExecContext(ctx, "create table c3_31913 as "+threeBranch)
	require.NoError(t, err)
	require.NoError(t, conn.QueryRowContext(ctx,
		"select lower(data_type), datetime_precision from information_schema.columns where table_schema = database() and table_name = 'c3_31913' and column_name = 'x'").
		Scan(&dataType, &datetimePrecision))
	require.Equal(t, "datetime", dataType)
	require.Equal(t, int64(3), datetimePrecision)

	// A pure-NULL branch contributes no type of its own but keeps its rows.
	var total, nullRows int64
	rows, err := conn.QueryContext(ctx,
		"select x is null from (select null as x union all select cast('2024-01-01' as date) union all select cast('10:30:00' as time)) u")
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		total++
		var isNull bool
		require.NoError(t, rows.Scan(&isNull))
		if isNull {
			nullRows++
		}
	}
	require.NoError(t, rows.Err())
	require.Equal(t, int64(3), total)
	require.Equal(t, int64(1), nullRows)

	// Empty branches keep a typed statement with no rows.
	var empty int
	erows, err := conn.QueryContext(ctx,
		"select x from (select d as x from a_31913 where false union all select t from b_31913 where false) u")
	require.NoError(t, err)
	defer erows.Close()
	for erows.Next() {
		empty++
	}
	require.NoError(t, erows.Err())
	require.Equal(t, 0, empty)

	// INTERSECT and EXCEPT keep the same widened column contract; the
	// DATE literal intentionally predates any current date so the widened
	// values never coincide through the TIME branch's current date part.
	var n int64
	require.NoError(t, conn.QueryRowContext(ctx,
		"select count(*) from (select cast('2000-01-01' as date) intersect select cast('10:30:00' as time)) u").Scan(&n))
	require.Equal(t, int64(0), n, "INTERSECT of disjoint DATE and TIME widened values is empty")
	require.NoError(t, conn.QueryRowContext(ctx,
		"select count(*) from (select cast('2000-01-01' as date) except select cast('10:30:00' as time)) u").Scan(&n))
	require.Equal(t, int64(1), n, "EXCEPT keeps the DATE row the TIME branch does not contain")

	// Same-family controls: unchanged types persist through CTAS.
	_, err = conn.ExecContext(ctx, "create table ct_31913 as select t as x from b_31913 union select t from b_31913")
	require.NoError(t, err)
	require.NoError(t, conn.QueryRowContext(ctx,
		"select lower(data_type) from information_schema.columns where table_schema = database() and table_name = 'ct_31913' and column_name = 'x'").
		Scan(&dataType))
	require.Equal(t, "time", dataType)
	_, err = conn.ExecContext(ctx, "create table cd_31913 as select d as x from a_31913 union select d from a_31913")
	require.NoError(t, err)
	require.NoError(t, conn.QueryRowContext(ctx,
		"select lower(data_type) from information_schema.columns where table_schema = database() and table_name = 'cd_31913' and column_name = 'x'").
		Scan(&dataType))
	require.Equal(t, "date", dataType)
}
