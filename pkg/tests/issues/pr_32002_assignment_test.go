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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestPR32002ReviewBoundaries(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer func() { require.NoError(t, db.Close()) }()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() { require.NoError(t, conn.Close()) }()
		name := testutils.GetDatabaseName(t)
		_, err = conn.ExecContext(ctx, "create database `"+name+"`")
		require.NoError(t, err)
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), time.Minute)
			defer done()
			_, dropErr := conn.ExecContext(cleanup, "drop database if exists `"+name+"`")
			require.NoError(t, dropErr)
		}()
		_, err = conn.ExecContext(ctx, "use `"+name+"`")
		require.NoError(t, err)

		exec := func(query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		warningCodes := func() []int {
			t.Helper()
			rows, err := conn.QueryContext(ctx, "show warnings")
			require.NoError(t, err)
			defer rows.Close()
			var codes []int
			for rows.Next() {
				var level, message string
				var code int
				require.NoError(t, rows.Scan(&level, &code, &message))
				require.True(t, utf8.ValidString(message), "warning %d contains malformed UTF-8", code)
				codes = append(codes, code)
			}
			require.NoError(t, rows.Err())
			return codes
		}

		exec("create table percentile_inputs (g int, v decimal(38,0))")
		exec("insert into percentile_inputs values (1,1),(1,2),(2,2),(2,3),(3,NULL)")
		const grouped = "select g,median(v) m from percentile_inputs group by g"
		exec("create view percentile_medians as " + grouped)
		for _, query := range []string{
			"select approx_percentile(m, 0.5) from (" + grouped + ") t",
			"select approx_percentile(m, 0.5) from percentile_medians",
		} {
			var value string
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&value), query)
			require.Equal(t, "2.0", value)
		}

		checkIntegerAssignment := func(warning, expected string) {
			t.Helper()
			func() {
				rows, err := conn.QueryContext(ctx, "show warnings")
				require.NoError(t, err)
				defer rows.Close()
				require.True(t, rows.Next())
				var level, message string
				var code uint16
				require.NoError(t, rows.Scan(&level, &code, &message))
				require.Equal(t, "Warning", level)
				require.Equal(t, uint16(moerr.ER_WARN_DATA_OUT_OF_RANGE), code)
				require.Equal(t, warning, message)
				require.False(t, rows.Next(), "one clipped value produces one warning")
				require.NoError(t, rows.Err())
			}()
			var values string
			require.NoError(t, conn.QueryRowContext(ctx, "select group_concat(v order by v) from row_warning").Scan(&values))
			require.Equal(t, expected, values)
		}
		for _, tc := range []struct {
			typ, warning, stored string
			inputs               []string
		}{
			{"tinyint", "TINYINT assignment", "1,127", []string{"'300'"}},
			{"tinyint unsigned", "TINYINT UNSIGNED assignment", "1,255",
				[]string{"256", "0x100", "(X'0100')", "0b100000000", "b'100000000'", "0x10000000000000000"}},
			{"mediumint", "column 'mediumint'", "1,8388607",
				[]string{"8388608", "2147483648", "2147483648.1", "2147483648e0", "'8388608tail'", "'2147483648tail'", "'0x1000000'"}},
			{"mediumint", "column 'mediumint'", "-8388608,1", []string{"-2147483649"}},
			{"mediumint unsigned", "column 'mediumint'", "1,16777215", []string{"4294967296", "'0x1000000'"}},
			{"mediumint unsigned", "column 'mediumint'", "0,1", []string{"-1"}},
		} {
			exec("create table row_warning (v " + tc.typ + ")")
			for _, mode := range []struct{ sqlMode, insert string }{
				{"", "insert"}, {"STRICT_TRANS_TABLES", "insert ignore"},
			} {
				exec("set session sql_mode='" + mode.sqlMode + "'")
				for _, value := range tc.inputs {
					exec("delete from row_warning")
					exec(mode.insert + " into row_warning values (1),(" + value + ")")
					checkIntegerAssignment("Out of range value for "+tc.warning+" at row 2", tc.stored)
				}
			}
			exec("drop table row_warning")
		}

		exec("create table row_warning (v tinyint unsigned)")
		exec("insert into row_warning values (0xff),((0b10)),(NULL)")
		require.Empty(t, warningCodes())
		var radixValues string
		require.NoError(t, conn.QueryRowContext(ctx, "select group_concat(v order by v) from row_warning").Scan(&radixValues))
		require.Equal(t, "2,255", radixValues)
		exec("delete from row_warning")
		for _, query := range []string{
			"insert into row_warning values (1),(0x100)",
			"insert into row_warning select 0x100",
		} {
			_, err := conn.ExecContext(ctx, query)
			var myErr *mysql.MySQLError
			require.ErrorAs(t, err, &myErr)
			require.Equal(t, uint16(moerr.ER_DATA_OUT_OF_RANGE), myErr.Number)
		}
		var rejectedRows int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from row_warning").Scan(&rejectedRows))
		require.Zero(t, rejectedRows)

		for _, query := range []string{
			"insert ignore into row_warning select 0x100",
			"insert ignore into row_warning select cast(256 as bit(9))",
		} {
			exec(query)
			checkIntegerAssignment("Out of range value for TINYINT UNSIGNED assignment at row 1", "255")
			exec("delete from row_warning")
		}
		exec("insert into row_warning values (1)")
		exec("update ignore row_warning set v=0x100")
		checkIntegerAssignment("Out of range value for TINYINT UNSIGNED assignment at row 1", "255")
		exec("drop table row_warning")

		exec("create table warning_source (v int)")
		exec("insert into warning_source values (1),(300)")
		exec("create table mixed_warnings (v tinyint)")
		checkWarningRows := func(expected ...int) {
			t.Helper()
			rows, err := conn.QueryContext(ctx, "show warnings")
			require.NoError(t, err)
			defer rows.Close()
			var messages []string
			for rows.Next() {
				var level, message string
				var code int
				require.NoError(t, rows.Scan(&level, &code, &message))
				require.Equal(t, int(moerr.ER_WARN_DATA_OUT_OF_RANGE), code)
				messages = append(messages, message)
			}
			require.NoError(t, rows.Err())
			want := make([]string, len(expected))
			for i, row := range expected {
				want[i] = fmt.Sprintf("Out of range value for TINYINT assignment at row %d", row)
			}
			require.ElementsMatch(t, want, messages)
		}
		checkStored := func(expected string) {
			t.Helper()
			var values string
			require.NoError(t, conn.QueryRowContext(ctx, "select group_concat(v order by v) from mixed_warnings").Scan(&values))
			require.Equal(t, expected, values)
		}

		exec("insert ignore into mixed_warnings values (2),((select v from warning_source where v=300)),('301'),((select v from warning_source where v=1)),('302')")
		checkWarningRows(2, 3, 5)
		checkStored("1,2,127,127,127")
		exec("truncate table mixed_warnings")
		stmt, err := conn.PrepareContext(ctx, "insert ignore into mixed_warnings values ((select v from warning_source where v=1)),(?)")
		require.NoError(t, err)
		defer stmt.Close()
		for _, value := range []string{"300", "301"} {
			_, err := stmt.ExecContext(ctx, value)
			require.NoError(t, err)
			checkWarningRows(2)
		}
		checkStored("1,1,127,127")

		// A scalar-cardinality error must still abort the entire statement.
		_, err = conn.ExecContext(ctx, "insert ignore into mixed_warnings values ('300'), ((select v from warning_source))")
		require.ErrorContains(t, err, "Subquery returns more than 1 row")
		checkStored("1,1,127,127")

		exec("set session sql_mode=''")
		exec("create table replace_warnings (id int primary key, v tinyint)")
		exec("replace into replace_warnings values (1, 1), (1,(select v from warning_source where v=300))")
		checkWarningRows(2)
		var replaced int
		require.NoError(t, conn.QueryRowContext(ctx, "select v from replace_warnings where id=1").Scan(&replaced))
		require.Equal(t, 127, replaced)

		exec("set session sql_mode=''")
		exec("create table ints (s tinyint, u tinyint unsigned)")
		exec("insert into ints values ('-18446744073709551616','-18446744073709551616')")
		var signed, unsigned int
		require.NoError(t, conn.QueryRowContext(ctx, "select s,u from ints").Scan(&signed, &unsigned))
		require.Equal(t, -128, signed)
		require.Equal(t, 0, unsigned)
		exec("insert ignore into ints values ('3e2','3e2')")
		require.NoError(t, conn.QueryRowContext(ctx, "select s,u from ints order by s desc limit 1").Scan(&signed, &unsigned))
		require.Equal(t, 127, signed)
		require.Equal(t, 255, unsigned)
		exec("delete from ints")
		exec("insert into ints values ('300tail','-1')")
		require.Equal(t, []int{int(moerr.ER_WARN_DATA_OUT_OF_RANGE), int(moerr.ER_WARN_DATA_OUT_OF_RANGE)}, warningCodes())
		require.NoError(t, conn.QueryRowContext(ctx, "select s,u from ints").Scan(&signed, &unsigned))
		require.Equal(t, 127, signed)
		require.Equal(t, 0, unsigned)
		exec("delete from ints")
		exec("insert into ints values ('12tail','12tail')")
		require.Equal(t, []int{int(moerr.WARN_DATA_TRUNCATED), int(moerr.WARN_DATA_TRUNCATED)}, warningCodes())
		require.NoError(t, conn.QueryRowContext(ctx, "select s,u from ints").Scan(&signed, &unsigned))
		require.Equal(t, 12, signed)
		require.Equal(t, 12, unsigned)
		exec("delete from ints")

		exec("set session sql_mode='STRICT_TRANS_TABLES'")
		_, err = conn.ExecContext(ctx, "insert into ints values ('300tail',0)")
		require.Error(t, err)
		_, err = conn.ExecContext(ctx, "insert into ints values (0,'-1')")
		require.Error(t, err)
		var intCount int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from ints").Scan(&intCount))
		require.Zero(t, intCount)
		exec("insert ignore into ints values ('300tail','-1')")
		require.Equal(t, []int{int(moerr.ER_WARN_DATA_OUT_OF_RANGE), int(moerr.ER_WARN_DATA_OUT_OF_RANGE)}, warningCodes())
		require.NoError(t, conn.QueryRowContext(ctx, "select s,u from ints").Scan(&signed, &unsigned))
		require.Equal(t, 127, signed)
		require.Equal(t, 0, unsigned)
		exec("delete from ints")
		exec("insert ignore into ints values ('12tail','12tail')")
		require.Equal(t, []int{int(moerr.WARN_DATA_TRUNCATED), int(moerr.WARN_DATA_TRUNCATED)}, warningCodes())
		require.NoError(t, conn.QueryRowContext(ctx, "select s,u from ints").Scan(&signed, &unsigned))
		require.Equal(t, 12, signed)
		require.Equal(t, 12, unsigned)

		exec("create table dates (d date)")
		for _, value := range []string{"2026-10-21 10-20-30", "2026-10-21 23:59:59.9999999"} {
			_, err = conn.ExecContext(ctx, "insert into dates values ('"+value+"')")
			require.Error(t, err, value)
		}
		exec("insert into dates values ('2026-10-21 00:00:00.0000000')")
		var date string
		require.NoError(t, conn.QueryRowContext(ctx, "select d from dates").Scan(&date))
		require.Equal(t, "2026-10-21", date)

		exec("set session sql_mode=''")
		for _, tc := range []struct {
			declaration string
			scale       int
		}{
			{"decimal(4,1)", 1},
			{"decimal(20,17)", 17},
			{"decimal(40,37)", 37},
			{"decimal(4,1) unsigned", 1},
		} {
			exec("create table decimal_radix (v " + tc.declaration + ")")
			assertValue := func(want string) {
				t.Helper()
				var value string
				var count int
				require.NoError(t, conn.QueryRowContext(ctx, "select max(v),count(*) from decimal_radix").Scan(&value, &count))
				require.Equal(t, 1, count)
				require.Equal(t, want, value, tc.declaration)
			}
			fitting := "100." + strings.Repeat("0", tc.scale)
			maximum := "999." + strings.Repeat("9", tc.scale)
			exec("set session sql_mode='STRICT_TRANS_TABLES'")
			for _, input := range []string{"select 0x64", "select b'1100100'", "values (0x64)"} {
				exec("insert into decimal_radix " + input)
				require.Empty(t, warningCodes())
				assertValue(fitting)
				exec("delete from decimal_radix")
			}

			exec("insert into decimal_radix select 0x64")
			_, err := conn.ExecContext(ctx, "insert into decimal_radix select 0xffff")
			require.Error(t, err, tc.declaration)
			assertValue(fitting)
			exec("update ignore decimal_radix set v=0xffff")
			require.Equal(t, []int{int(moerr.ER_WARN_DATA_OUT_OF_RANGE)}, warningCodes())
			assertValue(maximum)
			exec("delete from decimal_radix")

			exec("insert ignore into decimal_radix select 0xffff")
			require.Equal(t, []int{int(moerr.ER_WARN_DATA_OUT_OF_RANGE)}, warningCodes())
			assertValue(maximum)
			exec("delete from decimal_radix")
			exec("set session sql_mode=''")
			exec("insert into decimal_radix select 0xffff")
			require.Equal(t, []int{int(moerr.ER_WARN_DATA_OUT_OF_RANGE)}, warningCodes())
			assertValue(maximum)
			exec("drop table decimal_radix")
		}

		exec("set sql_mode='STRICT_TRANS_TABLES'")
		exec("create table decimal_derived (v decimal(4,1))")
		for _, source := range []string{
			"select v from (select 0xffffffffffffffff as v) s",
			"select v from (select 0x10000000000000000 as v) s",
			"select v from (select cast(0xffffffffffffffff as decimal(4,1)) as v) s",
			"select v from (select cast(0x10000000000000000 as decimal(4,1)) as v) s",
			"select v from (select 0xffff as v) s",
			"with s as (select 0xffff as v) select v from s",
			"select v from (select cast(0xffff as decimal(4,1)) as v) s",
		} {
			_, err := conn.ExecContext(ctx, "insert into decimal_derived "+source)
			require.Error(t, err, source)
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from decimal_derived").Scan(&count))
			require.Zero(t, count, source)
			exec("insert ignore into decimal_derived " + source)
			require.Equal(t, []int{int(moerr.ER_WARN_DATA_OUT_OF_RANGE)}, warningCodes(), source)
			var value string
			require.NoError(t, conn.QueryRowContext(ctx, "select v from decimal_derived").Scan(&value))
			require.Equal(t, "999.9", value, source)
			exec("delete from decimal_derived")
		}
		// A set consumes radix values as bytes in its resolved column domain.
		// Direct numeric radix assignments are already covered above.
		const radixSet = "select 0x3e8 union all select 0x64"
		_, err = conn.ExecContext(ctx, "insert into decimal_derived "+radixSet)
		var setErr *mysql.MySQLError
		require.ErrorAs(t, err, &setErr)
		require.Equal(t, moerr.ER_TRUNCATED_WRONG_VALUE_FOR_FIELD, setErr.Number)
		var storedRows int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from decimal_derived").Scan(&storedRows))
		require.Zero(t, storedRows)
		exec("insert ignore into decimal_derived " + radixSet)
		badDecimal := int(moerr.ER_TRUNCATED_WRONG_VALUE_FOR_FIELD)
		require.Equal(t, []int{badDecimal, badDecimal}, warningCodes())
		var setValues string
		require.NoError(t, conn.QueryRowContext(ctx, "select group_concat(v) from decimal_derived").Scan(&setValues))
		require.Equal(t, "0.0,0.0", setValues)
		exec("delete from decimal_derived")

		const byteSet = "select group_concat(hex(v) order by hex(v)) from (select 0xc3a9 v union select ?) s"
		preparedSet, err := conn.PrepareContext(ctx, byteSet)
		require.NoError(t, err)
		defer preparedSet.Close()
		for _, tc := range []struct {
			input []byte
			want  string
		}{
			{[]byte{0}, "00,C3A9"},
			{[]byte{0xc3, 0xa9}, "C3A9"},
		} {
			require.NoError(t, preparedSet.QueryRowContext(ctx, tc.input).Scan(&setValues))
			require.Equal(t, tc.want, setValues)
		}

		exec("set sql_mode=''")
		exec("insert into decimal_derived select v from (select 0xffff as v) s")
		require.Equal(t, []int{int(moerr.ER_WARN_DATA_OUT_OF_RANGE)}, warningCodes())
		exec("set sql_mode='STRICT_TRANS_TABLES'")
		exec("create table decimal_derived_scaled (v decimal(10,4))")
		exec("insert into decimal_derived_scaled select v from (select 0x33 as v) s")
		require.Empty(t, warningCodes())
		var derivedValue string
		require.NoError(t, conn.QueryRowContext(ctx, "select v from decimal_derived_scaled").Scan(&derivedValue))
		require.Equal(t, "0.0051", derivedValue)
		exec("insert into decimal_derived_scaled select v from (select v from decimal_derived_scaled) s")
		var copies int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from decimal_derived_scaled where v=0.0051").Scan(&copies))
		require.Equal(t, 2, copies)
		exec("create table decimal_derived_unsigned (v decimal(4,1) unsigned)")
		const derivedUnsigned = "select v from (select 0xffffffffffffffff as v) s"
		_, err = conn.ExecContext(ctx, "insert into decimal_derived_unsigned "+derivedUnsigned)
		require.Error(t, err)
		exec("insert ignore into decimal_derived_unsigned " + derivedUnsigned)
		require.Equal(t, []int{int(moerr.ER_WARN_DATA_OUT_OF_RANGE)}, warningCodes())
		require.NoError(t, conn.QueryRowContext(ctx, "select v from decimal_derived_unsigned").Scan(&derivedValue))
		require.Equal(t, "999.9", derivedValue)

		exec("create table decimal_upsert (a int, b int, qty int, v decimal(4,1), primary key (a,b))")
		exec("insert into decimal_upsert values (1,2,1,8.0)")
		const decimalUpsert = "insert into decimal_upsert select 1,2,3,v from (select cast(0x33 as decimal(4,1)) v) s " +
			"on duplicate key update a=a,b=b,qty=values(qty)"
		for _, want := range []int64{2, 0} {
			result, err := conn.ExecContext(ctx, decimalUpsert)
			require.NoError(t, err)
			affected, err := result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, want, affected)
		}
		var qty int
		require.NoError(t, conn.QueryRowContext(ctx, "select qty,v from decimal_upsert").Scan(&qty, &derivedValue))
		require.Equal(t, 3, qty)
		require.Equal(t, "8.0", derivedValue)

		for _, tc := range []struct {
			width int
			radix string
		}{
			{20, "0x056bc75e2d63100000"},
			{40, "0x1d6329f1c35ca4bfabb9f5610000000000"},
		} {
			exec(fmt.Sprintf("create table decimal_derived_wide (v decimal(%d,1))", tc.width))
			source := "select v from (select " + tc.radix + " as v) s"
			_, err := conn.ExecContext(ctx, "insert into decimal_derived_wide "+source)
			require.Error(t, err, "DECIMAL(%d,1)", tc.width)
			exec("insert ignore into decimal_derived_wide " + source)
			require.Equal(t, []int{int(moerr.ER_WARN_DATA_OUT_OF_RANGE)}, warningCodes())
			var value string
			require.NoError(t, conn.QueryRowContext(ctx, "select v from decimal_derived_wide").Scan(&value))
			require.Equal(t, strings.Repeat("9", tc.width-1)+".9", value)
			exec("drop table decimal_derived_wide")
		}

		exec("create table decimal_radix (v decimal(20,0))")
		exec("insert into decimal_radix values (0x10000000000000000)")
		var wideValue string
		require.NoError(t, conn.QueryRowContext(ctx, "select v from decimal_radix").Scan(&wideValue))
		require.Equal(t, "18446744073709551616", wideValue)
		exec("drop table decimal_radix")

		exec("create table decimal_radix (v decimal(40,1))")
		exec("insert into decimal_radix select 0x10000000000000000")
		require.Empty(t, warningCodes())
		var wideRadix string
		require.NoError(t, conn.QueryRowContext(ctx, "select v from decimal_radix").Scan(&wideRadix))
		require.Equal(t, "18446744073709551616.0", wideRadix)
		exec("delete from decimal_radix")
		exec("set sql_mode=''")
		exec("insert into decimal_radix select 0x01" + strings.Repeat("00", 32))
		require.Equal(t, []int{int(moerr.ER_WARN_DATA_OUT_OF_RANGE)}, warningCodes())
		require.NoError(t, conn.QueryRowContext(ctx, "select v from decimal_radix").Scan(&wideRadix))
		require.Equal(t, strings.Repeat("9", 39)+".9", wideRadix)
		exec("drop table decimal_radix")

		exec("create table decimals (d decimal(4,1))")
		exec("insert into decimals values (123.456)")
		require.Equal(t, []int{int(moerr.WARN_DATA_TRUNCATED)}, warningCodes())
		exec("insert into decimals values ('12.34567')")
		require.Equal(t, []int{int(moerr.WARN_DATA_TRUNCATED)}, warningCodes())
		exec("insert into decimals values ('12.30000')")
		require.Empty(t, warningCodes())
		rows, err := conn.QueryContext(ctx, "select d from decimals order by d")
		require.NoError(t, err)
		defer rows.Close()
		var values []string
		for rows.Next() {
			var value string
			require.NoError(t, rows.Scan(&value))
			values = append(values, value)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []string{"12.3", "12.3", "123.5"}, values)

		exec("set session sql_mode='STRICT_TRANS_TABLES'")
		exec("delete from decimals")
		exec("insert into decimals values ('12.34567')")
		require.Equal(t, []int{int(moerr.WARN_DATA_TRUNCATED)}, warningCodes())
		var assigned string
		require.NoError(t, conn.QueryRowContext(ctx, "select d from decimals").Scan(&assigned))
		require.Equal(t, "12.3", assigned)

		for _, source := range []string{"0xfff", "(0xfff)", "X'0FFF'", "0b111111111111", "b'111111111111'", "'9999tail'"} {
			_, err := conn.ExecContext(ctx, "insert into decimals values ("+source+")")
			require.Error(t, err, source)
			exec("delete from decimals")
			exec("insert ignore into decimals values (" + source + ")")
			require.Equal(t, []int{int(moerr.ER_WARN_DATA_OUT_OF_RANGE)}, warningCodes(), source)
			require.NoError(t, conn.QueryRowContext(ctx, "select d from decimals").Scan(&assigned))
			require.Equal(t, "999.9", assigned, source)
		}
		exec("delete from decimals")
		exec("insert ignore into decimals values (1),((0xfff))")
		func() {
			warnings, err := conn.QueryContext(ctx, "show warnings")
			require.NoError(t, err)
			defer warnings.Close()
			require.True(t, warnings.Next())
			var level, message string
			var code uint16
			require.NoError(t, warnings.Scan(&level, &code, &message))
			require.Equal(t, "Warning", level)
			require.Equal(t, uint16(moerr.ER_WARN_DATA_OUT_OF_RANGE), code)
			require.Equal(t, "Out of range value for column 'decimal' at row 2", message)
			require.False(t, warnings.Next())
			require.NoError(t, warnings.Err())
		}()
		var minimum, maximum string
		require.NoError(t, conn.QueryRowContext(ctx, "select min(d),max(d) from decimals").Scan(&minimum, &maximum))
		require.Equal(t, "1.0", minimum)
		require.Equal(t, "999.9", maximum)
		exec("set session sql_mode=''")
		exec("create table unsigned_decimal (d numeric(4,1) unsigned)")
		exec("insert into unsigned_decimal values ('-9999tail')")
		require.Equal(t, []int{int(moerr.ER_WARN_DATA_OUT_OF_RANGE)}, warningCodes())
		require.NoError(t, conn.QueryRowContext(ctx, "select d from unsigned_decimal").Scan(&assigned))
		require.Equal(t, "0.0", assigned)
		exec("delete from unsigned_decimal")
		exec("insert into unsigned_decimal values (0x10)")
		require.Empty(t, warningCodes())
		require.NoError(t, conn.QueryRowContext(ctx, "select d from unsigned_decimal").Scan(&assigned))
		require.Equal(t, "16.0", assigned)
		exec("set session sql_mode='STRICT_TRANS_TABLES'")

		exec("create table prepared_special (id int primary key, e enum('2','other'), s set('left','right'))")
		exec("insert into prepared_special values (1,'other','left')")
		checkPrepared := func(rhs string, value any, want string) {
			t.Helper()
			stmt, err := conn.PrepareContext(ctx, "update prepared_special set e="+rhs+" where id=?")
			require.NoError(t, err, rhs)
			defer stmt.Close()
			_, err = stmt.ExecContext(ctx, value, 1)
			require.NoError(t, err, rhs)
			require.NoError(t, conn.QueryRowContext(ctx, "select e from prepared_special where id=1").Scan(&assigned))
			require.Equal(t, want, assigned, rhs)
		}
		checkPrepared("coalesce(?,e)", "2", "2")
		checkPrepared("coalesce(?,e)", nil, "2")
		checkMarkerReuse := func(rhs string) [2]string {
			t.Helper()
			stmt, err := conn.PrepareContext(ctx, "update prepared_special set e="+rhs+" where id=?")
			require.NoError(t, err)
			defer stmt.Close()
			var results [2]string
			for i, value := range []any{"2", int64(2)} {
				_, err := stmt.ExecContext(ctx, value, 1)
				require.NoError(t, err, rhs)
				require.NoError(t, conn.QueryRowContext(ctx, "select e from prepared_special where id=1").Scan(&results[i]))
			}
			return results
		}
		bare := checkMarkerReuse("?")
		require.Equal(t, "2", bare[0])
		require.Equal(t, bare, checkMarkerReuse("(?)"))

		exec("create table median_near_limit (v decimal(37,0))")
		exec("insert into median_near_limit values (9999999999999999999999999999999999999),(9999999999999999999999999999999999998)")
		require.NoError(t, conn.QueryRowContext(ctx, "select median(v) from median_near_limit").Scan(&assigned))
		require.Equal(t, "9999999999999999999999999999999999998.5", assigned)

		exec("create table decimal_source (d decimal(12,2))")
		exec("create table decimal_wider (d decimal(14,2))")
		exec("insert into decimal_source values (-9999999999.99), (0.01), (null)")
		exec("insert into decimal_wider select d from decimal_source")
		require.Empty(t, warningCodes())
		var negative, positive, missing int
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(case when d = -9999999999.99 then 1 end), count(case when d = 0.01 then 1 end), count(case when d is null then 1 end) from decimal_wider").
			Scan(&negative, &positive, &missing))
		require.Equal(t, [3]int{1, 1, 1}, [3]int{negative, positive, missing})

		exec("create table decimal_less_integer_room (d decimal(14,6))")
		_, err = conn.ExecContext(ctx, "insert into decimal_less_integer_room select d from decimal_source")
		require.Error(t, err)
		exec("insert ignore into decimal_less_integer_room select d from decimal_source")
		require.Equal(t, []int{int(moerr.ER_WARN_DATA_OUT_OF_RANGE)}, warningCodes())
		var clamped int
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from decimal_less_integer_room where d = -99999999.999999").Scan(&clamped))
		require.Equal(t, 1, clamped)

		exec("create table median_input (g int, v decimal(38,0))")
		exec("insert into median_input values (1,1),(1,2),(2,3),(2,4)")
		exec("create table median_output (v decimal(40,2))")
		exec("insert into median_output select median(m) from (select median(v) as m from median_input group by g) grouped")
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from median_output where v = 2.50").Scan(&count))
		require.Equal(t, 1, count)
	})
}
