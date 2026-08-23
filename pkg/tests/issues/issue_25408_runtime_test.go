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
	"errors"
	"fmt"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue25408PreparedRuntimeNumericRebind(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false",
			cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))

		mustExec(t, ctx, conn, "prepare runtime_number from 'select ? + 1'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_number") }()

		for _, execution := range []struct {
			assignment string
			want       string
		}{
			{assignment: "set @runtime_value = 2", want: "3"},
			{assignment: "set @runtime_value = 2.5", want: "3.5"},
			{assignment: "set @runtime_value = -2", want: "-1"},
		} {
			mustExec(t, ctx, conn, execution.assignment)
			var got string
			require.NoError(t, conn.QueryRowContext(
				ctx, "execute runtime_number using @runtime_value").Scan(&got))
			require.Equal(t, execution.want, got)
		}

		for _, assignment := range []string{
			"set @runtime_value = '9007199254740993'",
			"set @runtime_value = '1e10'",
			"set @runtime_value = ' 1e-10 '",
			"set @runtime_value = '1e-10000'",
			"set @runtime_value = '-1e-10000'",
			"set @runtime_value = true",
			"set @runtime_value = false",
			"set @runtime_value = 9007199254740993.25",
			"set @runtime_value = 18446744073709551615",
			"set @runtime_value = 99999999999999999999999999999999999999999999999999999999999999999",
			"set @runtime_value = -99999999999999999999999999999999999999999999999999999999999999999",
			"set @runtime_value = 0.123456789012345678901234567890",
			"set @runtime_value = -0.123456789012345678901234567890",
		} {
			mustExec(t, ctx, conn, assignment)
			var direct, prepared string
			require.NoError(t, conn.QueryRowContext(
				ctx, "select @runtime_value + 1").Scan(&direct))
			require.NoError(t, conn.QueryRowContext(
				ctx, "execute runtime_number using @runtime_value").Scan(&prepared))
			require.Equal(t, direct, prepared, assignment)
		}

		mustExec(t, ctx, conn,
			"prepare runtime_derived from 'select x + 1 from (select ? as x) d'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_derived") }()
		mustExec(t, ctx, conn,
			"prepare runtime_nested_derived from 'select y + 1 from (select x + 1 as y from (select ? as x) d1) d2'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_nested_derived") }()
		mustExec(t, ctx, conn, "set @runtime_value = 2.5")
		var derived string
		require.NoError(t, conn.QueryRowContext(
			ctx, "execute runtime_derived using @runtime_value").Scan(&derived))
		require.Equal(t, "3.5", derived)
		require.NoError(t, conn.QueryRowContext(
			ctx, "execute runtime_nested_derived using @runtime_value").Scan(&derived))
		require.Equal(t, "4.5", derived)

		mustExec(t, ctx, conn,
			"prepare runtime_nested from 'select ? + (-1 + 0), ? + abs(1), ? + mod(3,2), ? + coalesce(1,0)'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_nested") }()
		mustExec(t, ctx, conn, "set @runtime_value = 2.5")
		var nestedMinus, nestedAbs, nestedMod, nestedCoalesce string
		require.NoError(t, conn.QueryRowContext(ctx,
			"execute runtime_nested using @runtime_value, @runtime_value, @runtime_value, @runtime_value").Scan(
			&nestedMinus, &nestedAbs, &nestedMod, &nestedCoalesce))
		require.Equal(t, []string{"1.5", "3.5", "3.5", "3.5"},
			[]string{nestedMinus, nestedAbs, nestedMod, nestedCoalesce})

		mustExec(t, ctx, conn,
			"prepare runtime_values from 'select ? + 1, concat(?, \\'-x\\')'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_values") }()
		for _, execution := range []struct {
			assignment string
			want       []string
		}{
			{assignment: "set @runtime_value = 1, @runtime_text = 'first'", want: []string{"2", "first-x"}},
			{assignment: "set @runtime_value = 2.5, @runtime_text = 'second'", want: []string{"3.5", "second-x"}},
		} {
			mustExec(t, ctx, conn, execution.assignment)
			var numeric, text string
			require.NoError(t, conn.QueryRowContext(ctx,
				"execute runtime_values using @runtime_value, @runtime_text").Scan(&numeric, &text))
			require.Equal(t, execution.want, []string{numeric, text})
		}

		mustExec(t, ctx, conn,
			"prepare runtime_compare from 'select (? + 1) > 0'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_compare") }()
		for _, execution := range []struct {
			assignment string
			want       string
		}{
			{assignment: "set @runtime_value = 2", want: "1"},
			{assignment: "set @runtime_value = -2", want: "0"},
		} {
			mustExec(t, ctx, conn, execution.assignment)
			var got string
			require.NoError(t, conn.QueryRowContext(ctx,
				"execute runtime_compare using @runtime_value").Scan(&got))
			require.Equal(t, execution.want, got)
		}

		mustExec(t, ctx, conn,
			"create table runtime_strings(id int primary key, v varchar(20))")
		mustExec(t, ctx, conn,
			"insert into runtime_strings values (1, 'abc'), (2, 'def'), (3, 'ghi')")
		mustExec(t, ctx, conn,
			"prepare runtime_string_predicate from 'delete from runtime_strings where concat(v, \\'\\') = ?'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_string_predicate") }()
		mustExec(t, ctx, conn, "set @runtime_text = 'abc'")
		mustExec(t, ctx, conn,
			"execute runtime_string_predicate using @runtime_text")
		var remainingIDs string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select group_concat(id order by id) from runtime_strings").Scan(&remainingIDs))
		require.Equal(t, "2,3", remainingIDs)

		mustExec(t, ctx, conn,
			"create table runtime_decimals(id int primary key, v decimal(20,4))")
		mustExec(t, ctx, conn,
			"insert into runtime_decimals values "+
				"(1, 9007199254740992.0000), "+
				"(2, 9007199254740992.0001), "+
				"(3, 9007199254740993.0000)")
		mustExec(t, ctx, conn, "set @runtime_decimal_text = '9007199254740992.0001'")
		for _, comparison := range []struct {
			name     string
			operator string
			want     string
		}{
			{name: "equal", operator: "=", want: "2"},
			{name: "null_safe_equal", operator: "<=>", want: "2"},
			{name: "not_equal", operator: "<>", want: "1,3"},
			{name: "less", operator: "<", want: "1"},
			{name: "less_equal", operator: "<=", want: "1,2"},
			{name: "greater", operator: ">", want: "3"},
			{name: "greater_equal", operator: ">=", want: "2,3"},
			{name: "in", operator: "in", want: "2"},
		} {
			t.Run("sql_prepare_decimal_"+comparison.name, func(t *testing.T) {
				predicate := "v " + comparison.operator + " ?"
				if comparison.operator == "in" {
					predicate = "v in (?)"
				}
				stmtName := "runtime_decimal_" + comparison.name
				mustExec(t, ctx, conn, fmt.Sprintf(
					"prepare %s from 'select group_concat(id order by id) from runtime_decimals where %s'",
					stmtName, predicate))
				defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare "+stmtName) }()
				var got string
				require.NoError(t, conn.QueryRowContext(ctx,
					"execute "+stmtName+" using @runtime_decimal_text").Scan(&got))
				require.Equal(t, comparison.want, got)
			})

			t.Run("binary_prepare_decimal_"+comparison.name, func(t *testing.T) {
				predicate := "v " + comparison.operator + " ?"
				if comparison.operator == "in" {
					predicate = "v in (?)"
				}
				stmt, prepareErr := conn.PrepareContext(ctx,
					"select group_concat(id order by id) from runtime_decimals where "+predicate)
				require.NoError(t, prepareErr)
				defer stmt.Close()
				var got string
				require.NoError(t, stmt.QueryRowContext(ctx,
					"9007199254740992.0001").Scan(&got))
				require.Equal(t, comparison.want, got)
			})
		}

		mustExec(t, ctx, conn, "create table n(a int)")
		mustExec(t, ctx, conn, "insert into n values (1), (2), (3)")
		mustExec(t, ctx, conn, "prepare runtime_count_null_first from 'select count(?) from n'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_count_null_first") }()
		mustExec(t, ctx, conn, "set @runtime_value = null")
		var nullFirstCount string
		require.NoError(t, conn.QueryRowContext(
			ctx, "execute runtime_count_null_first using @runtime_value").Scan(&nullFirstCount))
		require.Equal(t, "0", nullFirstCount)

		mustExec(t, ctx, conn, "prepare runtime_count from 'select count(?) from n'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_count") }()
		for _, execution := range []struct {
			assignment string
			want       string
		}{
			{assignment: "set @runtime_value = 1", want: "3"},
			{assignment: "set @runtime_value = null", want: "0"},
			{assignment: "set @runtime_value = 1", want: "3"},
		} {
			mustExec(t, ctx, conn, execution.assignment)
			var got string
			require.NoError(t, conn.QueryRowContext(
				ctx, "execute runtime_count using @runtime_value").Scan(&got))
			require.Equal(t, execution.want, got)
		}

		mustExec(t, ctx, conn,
			"create table runtime_binary(id int primary key, v binary(4), key idx_v(v))")
		mustExec(t, ctx, conn, "insert into runtime_binary values (1, 'ab'), (2, 'cd')")
		mustExec(t, ctx, conn, "set @runtime_binary = (select v from runtime_binary where id = 1)")
		mustExec(t, ctx, conn,
			"prepare runtime_binary_predicate from 'select id from runtime_binary where v = ?'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_binary_predicate") }()
		var binaryID string
		require.NoError(t, conn.QueryRowContext(ctx,
			"execute runtime_binary_predicate using @runtime_binary").Scan(&binaryID))
		require.Equal(t, "1", binaryID)

		mustExec(t, ctx, conn,
			"prepare runtime_aggregate from 'select (select sum(? + a) from n) + 1'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_aggregate") }()
		for _, execution := range []struct {
			assignment string
			want       string
		}{
			{assignment: "set @runtime_value = 1", want: "10"},
			{assignment: "set @runtime_value = 0.5", want: "8.5"},
			{assignment: "set @runtime_value = 1", want: "10"},
		} {
			mustExec(t, ctx, conn, execution.assignment)
			var got string
			require.NoError(t, conn.QueryRowContext(
				ctx, "execute runtime_aggregate using @runtime_value").Scan(&got))
			require.Equal(t, execution.want, got)
		}

		mustExec(t, ctx, conn,
			"prepare runtime_aggregate_top from 'select sum(? + 1) + 1 from n'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_aggregate_top") }()
		mustExec(t, ctx, conn,
			"prepare runtime_window from 'select sum(? + 1) over () + 1 from n limit 1'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_window") }()
		mustExec(t, ctx, conn,
			"prepare runtime_distinct_sum from 'select (select sum(distinct ? + a) from n) + 1'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_distinct_sum") }()
		mustExec(t, ctx, conn,
			"prepare runtime_aggregate_compare from 'select count(*) from n where a < (select avg(? + a) from n)'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_aggregate_compare") }()
		mustExec(t, ctx, conn, "set @runtime_value = 0.5")
		for _, query := range []string{
			"execute runtime_aggregate_top using @runtime_value",
			"execute runtime_window using @runtime_value",
		} {
			var got string
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&got))
			require.Equal(t, "5.5", got, query)
		}
		var distinctSum string
		require.NoError(t, conn.QueryRowContext(ctx,
			"execute runtime_distinct_sum using @runtime_value").Scan(&distinctSum))
		require.Equal(t, "8.5", distinctSum)
		var aggregateCompare string
		require.NoError(t, conn.QueryRowContext(ctx,
			"execute runtime_aggregate_compare using @runtime_value").Scan(&aggregateCompare))
		require.Equal(t, "2", aggregateCompare)

		mustExec(t, ctx, conn,
			"prepare runtime_union from 'select x + 1 from (select ? as x union all select ? as x) u order by x'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_union") }()
		mustExec(t, ctx, conn, "set @runtime_left = 2.5, @runtime_right = 3.5")
		rows, err := conn.QueryContext(ctx,
			"execute runtime_union using @runtime_left, @runtime_right")
		require.NoError(t, err)
		defer rows.Close()
		var unionValues []string
		for rows.Next() {
			var value string
			require.NoError(t, rows.Scan(&value))
			unionValues = append(unionValues, value)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []string{"3.5", "4.5"}, unionValues)

		queryStrings := func(testingT *testing.T, query string) []string {
			testingT.Helper()
			queryRows, queryErr := conn.QueryContext(ctx, query)
			require.NoError(testingT, queryErr)
			defer queryRows.Close()
			var values []string
			for queryRows.Next() {
				var value string
				require.NoError(testingT, queryRows.Scan(&value))
				values = append(values, value)
			}
			require.NoError(testingT, queryRows.Err())
			return values
		}
		setOperationCases := []struct {
			name          string
			preparedSQL   string
			directSQL     string
			textArguments string
			binaryArgs    []any
			want          []string
		}{
			{
				name:          "union_distinct",
				preparedSQL:   "select x + 1 from (select ? as x union select 2 as x) u order by x",
				directSQL:     "select x + 1 from (select '02' as x union select 2 as x) u order by x",
				textArguments: "set @runtime_set_left = '02'",
				binaryArgs:    []any{"02"},
				want:          []string{"3", "3"},
			},
			{
				name:          "intersect",
				preparedSQL:   "select x + 1 from (select ? as x intersect select 2 as x) u order by x",
				directSQL:     "select x + 1 from (select '02' as x intersect select 2 as x) u order by x",
				textArguments: "set @runtime_set_left = '02'",
				binaryArgs:    []any{"02"},
				want:          nil,
			},
			{
				name:          "minus",
				preparedSQL:   "select x + 1 from (select ? as x minus select 2 as x) u order by x",
				directSQL:     "select x + 1 from (select '02' as x minus select 2 as x) u order by x",
				textArguments: "set @runtime_set_left = '02'",
				binaryArgs:    []any{"02"},
				want:          []string{"3"},
			},
			{
				name:          "union_all_order",
				preparedSQL:   "select x + 1 from (select ? as x union all select 2 as x) u order by x",
				directSQL:     "select x + 1 from (select '10' as x union all select 2 as x) u order by x",
				textArguments: "set @runtime_set_left = '10'",
				binaryArgs:    []any{"10"},
				want:          []string{"11", "3"},
			},
		}
		for _, testCase := range setOperationCases {
			t.Run("sql_prepare_set_operation_"+testCase.name, func(t *testing.T) {
				require.Equal(t, testCase.want, queryStrings(t, testCase.directSQL))
				stmtName := "runtime_set_" + testCase.name
				mustExec(t, ctx, conn, fmt.Sprintf("prepare %s from '%s'", stmtName, testCase.preparedSQL))
				defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare "+stmtName) }()
				mustExec(t, ctx, conn, testCase.textArguments)
				require.Equal(t, testCase.want, queryStrings(t,
					fmt.Sprintf("execute %s using @runtime_set_left", stmtName)))
			})
		}
		for _, testCase := range setOperationCases {
			t.Run("binary_prepare_set_operation_"+testCase.name, func(t *testing.T) {
				stmt, prepareErr := conn.PrepareContext(ctx, testCase.preparedSQL)
				require.NoError(t, prepareErr)
				defer stmt.Close()
				queryRows, queryErr := stmt.QueryContext(ctx, testCase.binaryArgs...)
				require.NoError(t, queryErr)
				defer queryRows.Close()
				var values []string
				for queryRows.Next() {
					var value string
					require.NoError(t, queryRows.Scan(&value))
					values = append(values, value)
				}
				require.NoError(t, queryRows.Err())
				require.Equal(t, testCase.want, values)
			})
		}
		for _, assignment := range []string{
			"set @runtime_left = '2.5', @runtime_right = 3",
			"set @runtime_left = 2.5, @runtime_right = '3'",
		} {
			mustExec(t, ctx, conn, assignment)
			got := queryStrings(t, "execute runtime_union using @runtime_left, @runtime_right")
			require.Equal(t, []string{"3.5", "4"}, got)
		}

		mustExec(t, ctx, conn,
			"prepare runtime_ctas from 'create table runtime_ctas as select x + 1 as v from (select ? as x) d'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_ctas") }()
		mustExec(t, ctx, conn, "set @runtime_value = 2.5")
		mustExec(t, ctx, conn,
			"create table runtime_ctas_direct as select @runtime_value + 1 as v")
		mustExec(t, ctx, conn, "execute runtime_ctas using @runtime_value")
		var ctasValue, ctasType, directCTASType string
		require.NoError(t, conn.QueryRowContext(ctx, "select v from runtime_ctas").Scan(&ctasValue))
		require.Equal(t, "3.5", ctasValue)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select data_type from information_schema.columns "+
				"where table_schema = database() and table_name = 'runtime_ctas' and column_name = 'v'").Scan(&ctasType))
		require.NoError(t, conn.QueryRowContext(ctx,
			"select data_type from information_schema.columns "+
				"where table_schema = database() and table_name = 'runtime_ctas_direct' and column_name = 'v'").Scan(&directCTASType))
		require.Equal(t, directCTASType, ctasType)

		mustExec(t, ctx, conn, "create table runtime_sink(id int primary key, v double, s text)")
		mustExec(t, ctx, conn, "insert into runtime_sink values (1, 0, null), (2, 0, null)")
		mustExec(t, ctx, conn,
			"prepare runtime_update from 'update runtime_sink set v = ? + 1 where id = ?'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_update") }()
		mustExec(t, ctx, conn, "set @runtime_value = 2.5, @runtime_id = 1")
		mustExec(t, ctx, conn, "execute runtime_update using @runtime_value, @runtime_id")
		mustExec(t, ctx, conn, "set @runtime_value = 4, @runtime_id = 2")
		mustExec(t, ctx, conn, "execute runtime_update using @runtime_value, @runtime_id")
		var firstValue, secondValue string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select v from runtime_sink where id = 1").Scan(&firstValue))
		require.NoError(t, conn.QueryRowContext(ctx,
			"select v from runtime_sink where id = 2").Scan(&secondValue))
		require.Equal(t, []string{"3.5", "5"}, []string{firstValue, secondValue})

		mustExec(t, ctx, conn, "prepare runtime_bool_string from 'update runtime_sink set s = ? where id = 1'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_bool_string") }()
		mustExec(t, ctx, conn, "set @runtime_value = true")
		mustExec(t, ctx, conn, "execute runtime_bool_string using @runtime_value")
		var boolString string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select s from runtime_sink where id = 1").Scan(&boolString))
		require.Equal(t, "true", boolString)

		mustExec(t, ctx, conn,
			"prepare runtime_cast from 'select cast(? as bigint) + 1, 12 regexp (? + 0)'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_cast") }()
		mustExec(t, ctx, conn, "set @runtime_value = 2.5, @runtime_pattern = 2")
		var castValue, regexpValue string
		require.NoError(t, conn.QueryRowContext(ctx,
			"execute runtime_cast using @runtime_value, @runtime_pattern").Scan(&castValue, &regexpValue))
		require.Equal(t, []string{"4", "1"}, []string{castValue, regexpValue})

		mustExec(t, ctx, conn,
			"prepare runtime_decimal_cast from 'select cast(? as decimal(30,0)) + 1'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_decimal_cast") }()
		mustExec(t, ctx, conn, "set @runtime_text = '9007199254740993'")
		var decimalCast string
		require.NoError(t, conn.QueryRowContext(ctx,
			"execute runtime_decimal_cast using @runtime_text").Scan(&decimalCast))
		require.Equal(t, "9007199254740994", decimalCast)

		for _, query := range []string{
			"select cast('128' as tinyint)",
			"select cast('32768' as smallint)",
			"select cast('9223372036854775808' as bigint)",
			"select cast('256' as tinyint unsigned)",
		} {
			var ignored string
			err = conn.QueryRowContext(ctx, query).Scan(&ignored)
			require.Error(t, err, query)
			var mysqlErr *mysqlDriver.MySQLError
			require.True(t, errors.As(err, &mysqlErr), query)
			require.Equal(t, uint16(1690), mysqlErr.Number, query)
		}

		mustExec(t, ctx, conn, "create table runtime_bits(v bit(64))")
		mustExec(t, ctx, conn,
			"prepare runtime_bit_text from 'insert into runtime_bits values (?)'")
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare runtime_bit_text") }()
		mustExec(t, ctx, conn, "set @runtime_bit = b'101'")
		mustExec(t, ctx, conn, "execute runtime_bit_text using @runtime_bit")
		var bitValue string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select v + 0 from runtime_bits").Scan(&bitValue))
		require.Equal(t, "5", bitValue)

		stmt, err := conn.PrepareContext(ctx, "select ? + 1")
		require.NoError(t, err)
		defer stmt.Close()
		for _, execution := range []struct {
			value any
			want  string
		}{
			{value: int64(2), want: "3"},
			{value: float64(2.5), want: "3.5"},
			{value: int64(-2), want: "-1"},
		} {
			var got string
			require.NoError(t, stmt.QueryRowContext(ctx, execution.value).Scan(&got))
			require.Equal(t, execution.want, got)
		}

		binaryUnion, err := conn.PrepareContext(ctx,
			"select x + 1 from (select ? as x union all select ? as x) u order by x")
		require.NoError(t, err)
		defer binaryUnion.Close()
		for _, params := range [][2]any{
			{"2.5", int64(3)},
			{float64(2.5), "3"},
		} {
			func() {
				binaryRows, queryErr := binaryUnion.QueryContext(ctx, params[0], params[1])
				require.NoError(t, queryErr)
				defer binaryRows.Close()
				var values []string
				for binaryRows.Next() {
					var value string
					require.NoError(t, binaryRows.Scan(&value))
					values = append(values, value)
				}
				require.NoError(t, binaryRows.Err())
				require.Equal(t, []string{"3.5", "4"}, values)
			}()
		}

		binaryBit, err := conn.PrepareContext(ctx, "insert into runtime_bits values (?)")
		require.NoError(t, err)
		defer binaryBit.Close()
		_, err = binaryBit.ExecContext(ctx, "5")
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select v + 0 from runtime_bits order by v desc limit 1").Scan(&bitValue))
		require.Equal(t, "53", bitValue)

		binaryCTAS, err := conn.PrepareContext(ctx,
			"create table runtime_ctas_binary as select ? + 1 as v")
		require.NoError(t, err)
		defer binaryCTAS.Close()
		_, err = binaryCTAS.ExecContext(ctx, float64(2.5))
		require.NoError(t, err)
		var binaryCTASValue, binaryCTASType string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select v from runtime_ctas_binary").Scan(&binaryCTASValue))
		require.Equal(t, "3.5", binaryCTASValue)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select data_type from information_schema.columns "+
				"where table_schema = database() and table_name = 'runtime_ctas_binary' and column_name = 'v'").Scan(&binaryCTASType))
		require.Equal(t, "double", binaryCTASType)
	})
}
