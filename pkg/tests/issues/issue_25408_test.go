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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue25408PreparedNestedExactAndStringDomains(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
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
		mustExec(t, ctx, conn, "create table n(a int)")
		mustExec(t, ctx, conn, "insert into n values (1), (2), (3)")

		for _, query := range []string{
			"select (select sum(? + 1) from n) + 1",
			"select sum(? + 1) + 1 from n",
			"select sum(? + 1) over () + 1 from n limit 1",
		} {
			t.Run(query, func(t *testing.T) {
				stmt, prepareErr := conn.PrepareContext(ctx, query)
				require.NoError(t, prepareErr)
				defer stmt.Close()
				for _, execution := range []struct {
					arg  any
					want string
				}{{int64(1), "7"}, {float64(2.5), "11.5"}, {int64(1), "7"}} {
					var got string
					require.NoError(t, stmt.QueryRowContext(ctx, execution.arg).Scan(&got))
					require.Equal(t, execution.want, got)
				}
			})
		}
		mustExec(t, ctx, conn, "create table aggregate_sink(id int, v double)")
		mustExec(t, ctx, conn, "insert into aggregate_sink values (2, 8.5)")
		t.Run("approximate comparison domain", func(t *testing.T) {
			stmt, prepareErr := conn.PrepareContext(ctx,
				"delete from aggregate_sink where id = 2 and v > (select sum(? + 1) from n)")
			require.NoError(t, prepareErr)
			defer stmt.Close()
			result, execErr := stmt.ExecContext(ctx, float64(0.5))
			require.NoError(t, execErr)
			affected, affectedErr := result.RowsAffected()
			require.NoError(t, affectedErr)
			require.Equal(t, int64(1), affected)
		})

		mustExec(t, ctx, conn, "create table bool_strings(v text)")
		mustExec(t, ctx, conn, "set @bool_value = true")
		mustExec(t, ctx, conn, "prepare bool_insert from 'insert into bool_strings values (?)'")
		mustExec(t, ctx, conn, "execute bool_insert using @bool_value")
		requireIssue25408Scalar(t, ctx, conn, "true", "select v from bool_strings")
		mustExec(t, ctx, conn, "deallocate prepare bool_insert")
		mustExec(t, ctx, conn, "prepare bool_numeric from 'select ? + 1'")
		requireIssue25408Scalar(t, ctx, conn, "2", "execute bool_numeric using @bool_value")
		mustExec(t, ctx, conn, "set @bool_value = false")
		requireIssue25408Scalar(t, ctx, conn, "1", "execute bool_numeric using @bool_value")
		mustExec(t, ctx, conn, "deallocate prepare bool_numeric")
		mustExec(t, ctx, conn, "prepare bool_float from 'select cast(? as double)'")
		requireIssue25408Scalar(t, ctx, conn, "0", "execute bool_float using @bool_value")
		mustExec(t, ctx, conn, "set @bool_value = true")
		requireIssue25408Scalar(t, ctx, conn, "1", "execute bool_float using @bool_value")
		mustExec(t, ctx, conn, "deallocate prepare bool_float")
		mustExec(t, ctx, conn, "prepare bool_decimal from 'select cast(? as decimal(10,2))'")
		requireIssue25408Scalar(t, ctx, conn, "1.00", "execute bool_decimal using @bool_value")
		mustExec(t, ctx, conn, "set @bool_value = false")
		requireIssue25408Scalar(t, ctx, conn, "0.00", "execute bool_decimal using @bool_value")
		mustExec(t, ctx, conn, "deallocate prepare bool_decimal")
		mustExec(t, ctx, conn, "create table bool_numbers(v double)")
		mustExec(t, ctx, conn, "prepare bool_number_insert from 'insert into bool_numbers values (? + 1e0)'")
		mustExec(t, ctx, conn, "set @bool_value = true")
		mustExec(t, ctx, conn, "execute bool_number_insert using @bool_value")
		mustExec(t, ctx, conn, "set @bool_value = false")
		mustExec(t, ctx, conn, "execute bool_number_insert using @bool_value")
		requireIssue25408Scalar(t, ctx, conn, "3", "select sum(v) from bool_numbers")
		mustExec(t, ctx, conn, "deallocate prepare bool_number_insert")
		mustExec(t, ctx, conn, "set @bool_value = true")
		mustExec(t, ctx, conn, "prepare bool_ctas from 'create table bool_ctas as select ? + 1e0 as v'")
		mustExec(t, ctx, conn, "execute bool_ctas using @bool_value")
		requireIssue25408Scalar(t, ctx, conn, "2", "select v from bool_ctas")
		mustExec(t, ctx, conn, "deallocate prepare bool_ctas")
		mustExec(t, ctx, conn, "set @bool_value = true")
		mustExec(t, ctx, conn, "prepare bool_limit from 'select a from n order by a limit ?'")
		requireIssue25408Scalar(t, ctx, conn, "1", "execute bool_limit using @bool_value")
		mustExec(t, ctx, conn, "deallocate prepare bool_limit")

		// SQL PREPARE reaches the same nested exact-expression specialization as
		// EXECUTE USING user variables.
		mustExec(t, ctx, conn, "prepare nested_text from 'select ? + (-1 + 0)'")
		mustExec(t, ctx, conn, "set @nested_value = 3")
		requireIssue25408Scalar(t, ctx, conn, "2", "execute nested_text using @nested_value")
		mustExec(t, ctx, conn, "deallocate prepare nested_text")

		for _, test := range []struct {
			name  string
			query string
			arg   any
			want  string
		}{
			{name: "nested negative", query: "select ? + (-1 + 0)", arg: int64(3), want: "2"},
			{name: "nested positive", query: "select ? + (1 + 0)", arg: int64(3), want: "4"},
			{name: "abs", query: "select ? + abs(1)", arg: int64(3), want: "4"},
			{name: "mod", query: "select ? + mod(3, 2)", arg: int64(3), want: "4"},
			{name: "coalesce", query: "select ? + coalesce(1, 0)", arg: int64(3), want: "4"},
			{name: "double", query: "select ? + (-1 + 0)", arg: float64(2.5), want: "1.5"},
			{name: "boolean true float", query: "select ? + 1e0", arg: true, want: "2"},
			{name: "boolean false float", query: "select ? + 1e0", arg: false, want: "1"},
			{name: "uint64", query: "select ? + (-1 + 0)", arg: ^uint64(0), want: "18446744073709551614"},
		} {
			t.Run(test.name, func(t *testing.T) {
				stmt, prepareErr := conn.PrepareContext(ctx, test.query)
				require.NoError(t, prepareErr)
				defer stmt.Close()
				var got string
				require.NoError(t, stmt.QueryRowContext(ctx, test.arg).Scan(&got))
				require.Equal(t, test.want, got)
			})
		}

		ctas, err := conn.PrepareContext(ctx, "create table nested_ctas as select ? + (-1 + 0) as v")
		require.NoError(t, err)
		defer ctas.Close()
		_, err = ctas.ExecContext(ctx, int64(3))
		require.NoError(t, err)
		requireIssue25408Scalar(t, ctx, conn, "2", "select v from nested_ctas")

		mustExec(t, ctx, conn, `create table domains(
			e enum('low','mid','high'),
			s set('low','mid','high'),
			v varchar(16),
			d date)`)
		mustExec(t, ctx, conn,
			"insert into domains values ('low', 'low', 'low', '2026-01-01')")

		for _, test := range []struct {
			name  string
			query string
			arg   any
			want  []string
		}{
			{name: "enum", query: "select greatest(e, ?), least(e, ?) from domains", arg: "mid", want: []string{"mid", "low"}},
			{name: "enum reversed", query: "select greatest(?, e), least(?, e) from domains", arg: "mid", want: []string{"mid", "low"}},
			{name: "set", query: "select greatest(s, ?), least(s, ?) from domains", arg: "mid", want: []string{"mid", "low"}},
			{name: "varchar", query: "select greatest(v, ?), least(v, ?) from domains", arg: "mid", want: []string{"mid", "low"}},
			{name: "date", query: "select greatest(d, ?), least(d, ?) from domains", arg: "2026-02-01", want: []string{"2026-02-01", "2026-01-01"}},
			{name: "date reversed", query: "select greatest(?, d), least(?, d) from domains", arg: "2026-02-01", want: []string{"2026-02-01", "2026-01-01"}},
			{name: "numeric control", query: "select greatest(?, 10), least(?, 10)", arg: int64(2), want: []string{"10", "2"}},
		} {
			t.Run(test.name, func(t *testing.T) {
				stmt, prepareErr := conn.PrepareContext(ctx, test.query)
				require.NoError(t, prepareErr)
				defer stmt.Close()
				var greatest, least string
				require.NoError(t, stmt.QueryRowContext(ctx, test.arg, test.arg).Scan(&greatest, &least))
				require.Equal(t, test.want, []string{greatest, least})
			})
		}

		mustExec(t, ctx, conn,
			"prepare enum_text from 'select greatest(e, ?), least(e, ?) from domains'")
		mustExec(t, ctx, conn, "set @enum_value = 'mid'")
		var greatest, least string
		require.NoError(t, conn.QueryRowContext(ctx,
			"execute enum_text using @enum_value, @enum_value").Scan(&greatest, &least))
		require.Equal(t, []string{"mid", "low"}, []string{greatest, least})
		mustExec(t, ctx, conn, "deallocate prepare enum_text")
	})
}

func requireIssue25408Scalar(t *testing.T, ctx context.Context, conn *sql.Conn, want, query string) {
	t.Helper()
	var got string
	require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&got))
	require.Equal(t, want, got)
}
