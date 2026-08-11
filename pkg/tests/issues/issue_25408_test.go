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
