// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
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
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// Exercise COM_STMT_PREPARE and repeated COM_STMT_EXECUTE against the shared
// frontend. SQL PREPARE is covered separately by the optimizer BVT.
func TestCompositePointBinaryPrepareReuse(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		name := strings.ToLower(testutils.GetDatabaseName(t))
		_, err = conn.ExecContext(ctx, "create database `"+name+"`")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, cleanupErr := conn.ExecContext(cleanupCtx, "drop database if exists `"+name+"`")
			require.NoError(t, cleanupErr)
		}()
		for _, statement := range []string{
			"use `" + name + "`",
			"create table t(tenant varchar(16) not null, session_id varchar(16) not null, request_id varchar(16) not null, payload int, primary key(tenant, session_id, request_id))",
			"insert into t values ('u','s','r1',1),('u','s','r2',2),('u','other','r1',3),('v','s','r1',4),('u',x'6e007a','nul',5)",
			"create table idx_t(tenant varchar(16) not null, session_id varchar(16) not null, request_id varchar(16) not null, payload int, primary key(tenant, session_id, request_id), key idx_payload(payload))",
			"insert into idx_t values ('u','s','r1',1),('u','s','r2',2),('u','other','r1',3)",
		} {
			_, err = conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		// Use persisted rows so the assertion observes the actual PK reader,
		// not just an optimizer expression or a timing-dependent speedup.
		_, err = conn.ExecContext(ctx, "select mo_ctl('dn','flush','"+name+".t')")
		require.NoError(t, err)
		assertScanRows := func(t *testing.T, statement string, want int) {
			t.Helper()
			plan, err := testutils.QueryTextResult(ctx, conn, statement)
			require.NoError(t, err)
			lines := strings.Split(plan.Text, "\n")
			scanRows := -1
			for i, line := range lines {
				if strings.Contains(line, "Table Scan on") && i+1 < len(lines) {
					match := regexp.MustCompile(`\binputRows=(\d+)`).FindStringSubmatch(lines[i+1])
					require.Len(t, match, 2, plan.Text)
					scanRows, err = strconv.Atoi(match[1])
					require.NoError(t, err)
					break
				}
			}
			require.Equal(t, want, scanRows, "unexpected PK reader input: %s", plan.Text)
		}
		t.Run("two-point scan", func(t *testing.T) {
			assertScanRows(t, "explain analyze select payload from t where tenant='u' and ((session_id='s' and request_id='r1') or (session_id='other' and request_id='r1'))", 2)
		})
		readRows := func(t *testing.T, statement *sql.Stmt, args ...any) []string {
			t.Helper()
			rows, err := statement.QueryContext(ctx, args...)
			require.NoError(t, err)
			defer rows.Close()
			var got []string
			for rows.Next() {
				var session, request string
				require.NoError(t, rows.Scan(&session, &request))
				got = append(got, session+"/"+request)
			}
			require.NoError(t, rows.Err())
			return got
		}

		// Exercise the reported 50-point shape without a large data fixture.
		// SQL PREPARE supports EXPLAIN ANALYZE; COM_STMT_PREPARE does not.
		arms := make([]string, 50)
		values := []any{"u"}
		assignments, names := []string{"@p0='u'"}, []string{"@p0"}
		for i := range arms {
			arms[i] = "(session_id=? and request_id=?)"
			session, request := "absent", fmt.Sprintf("r%d", i)
			if i == 0 {
				session, request = "s", "r1"
			} else if i == 1 {
				session, request = "other", "r1"
			}
			for _, value := range []string{session, request} {
				param := fmt.Sprintf("@p%d", len(values))
				names = append(names, param)
				assignments = append(assignments, param+"='"+value+"'")
				values = append(values, value)
			}
		}
		manySQL := "select session_id, request_id from t where tenant=? and (" + strings.Join(arms, " or ") + ") order by session_id, request_id"
		_, err = conn.ExecContext(ctx, "prepare point_explain from 'explain analyze "+manySQL+"'")
		require.NoError(t, err)
		defer func() {
			_, err := conn.ExecContext(ctx, "deallocate prepare point_explain")
			require.NoError(t, err)
		}()
		_, err = conn.ExecContext(ctx, "set "+strings.Join(assignments, ","))
		require.NoError(t, err)
		t.Run("fifty-point prepared scan", func(t *testing.T) {
			assertScanRows(t, "execute point_explain using "+strings.Join(names, ","), 2)
			// The candidate budget depends on this execution's bytes. A large
			// value falls back, and must not cache that fallback for later values.
			_, err := conn.ExecContext(ctx, "set @p1=repeat('x', 1048576)")
			require.NoError(t, err)
			assertScanRows(t, "execute point_explain using "+strings.Join(names, ","), 4)
			_, err = conn.ExecContext(ctx, "set @p1='s'")
			require.NoError(t, err)
			assertScanRows(t, "execute point_explain using "+strings.Join(names, ","), 2)
		})
		many, err := conn.PrepareContext(ctx, manySQL)
		require.NoError(t, err)
		defer many.Close()
		require.Equal(t, []string{"other/r1", "s/r1"}, readRows(t, many, values...))

		where := "tenant=? and ((session_id=? and request_id=?) or (session_id=? and request_id=?))"
		query, err := conn.PrepareContext(ctx, "select session_id, request_id from t where "+where+" order by session_id, request_id")
		require.NoError(t, err)
		defer query.Close()
		for _, tc := range []struct {
			name string
			args []any
			want []string
		}{
			{"cross session", []any{"u", "s", "r1", "other", "r1"}, []string{"other/r1", "s/r1"}},
			{"rebind", []any{"u", "s", "r2", "other", "r1"}, []string{"other/r1", "s/r2"}},
			{"one null branch", []any{"u", nil, "r1", "s", "r2"}, []string{"s/r2"}},
			{"all null branches", []any{"u", nil, "r1", "s", nil}, nil},
			{"duplicate point", []any{"u", "s", "r1", "s", "r1"}, []string{"s/r1"}},
			{"binary parameter", []any{[]byte("u"), []byte("s"), []byte("r1"), "other", "r1"}, []string{"other/r1", "s/r1"}},
			{"embedded zero", []any{"u", []byte{'n', 0, 'z'}, "nul", "s", "r2"}, []string{"n\x00z/nul", "s/r2"}},
			{"oversized parameter", []any{"u", strings.Repeat("x", 1<<16), "r1", "s", "r2"}, []string{"s/r2"}},
			{"small after oversized", []any{"u", "s", "r1", "other", "r1"}, []string{"other/r1", "s/r1"}},
			{"different tenant", []any{"v", "s", "r1", "other", "r1"}, []string{"s/r1"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				require.Equal(t, tc.want, readRows(t, query, tc.args...))
			})
		}
		t.Run("secondary index prepared reuse", func(t *testing.T) {
			plan, err := testutils.QueryTextResult(ctx, conn, "explain select session_id,request_id from idx_t force index(idx_payload) where payload=2 and tenant='u' and ((session_id='s' and request_id='r2') or (session_id='other' and request_id='r1'))")
			require.NoError(t, err)
			require.Contains(t, plan.Text, "idx_payload")
			indexed, err := conn.PrepareContext(ctx, "select session_id,request_id from idx_t force index(idx_payload) where payload=? and "+where+" order by session_id,request_id")
			require.NoError(t, err)
			defer indexed.Close()
			for _, tc := range []struct {
				args []any
				want []string
			}{
				{[]any{2, "u", "s", "r2", "other", "r1"}, []string{"s/r2"}},
				{[]any{3, "u", "s", "r2", "other", "r1"}, []string{"other/r1"}},
				{[]any{3, "u", nil, "r2", "other", "r1"}, []string{"other/r1"}},
				{[]any{3, "u", strings.Repeat("x", 1<<16), "r2", "other", "r1"}, []string{"other/r1"}},
				{[]any{2, "u", "s", "r2", "other", "r1"}, []string{"s/r2"}},
			} {
				require.Equal(t, tc.want, readRows(t, indexed, tc.args...))
			}
		})
		rows, err := query.QueryContext(ctx, "u", "s", "r1", "other")
		if rows != nil {
			defer rows.Close()
			require.NoError(t, rows.Err())
		}
		require.Error(t, err, "candidate copies must not alter parameter arity")

		type statementOutcome struct {
			rows     []string
			error    uint16
			warnings int
		}
		outcome := func(tenant, residual string, tenantValue any, values ...any) statementOutcome {
			t.Helper()
			query := "select request_id from t where " + tenant + "=? and ((session_id=? and request_id=?) or (session_id=? and request_id=?))"
			if residual != "" {
				query += " and " + residual
			}
			statement, err := conn.PrepareContext(ctx, query+" order by request_id")
			require.NoError(t, err)
			defer statement.Close()
			args := append([]any{tenantValue, "s", "r1", "other", "r1"}, values...)
			rows, err := statement.QueryContext(ctx, args...)
			var got statementOutcome
			if rows != nil {
				defer rows.Close()
				for rows.Next() {
					var request string
					require.NoError(t, rows.Scan(&request))
					got.rows = append(got.rows, request)
				}
				if err == nil {
					err = rows.Err()
				}
				require.NoError(t, rows.Close())
			}
			if err != nil {
				var mysqlErr *mysqlDriver.MySQLError
				require.True(t, errors.As(err, &mysqlErr), "expected a MySQL error, got %v", err)
				got.error = mysqlErr.Number
			}
			require.NoError(t, conn.QueryRowContext(ctx, "show count(*) warnings").Scan(&got.warnings))
			return got
		}
		for _, tc := range []struct {
			name, residual string
			value          any
		}{
			{"invalid numeric cast", "cast(? as signed)>0", "not-an-integer"},
			{"invalid binary to text cast", "cast(? as char) is not null", []byte{0xff}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				require.Equal(t, outcome("concat(tenant,'')", tc.residual, "u", tc.value), outcome("tenant", tc.residual, "u", tc.value),
					"a fallible residual must retain its rows, error and warnings")
			})
		}
		t.Run("invalid binary key bytes", func(t *testing.T) {
			require.Equal(t, outcome("concat(tenant,'')", "", []byte{0xff}), outcome("tenant", "", []byte{0xff}),
				"a candidate must preserve the fallback's rows, error and warnings")
		})

		update, err := conn.PrepareContext(ctx, "update t set payload=case when session_id=? and request_id=? then 11 when session_id=? and request_id=? then 22 else payload end where "+where)
		require.NoError(t, err)
		defer update.Close()
		args := []any{"s", "r1", "other", "r1", "u", "s", "r1", "other", "r1"}
		tx, err := conn.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		txUpdate := tx.StmtContext(ctx, update)
		defer txUpdate.Close()
		result, err := txUpdate.ExecContext(ctx, args...)
		require.NoError(t, err)
		affected, err := result.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 2, affected)
		require.NoError(t, tx.Rollback())
		var sum int
		require.NoError(t, conn.QueryRowContext(ctx, "select sum(payload) from t").Scan(&sum))
		require.Equal(t, 15, sum, "rollback must restore both rows")
		result, err = update.ExecContext(ctx, args...)
		require.NoError(t, err)
		affected, err = result.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 2, affected)
		require.NoError(t, conn.QueryRowContext(ctx, "select sum(payload) from t").Scan(&sum))
		require.Equal(t, 44, sum)
		for _, row := range []struct {
			session, request string
			want             int
		}{{"s", "r1", 11}, {"other", "r1", 22}, {"s", "r2", 2}} {
			var payload int
			require.NoError(t, conn.QueryRowContext(ctx, "select payload from t where tenant='u' and session_id=? and request_id=?", row.session, row.request).Scan(&payload))
			require.Equal(t, row.want, payload, "%s/%s", row.session, row.request)
		}
	})
}
