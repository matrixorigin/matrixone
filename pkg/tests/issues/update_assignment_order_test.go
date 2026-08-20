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

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateAssignmentEvaluationIsLeftToRight(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port,
		))
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		const database = "issue_update_assignment_order"
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "rollback")
			_, _ = conn.ExecContext(cleanupCtx, "set autocommit = 1")
			_, _ = conn.ExecContext(cleanupCtx, "drop database if exists "+database)
		}()

		exec := func(statement string) {
			t.Helper()
			_, execErr := conn.ExecContext(ctx, statement)
			require.NoError(t, execErr, statement)
		}
		query := func(statement string, values ...any) {
			t.Helper()
			require.NoError(t, conn.QueryRowContext(ctx, statement).Scan(values...))
		}

		exec("create database " + database)
		exec("create table " + database + ".t (id int primary key, a int, b int, c int)")

		exec("insert into " + database + ".t values (1, 1, 10, 100)")
		exec("update " + database + ".t set a = a + 1, b = a, c = b where id = 1")
		var a, b, columnC int
		query("select a, b, c from "+database+".t where id = 1", &a, &b, &columnC)
		t.Logf("chained assignment: a=%d b=%d c=%d", a, b, columnC)
		assert.Equal(t, []int{2, 2, 2}, []int{a, b, columnC})

		exec("update " + database + ".t set a = 1, b = 2, c = 0 where id = 1")
		exec("update " + database + ".t set a = b, b = a where id = 1")
		query("select a, b from "+database+".t where id = 1", &a, &b)
		t.Logf("swap assignment: a=%d b=%d", a, b)
		assert.Equal(t, []int{2, 2}, []int{a, b})

		exec("update " + database + ".t set a = 1, b = 0, c = 0 where id = 1")
		exec("update " + database + ".t set a = a + 1, a = a + 1, b = a where id = 1")
		query("select a, b from "+database+".t where id = 1", &a, &b)
		t.Logf("repeated assignment: a=%d b=%d", a, b)
		assert.Equal(t, []int{3, 3}, []int{a, b})

		exec("update " + database + ".t set a = 1, b = 0, c = 0 where id = 1")
		exec("update " + database + ".t set a = a + 1, a = a * 10, b = a where id = 1")
		query("select a, b from "+database+".t where id = 1", &a, &b)
		t.Logf("repeated assignment uses the immediately preceding value: a=%d b=%d", a, b)
		assert.Equal(t, []int{20, 20}, []int{a, b})

		exec("create table " + database + ".source_values (id int primary key, v int)")
		exec("insert into " + database + ".source_values values (1, 7)")
		exec("update " + database + ".t set a = 1, b = 0, c = 0 where id = 1")
		exec("update " + database + ".t as x set a = (select s.v from " + database + ".source_values s where s.id = x.id), b = a, c = b where x.id = 1")
		query("select a, b, c from "+database+".t where id = 1", &a, &b, &columnC)
		t.Logf("early correlated scalar subquery: a=%d b=%d c=%d", a, b, columnC)
		assert.Equal(t, []int{7, 7, 7}, []int{a, b, columnC})

		exec("update " + database + ".t set a = 1, b = 0, c = 0 where id = 1")
		exec("update " + database + ".t as x set a = x.a + 1, b = a, c = (select s.v from " + database + ".source_values s where s.id = x.id) where x.id = 1")
		query("select a, b, c from "+database+".t where id = 1", &a, &b, &columnC)
		t.Logf("late correlated scalar subquery and qualified target: a=%d b=%d c=%d", a, b, columnC)
		assert.Equal(t, []int{2, 2, 7}, []int{a, b, columnC})
		_, err = conn.ExecContext(ctx, "update "+database+".t as x set a = nosuch.a, b = a where x.id = 1")
		require.ErrorContains(t, err, "missing FROM-clause entry for table 'nosuch'",
			"invalid qualified target reference must not be rebound as an unqualified column")

		exec("create table " + database + ".numeric_values (id int primary key, sal decimal(7,2), comm decimal(7,2))")
		exec("insert into " + database + ".numeric_values values (1, 0, 0)")
		exec("prepare numeric_update from 'update " + database + ".numeric_values set sal = abs(?), comm = sal where id = 1'")
		exec("set @prepared_sal = -12.34")
		exec("execute numeric_update using @prepared_sal")
		exec("deallocate prepare numeric_update")
		var sal, comm string
		query("select sal, comm from "+database+".numeric_values where id = 1", &sal, &comm)
		require.Equal(t, []string{"12.34", "12.34"}, []string{sal, comm})

		exec("update " + database + ".t set a = 1, b = 10, c = 100 where id = 1")
		exec("prepare chained_update from 'update " + database + ".t set a = a + 1, b = a, c = b where id = 1'")
		exec("execute chained_update")
		exec("deallocate prepare chained_update")
		query("select a, b, c from "+database+".t where id = 1", &a, &b, &columnC)
		t.Logf("prepared chained assignment: a=%d b=%d c=%d", a, b, columnC)
		assert.Equal(t, []int{2, 2, 2}, []int{a, b, columnC})

		exec("update " + database + ".t set a = 1, b = 10, c = 100 where id = 1")
		exec("set autocommit = 0")
		exec("update " + database + ".t set a = a + 1, b = a, c = b where id = 1")
		query("select a, b, c from "+database+".t where id = 1", &a, &b, &columnC)
		t.Logf("transaction before rollback: a=%d b=%d c=%d", a, b, columnC)
		assert.Equal(t, []int{2, 2, 2}, []int{a, b, columnC})
		exec("rollback")
		query("select a, b, c from "+database+".t where id = 1", &a, &b, &columnC)
		require.Equal(t, []int{1, 10, 100}, []int{a, b, columnC})
		exec("update " + database + ".t set a = a + 1, b = a, c = b where id = 1")
		exec("commit")
		query("select a, b, c from "+database+".t where id = 1", &a, &b, &columnC)
		t.Logf("transaction after commit: a=%d b=%d c=%d", a, b, columnC)
		assert.Equal(t, []int{2, 2, 2}, []int{a, b, columnC})

		exec("create table " + database + ".volatile_values (a double, b double)")
		exec("insert into " + database + ".volatile_values values (0, 0)")
		exec("update " + database + ".volatile_values set a = rand(), b = a")
		var volatileValuesMatch bool
		query("select a = b from "+database+".volatile_values", &volatileValuesMatch)
		t.Logf("volatile assignment values match: %t", volatileValuesMatch)
		require.True(t, volatileValuesMatch)
	})
}
