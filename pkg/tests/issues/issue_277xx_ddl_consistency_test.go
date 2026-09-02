// Copyright 2026 Matrix Origin
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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue277xxDDLConsistency(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()

		cn0, err := c.GetCNService(0)
		require.NoError(t, err)
		cn1, err := c.GetCNService(1)
		require.NoError(t, err)
		db0 := openIssue277xxDB(t, cn0.GetServiceConfig().CN.Frontend.Port)
		defer db0.Close()
		db1 := openIssue277xxDB(t, cn1.GetServiceConfig().CN.Frontend.Port)
		defer db1.Close()

		t.Run("27735 qualified alter drop index without default database", func(t *testing.T) {
			const database = "issue_27735_qualified_drop"
			resetIssue277xxDatabase(t, ctx, db0, database)
			defer execSQLMaybe(t, ctx, db0, "drop database if exists `"+database+"`")
			execSQLRequire(t, ctx, db0,
				"create table `"+database+"`.`t` (id int primary key, v int, key idx_v(v))")
			execSQLRequire(t, ctx, db0, "insert into `"+database+"`.`t` values (1, 7)")

			execSQLRequire(t, ctx, db0, "alter table `"+database+"`.`t` drop index `idx_v`")
			require.Equal(t, 0, queryIssue277xxInt(t, ctx, db0,
				"select count(*) from mo_catalog.mo_indexes where table_id = "+
					"(select rel_id from mo_catalog.mo_tables where reldatabase = ? and relname = 't') "+
					"and name = 'idx_v'", database))
			require.Equal(t, 1, queryIssue277xxInt(t, ctx, db0,
				"select count(*) from `"+database+"`.`t`"))
		})

		t.Run("27658 primary key rename keeps public index metadata", func(t *testing.T) {
			const database = "issue_27658_rename_pk"
			resetIssue277xxDatabase(t, ctx, db0, database)
			defer execSQLMaybe(t, ctx, db0, "drop database if exists `"+database+"`")
			execSQLRequire(t, ctx, db0, "create table `"+database+"`.`t` (a int primary key, v int)")
			execSQLRequire(t, ctx, db0, "alter table `"+database+"`.`t` rename column a to renamed_a")

			var columnName string
			require.NoError(t, db0.QueryRowContext(ctx,
				"select column_name from mo_catalog.mo_indexes where table_id = "+
					"(select rel_id from mo_catalog.mo_tables where reldatabase = ? and relname = 't') "+
					"and name = 'PRIMARY'", database).Scan(&columnName))
			require.Equal(t, "renamed_a", columnName)
		})

		t.Run("27708 duplicate create does not bind old foreign keys to replacement", func(t *testing.T) {
			const database = "issue_27708_duplicate_fk"
			resetIssue277xxDatabase(t, ctx, db0, database)
			defer execSQLMaybe(t, ctx, db0, "drop database if exists `"+database+"`")
			execSQLRequire(t, ctx, db0, "create table `"+database+"`.`parent` (id int primary key)")
			execSQLRequire(t, ctx, db0, "create table `"+database+"`.`child` ("+
				"cid int primary key, pid int, constraint fk_p foreign key(pid) references `"+
				database+"`.`parent`(id))")

			_, err := db0.ExecContext(ctx, "create table `"+database+"`.`parent` (x int)")
			requireIssue277xxMySQLError(t, err, 1050)
			execSQLRequire(t, ctx, db0,
				"create table if not exists `"+database+"`.`parent` (x int)")
			require.Equal(t, 1, queryIssue277xxInt(t, ctx, db0,
				"select count(*) from mo_catalog.mo_columns where att_database = ? "+
					"and att_relname = 'parent' and attname = 'id'", database))
			require.Equal(t, 0, queryIssue277xxInt(t, ctx, db0,
				"select count(*) from mo_catalog.mo_columns where att_database = ? "+
					"and att_relname = 'parent' and attname = 'x'", database))
		})

		t.Run("27766 copy alter retains child foreign key enforcement", func(t *testing.T) {
			const database = "issue_27766_copy_fk"
			resetIssue277xxDatabase(t, ctx, db0, database)
			defer execSQLMaybe(t, ctx, db0, "drop database if exists `"+database+"`")
			execSQLRequire(t, ctx, db0, "create table `"+database+"`.`parent` (a int primary key)")
			execSQLRequire(t, ctx, db0, "create table `"+database+"`.`child` ("+
				"id int primary key, a int, constraint fk_x foreign key(a) references `"+
				database+"`.`parent`(a))")
			execSQLRequire(t, ctx, db0, "insert into `"+database+"`.`parent` values (1)")
			_, err := db0.ExecContext(ctx, "insert into `"+database+"`.`child` values (1, 9)")
			requireIssue277xxForeignKeyError(t, err)

			execSQLRequire(t, ctx, db0,
				"alter table `"+database+"`.`child` add column note varchar(10) default 'x'")
			var tableName, createSQL string
			require.NoError(t, db0.QueryRowContext(ctx,
				"show create table `"+database+"`.`child`").Scan(&tableName, &createSQL))
			require.Equal(t, "child", tableName)
			require.Contains(t, strings.ToUpper(createSQL), "FOREIGN KEY")
			execSQLRequire(t, ctx, db0,
				"insert into `"+database+"`.`child` values (2, 1, 'ok')")
			_, err = db0.ExecContext(ctx,
				"insert into `"+database+"`.`child` values (3, 9, 'bad')")
			requireIssue277xxForeignKeyError(t, err)

			// COPY the parent as well. The source relation's live reverse-FK state
			// must drive child-table ID/column remapping even when a planned
			// TableDef is behind the engine constraint generation.
			execSQLRequire(t, ctx, db0,
				"alter table `"+database+"`.`parent` add column parent_note varchar(10) default 'p'")
			execSQLRequire(t, ctx, db0,
				"insert into `"+database+"`.`child` values (4, 1, 'ok')")
			_, err = db0.ExecContext(ctx,
				"insert into `"+database+"`.`child` values (5, 9, 'bad')")
			requireIssue277xxForeignKeyError(t, err)
			require.Equal(t, 1, queryIssue277xxInt(t, ctx, db0,
				"select count(*) from mo_catalog.mo_foreign_keys where db_name = ? and table_name = 'child'",
				database))

			// The live empty set is authoritative too. A stale planned generation
			// must not resurrect a foreign key removed before another COPY ALTER.
			execSQLRequire(t, ctx, db0,
				"alter table `"+database+"`.`child` drop foreign key fk_x")
			execSQLRequire(t, ctx, db0,
				"alter table `"+database+"`.`child` add column after_drop int default 0")
			require.NoError(t, db0.QueryRowContext(ctx,
				"show create table `"+database+"`.`child`").Scan(&tableName, &createSQL))
			require.NotContains(t, strings.ToUpper(createSQL), "FOREIGN KEY")
			require.Equal(t, 0, queryIssue277xxInt(t, ctx, db0,
				"select count(*) from mo_catalog.mo_foreign_keys where db_name = ? and table_name = 'child'",
				database))
			execSQLRequire(t, ctx, db0,
				"insert into `"+database+"`.`child` values (6, 9, 'allowed', 0)")
		})

		t.Run("27725 concurrent alter retries a stale physical index generation", func(t *testing.T) {
			const database = "issue_27725_concurrent_alter"
			resetIssue277xxDatabase(t, ctx, db0, database)
			defer execSQLMaybe(t, ctx, db0, "drop database if exists `"+database+"`")
			execSQLRequire(t, ctx, db0,
				"create table `"+database+"`.`t` (id int primary key, v int, key idx_v(v))")
			execSQLRequire(t, ctx, db0, "insert into `"+database+"`.`t` values (1, 7)")
			execSQLRequire(t, ctx, db1, "select mo_ctl('cn', 'SYNCCOMMIT', '')")
			require.Equal(t, 1, queryIssue277xxInt(t, ctx, db1,
				"select count(*) from `"+database+"`.`t`"))

			for round := 1; round <= 2; round++ {
				statements := []string{
					fmt.Sprintf("alter table `%s`.`t` add column c_%d_0 int default 0", database, round),
					fmt.Sprintf("alter table `%s`.`t` add column c_%d_1 int default 1", database, round),
				}
				dbs := []*sql.DB{db0, db1}
				start := make(chan struct{})
				errs := make([]error, len(dbs))
				var wg sync.WaitGroup
				wg.Add(len(dbs))
				for i := range dbs {
					go func(i int) {
						defer wg.Done()
						<-start
						_, errs[i] = dbs[i].ExecContext(ctx, statements[i])
					}(i)
				}
				close(start)
				wg.Wait()
				for i, alterErr := range errs {
					require.NoErrorf(t, alterErr, "concurrent ALTER %d exposed a stale index generation", i)
				}
			}

			require.Equal(t, 4, queryIssue277xxInt(t, ctx, db0,
				"select count(*) from mo_catalog.mo_columns where att_database = ? "+
					"and att_relname = 't' and attname like 'c\\_%'", database))
			require.Equal(t, 1, queryIssue277xxInt(t, ctx, db0,
				"select count(*) from `"+database+"`.`t` force index(idx_v) where v = 7"))
		})
	})
}

func openIssue277xxDB(t *testing.T, port int64) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
	require.NoError(t, err)
	return db
}

func resetIssue277xxDatabase(t *testing.T, ctx context.Context, db *sql.DB, name string) {
	t.Helper()
	execSQLRequire(t, ctx, db, "drop database if exists `"+name+"`")
	execSQLRequire(t, ctx, db, "create database `"+name+"`")
}

func queryIssue277xxInt(t *testing.T, ctx context.Context, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var value int
	require.NoError(t, db.QueryRowContext(ctx, query, args...).Scan(&value), "query failed: %s", query)
	return value
}

func requireIssue277xxMySQLError(t *testing.T, err error, code uint16) {
	t.Helper()
	require.Error(t, err)
	var mysqlErr *mysql.MySQLError
	require.ErrorAs(t, err, &mysqlErr)
	require.Equal(t, code, mysqlErr.Number)
}

func requireIssue277xxForeignKeyError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "foreign key constraint fails")
}
