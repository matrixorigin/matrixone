// Copyright 2021 - 2026 Matrix Origin
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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue26875ReplaceMaintainsIndexedForeignKeyChildren(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		cn2, err := c.GetCNService(1)
		require.NoError(t, err)
		db2, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", cn2.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db2.Close()
		conn2, err := db2.Conn(ctx)
		require.NoError(t, err)
		defer conn2.Close()

		dbName := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
		mustExec(t, ctx, conn2, fmt.Sprintf("use `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			mustExec(t, cleanupCtx, conn, "use mo_catalog")
			mustExec(t, cleanupCtx, conn, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()

		mustExec(t, ctx, conn, "create table cascade_p(id int primary key, note varchar(20))")
		mustExec(t, ctx, conn, `create table cascade_c(
			id int primary key, pid int, index idx_pid(pid),
			foreign key(pid) references cascade_p(id) on delete cascade)`)
		mustExec(t, ctx, conn, "replace into cascade_p values(1, 'first'),(2, 'unaffected')")
		mustExec(t, ctx, conn, "insert into cascade_c values(10, 1),(20, 2)")
		stmt, err := conn.PrepareContext(ctx, "replace into cascade_p values(?, ?)")
		require.NoError(t, err)
		defer stmt.Close()
		_, err = stmt.ExecContext(ctx, 1, "replaced")
		require.NoError(t, err)
		var count int
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from cascade_c force index(idx_pid) where pid=1").Scan(&count))
		require.Zero(t, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from cascade_c force index(idx_pid) where pid=2 and id=20").Scan(&count))
		require.Equal(t, 1, count)

		mustExec(t, ctx, conn, "create table setnull_p(id int primary key, note varchar(20))")
		mustExec(t, ctx, conn, `create table setnull_c(
			id int primary key, pid int, index idx_pid(pid),
			foreign key(pid) references setnull_p(id) on delete set null)`)
		mustExec(t, ctx, conn, "replace into setnull_p values(1, 'first'),(2, 'unaffected')")
		mustExec(t, ctx, conn, "insert into setnull_c values(10, 1),(20, 2)")
		setNullStmt, err := conn.PrepareContext(ctx, "replace into setnull_p values(?, ?)")
		require.NoError(t, err)
		defer setNullStmt.Close()
		_, err = setNullStmt.ExecContext(ctx, 1, "replaced")
		require.NoError(t, err)
		var pid sql.NullInt64
		require.NoError(t, conn.QueryRowContext(ctx, "select pid from setnull_c where id=10").Scan(&pid))
		require.False(t, pid.Valid)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from setnull_c force index(idx_pid) where pid=1").Scan(&count))
		require.Zero(t, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from setnull_c force index(idx_pid) where pid=2 and id=20").Scan(&count))
		require.Equal(t, 1, count)
		mustExec(t, ctx, conn, "insert into setnull_p values(5, 'select target'),(6, 'select unaffected')")
		mustExec(t, ctx, conn, "insert into setnull_c values(50, 5),(60, 6)")
		mustExec(t, ctx, conn, "replace into setnull_p select 5, 'selected replacement'")
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from setnull_c force index(idx_pid) where pid=6 and id=60").Scan(&count))
		require.Equal(t, 1, count)

		mustExec(t, ctx, conn, "create table unique_cascade_p(id int primary key, note varchar(20))")
		mustExec(t, ctx, conn, `create table unique_cascade_c(
			id int primary key, pid int, unique key uk_pid(pid),
			foreign key(pid) references unique_cascade_p(id) on delete cascade)`)
		mustExec(t, ctx, conn, "insert into unique_cascade_p values(1, 'first'),(2, 'empty'),(3, 'unaffected')")
		mustExec(t, ctx, conn, "insert into unique_cascade_c values(10, 1),(11, null),(13, 3)")
		uniqueCascadeStmt, err := conn2.PrepareContext(ctx, "replace into unique_cascade_p values(?, ?)")
		require.NoError(t, err)
		defer uniqueCascadeStmt.Close()
		_, err = uniqueCascadeStmt.ExecContext(ctx, 1, "replaced")
		require.NoError(t, err)
		_, err = uniqueCascadeStmt.ExecContext(ctx, 2, "still empty")
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from unique_cascade_c where id=10").Scan(&count))
		require.Zero(t, count)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from unique_cascade_c where id=11 and pid is null").Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from unique_cascade_c force index(uk_pid) where pid=3 and id=13").Scan(&count))
		require.Equal(t, 1, count)
		_, err = conn.ExecContext(ctx, "insert into unique_cascade_c values(14, 3)")
		require.Error(t, err)

		mustExec(t, ctx, conn, "create table unique_setnull_p(id int primary key, note varchar(20))")
		mustExec(t, ctx, conn, `create table unique_setnull_c(
			id int primary key, pid int, marker int,
			unique key uk_pid_marker(pid, marker),
			foreign key(pid) references unique_setnull_p(id) on delete set null)`)
		mustExec(t, ctx, conn, "insert into unique_setnull_p values(1, 'first'),(2, 'delete'),(3, 'empty'),(4, 'unaffected')")
		mustExec(t, ctx, conn, "insert into unique_setnull_c values(10, 1, 7),(11, 2, 8),(12, null, 9),(14, 4, 10)")
		uniqueSetNullStmt, err := conn2.PrepareContext(ctx, "replace into unique_setnull_p values(?, ?)")
		require.NoError(t, err)
		defer uniqueSetNullStmt.Close()
		_, err = uniqueSetNullStmt.ExecContext(ctx, 1, "replaced")
		require.NoError(t, err)
		_, err = uniqueSetNullStmt.ExecContext(ctx, 3, "still empty")
		require.NoError(t, err)
		mustExec(t, ctx, conn, "delete from unique_setnull_p where id=2")
		for _, childID := range []int{10, 11, 12} {
			require.NoError(t, conn.QueryRowContext(ctx,
				"select count(*) from unique_setnull_c where id=? and pid is null", childID).Scan(&count))
			require.Equal(t, 1, count)
		}
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from unique_setnull_c force index(uk_pid_marker) where pid in (1,2)").Scan(&count))
		require.Zero(t, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from unique_setnull_c force index(uk_pid_marker) where pid=4 and marker=10").Scan(&count))
		require.Equal(t, 1, count)
		_, err = conn.ExecContext(ctx, "insert into unique_setnull_c values(15, 4, 10)")
		require.Error(t, err)

		for _, tc := range []struct {
			tableName string
			indexDDL  string
		}{
			{tableName: "self_setnull_sk", indexDDL: "index idx_pid(pid)"},
			{tableName: "self_setnull_uk", indexDDL: "unique key uk_pid(pid)"},
		} {
			mustExec(t, ctx, conn, fmt.Sprintf(`create table %s(
				id int primary key, pid int, %s,
				foreign key(pid) references %s(id) on delete set null)`, tc.tableName, tc.indexDDL, tc.tableName))
			mustExec(t, ctx, conn, fmt.Sprintf("insert into %s values(1,null),(2,1)", tc.tableName))
			selfStmt, prepareErr := conn2.PrepareContext(ctx, fmt.Sprintf("replace into %s values(?,?)", tc.tableName))
			require.NoError(t, prepareErr)
			defer selfStmt.Close()
			_, execErr := selfStmt.ExecContext(ctx, 1, nil)
			require.NoError(t, execErr)
			require.NoError(t, conn.QueryRowContext(ctx,
				fmt.Sprintf("select count(*) from %s where id=2 and pid is null", tc.tableName)).Scan(&count))
			require.Equal(t, 1, count)
		}
	})
}

func TestIssue26875MixedIndexedSetNullAndCascadeActions(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		conn := openIssue26875Conn(t, ctx, c, 0)
		defer conn.Close()
		dbName := createIssue26875Database(t, ctx, conn)
		defer dropIssue26875Database(t, conn, dbName)

		for _, prepared := range []bool{false, true} {
			suffix := "literal"
			if prepared {
				suffix = "prepared"
			}
			parent := "mixed_parent_" + suffix
			child := "mixed_child_" + suffix
			mustExec(t, ctx, conn, fmt.Sprintf("create table %s(id int primary key)", parent))
			foreignKeys := fmt.Sprintf(`foreign key(set_id) references %s(id) on delete set null,
				foreign key(cascade_id) references %s(id) on delete cascade`, parent, parent)
			if prepared {
				foreignKeys = fmt.Sprintf(`foreign key(cascade_id) references %s(id) on delete cascade,
					foreign key(set_id) references %s(id) on delete set null`, parent, parent)
			}
			mustExec(t, ctx, conn, fmt.Sprintf(`create table %s(
				id int primary key, set_id int, cascade_id int, marker int,
				key sk_set(set_id), key sk_cascade(cascade_id),
				%s)`, child, foreignKeys))
			mustExec(t, ctx, conn, fmt.Sprintf("insert into %s values(1),(2)", parent))
			mustExec(t, ctx, conn, fmt.Sprintf(
				"insert into %s values(10,1,2,10),(20,2,1,20),(30,1,1,30),(40,2,2,40)", child))
			if prepared {
				func() {
					stmt, err := conn.PrepareContext(ctx, fmt.Sprintf("replace into %s values(?)", parent))
					require.NoError(t, err)
					defer stmt.Close()
					_, err = stmt.ExecContext(ctx, 1)
					require.NoError(t, err)
				}()
			} else {
				mustExec(t, ctx, conn, fmt.Sprintf("replace into %s values(1)", parent))
			}

			var setID sql.NullInt64
			require.NoError(t, conn.QueryRowContext(ctx,
				fmt.Sprintf("select set_id from %s where id=10", child)).Scan(&setID))
			require.False(t, setID.Valid)
			var count int
			require.NoError(t, conn.QueryRowContext(ctx,
				fmt.Sprintf("select count(*) from %s where id in (20,30)", child)).Scan(&count))
			require.Zero(t, count)
			require.NoError(t, conn.QueryRowContext(ctx,
				fmt.Sprintf("select count(*) from %s where id=40 and set_id=2 and cascade_id=2", child)).Scan(&count))
			require.Equal(t, 1, count)
			require.NoError(t, conn.QueryRowContext(ctx,
				fmt.Sprintf("select count(*) from %s force index(sk_set) where set_id=2", child)).Scan(&count))
			require.Equal(t, 1, count)
			require.NoError(t, conn.QueryRowContext(ctx,
				fmt.Sprintf("select count(*) from %s force index(sk_cascade) where cascade_id=2", child)).Scan(&count))
			require.Equal(t, 2, count)
		}
	})
}

func TestIssue26875CombinedSetNullAndCascadeActions(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		conn := openIssue26875Conn(t, ctx, c, 0)
		defer conn.Close()
		dbName := createIssue26875Database(t, ctx, conn)
		defer dropIssue26875Database(t, conn, dbName)

		for _, prepared := range []bool{false, true} {
			suffix := "literal"
			if prepared {
				suffix = "prepared"
			}
			parent := "combined_parent_" + suffix
			child := "combined_child_" + suffix
			mustExec(t, ctx, conn, fmt.Sprintf("create table %s(id int primary key)", parent))
			foreignKeys := fmt.Sprintf(`foreign key(set_a) references %s(id) on delete set null,
				foreign key(set_b) references %s(id) on delete set null,
				foreign key(cascade_id) references %s(id) on delete cascade`, parent, parent, parent)
			if prepared {
				foreignKeys = fmt.Sprintf(`foreign key(cascade_id) references %s(id) on delete cascade,
					foreign key(set_b) references %s(id) on delete set null,
					foreign key(set_a) references %s(id) on delete set null`, parent, parent, parent)
			}
			mustExec(t, ctx, conn, fmt.Sprintf(`create table %s(
				id int primary key, set_a int, set_b int, cascade_id int,
				key sk_a(set_a), key sk_b(set_b), key sk_c(cascade_id),
				%s)`, child, foreignKeys))
			mustExec(t, ctx, conn, fmt.Sprintf("insert into %s values(1),(2)", parent))
			mustExec(t, ctx, conn, fmt.Sprintf(
				"insert into %s values(10,1,2,2),(11,2,1,2),(20,2,2,1),(30,1,1,1),(40,2,2,2)", child))
			if prepared {
				func() {
					stmt, err := conn.PrepareContext(ctx, fmt.Sprintf("replace into %s values(?)", parent))
					require.NoError(t, err)
					defer stmt.Close()
					_, err = stmt.ExecContext(ctx, 1)
					require.NoError(t, err)
				}()
			} else {
				mustExec(t, ctx, conn, fmt.Sprintf("replace into %s values(1)", parent))
			}

			var setA, setB sql.NullInt64
			require.NoError(t, conn.QueryRowContext(ctx,
				fmt.Sprintf("select set_a,set_b from %s where id=10", child)).Scan(&setA, &setB))
			require.False(t, setA.Valid)
			require.Equal(t, int64(2), setB.Int64)
			require.NoError(t, conn.QueryRowContext(ctx,
				fmt.Sprintf("select set_a,set_b from %s where id=11", child)).Scan(&setA, &setB))
			require.Equal(t, int64(2), setA.Int64)
			require.False(t, setB.Valid)
			var count int
			require.NoError(t, conn.QueryRowContext(ctx,
				fmt.Sprintf("select count(*) from %s where id in (20,30)", child)).Scan(&count))
			require.Zero(t, count)
			require.NoError(t, conn.QueryRowContext(ctx,
				fmt.Sprintf("select count(*) from %s where id=40 and set_a=2 and set_b=2 and cascade_id=2", child)).Scan(&count))
			require.Equal(t, 1, count)
			for _, indexCheck := range []struct {
				indexName string
				predicate string
				expected  int
			}{
				{indexName: "sk_a", predicate: "set_a=2", expected: 2},
				{indexName: "sk_b", predicate: "set_b=2", expected: 2},
				{indexName: "sk_c", predicate: "cascade_id=2", expected: 3},
			} {
				require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
					"select count(*) from %s force index(%s) where %s",
					child, indexCheck.indexName, indexCheck.predicate)).Scan(&count))
				require.Equal(t, indexCheck.expected, count)
			}
		}
	})
}

func TestIssue26875MultiRowSelfSetNullExcludesReplaceOwnedRows(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		conn := openIssue26875Conn(t, ctx, c, 0)
		defer conn.Close()
		dbName := createIssue26875Database(t, ctx, conn)
		defer dropIssue26875Database(t, conn, dbName)

		cases := []struct {
			name string
			run  func(*testing.T, context.Context, *sql.Conn, string)
		}{
			{name: "literal forward", run: func(t *testing.T, ctx context.Context, conn *sql.Conn, table string) {
				mustExec(t, ctx, conn, fmt.Sprintf(
					"replace into %s values(1,null,11,'p-new'),(2,null,22,'c1-new')", table))
			}},
			{name: "literal reverse", run: func(t *testing.T, ctx context.Context, conn *sql.Conn, table string) {
				mustExec(t, ctx, conn, fmt.Sprintf(
					"replace into %s values(2,null,22,'c1-new'),(1,null,11,'p-new')", table))
			}},
			{name: "prepared", run: func(t *testing.T, ctx context.Context, conn *sql.Conn, table string) {
				stmt, err := conn.PrepareContext(ctx, fmt.Sprintf("replace into %s values(?,?,?,?),(?,?,?,?)", table))
				require.NoError(t, err)
				defer stmt.Close()
				_, err = stmt.ExecContext(ctx, 1, nil, 11, "p-new", 2, nil, 22, "c1-new")
				require.NoError(t, err)
			}},
			{name: "select", run: func(t *testing.T, ctx context.Context, conn *sql.Conn, table string) {
				source := table + "_src"
				mustExec(t, ctx, conn, fmt.Sprintf(
					"create table %s(seq int primary key, id int, pid int, marker int, v varchar(20))", source))
				mustExec(t, ctx, conn, fmt.Sprintf(
					"insert into %s values(1,1,null,11,'p-new'),(2,2,null,22,'c1-new')", source))
				mustExec(t, ctx, conn, fmt.Sprintf(
					"replace into %s select id,pid,marker,v from %s order by seq", table, source))
			}},
		}

		for i, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				table := fmt.Sprintf("self_setnull_owned_%d", i)
				mustExec(t, ctx, conn, fmt.Sprintf(`create table %s(
					id int primary key, pid int, marker int, v varchar(20),
					key sk_pid(pid), unique key uk_marker(marker),
					foreign key(pid) references %s(id) on delete set null)`, table, table))
				mustExec(t, ctx, conn, fmt.Sprintf(`insert into %s values
					(1,null,1,'p'),(2,1,2,'c1'),(3,2,3,'c2'),
					(4,1,4,'c3'),(9,null,9,'keep')`, table))

				tc.run(t, ctx, conn, table)

				var rows, distinctIDs int
				require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
					"select count(*), count(distinct id) from %s", table)).Scan(&rows, &distinctIDs))
				require.Equal(t, 5, rows)
				require.Equal(t, rows, distinctIDs)
				var nullChildren int
				require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
					"select count(*) from %s where id in (2,3,4) and pid is null", table)).Scan(&nullChildren))
				require.Equal(t, 3, nullChildren)
			})
		}
	})
}

func TestIssue26875MultipleUniqueSetNullActionsTerminate(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		conn := openIssue26875Conn(t, ctx, c, 0)
		defer conn.Close()
		dbName := createIssue26875Database(t, ctx, conn)
		defer dropIssue26875Database(t, conn, dbName)

		mustExec(t, ctx, conn, "create table multi_unique_p(id int primary key)")
		mustExec(t, ctx, conn, `create table multi_unique_c(
			id int primary key, p1 int, p2 int,
			unique key uk_p1(p1), unique key uk_p2(p2),
			foreign key(p1) references multi_unique_p(id) on delete set null,
			foreign key(p2) references multi_unique_p(id) on delete set null)`)
		mustExec(t, ctx, conn, "insert into multi_unique_p values(1),(2),(3),(9)")
		emptyExecCtx, emptyExecCancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := conn.ExecContext(emptyExecCtx, "replace into multi_unique_p values(3)")
		emptyExecCancel()
		require.NoError(t, err)
		mustExec(t, ctx, conn, "insert into multi_unique_c values(10,1,1),(20,2,2),(90,9,9)")

		execCtx, execCancel := context.WithTimeout(ctx, 10*time.Second)
		defer execCancel()
		_, err = conn.ExecContext(execCtx, "replace into multi_unique_p values(1)")
		require.NoError(t, err)

		var count int
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from multi_unique_c where id=10 and p1 is null and p2 is null").Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from multi_unique_c force index(uk_p1) where p1=9").Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from multi_unique_c force index(uk_p2) where p2=9").Scan(&count))
		require.Equal(t, 1, count)

		mustExec(t, ctx, conn, "create table multi_unique_mixed_p(id int primary key)")
		mustExec(t, ctx, conn, `create table multi_unique_mixed_c(
			id int primary key, p1 int, p2 int,
			unique key uk_p1(p1), unique key uk_p2(p2),
			foreign key(p1) references multi_unique_mixed_p(id) on delete set null,
			foreign key(p2) references multi_unique_mixed_p(id) on delete set null)`)
		mustExec(t, ctx, conn, "insert into multi_unique_mixed_p values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13)")
		mustExec(t, ctx, conn, `insert into multi_unique_mixed_c values
			(10,1,8),(11,9,1),(20,2,2),
			(30,3,6),(31,7,3),(40,4,10),(41,11,4),
			(50,5,12),(51,13,5)`)

		mixedExecCtx, mixedExecCancel := context.WithTimeout(ctx, 10*time.Second)
		defer mixedExecCancel()
		_, err = conn.ExecContext(mixedExecCtx, "replace into multi_unique_mixed_p values(1)")
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from multi_unique_mixed_c where id=10 and p1 is null and p2=8").Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from multi_unique_mixed_c where id=11 and p1=9 and p2 is null").Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from multi_unique_mixed_c force index(uk_p1) where p1=9").Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from multi_unique_mixed_c force index(uk_p2) where p2=8").Scan(&count))
		require.Equal(t, 1, count)
		_, err = conn.ExecContext(ctx, "insert into multi_unique_mixed_c values(12,null,8)")
		require.Error(t, err)
		_, err = conn.ExecContext(ctx, "insert into multi_unique_mixed_c values(13,9,null)")
		require.Error(t, err)

		prepared, err := conn.PrepareContext(ctx, "replace into multi_unique_mixed_p values(?)")
		require.NoError(t, err)
		defer prepared.Close()
		preparedCtx, preparedCancel := context.WithTimeout(ctx, 10*time.Second)
		_, err = prepared.ExecContext(preparedCtx, 3)
		require.NoError(t, err)
		_, err = prepared.ExecContext(preparedCtx, 4)
		preparedCancel()
		require.NoError(t, err)
		for _, check := range []struct {
			index string
			col   string
			value int
		}{
			{index: "uk_p2", col: "p2", value: 6},
			{index: "uk_p1", col: "p1", value: 7},
			{index: "uk_p2", col: "p2", value: 10},
			{index: "uk_p1", col: "p1", value: 11},
		} {
			require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
				"select count(*) from multi_unique_mixed_c force index(%s) where %s=?",
				check.index, check.col), check.value).Scan(&count))
			require.Equal(t, 1, count)
		}

		selectCtx, selectCancel := context.WithTimeout(ctx, 10*time.Second)
		_, err = conn.ExecContext(selectCtx, "replace into multi_unique_mixed_p select 5")
		selectCancel()
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from multi_unique_mixed_c force index(uk_p2) where p2=12").Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from multi_unique_mixed_c force index(uk_p1) where p1=13").Scan(&count))
		require.Equal(t, 1, count)
	})
}

func TestIssue26875MultilevelCascadeRemapClosure(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		conn := openIssue26875Conn(t, ctx, c, 0)
		defer conn.Close()
		dbName := createIssue26875Database(t, ctx, conn)
		defer dropIssue26875Database(t, conn, dbName)

		for i, indexDDL := range []string{
			"",
			", key sk_fk(%s)",
			", unique key uk_fk(%s)",
			", key sk_fk(%s), unique key uk_fk2(%s, id)",
		} {
			t.Run(fmt.Sprintf("index shape %d", i), func(t *testing.T) {
				p := fmt.Sprintf("cascade_l3_p_%d", i)
				m := fmt.Sprintf("cascade_l3_m_%d", i)
				g := fmt.Sprintf("cascade_l3_g_%d", i)
				mIndex := indexDDL
				gIndex := indexDDL
				if i > 0 && i < 3 {
					mIndex = fmt.Sprintf(indexDDL, "pid")
					gIndex = fmt.Sprintf(indexDDL, "mid")
				} else if i == 3 {
					mIndex = fmt.Sprintf(indexDDL, "pid", "pid")
					gIndex = fmt.Sprintf(indexDDL, "mid", "mid")
				}
				mustExec(t, ctx, conn, fmt.Sprintf("create table %s(id int primary key)", p))
				mustExec(t, ctx, conn, fmt.Sprintf(`create table %s(
					id int primary key, pid int%s,
					foreign key(pid) references %s(id) on delete cascade)`, m, mIndex, p))
				mustExec(t, ctx, conn, fmt.Sprintf(`create table %s(
					id int primary key, mid int%s,
					foreign key(mid) references %s(id) on delete cascade)`, g, gIndex, m))
				mustExec(t, ctx, conn, fmt.Sprintf("insert into %s values(1),(2)", p))
				mustExec(t, ctx, conn, fmt.Sprintf("insert into %s values(10,1),(20,2)", m))
				mustExec(t, ctx, conn, fmt.Sprintf("insert into %s values(100,10),(200,20)", g))

				mustExec(t, ctx, conn, fmt.Sprintf("replace into %s values(1)", p))

				var count int
				require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
					"select count(*) from %s where id in (10,100)", m)).Scan(&count))
				require.Zero(t, count)
				require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
					"select count(*) from %s where id=100", g)).Scan(&count))
				require.Zero(t, count)
				require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
					"select count(*) from %s where id=200 and mid=20", g)).Scan(&count))
				require.Equal(t, 1, count)
			})
		}
	})
}

func openIssue26875Conn(t *testing.T, ctx context.Context, c embed.Cluster, cnIndex int) *sql.Conn {
	t.Helper()
	cn, err := c.GetCNService(cnIndex)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf(
		"dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	return conn
}

func createIssue26875Database(t *testing.T, ctx context.Context, conn *sql.Conn) string {
	t.Helper()
	dbName := testutils.GetDatabaseName(t)
	mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
	mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
	return dbName
}

func dropIssue26875Database(t *testing.T, conn *sql.Conn, dbName string) {
	t.Helper()
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
	defer cleanupCancel()
	mustExec(t, cleanupCtx, conn, "use mo_catalog")
	mustExec(t, cleanupCtx, conn, fmt.Sprintf("drop database if exists `%s`", dbName))
}
