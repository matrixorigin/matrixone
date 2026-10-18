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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestOrderedInsertIgnoreForeignKeys(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(t *testing.T, statement string, args ...any) sql.Result {
			t.Helper()
			r, e := conn.ExecContext(ctx, statement, args...)
			require.NoError(t, e, statement)
			return r
		}
		count := func(t *testing.T, statement string, want int) {
			t.Helper()
			var got int
			require.NoError(t, conn.QueryRowContext(ctx, statement).Scan(&got), statement)
			require.Equal(t, want, got, statement)
		}
		const schema = "ordered_ignore_fk"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec(t, "create database "+schema)
		exec(t, "use "+schema)
		exec(t, "create table control_parent(id int primary key)")
		exec(t, "insert into control_parent values(1),(2),(3)")
		for _, kind := range []string{"pk", "unique", "resident"} {
			t.Run(kind, func(t *testing.T) {
				key := "id"
				extra := ""
				if kind != "pk" {
					key = "k"
				}
				if kind == "unique" {
					extra = ", unique(k)"
				} else if kind == "resident" {
					extra = ",unique(k,id)"
				}
				parent, child := "parent_"+kind, "child_"+kind
				exec(t, "create table "+parent+"(id int primary key,k int,payload int"+extra+")")
				exec(t, "insert into "+parent+" values(1,1,0),(2,2,0),(3,3,0)")
				exec(t, "create table "+child+"(id int auto_increment primary key,u int unique,foreign key(id) references "+parent+"("+key+"))")
				r := exec(t, "insert ignore into "+child+"(u) values(10),(10),(20),(30),(40)")
				affected, e := r.RowsAffected()
				require.NoError(t, e)
				require.EqualValues(t, 3, affected)
				count(t, "select count(*) from "+child+" where (id,u) in ((1,10),(2,20),(3,30))", 3)
				count(t, "select count(*) from "+child+" c left join "+parent+" p on c.id=p."+key+" where p.id is null", 0)
				exec(t, "begin")
				exec(t, "insert ignore into "+child+" values(2,99)")
				exec(t, "rollback")
				count(t, "select count(*) from "+child, 3)
			})
		}
		if t.Failed() {
			return
		}
		t.Run("nan", func(t *testing.T) {
			exec(t, "create table self_nan(id int primary key,k double,p double,unique(k,id),foreign key(p) references self_nan(k))")
			exec(t, "insert ignore into self_nan values(1,cast('NaN' as double),NULL),(2,1,cast('NaN' as double))")
			count(t, "select count(*) from self_nan", 1)
			exec(t, "insert ignore into self_nan values(3,cast('-0' as double),0),(4,2,cast('-0' as double))")
			count(t, "select count(*) from self_nan where id in (1,3,4)", 3)
		})
		t.Run("local_check", func(t *testing.T) {
			const handler = "ordered_insert_ignore_check"
			mysql.RegisterReaderHandler(handler, func() io.Reader { return strings.NewReader("-1\n1\n-1\n2\n") })
			defer mysql.DeregisterReaderHandler(handler)
			exec(t, "create table local_check(id int auto_increment primary key,v int check(v>0))")
			exec(t, "load data local infile 'Reader::"+handler+"' into table local_check fields terminated by ',' (v)")
			warnings, err := conn.QueryContext(ctx, "show warnings")
			require.NoError(t, err)
			defer warnings.Close()
			checkWarnings := 0
			for warnings.Next() {
				var level, message string
				var code int
				require.NoError(t, warnings.Scan(&level, &code, &message))
				if code == 3819 {
					checkWarnings++
				}
			}
			require.NoError(t, warnings.Err())
			require.Equal(t, 2, checkWarnings, "one CHECK warning per skipped row")
			count(t, "select count(*) from local_check where v<=0", 0)
			count(t, "select count(*) from local_check where (id,v) in ((1,1),(2,2))", 2)
			exec(t, "create table strict_check(v int check(v>0))")
			_, err = conn.ExecContext(ctx, "load data inline format='csv', data='-1\n1' into table strict_check fields terminated by ','")
			require.Error(t, err)
			require.Contains(t, strings.ToLower(err.Error()), "check")
			count(t, "select count(*) from strict_check", 0)
			exec(t, "load data inline format='csv', data='1' into table strict_check fields terminated by ','")
			count(t, "select count(*) from strict_check", 1)
			exec(t, "create table replace_check(v int check(v>0))")
			_, err = conn.ExecContext(ctx, "load data local infile 'Reader::"+handler+"' replace into table replace_check fields terminated by ',' (v)")
			require.Error(t, err)
			require.Contains(t, strings.ToLower(err.Error()), "check")
			count(t, "select count(*) from replace_check", 0)

		})

		exec(t, "create table self_pair(id int auto_increment primary key,p int,region int,child_region int,unique(id,region,p),foreign key(p,child_region) references self_pair(id,region))")
		exec(t, "insert ignore into self_pair values(1,1,10,10),(NULL,1,20,10),(NULL,999,30,10),(NULL,2,30,20)")
		count(t, "select count(*) from self_pair where (id,p,region,child_region) in ((1,1,10,10),(2,1,20,10),(3,2,30,20))", 3)
		exec(t, "create table self_text(id int auto_increment primary key,label varchar(16) collate utf8mb4_unicode_ci,ref varchar(16) collate utf8mb4_unicode_ci, unique(label,id), foreign key(ref) references self_text(label))")
		exec(t, "insert ignore into self_text values(1,'Résumé','resume'),(NULL,'X','RESUME'),(NULL,'bad','absent')")
		count(t, "select count(*) from self_text", 2)
		exec(t, "insert ignore into self_text values(NULL,'Y','resume')")
		count(t, "select count(*) from self_text", 3)
		exec(t, "create table prepare_child(id int auto_increment primary key,u int,foreign key(id) references control_parent(id))")
		prepared, err := conn.PrepareContext(ctx, "insert ignore into prepare_child(u) values(?)")
		require.NoError(t, err)
		defer prepared.Close()
		for i := range 2 {
			_, err = prepared.ExecContext(ctx, i)
			require.NoError(t, err)
		}
		count(t, "select count(*) from prepare_child", 2)
		exec(t, "set foreign_key_checks=0")
		exec(t, "insert into prepare_child values(999,999)")
		exec(t, "set foreign_key_checks=1")
		count(t, "select count(*) from prepare_child", 3)
		exec(t, "create table load_child(id int auto_increment primary key,u int unique,foreign key(id) references control_parent(id))")
		exec(t, "load data inline format='csv', data='10\n10\n20\n30\n40' ignore into table load_child fields terminated by ',' (u)")
		count(t, "select count(*) from load_child where (id,u) in ((1,10),(2,20),(3,30))", 3)
		exec(t, "create table rollback_child(id int auto_increment primary key,u int unique,foreign key(id) references control_parent(id))")
		exec(t, "begin")
		exec(t, "insert ignore into rollback_child(u) values(1),(2)")
		exec(t, "rollback")
		count(t, "select count(*) from rollback_child", 0)
		testOrderedFKWorkloads(t, conn, ctx, exec)
		for _, kind := range []string{"pk", "unique", "resident"} {
			for _, action := range []string{"cancel_parent", "cancel_child", "refresh"} {
				t.Run(kind+"/"+action, func(t *testing.T) { testOrderedFKWitnessLocks(t, db, schema, kind, action) })
			}
		}
	})
}

// Observe an actual lock-service waiter before cancellation or refresh. The
// unrelated parent write must finish while the child transaction still holds
// its witness; elapsed time without completion is never the wait oracle.
func testOrderedFKWitnessLocks(t *testing.T, db *sql.DB, schema, kind, action string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	parent := schema + ".lock_parent_" + kind + "_" + action
	child := schema + ".lock_child_" + kind + "_" + action
	exec := func(q string) { t.Helper(); _, err := db.ExecContext(ctx, q); require.NoError(t, err, q) }
	key, extra := "id", ""
	if kind != "pk" {
		key = "k"
		extra = ",unique(k,id)"
	}
	if kind == "unique" {
		extra = ",unique(k)"
	}
	exec("create table " + parent + "(id int primary key,k int,payload int" + extra + ")")
	exec("insert into " + parent + " values(1,1,0),(2,2,0)")
	exec("create table " + child + "(id int auto_increment primary key,u int,foreign key(id) references " + parent + "(" + key + "))")
	holder, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer holder.Rollback()
	childSQL := "insert ignore into " + child + "(u) values(1)"
	parentSQL := "delete from " + parent + " where id=1"
	held, competing := childSQL, parentSQL
	if action != "cancel_parent" {
		held, competing = parentSQL, childSQL
	}
	_, err = holder.ExecContext(ctx, held)
	require.NoError(t, err)
	if action == "cancel_parent" {
		// A table lock would block this statement until the holder ends.
		progressCtx, progressCancel := context.WithTimeout(ctx, 3*time.Second)
		_, err = db.ExecContext(progressCtx, "update "+parent+" set payload=1 where id=2")
		progressCancel()
		require.NoError(t, err, "unreferenced parent writes must progress before lock escalation")
	}
	var parentID uint64
	parentName := strings.TrimPrefix(parent, schema+".")
	require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?", schema, parentName).Scan(&parentID))
	namespaces := []string{strconv.FormatUint(parentID, 10)}
	indexNames, err := db.QueryContext(ctx, "select distinct index_table_name from mo_catalog.mo_indexes where table_id=? and index_table_name<>''", parentID)
	require.NoError(t, err)
	defer indexNames.Close()
	for indexNames.Next() {
		var name string
		require.NoError(t, indexNames.Scan(&name))
		var id uint64
		require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?", schema, name).Scan(&id))
		namespaces = append(namespaces, strconv.FormatUint(id, 10))
	}
	require.NoError(t, indexNames.Err())

	waitSQL := "select count(*) from mo_locks() l where l.lock_wait<>'' and l.table_id in (" + strings.Join(namespaces, ",") + ")"
	writerCtx, cancelWriter := context.WithCancel(ctx)
	defer cancelWriter()
	done := make(chan error, 1)
	go func() { _, e := db.ExecContext(writerCtx, competing); done <- e }()
	require.Eventually(t, func() bool {
		var waiting int
		e := db.QueryRowContext(ctx, waitSQL).Scan(&waiting)
		require.NoError(t, e)
		return waiting > 0
	}, 5*time.Second, 10*time.Millisecond, "must observe a real waiter on a base or hidden-index namespace")
	triggered := time.Now()
	if action == "refresh" {
		require.NoError(t, holder.Commit())
	} else {
		cancelWriter()
	}
	select {
	case err = <-done:
		if action == "refresh" {
			require.NoError(t, err, "refreshed input must be replayed instead of reusing its old witness")
		} else {
			require.Error(t, err)
			require.ErrorIs(t, writerCtx.Err(), context.Canceled)
		}
	case <-ctx.Done():
		t.Fatal("writer did not finish after cancellation/refresh")
	}
	if action != "refresh" {
		require.NoError(t, holder.Rollback())
	}
	var children int
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+child).Scan(&children))
	if action == "refresh" {
		// A retry consumes a fresh lease. ID 2 can now be accepted against its
		// still-existing parent; requiring zero rows would assert ID reuse.
		require.Equal(t, 1, children)
		var invalid int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+child+" c left join "+parent+" p on c.id=p."+key+" where p.id is null or c.id=1").Scan(&invalid))
		require.Zero(t, invalid, "replay must discard the old positive witness and deleted ID")
	} else {
		require.Zero(t, children, "cancelled/rolled-back children must not become visible")
	}
	require.Eventually(t, func() bool {
		var n int
		e := db.QueryRowContext(ctx, waitSQL).Scan(&n)
		require.NoError(t, e)
		return n == 0
	}, 3*time.Second, 10*time.Millisecond, "no waiter may outlive the completed operation")
	t.Logf("kind=%s action=%s actual_waiter=true namespaces=%v trigger_to_complete=%s children=%d", kind, action, namespaces, time.Since(triggered), children)
}

// Small workload samples are diagnostics, not p99 or production throughput
// qualification. DDL, fixture population and result oracles are outside timing.
func testOrderedFKWorkloads(t *testing.T, conn *sql.Conn, ctx context.Context, exec func(*testing.T, string, ...any) sql.Result) {
	for _, p := range []int{64, 1024} {
		parent := fmt.Sprintf("work_parent_%d", p)
		exec(t, "create table "+parent+"(id int primary key,k int,unique(k,id))")
		values := make([]string, p)
		for i := range values {
			values[i] = fmt.Sprintf("(%d,%d)", i+1, i/2+1)
		}
		exec(t, "insert into "+parent+" values"+strings.Join(values, ","))
		for _, n := range []int{1, 8, 32} {
			for _, ordered := range []bool{false, true} {
				child := fmt.Sprintf("work_child_%d_%d_%t", p, n, ordered)
				key := "p"
				if ordered {
					key = "id"
				}
				exec(t, "create table "+child+"(id int auto_increment primary key,u int,p int,foreign key("+key+") references "+parent+"(k))")
				rows := make([]string, n)
				for i := range rows {
					rows[i] = fmt.Sprintf("(%d,%d)", i+1, i+1)
				}
				started := time.Now()
				result, err := conn.ExecContext(ctx, "insert ignore into "+child+"(u,p) values"+strings.Join(rows, ","))
				elapsed := time.Since(started)
				require.NoError(t, err)
				affected, err := result.RowsAffected()
				require.NoError(t, err)
				require.EqualValues(t, n, affected)
				var invalid int
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+child+" c left join "+parent+" p on c."+key+"=p.k where p.id is null").Scan(&invalid))
				require.Zero(t, invalid)
				t.Logf("workload ordered=%t parents=%d candidates=%d accepted=%d sql_execution=%s", ordered, p, n, affected, elapsed)
			}
		}
	}
}
