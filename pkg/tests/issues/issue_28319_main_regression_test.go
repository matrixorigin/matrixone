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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/stretchr/testify/require"
)

func issue28319Rows(t *testing.T, ctx context.Context, db *sql.DB, table string) [][2]int {
	t.Helper()
	rows, err := db.QueryContext(ctx, "select id,v from "+table+" order by id,v")
	require.NoError(t, err)
	defer rows.Close()
	var got [][2]int
	for rows.Next() {
		var row [2]int
		require.NoError(t, rows.Scan(&row[0], &row[1]))
		got = append(got, row)
	}
	require.NoError(t, rows.Err())
	return got
}

func TestIssue28319MainRowOracleRejectsDuplicate(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openIssue277xxDB(t, cn.GetServiceConfig().CN.Frontend.Port)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		const name = "issue28319_row_oracle"
		resetIssue277xxDatabase(t, ctx, db, name)
		defer func() {
			clean, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if _, err := db.ExecContext(clean, "drop database if exists "+name); err != nil {
				t.Errorf("database cleanup: %v", err)
			}
		}()
		execSQLRequire(t, ctx, db, "create table "+name+".t(id int,v int)")
		execSQLRequire(t, ctx, db, "insert into "+name+".t values (1,11),(1,11)")
		// Both former count checks accept this wrong result.
		require.Equal(t, 2, queryIssue277xxInt(t, ctx, db, "select count(*) from "+name+".t"))
		require.Equal(t, 2, queryIssue277xxInt(t, ctx, db, "select count(*) from "+name+".t where (id=1 and v=11) or (id=2 and v=20)"))
		got := issue28319Rows(t, ctx, db, name+".t")
		require.Equal(t, [][2]int{{1, 11}, {1, 11}}, got)
		require.NotEqual(t, [][2]int{{1, 11}, {2, 20}}, got)
	})
}

func TestIssue28319MainAddPKIndependentPublication(t *testing.T) {
	issue28319MainIndependentPublication(t, true)
}

func TestIssue28319MainDropPKIndependentPublication(t *testing.T) {
	issue28319MainIndependentPublication(t, false)
}

// The executor hook runs only after A's INSERT SELECT has completed and before
// publication. B must complete while A is still paused at that exact boundary.
func issue28319MainIndependentPublication(t *testing.T, add bool) {
	t.Helper()
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openIssue277xxDB(t, cn.GetServiceConfig().CN.Frontend.Port)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		name := "issue28319_main_drop"
		if add {
			name = "issue28319_main_add"
		}
		resetIssue277xxDatabase(t, ctx, db, name)
		defer func() {
			clean, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if _, err := db.ExecContext(clean, "drop database if exists "+name); err != nil {
				t.Errorf("database cleanup: %v", err)
			}
		}()
		var workers sync.WaitGroup
		defer func() { cancel(); workers.Wait() }()
		key := " primary key"
		action := "drop primary key"
		if add {
			key = ""
			action = "add primary key(id)"
		}
		for _, table := range []string{"a", "b"} {
			execSQLRequire(t, ctx, db, fmt.Sprintf("create table %s.%s(id int%s, v int)", name, table, key))
			execSQLRequire(t, ctx, db, fmt.Sprintf("insert into %s.%s values (1,10),(2,20)", name, table))
		}
		paused, resume := make(chan struct{}), make(chan struct{})
		var once sync.Once
		release := func() { once.Do(func() { close(resume) }) }
		defer release()
		observer := issue31401Observe(t, cn.ServiceID())
		observer.afterCopy = func(call context.Context, n int32) error {
			if n != 1 {
				return nil
			}
			close(paused)
			select {
			case <-resume:
				return nil
			case <-call.Done():
				return call.Err()
			}
		}
		aDone := make(chan error, 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, e := db.ExecContext(ctx, fmt.Sprintf("alter table %s.a algorithm=copy, %s", name, action))
			aDone <- e
		}()
		select {
		case <-paused:
		case e := <-aDone:
			t.Fatalf("A ended before copy barrier: %v", e)
		case <-ctx.Done():
			t.Fatal("A did not reach copy barrier")
		}
		bDone := make(chan error, 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, e := db.ExecContext(ctx, fmt.Sprintf("alter table %s.b algorithm=copy, %s", name, action))
			bDone <- e
		}()
		select {
		case err := <-bDone:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Error("B did not finish while A was paused")
		}
		release()
		select {
		case err := <-aDone:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Error("A did not finish after release")
		}
		require.EqualValues(t, 2, observer.copies.Load())
		for _, table := range []string{"a", "b"} {
			require.Equal(t, [][2]int{{1, 10}, {2, 20}}, issue28319Rows(t, ctx, db, name+"."+table))
			var tableName, ddl string
			require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("show create table %s.%s", name, table)).Scan(&tableName, &ddl))
			require.Equal(t, add, strings.Contains(strings.ToUpper(ddl), "PRIMARY KEY"))
			execSQLRequire(t, ctx, db, fmt.Sprintf("insert into %s.%s values (3,30)", name, table))
		}
	})
}

func TestIssue28319MainAddPKConcurrentInsert(t *testing.T) {
	issue28319MainConcurrentWrite(t, true, "insert")
}

func TestIssue28319MainDropPKConcurrentInsert(t *testing.T) {
	issue28319MainConcurrentWrite(t, false, "insert")
}

// Commit a source write after the first physical copy. The committed row must
// survive the bounded refresh and the request must not prepare a third copy.
func TestIssue28319MainAddPKConcurrentUpdate(t *testing.T) {
	issue28319MainConcurrentWrite(t, true, "update")
}
func TestIssue28319MainDropPKConcurrentDelete(t *testing.T) {
	issue28319MainConcurrentWrite(t, false, "delete")
}
func TestIssue28319MainAddPKConcurrentDelete(t *testing.T) {
	issue28319MainConcurrentWrite(t, true, "delete")
}
func TestIssue28319MainDropPKConcurrentUpdate(t *testing.T) {
	issue28319MainConcurrentWrite(t, false, "update")
}

func TestIssue28319MainAddPKKillAfterCopy(t *testing.T) {
	issue28319MainKillAfterCopy(t, true)
}

func TestIssue28319MainDropPKKillAfterCopy(t *testing.T) {
	issue28319MainKillAfterCopy(t, false)
}

// KILL QUERY targets the server-side statement; a disconnected client alone
// does not establish whether the server committed or rolled back.
func issue28319MainKillAfterCopy(t *testing.T, add bool) {
	t.Helper()
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openIssue277xxDB(t, cn.GetServiceConfig().CN.Frontend.Port)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		name, key, action := "issue28319_kill_drop", " primary key", "drop primary key"
		if add {
			name, key, action = "issue28319_kill_add", "", "add primary key(id)"
		}
		resetIssue277xxDatabase(t, ctx, db, name)
		defer func() {
			clean, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if _, err := db.ExecContext(clean, "drop database if exists "+name); err != nil {
				t.Errorf("database cleanup: %v", err)
			}
		}()
		var workers sync.WaitGroup
		defer func() { cancel(); workers.Wait() }()
		execSQLRequire(t, ctx, db, fmt.Sprintf("create table %s.t(id int%s,v int)", name, key))
		execSQLRequire(t, ctx, db, "insert into "+name+".t values(1,10),(2,20)")
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() { cancel(); _ = conn.Close() }()
		var connectionID uint32
		require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
		paused, release := make(chan struct{}), make(chan struct{})
		defer close(release)
		observer := issue31401Observe(t, cn.ServiceID())
		observer.afterCopy = func(call context.Context, n int32) error {
			if n == 1 {
				close(paused)
				select {
				case <-call.Done():
					return call.Err()
				case <-release:
					return errors.New("copy barrier released during cleanup")
				}
			}
			return nil
		}
		done := make(chan error, 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := conn.ExecContext(ctx, fmt.Sprintf("alter table %s.t algorithm=copy, %s", name, action))
			done <- err
		}()
		select {
		case <-paused:
		case err := <-done:
			t.Fatalf("ALTER ended before copy barrier: %v", err)
		case <-ctx.Done():
			t.Fatal("copy barrier not reached")
		}
		execSQLRequire(t, ctx, db, fmt.Sprintf("kill query %d", connectionID))
		select {
		case err := <-done:
			require.Error(t, err)
		case <-ctx.Done():
			t.Fatal("killed ALTER did not terminate")
		}
		require.Equal(t, [][2]int{{1, 10}, {2, 20}}, issue28319Rows(t, ctx, db, name+".t"))
		var table, ddl string
		require.NoError(t, db.QueryRowContext(ctx, "show create table "+name+".t").Scan(&table, &ddl))
		require.Equal(t, !add, strings.Contains(strings.ToUpper(ddl), "PRIMARY KEY"))
		require.Zero(t, queryIssue277xxInt(t, ctx, db, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname like 't_copy_%'", name))
		execSQLRequire(t, ctx, db, fmt.Sprintf("alter table %s.t algorithm=copy, %s", name, action))
	})
}

func issue28319MainConcurrentWrite(t *testing.T, add bool, write string) {
	t.Helper()
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openIssue277xxDB(t, cn.GetServiceConfig().CN.Frontend.Port)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		name := "issue28319_main_write_drop_" + write
		if add {
			name = "issue28319_main_write_add_" + write
		}
		resetIssue277xxDatabase(t, ctx, db, name)
		defer func() {
			clean, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if _, e := db.ExecContext(clean, "drop database if exists "+name); e != nil {
				t.Errorf("database cleanup: %v", e)
			}
		}()
		var workers sync.WaitGroup
		defer func() { cancel(); workers.Wait() }()
		key, action := " primary key", "drop primary key"
		if add {
			key, action = "", "add primary key(id)"
		}
		execSQLRequire(t, ctx, db, fmt.Sprintf("create table %s.t(id int%s, v int)", name, key))
		execSQLRequire(t, ctx, db, fmt.Sprintf("insert into %s.t values (1,10),(2,20)", name))
		paused, resume := make(chan struct{}), make(chan struct{})
		var once sync.Once
		release := func() { once.Do(func() { close(resume) }) }
		defer release()
		observer := issue31401Observe(t, cn.ServiceID())
		observer.afterCopy = func(call context.Context, n int32) error {
			if n != 1 {
				return nil
			}
			close(paused)
			select {
			case <-resume:
				return nil
			case <-call.Done():
				return call.Err()
			}
		}
		done := make(chan error, 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, e := db.ExecContext(ctx, fmt.Sprintf("alter table %s.t algorithm=copy, %s", name, action))
			done <- e
		}()
		select {
		case <-paused:
		case e := <-done:
			t.Fatalf("ALTER ended before barrier: %v", e)
		case <-ctx.Done():
			t.Fatal("copy barrier not reached")
		}
		switch write {
		case "insert":
			execSQLRequire(t, ctx, db, fmt.Sprintf("insert into %s.t values (3,30)", name))
		case "update":
			execSQLRequire(t, ctx, db, fmt.Sprintf("update %s.t set v=11 where id=1", name))
		case "delete":
			execSQLRequire(t, ctx, db, fmt.Sprintf("delete from %s.t where id=1", name))
		default:
			t.Fatalf("unknown write: %s", write)
		}
		release()
		select {
		case e := <-done:
			require.NoError(t, e)
		case <-ctx.Done():
			t.Fatal("ALTER did not complete")
		}
		require.EqualValues(t, 2, observer.copies.Load())
		expected := [][2]int{{1, 10}, {2, 20}, {3, 30}}
		if write == "update" {
			expected = [][2]int{{1, 11}, {2, 20}}
		}
		if write == "delete" {
			expected = [][2]int{{2, 20}}
		}
		require.Equal(t, expected, issue28319Rows(t, ctx, db, name+".t"))
		var tableName, ddl string
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("show create table %s.t", name)).Scan(&tableName, &ddl))
		require.Equal(t, add, strings.Contains(strings.ToUpper(ddl), "PRIMARY KEY"))
	})
}

func TestIssue28319MainAddPKPublicationFailure(t *testing.T)  { issue28319PublicationFailure(t, true) }
func TestIssue28319MainDropPKPublicationFailure(t *testing.T) { issue28319PublicationFailure(t, false) }
func issue28319PublicationFailure(t *testing.T, add bool) {
	t.Helper()
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openIssue277xxDB(t, cn.GetServiceConfig().CN.Frontend.Port)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		name := "issue28319_fail_drop"
		key, action := " primary key", "drop primary key"
		if add {
			name = "issue28319_fail_add"
			key = ""
			action = "add primary key(id)"
		}
		resetIssue277xxDatabase(t, ctx, db, name)
		defer func() {
			clean, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if _, e := db.ExecContext(clean, "drop database if exists "+name); e != nil {
				t.Errorf("database cleanup: %v", e)
			}
		}()
		execSQLRequire(t, ctx, db, fmt.Sprintf("create table %s.t(id int%s,v int)", name, key))
		execSQLRequire(t, ctx, db, fmt.Sprintf("insert into %s.t values (1,10),(2,20)", name))
		observer := issue31401Observe(t, cn.ServiceID())
		var injected atomic.Bool
		observer.beforeSourceDrop = func() error {
			injected.Store(true)
			return errors.New("issue28319 injected source drop failure")
		}
		_, err = db.ExecContext(ctx, fmt.Sprintf("alter table %s.t algorithm=copy, %s", name, action))
		require.Error(t, err)
		require.True(t, injected.Load())
		observer.beforeSourceDrop = nil
		require.EqualValues(t, 1, observer.copies.Load())
		require.Equal(t, [][2]int{{1, 10}, {2, 20}}, issue28319Rows(t, ctx, db, name+".t"))
		var tableName, ddl string
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("show create table %s.t", name)).Scan(&tableName, &ddl))
		require.Equal(t, !add, strings.Contains(strings.ToUpper(ddl), "PRIMARY KEY"))
		require.Zero(t, queryIssue277xxInt(t, ctx, db, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname like 't_copy_%'", name))
		execSQLRequire(t, ctx, db, fmt.Sprintf("alter table %s.t algorithm=copy, %s", name, action))
	})
}

func TestIssue28319MainAddPKHeldGate(t *testing.T)  { issue28319MainHeldGate(t, true) }
func TestIssue28319MainDropPKHeldGate(t *testing.T) { issue28319MainHeldGate(t, false) }
func issue28319MainHeldGate(t *testing.T, add bool) {
	t.Helper()
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openIssue277xxDB(t, cn.GetServiceConfig().CN.Frontend.Port)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		name := "issue28319_gate_drop"
		key, action := " primary key", "drop primary key"
		if add {
			name = "issue28319_gate_add"
			key = ""
			action = "add primary key(id)"
		}
		resetIssue277xxDatabase(t, ctx, db, name)
		defer func() {
			clean, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if _, e := db.ExecContext(clean, "drop database if exists "+name); e != nil {
				t.Errorf("database cleanup: %v", e)
			}
		}()
		var workers sync.WaitGroup
		defer func() { cancel(); workers.Wait() }()
		for _, tbl := range []string{"a", "b"} {
			execSQLRequire(t, ctx, db, fmt.Sprintf("create table %s.%s(id int%s,v int)", name, tbl, key))
			execSQLRequire(t, ctx, db, fmt.Sprintf("insert into %s.%s values (1,10),(2,20)", name, tbl))
		}
		var registry uint64
		require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_feature_registry'").Scan(&registry))
		holder, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer holder.Rollback()
		var feature string
		require.NoError(t, holder.QueryRowContext(ctx, catalog.SnapshotLifecycleGateSQL).Scan(&feature))
		copied := make(chan struct{}, 2)
		observer := issue31401Observe(t, cn.ServiceID())
		observer.afterCopy = func(call context.Context, _ int32) error {
			select {
			case copied <- struct{}{}:
			default:
			}
			return call.Err()
		}
		waiters := make(chan string, 8)
		restore := lockservice.SetWaiterEnqueuedHookForTest(func(id uint64, actor []byte, _ [][]byte) {
			if id == registry {
				select {
				case waiters <- string(actor):
				default:
				}
			}
		})
		defer restore()
		done := make(chan error, 2)
		for _, tbl := range []string{"a", "b"} {
			workers.Add(1)
			go func() {
				defer workers.Done()
				_, e := db.ExecContext(ctx, fmt.Sprintf("alter table %s.%s algorithm=copy, %s", name, tbl, action))
				done <- e
			}()
		}
		for i := 0; i < 2; i++ {
			select {
			case <-copied:
			case e := <-done:
				t.Fatalf("ALTER ended before both copies: %v", e)
			case <-ctx.Done():
				t.Fatal("copy did not proceed under held gate")
			}
		}
		actors := map[string]bool{}
		for len(actors) < 2 {
			select {
			case actor := <-waiters:
				actors[actor] = true
			case e := <-done:
				t.Fatalf("ALTER ended while gate held: %v", e)
			case <-ctx.Done():
				t.Fatal("two publication waiters not observed")
			}
		}
		select {
		case e := <-done:
			t.Fatalf("ALTER completed while gate held: %v", e)
		default:
		}
		require.NoError(t, holder.Commit())
		for i := 0; i < 2; i++ {
			select {
			case e := <-done:
				require.NoError(t, e)
			case <-ctx.Done():
				t.Fatal("ALTER did not finish after gate release")
			}
		}
		require.EqualValues(t, 2, observer.copies.Load())
		for _, tbl := range []string{"a", "b"} {
			require.Equal(t, [][2]int{{1, 10}, {2, 20}}, issue28319Rows(t, ctx, db, name+"."+tbl))
			var tableName, ddl string
			require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("show create table %s.%s", name, tbl)).Scan(&tableName, &ddl))
			require.Equal(t, add, strings.Contains(strings.ToUpper(ddl), "PRIMARY KEY"))
		}
	})
}

func TestIssue28319MainSameTableRejectsStaleCopy(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openIssue277xxDB(t, cn.GetServiceConfig().CN.Frontend.Port)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		name := "issue28319_same_table"
		resetIssue277xxDatabase(t, ctx, db, name)
		defer func() {
			clean, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if _, e := db.ExecContext(clean, "drop database if exists "+name); e != nil {
				t.Errorf("database cleanup: %v", e)
			}
		}()
		var workers sync.WaitGroup
		defer func() { cancel(); workers.Wait() }()
		execSQLRequire(t, ctx, db, "create table "+name+".t(id int,v int)")
		execSQLRequire(t, ctx, db, "insert into "+name+".t values(1,10),(2,20)")
		paused, resume := make(chan struct{}), make(chan struct{})
		var once sync.Once
		release := func() { once.Do(func() { close(resume) }) }
		defer release()
		observer := issue31401Observe(t, cn.ServiceID())
		observer.afterCopy = func(call context.Context, n int32) error {
			if n != 1 {
				return nil
			}
			close(paused)
			select {
			case <-resume:
				return nil
			case <-call.Done():
				return call.Err()
			}
		}
		done := make(chan error, 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, e := db.ExecContext(ctx, "alter table "+name+".t algorithm=copy, add primary key(id)")
			done <- e
		}()
		select {
		case <-paused:
		case e := <-done:
			t.Fatalf("A ended before barrier: %v", e)
		case <-ctx.Done():
			t.Fatal("A did not prepare")
		}
		execSQLRequire(t, ctx, db, "alter table "+name+".t algorithm=copy, add primary key(id)")
		release()
		select {
		case e := <-done:
			requireIssue277xxMySQLError(t, e, moerr.ErrTxnWriteConflict)
		case <-ctx.Done():
			t.Fatal("stale ALTER did not terminate")
		}
		require.EqualValues(t, 2, observer.copies.Load())
		require.Equal(t, [][2]int{{1, 10}, {2, 20}}, issue28319Rows(t, ctx, db, name+".t"))
		var tbl, ddl string
		require.NoError(t, db.QueryRowContext(ctx, "show create table "+name+".t").Scan(&tbl, &ddl))
		require.Contains(t, strings.ToUpper(ddl), "PRIMARY KEY")
		require.Zero(t, queryIssue277xxInt(t, ctx, db, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname like 't_copy_%'", name))
	})
}

func TestIssue28319MainAddPKRefreshCopyFailure(t *testing.T)  { issue28319RefreshCopyFailure(t, true) }
func TestIssue28319MainDropPKRefreshCopyFailure(t *testing.T) { issue28319RefreshCopyFailure(t, false) }
func issue28319RefreshCopyFailure(t *testing.T, add bool) {
	t.Helper()
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openIssue277xxDB(t, cn.GetServiceConfig().CN.Frontend.Port)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		name := "issue28319_refresh_fail_drop"
		key, action := " primary key", "drop primary key"
		if add {
			name = "issue28319_refresh_fail_add"
			key = ""
			action = "add primary key(id)"
		}
		resetIssue277xxDatabase(t, ctx, db, name)
		defer func() {
			clean, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if _, e := db.ExecContext(clean, "drop database if exists "+name); e != nil {
				t.Errorf("database cleanup: %v", e)
			}
		}()
		var workers sync.WaitGroup
		defer func() { cancel(); workers.Wait() }()
		execSQLRequire(t, ctx, db, fmt.Sprintf("create table %s.t(id int%s,v int)", name, key))
		execSQLRequire(t, ctx, db, "insert into "+name+".t values(1,10),(2,20)")
		paused, resume := make(chan struct{}), make(chan struct{})
		var once sync.Once
		release := func() { once.Do(func() { close(resume) }) }
		defer release()
		observer := issue31401Observe(t, cn.ServiceID())
		var injected atomic.Bool
		observer.afterCopy = func(call context.Context, n int32) error {
			if n == 2 {
				injected.Store(true)
				return errors.New("issue28319 injected refresh failure")
			}
			if n != 1 {
				return nil
			}
			close(paused)
			select {
			case <-resume:
				return nil
			case <-call.Done():
				return call.Err()
			}
		}
		done := make(chan error, 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, e := db.ExecContext(ctx, fmt.Sprintf("alter table %s.t algorithm=copy, %s", name, action))
			done <- e
		}()
		select {
		case <-paused:
		case e := <-done:
			t.Fatalf("ALTER ended before copy barrier: %v", e)
		case <-ctx.Done():
			t.Fatal("copy barrier not reached")
		}
		execSQLRequire(t, ctx, db, "insert into "+name+".t values(3,30)")
		release()
		select {
		case e := <-done:
			require.Error(t, e)
		case <-ctx.Done():
			t.Fatal("failed refresh did not terminate")
		}
		require.True(t, injected.Load())
		require.EqualValues(t, 2, observer.copies.Load())
		require.Equal(t, [][2]int{{1, 10}, {2, 20}, {3, 30}}, issue28319Rows(t, ctx, db, name+".t"))
		var tbl, ddl string
		require.NoError(t, db.QueryRowContext(ctx, "show create table "+name+".t").Scan(&tbl, &ddl))
		require.Equal(t, !add, strings.Contains(strings.ToUpper(ddl), "PRIMARY KEY"))
		require.Zero(t, queryIssue277xxInt(t, ctx, db, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname like 't_copy_%'", name))
		execSQLRequire(t, ctx, db, fmt.Sprintf("alter table %s.t algorithm=copy, %s", name, action))
	})
}
