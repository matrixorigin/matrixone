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

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestIssue23392RenameIndexInplace(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		writer, err := cluster.GetCNService(0)
		require.NoError(t, err)
		reader, err := cluster.GetCNService(1)
		require.NoError(t, err)
		db := issue30936Open(t, cluster, writer)
		defer db.Close()
		other := issue30936Open(t, cluster, reader)
		defer other.Close()

		database := strings.ToLower(testutils.GetDatabaseName(t))
		table := database + ".target"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer execSQLMaybe(t, context.Background(), db, "drop database if exists "+database)
		execSQLRequire(t, ctx, db, "create table "+table+
			" (id int primary key, v int, w int, body mediumtext, index ix_a(v,w), unique key ix_b(w), unique key spare(id,w))")
		execSQLRequire(t, ctx, db, "insert into "+table+" values (1, 10, 20, repeat('x', 70000))")

		type indexIdentity struct {
			tableID, indexID, hiddenTableID uint64
			position                        uint32
			column                          string
		}
		readIndex := func(name string) []indexIdentity {
			t.Helper()
			query := fmt.Sprintf("select i.table_id, i.id, h.rel_id, i.ordinal_position, i.column_name, i.name "+
				"from mo_catalog.mo_indexes i join mo_catalog.mo_tables p on i.table_id = p.rel_id "+
				"join mo_catalog.mo_tables h on i.index_table_name = h.relname and p.reldatabase_id = h.reldatabase_id "+
				"where p.reldatabase = '%s' and p.relname = 'target' order by i.ordinal_position", database)
			rows, err := other.QueryContext(ctx, query)
			require.NoError(t, err)
			defer rows.Close()
			var identities []indexIdentity
			for rows.Next() {
				var index indexIdentity
				var actualName string
				require.NoError(t, rows.Scan(&index.tableID, &index.indexID, &index.hiddenTableID, &index.position, &index.column, &actualName))
				if actualName == name {
					identities = append(identities, index)
				}
			}
			require.NoError(t, rows.Err())
			return identities
		}
		readShowCreate := func() string {
			t.Helper()
			var name, ddl string
			require.NoError(t, other.QueryRowContext(ctx, "show create table "+table).Scan(&name, &ddl))
			return ddl
		}
		readBodyHash := func() string {
			t.Helper()
			var digest string
			var length int
			require.NoError(t, other.QueryRowContext(ctx, "select length(body), md5(body) from "+table+" where id = 1").Scan(&length, &digest))
			require.Equal(t, 70000, length)
			return digest
		}

		type ownership struct {
			creator, owner uint32
			createdTime    string
			logicalID      uint64
		}
		readOwnership := func() ownership {
			t.Helper()
			query := fmt.Sprintf("select creator, owner, cast(created_time as char), rel_logical_id "+
				"from mo_catalog.mo_tables where reldatabase = '%s' and relname = 'target'", database)
			var result ownership
			require.NoError(t, other.QueryRowContext(ctx, query).Scan(&result.creator, &result.owner, &result.createdTime, &result.logicalID))
			return result
		}
		originalOwner := readOwnership()
		first := readIndex("ix_a")
		second := readIndex("ix_b")
		require.GreaterOrEqual(t, len(first), 2)
		require.Len(t, second, 1)
		originalHash := readBodyHash()
		execSQLRequire(t, ctx, db, "alter table "+table+
			" rename index IX_A to ix_temp, rename key ix_b to Ix_A, rename index ix_temp to Ix_B, algorithm=inplace")
		require.Equal(t, second, readIndex("Ix_A"))
		require.Equal(t, first, readIndex("Ix_B"))
		require.Empty(t, readIndex("ix_a"))
		require.Empty(t, readIndex("ix_temp"))
		require.Equal(t, originalHash, readBodyHash())
		require.Equal(t, originalOwner, readOwnership())
		require.Contains(t, readShowCreate(), "Ix_B")
		require.Contains(t, readShowCreate(), "Ix_A")

		// Exercise a failure after an earlier clause binds, then verify that
		// neither catalog nor engine constraint names changed.
		_, err = db.ExecContext(ctx, "alter table "+table+" rename index Ix_B to free_name, rename index missing to Ix_B")
		issue289RequireMySQLError(t, err, 1091)
		require.Equal(t, first, readIndex("Ix_B"))
		require.Empty(t, readIndex("free_name"))
		require.NotContains(t, readShowCreate(), "free_name")

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = tx.ExecContext(ctx, "alter table "+table+" rename index Ix_B to rollback_name")
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
		require.Equal(t, first, readIndex("Ix_B"))
		require.Empty(t, readIndex("rollback_name"))
		require.NotContains(t, readShowCreate(), "rollback_name")

		_, err = other.ExecContext(ctx, "insert into "+table+" values (2, 11, 20, 'duplicate')")
		issue289RequireMySQLError(t, err, 1062)

		publication := database + "_pub"
		defer execSQLMaybe(t, context.Background(), db, "drop publication if exists "+publication)
		for _, source := range []string{database, "*"} {
			execSQLRequire(t, ctx, db, "create publication "+publication+" database "+source+" account all")
			_, err = other.ExecContext(ctx, "alter table "+table+" rename index Ix_B to published_name")
			require.ErrorContains(t, err, "published source database")
			require.Equal(t, first, readIndex("Ix_B"))
			require.Empty(t, readIndex("published_name"))
			execSQLRequire(t, ctx, db, "drop publication "+publication)
		}

		checkRenamePublicationLock(t, ctx, cluster, db, other, table, publication)
		require.Equal(t, first, readIndex("Ix_B"))
		require.Empty(t, readIndex("published_name"))

		execSQLRequire(t, ctx, db, "create table "+database+".child (id int primary key, v int, "+
			"constraint fk foreign key(v) references "+table+"(w))")
		_, err = db.ExecContext(ctx, "alter table "+table+" rename index Ix_A to new_unique")
		issue289RequireMySQLError(t, err, 1553)
		execSQLRequire(t, ctx, db, "alter table "+table+" rename index spare to spare_renamed")
		require.NotEmpty(t, readIndex("spare_renamed"))
		execSQLRequire(t, ctx, db, "alter table "+table+" rename index Ix_B to allowed")
		require.Equal(t, first, readIndex("allowed"))
		require.Equal(t, second, readIndex("Ix_A"))

		// Dropping the referenced parent with checks disabled leaves a
		// declaration under the old name. The replacement key uses other
		// columns and must not inherit that declaration's dependency.
		execSQLRequire(t, ctx, db, "create table "+database+".composite_child (v int, w int, "+
			"foreign key(v,w) references "+table+"(id,w))")
		replacement := database + ".replacement"
		execSQLRequire(t, ctx, db, "create table "+replacement+
			" (id int primary key, v int, w int, unique key Ix_A(v), unique key spare_renamed(w,id))")
		execSQLRequire(t, ctx, db, "set foreign_key_checks=0")
		execSQLRequire(t, ctx, db, "drop table "+table)
		execSQLRequire(t, ctx, db, "rename table "+replacement+" to "+table)
		execSQLRequire(t, ctx, db, "alter table "+table+
			" rename index Ix_A to unrelated_name, rename index spare_renamed to reversed_name")
		require.NotEmpty(t, readIndex("unrelated_name"))
		require.NotEmpty(t, readIndex("reversed_name"))
	})
}

// Publication creation owns its own transaction. Hold its exact catalog INSERT
// in a test transaction to control when that writer commits.
func checkRenamePublicationLock(t *testing.T, ctx context.Context, cluster embed.Cluster, db, other *sql.DB, table, name string) {
	t.Helper()
	var tableID uint64
	require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables "+
		"where reldatabase = 'mo_catalog' and relname = 'mo_pubs'").Scan(&tableID))
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	value, ok := runtime.ServiceRuntime(cn.ServiceID()).GetGlobalVariables(runtime.InternalSQLExecutor)
	require.True(t, ok)
	exec := value.(executor.SQLExecutor)
	opts := executor.Options{}.WithAccountID(0)
	insertPublication := func(accountID int) string {
		return fmt.Sprintf("insert into mo_catalog.mo_pubs "+
			"(account_id, account_name, pub_name, database_name, database_id, all_table, table_list, account_list, created_time, owner, creator, comment) "+
			"values (%d, 'sys', %s, '*', 0, true, '*', '*', now(), 0, 0, '')", accountID, sqlquote.String(name))
	}
	deletePublication := "delete from mo_catalog.mo_pubs where pub_name = " + sqlquote.String(name)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, err := exec.Exec(cleanup, deletePublication, opts)
		require.NoError(t, err)
		res.Close()
	}()

	publisherCtx, cancelPublisher := context.WithCancel(ctx)
	ready, commit, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var publisherErr error
	go func() {
		defer close(done)
		publisherErr = exec.ExecTxn(publisherCtx, func(tx executor.TxnExecutor) error {
			res, err := tx.Exec(insertPublication(0), executor.StatementOption{})
			if err != nil {
				return err
			}
			res.Close()
			close(ready)
			select {
			case <-commit:
				return nil
			case <-publisherCtx.Done():
				return publisherCtx.Err()
			}
		}, opts)
	}()
	defer func() {
		cancelPublisher()
		<-done
	}()
	select {
	case <-ready:
	case <-done:
		require.NoError(t, publisherErr)
		t.Fatal("publication transaction ended before inserting the row")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	renameSQL := "alter table " + table + " rename index Ix_B to published_name"
	renameCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := other.ExecContext(renameCtx, renameSQL)
		result <- err
	}()
	services := issue27487LockServices(cluster)
	require.Eventually(t, func() bool {
		waiting := false
		for _, service := range services {
			service.IterLocks(func(id uint64, _ [][]byte, held lockservice.Lock) bool {
				if id == tableID {
					held.IterWaiters(func(lockpb.WaitTxn) bool {
						waiting = true
						return false
					})
				}
				return !waiting
			})
			if waiting {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "rename must wait on the concurrent publication row")
	close(commit)
	<-done
	require.NoError(t, publisherErr)
	select {
	case err := <-result:
		require.ErrorContains(t, err, "published source database")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	res, err := exec.Exec(ctx, deletePublication, opts)
	require.NoError(t, err)
	res.Close()

	rename, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer rename.Rollback()
	_, err = rename.ExecContext(ctx, renameSQL)
	require.NoError(t, err)
	// An independent account's publication is outside this held PK range.
	unrelatedCtx, cancelUnrelated := context.WithTimeout(ctx, 5*time.Second)
	defer cancelUnrelated()
	res, err = exec.Exec(unrelatedCtx, insertPublication(1), opts)
	require.NoError(t, err)
	res.Close()
	require.NoError(t, rename.Rollback())
}
