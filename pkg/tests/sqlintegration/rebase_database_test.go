// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

type rebaseSQLFixture struct {
	t      *testing.T
	ctx    context.Context
	db     *sql.DB
	prefix string
}

func (f rebaseSQLFixture) exec(query string) {
	f.t.Helper()
	_, err := f.db.ExecContext(f.ctx, query)
	require.NoError(f.t, err, query)
}

func (f rebaseSQLFixture) scalar(query string) int64 {
	f.t.Helper()
	var n int64
	require.NoError(f.t, f.db.QueryRowContext(f.ctx, query).Scan(&n), query)
	return n
}

func (f rebaseSQLFixture) snapshot(database, suffix string) string {
	f.t.Helper()
	name := f.prefix + suffix
	f.exec("create snapshot " + name + " for database " + database)
	return database + "{snapshot='" + name + "'}"
}

func (f rebaseSQLFixture) rows(database, table string) []string {
	f.t.Helper()
	rows, err := f.db.QueryContext(f.ctx, "select id,v from "+database+"."+table+" order by id")
	require.NoError(f.t, err)
	defer rows.Close()
	var result []string
	for rows.Next() {
		var id int
		var v sql.NullInt64
		require.NoError(f.t, rows.Scan(&id, &v))
		value := "NULL"
		if v.Valid {
			value = fmt.Sprint(v.Int64)
		}
		result = append(result, fmt.Sprintf("%d:%s", id, value))
	}
	require.NoError(f.t, rows.Err())
	return result
}

// Isolate the Merge boundary from native Rebase: an ordinary clone and its
// mutations are legal in one transaction and must not disappear from Merge.
func TestCloneCreationCommitMutationMerge(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn, "drop snapshot if exists nr_creation_base", "drop database if exists nr_creation_f", "drop database if exists nr_creation_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_creation_"}
		f.exec("create database nr_creation_u")
		f.exec("create table nr_creation_u.a(id int primary key,v int)")
		f.exec("insert into nr_creation_u.a values(1,10),(2,20)")
		f.exec("create table nr_creation_u.b(id int primary key,v int)")
		f.exec("insert into nr_creation_u.b values(1,10),(2,20)")
		f.exec("create snapshot nr_creation_base for database nr_creation_u")
		var before, after string
		require.NoError(t, db.QueryRowContext(ctx, "select watermark from change_watermark() w").Scan(&before))
		f.exec("begin")
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			_, rollbackErr := db.ExecContext(cleanupCtx, "rollback")
			require.NoError(t, rollbackErr)
		}()
		f.exec("data branch create database nr_creation_f from nr_creation_u{snapshot='nr_creation_base'}")
		f.exec("update nr_creation_f.a set v=11 where id=1")
		f.exec("delete from nr_creation_f.a where id=2")
		f.exec("insert into nr_creation_f.a values(3,30)")
		f.exec("commit")
		require.NoError(t, db.QueryRowContext(ctx, "select watermark from change_watermark() w").Scan(&after))
		for _, table := range []string{"a", "b"} {
			var changeCount int
			query := "select count(*) from table_changes('nr_creation_f','" + table + "',?,?) c"
			if err := db.QueryRowContext(ctx, query, before, after).Scan(&changeCount); err != nil {
				t.Logf("creation commit %s table_changes diagnostic unavailable: %v", table, err)
			} else {
				t.Logf("creation commit %s table_changes rows: %d", table, changeCount)
			}
		}
		want := []string{"1:11", "3:30"}
		require.Equal(t, want, f.rows("nr_creation_f", "a"))
		unchanged, err := db.QueryContext(ctx, "data branch diff nr_creation_f.b against nr_creation_u.b")
		require.NoError(t, err)
		defer unchanged.Close()
		require.False(t, unchanged.Next(), "a pure clone must not report inherited rows as changes")
		require.NoError(t, unchanged.Err())
		require.NoError(t, unchanged.Close())
		f.exec("update nr_creation_u.b set v=99 where id=1")
		f.exec("data branch merge nr_creation_f.b into nr_creation_u.b when conflict accept")
		require.Equal(t, int64(99), f.scalar("select v from nr_creation_u.b where id=1"), "an unchanged clone must not overwrite upstream changes")
		f.exec("data branch merge nr_creation_f.a into nr_creation_u.a when conflict accept")
		require.Equal(t, want, f.rows("nr_creation_u", "a"))
	})
}

// The parent can advance after the fork snapshot but before clone creation.
// That later parent state is not the child's before-state.
func TestCloneCreationCommitMergeParentAdvanced(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn,
			"drop snapshot if exists nr_advance_base", "drop database if exists nr_advance_child",
			"drop database if exists nr_advance_parent")
		f := rebaseSQLFixture{t, ctx, db, "nr_advance_"}
		f.exec("create database nr_advance_parent")
		f.exec("create table nr_advance_parent.t(id int primary key,v int)")
		f.exec("insert into nr_advance_parent.t values(1,10),(2,20)")
		base := f.snapshot("nr_advance_parent", "base")
		f.exec("update nr_advance_parent.t set v=11 where id=1")
		f.exec("begin")
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			_, rollbackErr := db.ExecContext(cleanupCtx, "rollback")
			require.NoError(t, rollbackErr)
		}()
		f.exec("data branch create database nr_advance_child from " + base)
		f.exec("update nr_advance_child.t set v=22 where id=2")
		f.exec("commit")
		require.Equal(t, []string{"1:10", "2:22"}, f.rows("nr_advance_child", "t"))
		f.exec("data branch merge nr_advance_child.t into nr_advance_parent.t when conflict accept")
		require.Equal(t, []string{"1:11", "2:22"}, f.rows("nr_advance_parent", "t"))
	})
}

// Creation-commit changes must participate in Merge conflict detection, not
// merely be copied as unconditional inserts or updates.
func TestCloneCreationCommitMergeConflictPolicies(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn,
			"drop snapshot if exists nr_merge_conflict_base", "drop database if exists nr_merge_conflict_child",
			"drop database if exists nr_merge_conflict_skip", "drop database if exists nr_merge_conflict_accept",
			"drop database if exists nr_merge_conflict_origin")
		f := rebaseSQLFixture{t, ctx, db, "nr_merge_conflict_"}
		f.exec("create database nr_merge_conflict_origin")
		f.exec("create table nr_merge_conflict_origin.t(id int primary key,v int)")
		f.exec("insert into nr_merge_conflict_origin.t values(1,10),(2,20)")
		base := f.snapshot("nr_merge_conflict_origin", "base")
		for _, target := range []string{"nr_merge_conflict_skip", "nr_merge_conflict_accept"} {
			f.exec("data branch create database " + target + " from " + base)
			f.exec("update " + target + ".t set v=11 where id=1")
		}
		f.exec("begin")
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			_, rollbackErr := db.ExecContext(cleanupCtx, "rollback")
			require.NoError(t, rollbackErr)
		}()
		f.exec("data branch create database nr_merge_conflict_child from " + base)
		f.exec("update nr_merge_conflict_child.t set v=12 where id=1")
		f.exec("update nr_merge_conflict_child.t set v=22 where id=2")
		f.exec("commit")
		f.exec("data branch merge nr_merge_conflict_child.t into nr_merge_conflict_skip.t when conflict skip")
		f.exec("data branch merge nr_merge_conflict_child.t into nr_merge_conflict_accept.t when conflict accept")
		require.Equal(t, []string{"1:11", "2:22"}, f.rows("nr_merge_conflict_skip", "t"))
		require.Equal(t, []string{"1:12", "2:22"}, f.rows("nr_merge_conflict_accept", "t"))
	})
}

// A creation-commit edit must survive more than one clone edge without
// reviving an intermediate insert that a later branch deleted.
func TestCloneCreationCommitMultiHopMerge(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn,
			"drop snapshot if exists nr_multihop_mid",
			"drop snapshot if exists nr_multihop_base",
			"drop database if exists nr_multihop_child",
			"drop database if exists nr_multihop_parent",
			"drop database if exists nr_multihop_upstream")
		f := rebaseSQLFixture{t, ctx, db, "nr_multihop_"}
		f.exec("create database nr_multihop_upstream")
		f.exec("create table nr_multihop_upstream.t(id int primary key,v int)")
		f.exec("insert into nr_multihop_upstream.t values(1,10),(2,20),(3,30),(4,40)")
		f.exec("create snapshot nr_multihop_base for database nr_multihop_upstream")
		f.exec("begin")
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			_, rollbackErr := db.ExecContext(cleanupCtx, "rollback")
			require.NoError(t, rollbackErr)
		}()
		f.exec("data branch create database nr_multihop_parent from nr_multihop_upstream{snapshot='nr_multihop_base'}")
		f.exec("update nr_multihop_parent.t set v=11 where id=1")
		f.exec("delete from nr_multihop_parent.t where id=2")
		f.exec("insert into nr_multihop_parent.t values(5,50)")
		f.exec("commit")
		f.exec("create snapshot nr_multihop_mid for database nr_multihop_parent")
		f.exec("begin")
		f.exec("data branch create database nr_multihop_child from nr_multihop_parent{snapshot='nr_multihop_mid'}")
		f.exec("update nr_multihop_child.t set v=12 where id=1")
		f.exec("update nr_multihop_child.t set v=33 where id=3")
		f.exec("delete from nr_multihop_child.t where id=5")
		f.exec("insert into nr_multihop_child.t values(6,60)")
		f.exec("commit")
		f.exec("update nr_multihop_upstream.t set v=44 where id=4")
		f.exec("data branch merge nr_multihop_child.t into nr_multihop_upstream.t when conflict accept")
		require.Equal(t, []string{"1:12", "3:33", "4:44", "6:60"}, f.rows("nr_multihop_upstream", "t"))
	})
}

func TestCloneCreationCommitPickBetween(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn,
			"drop snapshot if exists nr_pick_before", "drop snapshot if exists nr_pick_after",
			"drop snapshot if exists nr_pick_later", "drop snapshot if exists nr_pick_base",
			"drop database if exists nr_pick_child", "drop database if exists nr_pick_upstream")
		f := rebaseSQLFixture{t, ctx, db, "nr_pick_"}
		f.exec("create database nr_pick_upstream")
		f.exec("create table nr_pick_upstream.t(id int primary key,v int)")
		f.exec("insert into nr_pick_upstream.t values(1,10),(2,20)")
		f.exec("create snapshot nr_pick_base for database nr_pick_upstream")
		f.exec("create snapshot nr_pick_before for account sys")
		f.exec("begin")
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			_, rollbackErr := db.ExecContext(cleanupCtx, "rollback")
			require.NoError(t, rollbackErr)
		}()
		f.exec("data branch create database nr_pick_child from nr_pick_upstream{snapshot='nr_pick_base'}")
		f.exec("update nr_pick_child.t set v=11 where id=1")
		f.exec("delete from nr_pick_child.t where id=2")
		f.exec("insert into nr_pick_child.t values(3,30)")
		f.exec("commit")
		f.exec("create snapshot nr_pick_after for account sys")
		f.exec("update nr_pick_child.t set v=12 where id=1")
		f.exec("insert into nr_pick_child.t values(4,40)")
		f.exec("create snapshot nr_pick_later for account sys")
		f.exec("data branch pick nr_pick_child.t into nr_pick_upstream.t between snapshot nr_pick_before and nr_pick_after when conflict accept")
		require.Equal(t, []string{"1:11", "3:30"}, f.rows("nr_pick_upstream", "t"))
		f.exec("data branch pick nr_pick_child.t into nr_pick_upstream.t between snapshot nr_pick_after and nr_pick_later when conflict accept")
		require.Equal(t, []string{"1:12", "3:30", "4:40"}, f.rows("nr_pick_upstream", "t"))
	})
}
