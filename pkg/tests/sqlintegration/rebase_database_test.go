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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
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

func (f rebaseSQLFixture) rebase(target, feature, upstream, featureBase, upstreamBase, policy string) (int64, [6]uint64, error) {
	return f.rebaseTables(target, feature, upstream, featureBase, upstreamBase, policy, 3)
}

func (f rebaseSQLFixture) rebaseTables(target, feature, upstream, featureBase, upstreamBase, policy string, expectedTables uint64) (int64, [6]uint64, error) {
	f.t.Helper()
	query := fmt.Sprintf("data branch rebase database %s from %s against %s between %s and %s %s", target, feature, upstream, featureBase, upstreamBase, policy)
	var database, base string
	var id, tables uint64
	var ts int64
	var counts [6]uint64
	err := f.db.QueryRowContext(f.ctx, query).Scan(&database, &id, &base, &ts, &tables,
		&counts[0], &counts[1], &counts[2], &counts[3], &counts[4], &counts[5])
	if err == nil {
		require.Equal(f.t, target, database)
		require.Positive(f.t, id)
		require.Positive(f.t, ts)
		require.Equal(f.t, expectedTables, tables)
	}
	return ts, counts, err
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

// These tests invoke the new coordinator through the real SQL protocol. The
// expected rows/counts are handwritten, not produced by the lowering helper.
func TestNativeRebaseDatabase(t *testing.T) {
	runNativeRebaseDatabaseCases(t, []string{"repeat"})
}

func TestNativeRebasePoliciesAndAdmission(t *testing.T) {
	runNativeRebaseDatabaseCases(t, []string{"fail", "skip", "accept", "changed_bag", "wrong_scope"})
}

func TestNativeRebaseFloatValueIdentity(t *testing.T) {
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
			"drop snapshot if exists nr_float_bu", "drop snapshot if exists nr_float_bf",
			"drop snapshot if exists nr_float_fh", "drop snapshot if exists nr_float_fh_bag",
			"drop snapshot if exists nr_float_uh", "drop database if exists nr_float_result",
			"drop database if exists nr_float_bag_result", "drop database if exists nr_float_f",
			"drop database if exists nr_float_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_float_"}
		f.exec("create database nr_float_u")
		f.exec("create table nr_float_u.t(id int primary key,v double)")
		f.exec("create table nr_float_u.bag(v double)")
		f.exec("insert into nr_float_u.t values(1,cast('0' as double)),(2,null),(3,2.5)")
		f.exec("insert into nr_float_u.bag values(cast('0' as double))")
		bu := f.snapshot("nr_float_u", "bu")
		f.exec("data branch create database nr_float_f from " + bu)
		bf := f.snapshot("nr_float_f", "bf")
		f.exec("update nr_float_f.t set v=cast('-0' as double) where id=1")
		f.exec("update nr_float_f.t set v=-1 where id=2")
		f.exec("update nr_float_f.t set v=null where id=3")
		// Prove the fixture is a physical change which ordinary SQL equality
		// alone cannot distinguish before using it as the replay oracle.
		require.EqualValues(t, 1, f.scalar("select v <=> cast('0' as double) from nr_float_f.t where id=1"))
		require.EqualValues(t, 1, f.scalar("select hex(serial(v)) <> hex(serial(cast('0' as double))) from nr_float_f.t where id=1"))
		fh, uh := f.snapshot("nr_float_f", "fh"), f.snapshot("nr_float_u", "uh")
		_, counts, err := f.rebaseTables("nr_float_result", fh, uh, bf, bu, "", 2)
		require.NoError(t, err)
		require.EqualValues(t, 3, counts[0])
		require.EqualValues(t, 1, f.scalar("select hex(serial(v)) = (select hex(serial(v)) from nr_float_f.t where id=1) from nr_float_result.t where id=1"))
		require.EqualValues(t, 1, f.scalar("select v=-1 from nr_float_result.t where id=2"))
		require.EqualValues(t, 1, f.scalar("select v is null from nr_float_result.t where id=3"))

		f.exec("update nr_float_f.bag set v=cast('-0' as double)")
		fhBag := f.snapshot("nr_float_f", "fh_bag")
		_, _, err = f.rebaseTables("nr_float_bag_result", fhBag, uh, bf, bu, "", 2)
		require.ErrorContains(t, err, "Rebase requires matching bases and rejects changed no-PK tables")
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_float_bag_result'"))
	})
}

func TestNativeRebaseCollatedValueIdentity(t *testing.T) {
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
			"drop snapshot if exists nr_collate_bu", "drop snapshot if exists nr_collate_bf",
			"drop snapshot if exists nr_collate_fh", "drop snapshot if exists nr_collate_fh_bag",
			"drop snapshot if exists nr_collate_uh", "drop database if exists nr_collate_result",
			"drop database if exists nr_collate_bag_result", "drop database if exists nr_collate_f",
			"drop database if exists nr_collate_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_collate_"}
		f.exec("create database nr_collate_u")
		f.exec("create table nr_collate_u.t(id int primary key,v varchar(20) collate utf8mb4_unicode_ci)")
		f.exec("create table nr_collate_u.bag(v varchar(20) collate utf8mb4_unicode_ci)")
		f.exec("insert into nr_collate_u.t values(1,'a')")
		f.exec("insert into nr_collate_u.bag values('a')")
		bu := f.snapshot("nr_collate_u", "bu")
		f.exec("data branch create database nr_collate_f from " + bu)
		bf := f.snapshot("nr_collate_f", "bf")
		f.exec("update nr_collate_f.t set v='A' where id=1")
		// SQL equality follows the collation, while serial retains value bytes.
		require.EqualValues(t, 1, f.scalar("select v='a' from nr_collate_f.t where id=1"))
		require.EqualValues(t, 1, f.scalar("select hex(serial(v)) <> hex(serial('a')) from nr_collate_f.t where id=1"))
		fh, uh := f.snapshot("nr_collate_f", "fh"), f.snapshot("nr_collate_u", "uh")
		_, counts, err := f.rebaseTables("nr_collate_result", fh, uh, bf, bu, "", 2)
		require.NoError(t, err)
		require.EqualValues(t, 1, counts[0])
		require.EqualValues(t, 1, f.scalar("select hex(serial(v)) = hex(serial('A')) from nr_collate_result.t where id=1"))

		f.exec("update nr_collate_f.bag set v='A'")
		fhBag := f.snapshot("nr_collate_f", "fh_bag")
		_, _, err = f.rebaseTables("nr_collate_bag_result", fhBag, uh, bf, bu, "", 2)
		require.ErrorContains(t, err, "Rebase requires matching bases and rejects changed no-PK tables")
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_collate_bag_result'"))
	})
}

func TestNativeRebaseConflictingRowStates(t *testing.T) {
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
			"drop snapshot if exists nr_row_bu", "drop snapshot if exists nr_row_bf",
			"drop snapshot if exists nr_row_fh", "drop snapshot if exists nr_row_uh",
			"drop database if exists nr_row_fail", "drop database if exists nr_row_skip",
			"drop database if exists nr_row_accept", "drop database if exists nr_row_f",
			"drop database if exists nr_row_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_row_"}
		f.exec("create database nr_row_u")
		f.exec("create table nr_row_u.t(id int primary key,v int)")
		f.exec("insert into nr_row_u.t values(1,10),(2,20),(3,30),(4,40),(5,50),(6,60)")
		bu := f.snapshot("nr_row_u", "bu")
		f.exec("data branch create database nr_row_f from " + bu)
		bf := f.snapshot("nr_row_f", "bf")
		f.exec("update nr_row_f.t set v=11 where id=1")
		f.exec("delete from nr_row_f.t where id in (2,4)")
		f.exec("update nr_row_f.t set v=31 where id=3")
		f.exec("update nr_row_f.t set v=51 where id=5")
		f.exec("insert into nr_row_f.t values(7,70),(8,80)")
		f.exec("delete from nr_row_u.t where id in (1,4)")
		f.exec("update nr_row_u.t set v=21 where id=2")
		f.exec("update nr_row_u.t set v=32 where id=3")
		f.exec("update nr_row_u.t set v=51 where id=5")
		f.exec("insert into nr_row_u.t values(7,71),(8,80)")
		fh, uh := f.snapshot("nr_row_f", "fh"), f.snapshot("nr_row_u", "uh")
		pins := f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_row_u'")
		_, _, err = f.rebaseTables("nr_row_fail", fh, uh, bf, bu, "", 1)
		require.ErrorContains(t, err, "Rebase conflict in table t")
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_row_fail'"))
		require.Equal(t, pins, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_row_u'"))

		_, counts, err := f.rebaseTables("nr_row_skip", fh, uh, bf, bu, "when conflict skip", 1)
		require.NoError(t, err)
		require.Equal(t, [6]uint64{7, 7, 4, 0, 0, 0}, counts)
		require.Equal(t, []string{"2:21", "3:32", "5:51", "6:60", "7:71", "8:80"}, f.rows("nr_row_skip", "t"))

		_, counts, err = f.rebaseTables("nr_row_accept", fh, uh, bf, bu, "when conflict accept", 1)
		require.NoError(t, err)
		require.Equal(t, [6]uint64{7, 7, 4, 1, 2, 1}, counts)
		require.Equal(t, []string{"1:11", "3:31", "5:51", "6:60", "7:70", "8:80"}, f.rows("nr_row_accept", "t"))
		require.Equal(t, []string{"1:11", "3:31", "5:51", "6:60", "7:70", "8:80"}, f.rows("nr_row_f", "t"))
		require.Equal(t, []string{"2:21", "3:32", "5:51", "6:60", "7:71", "8:80"}, f.rows("nr_row_u", "t"))
	})
}

func TestNativeRebaseRejectsSecondaryIndexBeforeWrites(t *testing.T) {
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
			"drop snapshot if exists nr_index_bu", "drop snapshot if exists nr_index_bf",
			"drop snapshot if exists nr_index_fh", "drop snapshot if exists nr_index_uh",
			"drop database if exists nr_index_result", "drop database if exists nr_index_f",
			"drop database if exists nr_index_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_index_"}
		f.exec("create database nr_index_u")
		f.exec("create table nr_index_u.t(id int primary key,v int, key ix_v(v))")
		f.exec("insert into nr_index_u.t values(1,10)")
		bu := f.snapshot("nr_index_u", "bu")
		f.exec("data branch create database nr_index_f from " + bu)
		bf := f.snapshot("nr_index_f", "bf")
		f.exec("update nr_index_f.t set v=11 where id=1")
		fh, uh := f.snapshot("nr_index_f", "fh"), f.snapshot("nr_index_u", "uh")
		pins := f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_index_u'")
		_, _, err = f.rebaseTables("nr_index_result", fh, uh, bf, bu, "", 1)
		require.ErrorContains(t, err, "Rebase does not support non-PK indexes")
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_index_result'"))
		require.Equal(t, pins, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_index_u'"))
		require.Equal(t, []string{"1:11"}, f.rows("nr_index_f", "t"))
		require.Equal(t, []string{"1:10"}, f.rows("nr_index_u", "t"))
	})
}

// The adapter writes a fork row before taking the branch's own base snapshot.
// Even without its secondary index, in-database history can therefore make the
// feature and upstream bases logically different to the generic R1 executor.
func TestNativeRebaseRejectsDivergentBranchHistoryBeforeWrites(t *testing.T) {
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
			"drop snapshot if exists nr_history_bu", "drop snapshot if exists nr_history_bf",
			"drop snapshot if exists nr_history_fh", "drop snapshot if exists nr_history_uh",
			"drop database if exists nr_history_result", "drop database if exists nr_history_f",
			"drop database if exists nr_history_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_history_"}
		f.exec("create database nr_history_u")
		f.exec("create table nr_history_u.t(id int primary key,v int)")
		f.exec("create table nr_history_u.history(seq bigint primary key,kind varchar(8))")
		f.exec("insert into nr_history_u.t values(1,10)")
		f.exec("insert into nr_history_u.history values(1,'root')")
		bu := f.snapshot("nr_history_u", "bu")
		f.exec("data branch create database nr_history_f from " + bu)
		f.exec("insert into nr_history_f.history values(2,'fork')")
		bf := f.snapshot("nr_history_f", "bf")
		f.exec("update nr_history_f.t set v=11 where id=1")
		fh, uh := f.snapshot("nr_history_f", "fh"), f.snapshot("nr_history_u", "uh")
		pins := f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_history_u'")
		_, _, err = f.rebaseTables("nr_history_result", fh, uh, bf, bu, "", 2)
		require.ErrorContains(t, err, "Rebase requires matching bases")
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_history_result'"))
		require.Equal(t, pins, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_history_u'"))
		require.Equal(t, int64(2), f.scalar("select count(*) from nr_history_f.history"))
		require.Equal(t, int64(1), f.scalar("select count(*) from nr_history_u.history"))
		require.Equal(t, []string{"1:11"}, f.rows("nr_history_f", "t"))
		require.Equal(t, []string{"1:10"}, f.rows("nr_history_u", "t"))
	})
}

func TestNativeRebaseSelectedTables(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn,
			"drop snapshot if exists nr_select_bu", "drop snapshot if exists nr_select_bf",
			"drop snapshot if exists nr_select_fh", "drop snapshot if exists nr_select_uh",
			"drop snapshot if exists nr_select_bad", "drop snapshot if exists nr_select_baduh",
			"drop snapshot if exists nr_select_f2", "drop snapshot if exists nr_select_u2",
			"drop database if exists nr_select_result", "drop database if exists nr_select_unknown",
			"drop database if exists nr_select_second",
			"drop database if exists nr_select_duplicate", "drop database if exists nr_select_index",
			"drop database if exists nr_select_nopk", "drop database if exists nr_select_schema",
			"drop database if exists nr_select_f", "drop database if exists nr_select_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_select_"}
		f.exec("create database nr_select_u")
		f.exec("create table nr_select_u.t(id int primary key,v int)")
		f.exec("create table nr_select_u.history(seq bigint primary key,kind varchar(8),key ix_kind(kind))")
		f.exec("create table nr_select_u.archive(v int)")
		f.exec("insert into nr_select_u.t values(1,10),(2,20)")
		f.exec("insert into nr_select_u.history values(1,'root')")
		f.exec("insert into nr_select_u.archive values(1),(1),(NULL)")
		bu := f.snapshot("nr_select_u", "bu")
		f.exec("data branch create database nr_select_f from " + bu)
		f.exec("insert into nr_select_f.history values(2,'fork')")
		bf := f.snapshot("nr_select_f", "bf")
		f.exec("update nr_select_f.t set v=11 where id=1")
		f.exec("update nr_select_u.t set v=21 where id=2")
		f.exec("insert into nr_select_u.history values(3,'up')")
		fh, uh := f.snapshot("nr_select_f", "fh"), f.snapshot("nr_select_u", "uh")
		ts, counts, err := f.rebaseTables("nr_select_result", fh, uh, bf, bu, "tables (t)", 3)
		require.NoError(t, err)
		require.Equal(t, [6]uint64{1, 1, 0, 0, 1, 0}, counts)
		require.Equal(t, []string{"1:11", "2:21"}, f.rows("nr_select_result", "t"))
		require.Equal(t, int64(1), f.scalar("select count(*) from nr_select_result.history force index (ix_kind) where kind='root'"))
		require.Equal(t, int64(1), f.scalar("select count(*) from nr_select_result.history force index (ix_kind) where kind='up'"))
		require.Equal(t, int64(1), f.scalar("select count(*) from nr_select_result.history ignore index (ix_kind) where kind='up'"))
		func() {
			rows, err := db.QueryContext(ctx, "explain select seq from nr_select_result.history force index (ix_kind) where kind='up'")
			require.NoError(t, err)
			defer rows.Close()
			var plan []string
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				plan = append(plan, line)
			}
			require.NoError(t, rows.Err())
			require.Regexp(t, `(?i)Index Table Scan[^\n]*ix_kind`, strings.Join(plan, "\n"))
		}()
		require.Equal(t, int64(0), f.scalar("select count(*) from nr_select_result.history ignore index (ix_kind) where kind='fork'"))
		require.Equal(t, int64(3), f.scalar("select count(*) from nr_select_result.archive"))
		for _, table := range []string{"t", "history", "archive"} {
			pin := fmt.Sprintf("select count(*) from mo_catalog.mo_branch_metadata m join mo_catalog.mo_tables t on m.table_id=t.rel_id join mo_catalog.mo_snapshots s on s.sname=concat('__mo_branch_',cast(m.table_id as char)) where t.reldatabase='nr_select_result' and t.relname='%s' and s.kind='branch' and s.obj_id=m.p_table_id and s.ts=m.clone_ts", table)
			require.Equal(t, int64(1), f.scalar(pin))
		}
		pins := f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_select_u'")
		for _, tc := range []struct{ target, clause, message string }{
			{"nr_select_unknown", "tables (missing)", "does not exist"},
			{"nr_select_duplicate", "tables (t,t)", "duplicate Rebase table selection"},
			{"nr_select_index", "tables (history)", "non-PK indexes"},
		} {
			_, _, err := f.rebaseTables(tc.target, fh, uh, bf, bu, tc.clause, 3)
			require.ErrorContains(t, err, tc.message)
			require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='"+tc.target+"'"))
			require.Equal(t, pins, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_select_u'"))
		}
		f.exec("insert into nr_select_f.archive values(2)")
		bad := f.snapshot("nr_select_f", "bad")
		_, _, err = f.rebaseTables("nr_select_nopk", bad, uh, bf, bu, "tables (t)", 3)
		require.ErrorContains(t, err, "rejects changed no-PK tables")
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_select_nopk'"))
		f.exec("drop snapshot nr_select_uh")
		f.exec("update nr_select_result.t set v=12 where id=1")
		f.exec("update nr_select_u.t set v=22 where id=2")
		f2, u2 := f.snapshot("nr_select_result", "f2"), f.snapshot("nr_select_u", "u2")
		base := fmt.Sprintf("nr_select_u{MO_TS=%d}", ts)
		_, counts, err = f.rebaseTables("nr_select_second", f2, u2, base, base, "tables (t)", 3)
		require.NoError(t, err, "the selected-table result must retain its new base")
		require.Equal(t, [6]uint64{1, 1, 0, 0, 1, 0}, counts)
		require.Equal(t, []string{"1:12", "2:22"}, f.rows("nr_select_second", "t"))
		require.Equal(t, int64(1), f.scalar("select count(*) from nr_select_second.history force index (ix_kind) where kind='up'"))
		require.Equal(t, int64(3), f.scalar("select count(*) from nr_select_second.archive"))
		f.exec("alter table nr_select_u.history add column extra int")
		badUH := f.snapshot("nr_select_u", "baduh")
		pinsAfterAlter := f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_select_u'")
		_, _, err = f.rebaseTables("nr_select_schema", fh, badUH, bf, bu, "tables (t)", 3)
		require.ErrorContains(t, err, "schemas differ")
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_select_schema'"))
		require.Equal(t, pinsAfterAlter, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_select_u'"))
	})
}

func TestNativeRebaseMismatchedBases(t *testing.T) {
	runNativeRebaseDatabaseCases(t, []string{"different_base"})
}

func TestNativeRebaseCrossGenerationDiff(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn,
			"drop snapshot if exists nr_cgen_head", "drop snapshot if exists nr_cgen_base",
			"drop database if exists nr_cgen_next", "drop database if exists nr_cgen_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_cgen_"}
		f.exec("create database nr_cgen_u")
		f.exec("create table nr_cgen_u.t(id int primary key,v int)")
		f.exec("insert into nr_cgen_u.t values(1,10)")
		base := f.snapshot("nr_cgen_u", "base")
		f.exec("data branch create database nr_cgen_next from " + base)
		f.exec("insert into nr_cgen_next.t values(2,20)")
		head := f.snapshot("nr_cgen_next", "head")
		rows, err := db.QueryContext(ctx, "data branch diff "+strings.Replace(head, "{", ".t{", 1)+" against "+strings.Replace(base, "{", ".t{", 1))
		require.NoError(t, err)
		defer rows.Close()
		require.True(t, rows.Next())
		var source, flag string
		var id, value int
		require.NoError(t, rows.Scan(&source, &flag, &id, &value))
		require.Equal(t, "INSERT", flag)
		require.Equal(t, 2, id)
		require.Equal(t, 20, value)
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
	})
}

func TestNativeRebaseRejectsEmptyBase(t *testing.T) {
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
			"drop snapshot if exists nr_empty_bu", "drop snapshot if exists nr_empty_bf",
			"drop snapshot if exists nr_empty_fh", "drop snapshot if exists nr_empty_uh",
			"drop database if exists nr_empty_result", "drop database if exists nr_empty_f",
			"drop database if exists nr_empty_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_empty_"}
		f.exec("create database nr_empty_u")
		bu := f.snapshot("nr_empty_u", "bu")
		f.exec("data branch create database nr_empty_f from " + bu)
		bf := f.snapshot("nr_empty_f", "bf")
		fh, uh := f.snapshot("nr_empty_f", "fh"), f.snapshot("nr_empty_u", "uh")
		pins := f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_empty_u'")
		_, _, err = f.rebaseTables("nr_empty_result", fh, uh, bf, bu, "", 0)
		require.ErrorContains(t, err, "Rebase requires at least one table to retain its base")
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_empty_result'"))
		require.Equal(t, pins, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_empty_u'"))
	})
}

func TestNativeRebaseRejectsFloatingPointKey(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn,
			"drop snapshot if exists nr_float_bu", "drop snapshot if exists nr_float_bf",
			"drop snapshot if exists nr_float_fh", "drop snapshot if exists nr_float_uh",
			"drop database if exists nr_float_result", "drop database if exists nr_float_f",
			"drop database if exists nr_float_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_float_"}
		f.exec("create database nr_float_u")
		f.exec("create table nr_float_u.t(id double primary key,v int)")
		f.exec("insert into nr_float_u.t values(1.5,10)")
		bu := f.snapshot("nr_float_u", "bu")
		f.exec("data branch create database nr_float_f from " + bu)
		bf := f.snapshot("nr_float_f", "bf")
		f.exec("update nr_float_f.t set v=11 where id=1.5")
		fh, uh := f.snapshot("nr_float_f", "fh"), f.snapshot("nr_float_u", "uh")
		pins := f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch'")
		edges := f.scalar("select count(*) from mo_catalog.mo_branch_metadata")
		_, _, err = f.rebaseTables("nr_float_result", fh, uh, bf, bu, "", 0)
		require.ErrorContains(t, err, "floating-point primary keys")
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_float_result'"))
		require.Equal(t, pins, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch'"))
		require.Equal(t, edges, f.scalar("select count(*) from mo_catalog.mo_branch_metadata"))
		require.Equal(t, int64(11), f.scalar("select v from nr_float_f.t where id=1.5"))
		require.Equal(t, int64(10), f.scalar("select v from nr_float_u.t where id=1.5"))
	})
}

func TestNativeRebaseRuleCursorSQLMode(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn, "drop database if exists nr_cursor")
		f := rebaseSQLFixture{t, ctx, db, "nr_cursor_"}
		f.exec("create database nr_cursor")
		f.exec("create table nr_cursor.rules(role_id int, rule_name varchar(200), primary key(role_id,rule_name))")
		// a\\y, a\\z and a]a in byte order, without SQL string escaping.
		f.exec("insert into nr_cursor.rules values (1,cast(unhex('615c79') as char)),(1,cast(unhex('615c7a') as char)),(1,cast(unhex('615d61') as char))")
		for _, mode := range []string{"", "NO_BACKSLASH_ESCAPES"} {
			f.exec("set session sql_mode = '" + mode + "'")
			require.Equal(t, int64(1), f.scalar("select count(*) from nr_cursor.rules where role_id=1 and rule_name=cast(unhex('615c79') as char)"))
			require.Equal(t, int64(2), f.scalar("select count(*) from nr_cursor.rules where role_id=1 and rule_name>cast(unhex('615c79') as char)"))
		}
	})
}

func TestNativeRebaseSQLMode(t *testing.T) {
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
			"drop snapshot if exists nr_mode_bu", "drop snapshot if exists nr_mode_bf",
			"drop snapshot if exists nr_mode_fh", "drop snapshot if exists nr_mode_uh",
			"drop database if exists nr_mode_result0", "drop database if exists nr_mode_result1",
			"drop database if exists nr_mode_f", "drop database if exists nr_mode_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_mode_"}
		f.exec("create database nr_mode_u")
		f.exec("create table nr_mode_u.t(id int primary key,v int)")
		f.exec("insert into nr_mode_u.t values(1,10)")
		bu := f.snapshot("nr_mode_u", "bu")
		f.exec("data branch create database nr_mode_f from " + bu)
		bf := f.snapshot("nr_mode_f", "bf")
		f.exec("update nr_mode_f.t set v=11 where id=1")
		fh, uh := f.snapshot("nr_mode_f", "fh"), f.snapshot("nr_mode_u", "uh")
		for i, mode := range []string{"", "NO_BACKSLASH_ESCAPES"} {
			f.exec("set session sql_mode = '" + mode + "'")
			target := fmt.Sprintf("nr_mode_result%d", i)
			_, _, err = f.rebaseTables(target, fh, uh, bf, bu, "", 1)
			require.NoError(t, err, mode)
			require.Equal(t, int64(11), f.scalar("select v from "+target+".t where id=1"))
		}
	})
}

// Unsupported caller ownership modes must not commit unrelated writes or leave
// a destination behind. The supported pessimistic RC path is tested separately.
func TestNativeRebaseRejectsCallerTransaction(t *testing.T) {
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
			"drop snapshot if exists nr_txn_bu", "drop snapshot if exists nr_txn_bf",
			"drop snapshot if exists nr_txn_fh", "drop snapshot if exists nr_txn_uh",
			"drop database if exists nr_txn_result", "drop database if exists nr_txn_f",
			"drop database if exists nr_txn_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_txn_"}
		f.exec("create database nr_txn_u")
		f.exec("create table nr_txn_u.t(id int primary key,v int)")
		f.exec("insert into nr_txn_u.t values(1,10)")
		bu := f.snapshot("nr_txn_u", "bu")
		f.exec("data branch create database nr_txn_f from " + bu)
		bf := f.snapshot("nr_txn_f", "bf")
		fh, uh := f.snapshot("nr_txn_f", "fh"), f.snapshot("nr_txn_u", "uh")
		pins := f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_txn_u'")
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer func() { _, _ = conn.ExecContext(context.Background(), "rollback") }()
		_, err = conn.ExecContext(ctx, "set session transaction isolation level repeatable read")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "begin")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "update nr_txn_u.t set v=99 where id=1")
		require.NoError(t, err)
		var value int64
		require.NoError(t, conn.QueryRowContext(ctx, "select v from nr_txn_u.t where id=1").Scan(&value))
		require.Equal(t, int64(99), value, "caller must observe its uncommitted update")
		query := fmt.Sprintf("data branch rebase database nr_txn_result from %s against %s between %s and %s", fh, uh, bf, bu)
		_, err = conn.ExecContext(ctx, query)
		require.ErrorContains(t, err, "pessimistic RC")
		_, err = conn.ExecContext(ctx, "rollback")
		require.NoError(t, err)
		var destinations, remainingPins int64
		require.NoError(t, conn.QueryRowContext(ctx, "select v from nr_txn_u.t where id=1").Scan(&value))
		require.Equal(t, int64(10), value, "rejected Rebase must not commit the caller's update")
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_txn_result'").Scan(&destinations))
		require.Zero(t, destinations)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_txn_u'").Scan(&remainingPins))
		require.Equal(t, pins, remainingPins)

		_, err = conn.ExecContext(ctx, "set autocommit=0")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "update nr_txn_u.t set v=98 where id=1")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, query)
		require.ErrorContains(t, err, "Rebase requires an independent transaction or explicit BEGIN")
		_, err = conn.ExecContext(ctx, "rollback")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "set autocommit=1")
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx, "select v from nr_txn_u.t where id=1").Scan(&value))
		require.Equal(t, int64(10), value)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_txn_result'").Scan(&destinations))
		require.Zero(t, destinations)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_txn_u'").Scan(&remainingPins))
		require.Equal(t, pins, remainingPins)
	})
}

func TestNativeRebasePrivileges(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
		defer cancel()
		port := cn.GetServiceConfig().CN.Frontend.Port
		admin, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer admin.Close()
		admin.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn,
			"drop user if exists nr_priv_read_user", "drop user if exists nr_priv_create_user",
			"drop role if exists nr_priv_read_role", "drop role if exists nr_priv_create_role",
			"drop snapshot if exists nr_priv_bu", "drop snapshot if exists nr_priv_bf",
			"drop snapshot if exists nr_priv_fh", "drop snapshot if exists nr_priv_uh",
			"drop database if exists nr_priv_result", "drop database if exists nr_priv_no_create",
			"drop database if exists nr_priv_no_read", "drop database if exists nr_priv_filtered", "drop database if exists nr_priv_other_role", "drop database if exists nr_priv_session", "drop database if exists nr_priv_noncanonical",
			"drop database if exists nr_priv_f", "drop database if exists nr_priv_u", "drop database if exists nr_priv_other", "drop database if exists nr_priv_aux")
		f := rebaseSQLFixture{t, ctx, admin, "nr_priv_"}
		f.exec("create database nr_priv_u")
		f.exec("create table nr_priv_u.t(id int primary key,v int)")
		f.exec("insert into nr_priv_u.t values(1,10),(2,20)")
		bu := f.snapshot("nr_priv_u", "bu")
		f.exec("data branch create database nr_priv_f from " + bu)
		bf := f.snapshot("nr_priv_f", "bf")
		f.exec("update nr_priv_f.t set v=11 where id=1")
		fh, uh := f.snapshot("nr_priv_f", "fh"), f.snapshot("nr_priv_u", "uh")
		for _, role := range []string{"nr_priv_read_role", "nr_priv_create_role"} {
			f.exec("create role " + role)
			f.exec("grant connect on account * to " + role)
			f.exec("grant select on table nr_priv_u.t to " + role)
		}
		f.exec("grant select on table nr_priv_f.t to nr_priv_read_role")
		f.exec("grant create database on account * to nr_priv_create_role")
		f.exec("create user nr_priv_read_user identified by '111' default role nr_priv_read_role")
		f.exec("create user nr_priv_create_user identified by '111' default role nr_priv_create_role")
		openUser := func(user, role string) *sql.DB {
			t.Helper()
			conn, err := sql.Open("mysql", fmt.Sprintf("sys#%s#%s:111@tcp(127.0.0.1:%d)/", user, role, port))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			require.NoError(t, conn.PingContext(ctx))
			return conn
		}
		reader := openUser("nr_priv_read_user", "nr_priv_read_role")
		creator := openUser("nr_priv_create_user", "nr_priv_create_role")
		var value int
		require.NoError(t, reader.QueryRowContext(ctx, "select v from nr_priv_f.t where id=1").Scan(&value))
		require.Equal(t, 11, value)
		require.Error(t, creator.QueryRowContext(ctx, "select v from nr_priv_f.t where id=1").Scan(&value))
		pinsBefore := f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_priv_u'")
		query := func(target string) string {
			return fmt.Sprintf("data branch rebase database %s from %s against %s between %s and %s", target, fh, uh, bf, bu)
		}
		for _, attempt := range []struct {
			db     *sql.DB
			target string
		}{{reader, "nr_priv_no_create"}, {creator, "nr_priv_no_read"}} {
			_, queryErr, closeErr := readRebaseQuery(ctx, attempt.db, query(attempt.target))
			require.NoError(t, closeErr)
			require.Error(t, queryErr)
			require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='"+attempt.target+"'"))
			require.Equal(t, pinsBefore, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_priv_u'"))
		}
		f.exec("grant select on table nr_priv_f.t to nr_priv_create_role")
		f.exec("create database nr_priv_other")
		f.exec("create table nr_priv_other.t(id int primary key)")
		f.exec("alter role nr_priv_create_role add rule \"select * from nr_priv_other.t where id=1\" on table nr_priv_other.t")
		authorized := openUser("nr_priv_create_user", "nr_priv_create_role")
		userFixture := rebaseSQLFixture{t, ctx, authorized, "nr_priv_"}
		_, _, err = userFixture.rebaseTables("nr_priv_result", fh, uh, bf, bu, "", 1)
		require.NoError(t, err)
		require.Equal(t, int64(11), f.scalar("select v from nr_priv_result.t where id=1"))
		require.Equal(t, int64(2), f.scalar("select count(*) from nr_priv_result.t"))
		pinsBefore = f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_priv_u'")
		f.exec("alter role nr_priv_read_role add rule \"select * from nr_priv_f.t where id=1\" on table nr_priv_f.t")
		_, err, closeErr := readRebaseQuery(ctx, authorized, query("nr_priv_other_role"))
		require.NoError(t, closeErr)
		require.ErrorContains(t, err, "Rebase does not support rewrite rules on source tables")
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_priv_other_role'"))
		require.Equal(t, pinsBefore, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_priv_u'"))
		f.exec("alter role nr_priv_read_role drop rule on table nr_priv_f.t")
		session, err := authorized.Conn(ctx)
		require.NoError(t, err)
		defer session.Close()
		_, err = session.ExecContext(ctx, "set enable_remap_hint = 1")
		require.NoError(t, err)
		_, err = session.ExecContext(ctx, `set remap_rewrites = '{"remapdb":{"nr_priv_u":"nr_priv_other"}}'`)
		require.NoError(t, err)
		_, err, closeErr = readRebaseQuery(ctx, session, query("nr_priv_session"))
		require.NoError(t, closeErr)
		require.ErrorContains(t, err, "Rebase does not support active session rewrites")
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_priv_session'"))
		require.Equal(t, pinsBefore, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_priv_u'"))
		// Insert a page of valid legacy spellings under an unrelated role. The
		// direct catalog fixture models old rows without 128 independent DDLs.
		f.exec("create database nr_priv_aux")
		f.exec("create table nr_priv_aux.t(id int primary key)")
		readRoleID := f.scalar("select role_id from mo_catalog.mo_role where role_name='nr_priv_read_role'")
		createRoleID := f.scalar("select role_id from mo_catalog.mo_role where role_name='nr_priv_create_role'")
		require.Less(t, readRoleID, createRoleID)
		var padding []string
		for i := 1; i <= 128; i++ {
			padding = append(padding, fmt.Sprintf("(%d,'nr_priv_aux.%st','select * from nr_priv_aux.t')", readRoleID, strings.Repeat(" ", i)))
		}
		defer func() {
			testutils.ExecSQL(t, "", cn, fmt.Sprintf("delete from mo_catalog.mo_role_rule where role_id=%d and rule_name like 'nr_priv_aux.%%'", readRoleID))
		}()
		testutils.ExecSQL(t, "", cn, "insert into mo_catalog.mo_role_rule (role_id,rule_name,`rule`) values "+strings.Join(padding, ","))
		// Legacy rule names may retain spaces in a quoted identifier even
		// though the rewrite resolver normalizes them to the actual table.
		f.exec("alter role nr_priv_create_role add rule \"select * from nr_priv_f.t where id=1\" on table nr_priv_f.` t`")
		require.Equal(t, int64(1), f.scalar(fmt.Sprintf("select count(*) from mo_catalog.mo_role_rule where role_id=%d and rule_name='nr_priv_f. t'", createRoleID)))
		require.Zero(t, f.scalar(fmt.Sprintf("select count(*) from (select role_id,rule_name from mo_catalog.mo_role_rule order by role_id,rule_name limit 128) r where role_id=%d and rule_name='nr_priv_f. t'", createRoleID)), "source rule must be beyond the first page")
		legacy := openUser("nr_priv_create_user", "nr_priv_create_role")
		require.NoError(t, legacy.QueryRowContext(ctx, "select count(*) from nr_priv_f.t").Scan(&value))
		require.Equal(t, 1, value, "legacy rule must actually filter the feature table")
		_, err, closeErr = readRebaseQuery(ctx, legacy, query("nr_priv_noncanonical"))
		require.NoError(t, closeErr)
		require.ErrorContains(t, err, "Rebase does not support rewrite rules on source tables")
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_priv_noncanonical'"))
		require.Equal(t, pinsBefore, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_priv_u'"))
		f.exec("alter role nr_priv_create_role drop rule on table nr_priv_f.` t`")
		f.exec("alter role nr_priv_create_role add rule \"select * from nr_priv_u.t where id=1\" on table nr_priv_u.t")
		f.exec("alter role nr_priv_create_role add rule \"select * from nr_priv_f.t where id=1\" on table nr_priv_f.t")
		filtered := openUser("nr_priv_create_user", "nr_priv_create_role")
		require.NoError(t, filtered.QueryRowContext(ctx, "select count(*) from nr_priv_f.t").Scan(&value))
		require.Equal(t, 1, value, "role rule must actually filter the feature table")
		pinsBefore = f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_priv_u'")
		_, err, closeErr = readRebaseQuery(ctx, filtered, query("nr_priv_filtered"))
		require.NoError(t, closeErr)
		require.ErrorContains(t, err, "Rebase does not support rewrite rules on source tables")
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_priv_filtered'"))
		require.Equal(t, pinsBefore, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='nr_priv_u'"))
	})
}

func TestNativeRebaseCompositePrimaryKey(t *testing.T) {
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
			"drop snapshot if exists nr_composite_bu", "drop snapshot if exists nr_composite_bf",
			"drop snapshot if exists nr_composite_fh", "drop snapshot if exists nr_composite_uh",
			"drop database if exists nr_composite_r", "drop database if exists nr_composite_f",
			"drop database if exists nr_composite_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_composite_"}
		f.exec("create database nr_composite_u")
		f.exec("create table nr_composite_u.t(id int, tenant_id int, v varchar(20), primary key(id,tenant_id))")
		f.exec("insert into nr_composite_u.t values(1,1,'a'),(1,2,NULL),(2,1,'x')")
		bu := f.snapshot("nr_composite_u", "bu")
		f.exec("data branch create database nr_composite_f from " + bu)
		bf := f.snapshot("nr_composite_f", "bf")
		f.exec("update nr_composite_f.t set v='branch' where id=1 and tenant_id=2")
		f.exec("delete from nr_composite_f.t where id=2 and tenant_id=1")
		f.exec("insert into nr_composite_f.t values(3,1,'new')")
		f.exec("update nr_composite_u.t set v='upstream' where id=1 and tenant_id=1")
		fh, uh := f.snapshot("nr_composite_f", "fh"), f.snapshot("nr_composite_u", "uh")
		_, counts, err := f.rebaseTables("nr_composite_r", fh, uh, bf, bu, "", 1)
		require.NoError(t, err)
		require.Equal(t, [6]uint64{3, 1, 0, 1, 1, 1}, counts)
		require.Equal(t, int64(3), f.scalar("select count(*) from nr_composite_r.t"))
		require.Equal(t, int64(1), f.scalar("select count(*) from nr_composite_r.t where id=1 and tenant_id=1 and v='upstream'"))
		require.Equal(t, int64(1), f.scalar("select count(*) from nr_composite_r.t where id=1 and tenant_id=2 and v='branch'"))
		require.Equal(t, int64(1), f.scalar("select count(*) from nr_composite_r.t where id=3 and tenant_id=1 and v='new'"))
	})
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
		defer cleanupSQLIntegration(t, cn, "drop snapshot if exists nr_creation_base", "drop database if exists nr_creation_f", "drop database if exists nr_creation_u", "drop database if exists nr_partition_merge")
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
		t.Run("partition_boundaries", func(t *testing.T) {
			checkBranchMergePreservesUnchangedPartitionRows(t, ctx, db)
		})
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

func runNativeRebaseDatabaseCases(t *testing.T, scenarios []string) {
	t.Helper()
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		for _, scenario := range scenarios {
			t.Run(scenario, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
				defer cancel()
				db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
				require.NoError(t, err)
				defer db.Close()
				f := rebaseSQLFixture{t, ctx, db, "nr_" + scenario + "_"}
				u, feature, target, second := f.prefix+"u", f.prefix+"f", f.prefix+"r", f.prefix+"r2"
				var cleanup []string
				for _, suffix := range []string{"bu", "bf", "fh", "uh", "f2", "u2"} {
					cleanup = append(cleanup, "drop snapshot if exists "+f.prefix+suffix)
				}
				for _, name := range []string{second, target, feature, u} {
					cleanup = append(cleanup, "drop database if exists "+name)
				}
				defer cleanupSQLIntegration(t, cn, cleanup...)
				f.exec("create database " + u)
				for _, table := range []string{"a", "b"} {
					f.exec("create table " + u + "." + table + "(id int primary key,v int)")
					f.exec("insert into " + u + "." + table + " values(1,10),(2,20),(3,NULL),(4,40)")
				}
				f.exec("create table " + u + ".history(v int,note varchar(20))")
				f.exec("insert into " + u + ".history values(1,'x'),(1,'x'),(NULL,NULL)")
				bu := f.snapshot(u, "bu")
				f.exec("data branch create database " + feature + " from " + bu)
				bf := f.snapshot(feature, "bf")
				f.exec("update " + feature + ".a set v=11 where id=1")
				f.exec("delete from " + feature + ".a where id=2")
				f.exec("insert into " + feature + ".a values(5,50)")
				f.exec("update " + feature + ".b set v=30 where id=3")
				f.exec("update " + u + ".a set v=44 where id=4")
				f.exec("insert into " + u + ".b values(6,60)")
				if scenario == "fail" || scenario == "skip" || scenario == "accept" {
					f.exec("update " + u + ".b set v=31 where id=3")
				}
				if scenario == "changed_bag" {
					f.exec("insert into " + feature + ".history values(1,'x')")
				}
				fh, uh := f.snapshot(feature, "fh"), f.snapshot(u, "uh")
				// Plain SQL is an independent prerequisite: different scans must
				// not borrow the process's first cached snapshot transaction.
				require.Equal(t, int64(4), f.scalar(fmt.Sprintf("select count(*) from %s.a{snapshot='%sbf'} f full outer join %s.a{snapshot='%sbu'} u on f.id=u.id where f.v <=> u.v", feature, f.prefix, u, f.prefix)))
				if scenario == "wrong_scope" {
					bf = feature + "{snapshot='" + f.prefix + "uh'}"
				}
				if scenario == "different_base" {
					// Both bases are valid ancestors, but the upstream base contains
					// a later update and is not the branch's original base state.
					bu = uh
				}
				policy := ""
				if scenario == "skip" {
					policy = "when conflict skip"
				}
				if scenario == "accept" {
					policy = "when conflict accept"
				}
				pinsBefore := f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='" + u + "'")
				ts, counts, err := f.rebase(target, fh, uh, bf, bu, policy)
				if scenario == "fail" || scenario == "changed_bag" || scenario == "wrong_scope" || scenario == "different_base" {
					require.Error(t, err)
					switch scenario {
					case "fail":
						require.ErrorContains(t, err, "Rebase conflict in table b")
					case "changed_bag":
						require.ErrorContains(t, err, "rejects changed no-PK tables")
					case "wrong_scope":
						require.ErrorContains(t, err, "does not belong")
					case "different_base":
						require.ErrorContains(t, err, "requires matching bases")
					}
					require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='"+target+"'"))
					require.Equal(t, pinsBefore, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='"+u+"'"))
					require.Equal(t, int64(11), f.scalar("select v from "+feature+".a where id=1"))
					require.Equal(t, int64(10), f.scalar("select v from "+u+".a where id=1"))
					return
				}
				require.NoError(t, err)
				require.Equal(t, []string{"1:11", "3:NULL", "4:44", "5:50"}, f.rows(target, "a"))
				wantB := []string{"1:10", "2:20", "3:30", "4:40", "6:60"}
				wantCounts := [6]uint64{4, 2, 0, 1, 2, 1}
				if scenario == "skip" {
					wantB[2] = "3:31"
					wantCounts = [6]uint64{4, 3, 1, 1, 1, 1}
				}
				if scenario == "accept" {
					wantCounts = [6]uint64{4, 3, 1, 1, 2, 1}
				}
				require.Equal(t, wantB, f.rows(target, "b"))
				require.Equal(t, wantCounts, counts)
				require.Equal(t, int64(3), f.scalar("select count(*) from "+target+".history"))
				f.exec("drop snapshot " + f.prefix + "uh")
				for _, table := range []string{"a", "b", "history"} {
					require.Equal(t, int64(1), f.scalar(fmt.Sprintf("select count(*) from mo_catalog.mo_branch_metadata m join mo_catalog.mo_tables t on m.table_id=t.rel_id join mo_catalog.mo_snapshots s on s.sname=concat('__mo_branch_',cast(m.table_id as char)) where t.reldatabase='%s' and t.relname='%s' and m.clone_ts=%d and s.kind='branch' and s.obj_id=m.p_table_id and s.ts=m.clone_ts", target, table, ts)))
				}
				if scenario != "repeat" {
					return
				}
				f.exec("update " + target + ".a set v=12 where id=1")
				f.exec("update " + u + ".a set v=45 where id=4")
				f2, u2 := f.snapshot(target, "f2"), f.snapshot(u, "u2")
				base := fmt.Sprintf("%s{MO_TS=%d}", u, ts)
				_, counts, err = f.rebase(second, f2, u2, base, base, "")
				require.NoError(t, err, "the retained new base must exclude already inherited upstream changes")
				require.Zero(t, counts[2])
				require.Equal(t, []string{"1:12", "3:NULL", "4:45", "5:50"}, f.rows(second, "a"))
				f.exec("data branch merge " + second + ".a into " + u + ".a when conflict accept")
				require.Equal(t, f.rows(second, "a"), f.rows(u, "a"))
			})
		}
	})
}
