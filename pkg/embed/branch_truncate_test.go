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

package embed

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataBranchAfterTableClear(t *testing.T) {
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	c, err := NewCluster(WithTesting(), WithPreStart(adjustBasicClusterService))
	require.NoError(t, err)
	t.Cleanup(func() {
		if assert.NoError(t, c.Close()) {
			assert.NoError(t, os.RemoveAll(c.(*cluster).options.dataPath))
		}
	})
	require.NoError(t, c.Start())
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	port := cn.GetServiceConfig().CN.Frontend.Port
	require.NotEqualValues(t, 6001, port)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)

	for i, tc := range []struct {
		name    string
		clear   string
		merged  bool
		source  bool
		replace bool
	}{
		{"merged_delete", "delete from m1", true, false, true},
		{"merged_where", "delete from m1 where k > 0", true, false, false},
		{"merged_truncate", "truncate table m1", true, false, true},
		{"fresh_dst_delete", "delete from m1", false, false, true},
		{"fresh_dst_truncate", "truncate table m1", false, false, true},
		{"fresh_src_delete", "delete from m2", false, true, true},
		{"fresh_src_truncate", "truncate table m2", false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec, query := branchClearSQL(t, db)
			dbName := fmt.Sprintf("branch_clear_%d", i)
			idsSQL := "select relname, rel_id from mo_catalog.mo_tables where reldatabase = '" + dbName + "' order by relname"
			exec("create database " + dbName)
			exec("use " + dbName)
			exec("create table m1 (k int primary key, v varchar(10))")
			exec("insert into m1 values (1,'a'),(2,'b')")
			exec("data branch create table m2 from m1")
			before := query(idsSQL, 2)
			if tc.merged {
				exec("insert into m2 values (3,'new')")
				exec("update m2 set v='mod' where k=1")
				exec("data branch merge m2 into m1")
				assert.Equal(t, [][]string{{"1", "mod"}, {"2", "b"}, {"3", "new"}}, query("select * from m1 order by k", 2))
			}
			exec(tc.clear)
			after := query(idsSQL, 2)
			require.Equal(t, tc.replace, before[0][1] != after[0][1] || before[1][1] != after[1][1])
			if tc.replace {
				idx, level := 0, "truncate"
				if tc.source {
					idx, level = 1, "truncate:table"
				}
				require.Equal(t, [][]string{{before[idx][1], level}}, query(
					"select p_table_id, level from mo_catalog.mo_branch_metadata where table_id="+after[idx][1]+" and not table_deleted", 2,
				))
			}
			if tc.merged {
				exec("insert into m1 values (1,'x')")
			}
			exec("insert into m2 values (5,'postmerge')")
			if !tc.merged {
				diff := query("data branch diff m2 against m1", 4)
				t.Logf("fresh DIFF before PICK: %v", diff)
				side := "m1"
				if tc.source {
					side = "m2"
				}
				assert.ElementsMatch(t, [][]string{{side, "DELETE", "1", "a"}, {side, "DELETE", "2", "b"}, {"m2", "INSERT", "5", "postmerge"}}, diff)
			}
			exec("data branch pick m2 into m1 keys (5)")
			afterFive := query("select * from m1 order by k", 2)
			t.Logf("after PICK(5): m1=%v", afterFive)
			switch {
			case tc.merged:
				assert.Equal(t, [][]string{{"1", "x"}, {"5", "postmerge"}}, afterFive)
				exec("data branch pick m2 into m1 keys (3)")
				afterThree := query("select * from m1 order by k", 2)
				t.Logf("after PICK(3): m1=%v", afterThree)
				assert.Equal(t, [][]string{{"1", "x"}, {"3", "new"}, {"5", "postmerge"}}, afterThree)
			case tc.source:
				assert.Equal(t, [][]string{{"1", "a"}, {"2", "b"}, {"5", "postmerge"}}, afterFive)
				exec("data branch pick m2 into m1 keys (1)")
				assert.Equal(t, [][]string{{"2", "b"}, {"5", "postmerge"}}, query("select * from m1 order by k", 2))
			default:
				assert.Equal(t, [][]string{{"5", "postmerge"}}, afterFive)
			}
			diff := query("data branch diff m2 against m1", 4)
			t.Logf("final DIFF: %v", diff)
			switch {
			case tc.merged:
				assert.ElementsMatch(t, [][]string{{"m2", "UPDATE", "1", "mod"}, {"m1", "UPDATE", "1", "x"}, {"m1", "DELETE", "2", "b"}}, diff)
			case tc.source:
				assert.ElementsMatch(t, [][]string{{"m2", "DELETE", "2", "b"}}, diff)
			default:
				assert.ElementsMatch(t, [][]string{{"m1", "DELETE", "1", "a"}, {"m1", "DELETE", "2", "b"}}, diff)
			}
			exec("data branch delete table m2")
			exec("drop database " + dbName)
		})
	}
	t.Run("copy_alter_on_both_sides", func(t *testing.T) {
		exec, _ := branchClearSQL(t, db)
		exec("create database branch_clear_copy")
		exec("use branch_clear_copy")
		exec("create table m1 (k int primary key, v varchar(10))")
		exec("insert into m1 values (1,'a'),(2,'b')")
		exec("data branch create table m2 from m1")
		exec("alter table m1 add column extra int default 0, algorithm=copy")
		exec("alter table m2 add column extra int default 0, algorithm=copy")
		exec("update m2 set extra=33 where k=1")
		_, query := branchClearSQL(t, db)
		require.Equal(t, [][]string{{"m2", "UPDATE", "1", "a", "33"}}, query("data branch diff m2 against m1", 5))
		exec("drop database branch_clear_copy")
	})
	t.Run("between", func(t *testing.T) {
		exec, query := branchClearSQL(t, db)
		for _, stmt := range []string{
			"create database branch_clear_between",
			"use branch_clear_between",
			"create table root (k int primary key, v varchar(10))",
			"insert into root values (1,'a'),(2,'b')",
			"data branch create table src from root",
			"data branch create table dst_before from root",
			"data branch create table dst_inside from root",
			"create table dst_independent (k int primary key, v varchar(10))",
			"insert into dst_independent values (1,'local'),(2,'b'),(4,'at_lo')",
			"insert into src values (4,'at_lo')",
			"create snapshot clear_lo for account sys",
			"data branch pick src into dst_inside keys(4)",
			"update dst_inside set v='local' where k=1",
			"truncate table src",
			"insert into src values (1,'a'),(9,'wiped')",
			"delete from src",
			"insert into src values (1,'a')",
			"create snapshot clear_after for account sys",
			"alter table src add column extra int default 0, algorithm=copy",
			"insert into src values (3,'c',0)",
			"create snapshot clear_hi for account sys",
			"truncate table src",
			"insert into src values (8,'later',0)",
		} {
			exec(stmt)
		}
		require.ElementsMatch(t, [][]string{{"src", "INSERT", "4", "at_lo"}},
			query("data branch diff src{snapshot='clear_lo'} against root", 4))
		// Both windows are bounded before the final truncate. The first also
		// starts after the previous truncate and therefore picks only key 3.
		exec("data branch pick src into dst_before between snapshot clear_after and clear_hi")
		require.Equal(t, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}},
			query("select * from dst_before order by k", 2))
		// Key 4 was independently written to the destination after its fork.
		// Explicitly accept the source's bounded deletion of that key.
		exec("data branch pick src into dst_inside between snapshot clear_lo and clear_hi when conflict accept")
		require.Equal(t, [][]string{{"1", "local"}, {"3", "c"}},
			query("select * from dst_inside order by k", 2))
		exec("data branch pick src into dst_independent between snapshot clear_lo and clear_hi keys(2,3,4)")
		require.Equal(t, [][]string{{"1", "local"}, {"3", "c"}},
			query("select * from dst_independent order by k", 2))
		for _, stmt := range []string{
			"drop snapshot clear_lo", "drop snapshot clear_after", "drop snapshot clear_hi",
			"drop database branch_clear_between",
		} {
			exec(stmt)
		}
	})
	t.Run("bounded_delete_conflicts_with_insert", func(t *testing.T) {
		exec, _ := branchClearSQL(t, db)
		exec("create database branch_clear_insert_conflict")
		t.Cleanup(func() { _, _ = db.Exec("drop database branch_clear_insert_conflict") })
		exec("use branch_clear_insert_conflict")
		exec("create table dst (k int primary key, v varchar(10))")
		exec("insert into dst values (1,'fork')")
		exec("data branch create table src from dst")
		exec("insert into src values (4,'old')")
		exec("create snapshot clear_insert_lo for account sys")
		t.Cleanup(func() { _, _ = db.Exec("drop snapshot clear_insert_lo") })
		exec("insert into dst values (4,'local')")
		exec("truncate table src")
		exec("create snapshot clear_insert_hi for account sys")
		t.Cleanup(func() { _, _ = db.Exec("drop snapshot clear_insert_hi") })
		const pick = "data branch pick src into dst between snapshot clear_insert_lo and clear_insert_hi keys(4) when conflict "
		for _, opt := range []string{"fail", "skip", "accept"} {
			t.Run(opt, func(t *testing.T) {
				exec, query := branchClearSQL(t, db)
				exec("insert ignore into dst values (4,'local')")
				_, err := db.ExecContext(t.Context(), pick+opt)
				want := [][]string{{"1", "fork"}, {"4", "local"}}
				switch opt {
				case "fail":
					assert.ErrorContains(t, err, "conflict")
				case "skip":
					require.NoError(t, err)
				case "accept":
					require.NoError(t, err)
					want = want[:1]
				}
				assert.Equal(t, want, query("select * from dst order by k", 2))
			})
		}
	})
	t.Run("bounded_delete_of_inherited_row", func(t *testing.T) {
		exec, _ := branchClearSQL(t, db)
		exec("create database branch_clear_inherited")
		t.Cleanup(func() { _, _ = db.Exec("drop database branch_clear_inherited") })
		exec("use branch_clear_inherited")
		exec("create table ancestor (k int primary key, v varchar(10))")
		exec("insert into ancestor values (1,'keep')")
		exec("data branch create table src from ancestor")
		exec("insert into src values (4,'source'),(5,'source')")
		exec("insert into ancestor values (4,'inherited'),(5,'inherited')")
		exec("create snapshot clear_inherited_lo for account sys")
		t.Cleanup(func() { _, _ = db.Exec("drop snapshot clear_inherited_lo") })
		exec("truncate table src")
		exec("create snapshot clear_inherited_hi for account sys")
		t.Cleanup(func() { _, _ = db.Exec("drop snapshot clear_inherited_hi") })

		for _, opt := range []string{"fail", "skip", "accept"} {
			t.Run(opt, func(t *testing.T) {
				exec, query := branchClearSQL(t, db)
				exec("data branch create table dst from ancestor")
				t.Cleanup(func() { _, _ = db.Exec("drop table dst") })
				exec("update dst set v='local' where k=5")
				const pick = "data branch pick src into dst between snapshot clear_inherited_lo and clear_inherited_hi"
				exec(pick + " keys(4) when conflict " + opt)
				require.Equal(t, [][]string{{"1", "keep"}, {"5", "local"}},
					query("select * from dst order by k", 2))

				_, err := db.ExecContext(t.Context(), pick+" keys(5) when conflict "+opt)
				want := [][]string{{"1", "keep"}, {"5", "local"}}
				switch opt {
				case "fail":
					require.ErrorContains(t, err, "conflict")
				case "skip":
					require.NoError(t, err)
				case "accept":
					require.NoError(t, err)
					want = want[:1]
				}
				require.Equal(t, want, query("select * from dst order by k", 2))
			})
		}
	})
	t.Run("bounded_update_of_inherited_row", func(t *testing.T) {
		exec, _ := branchClearSQL(t, db)
		exec("create database branch_clear_inherited_update")
		t.Cleanup(func() { _, _ = db.Exec("drop database branch_clear_inherited_update") })
		exec("use branch_clear_inherited_update")
		exec("create table ancestor (k int primary key, v varchar(10))")
		exec("data branch create table src from ancestor")
		exec("insert into src values (4,'old')")
		exec("insert into ancestor values (4,'inherited')")

		cases := []struct {
			dst      string
			strategy string
			modified bool
			wantErr  bool
			want     string
		}{
			{"inherited_fail", "fail", false, false, "new"},
			{"inherited_skip", "skip", false, false, "new"},
			{"inherited_accept", "accept", false, false, "new"},
			{"modified_fail", "fail", true, true, "local"},
			{"modified_skip", "skip", true, false, "local"},
			{"modified_accept", "accept", true, false, "new"},
		}
		for _, tc := range cases {
			exec("data branch create table " + tc.dst + " from ancestor")
			if tc.modified {
				exec("update " + tc.dst + " set v='local' where k=4")
			}
		}
		exec("create snapshot clear_inherited_update_lo for account sys")
		t.Cleanup(func() { _, _ = db.Exec("drop snapshot clear_inherited_update_lo") })
		exec("truncate table src")
		exec("insert into src values (4,'new')")
		exec("create snapshot clear_inherited_update_hi for account sys")
		t.Cleanup(func() { _, _ = db.Exec("drop snapshot clear_inherited_update_hi") })

		for _, tc := range cases {
			t.Run(tc.dst, func(t *testing.T) {
				_, query := branchClearSQL(t, db)
				pick := "data branch pick src into " + tc.dst +
					" between snapshot clear_inherited_update_lo and clear_inherited_update_hi keys(4) when conflict " + tc.strategy
				_, err := db.ExecContext(t.Context(), pick)
				if tc.wantErr {
					require.ErrorContains(t, err, "conflict: src INSERT and "+tc.dst+" INSERT on pk(4) with different values")
				} else {
					require.NoError(t, err, "%s", pick)
				}
				require.Equal(t, [][]string{{"4", tc.want}}, query("select * from "+tc.dst, 2))
			})
		}
	})
	t.Run("bounded_update_of_missing_row", func(t *testing.T) {
		exec, _ := branchClearSQL(t, db)
		exec("create database branch_clear_missing")
		t.Cleanup(func() { _, _ = db.Exec("drop database branch_clear_missing") })
		exec("use branch_clear_missing")
		exec("create table ancestor (k int primary key, v varchar(10))")
		exec("insert into ancestor values (1,'old')")
		exec("data branch create table src from ancestor")
		exec("insert into src values (4,'old'),(5,'old')")
		exec("create snapshot clear_missing_lo for account sys")
		t.Cleanup(func() { _, _ = db.Exec("drop snapshot clear_missing_lo") })
		exec("truncate table src")
		exec("insert into src values (1,'new'),(4,'new'),(5,'new')")
		exec("create snapshot clear_missing_hi for account sys")
		t.Cleanup(func() { _, _ = db.Exec("drop snapshot clear_missing_hi") })

		for _, opt := range []string{"fail", "skip", "accept"} {
			t.Run(opt, func(t *testing.T) {
				exec, query := branchClearSQL(t, db)
				exec("data branch create table dst from ancestor")
				t.Cleanup(func() { _, _ = db.Exec("drop table dst") })
				exec("insert into dst values (5,'local')")
				const pick = "data branch pick src into dst between snapshot clear_missing_lo and clear_missing_hi"
				exec(pick + " keys(1,4) when conflict " + opt)
				require.Equal(t, [][]string{{"1", "new"}, {"4", "new"}, {"5", "local"}},
					query("select * from dst order by k", 2))

				_, err := db.ExecContext(t.Context(), pick+" keys(5) when conflict "+opt)
				value := "local"
				switch opt {
				case "fail":
					require.ErrorContains(t, err, "conflict")
				case "skip":
					require.NoError(t, err)
				case "accept":
					require.NoError(t, err)
					value = "new"
				}
				require.Equal(t, [][]string{{"1", "new"}, {"4", "new"}, {"5", value}},
					query("select * from dst order by k", 2))
			})
		}
	})
	t.Run("fake_pk_double_truncate", func(t *testing.T) {
		exec, _ := branchClearSQL(t, db)
		exec("create database branch_clear_fake_rekey")
		t.Cleanup(func() { _, _ = db.Exec("drop database branch_clear_fake_rekey") })
		exec("use branch_clear_fake_rekey")
		for _, tc := range []struct {
			atFork, atMerge int
		}{{1, 1}, {2, 2}, {2, 3}} {
			t.Run(fmt.Sprintf("%d_to_%d", tc.atFork, tc.atMerge), func(t *testing.T) {
				exec, query := branchClearSQL(t, db)
				exec("create table dst (v varchar(10))")
				t.Cleanup(func() { _, _ = db.Exec("drop table dst") })
				exec("insert into dst values ('padding')" + strings.Repeat(",('a')", tc.atFork))
				exec("delete from dst where v='padding'")
				exec("data branch create table src from dst")
				t.Cleanup(func() { _, _ = db.Exec("drop table src") })
				exec("truncate table src")
				exec("truncate table dst")
				exec("insert into dst values ('a')" + strings.Repeat(",('a')", tc.atMerge-1))
				exec("data branch merge src into dst")
				want := make([][]string, tc.atMerge-tc.atFork)
				if len(want) > 0 {
					want[0] = []string{"a"}
				}
				require.Equal(t, want, query("select v from dst order by v", 1))
			})
		}
	})
	t.Run("copy_added_common_default", func(t *testing.T) {
		exec, query := branchClearSQL(t, db)
		exec("create database branch_clear_default")
		t.Cleanup(func() { _, _ = db.Exec("drop database branch_clear_default") })
		exec("use branch_clear_default")
		exec("create table root (k int primary key, v varchar(10))")
		exec("insert into root values (1,'a')")
		exec("data branch create table src from root")
		exec("data branch create table dst from root")
		exec("alter table src add column extra int default 0, algorithm=copy")
		exec("alter table dst add column extra int default 0, algorithm=copy")
		exec("truncate table src")
		exec("insert into src values (1,'a',0)")
		assert.Empty(t, query("data branch diff src against dst", 5))
		exec("update src set extra=33 where k=1")
		require.Equal(t, [][]string{{"src", "UPDATE", "1", "a", "33"}},
			query("data branch diff src against dst", 5))
	})
	t.Run("bounded_clear_renamed_columns", func(t *testing.T) {
		exec, query := branchClearSQL(t, db)
		exec("create database branch_clear_rename")
		t.Cleanup(func() { _, _ = db.Exec("drop database branch_clear_rename") })
		exec("use branch_clear_rename")
		exec("create table dst (k int primary key, v varchar(10), w varchar(10))")
		exec("insert into dst values (1,'a','b')")
		exec("data branch create table src from dst")
		exec("create snapshot clear_rename_lo for account sys")
		t.Cleanup(func() { _, _ = db.Exec("drop snapshot clear_rename_lo") })
		exec("alter table src change column v renamed varchar(10), algorithm=copy")
		exec("alter table src change column w v varchar(10), algorithm=copy")
		exec("alter table dst change column v renamed varchar(10), algorithm=copy")
		exec("alter table dst change column w v varchar(10), algorithm=copy")
		exec("truncate table src")
		exec("insert into src values (1,'a','b')")
		exec("create snapshot clear_rename_hi for account sys")
		t.Cleanup(func() { _, _ = db.Exec("drop snapshot clear_rename_hi") })
		for _, table := range []string{"src", "dst"} {
			exec("alter table " + table + " change column renamed final_name varchar(10), algorithm=copy")
			exec("alter table " + table + " change column v renamed varchar(10), algorithm=copy")
		}
		exec("update dst set final_name='local' where k=1")
		exec("data branch pick src into dst between snapshot clear_rename_lo and clear_rename_hi keys(1) when conflict fail")
		require.Equal(t, [][]string{{"1", "local", "b"}}, query("select * from dst order by k", 3))
	})
	t.Run("copy_alter_before_and_after", func(t *testing.T) {
		exec, query := branchClearSQL(t, db)
		for _, stmt := range []string{
			"create database branch_clear_evolve",
			"use branch_clear_evolve",
			"create table root (k int primary key, v varchar(10))",
			"insert into root values (1,'a'),(2,'b')",
			"data branch create table src from root",
			"alter table src add column extra int default 0, algorithm=copy",
			"truncate table src",
			"insert into src values (1,'a',33),(3,'c',36)",
			"alter table src change column v renamed varchar(10), algorithm=copy",
		} {
			exec(stmt)
		}
		require.ElementsMatch(t, [][]string{{"src", "DELETE", "2", "b", "NULL"}, {"src", "INSERT", "3", "c", "36"}},
			query("data branch diff src against root", 5))
		exec("data branch merge src into root")
		require.Equal(t, [][]string{{"1", "a"}, {"3", "c"}}, query("select * from root order by k", 2))
		const snapshots = "select count(*) from mo_catalog.mo_snapshots where kind = 'branch' and database_name = 'branch_clear_evolve'"
		require.Equal(t, [][]string{{"4"}}, query(snapshots, 1))
		exec("drop table src")
		require.Equal(t, [][]string{{"0"}}, query(snapshots, 1))
		exec("drop database branch_clear_evolve")
	})
	t.Run("both_sides_and_conflicts", func(t *testing.T) {
		exec, query := branchClearSQL(t, db)
		exec("create database branch_clear_conflict")
		exec("use branch_clear_conflict")
		exec("create table root (k int primary key, v varchar(10))")
		exec("insert into root values (1,'a'),(2,'b')")
		exec("data branch create table src from root")
		exec("truncate table src")
		exec("insert into src values (1,'x'),(3,'c')")
		exec("truncate table root")
		exec("insert into root values (1,'y')")
		const quotaSQL = "select count(*) from mo_catalog.mo_branch_metadata b join mo_catalog.mo_tables t on b.table_id = t.rel_id where t.account_id = 0 and t.reldatabase = 'branch_clear_conflict' and not b.table_deleted and b.level not in ('alter', 'truncate')"
		require.Equal(t, [][]string{{"1"}}, query(quotaSQL, 1))
		_, rootDeleteErr := db.ExecContext(t.Context(), "data branch delete table root")
		require.ErrorContains(t, rootDeleteErr, "not an active branch table")
		require.ElementsMatch(t, [][]string{{"src", "UPDATE", "1", "x"}, {"root", "UPDATE", "1", "y"}, {"src", "INSERT", "3", "c"}},
			query("data branch diff src against root", 4))
		_, err := db.ExecContext(t.Context(), "data branch pick src into root keys(1,3) when conflict fail")
		require.ErrorContains(t, err, "conflict")
		require.Equal(t, [][]string{{"1", "y"}}, query("select * from root order by k", 2))
		exec("data branch pick src into root keys(1,3) when conflict skip")
		require.Equal(t, [][]string{{"1", "y"}, {"3", "c"}}, query("select * from root order by k", 2))
		exec("data branch pick src into root keys(1) when conflict accept")
		require.Equal(t, [][]string{{"1", "x"}, {"3", "c"}}, query("select * from root order by k", 2))
		exec("drop database branch_clear_conflict")
	})
	t.Run("composite_and_fake_pk", func(t *testing.T) {
		exec, query := branchClearSQL(t, db)
		for _, stmt := range []string{
			"create database branch_clear_keys", "use branch_clear_keys",
			"create table root (a int, b int, v varchar(10), primary key(a,b))",
			"insert into root values (1,10,'a'),(1,20,'b')",
			"data branch create table src from root",
			"truncate table src",
			"insert into src values (1,10,'x'),(3,30,'c')",
		} {
			exec(stmt)
		}
		require.ElementsMatch(t, [][]string{{"src", "UPDATE", "1", "10", "x"}, {"src", "DELETE", "1", "20", "b"}, {"src", "INSERT", "3", "30", "c"}},
			query("data branch diff src against root", 5))
		exec("data branch pick src into root keys((1,20))")
		require.Equal(t, [][]string{{"1", "10", "a"}}, query("select * from root order by a,b", 3))
		exec("data branch merge src into root when conflict accept")
		require.Equal(t, [][]string{{"1", "10", "x"}, {"3", "30", "c"}}, query("select * from root order by a,b", 3))
		exec("create table fake_root (v varchar(10))")
		exec("insert into fake_root values ('a'),('a'),('b')")
		exec("data branch create table fake_src from fake_root")
		exec("truncate table fake_src")
		exec("insert into fake_src values ('a'),('c')")
		require.ElementsMatch(t, [][]string{{"fake_src", "DELETE", "a"}, {"fake_src", "DELETE", "b"}, {"fake_src", "INSERT", "c"}},
			query("data branch diff fake_src against fake_root", 3))
		exec("data branch merge fake_src into fake_root")
		require.Equal(t, [][]string{{"a"}, {"c"}}, query("select * from fake_root order by v", 1))
		exec("alter table fake_root add column extra int default 0, algorithm=copy")
		exec("alter table fake_src add column extra int default 0, algorithm=copy")
		_, err := db.ExecContext(t.Context(), "data branch diff fake_src against fake_root")
		require.ErrorContains(t, err, "fake primary key schema differs")
		exec("drop database branch_clear_keys")
	})
	t.Run("keyed_clear_baseline", func(t *testing.T) {
		exec, query := branchClearSQL(t, db)
		exec("create database branch_clear_key_filter")
		exec("use branch_clear_key_filter")
		exec("create table dst (k int primary key, v int)")
		exec("insert into dst select result,result from generate_series(1,32768) g")
		exec("data branch create table src from dst")
		exec("truncate table src")
		exec("data branch pick src into dst keys(7)")
		require.Equal(t, [][]string{{"32767"}}, query("select count(*) from dst", 1))
		require.Empty(t, query("select k from dst where k=7", 1))
		require.Equal(t, [][]string{{"6"}, {"8"}}, query("select k from dst where k between 6 and 8 order by k", 1))
		exec("drop database branch_clear_key_filter")
	})
	t.Run("merge_fixture", func(t *testing.T) {
		assertBranchClearFixture(t, db, "../../test/distributed/cases/git4data/branch/merge/merge_issue_32003")
	})
	t.Run("lifecycle_fixture", func(t *testing.T) {
		assertBranchClearFixture(t, db, "../../test/distributed/cases/git4data/branch/metadata/branch_rebuild_lifecycle")
	})
}

func TestDataBranchClearBoundaryAfterGC(t *testing.T) {
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	c, err := NewCluster(WithTesting(), WithPreStart(func(svc ServiceOperator) {
		adjustBasicClusterService(svc)
		if svc.ServiceType() == metadata.ServiceType_TN {
			svc.Adjust(func(cfg *ServiceConfig) {
				tn := cfg.TN_please_use_getTNServiceConfig
				if tn == nil {
					tn = cfg.TNCompatible
				}
				tn.GCCfg.CatalogGCInterval.Duration = time.Second
				tn.GCCfg.ScanGCInterval.Duration = 2 * time.Second
				tn.GCCfg.GCTTL.Duration = time.Millisecond
				tn.GCCfg.GCInMemoryTTL.Duration = time.Millisecond
				tn.Ckp.IncrementalInterval.Duration = 2 * time.Second
			})
		}
	}))
	require.NoError(t, err)
	t.Cleanup(func() {
		if assert.NoError(t, c.Close()) {
			assert.NoError(t, os.RemoveAll(c.(*cluster).options.dataPath))
		}
	})
	require.NoError(t, c.Start())
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	db := recoverySQLClient(t, cn)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })
	ctx := t.Context()
	exec := func(stmt string) {
		t.Helper()
		_, err = db.ExecContext(ctx, stmt)
		require.NoError(t, err, "%s", stmt)
	}
	for _, stmt := range []string{
		"create database bd_boundary_gc",
		"use bd_boundary_gc",
		"create table m1(k int primary key, v int)",
		"insert into m1 select result, result from generate_series(1, 5000) g",
		"data branch create table m2 from m1",
		"alter table m2 add column extra int default 0, algorithm=copy",
		"select mo_ctl('dn','flush','bd_boundary_gc.m2')",
	} {
		exec(stmt)
	}
	initial := func() []string {
		rows, err := db.QueryContext(ctx, "select object_name from metadata_scan('bd_boundary_gc.m2','k') m")
		require.NoError(t, err)
		defer rows.Close()
		var names []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			names = append(names, name)
		}
		require.NoError(t, rows.Err())
		return names
	}()
	require.NotEmpty(t, initial)
	exec("update m2 set v=10000 where k=1")
	exec("insert into m2 select result, result, 0 from generate_series(5001, 6000) g")
	exec("select mo_ctl('dn','flush','bd_boundary_gc.m2')")
	exec("select mo_ctl('dn','inspect','merge trigger -t bd_boundary_gc.m2 --kind l0 --l0-oneshot')")
	const diff = "data branch diff m2 against m1 output count"
	var count int
	require.NoError(t, db.QueryRowContext(ctx, diff).Scan(&count))
	require.Equal(t, 1001, count)
	deadline := time.Now().Add(90 * time.Second)
	for {
		exec("select mo_ctl('dn','globalcheckpoint','')")
		exec("select mo_ctl('dn','diskcleaner','force_gc')")
		reclaimed := true
		for _, name := range initial {
			_, err := os.Stat(c.(*cluster).options.dataPath + "/s3/" + name)
			if !os.IsNotExist(err) {
				reclaimed = false
			}
		}
		if reclaimed {
			break
		}
		require.True(t, time.Now().Before(deadline), "copy materialization objects %v were not reclaimed", initial)
		time.Sleep(2 * time.Second)
	}
	t.Logf("copy materialization objects reclaimed: %v", initial)
	require.NoError(t, db.QueryRowContext(ctx, diff).Scan(&count))
	require.Equal(t, 1001, count)

	exec("create table clear_root(k int primary key, v int)")
	exec("insert into clear_root values (1,10),(2,20),(3,30)")
	exec("data branch create table clear_src from clear_root")
	const clearIDSQL = "select rel_id from mo_catalog.mo_tables where reldatabase='bd_boundary_gc' and relname='clear_src'"
	_, queryBeforeRestart := branchClearSQL(t, db)
	beforeClear := queryBeforeRestart(clearIDSQL, 1)
	require.Len(t, beforeClear, 1)
	exec("select mo_ctl('dn','flush','bd_boundary_gc.clear_src')")
	exec("truncate table clear_src")
	exec("insert into clear_src values (2,20),(3,31)")
	assertTruncateDiff := func(query func(string, int) [][]string) {
		t.Helper()
		afterClear := query(clearIDSQL, 1)
		require.Len(t, afterClear, 1)
		require.NotEqual(t, beforeClear[0][0], afterClear[0][0])
		require.Equal(t, [][]string{{beforeClear[0][0], "truncate:table"}}, query(
			"select p_table_id, level from mo_catalog.mo_branch_metadata where not table_deleted and table_id="+afterClear[0][0], 2))
		require.ElementsMatch(t, [][]string{{"clear_src", "DELETE", "1", "10"}, {"clear_src", "UPDATE", "3", "31"}},
			query("data branch diff bd_boundary_gc.clear_src against bd_boundary_gc.clear_root", 4))
		require.Equal(t, [][]string{{"1", "10"}, {"2", "20"}, {"3", "30"}},
			query("select k,v from bd_boundary_gc.clear_root order by k", 2))
	}
	assertTruncateDiff(queryBeforeRestart)
	assertDiskGCReclaimsProbe(t, ctx, db, c, branchRestartScenario{
		database: "bd_boundary_gc", reclaimProbeTable: "truncate_gc_probe",
	})
	assertTruncateDiff(queryBeforeRestart)
	require.NoError(t, db.Close())
	require.NoError(t, c.Stop())
	require.NoError(t, c.Start())
	cn, err = c.GetCNService(0)
	require.NoError(t, err)
	restarted := recoverySQLClient(t, cn)
	defer restarted.Close()
	_, query := branchClearSQL(t, restarted)
	assertTruncateDiff(query)
	rows := query("data branch diff bd_boundary_gc.m2 against bd_boundary_gc.m1", 5)
	require.Len(t, rows, 1001)
	want := make(map[string][]string, 1001)
	want["1"] = []string{"m2", "UPDATE", "1", "10000"}
	for id := 5001; id <= 6000; id++ {
		k := strconv.Itoa(id)
		want[k] = []string{"m2", "INSERT", k, k}
	}
	for _, row := range rows {
		require.Equal(t, want[row[2]], row[:4])
		delete(want, row[2])
	}
	require.Empty(t, want)
	t.Log("after restart: 1 update, 1000 inserts, 0 deletes")
}

func branchClearSQL(t *testing.T, db *sql.DB) (exec func(string), query func(string, int) [][]string) {
	t.Helper()
	return func(stmt string) {
			t.Helper()
			_, err := db.ExecContext(t.Context(), stmt)
			require.NoError(t, err, "%s", stmt)
		},
		func(stmt string, width int) [][]string {
			t.Helper()
			rows, err := db.QueryContext(t.Context(), stmt)
			require.NoError(t, err, "%s", stmt)
			defer rows.Close()
			cols, err := rows.Columns()
			require.NoError(t, err)
			require.Len(t, cols, width, "%s", stmt)
			result := make([][]string, 0)
			for rows.Next() {
				values := make([]sql.NullString, width)
				dest := make([]any, width)
				for i := range values {
					dest[i] = &values[i]
				}
				require.NoError(t, rows.Scan(dest...), "%s", stmt)
				row := make([]string, width)
				for i, value := range values {
					row[i] = value.String
					if !value.Valid {
						row[i] = "NULL"
					}
				}
				result = append(result, row)
			}
			require.NoError(t, rows.Err(), "%s", stmt)
			return result
		}
}

// Execute the two branch fixtures through the isolated cluster and match
// their recorded statement sequence, rows, and transaction rejection.
func assertBranchClearFixture(t *testing.T, db *sql.DB, path string) {
	t.Helper()
	sqlData, err := os.ReadFile(path + ".sql")
	require.NoError(t, err)
	resultFile, err := os.Open(path + ".result")
	require.NoError(t, err)
	defer resultFile.Close()
	result := bufio.NewScanner(resultFile)
	var buffered string
	var hasBuffered bool
	peekLine := func() string {
		if !hasBuffered && result.Scan() {
			buffered, hasBuffered = strings.TrimSpace(result.Text()), true
		}
		if hasBuffered {
			return buffered
		}
		return ""
	}
	nextLine := func() string {
		t.Helper()
		line := peekLine()
		require.True(t, hasBuffered, "unexpected end of %s", path+".result")
		hasBuffered = false
		return line
	}
	var sqlText strings.Builder
	for line := range strings.SplitSeq(string(sqlData), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			sqlText.WriteString(line)
			sqlText.WriteByte('\n')
		}
	}
	exec, query := branchClearSQL(t, db)
	for text := range strings.SplitSeq(sqlText.String(), ";") {
		stmt := strings.TrimSpace(text)
		if stmt == "" {
			continue
		}
		lines := []string{nextLine()}
		for !strings.HasSuffix(lines[len(lines)-1], ";") {
			lines = append(lines, nextLine())
		}
		require.Equal(t, strings.Fields(stmt+";"), strings.Fields(strings.Join(lines, " ")))
		switch {
		case strings.HasPrefix(peekLine(), "-- @regex("):
			nextLine()
			want := nextLine()
			_, err := db.ExecContext(t.Context(), stmt)
			require.ErrorContains(t, err, want)
		case strings.HasPrefix(stmt, "select "), strings.HasPrefix(stmt, "data branch diff "):
			header := nextLine()
			require.True(t, strings.HasPrefix(header, "➤"), "%s", stmt)
			width := 1 + strings.Count(header, "¦")
			want := make([][]string, 0)
			for line := header; strings.HasSuffix(line, "𝄀"); {
				line = nextLine()
				row := strings.Split(strings.TrimSuffix(line, "𝄀"), "¦")
				for i := range row {
					row[i] = strings.TrimSpace(row[i])
				}
				want = append(want, row)
			}
			rows := query(stmt, width)
			require.Equal(t, want, rows, "%s", stmt)
			t.Logf("%s -> %v", strings.Join(strings.Fields(stmt), " "), rows)
		default:
			exec(stmt)
		}
	}
	line := peekLine()
	require.False(t, hasBuffered, "unexpected remaining result line %q", line)
	require.NoError(t, result.Err())
}
