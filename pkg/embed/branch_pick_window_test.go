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

package embed

import (
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataBranchPickWindowBaseline(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		require.NotEqualValues(t, 6001, cn.GetServiceConfig().CN.Frontend.Port)
		db := recoverySQLClient(t, cn)
		defer db.Close()

		for _, tc := range []struct {
			name             string
			before           string
			inside           string
			after            string
			copy             bool
			commentAfterCopy bool
		}{
			{name: "plain"},
			{name: "copy_before_lo", before: "algorithm=copy, add column extra int default 7", copy: true},
			{name: "rename_in_window", inside: "algorithm=inplace, rename column v to v2"},
			{name: "rename_after_hi", after: "algorithm=inplace, rename column v to v2"},
			{name: "copy_rename_in_window", inside: "algorithm=copy, rename column v to v2", copy: true},
			{name: "copy_comment_in_window", inside: "algorithm=copy, add column extra int default 7", copy: true, commentAfterCopy: true},
			{name: "copy_rename_after_hi", after: "algorithm=copy, rename column v to v2", copy: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := t.Context()
				ns := "pick_baseline_" + tc.name
				root, src, dst := ns+".root", ns+".src", ns+".dst"
				lo, hi := ns+"_lo", ns+"_hi"
				exec := func(query string) {
					t.Helper()
					_, err := db.ExecContext(ctx, query)
					require.NoError(t, err, query)
				}
				exec("create database " + ns)
				t.Cleanup(func() {
					_, err := db.Exec("drop database " + ns)
					assert.NoError(t, err)
				})
				createSnapshot := func(name string) {
					t.Helper()
					exec("create snapshot " + name + " for table " + ns + " src")
					t.Cleanup(func() {
						_, err := db.Exec("drop snapshot " + name)
						assert.NoError(t, err)
					})
				}
				assertRows := func(rel, col string, want [][2]int) {
					t.Helper()
					rows, err := db.QueryContext(ctx, "select k,"+col+" from "+rel+" order by k")
					require.NoError(t, err)
					defer rows.Close()
					var got [][2]int
					for rows.Next() {
						var row [2]int
						require.NoError(t, rows.Scan(&row[0], &row[1]))
						got = append(got, row)
					}
					require.NoError(t, rows.Err())
					require.Equal(t, want, got, rel)
				}
				alter := func(clause string, copiesTable bool) {
					t.Helper()
					const query = "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='src'"
					var oldID, newID uint64
					require.NoError(t, db.QueryRowContext(ctx, query, ns).Scan(&oldID))
					exec("alter table " + src + " " + clause)
					require.NoError(t, db.QueryRowContext(ctx, query, ns).Scan(&newID))
					if copiesTable {
						require.NotEqual(t, oldID, newID)
					} else {
						require.Equal(t, oldID, newID)
					}
				}

				exec("create table " + root + " (k int primary key, v int)")
				exec("insert into " + root + " values (1,10),(2,10),(3,10)")
				exec("data branch create table " + src + " from " + root)
				if tc.before != "" {
					alter(tc.before, tc.copy)
				}
				exec("update " + src + " set v=20")
				createSnapshot(lo)
				col := "v"
				if tc.inside != "" {
					alter(tc.inside, tc.copy)
					if tc.commentAfterCopy {
						alter("comment = 'updated'", false)
					} else {
						col = "v2"
					}
				}
				exec("update " + src + " set " + col + "=10 where k in (1,3)")
				exec("update " + src + " set " + col + "=25 where k=2")
				exec("update " + src + " set " + col + "=20 where k=2")
				createSnapshot(hi)
				if tc.after != "" {
					alter(tc.after, tc.copy)
				}
				exec("update " + root + " set v=30")
				exec("data branch create table " + dst + " from " + root)
				exec("update " + dst + " set v=20 where k=3")

				assertRows(src+" {snapshot = "+lo+"}", "v", [][2]int{{1, 20}, {2, 20}, {3, 20}})
				assertRows(src+" {snapshot = "+hi+"}", col, [][2]int{{1, 10}, {2, 20}, {3, 10}})
				pick := "data branch pick " + src + " into " + dst + " between snapshot " + lo + " and " + hi
				// k=1 returns to the source fork and must apply. k=2 returns to lo
				// and must preserve the destination. k=3 equals lo at the destination
				// but changed after the destination fork, so SKIP must preserve it.
				exec(pick + " when conflict skip")
				assertRows(dst, "v", [][2]int{{1, 10}, {2, 30}, {3, 20}})
				exec(pick + " when conflict accept")
				assertRows(dst, "v", [][2]int{{1, 10}, {2, 30}, {3, 10}})
			})
		}
	})
}

func TestDataBranchPickWindowRegressions(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		require.NotEqualValues(t, 6001, cn.GetServiceConfig().CN.Frontend.Port)
		db := recoverySQLClient(t, cn)
		defer db.Close()

		for _, scenario := range []string{"delete_insert", "reused_column_name"} {
			t.Run(scenario, func(t *testing.T) {
				ctx := t.Context()
				ns := "pick_window_" + scenario
				root, src, dst := ns+".root", ns+".src", ns+".dst"
				lo, hi := ns+"_lo", ns+"_hi"
				exec := func(query string) {
					t.Helper()
					_, err := db.ExecContext(ctx, query)
					require.NoError(t, err, query)
				}
				exec("create database " + ns)
				t.Cleanup(func() {
					_, err := db.Exec("drop database " + ns)
					assert.NoError(t, err)
				})
				snapshot := func(name string) {
					t.Helper()
					exec("create snapshot " + name + " for table " + ns + " src")
					t.Cleanup(func() {
						_, err := db.Exec("drop snapshot " + name)
						assert.NoError(t, err)
					})
				}
				pick := "data branch pick " + src + " into " + dst + " between snapshot " + lo + " and " + hi

				if scenario == "delete_insert" {
					exec("create table " + root + " (k int primary key, v int)")
					exec("insert into " + root + " values (2,10)")
					exec("data branch create table " + src + " from " + root)
					exec("insert into " + src + " values (1,20)")
					snapshot(lo)
					exec("delete from " + src + " where k in (1,2)")
					snapshot(hi)

					for _, policy := range []string{"fail", "skip", "accept"} {
						t.Run(policy, func(t *testing.T) {
							_, err := db.ExecContext(ctx, "data branch create table "+dst+" from "+root)
							require.NoError(t, err)
							t.Cleanup(func() {
								_, err := db.Exec("drop table " + dst)
								assert.NoError(t, err)
							})
							_, err = db.ExecContext(ctx, "insert into "+dst+" values (1,99)")
							require.NoError(t, err)
							_, err = db.ExecContext(ctx, pick+" when conflict "+policy)
							var want [][2]int
							if policy == "fail" {
								var sqlErr *mysql.MySQLError
								if assert.ErrorAs(t, err, &sqlErr) {
									assert.Equal(t, "conflict: src DELETE and dst INSERT on pk(1)", sqlErr.Message)
								}
								want = [][2]int{{1, 99}, {2, 10}}
							} else {
								require.NoError(t, err)
								if policy == "skip" {
									want = [][2]int{{1, 99}}
								}
							}
							rows, err := db.QueryContext(ctx, "select k,v from "+dst+" order by k")
							require.NoError(t, err)
							defer rows.Close()
							var got [][2]int
							for rows.Next() {
								var row [2]int
								require.NoError(t, rows.Scan(&row[0], &row[1]))
								got = append(got, row)
							}
							require.NoError(t, rows.Err())
							assert.Equal(t, want, got)
						})
					}
					return
				}

				exec("create table " + root + " (k int primary key, v int, w int)")
				exec("insert into " + root + " values (1,30,10)")
				exec("data branch create table " + src + " from " + root)
				exec("data branch create table " + dst + " from " + root)
				exec("update " + src + " set v=20 where k=1")
				exec("alter table " + src + " algorithm=inplace, rename column w to v2")
				snapshot(lo)
				exec("alter table " + src + " algorithm=inplace, rename column v2 to w")
				exec("alter table " + src + " algorithm=inplace, rename column v to v2")
				exec("update " + src + " set v2=10 where k=1")
				snapshot(hi)

				var v, w int
				require.NoError(t, db.QueryRowContext(ctx, "select v,v2 from "+src+" {snapshot = "+lo+"} where k=1").Scan(&v, &w))
				require.Equal(t, [2]int{20, 10}, [2]int{v, w})
				require.NoError(t, db.QueryRowContext(ctx, "select v2,w from "+src+" {snapshot = "+hi+"} where k=1").Scan(&v, &w))
				require.Equal(t, [2]int{10, 10}, [2]int{v, w})
				exec(pick + " when conflict accept")
				require.NoError(t, db.QueryRowContext(ctx, "select v,w from "+dst+" where k=1").Scan(&v, &w))
				require.Equal(t, [2]int{10, 10}, [2]int{v, w})
			})
		}
	})
}

func TestDataBranchPickWindowForkConflicts(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		require.NotEqualValues(t, 6001, cn.GetServiceConfig().CN.Frontend.Port)
		db := recoverySQLClient(t, cn)
		defer db.Close()

		for _, tc := range []struct {
			name      string
			delete    bool
			inherited bool
			copy      bool
		}{
			{name: "inherited_delete", delete: true, inherited: true},
			{name: "inherited_delete_after_copy", delete: true, inherited: true, copy: true},
			{name: "post_fork_delete", delete: true},
			{name: "source_update"},
			{name: "inherited_insert", inherited: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ns := "pick_fork_" + tc.name
				root, src := ns+".root", ns+".src"
				lo, hi := ns+"_lo", ns+"_hi"
				exec := func(query string) {
					t.Helper()
					_, err := db.ExecContext(t.Context(), query)
					require.NoError(t, err, query)
				}
				exec("create database " + ns)
				t.Cleanup(func() {
					_, err := db.Exec("drop database " + ns)
					assert.NoError(t, err)
				})
				snapshot := func(name string) {
					exec("create snapshot " + name + " for table " + ns + " src")
					t.Cleanup(func() {
						_, err := db.Exec("drop snapshot " + name)
						assert.NoError(t, err)
					})
				}
				exec("create table " + root + " (k int primary key, v int)")
				exec("data branch create table " + src + " from " + root)
				exec("insert into " + src + " values (1,20)")
				if tc.copy {
					const query = "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='root'"
					var before, after uint64
					require.NoError(t, db.QueryRowContext(t.Context(), query, ns).Scan(&before))
					exec("alter table " + root + " algorithm=copy, modify column v int")
					require.NoError(t, db.QueryRowContext(t.Context(), query, ns).Scan(&after))
					require.NotEqual(t, before, after)
				}
				if tc.inherited {
					exec("insert into " + root + " values (1,99)")
				}
				policies := []string{"fail", "skip", "accept"}
				if !tc.delete && !tc.inherited {
					policies = append(policies, "absent")
				}
				if !tc.delete && tc.inherited {
					policies = append(policies, "keys_accept")
				}
				for _, policy := range policies {
					dst := ns + ".dst_" + policy
					exec("data branch create table " + dst + " from " + root)
					if !tc.inherited && policy != "absent" {
						exec("insert into " + dst + " values (1,99)")
					}
				}
				snapshot(lo)
				if tc.delete {
					exec("delete from " + src + " where k=1")
				} else {
					exec("update " + src + " set v=30 where k=1")
				}
				snapshot(hi)
				for _, policy := range policies {
					t.Run(policy, func(t *testing.T) {
						dstName := "dst_" + policy
						dst := ns + "." + dstName
						pick := "data branch pick " + src + " into " + dst
						window := " between snapshot " + lo + " and " + hi
						assertValue := func(want string) {
							t.Helper()
							var got string
							require.NoError(t, db.QueryRowContext(t.Context(),
								"select coalesce(group_concat(concat(k,':',v) order by k), '') from "+dst).Scan(&got))
							assert.Equal(t, want, got)
						}
						assertConflict := func(err error) {
							t.Helper()
							var sqlErr *mysql.MySQLError
							op := "INSERT"
							if tc.delete {
								op = "DELETE"
							}
							if assert.ErrorAs(t, err, &sqlErr) {
								message := "conflict: src " + op + " and " + dstName + " INSERT on pk(1)"
								if !tc.delete {
									message += " with different values"
								}
								assert.Equal(t, message, sqlErr.Message)
							}
						}
						if policy == "keys_accept" {
							_, err := db.ExecContext(t.Context(), pick+" keys (1) when conflict accept")
							require.NoError(t, err)
							assertValue("1:30")
							return
						}
						if tc.inherited && !tc.delete && policy != "accept" {
							_, err := db.ExecContext(t.Context(), pick+" keys (1) when conflict "+policy)
							if policy == "fail" {
								assertConflict(err)
							} else {
								require.NoError(t, err)
							}
							assertValue("1:99")
						}
						if policy == "absent" {
							_, err := db.ExecContext(t.Context(), pick+window+" when conflict fail")
							require.NoError(t, err)
							assertValue("1:30")
							return
						}
						_, err := db.ExecContext(t.Context(), pick+window+" when conflict "+policy)
						switch {
						case tc.delete && tc.inherited:
							assert.NoError(t, err)
							assertValue("")
						case policy == "fail":
							assertConflict(err)
							assertValue("1:99")
						case policy == "skip":
							require.NoError(t, err)
							assertValue("1:99")
						case tc.delete:
							require.NoError(t, err)
							assertValue("")
						default:
							require.NoError(t, err)
							assertValue("1:30")
						}
					})
				}
			})
		}
	})
}

func TestDataBranchPickWindowReviewCases(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db := recoverySQLClient(t, cn)
		defer db.Close()

		run := func(t *testing.T, ns string, setup, changes []string, want string) {
			t.Helper()
			exec := func(sql string) {
				t.Helper()
				_, err := db.ExecContext(t.Context(), sql)
				require.NoError(t, err, sql)
			}
			exec("create database " + ns)
			t.Cleanup(func() { _, _ = db.Exec("drop database " + ns) })
			exec("use " + ns)
			for _, sql := range setup {
				exec(sql)
			}
			take := func(name string) {
				exec("create snapshot " + name + " for table " + ns + " src")
				t.Cleanup(func() { _, _ = db.Exec("drop snapshot " + name) })
			}
			lo, hi := ns+"_lo", ns+"_hi"
			take(lo)
			for _, sql := range changes {
				exec(sql)
			}
			take(hi)
			exec("data branch pick src into dst between snapshot " + lo + " and " + hi + " when conflict accept")
			var got string
			require.NoError(t, db.QueryRowContext(t.Context(),
				"select group_concat(concat(k,':',v) order by k) from dst").Scan(&got))
			require.Equal(t, want, got)
		}

		t.Run("independent_tables", func(t *testing.T) {
			run(t, "pick_review_independent", []string{
				"create table src(k int primary key,v int)", "insert into src values(1,20),(2,20)",
				"create table dst(k int primary key,v int)", "insert into dst values(1,99),(2,99)",
			}, []string{"update src set v=25", "update src set v=20 where k=1", "insert into src values(3,30)"}, "1:99,2:25,3:30")
		})
		t.Run("restore_and_update_in_same_batch", func(t *testing.T) {
			a, b := pickWindowSameShardKeys(t)
			run(t, "pick_review_mixed", []string{
				"create table root(k int primary key,v int)",
				fmt.Sprintf("insert into root values(%d,10),(%d,10)", a, b),
				"data branch create table src from root", "data branch create table dst from root",
				fmt.Sprintf("delete from dst where k=%d", a),
			}, []string{"update src set v=20"}, fmt.Sprintf("%d:20,%d:20", a, b))
		})
	})
}

func pickWindowSameShardKeys(t *testing.T) (int32, int32) {
	t.Helper()
	// Use the same map and key encoding as the production tombstone batches.
	hm, err := databranchutils.NewBranchHashmap()
	require.NoError(t, err)
	defer hm.Close()
	mp := mpool.MustNewZero()
	keys := vector.NewVec(types.T_int32.ToType(), mp)
	defer keys.Free(mp)
	for i := 1; i <= hm.ShardCount()+1; i++ {
		require.NoError(t, vector.AppendFixed(keys, int32(i), false, mp))
	}
	require.NoError(t, hm.PutByVectors([]*vector.Vector{keys}, []int{0}))
	var a, b int32
	require.NoError(t, hm.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		if b != 0 {
			return nil
		}
		var first int32
		return cursor.ForEach(func(_, row []byte) error {
			tuple, _, err := hm.DecodeRow(row)
			if err != nil {
				return err
			}
			k := tuple[0].(int32)
			if first == 0 {
				first = k
			} else if b == 0 {
				a, b = min(first, k), max(first, k)
			}
			return nil
		})
	}, 1))
	require.NotZero(t, b)
	return a, b
}

func TestDataBranchPickWindowCopyColumnSwap(t *testing.T) {
	t.Run("stable_coordinates", func(t *testing.T) {
		testDataBranchPickWindowCopyColumnSwap(t, "add column e1 int", "add column e2 int")
	})
	t.Run("shifted_coordinates", func(t *testing.T) {
		testDataBranchPickWindowCopyColumnSwap(t, "add column e1 varchar(8)",
			"add column e2 int after k, add column e3 int after e2")
	})
	t.Run("reorder_masked_by_swap", func(t *testing.T) {
		testDataBranchPickWindowCopyColumnSwap(t, "add column e1 int", "modify column a int after k")
	})
}

func testDataBranchPickWindowCopyColumnSwap(t *testing.T, firstCopy, secondCopy string) {
	t.Helper()
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		require.NotEqualValues(t, 6001, cn.GetServiceConfig().CN.Frontend.Port)
		db := recoverySQLClient(t, cn)
		defer db.Close()

		const ns = "pick_copy_swap"
		const src = ns + ".src"
		const dst = ns + ".dst"
		exec := func(query string) {
			t.Helper()
			_, err := db.ExecContext(t.Context(), query)
			require.NoError(t, err, query)
		}
		cleanup := func(query string) {
			t.Helper()
			_, err := db.Exec(query)
			assert.NoError(t, err, query)
		}
		exec("create database " + ns)
		defer cleanup("drop database " + ns)
		tableID := func() uint64 {
			var id uint64
			require.NoError(t, db.QueryRowContext(t.Context(),
				"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='src'", ns).Scan(&id))
			return id
		}
		swap := func() {
			exec("alter table " + src + " algorithm=inplace, rename column a to tmp")
			exec("alter table " + src + " algorithm=inplace, rename column b to a")
			exec("alter table " + src + " algorithm=inplace, rename column tmp to b")
		}
		values := func(rel string) [2]int {
			t.Helper()
			var got [2]int
			require.NoError(t, db.QueryRowContext(t.Context(),
				"select a,b from "+rel+" where k=1").Scan(&got[0], &got[1]))
			return got
		}

		exec("create table " + ns + ".root (k int primary key, a int, b int)")
		exec("insert into " + ns + ".root values (1,40,50)")
		exec("data branch create table " + dst + " from " + ns + ".root")
		exec("data branch create table " + src + " from " + ns + ".root")
		p0 := tableID()
		exec("alter table " + src + " algorithm=copy, " + firstCopy)
		p1 := tableID()
		require.NotEqual(t, p0, p1)
		exec("update " + src + " set a=20,b=10 where k=1")
		swap()
		require.Equal(t, p1, tableID())
		exec("create snapshot pick_swap_lo for table " + ns + " src")
		defer cleanup("drop snapshot pick_swap_lo")
		exec("alter table " + src + " algorithm=copy, " + secondCopy)
		p2 := tableID()
		require.NotEqual(t, p1, p2)
		swap()
		require.Equal(t, p2, tableID())
		require.Equal(t, [2]int{20, 10}, values(src))
		exec("update " + src + " set a=10,b=20 where k=1")
		exec("create snapshot pick_swap_hi for table " + ns + " src")
		defer cleanup("drop snapshot pick_swap_hi")

		require.Equal(t, [2]int{10, 20}, values(src+" {snapshot = pick_swap_lo}"))
		require.Equal(t, [2]int{10, 20}, values(src+" {snapshot = pick_swap_hi}"))
		_, err = db.ExecContext(t.Context(), "data branch pick "+src+" into "+dst+
			" between snapshot pick_swap_lo and pick_swap_hi when conflict accept")
		var sqlErr *mysql.MySQLError
		if assert.ErrorAs(t, err, &sqlErr) {
			assert.Regexp(t, `DATA BRANCH PICK has ambiguous COPY ALTER column lineage for "[^"]+" at the window start`, sqlErr.Message)
		}
		require.Equal(t, [2]int{40, 50}, values(dst))
	})
}
