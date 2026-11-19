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

package dml

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestODKUSequentialSourceOrder(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		db := openRetestSQLDB(t, c)
		defer db.Close()
		name := strings.ToLower(testutils.GetDatabaseName(t))
		defer cleanupTestDatabases(t, db, name)
		execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", name))
		execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", name))
		for _, tc := range []struct {
			name, seed, input string
			want              [][]string
		}{
			{"existing_target", "(1,10,100,0),(4,40,400,0),(5,50,500,0)", "(2,10,200,1),(3,30,100,2)", [][]string{{"1", "10", "100", "2"}, {"4", "40", "400", "0"}, {"5", "50", "500", "0"}}},
			{"constraint_priority", "(1,1,10,0),(4,4,40,0),(5,5,50,0)", "(100,20,200,1),(101,20,10,2)", [][]string{{"1", "1", "10", "0"}, {"4", "4", "40", "0"}, {"5", "5", "50", "0"}, {"100", "20", "200", "2"}}},
			{"unused_update_key", "(1,10,100,0)", "(2,10,200,1),(3,30,200,2)", [][]string{{"1", "10", "100", "1"}, {"3", "30", "200", "2"}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				for _, table := range []string{"batch_t", "sequential_t"} {
					execSQLDB(t, ctx, db, "create table "+table+"(id int primary key,u1 int,u2 int,payload int,unique(u1),unique(u2))")
					defer execSQLDB(t, ctx, db, "drop table "+table)
					execSQLDB(t, ctx, db, "insert into "+table+" values "+tc.seed)
				}
				execSQLDB(t, ctx, db, "insert into batch_t values "+tc.input+" on duplicate key update payload=values(payload)")
				// Each witness has two actions. The single-row control is independent of
				// lookup-join scheduling and of the statement-local key publication path.
				boundary := 0
				for i := range tc.input {
					if tc.input[i] == ')' {
						boundary = i + 1
						break
					}
				}
				for _, input := range []string{tc.input[:boundary], tc.input[boundary+1:]} {
					execSQLDB(t, ctx, db, "insert into sequential_t values "+input+" on duplicate key update payload=values(payload)")
				}
				for _, table := range []string{"batch_t", "sequential_t"} {
					require.Equal(t, tc.want, queryStringRows(t, ctx, db, "select id,u1,u2,payload from "+table+" order by id"))
					// Physical index contents must agree with every retained logical key.
					indexes := queryStringRows(t, ctx, db, "select distinct index_table_name,column_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='"+name+"' and relname='"+table+"') and type='UNIQUE'")
					require.Len(t, indexes, 2)
					for _, index := range indexes {
						require.Equal(t, queryStringRows(t, ctx, db, "select `"+index[1]+"`,id from "+table+" order by 1,2"), queryStringRows(t, ctx, db, "select __mo_index_idx_col,__mo_index_pri_col from `"+index[0]+"` order by 1,2"))
					}
				}
			})
		}
		for _, tc := range []struct {
			name, ddl, seed, input, query string
			want                          [][]string
		}{
			{"fake_primary", "create table target_t(u1 int,u2 int,payload int,unique(u1),unique(u2))", "(10,100,0),(40,400,0),(50,500,0)", "(10,200,1),(30,100,2)", "select u1,u2,payload from target_t order by u1", [][]string{{"10", "100", "2"}, {"40", "400", "0"}, {"50", "500", "0"}}},
			{"composite_primary", "create table target_t(a int,b int,u1 int,u2 int,payload int,primary key(a,b),unique(u1),unique(u2))", "(1,1,10,100,0),(4,4,40,400,0),(5,5,50,500,0)", "(2,2,10,200,1),(3,3,30,100,2)", "select a,b,payload from target_t order by a,b", [][]string{{"1", "1", "2"}, {"4", "4", "0"}, {"5", "5", "0"}}},
			{"prefix_unique", "create table target_t(u1 int,note varchar(20),payload int,unique(u1),unique(note(4)))", "(10,'abcd-old',0),(40,'efgh-old',0),(50,'ijkl-old',0)", "(10,'mnop-new',1),(30,'abcd-new',2)", "select u1,note,payload from target_t order by u1", [][]string{{"10", "abcd-old", "2"}, {"40", "efgh-old", "0"}, {"50", "ijkl-old", "0"}}},
			{"enum_unique", "create table target_t(u1 int,color enum('red','blue','green'),payload int,unique(u1),unique(color))", "(10,'red',0),(40,'blue',0),(50,'green',0)", "(10,'blue',1),(30,'red',2)", "select u1,color,payload from target_t order by u1", [][]string{{"10", "red", "2"}, {"40", "blue", "0"}, {"50", "green", "0"}}},
			{"null_keys", "create table target_t(u1 int,u2 int,payload int,unique(u1),unique(u2))", "(10,100,0),(40,400,0),(50,500,0)", "(10,null,1),(null,100,2)", "select u1,u2,payload from target_t order by u1", [][]string{{"10", "100", "2"}, {"40", "400", "0"}, {"50", "500", "0"}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				execSQLDB(t, ctx, db, tc.ddl)
				defer execSQLDB(t, ctx, db, "drop table target_t")
				execSQLDB(t, ctx, db, "insert into target_t values "+tc.seed)
				execSQLDB(t, ctx, db, "insert into target_t values "+tc.input+" on duplicate key update payload=values(payload)")
				require.Equal(t, tc.want, queryStringRows(t, ctx, db, tc.query))
			})
		}

		t.Run("bounded_workloads", func(t *testing.T) {
			for _, workload := range []string{"no_conflict", "distinct_conflict", "hot_key"} {
				func() {
					execSQLDB(t, ctx, db, "create table perf_t(id int primary key,u1 int,u2 int,payload int,unique(u1),unique(u2))")
					defer execSQLDB(t, ctx, db, "drop table perf_t")
					u1, u2 := "result*10", "result*100"
					wantCount := "128"
					if workload == "distinct_conflict" {
						execSQLDB(t, ctx, db, "insert into perf_t select result,result*10,result*100,0 from generate_series(1,128) g")
					}
					if workload == "hot_key" {
						execSQLDB(t, ctx, db, "insert into perf_t values(1,10,100,0)")
						u1, u2 = "10", "10000+result"
						wantCount = "1"
					}
					start := time.Now()
					execSQLDB(t, ctx, db, "insert into perf_t select 1000+result,"+u1+","+u2+",result from generate_series(1,128) g order by result on duplicate key update payload=values(payload)")
					t.Logf("bounded sequential %s: 128 actions in %s", workload, time.Since(start))
					require.Equal(t, [][]string{{wantCount}}, queryStringRows(t, ctx, db, "select count(*) from perf_t"))
					if workload == "hot_key" {
						require.Equal(t, [][]string{{"128"}}, queryStringRows(t, ctx, db, "select payload from perf_t where id=1"))
					}
				}()
			}
		})

		for _, tc := range []struct{ name, tail, want string }{
			{"select_ascending", "order by seq asc", "2"},
			{"select_descending", "order by seq desc", "1"},
			{"select_limit", "order by seq asc limit 1", "1"},
			{"select_empty", "where seq<0 order by seq asc", "0"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				execSQLDB(t, ctx, db, "create table select_target(id int primary key,u1 int,u2 int,payload int,unique(u1),unique(u2))")
				defer execSQLDB(t, ctx, db, "drop table select_target")
				execSQLDB(t, ctx, db, "create table select_source(seq int,id int,u1 int,u2 int,payload int)")
				defer execSQLDB(t, ctx, db, "drop table select_source")
				execSQLDB(t, ctx, db, "insert into select_target values(1,10,100,0),(4,40,400,0),(5,50,500,0)")
				execSQLDB(t, ctx, db, "insert into select_source values(1,2,10,200,1),(2,3,30,100,2)")
				execSQLDB(t, ctx, db, "insert into select_target select id,u1,u2,payload from select_source "+tc.tail+" on duplicate key update payload=values(payload)")
				require.Equal(t, [][]string{{"1", "10", "100", tc.want}, {"4", "40", "400", "0"}, {"5", "50", "500", "0"}}, queryStringRows(t, ctx, db, "select id,u1,u2,payload from select_target order by id"))
			})
		}
		t.Run("ordinary_row_number_and_replace", func(t *testing.T) {
			require.Equal(t, [][]string{{"8193", "8193", "1", "8193", "33566721"}}, queryStringRows(t, ctx, db, "select count(*),count(distinct rn),min(rn),max(rn),sum(rn) from (select row_number() over() as rn from generate_series(1,8193) g) s"))
			execSQLDB(t, ctx, db, "create table replace_control(id int primary key,u int unique,payload int)")
			defer execSQLDB(t, ctx, db, "drop table replace_control")
			execSQLDB(t, ctx, db, "insert into replace_control values(1,10,0),(2,20,0)")
			execSQLDB(t, ctx, db, "replace into replace_control values(3,10,1),(4,30,2)")
			require.Equal(t, [][]string{{"2", "20", "0"}, {"3", "10", "1"}, {"4", "30", "2"}}, queryStringRows(t, ctx, db, "select id,u,payload from replace_control order by id"))
		})

		t.Run("cross_batch", func(t *testing.T) {
			execSQLDB(t, ctx, db, "create table cross_batch(id int primary key,u1 int,u2 int,payload int,unique(u1),unique(u2))")
			execSQLDB(t, ctx, db, "insert into cross_batch values (1,10,100,0),(4,40,400,0),(5,50,500,0)")
			// 8193 crosses the executor's 8192-row batch boundary. Only the two end
			// actions conflict; the intervening inputs have distinct keys.
			execSQLDB(t, ctx, db, "insert into cross_batch select 100+result,if(result=1,10,1000+result),if(result=8193,100,10000+result),result from generate_series(1,8193) g order by result on duplicate key update payload=values(payload)")
			require.Equal(t, [][]string{{"8193"}}, queryStringRows(t, ctx, db, "select payload from cross_batch where id=1"))
			require.Equal(t, [][]string{{"8194"}}, queryStringRows(t, ctx, db, "select count(*) from cross_batch"))
		})
	})
}
