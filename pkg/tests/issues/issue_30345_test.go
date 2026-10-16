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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue30345LoadIgnoreCheckConstraints(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		open := func(index int) *sql.DB {
			cn, err := c.GetCNService(index)
			require.NoError(t, err)
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			return db
		}
		db := open(0)
		defer db.Close()
		peer := open(1)
		defer peer.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(t *testing.T, query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		const database = "issue30345_load_check"
		exec(t, "create database "+database)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			_, err := conn.ExecContext(cleanup, "drop database "+database)
			require.NoError(t, err)
		}()
		csv := filepath.Join(t.TempDir(), "checks.csv")
		require.NoError(t, os.WriteFile(csv, []byte("1,k_bad,1,-1\n2,k_ok,1,5\n3,k_tail,1,8\n4,k_null,1,\\N\n"), 0o600))
		mysql.RegisterLocalFile(csv)
		defer mysql.DeregisterLocalFile(csv)
		sharedKeyCSV := filepath.Join(filepath.Dir(csv), "shared_key.csv")
		require.NoError(t, os.WriteFile(sharedKeyCSV, []byte("1,k_bad,1,-1\n1,k_bad,1,6\n"), 0o600))
		mysql.RegisterLocalFile(sharedKeyCSV)
		defer mysql.DeregisterLocalFile(sharedKeyCSV)
		read := func(t *testing.T, table, generated string) [][3]int {
			t.Helper()
			rows, err := peer.QueryContext(ctx, "select id,coalesce(v,999),coalesce("+generated+",999) from "+table+" order by id")
			require.NoError(t, err)
			defer rows.Close()
			var result [][3]int
			for rows.Next() {
				var row [3]int
				require.NoError(t, rows.Scan(&row[0], &row[1], &row[2]))
				result = append(result, row)
			}
			require.NoError(t, rows.Err())
			return result
		}
		for _, local := range []bool{false, true} {
			for index, tc := range []struct {
				name, definition, generated string
				warnings                    int
				want                        [][3]int
			}{
				{"single", "unique(k), constraint chk_pos check(v>0)", "v*2", 1, [][3]int{{2, 5, 10}, {3, 8, 16}, {4, 999, 999}}},
				{"multiple", "unique(k), constraint chk_pos check(v>0), constraint chk_upper check(v<7)", "v*2", 2, [][3]int{{2, 5, 10}, {4, 999, 999}}},
				{"generated_composite", "g int generated always as (v*2) stored, unique(k,tag), constraint chk_gen check(g>0)", "g", 1, [][3]int{{2, 5, 10}, {3, 8, 16}, {4, 999, 999}}},
			} {
				t.Run(fmt.Sprintf("local_%t/%s", local, tc.name), func(t *testing.T) {
					table := fmt.Sprintf("%s.t_%t_%d", database, local, index)
					exec(t, "create table "+table+"(id int primary key,k varchar(20),tag int,v int,"+tc.definition+")")
					prefix := "load data "
					if local {
						prefix += "local "
					}
					prefix += "infile '" + csv + "' "
					suffix := "into table " + table + " fields terminated by ',' (id,k,tag,v)"
					exec(t, prefix+"ignore "+suffix)
					requireWarningCodes(t, ctx, conn, map[uint16]int{3819: tc.warnings})
					require.Equal(t, tc.want, read(t, table, tc.generated), "second CN must see only CHECK-valid committed rows")
					// A rejected row must not reserve its primary or unique key.
					exec(t, "insert into "+table+"(id,k,tag,v) values(1,'k_bad',1,6)")
					want := append([][3]int{{1, 6, 12}}, tc.want...)
					require.Equal(t, want, read(t, table, tc.generated))
					exec(t, "truncate table "+table)
					_, err := conn.ExecContext(ctx, prefix+suffix)
					var checkError *mysql.MySQLError
					require.ErrorAs(t, err, &checkError)
					require.Equal(t, uint16(3819), checkError.Number)
					require.Empty(t, read(t, table, tc.generated), "ordinary LOAD must roll back all rows")
					exec(t, "insert ignore into "+table+"(id,k,tag,v) values(1,'k_bad',1,-1),(2,'k_ok',1,5)")
					requireWarningCodes(t, ctx, conn, map[uint16]int{3819: 1})
					require.Equal(t, [][3]int{{2, 5, 10}}, read(t, table, tc.generated))
					exec(t, "truncate table "+table)
					// The invalid candidate must be filtered before dedup sees a
					// later valid row with the same primary and unique keys.
					keyPrefix := "load data "
					if local {
						keyPrefix += "local "
					}
					keyPrefix += "infile '" + sharedKeyCSV + "' ignore "
					exec(t, keyPrefix+suffix)
					requireWarningCodes(t, ctx, conn, map[uint16]int{3819: 1})
					require.Equal(t, [][3]int{{1, 6, 12}}, read(t, table, tc.generated))
				})
			}
		}
		t.Run("surviving rows keep duplicate and foreign key policies", func(t *testing.T) {
			validCSV := filepath.Join(filepath.Dir(csv), "valid_checks.csv")
			require.NoError(t, os.WriteFile(validCSV, []byte("2,k_input,1,6\n3,k_tail,1,8\n"), 0o600))
			exec(t, "create table "+database+".dup(id int primary key,k varchar(20),tag int,v int,check(v>0))")
			exec(t, "insert into "+database+".dup values(2,'seed',1,5)")
			// Existing LOAD primary-key conflicts are tracked by #25470. CHECK
			// filtering must not enable INSERT IGNORE's duplicate arbitration.
			exec(t, "load data infile '"+validCSV+"' ignore into table "+database+".dup fields terminated by ','")
			require.ElementsMatch(t, [][3]int{{2, 5, 10}, {2, 6, 12}, {3, 8, 16}}, read(t, database+".dup", "v*2"))
			exec(t, "create table "+database+".parent(id int primary key)")
			exec(t, "create table "+database+".child(id int primary key,k varchar(20),tag int,v int,check(v>0),foreign key(tag) references "+database+".parent(id))")
			_, err := conn.ExecContext(ctx, "load data infile '"+validCSV+"' ignore into table "+database+".child fields terminated by ','")
			require.ErrorContains(t, err, "foreign key constraint fails")
			require.Empty(t, read(t, database+".child", "v*2"))
		})
	})
}
