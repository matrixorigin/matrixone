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
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/stretchr/testify/require"
)

func TestLoadRejectsDuplicatePrimaryKeys(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const database = "load_duplicate_primary_keys"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			execSQLRequire(t, cleanup, db, "drop database "+database)
		}()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(t *testing.T, query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		// Prove enforcement independently of optional TN full PK checking.
		exec(t, "set session mo_pk_check_by_dn=off")
		exec(t, "use "+database)
		requireDuplicate := func(t *testing.T, query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			var duplicate *mysql.MySQLError
			require.ErrorAs(t, err, &duplicate)
			require.Equal(t, uint16(1062), duplicate.Number)
		}
		count := func(t *testing.T, table string) int {
			t.Helper()
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+table).Scan(&count))
			return count
		}
		inputDir := t.TempDir()
		writeInput := func(t *testing.T, name, data string) string {
			t.Helper()
			path := filepath.Join(inputDir, name)
			require.NoError(t, os.WriteFile(path, []byte(data), 0600))
			return path
		}
		load := func(path, table string) string {
			return fmt.Sprintf("load data infile '%s' into table %s fields terminated by ','", path, table)
		}
		exec(t, "create table t(id bigint primary key, name varchar(256))")
		t.Run("input duplicate and rollback", func(t *testing.T) {
			path := writeInput(t, "duplicate.csv", "1,a\n1,b\n")
			requireDuplicate(t, load(path, "t"))
			require.Zero(t, count(t, "t"))
			requireDuplicate(t, "insert into t values(1,'a'),(1,'b')")
			require.Zero(t, count(t, "t"))
		})
		t.Run("existing row collision and rollback", func(t *testing.T) {
			exec(t, "insert into t values(1,'old')")
			path := writeInput(t, "existing.csv", "2,new\n1,conflict\n3,new\n")
			requireDuplicate(t, load(path, "t"))
			require.Equal(t, 1, count(t, "t"))
			var payload string
			require.NoError(t, conn.QueryRowContext(ctx, "select name from t where id=1").Scan(&payload))
			require.Equal(t, "old", payload)
			exec(t, load(writeInput(t, "valid.csv", "2,new\n3,new\n"), "t"))
			require.Equal(t, 3, count(t, "t"), "connection and table remain reusable after rejection")
		})
		t.Run("composite tuple identity", func(t *testing.T) {
			exec(t, "create table composite(a int,b int,primary key(a,b))")
			requireDuplicate(t, load(writeInput(t, "composite_dup.csv", "1,1\n1,1\n"), "composite"))
			require.Zero(t, count(t, "composite"))
			exec(t, load(writeInput(t, "composite_valid.csv", "1,1\n1,2\n"), "composite"))
			require.Equal(t, 2, count(t, "composite"))
		})
		t.Run("shared prepared image and index rollback", func(t *testing.T) {
			exec(t, "create table indexed(id int auto_increment primary key, u int unique key, v int, key idx_v(v))")
			path := writeInput(t, "indexed.csv", "10,100\n20,200\n")
			exec(t, load(path, "indexed")+" (u,v)")
			var image string
			require.NoError(t, conn.QueryRowContext(ctx,
				"select group_concat(concat(id,':',u,':',v) order by id) from indexed").Scan(&image))
			require.Equal(t, "1:10:100,2:20:200", image, "auto-increment image is evaluated once for validation and writes")
			requireDuplicate(t, load(writeInput(t, "indexed_conflict.csv", "3,30,300\n1,40,400\n"), "indexed"))
			require.Equal(t, 2, count(t, "indexed"))
			var visible int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from indexed force index(idx_v) where v>=300").Scan(&visible))
			require.Zero(t, visible, "rejected LOAD leaves no secondary-index rows")
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from indexed where u in (30,40)").Scan(&visible))
			require.Zero(t, visible, "rejected LOAD leaves no unique-index rows")
			exec(t, load(writeInput(t, "indexed_reuse.csv", "3,30,300\n"), "indexed"))
			require.Equal(t, 3, count(t, "indexed"))
		})
		t.Run("foreign key fallback", func(t *testing.T) {
			exec(t, "create table parent(id int primary key)")
			exec(t, "insert into parent values(1)")
			exec(t, "create table child(id int primary key,p int,foreign key(p) references parent(id))")
			requireDuplicate(t, load(writeInput(t, "child.csv", "1,1\n1,1\n"), "child"))
			require.Zero(t, count(t, "child"))
			exec(t, "insert into child values(1,1)")
			requireDuplicate(t, load(writeInput(t, "child_existing.csv", "2,1\n1,1\n"), "child"))
			require.Equal(t, 1, count(t, "child"))
		})
		t.Run("parallel files", func(t *testing.T) {
			exec(t, "create table files(id int primary key,name varchar(20))")
			writeInput(t, "part0.csv", "1,a\n")
			writeInput(t, "part1.csv", "1,b\n")
			requireDuplicate(t, load(filepath.Join(inputDir, "part*.csv"), "files")+" parallel 'true'")
			require.Zero(t, count(t, "files"))
		})
		t.Run("cross batch bulk write", func(t *testing.T) {
			exec(t, "create table bulk(id int primary key,name varchar(256))")
			var data strings.Builder
			// Minimum cardinality crossing the external reader's batch boundary;
			// 128-byte payload also crosses the 1MiB bulk-write threshold.
			for i := 1; i <= external.OneBatchMaxRow; i++ {
				fmt.Fprintf(&data, "%d,%s\n", i, strings.Repeat("x", 128))
			}
			data.WriteString("1,duplicate\n")
			path := writeInput(t, "bulk.csv", data.String())
			for _, parallel := range []string{"false", "true"} {
				t.Run("parallel="+parallel, func(t *testing.T) {
					requireDuplicate(t, load(path, "bulk")+" parallel '"+parallel+"'")
					require.Zero(t, count(t, "bulk"))
				})
			}
		})
		t.Run("explicit transaction rollback", func(t *testing.T) {
			exec(t, "create table transactional(id int primary key,name varchar(20))")
			exec(t, "insert into transactional values(1,'seed')")
			exec(t, "begin")
			exec(t, "insert into transactional values(2,'prior')")
			defer func() { _, _ = conn.ExecContext(ctx, "rollback") }()
			requireDuplicate(t, load(writeInput(t, "txn.csv", "4,a\n4,b\n"), "transactional"))
			// Observe through a different session, independent of error rollback policy.
			var visible int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+database+".transactional").Scan(&visible))
			require.Equal(t, 1, visible)
			exec(t, "rollback")
			require.Equal(t, 1, count(t, "transactional"))
		})
	})
}
