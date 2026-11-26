// Copyright 2021 - 2024 Matrix Origin
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

package partition

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestPartitionedOrderedODKU(t *testing.T) {
	require.NoError(t, runPartitionClusterTest(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		name := strings.ToLower(testutils.GetDatabaseName(t))
		testutils.CreateTestDatabase(t, name, cn)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/%s", cn.GetServiceConfig().CN.Frontend.Port, name))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		exec := func(t *testing.T, stmt string) {
			t.Helper()
			_, err := db.ExecContext(ctx, stmt)
			require.NoError(t, err, stmt)
		}
		check := func(t *testing.T, reader testutils.SQLTextQueryer, stmt, want string) {
			t.Helper()
			got, err := testutils.QueryText(ctx, reader, stmt)
			require.NoError(t, err, stmt)
			require.Equal(t, want, got, stmt)
		}

		for _, tc := range []struct{ name, seed string }{
			{"key_insert", ""},
			{"key_update", "insert into key_update(id,a) values(1,10)"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec(t, "create table "+tc.name+"(id int primary key, a int, g int generated always as (a+1) stored, unique key ug(id,g)) partition by key(id) partitions 2")
				if tc.seed != "" {
					exec(t, tc.seed)
				}
				exec(t, "insert into "+tc.name+"(id,a) values(1,20) on duplicate key update a=values(a)")
				check(t, db, "select concat_ws('|',id,a,g) from "+tc.name, "1|20|21")
				check(t, db, "select concat_ws('|',id,g) from "+tc.name+" force index(ug) where id=1 and g=21", "1|21")
			})
		}

		t.Run("composite_pk", func(t *testing.T) {
			exec(t, "create table ranged(id int, a int, p int, g int generated always as (a+1) stored, primary key(id,p), unique key ug(p,g)) partition by range columns(p) (partition p0 values less than (10), partition p1 values less than (maxvalue))")
			exec(t, "insert into ranged(id,a,p) values(1,10,1),(2,20,11),(3,40,1)")
			// The first action finds the old owner through the local unique index.
			// The second finds the owner in the other partition by its packed PK.
			exec(t, "insert into ranged(id,a,p) values(7,10,1),(2,25,11) on duplicate key update a=values(a)+1")
			const baseRows = "select concat_ws('|',id,a,p,g) from ranged order by id"
			const want = "1|11|1|12\n2|26|11|27\n3|40|1|41"
			check(t, db, baseRows, want)
			check(t, db, "select concat_ws('|',id,g) from ranged force index(ug) where p=1 and g in (11,12,41) order by id", "1|12\n3|41")
			check(t, db, "select concat_ws('|',id,g) from ranged force index(ug) where p=11 and g in (21,27)", "2|27")

			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			_, err = tx.ExecContext(ctx, "insert into ranged(id,a,p) values (9,90,11)")
			require.NoError(t, err)
			_, err = tx.ExecContext(ctx, "insert into ranged(id,a,p) values (9,91,11) on duplicate key update a=values(a)")
			require.NoError(t, err)
			check(t, tx, "select concat_ws('|',id,g) from ranged force index(ug) where p=11 and g in (91,92)", "9|92")
			require.NoError(t, tx.Rollback())

			_, err = db.ExecContext(ctx, "insert into ranged(id,a,p) values (8,80,11),(1,39,1) on duplicate key update a=values(a)+1")
			var duplicate *mysqlDriver.MySQLError
			require.ErrorAs(t, err, &duplicate)
			require.Equal(t, uint16(1062), duplicate.Number)
			check(t, db, baseRows, want)
			check(t, db, "select concat_ws('|',id,g) from ranged force index(ug) where p=11 and g=81", "")

			// Commit a competing UPDATE after the ODKU has captured prior data.
			// The existing logical row lock must make it retry against that update.
			peer, err := c.GetCNService(1)
			require.NoError(t, err)
			testutils.WaitTableCreated(t, name, "ranged", peer)
			writer, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/%s", peer.GetServiceConfig().CN.Frontend.Port, name))
			require.NoError(t, err)
			defer writer.Close()
			var tableID uint64
			require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='ranged'", name).Scan(&tableID))
			update, err := writer.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer update.Rollback()
			_, err = update.ExecContext(ctx, "update ranged set a=a+1 where id=2 and p=11")
			require.NoError(t, err)
			arrived := make(chan struct{}, 1)
			testingContext := moruntime.MustGetTestingContext(cn.ServiceID())
			beforeLock := testingContext.GetBeforeLockFunc()
			defer testingContext.SetBeforeLockFunc(beforeLock)
			testingContext.SetBeforeLockFunc(func(id []byte, table uint64) {
				beforeLock(id, table)
				if table == tableID {
					select {
					case arrived <- struct{}{}:
					default:
					}
				}
			})
			done := make(chan error, 1)
			go func() {
				_, err := db.ExecContext(ctx, "insert into ranged(id,a,p) values (2,0,11) on duplicate key update a=a+1")
				done <- err
			}()
			select {
			case <-arrived:
			case err := <-done:
				t.Fatalf("ODKU did not reach the logical table lock: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.NoError(t, update.Commit())
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			check(t, db, "select concat_ws('|',id,a,g) from ranged force index(ug) where p=11 and g in (27,28,29)", "2|28|29")
		})
	}))
}

func TestInsertAndDeleteWithIndex(
	t *testing.T,
) {
	runPartitionClusterTest(
		t,
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)
			eng := cn.RawService().(cnservice.Service).GetEngine()
			exec := testutils.GetSQLExecutor(cn)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabase(t, db, cn)

			sql := fmt.Sprintf(
				"create table %s (c int primary key, d int, unique key(d)) partition by list (c) (partition p1 values in (1,2), partition p2 values in (3,4))",
				t.Name(),
			)

			testutils.ExecSQL(
				t,
				db,
				cn,
				sql,
			)

			sql = fmt.Sprintf("insert into %s values (1,1), (2,2), (3,3), (4,4)", t.Name())
			testutils.ExecSQL(
				t,
				db,
				cn,
				sql,
			)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			exec.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					metadata := getMetadata(
						t,
						0,
						db,
						t.Name(),
						cn,
					)

					for _, p := range metadata.Partitions {
						_, _, r, err := eng.GetRelationById(
							defines.AttachAccountId(ctx, 0),
							txn.Txn(),
							p.PartitionID,
						)
						require.NoError(t, err)

						_, _, indexR, err := eng.GetRelationById(
							defines.AttachAccountId(ctx, 0),
							txn.Txn(),
							r.GetExtraInfo().IndexTables[0],
						)
						require.NoError(t, err)

						txn.Use(db)
						rs, err := txn.Exec(
							fmt.Sprintf("select count(1) from `%s`", indexR.GetTableName()),
							executor.StatementOption{},
						)
						require.NoError(t, err)
						n := int64(0)
						rs.ReadRows(
							func(rows int, cols []*vector.Vector) bool {
								n += executor.GetFixedRows[int64](cols[0])[0]
								return true
							},
						)
						rs.Close()
						require.Equal(t, int64(2), n)
					}

					return nil
				},
				executor.Options{},
			)
		},
	)
}

func TestSelectWithUniqueIndex(
	t *testing.T,
) {
	runPartitionClusterTest(
		t,
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)
			exec := testutils.GetSQLExecutor(cn)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabase(t, db, cn)

			sql := fmt.Sprintf(
				"create table %s (c int primary key, d int, unique key(d)) partition by list (c) (partition p1 values in (1,2), partition p2 values in (3,4))",
				t.Name(),
			)

			testutils.ExecSQL(
				t,
				db,
				cn,
				sql,
			)

			sql = fmt.Sprintf("insert into %s values (1,1), (2,2), (3,3), (4,4)", t.Name())
			testutils.ExecSQL(
				t,
				db,
				cn,
				sql,
			)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			for i := 0; i < 4; i++ {
				res, err := exec.Exec(
					ctx,
					fmt.Sprintf("select c from %s where d = %d", t.Name(), i+1),
					executor.Options{}.WithDatabase(db),
				)
				require.NoError(t, err)
				n := 0
				v := int32(0)
				res.ReadRows(
					func(rows int, cols []*vector.Vector) bool {
						n += rows
						v = executor.GetFixedRows[int32](cols[0])[0]
						return true
					},
				)
				res.Close()
				require.Equal(t, 1, n)
				require.Equal(t, int32(i+1), v)
			}
		},
	)
}

func TestUniqueIndexCanWork(
	t *testing.T,
) {
	runPartitionClusterTest(
		t,
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)
			exec := testutils.GetSQLExecutor(cn)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabase(t, db, cn)

			sql := fmt.Sprintf(
				"create table %s (c int primary key, d int, unique key(d)) partition by list (c) (partition p1 values in (1,2), partition p2 values in (3,4))",
				t.Name(),
			)

			testutils.ExecSQL(
				t,
				db,
				cn,
				sql,
			)

			sql = fmt.Sprintf("insert into %s values (1,1)", t.Name())
			testutils.ExecSQL(
				t,
				db,
				cn,
				sql,
			)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			sql = fmt.Sprintf("insert into %s values (2,1)", t.Name())
			_, err = exec.Exec(
				ctx,
				sql,
				executor.Options{}.WithDatabase(db),
			)
			require.Error(t, err)

			sql = fmt.Sprintf("insert into %s values (3,1)", t.Name())
			_, err = exec.Exec(
				ctx,
				sql,
				executor.Options{}.WithDatabase(db),
			)
			require.Error(t, err)
		},
	)
}
