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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestPartitionUniqueIndex(t *testing.T) {
	const (
		populatedTable = "partition_unique_index_fixture"
		conflictTable  = "partition_unique_index_conflict"
	)

	runPartitionClusterTest(
		t,
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)
			eng := cn.RawService().(cnservice.Service).GetEngine()
			sqlExec := testutils.GetSQLExecutor(cn)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabase(t, db, cn)

			createPartitionedTable := func(test *testing.T, table string) {
				testutils.ExecSQL(
					test,
					db,
					cn,
					fmt.Sprintf(
						"create table `%s` (c int primary key, d int, unique key(d)) partition by list (c) (partition p1 values in (1,2), partition p2 values in (3,4))",
						table,
					),
				)
			}

			createPartitionedTable(t, populatedTable)
			testutils.ExecSQL(
				t,
				db,
				cn,
				fmt.Sprintf(
					"insert into `%s` values (1,1), (2,2), (3,3), (4,4)",
					populatedTable,
				),
			)

			t.Run("InsertAndDeleteWithIndex", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
				defer cancel()
				err := sqlExec.ExecTxn(
					ctx,
					func(txn executor.TxnExecutor) error {
						metadata := getMetadata(
							t,
							0,
							db,
							populatedTable,
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
				require.NoError(t, err)
			})

			t.Run("SelectWithUniqueIndex", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
				defer cancel()
				for i := 0; i < 4; i++ {
					res, err := sqlExec.Exec(
						ctx,
						fmt.Sprintf(
							"select c from `%s` where d = %d",
							populatedTable,
							i+1,
						),
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
			})

			t.Run("UniqueIndexCanWork", func(t *testing.T) {
				createPartitionedTable(t, conflictTable)
				testutils.ExecSQL(
					t,
					db,
					cn,
					fmt.Sprintf("insert into `%s` values (1,1)", conflictTable),
				)

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
				defer cancel()

				_, err := sqlExec.Exec(
					ctx,
					fmt.Sprintf("insert into `%s` values (2,1)", conflictTable),
					executor.Options{}.WithDatabase(db),
				)
				require.Error(t, err)

				_, err = sqlExec.Exec(
					ctx,
					fmt.Sprintf("insert into `%s` values (3,1)", conflictTable),
					executor.Options{}.WithDatabase(db),
				)
				require.Error(t, err)

				res, err := sqlExec.Exec(
					ctx,
					fmt.Sprintf("select c, d from `%s`", conflictTable),
					executor.Options{}.WithDatabase(db),
				)
				require.NoError(t, err)
				n := 0
				var c, d int32
				res.ReadRows(
					func(rows int, cols []*vector.Vector) bool {
						n += rows
						if rows > 0 {
							c = executor.GetFixedRows[int32](cols[0])[0]
							d = executor.GetFixedRows[int32](cols[1])[0]
						}
						return true
					},
				)
				res.Close()
				require.Equal(t, 1, n)
				require.Equal(t, int32(1), c)
				require.Equal(t, int32(1), d)
			})
		},
	)
}
