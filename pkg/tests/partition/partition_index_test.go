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
