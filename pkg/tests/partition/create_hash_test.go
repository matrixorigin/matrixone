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
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestCreateAndDeleteHashBased(t *testing.T) {
	runPartitionClusterTest(
		t,
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabase(t, db, cn)

			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {

				},
				fmt.Sprintf("create table %s (c int) partition by hash(c) partitions 2", t.Name()),
			)

			metadata := getMetadata(
				t,
				0,
				db,
				t.Name(),
				cn,
			)
			require.Equal(t, 2, len(metadata.Partitions))
			require.Equal(t, partition.PartitionMethod_Hash, metadata.Method)

			var tables []string
			for idx, p := range metadata.Partitions {
				tables = append(tables, p.PartitionTableName)
				require.NotEqual(t, uint64(0), p.PartitionID)
				require.Equal(t, metadata.TableID, p.PrimaryTableID)
				require.Equal(t, uint32(idx), p.Position)
				require.Equal(t, fmt.Sprintf("%s_%s", metadata.TableName, p.Name), p.PartitionTableName)
			}

			testutils.ExecSQL(
				t,
				db,
				cn,
				fmt.Sprintf("drop table %s", t.Name()),
			)
			metadata = getMetadata(
				t,
				0,
				db,
				t.Name(),
				cn,
			)
			require.Equal(t, partition.PartitionMetadata{}, metadata)

			for _, name := range tables {
				require.False(t, testutils.TableExists(t, db, name, cn))
			}

		},
	)
}

func TestInsertAndDeleteHashBased(t *testing.T) {
	creates := []string{
		"create table %s (c int) partition by hash(c) partitions 2",
		"create table %s (c int, b vecf32(2)) partition by hash(c) partitions 2",
	}
	inserts := []string{
		"insert into %s values(1)",
		"insert into %s values(1, '[1.1, 2.2]')",
	}
	deletes := []string{
		"delete from %s where c = 1",
		"delete from %s where c = 1",
	}

	runPartitionClusterTest(
		t,
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabase(t, db, cn)

			for idx := range creates {
				table := fmt.Sprintf("%s_%d", t.Name(), idx)
				create := fmt.Sprintf(creates[idx], table)
				insert := fmt.Sprintf(inserts[idx], table)
				delete := fmt.Sprintf(deletes[idx], table)

				testutils.ExecSQL(
					t,
					db,
					cn,
					create,
				)

				fn := func() int64 {
					n := int64(0)
					for i := 0; i < 2; i++ {
						testutils.ExecSQLWithReadResult(
							t,
							db,
							cn,
							func(i int, s string, r executor.Result) {
								r.ReadRows(
									func(rows int, cols []*vector.Vector) bool {
										n += executor.GetFixedRows[int64](cols[0])[0]
										return true
									},
								)
							},
							fmt.Sprintf("select count(1) from %s_p%d", table, i),
						)
					}
					return n
				}

				testutils.ExecSQL(
					t,
					db,
					cn,
					insert,
				)
				require.Equal(t, int64(1), fn())

				testutils.ExecSQLWithReadResult(
					t,
					db,
					cn,
					func(i int, s string, r executor.Result) {
						r.ReadRows(
							func(rows int, cols []*vector.Vector) bool {
								require.Equal(t, int64(1), executor.GetFixedRows[int64](cols[0])[0])
								return true
							},
						)
					},
					fmt.Sprintf("select count(1) from %s", table),
				)

				testutils.ExecSQL(
					t,
					db,
					cn,
					delete,
				)
				require.Equal(t, int64(0), fn())

				testutils.ExecSQLWithReadResult(
					t,
					db,
					cn,
					func(i int, s string, r executor.Result) {
						r.ReadRows(
							func(rows int, cols []*vector.Vector) bool {
								require.Equal(t, int64(0), executor.GetFixedRows[int64](cols[0])[0])
								return true
							},
						)
					},
					fmt.Sprintf("select count(1) from %s", table),
				)
			}
		},
	)

}

func getMetadata(
	t *testing.T,
	accountID uint32,
	db string,
	table string,
	cn embed.ServiceOperator,
) partition.PartitionMetadata {
	ps := partitionservice.GetService(cn.ServiceID())
	store := ps.GetStorage()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = defines.AttachAccountId(ctx, accountID)

	var value partition.PartitionMetadata
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	err := exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			id := testutils.GetTableID(
				t,
				db,
				table,
				txn,
			)
			if id == 0 {
				return nil
			}

			metadata, ok, err := store.GetMetadata(
				ctx,
				id,
				txn.Txn(),
			)
			require.NoError(t, err)
			require.True(t, ok)
			value = metadata
			return nil
		},
		executor.Options{},
	)
	require.NoError(t, err)
	return value
}
