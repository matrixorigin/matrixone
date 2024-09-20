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

package shard

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

func TestPartitionBasedTableCanBeCreated(
	t *testing.T,
) {
	runShardClusterTest(
		func(c embed.Cluster) {
			db := testutils.GetDatabaseName(t)
			tableID := mustCreatePartitionBasedTable(t, c, db, 3)
			waitReplica(t, c, tableID, []int64{1, 1, 1})
		},
	)
}

func TestPartitionBasedTableCanBeDeleted(
	t *testing.T,
) {
	runShardClusterTest(
		func(c embed.Cluster) {
			db := testutils.GetDatabaseName(t)
			tableID := mustCreatePartitionBasedTable(t, c, db, 3)
			waitReplica(t, c, tableID, []int64{1, 1, 1})

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			testutils.ExecSQL(
				t,
				db,
				cn1,
				fmt.Sprintf("drop table %s", t.Name()),
			)

			waitReplica(t, c, tableID, []int64{0, 0, 0})
		},
	)
}

func TestInsertIntoWithLocalPartition(
	t *testing.T,
) {
	runShardClusterTest(
		func(c embed.Cluster) {
			db := testutils.GetDatabaseName(t)
			tableID := mustCreatePartitionBasedTable(t, c, db, 3)
			waitReplica(t, c, tableID, []int64{1, 1, 1})

			for i := 0; i < 3; i++ {
				cn, err := c.GetCNService(i)
				require.NoError(t, err)

				value := getLocalPartitionValue(
					t,
					tableID,
					i,
					c,
				)
				min := testutils.ExecSQL(
					t,
					db,
					cn,
					fmt.Sprintf("insert into %s(id, value) values (%d, %d)", t.Name(), value, value),
				)
				mustValueCanRead(
					t,
					db,
					t.Name(),
					value,
					value,
					cn,
					min,
				)
			}
			partitionsMustSubscribeOnlyOnce(
				t,
				tableID,
				c,
			)
		},
	)
}

func TestUpdateWithLocalPartition(
	t *testing.T,
) {
	runShardClusterTest(
		func(c embed.Cluster) {
			db := testutils.GetDatabaseName(t)
			tableID := mustCreatePartitionBasedTable(t, c, db, 3)
			waitReplica(t, c, tableID, []int64{1, 1, 1})

			for i := 0; i < 3; i++ {
				cn, err := c.GetCNService(i)
				require.NoError(t, err)

				value := getLocalPartitionValue(
					t,
					tableID,
					i,
					c,
				)
				min := testutils.ExecSQL(
					t,
					db,
					cn,
					fmt.Sprintf("insert into %s(id, value) values (%d, %d)", t.Name(), value, value),
				)
				min = testutils.ExecSQLWithMinCommittedTS(
					t,
					db,
					cn,
					min,
					fmt.Sprintf("update %s set value = %d where id = %d", t.Name(), value*100, value),
				)
				mustValueCanRead(
					t,
					db,
					t.Name(),
					value,
					value*100,
					cn,
					min,
				)
			}
			partitionsMustSubscribeOnlyOnce(
				t,
				tableID,
				c,
			)
		},
	)
}

func TestInsertIntoWithRemotePartition(
	t *testing.T,
) {
	runShardClusterTest(
		func(c embed.Cluster) {
			db := testutils.GetDatabaseName(t)
			tableID := mustCreatePartitionBasedTable(t, c, db, 3)
			waitReplica(t, c, tableID, []int64{1, 1, 1})

			values := getRemotePartitionValue(
				t,
				tableID,
				c,
			)
			for i := 0; i < 3; i++ {
				cn, err := c.GetCNService(i)
				require.NoError(t, err)

				value := values[i]
				min := testutils.ExecSQL(
					t,
					db,
					cn,
					fmt.Sprintf("insert into %s(id, value) values (%d, %d)", t.Name(), value, value),
				)
				mustValueCanRead(
					t,
					db,
					t.Name(),
					value,
					value,
					cn,
					min,
				)
			}
			partitionsMustSubscribeOnlyOnce(
				t,
				tableID,
				c,
			)
		},
	)
}

func TestUpdateWithRemotePartition(
	t *testing.T,
) {
	runShardClusterTest(
		func(c embed.Cluster) {
			db := testutils.GetDatabaseName(t)
			tableID := mustCreatePartitionBasedTable(t, c, db, 3)
			waitReplica(t, c, tableID, []int64{1, 1, 1})

			values := getRemotePartitionValue(
				t,
				tableID,
				c,
			)
			for i := 0; i < 3; i++ {
				cn, err := c.GetCNService(i)
				require.NoError(t, err)

				value := values[i]
				min := testutils.ExecSQL(
					t,
					db,
					cn,
					fmt.Sprintf("insert into %s(id, value) values (%d, %d)", t.Name(), value, value),
				)
				min = testutils.ExecSQLWithMinCommittedTS(
					t,
					db,
					cn,
					min,
					fmt.Sprintf("update %s set value = %d where id = %d", t.Name(), value*100, value),
				)
				mustValueCanRead(
					t,
					db,
					t.Name(),
					value,
					value*100,
					cn,
					min,
				)
			}
			partitionsMustSubscribeOnlyOnce(
				t,
				tableID,
				c,
			)
		},
	)
}

func TestSelectWithMultiPartition(
	t *testing.T,
) {
	runShardClusterTest(
		func(c embed.Cluster) {
			db := testutils.GetDatabaseName(t)
			tableID := mustCreatePartitionBasedTable(t, c, db, 3)
			waitReplica(t, c, tableID, []int64{1, 1, 1})

			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			values := getAllPartitionValues(
				t,
				tableID,
				c,
			)

			var min timestamp.Timestamp
			for _, v := range values {
				ts := testutils.ExecSQL(
					t,
					db,
					cn,
					fmt.Sprintf("insert into %s(id, value) values (%d, %d)", t.Name(), v, v),
				)
				if ts.Greater(min) {
					min = ts
				}
			}

			for i := 0; i < 3; i++ {
				cn, err := c.GetCNService(i)
				require.NoError(t, err)

				mustValuesCanRead(
					t,
					db,
					t.Name(),
					values,
					cn,
					min,
				)
			}

			partitionsMustSubscribeOnlyOnce(
				t,
				tableID,
				c,
			)
		},
	)
}

func mustCreatePartitionBasedTable(
	t *testing.T,
	c embed.Cluster,
	db string,
	partitions int,
) uint64 {
	cn1, err := c.GetCNService(0)
	require.NoError(t, err)

	testutils.CreateTestDatabase(t, db, cn1)

	committedAt := testutils.ExecSQL(
		t,
		db,
		cn1,
		getPartitionTableSQL(t.Name(), partitions),
	)
	testutils.WaitClusterAppliedTo(t, c, committedAt)

	shardTableID := mustGetTableIDByCN(t, db, t.Name(), cn1)

	// check shard metadata created
	s1 := shardservice.GetService(cn1.RawService().(cnservice.Service).ID())
	store := s1.GetStorage()

	checkPartitionBasedShardMetadata(
		t,
		store,
		shardTableID,
		partitions,
	)

	return shardTableID
}

func partitionsMustSubscribeOnlyOnce(
	t *testing.T,
	tableID uint64,
	c embed.Cluster,
) {
	cn1, err := c.GetCNService(0)
	require.NoError(t, err)

	_, v, err := shardservice.GetService(cn1.ServiceID()).GetStorage().Get(tableID)
	require.NoError(t, err)
	shards := v.ShardIDs

	n := 0
	c.ForeachServices(
		func(so embed.ServiceOperator) bool {
			if so.ServiceType() != metadata.ServiceType_CN {
				return true
			}

			eng := so.RawService().(cnservice.Service).GetEngine()
			for _, shardID := range shards {
				if eng.(*disttae.Engine).PushClient().IsSubscribed(shardID) {
					n++
				}
			}
			return true
		},
	)
	require.Equal(t, len(shards), n)
}

func mustValueCanRead(
	t *testing.T,
	db string,
	table string,
	id int,
	expectValue int,
	cn embed.ServiceOperator,
	min timestamp.Timestamp,
) {
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	ctx, cancel := context.WithTimeout(
		defines.AttachAccountId(context.Background(), 0),
		time.Second*10,
	)
	defer cancel()

	actual := 0
	err := exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			res, err := txn.Exec(
				fmt.Sprintf("select value from %s where id = %d", table, id),
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}
			defer res.Close()

			res.ReadRows(
				func(rows int, cols []*vector.Vector) bool {
					require.Equal(t, 1, rows)
					actual = int(executor.GetFixedRows[int32](cols[0])[0])
					return true
				},
			)
			return nil
		},
		executor.Options{}.
			WithDatabase(db).
			WithMinCommittedTS(min),
	)
	require.NoError(t, err)
	require.Equal(t, expectValue, actual)
}

func mustValuesCanRead(
	t *testing.T,
	db string,
	table string,
	expectValues []int,
	cn embed.ServiceOperator,
	min timestamp.Timestamp,
) {
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	ctx, cancel := context.WithTimeout(
		defines.AttachAccountId(context.Background(), 0),
		time.Second*10,
	)
	defer cancel()

	var actual []int
	err := exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			res, err := txn.Exec(
				fmt.Sprintf("select value from %s", table),
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}
			defer res.Close()

			res.ReadRows(
				func(rows int, cols []*vector.Vector) bool {
					values := executor.GetFixedRows[int32](cols[0])
					for _, v := range values {
						actual = append(actual, int(v))
					}
					return true
				},
			)
			return nil
		},
		executor.Options{}.
			WithDatabase(db).
			WithMinCommittedTS(min),
	)
	require.NoError(t, err)

	sort.IntSlice(expectValues).Sort()
	sort.IntSlice(actual).Sort()
	require.Equal(t, expectValues, actual)
}
