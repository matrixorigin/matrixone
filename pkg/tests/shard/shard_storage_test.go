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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestInitSQLCanCreated(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			initShardsOnce(t, cn1)
		},
	)
}

func TestPartitionBasedShardCanBeCreated(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabase(t, db, cn1)

			store := mustCreateShardStorage(cn1)
			partitions := 2
			shardTableID := mustCreatePartitionTable(
				t,
				partitions,
				db,
				t.Name(),
				cn1,
				store,
			)

			id, metadata, err := store.Get(shardTableID)
			require.NoError(t, err)
			require.Equal(t, shardTableID, id)
			require.Equal(t, shard.Policy_Partition, metadata.Policy)
			require.Equal(t, uint32(partitions), metadata.ShardsCount)
			require.Equal(t, partitions, len(metadata.ShardIDs))

			// check get sharding metadata by partition id
			for _, pid := range metadata.ShardIDs {
				id2, metadata2, err := store.Get(pid)
				require.NoError(t, err)
				require.Equal(t, shardTableID, id2)
				require.Equal(t, metadata, metadata2)
			}
		},
	)
}

func TestPartitionBasedShardCanBeDeleted(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			accountID := uint32(0)
			ctx, cancel := context.WithTimeout(
				defines.AttachAccountId(context.Background(), accountID),
				time.Second*10,
			)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabase(t, db, cn1)

			store := mustCreateShardStorage(cn1)
			tableID := mustCreatePartitionTable(
				t,
				2,
				db,
				t.Name(),
				cn1,
				store,
			)

			exec := testutils.GetSQLExecutor(cn1)
			err = exec.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					ok, err := store.Delete(ctx, tableID, txn.Txn())
					if err != nil {
						return err
					}
					require.True(t, ok)
					return nil
				},
				executor.Options{}.WithDatabase(db),
			)
			require.NoError(t, err)

			id, metadata, err := store.Get(tableID)
			require.NoError(t, err)
			require.True(t, metadata.IsEmpty())
			require.Equal(t, uint64(0), id)
		},
	)
}

func mustCreateShardStorage(
	cn embed.ServiceOperator,
) shardservice.ShardStorage {
	svc := cn.RawService().(cnservice.Service)
	return shardservice.NewShardStorage(
		svc.GetClock(),
		svc.GetSQLExecutor(),
		svc.GetTimestampWaiter(),
		map[int]shardservice.ReadFunc{},
		svc.GetEngine(),
	)
}

func mustGetTableID(
	t *testing.T,
	db string,
	table string,
	txn executor.TxnExecutor,
) uint64 {
	res, err := txn.Exec(
		fmt.Sprintf("select rel_id from mo_catalog.mo_tables where relname = '%s' and reldatabase = '%s'",
			strings.ToLower(table),
			strings.ToLower(db),
		),
		executor.StatementOption{},
	)
	require.NoError(t, err)
	defer res.Close()

	id := uint64(0)
	res.ReadRows(
		func(rows int, cols []*vector.Vector) bool {
			id = executor.GetFixedRows[uint64](cols[0])[0]
			return false
		},
	)

	require.NotEqual(t, uint64(0), id)
	return id
}

func mustCreatePartitionTable(
	t *testing.T,
	n int,
	db string,
	table string,
	cn embed.ServiceOperator,
	store shardservice.ShardStorage,
) uint64 {
	accountID := uint32(0)
	ctx, cancel := context.WithTimeout(
		defines.AttachAccountId(context.Background(), accountID),
		time.Second*10,
	)
	defer cancel()

	committedAt := initShardsOnce(t, cn)

	sql := getPartitionTableSQL(table, n)

	tableID := uint64(0)
	exec := testutils.GetSQLExecutor(cn)
	err := exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txnOp := txn.Txn()

			res, err := txn.Exec(sql, executor.StatementOption{})
			if err != nil {
				return err
			}
			res.Close()

			tableID = mustGetTableID(t, db, table, txn)
			ok, err := store.Create(
				ctx,
				tableID,
				txnOp,
			)
			if err != nil {
				return err
			}
			require.True(t, ok)
			return nil
		},
		executor.Options{}.
			WithDatabase(db).
			WithMinCommittedTS(committedAt),
	)
	require.NoError(t, err)
	return tableID
}

func initShardsOnce(
	t *testing.T,
	cn embed.ServiceOperator,
) timestamp.Timestamp {
	exists := testutils.TableExists(
		t,
		catalog.MO_CATALOG,
		catalog.MOShards,
		cn,
	)
	if exists {
		return timestamp.Timestamp{}
	}

	return testutils.ExecSQL(
		t,
		catalog.MO_CATALOG,
		cn,
		shardservice.InitSQLs...,
	)
}
