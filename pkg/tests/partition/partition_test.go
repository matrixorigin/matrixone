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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

var sharedCluster embed.SharedTestCluster

// Regression for partition protobuf metadata being written through the SQL
// storage path. partition_expression is binary data, so this must work with
// compatible-mode UTF-8 validation enabled on text columns.
func TestPartitionExpressionBinaryMetadataRoundTrip(t *testing.T) {
	creates := []string{
		"create table %s (c int) partition by hash(c) partitions 2",
		"create table %s (c int primary key) partition by list (c) (partition p0 values in (1), partition p1 values in (2))",
		"create table %s (c int primary key) partition by range (c) (partition p0 values less than (10), partition p1 values less than maxvalue)",
	}

	runPartitionClusterTest(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)

		db := testutils.GetDatabaseName(t)
		testutils.CreateTestDatabase(t, db, cn)
		for idx, create := range creates {
			table := fmt.Sprintf("%s_%d", t.Name(), idx)
			testutils.ExecSQL(t, db, cn, fmt.Sprintf(create, table))

			metadata := getMetadata(t, 0, db, table, cn)
			require.NotEmpty(t, metadata.Partitions)
			for _, p := range metadata.Partitions {
				require.NotNil(t, p.Expr)
				encoded, err := p.Expr.Marshal()
				require.NoError(t, err)
				require.NotEmpty(t, encoded)
			}
		}
	})
}

func runPartitionTableCreateAndDeleteTestsWithAware(
	t *testing.T,
	prepare func(c embed.Cluster) int32,
	sql string,
	method partition.PartitionMethod,
	validPartition func(idx int, p partition.Partition),
	beforeDrop func(string, string, embed.ServiceOperator, partition.PartitionMetadata),
	afterDrop func(embed.ServiceOperator, partition.PartitionMetadata),
) {
	runPartitionClusterTest(
		t,
		func(c embed.Cluster) {
			account := prepare(c)

			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabaseWithAccount(t, account, db, cn)

			testutils.ExecSQLWithReadResultAndAccount(
				t,
				account,
				db,
				cn,
				func(i int, s string, r executor.Result) {

				},
				fmt.Sprintf(sql, t.Name()),
			)

			metadata := getMetadata(
				t,
				uint32(account),
				db,
				t.Name(),
				cn,
			)
			require.NotEqual(t, 0, len(metadata.Partitions))
			require.Equal(t, method, metadata.Method)

			var tables []string
			for idx, p := range metadata.Partitions {
				tables = append(tables, p.PartitionTableName)
				require.NotEqual(t, uint64(0), p.PartitionID)
				require.Equal(t, metadata.TableID, p.PrimaryTableID)
				require.Equal(t, uint32(idx), p.Position)
				require.Equal(t, partitionservice.GetPartitionTableName(metadata.TableName, p.Name), p.PartitionTableName)
				validPartition(idx, p)
			}

			beforeDrop(db, t.Name(), cn, metadata)

			testutils.ExecSQLWithAccount(
				t,
				account,
				db,
				cn,
				fmt.Sprintf("drop table %s", t.Name()),
			)
			metadata = getMetadata(
				t,
				uint32(account),
				db,
				t.Name(),
				cn,
			)
			require.Equal(t, partition.PartitionMetadata{}, metadata)

			for _, name := range tables {
				require.False(t, testutils.TableExistsWithAccount(t, account, db, name, cn))
			}

			afterDrop(cn, metadata)
		},
	)
}

func runPartitionTableCreateAndDeleteTests(
	t *testing.T,
	sql string,
	method partition.PartitionMethod,
	validPartition func(idx int, p partition.Partition),
) {
	runPartitionTableCreateAndDeleteTestsWithPrepare(
		t,
		func(c embed.Cluster) int32 { return 0 },
		sql,
		method,
		validPartition,
	)
}

func runPartitionTableCreateAndDeleteTestsWithPrepare(
	t *testing.T,
	prepare func(c embed.Cluster) int32,
	sql string,
	method partition.PartitionMethod,
	validPartition func(idx int, p partition.Partition),
) {
	runPartitionTableCreateAndDeleteTestsWithAware(
		t,
		prepare,
		sql,
		method,
		validPartition,
		func(string, string, embed.ServiceOperator, partition.PartitionMetadata) {},
		func(embed.ServiceOperator, partition.PartitionMetadata) {},
	)
}

func runPartitionClusterTest(
	t *testing.T,
	fn func(embed.Cluster),
	options ...embed.Option,
) error {
	return runPartitionClusterTestWithReuse(
		t,
		fn,
		true,
		options...,
	)
}

func runPartitionClusterTestWithReuse(
	t *testing.T,
	fn func(embed.Cluster),
	reuse bool,
	options ...embed.Option,
) error {
	createFunc := func() (embed.Cluster, error) {
		options = append(
			[]embed.Option{
				embed.WithCNCount(3),
				// The shared test cluster runs one TN and three CNs on the same
				// CI worker. Keep the test-only RPC deadline above transient
				// scheduling stalls without changing production defaults.
				embed.WithHAKeeperHeartbeatTimeout(15 * time.Second),
			},
			options...,
		)
		return embed.StartTestCluster(options...)
	}

	run := func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		if !testutils.TableExists(t, catalog.MO_CATALOG, catalog.MOPartitionMetadata, cn) {
			testutils.ExecSQL(
				t,
				catalog.MO_CATALOG,
				cn,
				partitionservice.InitSQLs...,
			)
		}
		fn(c)
	}

	if reuse {
		sharedCluster.Run(t, createFunc, run)
		return nil
	}
	c, err := createFunc()
	if c != nil {
		t.Cleanup(func() { require.NoError(t, c.Close()) })
	}
	require.NoError(t, err)
	run(c)
	return nil
}
