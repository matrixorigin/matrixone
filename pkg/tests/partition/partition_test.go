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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
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
			execSQLWithSQLMode(t, db, cn, "STRICT_TRANS_TABLES", fmt.Sprintf(create, table))

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

// Regression for upgrading a pre-v4.0.7 catalog. The old VARCHAR declaration
// already contains raw plan.Expr protobuf bytes; converting its storage type
// must preserve those bytes and keep metadata readable in strict mode.
func TestPartitionExpressionBinaryMetadataUpgradePreservesRawBytes(t *testing.T) {
	runPartitionClusterTestWithReuse(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)

		db := testutils.GetDatabaseName(t)
		testutils.CreateTestDatabase(t, db, cn)
		table := t.Name()
		execSQLWithSQLMode(t, db, cn, "STRICT_TRANS_TABLES",
			fmt.Sprintf("create table %s (c int primary key) partition by range (c) (partition p0 values less than (10), partition p1 values less than maxvalue)", table),
		)

		before := getMetadata(t, 0, db, table, cn)
		expected := partitionExpressionBytes(t, before)

		// Simulate an old catalog without attempting to convert the current
		// VARBINARY data back to VARCHAR: that reverse conversion is rightly
		// rejected by the new UTF-8 invariant. Empty the real metadata rows
		// first, change the schema, then write the historical raw bytes through
		// MATRIXONE_NATIVE.
		execPartitionCatalogTxn(t, cn, "", func(txn executor.TxnExecutor) error {
			res, err := txn.Exec(
				fmt.Sprintf("delete from mo_catalog.mo_partition_tables where primary_table_id = %d", before.TableID),
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}
			res.Close()
			res, err = txn.Exec(
				"alter table mo_catalog.mo_partition_tables modify column partition_expression varchar(2048) not null",
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}
			res.Close()
			return nil
		})

		insertLegacyPartitionMetadata(t, cn, before)

		execPartitionCatalogTxn(t, cn, "", func(txn executor.TxnExecutor) error {
			return v4_0_7.Handler.HandleTenantUpgrade(context.Background(), 0, txn)
		})

		after := getMetadata(t, 0, db, table, cn)
		require.Equal(t, expected, partitionExpressionBytes(t, after))
	}, false, embed.WithPreStart(func(s embed.ServiceOperator) {
		// The historical-catalog fixture must write a system table through the
		// frontend so MATRIXONE_NATIVE takes effect. Restrict this test-only
		// privilege bypass to its isolated embedded cluster.
		s.Adjust(func(cfg *embed.ServiceConfig) {
			cfg.CN.Frontend.SkipCheckPrivilege = true
		})
	}))
}

func insertLegacyPartitionMetadata(t *testing.T, cn embed.ServiceOperator, metadata partition.PartitionMetadata) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", fmt.Sprintf("sys#root#moadmin:111@tcp(127.0.0.1:%d)/mo_catalog", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	db.SetMaxOpenConns(1)

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	_, err = conn.ExecContext(ctx, "set role moadmin")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "set session sql_mode = 'MATRIXONE_NATIVE'")
	require.NoError(t, err)

	for _, p := range metadata.Partitions {
		encoded, err := p.Expr.Marshal()
		require.NoError(t, err)
		_, err = conn.ExecContext(
			ctx,
			`insert into mo_catalog.mo_partition_tables (
				partition_id, partition_table_name, primary_table_id, partition_name,
				partition_ordinal_position, partition_expression_str, partition_expression
			) values (?, ?, ?, ?, ?, ?, ?)`,
			p.PartitionID,
			p.PartitionTableName,
			p.PrimaryTableID,
			p.Name,
			p.Position,
			p.ExprStr,
			encoded,
		)
		require.NoError(t, err)
	}
}

func execPartitionCatalogTxn(
	t *testing.T,
	cn embed.ServiceOperator,
	sqlMode string,
	fn func(executor.TxnExecutor) error,
) {
	t.Helper()

	exec := testutils.GetSQLExecutor(cn)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var txnOp client.TxnOperator
	err := exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txnOp = txn.Txn()
			return fn(txn)
		},
		executor.Options{}.
			WithDatabase(catalog.MO_CATALOG).
			WithAccountID(catalog.System_Account).
			WithResolveVariableFunc(func(name string, isSystemVar, isGlobalVar bool) (interface{}, error) {
				if name == "sql_mode" {
					return sqlMode, nil
				}
				return nil, fmt.Errorf("variable %s not supported", name)
			}),
	)
	require.NoError(t, err)
	testutils.WaitLogtailApplied(t, txnOp.Txn().CommitTS, cn)
}

func execSQLWithSQLMode(
	t *testing.T,
	db string,
	cn embed.ServiceOperator,
	sqlMode string,
	statements ...string,
) {
	t.Helper()

	exec := testutils.GetSQLExecutor(cn)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var txnOp client.TxnOperator
	err := exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txnOp = txn.Txn()
			for _, statement := range statements {
				res, err := txn.Exec(statement, executor.StatementOption{})
				if err != nil {
					return err
				}
				res.Close()
			}
			return nil
		},
		executor.Options{}.
			WithDatabase(db).
			WithAccountID(catalog.System_Account).
			WithResolveVariableFunc(func(name string, isSystemVar, isGlobalVar bool) (interface{}, error) {
				if name == "sql_mode" {
					return sqlMode, nil
				}
				return nil, fmt.Errorf("variable %s not supported", name)
			}),
	)
	require.NoError(t, err)
	testutils.WaitLogtailApplied(t, txnOp.Txn().CommitTS, cn)
}

func partitionExpressionBytes(t *testing.T, metadata partition.PartitionMetadata) [][]byte {
	t.Helper()

	encoded := make([][]byte, 0, len(metadata.Partitions))
	for _, p := range metadata.Partitions {
		value, err := p.Expr.Marshal()
		require.NoError(t, err)
		encoded = append(encoded, value)
	}
	return encoded
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
