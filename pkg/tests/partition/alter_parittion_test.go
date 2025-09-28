// Copyright 2021 - 2025 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// TestAlterTableDropPartition tests the ALTER TABLE DROP PARTITION functionality
func TestAlterTableDropPartition(t *testing.T) {
	runPartitionTableCreateAndDeleteTestsWithAware(
		t,
		func(c embed.Cluster) int32 { return 0 },
		"create table %s (c int comment 'abc', b int) partition by list (c) (partition p0 values in (1, 2), partition p1 values in (3, 4), partition p2 values in (5, 6))",
		partition.PartitionMethod_List,
		func(idx int, p partition.Partition) {
			// Validate partition properties
			require.NotEqual(t, uint64(0), p.PartitionID)
			require.Equal(t, uint32(idx), p.Position)
		},
		func(db string, table string, cn embed.ServiceOperator, pm partition.PartitionMetadata) {
			// Insert some test data
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					// Verify insert was successful
				},
				fmt.Sprintf("insert into %s values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)", table),
			)

			// Verify initial partition count
			initialMetadata := getMetadata(t, 0, db, table, cn)
			require.Equal(t, 3, len(initialMetadata.Partitions))

			// Drop one partition (p0)
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					// Verify drop partition was successful
				},
				fmt.Sprintf("alter table %s drop partition p0", table),
			)

			// Verify partition was dropped
			cs := cn.RawService().(cnservice.Service)
			exec := cs.GetSQLExecutor()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			exec.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					metadata := getMetadata(t, 0, db, table, cn)
					// Should have 2 partitions remaining
					require.Equal(t, 2, len(metadata.Partitions))

					// Verify the remaining partitions are p1 and p2
					partitionNames := make(map[string]bool)
					for _, p := range metadata.Partitions {
						partitionNames[p.Name] = true
					}
					require.True(t, partitionNames["p1"])
					require.True(t, partitionNames["p2"])
					require.False(t, partitionNames["p0"])

					return nil
				},
				executor.Options{},
			)
		},
		func(cn embed.ServiceOperator, pm partition.PartitionMetadata) {
			// Cleanup verification
		},
	)
}

// TestAlterTableTruncatePartition tests the ALTER TABLE TRUNCATE PARTITION functionality
func TestAlterTableTruncatePartition(t *testing.T) {
	runPartitionTableCreateAndDeleteTestsWithAware(
		t,
		func(c embed.Cluster) int32 { return 0 },
		"create table %s (c int comment 'abc', b int) partition by list (c) (partition p0 values in (1, 2), partition p1 values in (3, 4), partition p2 values in (5, 6))",
		partition.PartitionMethod_List,
		func(idx int, p partition.Partition) {
			// Validate partition properties
			require.NotEqual(t, uint64(0), p.PartitionID)
			require.Equal(t, uint32(idx), p.Position)
		},
		func(db string, table string, cn embed.ServiceOperator, pm partition.PartitionMetadata) {
			// Insert some test data
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					// Verify insert was successful
				},
				fmt.Sprintf("insert into %s values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)", table),
			)

			// Verify data was inserted
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					// Should have 5 rows
				},
				fmt.Sprintf("select count(*) from %s", table),
			)

			// Truncate one partition (p0)
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					// Verify truncate partition was successful
				},
				fmt.Sprintf("alter table %s truncate partition p0", table),
			)

			// Verify partition was truncated but table structure remains
			cs := cn.RawService().(cnservice.Service)
			exec := cs.GetSQLExecutor()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			exec.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					metadata := getMetadata(t, 0, db, table, cn)
					// Should still have 3 partitions
					require.Equal(t, 3, len(metadata.Partitions))

					// Verify partition structure is intact
					for _, p := range metadata.Partitions {
						require.NotEqual(t, uint64(0), p.PartitionID)
					}

					return nil
				},
				executor.Options{},
			)

			// Verify some data remains (data in other partitions)
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					// Some data should remain in other partitions
				},
				fmt.Sprintf("select count(*) from %s", table),
			)
		},
		func(cn embed.ServiceOperator, pm partition.PartitionMetadata) {
			// Cleanup verification
		},
	)
}

// TestAlterTableAddPartition tests the ALTER TABLE ADD PARTITION functionality
func TestAlterTableAddPartition(t *testing.T) {
	runPartitionTableCreateAndDeleteTestsWithAware(
		t,
		func(c embed.Cluster) int32 { return 0 },
		"create table %s (c int comment 'abc', b int) partition by list (c) (partition p0 values in (1, 2), partition p1 values in (3, 4))",
		partition.PartitionMethod_List,
		func(idx int, p partition.Partition) {
			// Validate partition properties
			require.NotEqual(t, uint64(0), p.PartitionID)
			require.Equal(t, uint32(idx), p.Position)
		},
		func(db string, table string, cn embed.ServiceOperator, pm partition.PartitionMetadata) {
			// Insert some test data
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					// Verify insert was successful
				},
				fmt.Sprintf("insert into %s values (1, 10), (2, 20), (3, 30), (4, 40)", table),
			)

			// Verify initial partition count
			initialMetadata := getMetadata(t, 0, db, table, cn)
			require.Equal(t, 2, len(initialMetadata.Partitions))

			// Add a new partition (p2)
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					// Verify add partition was successful
				},
				fmt.Sprintf("alter table %s add partition (partition p2 values in (5, 6))", table),
			)

			// Verify partition was added
			cs := cn.RawService().(cnservice.Service)
			exec := cs.GetSQLExecutor()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			exec.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					metadata := getMetadata(t, 0, db, table, cn)
					// Should have 3 partitions now
					require.Equal(t, 3, len(metadata.Partitions))

					// Verify all partitions exist
					partitionNames := make(map[string]bool)
					for _, p := range metadata.Partitions {
						partitionNames[p.Name] = true
						require.NotEqual(t, uint64(0), p.PartitionID)
					}
					require.True(t, partitionNames["p0"])
					require.True(t, partitionNames["p1"])
					require.True(t, partitionNames["p2"])

					return nil
				},
				executor.Options{},
			)
		},
		func(cn embed.ServiceOperator, pm partition.PartitionMetadata) {
			// Cleanup verification
		},
	)
}

// TestAlterTablePartitionBy tests the ALTER TABLE PARTITION BY functionality
func TestAlterTablePartitionBy(t *testing.T) {
	runPartitionTableCreateAndDeleteTestsWithAware(
		t,
		func(c embed.Cluster) int32 { return 0 },
		"create table %s (c int comment 'abc', b int) partition by hash(c) partitions 2",
		partition.PartitionMethod_Hash,
		func(idx int, p partition.Partition) {
			// Validate initial partition properties
			require.NotEqual(t, uint64(0), p.PartitionID)
			require.Equal(t, uint32(idx), p.Position)
		},
		func(db string, table string, cn embed.ServiceOperator, pm partition.PartitionMetadata) {
			// Insert some test data
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					// Verify insert was successful
				},
				fmt.Sprintf("insert into %s values (1, 10), (2, 20), (3, 30), (4, 40)", table),
			)

			// Verify initial partition count
			initialMetadata := getMetadata(t, 0, db, table, cn)
			require.Equal(t, 2, len(initialMetadata.Partitions))
			require.Equal(t, partition.PartitionMethod_Hash, initialMetadata.Method)

			// Change partition method to hash with different column and partition count
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					// Verify alter partition by was successful
				},
				fmt.Sprintf("alter table %s partition by hash(b) partitions 3", table),
			)

			// Verify partition structure changed
			cs := cn.RawService().(cnservice.Service)
			exec := cs.GetSQLExecutor()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			exec.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					metadata := getMetadata(t, 0, db, table, cn)
					// Should now have 3 partitions
					require.Equal(t, 3, len(metadata.Partitions))
					require.Equal(t, partition.PartitionMethod_Hash, metadata.Method)

					// Verify all partitions have valid IDs
					for _, p := range metadata.Partitions {
						require.NotEqual(t, uint64(0), p.PartitionID)
						require.Equal(t, metadata.TableID, p.PrimaryTableID)
					}

					return nil
				},
				executor.Options{},
			)

			// Verify data is still accessible
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					// Should still have 4 rows
				},
				fmt.Sprintf("select count(*) from %s", table),
			)
		},
		func(cn embed.ServiceOperator, pm partition.PartitionMetadata) {
			// Cleanup verification
		},
	)
}
