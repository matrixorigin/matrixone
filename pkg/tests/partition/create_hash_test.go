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
	runPartitionTableCreateAndDeleteTests(
		t,
		"create table %s (c int comment 'abc') partition by hash(c) partitions 2",
		partition.PartitionMethod_Hash,
		func(idx int, p partition.Partition) {},
	)
}

func TestCreateAndDeleteLinearHashBased(t *testing.T) {
	runPartitionTableCreateAndDeleteTests(
		t,
		"create table %s (c int comment 'abc') partition by linear hash(c) partitions 2",
		partition.PartitionMethod_LinearHash,
		func(idx int, p partition.Partition) {},
	)
}

func TestInsertAndDeleteHashBased(t *testing.T) {
	creates := []string{
		"create table %s (c int) partition by hash(c) partitions 2",
		"create table %s (c int, b vecf32(2)) partition by hash(c) partitions 2",
		"create table %s (c int, b vecf32(2)) partition by hash(c) partitions 3",
		"create table %s (c int primary key) partition by range columns(c) (partition p0 values less than (1), partition p1 values less than (5))",
		"create table %s (c int primary key) partition by list (c) (partition p0 values in (1, 2), partition p1 values in (3, 4))",
		"create table %s (a int unsigned, c int) partition by range columns(c) (partition p0 values less than (1), partition p1 values less than (5))",
		"create table %s (b vecf32(2), c int) partition by hash(c) partitions 2",
		"create table %s (c datetime) partition by hash(year(c)) partitions 2",
	}
	inserts := []string{
		"insert into %s values(1)",
		"insert into %s values(1, '[1.1, 2.2]')",
		"insert into %s values(1, '[1.1, 2.2]'), (2, '[1.1, 2.2]'), (3, '[1.1, 2.2]')",
		"insert into %s values(1), (2), (3), (4)",
		"insert into %s values(1), (2), (3), (4)",
		"insert into %s values(1, 1), (2,2), (3,3), (6,4)",
		"insert into %s values('[1.1, 2.2]', 1), ('[1.1, 2.2]', 2), ('[1.1, 2.2]', 3)",
		"insert into %s values('1996-01-01'), ('1997-01-01'), ('1998-01-01'), ('1999-01-01')",
	}
	deletes := []string{
		"delete from %s where c = 1",
		"delete from %s where c = 1",
		"delete from %s where c > 0",
		"delete from %s where c > 0",
		"delete from %s where c > 0",
		"delete from %s where c > 0",
		"delete from %s where c > 0",
		"delete from %s where c > '1995-01-01'",
	}

	partitionCount := []int{
		2,
		2,
		3,
		2,
		2,
		2,
		2,
		2,
	}

	results := [][]int{
		{1, 0},
		{1, 0},
		{3, 0},
		{4, 0},
		{4, 0},
		{4, 0},
		{3, 0},
		{4, 0},
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
					for i := 0; i < partitionCount[idx]; i++ {
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
				require.Equal(t, int64(results[idx][0]), fn())

				testutils.ExecSQLWithReadResult(
					t,
					db,
					cn,
					func(i int, s string, r executor.Result) {
						r.ReadRows(
							func(rows int, cols []*vector.Vector) bool {
								require.Equal(t, int64(results[idx][0]), executor.GetFixedRows[int64](cols[0])[0])
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
				require.Equal(t, int64(results[idx][1]), fn())

				testutils.ExecSQLWithReadResult(
					t,
					db,
					cn,
					func(i int, s string, r executor.Result) {
						r.ReadRows(
							func(rows int, cols []*vector.Vector) bool {
								require.Equal(t, int64(results[idx][1]), executor.GetFixedRows[int64](cols[0])[0])
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

func TestUpdateHashBased(t *testing.T) {
	creates := []string{
		"create table %s (c int primary key, d int) partition by hash(c) partitions 2",
	}
	inserts := []string{
		"insert into %s values(1, 1), (2, 5), (3,3), (6,4)",
	}
	updates := []string{
		"update %s set d = 2 where c = 2",
	}

	partitionCount := []int{
		2,
	}

	results := [][]int{
		{4, 4},
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
				update := fmt.Sprintf(updates[idx], table)

				testutils.ExecSQL(
					t,
					db,
					cn,
					create,
				)

				fn := func() int64 {
					n := int64(0)
					for i := 0; i < partitionCount[idx]; i++ {
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
				require.Equal(t, int64(results[idx][0]), fn())

				testutils.ExecSQLWithReadResult(
					t,
					db,
					cn,
					func(i int, s string, r executor.Result) {
						r.ReadRows(
							func(rows int, cols []*vector.Vector) bool {
								require.Equal(t, int64(results[idx][0]), executor.GetFixedRows[int64](cols[0])[0])
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
					update,
				)
				require.Equal(t, int64(results[idx][1]), fn())

				testutils.ExecSQLWithReadResult(
					t,
					db,
					cn,
					func(i int, s string, r executor.Result) {
						r.ReadRows(
							func(rows int, cols []*vector.Vector) bool {
								require.Equal(t, int64(results[idx][1]), executor.GetFixedRows[int64](cols[0])[0])
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

func TestShowCreatePartitionTable(t *testing.T) {
	creates := []string{
		"create table %s (c int) partition by hash(c) partitions 2",
		"create table %s (c int, b vecf32(2)) partition by hash(c) partitions 2",
		"create table %s (c int, b vecf32(2)) partition by hash(c) partitions 3",
		"create table %s (c int primary key) partition by range columns(c) (partition p0 values less than (1), partition p1 values less than (5))",
		"create table %s (c int primary key) partition by list (c) (partition p0 values in (1, 2), partition p1 values in (3, 4))",
		"create table %s (a int unsigned, c int) partition by range columns(c) (partition p0 values less than (1), partition p1 values less than (5))",
		"create table %s (b vecf32(2), c int) partition by hash(c) partitions 2",
		"create table %s (b vecf32(2), c int) partition by linear hash(c) partitions 2",
	}
	showCreates := []string{
		"show create table %s",
		"show create table %s",
		"show create table %s",
		"show create table %s",
		"show create table %s",
		"show create table %s",
		"show create table %s",
		"show create table %s",
	}
	results := []string{
		"CREATE TABLE `testshowcreatepartitiontable_0` (\n  `c` int DEFAULT NULL\n) partition by hash(`c`) partitions 2",
		"CREATE TABLE `testshowcreatepartitiontable_1` (\n  `c` int DEFAULT NULL,\n  `b` vecf32(2) DEFAULT NULL\n) partition by hash(`c`) partitions 2",
		"CREATE TABLE `testshowcreatepartitiontable_2` (\n  `c` int DEFAULT NULL,\n  `b` vecf32(2) DEFAULT NULL\n) partition by hash(`c`) partitions 3",
		"CREATE TABLE `testshowcreatepartitiontable_3` (\n  `c` int NOT NULL,\n  PRIMARY KEY (`c`)\n) partition by range columns(`c`) (partition p0 values less than (1), partition p1 values less than (5))",
		"CREATE TABLE `testshowcreatepartitiontable_4` (\n  `c` int NOT NULL,\n  PRIMARY KEY (`c`)\n) partition by List columns(`c`) (partition p0 values in (1, 2), partition p1 values in (3, 4))",
		"CREATE TABLE `testshowcreatepartitiontable_5` (\n  `a` int unsigned DEFAULT NULL,\n  `c` int DEFAULT NULL\n) partition by range columns(`c`) (partition p0 values less than (1), partition p1 values less than (5))",
		"CREATE TABLE `testshowcreatepartitiontable_6` (\n  `b` vecf32(2) DEFAULT NULL,\n  `c` int DEFAULT NULL\n) partition by hash(`c`) partitions 2",
		"CREATE TABLE `testshowcreatepartitiontable_7` (\n  `b` vecf32(2) DEFAULT NULL,\n  `c` int DEFAULT NULL\n) partition by linear hash(`c`) partitions 2",
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
				showCreate := fmt.Sprintf(showCreates[idx], table)

				testutils.ExecSQL(
					t,
					db,
					cn,
					create,
				)

				if idx == 3 {
					fmt.Println("111")
				}

				testutils.ExecSQLWithReadResult(
					t,
					db,
					cn,
					func(i int, s string, r executor.Result) {
						r.ReadRows(
							func(rows int, cols []*vector.Vector) bool {
								require.Equal(t, results[idx], executor.GetStringRows(cols[1])[0])
								return true
							},
						)
					},
					showCreate,
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
