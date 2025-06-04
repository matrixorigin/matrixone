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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestCreatePartitionIndexes(t *testing.T) {
	var indexes []uint64
	runPartitionTableCreateAndDeleteTestsWithAware(
		t,
		"create table %s (c int comment 'abc', b int) partition by hash(c) partitions 2",
		partition.PartitionMethod_Hash,
		func(idx int, p partition.Partition) {

		},
		func(db string, table string, cn embed.ServiceOperator, pm partition.PartitionMetadata) {
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {

				},
				fmt.Sprintf("create index id_01 on %s(b); ", t.Name()),
			)

			cs := cn.RawService().(cnservice.Service)
			exec := cs.GetSQLExecutor()
			eng := cs.GetEngine()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			exec.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					for _, p := range pm.Partitions {
						_, _, r, err := eng.GetRelationById(
							defines.AttachAccountId(ctx, 0),
							txn.Txn(),
							p.PartitionID,
						)
						require.NoError(t, err)
						require.Equal(t, 1, len(r.GetExtraInfo().IndexTables))

						_, _, r, err = eng.GetRelationById(
							defines.AttachAccountId(ctx, 0),
							txn.Txn(),
							r.GetExtraInfo().IndexTables[0],
						)
						require.NoError(t, err)
						require.Equal(t, p.PartitionID, r.GetExtraInfo().ParentTableID)
						require.True(t, features.IsIndexTable(r.GetExtraInfo().FeatureFlag))

						indexes = append(indexes, r.GetExtraInfo().IndexTables...)
					}

					return nil
				},
				executor.Options{},
			)
		},
		func(cn embed.ServiceOperator, pm partition.PartitionMetadata) {
			cs := cn.RawService().(cnservice.Service)
			eng := cs.GetEngine()
			exec := cs.GetSQLExecutor()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			exec.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					for _, id := range indexes {
						_, _, _, err := eng.GetRelationById(
							ctx,
							txn.Txn(),
							id,
						)
						require.Error(t, err)
					}

					return nil
				},
				executor.Options{},
			)
		},
	)
}

func TestPartitionIndexCanWorkWithCreateIndex(t *testing.T) {

	runPartitionTableCreateAndDeleteTestsWithAware(
		t,
		"create table %s (c int primary key, b int) partition by list (c) (partition p0 values in (1, 3), partition p1 values in (2, 4))",
		partition.PartitionMethod_List,
		func(idx int, p partition.Partition) {

		},
		func(db string, table string, cn embed.ServiceOperator, pm partition.PartitionMetadata) {
			testutils.ExecSQL(
				t,
				db,
				cn,
				fmt.Sprintf("insert into %s values (1,1)", table),
			)

			testutils.ExecSQL(
				t,
				db,
				cn,
				fmt.Sprintf("insert into %s values (2,2)", table),
			)

			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {

				},
				fmt.Sprintf("create index id_01 on %s(b); ", t.Name()),
			)

			cs := cn.RawService().(cnservice.Service)
			exec := cs.GetSQLExecutor()
			eng := cs.GetEngine()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			exec.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					for _, p := range pm.Partitions {
						_, _, r, err := eng.GetRelationById(
							defines.AttachAccountId(ctx, 0),
							txn.Txn(),
							p.PartitionID,
						)
						require.NoError(t, err)

						_, _, index, err := eng.GetRelationById(
							defines.AttachAccountId(ctx, 0),
							txn.Txn(),
							r.GetExtraInfo().IndexTables[0],
						)
						require.NoError(t, err)

						txn.Use(db)
						rs, err := txn.Exec(
							fmt.Sprintf("select count(1) from `%s`", index.GetTableName()),
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
						require.Equal(t, int64(1), n)
					}

					return nil
				},
				executor.Options{},
			)
		},
		func(cn embed.ServiceOperator, pm partition.PartitionMetadata) {

		},
	)
}
