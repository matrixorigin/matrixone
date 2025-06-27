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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestAlterPartitionTableWithAddColumn(t *testing.T) {
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
				fmt.Sprintf("alter table %s add column d int", t.Name()),
			)

			cs := cn.RawService().(cnservice.Service)
			exec := cs.GetSQLExecutor()
			eng := cs.GetEngine()

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
						def := r.GetTableDef(ctx)
						for _, c := range def.Cols {
							if c.Name == "d" {
								return nil
							}
						}
						require.Fail(t, "missing column d")
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

func TestAlterPartitionTableWithDropColumn(t *testing.T) {
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
				fmt.Sprintf("alter table %s drop column b", t.Name()),
			)

			cs := cn.RawService().(cnservice.Service)
			exec := cs.GetSQLExecutor()
			eng := cs.GetEngine()

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
						def := r.GetTableDef(ctx)
						for _, c := range def.Cols {
							require.NotEqual(t, "b", c)
						}
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

func TestAlterPartitionTableWithAddIndex(t *testing.T) {
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
				fmt.Sprintf("alter table %s add index id_01(b)", t.Name()),
			)

			cs := cn.RawService().(cnservice.Service)
			exec := cs.GetSQLExecutor()
			eng := cs.GetEngine()

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

		},
	)
}

func TestAlterPartitionTableWithDropIndex(t *testing.T) {
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
				fmt.Sprintf("alter table %s add index id_01(b)", t.Name()),
			)

			cs := cn.RawService().(cnservice.Service)
			exec := cs.GetSQLExecutor()
			eng := cs.GetEngine()

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

			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {

				},
				fmt.Sprintf("alter table %s drop index id_01", t.Name()),
			)
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
						require.Equal(t, 0, len(r.GetExtraInfo().IndexTables))
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
