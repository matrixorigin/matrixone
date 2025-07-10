// Copyright 2021-2024 Matrix Origin
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

package partitionservice

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/partition"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateList(t *testing.T) {
	num := uint64(2)
	tableID := uint64(1)
	columns := []string{"a"}
	allowedT := []types.T{
		types.T_int8,
		types.T_int16,
		types.T_int32,
		types.T_int64,
		types.T_uint8,
		types.T_uint16,
		types.T_uint32,
		types.T_uint64,
	}

	for _, v := range allowedT {
		runTestPartitionServiceTest(
			func(
				ctx context.Context,
				txnOp client.TxnOperator,
				s *Service,
				store PartitionStorage,
			) {
				def := newTestTablePartitionDefine(1, columns, []types.T{v}, num, partition.PartitionMethod_List)
				memStore := store.(*memStorage)
				memStore.addUncommittedTable(def)

				stmt := newTestListOption(t, columns[0], num)
				assert.NoError(t, s.Create(ctx, tableID, stmt, txnOp))

				v, ok := memStore.uncommitted[tableID]
				assert.True(t, ok)
				assert.Equal(t, "`"+columns[0]+"`", v.metadata.Description)
				assert.Equal(t, 2, len(v.partitions))
				for _, p := range v.partitions {
					assert.NotEqual(t, 0, p.PartitionID)
				}
			},
		)
	}
}

func TestGetMetadataByListType(t *testing.T) {
	runTestPartitionServiceTest(
		func(
			ctx context.Context,
			txnOp client.TxnOperator,
			s *Service,
			store PartitionStorage,
		) {
			def := newTestTableDefine(1, []string{"c1"}, []types.T{types.T_varchar})
			stmt := newTestListOption(t, "c1", 1)

			_, err := s.getMetadataByListType(stmt.PartitionOption, def)
			require.Error(t, err)

			def = newTestTableDefine(1, []string{"c1"}, []types.T{types.T_int32})
			method := stmt.PartitionOption.PartBy.PType.(*tree.ListType)

			columns, _ := method.Expr.(*tree.UnresolvedName)
			columns.NumParts = 0
			_, err = s.getMetadataByListType(stmt.PartitionOption, def)
			require.Error(t, err)
			columns.NumParts = 1

			method.Expr = tree.NewMaxValue()
			_, err = s.getMetadataByListType(stmt.PartitionOption, def)
			require.Error(t, err)
		},
	)
}

func TestGetMetadataByListColumnsType(t *testing.T) {
	runTestPartitionServiceTest(
		func(
			ctx context.Context,
			txnOp client.TxnOperator,
			s *Service,
			store PartitionStorage,
		) {
			def := newTestTableDefine(1, []string{"c1"}, []types.T{types.T_json})
			stmt := newTestListColumnsOption(t, "c1", 2)

			_, err := s.getMetadataByListType(stmt.PartitionOption, def)
			require.Error(t, err)

			def = newTestTableDefine(1, []string{"c1"}, []types.T{types.T_date})
			method := stmt.PartitionOption.PartBy.PType.(*tree.ListType)

			method.ColumnList = nil
			_, err = s.getMetadataByListType(stmt.PartitionOption, def)
			require.Error(t, err)
		},
	)
}

func TestCreateListColumns(t *testing.T) {
	num := uint64(2)
	tableID := uint64(1)
	columns := []string{"a"}
	allowedT := []types.T{
		types.T_datetime,
		types.T_date,
		types.T_varchar,
	}

	for _, v := range allowedT {
		runTestPartitionServiceTest(
			func(
				ctx context.Context,
				txnOp client.TxnOperator,
				s *Service,
				store PartitionStorage,
			) {
				def := newTestTablePartitionDefine(1, columns, []types.T{v}, num, partition.PartitionMethod_List)
				memStore := store.(*memStorage)
				memStore.addUncommittedTable(def)

				stmt := newTestListColumnsOption(t, columns[0], num)
				assert.NoError(t, s.Create(ctx, tableID, stmt, txnOp))

				v, ok := memStore.uncommitted[tableID]
				assert.True(t, ok)
				assert.Equal(t, 2, len(v.partitions))
				for _, p := range v.partitions {
					assert.NotEqual(t, 0, p.PartitionID)
				}
			},
		)
	}
}

func newTestListOption(
	t *testing.T,
	column string,
	num uint64,
) *tree.CreateTable {
	partitions := ""
	for i := uint64(1); i <= num; i++ {
		partitions += fmt.Sprintf("partition p%d values in (%d)", i, i)
		if i != num {
			partitions += ", "
		}
	}

	return getCreateTableStatement(
		t,
		fmt.Sprintf(
			"create table t(%s int) partition by list (%s) (%s)",
			column,
			column,
			partitions,
		),
	)
}

func newTestListColumnsOption(
	t *testing.T,
	column string,
	num uint64,
) *tree.CreateTable {
	partitions := ""
	for i := uint64(1); i <= num; i++ {
		partitions += fmt.Sprintf("partition p%d values in ('2024-10-1%d')", i, i)
		if i != num {
			partitions += ", "
		}
	}

	return getCreateTableStatement(
		t,
		fmt.Sprintf(
			"create table t(%s date) partition by list columns (%s) (%s)",
			column,
			column,
			partitions,
		),
	)
}
