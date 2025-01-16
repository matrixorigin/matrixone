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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateHash(t *testing.T) {
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
				s *service,
				store *memStorage,
			) {
				def := newTestTableDefine(1, columns, []types.T{v})
				store.addUncommittedTable(def)

				stmt := newTestHashOption(t, columns[0], num)
				assert.NoError(t, s.Create(ctx, tableID, stmt, txnOp))

				v, ok := store.uncommitted[tableID]
				assert.True(t, ok)
				assert.Equal(t, columns[0], v.metadata.Description)
				assert.Equal(t, 2, len(v.partitions))
				for _, p := range v.partitions {
					assert.NotEqual(t, 0, p.PartitionID)
				}
			},
		)
	}
}

func TestDelete(t *testing.T) {
	num := uint64(2)
	tableID := uint64(1)
	columns := []string{"a"}

	runTestPartitionServiceTest(
		func(
			ctx context.Context,
			txnOp client.TxnOperator,
			s *service,
			store *memStorage,
		) {
			def := newTestTableDefine(1, columns, []types.T{types.T_int8})
			store.addUncommittedTable(def)

			stmt := newTestHashOption(t, columns[0], num)
			assert.NoError(t, s.Create(ctx, tableID, stmt, txnOp))

			require.NoError(t, txnOp.Commit(ctx))

			require.NoError(t, s.Delete(ctx, tableID, nil))

			ok, metadata, err := s.Is(ctx, tableID, nil)
			require.NoError(t, err)
			require.False(t, ok)
			require.True(t, metadata.IsEmpty())
		},
	)
}

func TestIs(t *testing.T) {
	num := uint64(2)
	tableID := uint64(1)
	columns := []string{"a"}

	runTestPartitionServiceTest(
		func(
			ctx context.Context,
			txnOp client.TxnOperator,
			s *service,
			store *memStorage,
		) {
			def := newTestTableDefine(1, columns, []types.T{types.T_int8})
			store.addUncommittedTable(def)

			stmt := newTestHashOption(t, columns[0], num)
			assert.NoError(t, s.Create(ctx, tableID, stmt, txnOp))

			ok, metadata, err := s.Is(ctx, tableID, txnOp)
			require.NoError(t, err)
			require.True(t, ok)
			require.True(t, !metadata.IsEmpty())

			ok, metadata, err = s.Is(ctx, tableID+1, txnOp)
			require.NoError(t, err)
			require.False(t, ok)
			require.True(t, metadata.IsEmpty())
		},
	)
}

func runTestPartitionServiceTest(
	fn func(
		ctx context.Context,
		txnOp client.TxnOperator,
		s *service,
		store *memStorage,
	),
) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	txnOp, close := client.NewTestTxnOperator(ctx)
	defer close()

	store := newMemPartitionStorage()
	s := NewService(Config{Enable: true}, store)
	fn(ctx, txnOp, s.(*service), store.(*memStorage))
}

func newTestTableDefine(
	id uint64,
	columns []string,
	types []types.T,
) *plan.TableDef {
	def := &plan.TableDef{
		TblId: id,
	}

	for idx, col := range columns {
		def.Cols = append(
			def.Cols,
			&plan.ColDef{
				Name: col,
				Typ:  plan.Type{Id: int32(types[idx])},
			},
		)
	}
	return def
}

func newTestHashOption(
	t *testing.T,
	column string,
	num uint64,
) *tree.CreateTable {
	return getCreateTableStatement(
		t,
		fmt.Sprintf(
			"create table t(%s int) partition by hash(%s) partitions %d",
			column,
			column,
			num,
		),
	)
}

func getCreateTableStatement(
	t *testing.T,
	sql string,
) *tree.CreateTable {
	stmt, err := parsers.ParseOne(
		context.TODO(),
		dialect.MYSQL,
		sql,
		1,
	)
	require.NoError(t, err)
	return stmt.(*tree.CreateTable)
}

func TestGetService(t *testing.T) {
	require.Nil(t, GetService(""))
}
