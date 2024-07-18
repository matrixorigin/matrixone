// Copyright 2024 Matrix Origin
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

package test

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	testutil2 "github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
)

func Test_ReaderCanReadInMemInserts(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_reader_table"
		databaseName = "test_reader_database"

		primaryKeyIdx int = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	blockCnt := 1
	rowsCount := int(options.DefaultBlockMaxRows) * blockCnt
	bats := catalog2.MockBatch(schema, rowsCount).Split(blockCnt)

	// write table
	{
		for idx := 0; idx < blockCnt; idx++ {
			_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
			require.NoError(t, err)
			require.NoError(t, relation.Write(ctx, containers.ToCNBatch(bats[idx])))
		}

		require.NoError(t, txn.Commit(ctx))
	}

	require.NoError(t, disttaeEngine.SubscribeTable(ctx, relation.GetDBID(ctx), relation.GetTableID(ctx), false))

	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, relation.GetDBID(ctx), relation.GetTableID(ctx))
		require.NoError(t, err)

		require.Equal(t, rowsCount, stats.InmemRows.VisibleCnt)
	}

	expr := []*plan.Expr{
		disttae.MakeFunctionExprForTest("=", []*plan.Expr{
			disttae.MakeColExprForTest(int32(primaryKeyIdx), schema.ColDefs[primaryKeyIdx].Type.Oid),
			plan2.MakePlan2Int64ConstExprWithType(bats[0].Vecs[primaryKeyIdx].Get(0).(int64)),
		}),
	}

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)

	reader, err := testutil.NewDefaultTableReader(
		ctx, relation, databaseName, schema,
		expr[0], testutil2.NewProcessWithMPool(mp),
		disttaeEngine.Engine.FS(),
		txn.SnapshotTS(),
		disttaeEngine.Engine.PackerPool())
	require.NoError(t, err)

	ret, err := reader.Read(ctx, []string{schema.ColDefs[primaryKeyIdx].Name}, expr[0], mp, nil)
	require.NoError(t, err)
	require.NotNil(t, ret)

	require.Equal(t, rowsCount, ret.RowCount())
}
