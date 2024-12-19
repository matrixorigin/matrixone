// Copyright 2022 Matrix Origin
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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// #region basic test

// insert 10 rows and delete 5 rows
func Test_BasicInsertDelete(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 10
	bat := catalog2.MockBatch(schema, rowsCount)
	bat1 := containers.ToCNBatch(bat)

	// write table
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, true))

		var bat2 *batch.Batch
		txn.GetWorkspace().(*disttae.Transaction).ForEachTableWrites(
			relation.GetDBID(ctx), relation.GetTableID(ctx), 1, func(entry disttae.Entry) {
				waitedDeletes := vector.MustFixedColWithTypeCheck[types.Rowid](entry.Bat().GetVector(0))
				waitedDeletes = waitedDeletes[:rowsCount/2]
				bat2 = batch.NewWithSize(1)
				bat2.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
				require.NoError(t, vector.AppendFixedList[types.Rowid](bat2.Vecs[0], waitedDeletes, nil, mp))
				bat2.SetRowCount(len(waitedDeletes))
			})
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat2, true, true))
		require.NoError(t, txn.Commit(ctx))
	}

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
	require.NoError(t, err)

	require.Equal(t, 5, ret.RowCount())
	require.NoError(t, txn.Commit(ctx))
}

func Test_BasicS3InsertDelete(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineInsertEntryMaxCount(1),
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 10
	bat := catalog2.MockBatch(schema, rowsCount)
	bat1 := containers.ToCNBatch(bat)

	// write table
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, true))
	}

	// read row id
	tombstoneBat := batch.NewWithSize(1)
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	{
		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := batch.NewWithSize(1)
		ret.Attrs = []string{catalog.Row_ID}
		ret.Vecs = []*vector.Vector{vector.NewVec(types.T_Rowid.ToType())}

		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtWithTypeCheck[types.Rowid](ret.Vecs[0], i),
					false, mp)
				require.NoError(t, err)
			}
		}

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat.Length(), tombstoneBat.Vecs[0].Length())
	}

	// delete batch
	{
		require.NoError(t, err)
		bat2, err := tombstoneBat.Window(0, 5)
		require.NoError(t, err)
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat2, true, true))
		require.NoError(t, txn.Commit(ctx))
	}

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)

	require.NoError(t, err)
	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
	require.NoError(t, err)

	require.Equal(t, 5, ret.RowCount())
	require.NoError(t, txn.Commit(ctx))
}

// #endregion
// #region multi-txn test

// txn1 insert 0-10
// txn1 commit
// txn2 insert 10-20
// txn2 delete 5-15
// txn2 commit
// read -> 10 rows
func Test_MultiTxnInsertDelete(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 20
	bat := containers.ToCNBatch(catalog2.MockBatch(schema, rowsCount))
	bat1, err := bat.Window(0, 10)
	require.NoError(t, err)
	bat2, err := bat.Window(10, 20)
	require.NoError(t, err)

	// txn1 insert 0-10
	// txn1 commit
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, true))
		require.NoError(t, txn.Commit(ctx))
	}

	// read row id and pk data
	tombstoneBat := batch.NewWithSize(2)
	tombstoneBat.Attrs = []string{catalog.Row_ID, schema.GetPrimaryKey().Name}
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tombstoneBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	{
		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := batch.NewWithSize(2)
		ret.Attrs = []string{schema.GetPrimaryKey().Name, catalog.Row_ID}
		ret.Vecs = []*vector.Vector{vector.NewVec(schema.GetPrimaryKey().Type), vector.NewVec(types.T_Rowid.ToType())}

		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtWithTypeCheck[types.Rowid](ret.Vecs[1], i),
					false, mp)
				require.NoError(t, err)

				err = vector.AppendFixed[int64](
					tombstoneBat.Vecs[1],
					vector.GetFixedAtWithTypeCheck[int64](ret.Vecs[0], i),
					false, mp)
				require.NoError(t, err)
			}
		}

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat1.RowCount(), tombstoneBat.Vecs[0].Length())
	}

	// txn2 insert 10-20
	// txn2 delete 5-15
	// txn2 commit
	{
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat2, false, true))

		txn.GetWorkspace().(*disttae.Transaction).ForEachTableWrites(
			relation.GetDBID(ctx), relation.GetTableID(ctx), 1, func(entry disttae.Entry) {
				waitedDeletes := vector.MustFixedColWithTypeCheck[types.Rowid](entry.Bat().GetVector(0))
				require.NoError(t, vector.AppendFixedList[types.Rowid](tombstoneBat.Vecs[0], waitedDeletes, nil, mp))
				tombstoneBat.SetRowCount(tombstoneBat.RowCount() + len(waitedDeletes))
			})

		bat, err := tombstoneBat.Window(5, 15)
		require.NoError(t, err)
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat, true, true))
		require.NoError(t, txn.Commit(ctx))
	}

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
	require.NoError(t, err)

	require.Equal(t, 10, ret.RowCount())
	require.NoError(t, txn.Commit(ctx))
}

// txn1 insert 0-10
// txn1 commit
// txn2 insert 10-20
// txn2 delete 5-15
// txn2 commit
// read -> 10 rows
func Test_MultiTxnS3InsertDelete(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineInsertEntryMaxCount(1),
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 20
	bat := containers.ToCNBatch(catalog2.MockBatch(schema, rowsCount))
	bat1, err := bat.Window(0, 10)
	require.NoError(t, err)
	bat2, err := bat.Window(10, 20)
	require.NoError(t, err)

	// txn1 insert 0-10
	// txn1 commit
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, true))
		require.NoError(t, txn.Commit(ctx))
	}

	// txn2 insert 10-20
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat2, false, true))
	}

	// read row id and pk data
	tombstoneBat := batch.NewWithSize(2)
	tombstoneBat.Attrs = []string{catalog.Row_ID, schema.GetPrimaryKey().Name}
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tombstoneBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())

	{
		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := batch.NewWithSize(2)
		ret.Attrs = []string{schema.GetPrimaryKey().Name, catalog.Row_ID}
		ret.Vecs = []*vector.Vector{vector.NewVec(schema.GetPrimaryKey().Type), vector.NewVec(types.T_Rowid.ToType())}

		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtWithTypeCheck[types.Rowid](ret.Vecs[1], i),
					false, mp)
				require.NoError(t, err)

				err = vector.AppendFixed[int64](
					tombstoneBat.Vecs[1],
					vector.GetFixedAtWithTypeCheck[int64](ret.Vecs[0], i),
					false, mp)
				require.NoError(t, err)
			}
		}

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat.RowCount(), tombstoneBat.Vecs[0].Length())
	}

	// txn2 insert 10-20
	{
		bat, err := tombstoneBat.Window(5, 15)
		require.NoError(t, err)
		require.Equal(t, 10, bat.RowCount())
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat, true, true))
		require.NoError(t, txn.Commit(ctx))
	}
	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)

	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	cnt := 0
	for {
		done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)
		require.NoError(t, err)
		cnt += ret.RowCount()

		if done {
			break
		}
	}

	require.Equal(t, 10, cnt)
	require.NoError(t, txn.Commit(ctx))
}

func Test_MultiTxnS3Tombstones(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		relation engine.Relation
		_        engine.Database

		primaryKeyIdx = 1

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaEnhanced(2, primaryKeyIdx, 2)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineInsertEntryMaxCount(1),
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 30
	bat := catalog2.MockBatch(schema, 0)
	for i := range rowsCount {
		bat.Vecs[0].Append(int32(i/10), false)
		bat.Vecs[1].Append(int64(i), false)
	}

	// txn1 insert 0-30
	// txn1 commit
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, containers.ToCNBatch(bat), false, true))
		require.NoError(t, txn.Commit(ctx))
	}

	// read row id and pk data
	tombstoneBat := batch.NewWithSize(2)
	tombstoneBat.Attrs = []string{catalog.Row_ID, schema.GetPrimaryKey().Name}
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tombstoneBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())

	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)
		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := batch.NewWithSize(2)
		ret.Attrs = []string{schema.GetPrimaryKey().Name, catalog.Row_ID}
		ret.Vecs = []*vector.Vector{vector.NewVec(schema.GetPrimaryKey().Type), vector.NewVec(types.T_Rowid.ToType())}

		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtWithTypeCheck[types.Rowid](ret.Vecs[1], i),
					false, mp)
				require.NoError(t, err)

				err = vector.AppendFixed[int64](
					tombstoneBat.Vecs[1],
					vector.GetFixedAtWithTypeCheck[int64](ret.Vecs[0], i),
					false, mp)
				require.NoError(t, err)
			}
		}

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat.Length(), tombstoneBat.Vecs[0].Length())
		require.NoError(t, txn.Commit(ctx))
	}

	bat1, _ := tombstoneBat.Window(0, 10)
	bat2, _ := tombstoneBat.Window(10, 20)

	// txn2 delete 0-10
	// txn2 commit
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, true, true))
		require.NoError(t, txn.Commit(ctx))
	}

	// txn3 delete 10-20
	// txn3 commit
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat2, true, true))
		require.NoError(t, txn.Commit(ctx))
	}

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)

	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema)
	cnt := 0
	for {
		done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)
		require.NoError(t, err)
		cnt += ret.RowCount()

		if done {
			break
		}
	}

	require.Equal(t, 10, cnt)
	require.NoError(t, txn.Commit(ctx))
}

// #endregion
// #region rollback test

// insert 0-10
// insert 10-20 and rollback
// delete 0-5 and rollback
// delete 0-5 and commit
// read -> 5 rows
func Test_BasicRollbackStatement(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 20
	bat := catalog2.MockBatch(schema, rowsCount)
	bat1 := containers.ToCNBatch(bat.Window(0, 10))
	bat2 := containers.ToCNBatch(bat.Window(10, 10))

	// write table
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, true))

		txn.GetWorkspace().StartStatement()
		require.NoError(t, relation.Write(ctx, bat2))
		require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
		require.NoError(t, txn.GetWorkspace().IncrStatementID(ctx, false))
	}

	{
		var tombstoneBat *batch.Batch
		txn.GetWorkspace().(*disttae.Transaction).ForEachTableWrites(
			relation.GetDBID(ctx), relation.GetTableID(ctx), 1, func(entry disttae.Entry) {
				waitedDeletes := vector.MustFixedColWithTypeCheck[types.Rowid](entry.Bat().GetVector(0))
				waitedDeletes = waitedDeletes[:rowsCount/2]
				tombstoneBat = batch.NewWithSize(1)
				tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
				require.NoError(t, vector.AppendFixedList[types.Rowid](tombstoneBat.Vecs[0], waitedDeletes, nil, mp))
				tombstoneBat.SetRowCount(len(waitedDeletes))
			})

		tombstoneBat2, err := tombstoneBat.Window(0, 5)
		require.NoError(t, err)

		require.NoError(t, relation.Delete(ctx, tombstoneBat, catalog.Row_ID))
		require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
		require.NoError(t, txn.GetWorkspace().IncrStatementID(ctx, false))

		require.NoError(t, relation.Delete(ctx, tombstoneBat2, catalog.Row_ID))
		require.NoError(t, txn.Commit(ctx))
	}

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)

	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
	require.NoError(t, err)

	require.Equal(t, 5, ret.RowCount())
	require.NoError(t, txn.Commit(ctx))
}

// insert 0-10
// insert 10-20 and rollback
// delete 0-5 and rollback
// delete 0-5 and commit
// read -> 5 rows
func Test_BasicRollbackStatementS3(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineInsertEntryMaxCount(1),
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 20
	bat := catalog2.MockBatch(schema, rowsCount)
	bat1 := containers.ToCNBatch(bat.Window(0, 10))
	bat2 := containers.ToCNBatch(bat.Window(10, 10))

	// write table
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, true))

		txn.GetWorkspace().StartStatement()
		require.NoError(t, relation.Write(ctx, bat2))
		require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
		require.NoError(t, txn.GetWorkspace().IncrStatementID(ctx, false))
	}

	// read row id
	tombstoneBat := batch.NewWithSize(1)
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	{
		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := batch.NewWithSize(1)
		ret.Attrs = []string{catalog.Row_ID}
		ret.Vecs = []*vector.Vector{vector.NewVec(types.T_Rowid.ToType())}

		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtWithTypeCheck[types.Rowid](ret.Vecs[0], i),
					false, mp)
				require.NoError(t, err)
			}
		}

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat1.RowCount(), tombstoneBat.Vecs[0].Length())
	}

	// delete batch
	{
		tb1, err := tombstoneBat.Window(0, 5)
		require.NoError(t, err)
		tb2, err := tombstoneBat.Window(5, 10)
		require.NoError(t, err)

		require.NoError(t, relation.Delete(ctx, tb1, catalog.Row_ID))
		require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
		require.NoError(t, txn.GetWorkspace().IncrStatementID(ctx, false))

		require.NoError(t, relation.Delete(ctx, tb2, catalog.Row_ID))
		require.NoError(t, txn.Commit(ctx))
	}

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)

	require.NoError(t, err)
	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
	require.NoError(t, err)

	require.Equal(t, 5, ret.RowCount())
	require.NoError(t, txn.Commit(ctx))
}

// https://github.com/matrixorigin/MO-Cloud/issues/4602
func Test_RollbackDeleteAndDrop(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(testutil.TestOptions{
		TaeEngineOptions: opts,
		DisttaeOptions:   []testutil.TestDisttaeEngineOptions{testutil.WithDisttaeEngineInsertEntryMaxCount(5)}},
		t)
	defer p.Close()

	schema := catalog2.MockSchemaAll(10, 1)
	schema.Name = "test"
	schema2 := catalog2.MockSchemaAll(10, 1)
	schema2.Name = "test2"
	schema3 := catalog2.MockSchemaAll(10, 1)
	schema3.Name = "test3"
	txnop := p.StartCNTxn()

	bat := catalog2.MockBatch(schema, 10)
	_, rels := p.CreateDBAndTables(txnop, "db", schema, schema2, schema3)
	require.NoError(t, rels[2].Write(p.Ctx, containers.ToCNBatch(bat)))
	require.NoError(t, txnop.Commit(p.Ctx))

	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		panic(fmt.Sprintf("missing sql executor in service %q", ""))
	}
	txnop = p.StartCNTxn()
	exec := v.(executor.SQLExecutor)
	execopts := executor.Options{}.WithTxn(txnop).WithDisableIncrStatement()
	txnop.GetWorkspace().StartStatement()
	txnop.GetWorkspace().IncrStatementID(p.Ctx, false)
	dropTable := func() {
		_, err := exec.Exec(p.Ctx, "delete from db.test3 where mock_1 = 0", execopts)
		require.NoError(t, err)
		p.DeleteTableInDB(txnop, "db", "test")
		p.DeleteTableInDB(txnop, "db", "test2")
		_, err = exec.Exec(p.Ctx, "delete from db.test3 where mock_1 = 2", execopts)
		require.NoError(t, err)
	}
	dropTable() // approximateInMemDeleteCnt = 2
	txnop.GetWorkspace().RollbackLastStatement(p.Ctx)
	txnop.GetWorkspace().IncrStatementID(p.Ctx, false)
	dropTable() // approximateInMemDeleteCnt = 4
	txnop.GetWorkspace().RollbackLastStatement(p.Ctx)
	txnop.GetWorkspace().IncrStatementID(p.Ctx, false)
	dropTable() // approximateInMemDeleteCnt = 6
	t.Log(txnop.GetWorkspace().PPString())
	err := txnop.Commit(p.Ctx) // dumpDeleteBatchLocked messes up the writes list and get bad write format error
	require.NoError(t, err)
}

// #endregion
// #region multi-txn rollback test

// txn1 insert 0-10
// txn1 commit
// txn2 insert 10-20
// txn2 insert 20-30 and rollback
// txn2 delete 5-15 and rollback
// txn2 delete 5-15
// txn2 commit
// read -> 10 rows
func Test_MultiTxnRollbackStatement(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 20
	bat := containers.ToCNBatch(catalog2.MockBatch(schema, rowsCount))
	bat1, err := bat.Window(0, 10)
	require.NoError(t, err)
	bat2, err := bat.Window(10, 20)
	require.NoError(t, err)

	// txn1 insert 0-10
	// txn1 commit
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, true))
		require.NoError(t, txn.Commit(ctx))
	}

	// read row id and pk data
	tombstoneBat := batch.NewWithSize(2)
	tombstoneBat.Attrs = []string{catalog.Row_ID, schema.GetPrimaryKey().Name}
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tombstoneBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	{
		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := batch.NewWithSize(2)
		ret.Attrs = []string{schema.GetPrimaryKey().Name, catalog.Row_ID}
		ret.Vecs = []*vector.Vector{vector.NewVec(schema.GetPrimaryKey().Type), vector.NewVec(types.T_Rowid.ToType())}

		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtWithTypeCheck[types.Rowid](ret.Vecs[1], i),
					false, mp)
				require.NoError(t, err)

				err = vector.AppendFixed[int64](
					tombstoneBat.Vecs[1],
					vector.GetFixedAtWithTypeCheck[int64](ret.Vecs[0], i),
					false, mp)
				require.NoError(t, err)
			}
		}

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat1.RowCount(), tombstoneBat.Vecs[0].Length())
	}

	// txn2 insert 10-20
	// txn2 delete 5-15
	// txn2 commit
	{
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat2, false, true))

		txn.GetWorkspace().StartStatement()
		require.NoError(t, relation.Write(ctx, bat2))
		require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
		require.NoError(t, txn.GetWorkspace().IncrStatementID(ctx, false))

		txn.GetWorkspace().(*disttae.Transaction).ForEachTableWrites(
			relation.GetDBID(ctx), relation.GetTableID(ctx), 1, func(entry disttae.Entry) {
				waitedDeletes := vector.MustFixedColWithTypeCheck[types.Rowid](entry.Bat().GetVector(0))
				require.NoError(t, vector.AppendFixedList[types.Rowid](tombstoneBat.Vecs[0], waitedDeletes, nil, mp))
				tombstoneBat.SetRowCount(tombstoneBat.RowCount() + len(waitedDeletes))
			})

		tb1, err := tombstoneBat.Window(5, 15)
		require.NoError(t, err)
		tb2, err := tombstoneBat.Window(5, 15)
		require.NoError(t, err)
		require.NoError(t, relation.Delete(ctx, tb1, catalog.Row_ID))
		require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
		require.NoError(t, txn.GetWorkspace().IncrStatementID(ctx, false))

		require.NoError(t, relation.Delete(ctx, tb2, catalog.Row_ID))
		require.NoError(t, txn.Commit(ctx))
	}

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
	require.NoError(t, err)

	require.Equal(t, 10, ret.RowCount())
	require.NoError(t, txn.Commit(ctx))
}

// txn1 insert 0-10
// txn1 commit
// txn2 insert 10-20
// txn2 insert 20-30 and rollback
// txn2 delete 5-15 and rollback
// txn2 delete 5-15
// txn2 commit
// read -> 10 rows
// TODO: fix me
func Test_MultiTxnRollbackStatementS3(t *testing.T) {
	t.Skip("skip")
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineInsertEntryMaxCount(1),
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 20
	bat := containers.ToCNBatch(catalog2.MockBatch(schema, rowsCount))
	bat1, err := bat.Window(0, 10)
	require.NoError(t, err)
	bat2, err := bat.Window(10, 20)
	require.NoError(t, err)

	// txn1 insert 0-10
	// txn1 commit
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, true))
		require.NoError(t, txn.Commit(ctx))
	}
	ws := txn.GetWorkspace().(*disttae.Transaction)
	_ = ws
	// txn2 insert 10-20
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat2, false, true))
	}

	// read row id and pk data
	tombstoneBat := batch.NewWithSize(2)
	tombstoneBat.Attrs = []string{catalog.Row_ID, schema.GetPrimaryKey().Name}
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tombstoneBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())

	{
		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := batch.NewWithSize(2)
		ret.Attrs = []string{schema.GetPrimaryKey().Name, catalog.Row_ID}
		ret.Vecs = []*vector.Vector{vector.NewVec(schema.GetPrimaryKey().Type), vector.NewVec(types.T_Rowid.ToType())}

		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtWithTypeCheck[types.Rowid](ret.Vecs[1], i),
					false, mp)
				require.NoError(t, err)

				err = vector.AppendFixed[int64](
					tombstoneBat.Vecs[1],
					vector.GetFixedAtWithTypeCheck[int64](ret.Vecs[0], i),
					false, mp)
				require.NoError(t, err)
			}
		}

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat.RowCount(), tombstoneBat.Vecs[0].Length())
	}

	// txn2 delete 5-15
	{
		txn.GetWorkspace().StartStatement()
		require.NoError(t, relation.Write(ctx, bat2))
		require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
		require.NoError(t, txn.GetWorkspace().IncrStatementID(ctx, false))

		tb1, err := tombstoneBat.Window(5, 15)
		require.NoError(t, err)
		tb2, err := tombstoneBat.Window(5, 15)

		require.NoError(t, err)
		require.NoError(t, relation.Delete(ctx, tb1, catalog.Row_ID))
		require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
		require.NoError(t, txn.GetWorkspace().IncrStatementID(ctx, false))

		require.NoError(t, relation.Delete(ctx, tb2, catalog.Row_ID))
		require.NoError(t, txn.Commit(ctx))
	}
	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)

	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	cnt := 0
	for {
		done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)
		require.NoError(t, err)
		cnt += ret.RowCount()

		if done {
			break
		}
	}

	require.Equal(t, 10, cnt)
	require.NoError(t, txn.Commit(ctx))
}

func Test_DeleteUncommittedBlock(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		accountId    = catalog.System_Account
		tableName    = "test_reader_table"
		databaseName = "test_reader_database"

		primaryKeyIdx = 3

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	_ = colexec.NewServer(nil)

	// mock a schema with 4 columns and the 4th column as primary key
	// the first column is the 9th column in the predefined columns in
	// the mock function. Here we exepct the type of the primary key
	// is types.T_char or types.T_varchar
	schema := catalog2.MockSchemaEnhanced(4, primaryKeyIdx, 9)
	schema.Name = tableName

	{

		disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
			ctx,
			testutil.TestOptions{},
			t,
			testutil.WithDisttaeEngineInsertEntryMaxCount(1),
			testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
			testutil.WithDisttaeEngineWriteWorkspaceThreshold(1),
		)
		defer func() {
			disttaeEngine.Close(ctx)
			taeEngine.Close(true)
			rpcAgent.Close()
		}()

		ctx, cancel = context.WithTimeout(ctx, time.Minute)
		defer cancel()
		_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
		require.NoError(t, err)
	}

	insertCnt := 150
	deleteCnt := 1
	var bat2 *batch.Batch

	{
		// insert 150 rows
		_, table, txn, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		bat := catalog2.MockBatch(schema, insertCnt)
		err = table.Write(ctx, containers.ToCNBatch(bat))
		require.NoError(t, err)

		entryCnt := 0
		txn.GetWorkspace().(*disttae.Transaction).ForEachTableWrites(
			table.GetDBID(ctx), table.GetTableID(ctx), 1, func(entry disttae.Entry) {
				if entry.Bat() == nil ||
					entry.Bat().RowCount() == 0 ||
					entry.FileName() == "" {
					return
				}
				entryCnt++

				bat2 = batch.NewWithSize(1)
				bat2.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())

				locstr := string(entry.Bat().GetVector(0).GetBytesAt(0))
				loc, _ := blockio.EncodeLocationFromString(locstr)
				sid := loc.Name().SegmentId()
				bid := objectio.NewBlockid(
					&sid,
					loc.Name().Num(),
					loc.ID(),
				)
				for i := 0; i < deleteCnt; i++ {
					rid := types.NewRowid(bid, uint32(i))
					require.NoError(t, vector.AppendFixed[types.Rowid](bat2.Vecs[0], *rid, false, mp))
				}
				bat2.SetRowCount(deleteCnt)

			})
		require.Equal(t, 1, entryCnt)
		//delete 100 rows
		require.NoError(t, table.Delete(ctx, bat2, catalog.Row_ID))
		require.NoError(t, txn.Commit(ctx))
	}

	{
		_, _, reader, err := testutil.GetTableTxnReader(
			ctx,
			disttaeEngine,
			databaseName,
			tableName,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
		_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
		require.NoError(t, err)
		require.Equal(t, insertCnt-deleteCnt, ret.RowCount())
	}
}

func Test_BigDeleteWriteS3(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		accountId    = catalog.System_Account
		tableName    = "test_reader_table"
		databaseName = "test_reader_database"

		primaryKeyIdx = 3

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	// mock a schema with 4 columns and the 4th column as primary key
	// the first column is the 9th column in the predefined columns in
	// the mock function. Here we exepct the type of the primary key
	// is types.T_char or types.T_varchar
	schema := catalog2.MockSchemaEnhanced(4, primaryKeyIdx, 9)
	schema.Name = tableName

	{
		disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
			ctx,
			testutil.TestOptions{},
			t,
			testutil.WithDisttaeEngineInsertEntryMaxCount(1),
			testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
		)
		defer func() {
			disttaeEngine.Close(ctx)
			taeEngine.Close(true)
			rpcAgent.Close()
		}()

		ctx, cancel = context.WithTimeout(ctx, time.Minute)
		defer cancel()
		_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
		require.NoError(t, err)
	}

	insertCnt := 150
	//deleteCnt := 100
	//var bat2 *batch.Batch

	{
		// insert 150 rows
		_, table, txn, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		bat := catalog2.MockBatch(schema, insertCnt)
		err = table.Write(ctx, containers.ToCNBatch(bat))
		require.NoError(t, err)

		//delete 100 rows
		//require.NoError(t, table.Delete(ctx, bat2, catalog.Row_ID))
		require.NoError(t, txn.Commit(ctx))
	}

	{
		_, _, reader, err := testutil.GetTableTxnReader(
			ctx,
			disttaeEngine,
			databaseName,
			tableName,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
		_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
		require.NoError(t, err)
		//require.Equal(t, insertCnt-deleteCnt, ret.RowCount())
	}
}

func Test_CNTransferTombstoneObjects(t *testing.T) {
	var (
		err          error
		opts         testutil.TestOptions
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(0))

	opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)

	p := testutil.InitEnginePack(opts, t)
	defer p.Close()

	schema := catalog2.MockSchemaEnhanced(1, 0, 2)
	schema.Name = tableName

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	cnTxnOp := p.StartCNTxn()
	_, rel := p.CreateDBAndTable(cnTxnOp, databaseName, schema)
	require.NotNil(t, rel)
	require.NoError(t, cnTxnOp.Commit(ctx))

	bat := catalog2.MockBatch(schema, 20)
	bats := bat.Split(2)

	require.Equal(t, 2, len(bats))
	require.Equal(t, 10, bats[0].Length())

	// append data
	{
		for i := 0; i < len(bats); i++ {
			tnTxnOp, err := p.T.GetDB().StartTxn(nil)
			require.NoError(t, err)

			tnDB, err := tnTxnOp.GetDatabase(databaseName)
			require.NoError(t, err)

			tnRel, err := tnDB.GetRelationByName(tableName)
			require.NoError(t, err)

			err = tnRel.Append(ctx, bats[i])
			require.NoError(t, err)

			require.NoError(t, tnTxnOp.Commit(ctx))

			testutil2.CompactBlocks(t, 0, p.T.GetDB(), databaseName, schema, true)
		}
	}

	{
		_, _, cnTxnOp, err = p.D.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		cnTxnOp.GetWorkspace().StartStatement()
		err = cnTxnOp.GetWorkspace().IncrStatementID(ctx, false)
		require.NoError(t, err)
	}

	// read row id and pk data
	tombstoneBat := batch.NewWithSize(2)
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tombstoneBat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	{
		txn, _, reader, err := testutil.GetTableTxnReader(
			ctx, p.D, databaseName, tableName, nil, p.Mp, t,
		)
		require.NoError(t, err)

		ret := testutil.EmptyBatchFromSchema(schema)

		for {
			done, err := reader.Read(ctx,
				[]string{schema.GetPrimaryKey().Name, catalog.Row_ID}, nil, p.Mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtNoTypeCheck[types.Rowid](ret.Vecs[1], i),
					false, p.Mp)
				require.NoError(t, err)

				err = vector.AppendFixed[int32](
					tombstoneBat.Vecs[1],
					vector.GetFixedAtNoTypeCheck[int32](ret.Vecs[0], i),
					false, p.Mp)
				require.NoError(t, err)
			}
		}

		require.NoError(t, txn.Commit(ctx))

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat.Length(), tombstoneBat.Vecs[0].Length())
	}

	// merge data objects
	{
		testutil2.MergeBlocks(t, 0, p.T.GetDB(), databaseName, schema, true)

		ss, err := p.D.GetPartitionStateStats(ctx, rel.GetDBID(ctx), rel.GetTableID(ctx))
		require.NoError(t, err)

		fmt.Println(ss.String())
		require.Equal(t, 1, ss.DataObjectsVisible.ObjCnt)
	}

	// mock tombstone object and put it into workspace
	{
		w, err := colexec.NewS3TombstoneWriter()
		require.NoError(t, err)

		w.StashBatch(rel.GetProcess().(*process.Process), tombstoneBat)
		_, ss, err := w.SortAndSync(ctx, rel.GetProcess().(*process.Process))
		require.NoError(t, err)

		require.Equal(t, bat.Length(), int(ss.Rows()))

		tbat := batch.NewWithSize(1)
		tbat.Attrs = []string{catalog.ObjectMeta_ObjectStats}
		tbat.Vecs[0] = vector.NewVec(types.T_text.ToType())
		err = vector.AppendBytes(tbat.Vecs[0], ss.Marshal(), false, p.Mp)
		require.NoError(t, err)

		tbat.SetRowCount(tbat.Vecs[0].Length())

		transaction := cnTxnOp.GetWorkspace().(*disttae.Transaction)
		err = transaction.WriteFile(
			disttae.DELETE,
			0, rel.GetDBID(ctx), rel.GetTableID(ctx), databaseName, tableName,
			ss.ObjectLocation().String(),
			tbat,
			p.D.Engine.GetTNServices()[0],
		)
		require.NoError(t, err)

		err = cnTxnOp.UpdateSnapshot(ctx, p.D.Now())
		require.NoError(t, err)
	}

	// check result
	{
		expected := 20
		for i := 0; i < 2; i++ {
			if i == 1 {
				expected = 0
				ctx = context.WithValue(ctx, disttae.UT_ForceTransCheck{}, "yes")
				require.NoError(t, cnTxnOp.Commit(ctx))
			}

			txnop := p.StartCNTxn()
			v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
			require.True(t, ok)

			exec := v.(executor.SQLExecutor)

			res, err := exec.Exec(p.Ctx,
				fmt.Sprintf("select count(*) from `%s`.`%s`;",
					databaseName, tableName), executor.Options{}.WithTxn(txnop))
			require.NoError(t, err)
			require.NoError(t, txnop.Commit(ctx))

			res.ReadRows(func(rows int, cols []*vector.Vector) bool {
				require.Equal(t, 1, rows)
				require.Equal(t, 1, len(cols))

				result := vector.MustFixedColNoTypeCheck[int64](cols[0])
				require.Equal(t, expected, int(result[0]))
				return true
			})

			res.Close()
		}
	}
}
