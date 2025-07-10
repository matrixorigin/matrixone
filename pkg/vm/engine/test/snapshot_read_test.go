// Copyright 2025 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSnapshot(t *testing.T) {
	var (
		txn       txnif.AsyncTxn
		opts      testutil.TestOptions
		relHandle handle.Relation
		rel       engine.Relation

		database handle.Database
		//accountId    = uint32(0)
		tableName    = "test2"
		databaseName = "db1"

		txnOp client.TxnOperator
	)

	// make sure that disabled all auto ckp and flush
	opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(opts, t)
	defer p.Close()
	taeEngine := p.T

	// one object, two blocks

	schema := catalog.MockSchemaAll(3, 2)
	schema.Name = tableName
	schema.Comment = "rows:20;blks:2"
	schema.Extra.BlockMaxRows = 40
	schema.Extra.ObjectMaxBlocks = 1
	taeEngine.BindSchema(schema)

	var err error

	rowsCnt := 40
	{
		txnOp = p.StartCNTxn()
		p.CreateDBAndTables(txnOp, databaseName, schema)
		require.Nil(t, txnOp.Commit(p.Ctx))

		_, rel, txnOp, err = p.D.GetTable(p.Ctx, databaseName, tableName)
		require.Nil(t, err)

	}

	{
		dbBatch := catalog.MockBatch(schema, rowsCnt)
		defer dbBatch.Close()
		bat := containers.ToCNBatch(dbBatch)
		require.Nil(t, rel.Write(p.Ctx, bat))
		require.Nil(t, txnOp.Commit(p.Ctx))
	}

	disttaeEngine := p.D
	ctx := p.Ctx

	{
		txn, _ = taeEngine.StartTxn()
		database, _ = txn.GetDatabase(databaseName)
		relHandle, _ = database.GetRelationByName(schema.Name)
		txn.Commit(context.Background())
	}

	{
		// make a checkpoint
		txn, _ = taeEngine.StartTxn()
		ts := txn.GetStartTS()

		// the creation event of the appendable object will be record in this ckp
		taeEngine.GetDB().ForceCheckpoint(ctx, ts.Next())
		require.Nil(t, txn.Commit(context.Background()))
	}

	ckp := taeEngine.GetDB().BGCheckpointRunner.MaxIncrementalCheckpoint()
	expectedTS := ckp.GetEnd()

	{
		// the deletion event of the appendable object will be record in this ckp, and a non-appendable
		// object creation event also will be record.
		// appendable object --> soft delete --> non-appendable object
		testutil2.CompactBlocks(t, 0, taeEngine.GetDB(), databaseName, schema, false)

		// make a checkpoint
		txn, _ = taeEngine.StartTxn()

		ts := txn.GetStartTS()
		taeEngine.GetDB().ForceCheckpoint(ctx, ts.Next())
		require.Nil(t, txn.Commit(context.Background()))
	}

	{
		txn, _ = taeEngine.StartTxn()

		ts := txn.GetStartTS()
		taeEngine.GetDB().ForceGlobalCheckpoint(p.Ctx, ts, 0)
		txn.Commit(p.Ctx)
	}

	ckps := taeEngine.GetDB().BGCheckpointRunner.GetAllIncrementalCheckpoints()
	for _, c := range ckps {
		fmt.Println(c.GetStart().ToString(), c.GetEnd().ToString(), expectedTS.ToString())
	}

	err = disttaeEngine.SubscribeTable(ctx, database.GetID(), relHandle.ID(), databaseName, tableName, false)
	require.Nil(t, err)

	txnOp = p.StartCNTxn()
	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	require.True(t, ok)

	exec := v.(executor.SQLExecutor)

	{
		res, err := exec.Exec(p.Ctx,
			fmt.Sprintf("select count(*) from %s.%s {MO_TS = %d};",
				databaseName, tableName, expectedTS.Physical()),
			executor.Options{}.
				WithTxn(txnOp).
				WithWaitCommittedLogApplied())
		require.NotNil(t, err)

		//res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		//	common.MoVectorToString(cols[0], rows)
		//	return true
		//})

		res.Close()
		require.NoError(t, txnOp.Commit(ctx))
	}

	//{
	//	ckps := taeEngine.GetDB().BGCheckpointRunner.GetAllCheckpoints()
	//	readers := make([]*logtail.CKPReader, len(ckps))
	//	for i := 0; i < len(ckps); i++ {
	//
	//		if ckps[i] == nil {
	//			continue
	//		}
	//
	//		ioutil.Prefetch(
	//			taeEngine.GetDB().Runtime.SID(),
	//			taeEngine.GetDB().Runtime.Fs,
	//			ckps[i].GetLocation(),
	//		)
	//	}
	//
	//	for i := 0; i < len(ckps); i++ {
	//
	//		if ckps[i] == nil {
	//			continue
	//		}
	//
	//		readers[i] = logtail.NewCKPReader(
	//			ckps[i].GetVersion(),
	//			ckps[i].GetLocation(),
	//			common.CheckpointAllocator,
	//			taeEngine.GetDB().Runtime.Fs,
	//		)
	//
	//		if err = readers[i].ReadMeta(ctx); err != nil {
	//			return
	//		}
	//		readers[i].PrefetchData(taeEngine.GetDB().Runtime.SID())
	//	}
	//
	//	for i := 0; i < len(ckps); i++ {
	//
	//		if ckps[i] == nil {
	//			continue
	//		}
	//
	//		readers[i].ForEachRow(
	//			ctx,
	//			func(
	//				accout uint32,
	//				dbid, tid uint64,
	//				objectType int8,
	//				objectStats objectio.ObjectStats,
	//				create, delete types.TS,
	//				rowID types.Rowid,
	//			) error {
	//				//e1, _ := taeEngine.GetDB().Catalog.GetDatabaseByID(dbid)
	//				//e2, _ := e1.GetTableEntryByID(tid)
	//				//e2.GetObjectByID(obj)
	//				fmt.Println(dbid, tid, objectStats.FlagString(), create.ToString(), delete.ToString(), relHandle.ID())
	//				return nil
	//			},
	//		)
	//
	//		fmt.Println()
	//		fmt.Println()
	//	}
	//}
}
