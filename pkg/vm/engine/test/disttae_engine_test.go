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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	ops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
)

func Test_InsertRows(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountId)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.Nil(t, err)

	err = disttaeEngine.Engine.Create(ctx, databaseName, txn)
	require.Nil(t, err)

	db, err := disttaeEngine.Engine.Database(ctx, databaseName, txn)
	require.Nil(t, err)

	schema := catalog2.MockSchemaAll(10, 0)
	schema.Name = tableName

	defs, err := testutil.EngineTableDefBySchema(schema)
	require.Nil(t, err)

	err = db.Create(ctx, tableName, defs)
	require.Nil(t, err)

	rel, err := db.Relation(ctx, tableName, nil)
	require.Nil(t, err)
	require.Contains(t, rel.GetTableName(), tableName)

	bat := catalog2.MockBatch(schema, 10)
	err = rel.Write(ctx, containers.ToCNBatch(bat))
	require.Nil(t, err)

	err = txn.Commit(ctx)
	require.Nil(t, err)

	err = disttaeEngine.SubscribeTable(ctx, rel.GetDBID(ctx), rel.GetTableID(ctx), false)
	require.Nil(t, err)

	// check partition state, before flush
	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.GetDBID(ctx), rel.GetTableID(ctx))
		require.Nil(t, err)

		fmt.Println(stats.String())

		require.Equal(t, 10, stats.InmemRows.VisibleCnt)
	}

	err = taeHandler.GetDB().FlushTable(ctx, accountId, rel.GetDBID(ctx), rel.GetTableID(ctx), types.TimestampToTS(disttaeEngine.Now()))
	require.Nil(t, err)
	// check partition state, after flush
	{

		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.GetDBID(ctx), rel.GetTableID(ctx))
		require.Nil(t, err)

		fmt.Println(stats.String())

		expect := testutil.PartitionStateStats{
			DataObjectsVisible:   testutil.PObjectStats{ObjCnt: 1, BlkCnt: 1, RowCnt: 10},
			DataObjectsInvisible: testutil.PObjectStats{ObjCnt: 1, BlkCnt: 1, RowCnt: 10},
			InmemRows:            testutil.PInmemRowsStats{},
			CheckpointCnt:        0,
		}

		require.Equal(t, expect, stats.Summary())

	}
}

// Create database and tables, and check the data length in the system tables in TN
func TestSystemDB1(t *testing.T) {

	p := testutil.InitEnginePack(testutil.TestOptions{}, t)
	defer p.Close()

	txnop := p.StartCNTxn()

	var err error
	p.CreateDB(txnop, "db1")

	schema := catalog2.MockSchema(2, 0)
	schema.Name = "test1inDb2"
	p.CreateDBAndTable(txnop, "db2", schema)

	dbs, err := p.D.Engine.Databases(p.Ctx, txnop)
	require.NoError(t, err)
	require.Equal(t, 2+1, len(dbs))
	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	dbs, err = p.D.Engine.Databases(p.Ctx, txnop)
	require.NoError(t, err)
	txnop.GetWorkspace().StartStatement()
	require.Equal(t, 2+1, len(dbs))

	txn, err := p.T.GetDB().StartTxn(nil)
	require.NoError(t, err)

	catalogDB, err := txn.GetDatabase(catalog.MO_CATALOG)
	require.NoError(t, err)
	table, err := catalogDB.GetRelationByName(catalog.MO_DATABASE)
	require.NoError(t, err)
	it := table.MakeObjectIt()
	for it.Next() {
		blk := it.GetObject()
		view, err := blk.GetColumnDataByName(p.Ctx, 0, catalog.SystemDBAttr_Name, common.DefaultAllocator)
		require.Nil(t, err)
		defer view.Close()
		require.Equal(t, 2, view.Length())
		view, err = blk.GetColumnDataByName(p.Ctx, 0, catalog.SystemDBAttr_CreateSQL, common.DefaultAllocator)
		require.Nil(t, err)
		defer view.Close()
		require.Equal(t, 2, view.Length())
	}

	table, err = catalogDB.GetRelationByName(catalog.MO_TABLES)
	require.Nil(t, err)
	it = table.MakeObjectIt()
	for it.Next() {
		blk := it.GetObject()
		view, err := blk.GetColumnDataByName(p.Ctx, 0, catalog.SystemRelAttr_Name, common.DefaultAllocator)
		require.Nil(t, err)
		defer view.Close()
		require.Equal(t, 1, view.Length())
		view, err = blk.GetColumnDataByName(p.Ctx, 0, catalog.SystemRelAttr_Kind, common.DefaultAllocator)
		require.NoError(t, err)
		defer view.Close()
		require.Equal(t, 1, view.Length())
	}

	table, err = catalogDB.GetRelationByName(catalog.MO_COLUMNS)
	require.Nil(t, err)
	bat := containers.NewBatch()
	defer bat.Close()
	it = table.MakeObjectIt()
	for it.Next() {
		blk := it.GetObject()
		view, err := blk.GetColumnDataByName(p.Ctx, 0, catalog.SystemColAttr_DBName, common.DefaultAllocator)
		require.NoError(t, err)
		defer view.Close()
		bat.AddVector(catalog.SystemColAttr_DBName, view.Vecs[0])

		view, err = blk.GetColumnDataByName(p.Ctx, 0, catalog.SystemColAttr_RelName, common.DefaultAllocator)
		require.Nil(t, err)
		defer view.Close()
		bat.AddVector(catalog.SystemColAttr_RelName, view.Vecs[0])

		view, err = blk.GetColumnDataByName(p.Ctx, 0, catalog.SystemColAttr_Name, common.DefaultAllocator)
		require.Nil(t, err)
		defer view.Close()
		bat.AddVector(catalog.SystemColAttr_Name, view.Vecs[0])
	}
	require.Equal(t, 3, bat.Length())
	t.Log(bat.PPString(10))
}

type dummyCpkGetter struct{}

func (c *dummyCpkGetter) CollectCheckpointsInRange(ctx context.Context, start, end types.TS) (ckpLoc string, lastEnd types.TS, err error) {
	return "", types.TS{}, nil
}

func (c *dummyCpkGetter) FlushTable(ctx context.Context, dbID, tableID uint64, ts types.TS) error {
	return nil
}

func totsp(ts types.TS) *timestamp.Timestamp {
	t := ts.ToTimestamp()
	return &t
}

func TestLogtailBasic(t *testing.T) {

	opts := config.WithLongScanAndCKPOpts(nil)
	opts.LogtailCfg = &options.LogtailCfg{PageSize: 30}
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()
	tae := p.T.GetDB()
	logMgr := tae.LogtailMgr

	// at first, we can see nothing
	minTs, maxTs := types.BuildTS(0, 0), types.BuildTS(1000, 1000)
	reader := logMgr.GetReader(minTs, maxTs)
	require.False(t, reader.HasCatalogChanges())
	require.Equal(t, 0, len(reader.GetDirtyByTable(1000, 1000).Objs))

	schema := catalog2.MockSchemaAll(2, -1)
	schema.Name = "test"
	schema.Comment = "rows:10;blks=1"
	// craete 2 db and 2 tables
	txnop := p.StartCNTxn()
	p.CreateDBAndTable(txnop, "todrop", schema)
	_, tH := p.CreateDBAndTable(txnop, "db", schema)
	dbID := tH.GetDBID(p.Ctx)
	tableID := tH.GetTableID(p.Ctx)
	require.NoError(t, txnop.Commit(p.Ctx))
	catalogWriteTs := txnop.Txn().CommitTS

	// drop the first db
	txnop = p.StartCNTxn()
	require.Nil(t, p.D.Engine.Delete(p.Ctx, "todrop", txnop))
	require.NoError(t, txnop.Commit(p.Ctx))
	catalogDropTs := txnop.Txn().CommitTS

	writeTs := make([]types.TS, 0, 120)
	deleteRowIDs := make([]types.Rowid, 0, 10)

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		// insert 100 rows
		for i := 0; i < 100; i++ {
			txn, _ := tae.StartTxn(nil)
			db, _ := txn.GetDatabase("db")
			tbl, _ := db.GetRelationByName("test")
			tbl.Append(p.Ctx, catalog2.MockBatch(schema, 1))
			require.NoError(t, txn.Commit(p.Ctx))
			writeTs = append(writeTs, txn.GetPrepareTS())
		}
		// delete the row whose offset is 5 for every block
		{
			// collect rowid
			txn, _ := tae.StartTxn(nil)
			db, _ := txn.GetDatabase("db")
			tbl, _ := db.GetRelationByName("test")
			blkIt := tbl.MakeObjectIt()
			for blkIt.Next() {
				obj := blkIt.GetObject()
				id := obj.GetMeta().(*catalog2.ObjectEntry).ID()
				for j := 0; j < obj.BlkCnt(); j++ {
					blkID := objectio.NewBlockidWithObjectID(id, uint16(j))
					deleteRowIDs = append(deleteRowIDs, *objectio.NewRowid(blkID, 5))
				}
			}
			blkIt.Close()
			require.NoError(t, txn.Commit(p.Ctx))
		}

		// delete two 2 rows one time. no special reason, it just comes up
		for i := 0; i < len(deleteRowIDs); i += 2 {
			txn, _ := tae.StartTxn(nil)
			db, _ := txn.GetDatabase("db")
			tbl, _ := db.GetRelationByName("test")
			require.NoError(t, tbl.DeleteByPhyAddrKey(deleteRowIDs[i]))
			if i+1 < len(deleteRowIDs) {
				tbl.DeleteByPhyAddrKey(deleteRowIDs[i+1])
			}
			require.NoError(t, txn.Commit(p.Ctx))
			writeTs = append(writeTs, txn.GetPrepareTS())
		}
		wg.Done()
	}()

	// concurrent read to test race
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 10; i++ {
				reader := logMgr.GetReader(minTs, maxTs)
				_ = reader.GetDirtyByTable(dbID, tableID)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	firstWriteTs, lastWriteTs := writeTs[0], writeTs[len(writeTs)-1]

	reader = logMgr.GetReader(minTs, types.TimestampToTS(catalogWriteTs))
	require.Equal(t, 0, len(reader.GetDirtyByTable(dbID, tableID).Objs))
	reader = logMgr.GetReader(firstWriteTs, lastWriteTs)
	require.Equal(t, 0, len(reader.GetDirtyByTable(dbID, tableID-1).Objs))
	reader = logMgr.GetReader(firstWriteTs, lastWriteTs)
	dirties := reader.GetDirtyByTable(dbID, tableID)
	require.Equal(t, 10, len(dirties.Objs))

	fixedColCnt := 2 // __rowid + commit_time, the columns for a delBatch
	// check Bat rows count consistency
	check_same_rows := func(bat *api.Batch, expect int) {
		for i, vec := range bat.Vecs {
			col, err := vector.ProtoVectorToVector(vec)
			require.NoError(t, err)
			require.Equal(t, expect, col.Length(), "columns %d", i)
		}
	}

	// get db catalog change
	resp, close, err := logtail.HandleSyncLogTailReq(p.Ctx, new(dummyCpkGetter), tae.LogtailMgr, tae.Catalog, api.SyncLogTailReq{
		CnHave: totsp(minTs),
		CnWant: &catalogDropTs,
		Table:  &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_DATABASE_ID},
	}, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(resp.Commands)) // insert and delete

	require.Equal(t, api.Entry_Insert, resp.Commands[0].EntryType)
	require.Equal(t, len(catalog2.SystemDBSchema.ColDefs)+1 /*commit ts*/, len(resp.Commands[0].Bat.Vecs))
	check_same_rows(resp.Commands[0].Bat, 2)                                 // 2 db
	datname, err := vector.ProtoVectorToVector(resp.Commands[0].Bat.Vecs[3]) // datname column
	require.NoError(t, err)
	require.Equal(t, "todrop", datname.UnsafeGetStringAt(0))
	require.Equal(t, "db", datname.UnsafeGetStringAt(1))

	require.Equal(t, api.Entry_Delete, resp.Commands[1].EntryType)
	require.Equal(t, fixedColCnt+1, len(resp.Commands[1].Bat.Vecs))
	check_same_rows(resp.Commands[1].Bat, 1) // 1 drop db

	close()

	// get table catalog change
	resp, close, err = logtail.HandleSyncLogTailReq(p.Ctx, new(dummyCpkGetter), tae.LogtailMgr, tae.Catalog, api.SyncLogTailReq{
		CnHave: totsp(minTs),
		CnWant: &catalogDropTs,
		Table:  &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_TABLES_ID},
	}, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(resp.Commands)) // insert + delete
	require.Equal(t, api.Entry_Insert, resp.Commands[0].EntryType)
	require.Equal(t, len(catalog2.SystemTableSchema.ColDefs)+1, len(resp.Commands[0].Bat.Vecs))
	check_same_rows(resp.Commands[0].Bat, 2)                                 // 2 tables
	relname, err := vector.ProtoVectorToVector(resp.Commands[0].Bat.Vecs[3]) // relname column
	require.NoError(t, err)
	require.Equal(t, schema.Name, relname.UnsafeGetStringAt(0))
	require.Equal(t, schema.Name, relname.UnsafeGetStringAt(1))

	require.Equal(t, api.Entry_Delete, resp.Commands[1].EntryType)
	require.Equal(t, fixedColCnt+1, len(resp.Commands[1].Bat.Vecs))
	check_same_rows(resp.Commands[1].Bat, 1) // 1 drop table
	close()

	// get columns catalog change
	resp, close, err = logtail.HandleSyncLogTailReq(p.Ctx, new(dummyCpkGetter), tae.LogtailMgr, tae.Catalog, api.SyncLogTailReq{
		CnHave: totsp(minTs),
		CnWant: &catalogDropTs,
		Table:  &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_COLUMNS_ID},
	}, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(resp.Commands)) // insert + delete
	require.Equal(t, api.Entry_Insert, resp.Commands[0].EntryType)
	require.Equal(t, len(catalog2.SystemColumnSchema.ColDefs)+1, len(resp.Commands[0].Bat.Vecs))
	check_same_rows(resp.Commands[0].Bat, len(schema.ColDefs)*2) // column count of 2 tables
	close()

	// get user table change
	resp, close, err = logtail.HandleSyncLogTailReq(p.Ctx, new(dummyCpkGetter), tae.LogtailMgr, tae.Catalog, api.SyncLogTailReq{
		CnHave: totsp(firstWriteTs.Next()), // skip the first write deliberately,
		CnWant: totsp(lastWriteTs),
		Table:  &api.TableID{DbId: dbID, TbId: tableID},
	}, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(resp.Commands)) // 2 insert data and delete data

	// check data change
	insDataEntry := resp.Commands[0]
	require.Equal(t, api.Entry_Insert, insDataEntry.EntryType)
	require.Equal(t, len(schema.ColDefs)+1, len(insDataEntry.Bat.Vecs)) // 5 columns, rowid + commit ts + 2 visibile
	check_same_rows(insDataEntry.Bat, 99)                               // 99 rows, because the first write is excluded.
	// test first user col, this is probably fragile, it depends on the details of MockSchema
	// if something changes, delete this is okay.
	firstCol, err := vector.ProtoVectorToVector(insDataEntry.Bat.Vecs[2]) // mock_0 column, int8 type
	require.Equal(t, types.T_int8, firstCol.GetType().Oid)
	require.NoError(t, err)

	delDataEntry := resp.Commands[1]
	require.Equal(t, api.Entry_Delete, delDataEntry.EntryType)
	require.Equal(t, fixedColCnt+1, len(delDataEntry.Bat.Vecs)) // 3 columns, rowid + commit_ts + aborted
	check_same_rows(delDataEntry.Bat, 10)

	// check delete rowids are exactly what we want
	rowids, err := vector.ProtoVectorToVector(delDataEntry.Bat.Vecs[0])
	require.NoError(t, err)
	require.Equal(t, types.T_Rowid, rowids.GetType().Oid)
	rowidMap := make(map[types.Rowid]int)
	for _, id := range deleteRowIDs {
		rowidMap[id] = 1
	}
	for i := int64(0); i < 10; i++ {
		id := vector.MustFixedCol[types.Rowid](rowids)[i]
		rowidMap[id] = rowidMap[id] + 1
	}
	require.Equal(t, 10, len(rowidMap))
	for _, v := range rowidMap {
		require.Equal(t, 2, v)
	}
	close()
}

func TestAlterTableBasic(t *testing.T) {

	opts := config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()
	tae := p.T.GetDB()

	schema := catalog2.MockSchemaAll(2, -1)
	schema.Name = "test"
	schema.Constraint = []byte("")
	initComment := "comment version; rows:10; blks:2"
	schema.Comment = initComment

	txnop := p.StartCNTxn()
	p.CreateDBAndTable(txnop, "db", schema)
	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	db, _ := p.D.Engine.Database(p.Ctx, "db", txnop)
	tbl, _ := db.Relation(p.Ctx, "test", nil)
	cstr1 := &engine.ConstraintDef{
		Cts: []engine.Constraint{&engine.RefChildTableDef{Tables: []uint64{1}}},
	}
	err := tbl.UpdateConstraint(p.Ctx, cstr1)
	require.NoError(t, err)
	err = tbl.AlterTable(p.Ctx, nil, []*api.AlterTableReq{
		api.NewUpdateCommentReq(tbl.GetDBID(p.Ctx), tbl.GetTableID(p.Ctx), "comment version 1")})
	require.NoError(t, err)
	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	db, _ = p.D.Engine.Database(p.Ctx, "db", txnop)
	tbl, _ = db.Relation(p.Ctx, "test", nil)
	cstr2 := &engine.ConstraintDef{
		Cts: []engine.Constraint{&engine.RefChildTableDef{Tables: []uint64{1, 2}}},
	}
	err = tbl.UpdateConstraint(p.Ctx, cstr2)
	require.NoError(t, err)
	require.NoError(t, txnop.Commit(p.Ctx))

	resp, close, _ := logtail.HandleSyncLogTailReq(p.Ctx, new(dummyCpkGetter), tae.LogtailMgr, tae.Catalog, api.SyncLogTailReq{
		CnHave: totsp(types.BuildTS(0, 0)),
		CnWant: totsp(types.MaxTs()),
		Table:  &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_TABLES_ID},
	}, true)

	bat, _ := batch.ProtoBatchToBatch(resp.Commands[0].Bat)
	cstrCol := containers.NewNonNullBatchWithSharedMemory(bat, common.DefaultAllocator).GetVectorByName(catalog.SystemRelAttr_Constraint)
	require.Equal(t, 3, cstrCol.Length())
	c1, err := cstr1.MarshalBinary()
	require.NoError(t, err)
	c2, err := cstr2.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, []byte{}, cstrCol.Get(0).([]byte))
	require.Equal(t, c1, cstrCol.Get(1).([]byte))
	require.Equal(t, c2, cstrCol.Get(2).([]byte))

	commetCol := containers.NewNonNullBatchWithSharedMemory(bat, common.DefaultAllocator).GetVectorByName(catalog.SystemRelAttr_Comment)
	require.Equal(t, 3, cstrCol.Length())
	require.Equal(t, []byte(initComment), commetCol.Get(0).([]byte))
	require.Equal(t, []byte("comment version 1"), commetCol.Get(1).([]byte))
	require.Equal(t, []byte("comment version 1"), commetCol.Get(2).([]byte))

	require.Equal(t, api.Entry_Delete, resp.Commands[1].EntryType)

	close()

	txnop = p.StartCNTxn()
	db, _ = p.D.Engine.Database(p.Ctx, "db", txnop)
	require.NoError(t, db.Delete(p.Ctx, "test"))
	require.NoError(t, txnop.Commit(p.Ctx))

	resp, close, _ = logtail.HandleSyncLogTailReq(p.Ctx, new(dummyCpkGetter), tae.LogtailMgr, tae.Catalog, api.SyncLogTailReq{
		CnHave: totsp(types.BuildTS(0, 0)),
		CnWant: totsp(types.MaxTs()),
		Table:  &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_COLUMNS_ID},
	}, true)

	require.Equal(t, 2, len(resp.Commands)) // create and drop
	require.Equal(t, api.Entry_Insert, resp.Commands[0].EntryType)
	require.Equal(t, api.Entry_Delete, resp.Commands[1].EntryType)
	close()
}

func TestColumnsTransfer(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	dir := testutil.MakeDefaultTestPath("partition_state", t)
	opts.Fs = objectio.TmpNewSharedFileservice(context.Background(), dir)
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()
	tae := p.T.GetDB()

	schema := catalog2.MockSchemaAll(8188, -1)
	schema.Name = "test"

	schema2 := catalog2.MockSchemaAll(10, -1)
	schema2.Name = "todrop"

	txnop := p.StartCNTxn()
	p.CreateDBAndTables(txnop, "db", schema, schema2)
	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	txnop.GetWorkspace().StartStatement()
	p.DeleteTableInDB(txnop, "db", schema2.Name)

	txn, _ := tae.StartTxn(nil)
	catalogDB, _ := txn.GetDatabaseByID(catalog.MO_CATALOG_ID)
	columnsTbl, _ := catalogDB.GetRelationByID(catalog.MO_COLUMNS_ID)

	worker := ops.NewOpWorker(context.Background(), "xx")
	worker.Start()
	defer worker.Stop()

	it := columnsTbl.MakeObjectIt()
	it.Next()
	firstEntry := it.GetObject().GetMeta().(*catalog2.ObjectEntry)
	t.Log(firstEntry.ID().ShortStringEx())
	task1, err := jobs.NewFlushTableTailTask(
		tasks.WaitableCtx, txn,
		[]*catalog2.ObjectEntry{firstEntry},
		tae.Runtime, txn.GetStartTS())
	require.NoError(t, err)
	worker.SendOp(task1)
	err = task1.WaitDone(context.Background())
	require.NoError(t, err)

	require.NoError(t, txn.Commit(p.Ctx))

	time.Sleep(200 * time.Millisecond)
	ctx := context.WithValue(p.Ctx, disttae.UT_ForceTransCheck{}, 42)
	require.NoError(t, txnop.GetWorkspace().IncrStatementID(ctx, true))
	require.NoError(t, txnop.Commit(p.Ctx))

}

func TestCacheGC(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()

	schema1 := catalog2.MockSchemaAll(13, 3)
	schema1.Name = "test1"
	schema2 := catalog2.MockSchemaAll(13, 3)
	schema2.Name = "test2"
	schema3 := catalog2.MockSchemaAll(13, 3)
	schema3.Name = "test3"
	schema4 := catalog2.MockSchemaAll(13, 3)
	schema4.Name = "test4"

	txnop := p.StartCNTxn()
	p.CreateDBAndTables(txnop, "db", schema1, schema2, schema3)
	require.NoError(t, txnop.Commit(p.Ctx))

	// test1: I D I | gc 2
	txnop = p.StartCNTxn()
	p.DeleteTableInDB(txnop, "db", schema1.Name)
	require.NoError(t, txnop.Commit(p.Ctx))
	txnop = p.StartCNTxn()
	p.CreateTableInDB(txnop, "db", schema1)
	require.NoError(t, txnop.Commit(p.Ctx))

	// test2: I D   | gc 2
	txnop = p.StartCNTxn()
	p.DeleteTableInDB(txnop, "db", schema2.Name)
	require.NoError(t, txnop.Commit(p.Ctx))

	// test3 I      | gc 0

	gcTime := txnop.Txn().CommitTS.Next()

	// test4        | I D I gc 0
	txnop = p.StartCNTxn()
	p.CreateTableInDB(txnop, "db", schema4)
	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	p.DeleteTableInDB(txnop, "db", schema4.Name)
	require.NoError(t, txnop.Commit(p.Ctx))

	txnop = p.StartCNTxn() // wait the last txn to be committed
	require.NoError(t, txnop.Commit(p.Ctx))

	// gc
	cc := p.D.Engine.GetLatestCatalogCache()
	r := cc.GC(gcTime)
	require.Equal(t, 7 /*because of three tables inserted at 0 time*/, r.TStaleItem)
	require.Equal(t, 2 /*test2 & test 4*/, r.TDelCpk)

}

func TestShowDatabasesInRestoreTxn(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer p.Close()
	tae := p.T.GetDB()

	schema := catalog2.MockSchemaAll(10, -1)
	schema.Name = "test"
	txnop := p.StartCNTxn()
	p.CreateDBAndTable(txnop, "db", schema)
	require.NoError(t, txnop.Commit(p.Ctx))

	ts := time.Now().UTC().UnixNano()

	time.Sleep(10 * time.Millisecond)

	schema2 := catalog2.MockSchemaAll(10, -1)
	schema2.Name = "test2"
	txnop = p.StartCNTxn()
	p.CreateDBAndTable(txnop, "db2", schema)
	require.NoError(t, txnop.Commit(p.Ctx))

	txn, _ := tae.StartTxn(nil)
	catalogDB, _ := txn.GetDatabaseByID(catalog.MO_CATALOG_ID)
	dbTbl, _ := catalogDB.GetRelationByID(catalog.MO_DATABASE_ID)

	worker := ops.NewOpWorker(context.Background(), "xx")
	worker.Start()
	defer worker.Stop()

	it := dbTbl.MakeObjectIt()
	it.Next()
	firstEntry := it.GetObject().GetMeta().(*catalog2.ObjectEntry)
	task1, err := jobs.NewFlushTableTailTask(
		tasks.WaitableCtx, txn,
		[]*catalog2.ObjectEntry{firstEntry},
		tae.Runtime, txn.GetStartTS())
	require.NoError(t, err)
	worker.SendOp(task1)
	err = task1.WaitDone(context.Background())
	require.NoError(t, err)

	require.NoError(t, txn.Commit(p.Ctx))

	txnop = p.StartCNTxn()
	require.NoError(t, p.D.Engine.Delete(p.Ctx, "db2", txnop))
	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		panic(fmt.Sprintf("missing sql executor in service %q", ""))
	}
	exec := v.(executor.SQLExecutor)
	res, err := exec.Exec(p.Ctx, fmt.Sprintf("show databases {MO_TS=%d}", ts), executor.Options{}.WithTxn(txnop))
	require.NoError(t, err)
	var rels []string
	for _, b := range res.Batches {
		for i, v := 0, b.Vecs[0]; i < v.Length(); i++ {
			rels = append(rels, v.GetStringAt(i))
		}
	}
	require.Equal(t, 2, len(rels), rels) // mo_catalog + db
	require.NotContains(t, rels, "db2")
	// res, err = exec.Exec(p.Ctx, "show databases", executor.Options{}.WithTxn(txnop))
	// var brels []string
	// for _, b := range res.Batches {
	// 	for i, v := 0, b.Vecs[0]; i < v.Length(); i++ {
	// 		brels = append(brels, v.GetStringAt(i))
	// 	}
	// }
	// require.NoError(t, err)
	// t.Log(rels, brels)

}
