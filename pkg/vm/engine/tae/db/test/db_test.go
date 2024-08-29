// Copyright 2021 Matrix Origin
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sort"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	gc "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	ops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"

	"net/http"
	_ "net/http/pprof"
)

func init() {
	go http.ListenAndServe("0.0.0.0:6060", nil)
}

const (
	ModuleName               = "TAEDB"
	smallCheckpointBlockRows = 10
	smallCheckpointSize      = 1024
)

func TestAppend1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.NewTestEngine(ctx, ModuleName, t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(14, 3)
	schema.BlockMaxRows = options.DefaultBlockMaxRows
	schema.ObjectMaxBlocks = options.DefaultBlocksPerObject
	tae.BindSchema(schema)
	data := catalog.MockBatch(schema, int(schema.BlockMaxRows*2))
	defer data.Close()
	bats := data.Split(4)
	now := time.Now()
	tae.CreateRelAndAppend(bats[0], true)
	t.Log(time.Since(now))
	tae.CheckRowsByScan(bats[0].Length(), false)

	txn, rel := tae.GetRelation()
	err := rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	// FIXME
	// testutil.CheckAllColRowsByScan(t, rel, bats[0].Length()+bats[1].Length(), false)
	err = rel.Append(context.Background(), bats[2])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
	tae.CheckRowsByScan(bats[0].Length()+bats[1].Length()+bats[2].Length(), false)
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestAppend2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	db := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer db.Close()

	// this task won't affect logic of TestAppend2, it just prints logs about dirty count
	// forest := logtail.NewDirtyCollector(db.LogtailMgr, opts.Clock, db.Catalog, new(catalog.LoopProcessor))
	// hb := ops.NewHeartBeaterWithFunc(5*time.Millisecond, func() {
	// 	forest.Run()
	// 	t.Log(forest.String())
	// }, nil)
	// hb.Start()
	// defer hb.Stop()

	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 10
	testutil.CreateRelation(t, db, "db", schema, true)

	totalRows := uint64(schema.BlockMaxRows * 30)
	bat := catalog.MockBatch(schema, int(totalRows))
	defer bat.Close()
	bats := bat.Split(100)

	var wg sync.WaitGroup
	pool, _ := ants.NewPool(80)
	defer pool.Release()

	start := time.Now()
	for _, data := range bats {
		wg.Add(1)
		err := pool.Submit(testutil.AppendClosure(t, data, schema.Name, db, &wg))
		assert.Nil(t, err)
	}
	wg.Wait()
	t.Logf("Append %d rows takes: %s", totalRows, time.Since(start))
	{
		txn, rel := testutil.GetDefaultRelation(t, db, schema.Name)
		testutil.CheckAllColRowsByScan(t, rel, int(totalRows), false)
		assert.NoError(t, txn.Commit(context.Background()))
	}
	t.Log(db.Catalog.SimplePPString(common.PPL1))

	now := time.Now()
	testutils.WaitExpect(20000, func() bool {
		return db.Runtime.Scheduler.GetPenddingLSNCnt() == 0
	})
	t.Log(time.Since(now))
	t.Logf("Checkpointed: %d", db.Runtime.Scheduler.GetCheckpointedLSN())
	t.Logf("GetPenddingLSNCnt: %d", db.Runtime.Scheduler.GetPenddingLSNCnt())
	assert.Equal(t, uint64(0), db.Runtime.Scheduler.GetPenddingLSNCnt())
	t.Log(db.Catalog.SimplePPString(common.PPL1))
	wg.Add(1)
	testutil.AppendFailClosure(t, bats[0], schema.Name, db, &wg)()
	wg.Wait()
}

func TestAppend3(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchema(2, 0)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	testutil.CreateRelation(t, tae, "db", schema, true)
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows))
	defer bat.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	testutil.AppendClosure(t, bat, schema.Name, tae, &wg)()
	wg.Wait()
	testutils.WaitExpect(2000, func() bool {
		return tae.Runtime.Scheduler.GetPenddingLSNCnt() == 0
	})
	// t.Log(tae.Catalog.SimplePPString(common.PPL3))
	wg.Add(1)
	testutil.AppendFailClosure(t, bat, schema.Name, tae, &wg)()
	wg.Wait()
}

func TestAppend4(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema1 := catalog.MockSchemaAll(18, 14)
	schema2 := catalog.MockSchemaAll(18, 15)
	schema3 := catalog.MockSchemaAll(18, 16)
	schema4 := catalog.MockSchemaAll(18, 11)
	schema1.BlockMaxRows = 10
	schema2.BlockMaxRows = 10
	schema3.BlockMaxRows = 10
	schema4.BlockMaxRows = 10
	schema1.ObjectMaxBlocks = 2
	schema2.ObjectMaxBlocks = 2
	schema3.ObjectMaxBlocks = 2
	schema4.ObjectMaxBlocks = 2
	schemas := []*catalog.Schema{schema1, schema2, schema3, schema4}
	testutil.CreateDB(t, tae, testutil.DefaultTestDB)
	for _, schema := range schemas {
		bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*3-1))
		defer bat.Close()
		bats := bat.Split(1)
		testutil.CreateRelation(t, tae, testutil.DefaultTestDB, schema, false)
		for i := range bats {
			txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
			err := rel.Append(context.Background(), bats[i])
			assert.NoError(t, err)
			err = txn.Commit(context.Background())
			assert.NoError(t, err)
		}
		txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
		testutil.CheckAllColRowsByScan(t, rel, bat.Length(), false)

		v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(3)
		filter := handle.NewEQFilter(v)
		err := rel.DeleteByFilter(context.Background(), filter)
		assert.NoError(t, err)
		err = txn.Commit(context.Background())
		assert.NoError(t, err)

		txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
		testutil.CheckAllColRowsByScan(t, rel, bat.Length()-1, true)
		err = txn.Commit(context.Background())
		assert.NoError(t, err)
		testutil.CompactBlocks(t, 0, tae, testutil.DefaultTestDB, schema, false)
		txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
		testutil.CheckAllColRowsByScan(t, rel, bat.Length()-1, false)
		err = txn.Commit(context.Background())
		assert.NoError(t, err)
	}
}

func testCRUD(t *testing.T, tae *db.DB, schema *catalog.Schema) {
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*(uint32(schema.ObjectMaxBlocks)+1)-1))
	defer bat.Close()
	bats := bat.Split(4)

	var updateColIdx int
	if schema.GetSingleSortKeyIdx() >= 17 {
		updateColIdx = 0
	} else {
		updateColIdx = schema.GetSingleSortKeyIdx() + 1
	}

	testutil.CreateRelationAndAppend(t, 0, tae, testutil.DefaultTestDB, schema, bats[0], false)

	txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
	err := rel.Append(context.Background(), bats[0])
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length(), false)
	v := bats[0].Vecs[schema.GetSingleSortKeyIdx()].Get(2)
	filter := handle.NewEQFilter(v)
	err = rel.DeleteByFilter(context.Background(), filter)
	assert.NoError(t, err)

	oldv := bats[0].Vecs[updateColIdx].Get(5)
	oldvIsNull := bats[0].Vecs[updateColIdx].IsNull(5)

	v = bats[0].Vecs[schema.GetSingleSortKeyIdx()].Get(5)
	ufilter := handle.NewEQFilter(v)
	{
		ot := reflect.ValueOf(&oldv).Elem()
		nv := reflect.ValueOf(int8(99))
		if nv.CanConvert(reflect.TypeOf(oldv)) {
			ot.Set(nv.Convert(reflect.TypeOf(oldv)))
		}
	}
	err = rel.UpdateByFilter(context.Background(), ufilter, uint16(updateColIdx), oldv, oldvIsNull)
	assert.NoError(t, err)

	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length()-1, true)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length()-1, true)
	for _, b := range bats[1:] {
		err = rel.Append(context.Background(), b)
		assert.NoError(t, err)
	}
	testutil.CheckAllColRowsByScan(t, rel, bat.Length()-1, true)
	assert.NoError(t, txn.Commit(context.Background()))

	testutil.CompactBlocks(t, 0, tae, testutil.DefaultTestDB, schema, false)

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, bat.Length()-1, false)
	v = bats[0].Vecs[schema.GetSingleSortKeyIdx()].Get(3)
	filter = handle.NewEQFilter(v)
	err = rel.DeleteByFilter(context.Background(), filter)
	assert.NoError(t, err)
	testutil.CheckAllColRowsByScan(t, rel, bat.Length()-2, true)
	assert.NoError(t, txn.Commit(context.Background()))

	// After merging blocks, the logic of read data is modified
	//compactObjs(t, tae, schema)

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	//testutil.CheckAllColRowsByScan(t, rel, bat.Length()-2, false)
	testutil.CheckAllColRowsByScan(t, rel, bat.Length()-1, false)
	assert.NoError(t, txn.Commit(context.Background()))

	// t.Log(rel.GetMeta().(*catalog.TableEntry).PPString(common.PPL1, 0, ""))
	txn, err = tae.StartTxn(nil)
	assert.NoError(t, err)
	db, err := txn.GetDatabase(testutil.DefaultTestDB)
	assert.NoError(t, err)
	_, err = db.DropRelationByName(schema.Name)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestCRUD(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer tae.Close()
	testutil.CreateDB(t, tae, testutil.DefaultTestDB)
	testutil.WithTestAllPKType(t, tae, testCRUD)
}

func TestTableHandle(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	db := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer db.Close()

	schema := catalog.MockSchema(2, 0)
	schema.BlockMaxRows = 1000
	schema.ObjectMaxBlocks = 2

	txn, _ := db.StartTxn(nil)
	database, _ := txn.CreateDatabase("db", "", "")
	rel, _ := database.CreateRelation(schema)

	tableMeta := rel.GetMeta().(*catalog.TableEntry)
	t.Log(tableMeta.String())
	table := tableMeta.GetTableData()

	handle := table.GetHandle(false)
	appender, err := handle.GetAppender()
	assert.Nil(t, appender)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrAppendableObjectNotFound))
}

func TestCreateBlock(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	db := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer db.Close()

	txn, _ := db.StartTxn(nil)
	database, _ := txn.CreateDatabase("db", "", "")
	schema := catalog.MockSchemaAll(13, 12)
	rel, err := database.CreateRelation(schema)
	assert.Nil(t, err)
	_, err = rel.CreateObject(false)
	assert.Nil(t, err)

	t.Log(db.Catalog.SimplePPString(common.PPL1))
	assert.Nil(t, txn.Commit(context.Background()))
	t.Log(db.Catalog.SimplePPString(common.PPL1))
}

func TestNonAppendableBlock(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	db := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer db.Close()
	schema := catalog.MockSchemaAll(13, 1)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2

	bat := catalog.MockBatch(schema, 8)
	defer bat.Close()

	testutil.CreateRelation(t, db, "db", schema, true)

	{
		txn, _ := db.StartTxn(nil)
		database, err := txn.GetDatabase("db")
		assert.Nil(t, err)
		rel, err := database.GetRelationByName(schema.Name)
		readSchema := rel.Schema(false)
		assert.Nil(t, err)
		obj, err := rel.CreateNonAppendableObject(false, nil)
		assert.Nil(t, err)
		dataBlk := obj.GetMeta().(*catalog.ObjectEntry).GetObjectData()
		name := objectio.BuildObjectNameWithObjectID(obj.GetID())
		writer, err := blockio.NewBlockWriterNew(dataBlk.GetFs().Service, name, 0, nil)
		assert.Nil(t, err)
		_, err = writer.WriteBatch(containers.ToCNBatch(bat))
		assert.Nil(t, err)
		_, _, err = writer.Sync(context.Background())
		assert.Nil(t, err)
		obj.UpdateStats(writer.Stats())
		v, _, err := dataBlk.GetValue(context.Background(), txn, readSchema, 0, 4, 2, false, common.DefaultAllocator)
		assert.Nil(t, err)
		expectVal := bat.Vecs[2].Get(4)
		assert.Equal(t, expectVal, v)

		var view *containers.Batch
		tbl := rel.GetMeta().(*catalog.TableEntry)
		blkID := objectio.NewBlockidWithObjectID(obj.GetID(), 0)
		err = tables.HybridScanByBlock(context.Background(), tbl, txn, &view, schema, []int{2}, blkID, common.DefaultAllocator)
		assert.Nil(t, err)
		assert.Nil(t, view.Deletes)
		assert.Equal(t, bat.Vecs[2].Length(), view.Length())
		view.Close()
		view = nil

		pkDef := schema.GetPrimaryKey()
		pkVec := containers.MakeVector(pkDef.Type, common.DefaultAllocator)
		val1, _, err := dataBlk.GetValue(ctx, txn, schema, 0, 1, pkDef.Idx, false, common.DefaultAllocator)
		assert.NoError(t, err)
		pkVec.Append(val1, false)
		val2, _, err := dataBlk.GetValue(ctx, txn, schema, 0, 2, pkDef.Idx, false, common.DefaultAllocator)
		assert.NoError(t, err)
		pkVec.Append(val2, false)
		err = rel.RangeDelete(obj.Fingerprint(), 1, 2, handle.DT_Normal)
		assert.Nil(t, err)

		err = tables.HybridScanByBlock(ctx, tbl, txn, &view, schema, []int{2}, blkID, common.DefaultAllocator)
		assert.Nil(t, err)
		assert.True(t, view.Deletes.Contains(1))
		assert.True(t, view.Deletes.Contains(2))
		assert.Equal(t, bat.Vecs[2].Length(), view.Length())
		view.Close()
		view = nil

		// _, err = dataBlk.Update(txn, 3, 2, int32(999))
		// assert.Nil(t, err)

		err = tables.HybridScanByBlock(ctx, tbl, txn, &view, schema, []int{2}, blkID, common.DefaultAllocator)
		assert.Nil(t, err)
		assert.True(t, view.Deletes.Contains(1))
		assert.True(t, view.Deletes.Contains(2))
		assert.Equal(t, bat.Vecs[2].Length(), view.Length())
		// v = view.Vecs[0].Get(3)
		// assert.Equal(t, int32(999), v)
		view.Close()

		assert.Nil(t, txn.Commit(context.Background()))
	}
}

func TestCreateObject(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(1, 0)
	txn, _ := tae.StartTxn(nil)
	db, err := txn.CreateDatabase("db", "", "")
	assert.Nil(t, err)
	_, err = db.CreateRelation(schema)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	bat := catalog.MockBatch(schema, 5)
	defer bat.Close()
	testutil.AppendClosure(t, bat, schema.Name, tae, nil)()

	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	rel, _ := db.GetRelationByName(schema.Name)
	obj, err := rel.CreateNonAppendableObject(false, nil)
	assert.Nil(t, err)
	testutil.MockObjectStats(t, obj)
	assert.Nil(t, txn.Commit(context.Background()))

	objCnt := 0
	processor := new(catalog.LoopProcessor)
	processor.ObjectFn = func(Object *catalog.ObjectEntry) error {
		objCnt++
		return nil
	}
	err = tae.Catalog.RecurLoop(processor)
	assert.Nil(t, err)
	assert.Equal(t, 2, objCnt)
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestAddObjsWithMetaLoc(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	db := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer db.Close()

	worker := ops.NewOpWorker(context.Background(), "xx")
	worker.Start()
	defer worker.Stop()
	schema := catalog.MockSchemaAll(13, 2)
	schema.Name = "tb-0"
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*4))
	defer bat.Close()
	bats := bat.Split(4)
	{
		txn, _, rel := testutil.CreateRelationNoCommit(t, db, "db", schema, true)
		err := rel.Append(context.Background(), bats[0])
		assert.NoError(t, err)
		err = rel.Append(context.Background(), bats[1])
		assert.NoError(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	//compact blocks
	var newBlockFp1 *common.ID
	var stats1 objectio.ObjectStats
	var newBlockFp2 *common.ID
	var stats2 objectio.ObjectStats
	var metaLoc1 objectio.Location
	{
		txn, rel := testutil.GetRelation(t, 0, db, "db", schema.Name)
		it := rel.MakeObjectIt(false)
		it.Next()
		blkMeta1 := it.GetObject().GetMeta().(*catalog.ObjectEntry)
		it.Next()
		blkMeta2 := it.GetObject().GetMeta().(*catalog.ObjectEntry)
		it.Close()

		task1, err := jobs.NewFlushTableTailTask(tasks.WaitableCtx, txn, []*catalog.ObjectEntry{blkMeta1, blkMeta2}, nil, db.Runtime)
		assert.NoError(t, err)
		worker.SendOp(task1)
		err = task1.WaitDone(context.Background())
		assert.NoError(t, err)
		newBlockFp1 = task1.GetCreatedObjects().Fingerprint()
		stats1 = task1.GetCreatedObjects().GetMeta().(*catalog.ObjectEntry).GetLatestNode().GetObjectStats()
		metaLoc1 = task1.GetCreatedObjects().GetMeta().(*catalog.ObjectEntry).GetLocation()
		metaLoc1.SetID(0)
		metaLoc1.SetRows(schema.BlockMaxRows)
		newBlockFp2 = task1.GetCreatedObjects().Fingerprint()
		newBlockFp2.SetBlockOffset(1)
		stats2 = task1.GetCreatedObjects().GetMeta().(*catalog.ObjectEntry).GetLatestNode().GetObjectStats()
		assert.Nil(t, txn.Commit(context.Background()))
	}
	//read new non-appendable block data and check
	{
		txn, rel := testutil.GetRelation(t, 0, db, "db", schema.Name)
		assert.True(t, newBlockFp2.ObjectID().Eq(*newBlockFp1.ObjectID()))
		obj, err := rel.GetObject(newBlockFp1.ObjectID(), false)
		assert.Nil(t, err)
		var view *containers.Batch
		err = obj.Scan(ctx, &view, 0, []int{2}, common.DefaultAllocator)
		assert.NoError(t, err)
		assert.True(t, view.Vecs[0].Equals(bats[0].Vecs[2]))
		view.Close()
		view = nil

		err = obj.Scan(ctx, &view, 1, []int{2}, common.DefaultAllocator)
		assert.NoError(t, err)
		assert.True(t, view.Vecs[0].Equals(bats[1].Vecs[2]))
		view.Close()
		assert.Nil(t, txn.Commit(context.Background()))
	}

	{

		schema = catalog.MockSchemaAll(13, 2)
		schema.Name = "tb-1"
		schema.BlockMaxRows = 20
		schema.ObjectMaxBlocks = 2
		txn, _, rel := testutil.CreateRelationNoCommit(t, db, "db", schema, false)
		txn.SetDedupType(txnif.DedupPolicy_SkipWorkspace)
		vec1 := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
		vec1.Append(stats1[:], false)
		defer vec1.Close()
		err := rel.AddObjsWithMetaLoc(context.Background(), vec1)
		assert.Nil(t, err)
		err = rel.Append(context.Background(), bats[0])
		assert.Nil(t, err)

		vec2 := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
		vec2.Append(stats2[:], false)
		defer vec1.Close()
		err = rel.AddObjsWithMetaLoc(context.Background(), vec2)
		assert.Nil(t, err)
		err = rel.Append(context.Background(), bats[1])
		assert.Nil(t, err)
		//err = rel.RangeDeleteLocal(start, end)
		//assert.Nil(t, err)
		//assert.True(t, rel.IsLocalDeleted(start, end))
		err = txn.Commit(context.Background())
		assert.Nil(t, err)

		//"tb-1" table now has one committed non-appendable Object which contains
		//two non-appendable block, and one committed appendable Object which contains two appendable block.

		//do deduplication check against sanpshot data.
		txn, rel = testutil.GetRelation(t, 0, db, "db", schema.Name)
		txn.SetDedupType(txnif.DedupPolicy_SkipWorkspace)
		err = rel.Append(context.Background(), bats[0])
		assert.NotNil(t, err)
		err = rel.Append(context.Background(), bats[1])
		assert.NotNil(t, err)

		vec3 := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
		vec3.Append(stats1[:], false)
		vec3.Append(stats2[:], false)
		defer vec1.Close()
		err = rel.AddObjsWithMetaLoc(context.Background(), vec3)
		assert.NotNil(t, err)

		//check blk count.
		t.Log(db.Catalog.SimplePPString(3))
		cntOfAblk := 0
		cntOfblk := 0
		testutil.ForEachObject(t, rel, func(blk handle.Object) (err error) {
			var view *containers.Batch
			if blk.IsAppendable() {
				err := blk.Scan(ctx, &view, 0, []int{3}, common.DefaultAllocator)
				assert.NoError(t, err)
				view.Close()
				cntOfAblk += blk.BlkCnt()
				return nil
			}
			metaLoc := blk.GetMeta().(*catalog.ObjectEntry).GetLocation()
			metaLoc.SetID(0)
			metaLoc.SetRows(schema.BlockMaxRows)
			assert.True(t, !metaLoc.IsEmpty())
			if bytes.Equal(metaLoc, metaLoc1) {
				err := blk.Scan(ctx, &view, 0, []int{2}, common.DefaultAllocator)
				assert.NoError(t, err)
				assert.True(t, view.Vecs[0].Equals(bats[0].Vecs[2]))
				view.Close()
			} else {
				err := blk.Scan(ctx, &view, 1, []int{3}, common.DefaultAllocator)
				assert.NoError(t, err)
				assert.True(t, view.Vecs[0].Equals(bats[1].Vecs[3]))
				view.Close()

			}
			cntOfblk += blk.BlkCnt()
			return
		})
		assert.Equal(t, 2, cntOfblk)
		assert.Equal(t, 2, cntOfAblk)
		assert.Nil(t, txn.Commit(context.Background()))

		//check count of committed Objects.
		cntOfAobj := 0
		cntOfobj := 0
		txn, rel = testutil.GetRelation(t, 0, db, "db", schema.Name)
		testutil.ForEachObject(t, rel, func(obj handle.Object) (err error) {
			if obj.IsAppendable() {
				cntOfAobj++
				return
			}
			cntOfobj++
			return
		})
		assert.True(t, cntOfobj == 1)
		assert.True(t, cntOfAobj == 2)
		assert.Nil(t, txn.Commit(context.Background()))
	}
}

func TestCompactMemAlter(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	db := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer db.Close()

	worker := ops.NewOpWorker(context.Background(), "xx")
	worker.Start()
	defer worker.Stop()
	schema := catalog.MockSchemaAll(5, 2)
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows))
	defer bat.Close()
	testutil.CreateRelationAndAppend(t, 0, db, "db", schema, bat, true)

	// Alter: add a column to the last
	{
		txn, rel := testutil.GetDefaultRelation(t, db, schema.Name)
		err := rel.AlterTable(context.TODO(), api.NewAddColumnReq(0, 0, "xyz", types.NewProtoType(types.T_char), 5))
		assert.NoError(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	var newBlockFp *common.ID
	{
		txn, rel := testutil.GetDefaultRelation(t, db, schema.Name)
		blkMeta := testutil.GetOneBlockMeta(rel)
		// ablk-0 & nablk-1
		task, err := jobs.NewFlushTableTailTask(tasks.WaitableCtx, txn, []*catalog.ObjectEntry{blkMeta}, nil, db.Runtime)
		assert.NoError(t, err)
		worker.SendOp(task)
		err = task.WaitDone(ctx)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
		newBlockFp = task.GetCreatedObjects().Fingerprint()
	}
	{
		txn, rel := testutil.GetDefaultRelation(t, db, schema.Name)
		obj, err := rel.GetObject(newBlockFp.ObjectID(), false)
		assert.Nil(t, err)
		for i := 0; i <= 5; i++ {
			var view *containers.Batch
			err := obj.Scan(ctx, &view, 0, []int{i}, common.DefaultAllocator)
			assert.NoError(t, err)
			if i < 5 {
				assert.Equal(t, bat.Vecs[i].GetType().Oid, view.Vecs[0].GetType().Oid)
			} else {
				assert.Equal(t, types.T_char.ToType().Oid, view.Vecs[0].GetType().Oid)
			}
			if i == 3 {
				assert.True(t, view.Vecs[0].Equals(bat.Vecs[3]))
			}
			view.Close()
		}
		assert.NoError(t, txn.Commit(context.Background()))
	}
}

func TestFlushTableMergeOrder(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	worker := ops.NewOpWorker(context.Background(), "xx")
	worker.Start()
	defer worker.Stop()

	schema := catalog.NewEmptySchema("test")
	schema.AppendCol("aa", types.T_int64.ToType())
	schema.AppendCol("bb", types.T_int32.ToType())
	schema.AppendFakePKCol()
	schema.BlockMaxRows = 78
	schema.ObjectMaxBlocks = 256
	assert.NoError(t, schema.Finalize(false))
	tae.BindSchema(schema)

	// new bacth for aa and bb vector, and fill aa and bb with some random values
	bat := containers.NewBatch()
	bat.AddVector("aa", containers.NewVector(types.T_int64.ToType()))
	bat.AddVector("bb", containers.NewVector(types.T_int32.ToType()))

	dedup := make(map[int32]bool)

	rows := 500

	for i := 0; i < rows; i++ {
		bb := int32(rand.Intn(100000))
		if _, ok := dedup[bb]; ok {
			continue
		} else {
			dedup[bb] = true
		}
		aa := int64(20000000 + bb)
		bat.Vecs[0].Append(aa, false)
		bat.Vecs[1].Append(bb, false)
	}

	defer bat.Close()
	testutil.CreateRelationAndAppend(t, 0, tae.DB, "db", schema, bat, true)

	{
		txn, rel := testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		it := rel.MakeObjectIt(false)
		for it.Next() {
			blk := it.GetObject()
			id := blk.Fingerprint()
			for i := 0; i < blk.BlkCnt(); i++ {
				id.SetBlockOffset(uint16(i))
				rel.RangeDelete(id, 0, 0, handle.DT_Normal)
				rel.RangeDelete(id, 3, 3, handle.DT_Normal)

			}
		}
		it.Close()
		assert.NoError(t, txn.Commit(context.Background()))
	}

	txn, rel := testutil.GetDefaultRelation(t, tae.DB, schema.Name)
	blkMetas := testutil.GetAllBlockMetas(rel, false)
	tombstoneMetas := testutil.GetAllBlockMetas(rel, true)
	task, err := jobs.NewFlushTableTailTask(tasks.WaitableCtx, txn, blkMetas, tombstoneMetas, tae.DB.Runtime)
	assert.NoError(t, err)
	worker.SendOp(task)
	err = task.WaitDone(ctx)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestFlushTableMergeOrderPK(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	worker := ops.NewOpWorker(context.Background(), "xx")
	worker.Start()
	defer worker.Stop()

	schema := catalog.NewEmptySchema("test")
	schema.AppendPKCol("aa", types.T_int64.ToType(), 0)
	schema.AppendCol("bb", types.T_int32.ToType())
	schema.BlockMaxRows = 78
	schema.ObjectMaxBlocks = 256
	assert.NoError(t, schema.Finalize(false))
	tae.BindSchema(schema)

	// new bacth for aa and bb vector, and fill aa and bb with some random values
	bat := containers.NewBatch()
	bat.AddVector("aa", containers.NewVector(types.T_int64.ToType()))
	bat.AddVector("bb", containers.NewVector(types.T_int32.ToType()))

	dedup := make(map[int32]bool)

	target := 500
	rows := 0

	for i := 0; i < target; i++ {
		bb := int32(rand.Intn(100000))
		if _, ok := dedup[bb]; ok {
			continue
		} else {
			dedup[bb] = true
		}
		rows++
		aa := int64(20000000 + bb)
		bat.Vecs[0].Append(aa, false)
		bat.Vecs[1].Append(bb, false)
	}

	defer bat.Close()
	testutil.CreateRelationAndAppend(t, 0, tae.DB, "db", schema, bat, true)

	deleted := 0
	{
		txn, rel := testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		for x := range dedup {
			err := rel.DeleteByFilter(context.Background(), handle.NewEQFilter(int64(x+20000000)))
			assert.NoError(t, err)
			deleted++
			if deleted > rows/2 {
				break
			}
		}
		assert.NoError(t, txn.Commit(context.Background()))
	}

	txn, rel := testutil.GetDefaultRelation(t, tae.DB, schema.Name)
	blkMetas := testutil.GetAllBlockMetas(rel, false)
	tombstoneMetas := testutil.GetAllBlockMetas(rel, true)
	task, err := jobs.NewFlushTableTailTask(tasks.WaitableCtx, txn, blkMetas, tombstoneMetas, tae.DB.Runtime)
	assert.NoError(t, err)
	worker.SendOp(task)
	err = task.WaitDone(ctx)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.Restart(ctx)
	tae.CheckRowsByScan(rows-deleted, true)
}

func TestFlushTableNoPk(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	// db := initDB(ctx, t, opts)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	worker := ops.NewOpWorker(context.Background(), "xx")
	worker.Start()
	defer worker.Stop()
	schema := catalog.MockSchemaAll(13, -1)
	schema.Name = "table"
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 10
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 2*(int(schema.BlockMaxRows)*2+int(schema.BlockMaxRows/2)))
	defer bat.Close()
	testutil.CreateRelationAndAppend(t, 0, tae.DB, "db", schema, bat, true)

	txn, rel := testutil.GetDefaultRelation(t, tae.DB, schema.Name)
	blkMetas := testutil.GetAllBlockMetas(rel, false)
	tombstoneMetas := testutil.GetAllBlockMetas(rel, true)
	task, err := jobs.NewFlushTableTailTask(tasks.WaitableCtx, txn, blkMetas, tombstoneMetas, tae.DB.Runtime)
	assert.NoError(t, err)
	worker.SendOp(task)
	err = task.WaitDone(ctx)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.Restart(ctx)
	tae.CheckRowsByScan(100, true)
}

func TestFlushTableErrorHandle(t *testing.T) {
	ctx := context.WithValue(context.Background(), jobs.TestFlushBailoutPos1{}, "bail")

	opts := config.WithLongScanAndCKPOpts(nil)
	opts.Ctx = ctx

	tae := testutil.NewTestEngine(context.Background(), ModuleName, t, opts)
	defer tae.Close()

	worker := ops.NewOpWorker(ctx, "xx")
	worker.Start()
	defer worker.Stop()
	schema := catalog.MockSchemaAll(13, 2)
	schema.Name = "table"
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 10
	bat := catalog.MockBatch(schema, (int(schema.BlockMaxRows)*2 + int(schema.BlockMaxRows/2)))

	txn, _ := tae.StartTxn(nil)
	txn.CreateDatabase("db", "", "")
	txn.Commit(ctx)

	createAndInsert := func() {
		testutil.CreateRelationAndAppend(t, 0, tae.DB, "db", schema, bat, false)
	}

	droptable := func() {
		txn, _ := tae.StartTxn(nil)
		d, _ := txn.GetDatabase("db")
		d.DropRelationByName(schema.Name)
		txn.Commit(ctx)
	}

	flushTable := func() {
		txn, rel := testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		blkMetas := testutil.GetAllBlockMetas(rel, false)
		tombstoneMetas := testutil.GetAllBlockMetas(rel, true)
		task, err := jobs.NewFlushTableTailTask(tasks.WaitableCtx, txn, blkMetas, tombstoneMetas, tae.Runtime)
		assert.NoError(t, err)
		worker.SendOp(task)
		err = task.WaitDone(ctx)
		assert.Error(t, err)
		assert.NoError(t, txn.Rollback(context.Background()))
	}
	for i := 0; i < 20; i++ {
		createAndInsert()
		flushTable()
		droptable()
	}
}

func TestFlushTableErrorHandle2(t *testing.T) {
	ctx := context.WithValue(context.Background(), jobs.TestFlushBailoutPos2{}, "bail")

	opts := config.WithLongScanAndCKPOpts(nil)
	opts.Ctx = ctx

	tae := testutil.NewTestEngine(context.Background(), ModuleName, t, opts)
	defer tae.Close()

	worker := ops.NewOpWorker(ctx, "xx")
	worker.Start()
	defer worker.Stop()
	goodworker := ops.NewOpWorker(context.Background(), "goodworker")
	goodworker.Start()
	defer goodworker.Stop()
	schema := catalog.MockSchemaAll(13, 2)
	schema.Name = "table"
	schema.BlockMaxRows = 20
	bats := catalog.MockBatch(schema, (int(schema.BlockMaxRows)*2 + int(schema.BlockMaxRows/2))).Split(2)
	bat1, bat2 := bats[0], bats[1]
	defer bat1.Close()
	defer bat2.Close()
	flushTable := func(worker *ops.OpWorker) {
		txn, rel := testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		blkMetas := testutil.GetAllAppendableMetas(rel, false)
		tombstoneMetas := testutil.GetAllAppendableMetas(rel, true)
		task, err := jobs.NewFlushTableTailTask(tasks.WaitableCtx, txn, blkMetas, tombstoneMetas, tae.Runtime)
		assert.NoError(t, err)
		worker.SendOp(task)
		err = task.WaitDone(ctx)
		if err != nil {
			t.Logf("flush task outter wait %v", err)
		}
		assert.NoError(t, txn.Commit(context.Background()))
	}
	testutil.CreateRelationAndAppend(t, 0, tae.DB, "db", schema, bat1, true)
	flushTable(goodworker)

	{
		txn, rel := testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		assert.NoError(t, rel.DeleteByFilter(context.Background(), handle.NewEQFilter(bat1.Vecs[2].Get(1))))
		assert.NoError(t, rel.Append(ctx, bat2))
		assert.NoError(t, txn.Commit(context.Background()))
	}

	flushTable(worker)
	t.Log(tae.Catalog.SimplePPString(common.PPL0))
}

func TestFlushTabletail(t *testing.T) {
	// TODO
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	// db := initDB(ctx, t, opts)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	worker := ops.NewOpWorker(context.Background(), "xx")
	worker.Start()
	defer worker.Stop()
	schema := catalog.MockSchemaAll(13, 2)
	schema.Name = "table"
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 10
	bats := catalog.MockBatch(schema, 2*(int(schema.BlockMaxRows)*2+int(schema.BlockMaxRows/2))).Split(2)
	bat := bats[0]  // 50 rows
	bat2 := bats[1] // 50 rows

	defer bat.Close()
	defer bat2.Close()
	testutil.CreateRelationAndAppend(t, 0, tae.DB, "db", schema, bat, true)

	{
		txn, rel := testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		assert.NoError(t, rel.DeleteByFilter(context.Background(), handle.NewEQFilter(bat.Vecs[2].Get(1))))
		assert.NoError(t, rel.DeleteByFilter(context.Background(), handle.NewEQFilter(bat.Vecs[2].Get(19)))) // ab0 has 2
		assert.NoError(t, rel.DeleteByFilter(context.Background(), handle.NewEQFilter(bat.Vecs[2].Get(21)))) // ab1 has 1
		assert.NoError(t, rel.DeleteByFilter(context.Background(), handle.NewEQFilter(bat.Vecs[2].Get(45)))) // ab2 has 1

		assert.NoError(t, txn.Commit(context.Background()))
	}

	var commitDeleteAfterFlush txnif.AsyncTxn
	{
		var rel handle.Relation
		commitDeleteAfterFlush, rel = testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		assert.NoError(t, rel.DeleteByFilter(context.Background(), handle.NewEQFilter(bat.Vecs[2].Get(42)))) // expect to transfer to nablk1
	}

	flushTable := func() {
		txn, rel := testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		blkMetas := testutil.GetAllAppendableMetas(rel, false)
		tombstoneMetas := testutil.GetAllAppendableMetas(rel, true)
		task, err := jobs.NewFlushTableTailTask(tasks.WaitableCtx, txn, blkMetas, tombstoneMetas, tae.Runtime)
		assert.NoError(t, err)
		worker.SendOp(task)
		err = task.WaitDone(ctx)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
	}

	flushTable()

	{
		assert.NoError(t, commitDeleteAfterFlush.Commit(context.Background()))
		txn, rel := testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		_, _, err := rel.GetByFilter(context.Background(), handle.NewEQFilter(bat.Vecs[2].Get(42)))
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound))

		assert.NoError(t, rel.Append(context.Background(), bat2))
		assert.NoError(t, txn.Commit(context.Background()))
	}
	{
		txn, rel := testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		assert.NoError(t, rel.DeleteByFilter(context.Background(), handle.NewEQFilter(bat.Vecs[2].Get(15))))
		assert.NoError(t, rel.DeleteByFilter(context.Background(), handle.NewEQFilter(bat.Vecs[2].Get(20)))) // nab0 has 2
		assert.NoError(t, rel.DeleteByFilter(context.Background(), handle.NewEQFilter(bat.Vecs[2].Get(27)))) // nab1 has 2
		assert.NoError(t, rel.DeleteByFilter(context.Background(), handle.NewEQFilter(bat2.Vecs[2].Get(11))))
		assert.NoError(t, rel.DeleteByFilter(context.Background(), handle.NewEQFilter(bat2.Vecs[2].Get(15)))) // ab3 has 2, ab4 and ab5 has 0
		assert.NoError(t, txn.Commit(context.Background()))
	}

	flushTable()

	{
		txn, rel := testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		assert.NoError(t, rel.DeleteByFilter(context.Background(), handle.NewEQFilter(bat.Vecs[2].Get(10)))) // nab0 has 2+1, nab1 has 2
		assert.NoError(t, rel.DeleteByFilter(context.Background(), handle.NewEQFilter(bat2.Vecs[2].Get(44))))
		assert.NoError(t, rel.DeleteByFilter(context.Background(), handle.NewEQFilter(bat2.Vecs[2].Get(45)))) // nab5 has 2
		assert.NoError(t, txn.Commit(context.Background()))
	}

	flushTable()

	{
		txn, rel := testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		it := rel.MakeObjectIt(false)
		// 6 nablks has 87 rows
		dels := []int{3, 2, 0, 0, 0, 2}
		total := 0
		i := 0
		for it.Next() {
			obj := it.GetObject()
			for j := uint16(0); j < uint16(obj.BlkCnt()); j++ {
				var view *containers.Batch
				err := obj.HybridScan(ctx, &view, j, []int{2}, common.DefaultAllocator)
				assert.NoError(t, err)
				defer view.Close()
				viewDel := 0
				if view.Deletes != nil {
					viewDel = view.Deletes.GetCardinality()
				}
				assert.Equal(t, dels[i], viewDel)
				view.Compact()
				total += view.Length()
				i++
			}
		}
		it.Close()
		assert.Equal(t, 87, total)
		assert.NoError(t, txn.Commit(context.Background()))
	}

	t.Log(tae.Catalog.SimplePPString(common.PPL2))

	tae.Restart(ctx)
	{
		txn, rel := testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		it := rel.MakeObjectIt(false)
		// 6 nablks has 87 rows
		dels := []int{3, 2, 0, 0, 0, 2}
		total := 0
		idxs := make([]int, 0, len(schema.ColDefs)-1)
		for i := 0; i < len(schema.ColDefs)-1; i++ {
			idxs = append(idxs, i)
		}
		i := 0
		tblEntry := rel.GetMeta().(*catalog.TableEntry)
		for it.Next() {
			obj := it.GetObject()
			var views *containers.Batch
			for j := uint16(0); j < uint16(obj.BlkCnt()); j++ {
				blkID := objectio.NewBlockidWithObjectID(obj.GetID(), j)
				err := tables.HybridScanByBlock(ctx, tblEntry, txn, &views, schema, idxs, blkID, common.DefaultAllocator)
				assert.NoError(t, err)
				defer views.Close()
				for j, view := range views.Vecs {
					assert.Equal(t, schema.ColDefs[j].Type.Oid, view.GetType().Oid)
				}

				viewDel := 0
				if views.Deletes != nil {
					viewDel = views.Deletes.GetCardinality()
				}
				assert.Equal(t, dels[i], viewDel)
				views.Compact()
				i++
			}
			total += views.Length()
		}
		it.Close()
		assert.Equal(t, 87, total)
		assert.NoError(t, txn.Commit(context.Background()))
	}
}

func TestRollback1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	db := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer db.Close()
	schema := catalog.MockSchema(2, 0)

	testutil.CreateRelation(t, db, "db", schema, true)

	objCnt := 0
	onSegFn := func(object *catalog.ObjectEntry) error {
		objCnt++
		return nil
	}
	processor := new(catalog.LoopProcessor)
	processor.ObjectFn = onSegFn
	txn, rel := testutil.GetDefaultRelation(t, db, schema.Name)
	_, err := rel.CreateObject(false)
	assert.Nil(t, err)

	tableMeta := rel.GetMeta().(*catalog.TableEntry)
	err = tableMeta.RecurLoop(processor)
	assert.Nil(t, err)
	assert.Equal(t, objCnt, 1)

	assert.Nil(t, txn.Rollback(context.Background()))
	objCnt = 0
	err = tableMeta.RecurLoop(processor)
	assert.Nil(t, err)
	assert.Equal(t, objCnt, 0)

	txn, rel = testutil.GetDefaultRelation(t, db, schema.Name)
	obj, err := rel.CreateObject(false)
	assert.Nil(t, err)
	objMeta := obj.GetMeta().(*catalog.ObjectEntry)
	assert.Nil(t, txn.Commit(context.Background()))
	objCnt = 0
	err = tableMeta.RecurLoop(processor)
	assert.Nil(t, err)
	assert.Equal(t, objCnt, 1)

	txn, rel = testutil.GetDefaultRelation(t, db, schema.Name)
	_, err = rel.GetObject(objMeta.ID(), false)
	assert.Nil(t, err)
	err = tableMeta.RecurLoop(processor)
	assert.Nil(t, err)

	err = txn.Rollback(context.Background())
	assert.Nil(t, err)
	err = tableMeta.RecurLoop(processor)
	assert.Nil(t, err)

	t.Log(db.Catalog.SimplePPString(common.PPL1))
}

func TestMVCC1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	db := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer db.Close()
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 40
	schema.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*10))
	defer bat.Close()
	bats := bat.Split(40)

	txn, _, rel := testutil.CreateRelationNoCommit(t, db, "db", schema, true)
	err := rel.Append(context.Background(), bats[0])
	assert.NoError(t, err)

	row := 5
	expectVal := bats[0].Vecs[schema.GetSingleSortKeyIdx()].Get(row)
	filter := handle.NewEQFilter(expectVal)
	actualVal, _, err := rel.GetValueByFilter(context.Background(), filter, schema.GetSingleSortKeyIdx())
	assert.NoError(t, err)
	assert.Equal(t, expectVal, actualVal)
	assert.NoError(t, txn.Commit(context.Background()))

	_, rel = testutil.GetDefaultRelation(t, db, schema.Name)
	actualVal, _, err = rel.GetValueByFilter(context.Background(), filter, schema.GetSingleSortKeyIdx())
	assert.NoError(t, err)
	assert.Equal(t, expectVal, actualVal)

	txn2, rel2 := testutil.GetDefaultRelation(t, db, schema.Name)
	err = rel2.Append(context.Background(), bats[1])
	assert.NoError(t, err)

	val2 := bats[1].Vecs[schema.GetSingleSortKeyIdx()].Get(row)
	filter.Val = val2
	actualVal, _, err = rel2.GetValueByFilter(context.Background(), filter, schema.GetSingleSortKeyIdx())
	assert.NoError(t, err)
	assert.Equal(t, val2, actualVal)

	assert.NoError(t, txn2.Commit(context.Background()))

	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.Error(t, err)
	var id *common.ID

	{
		txn, rel := testutil.GetDefaultRelation(t, db, schema.Name)
		id, _, err = rel.GetByFilter(context.Background(), filter)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
	}

	it := rel.MakeObjectIt(false)
	for it.Next() {
		block := it.GetObject()
		bid := block.Fingerprint()
		_, targetBlkOffset := id.BlockID.Offsets()
		if bid.ObjectID() == id.ObjectID() {
			var view *containers.Batch
			err := block.HybridScan(ctx, &view, targetBlkOffset, []int{schema.GetSingleSortKeyIdx()}, common.DefaultAllocator)
			assert.Nil(t, err)
			defer view.Close()
			assert.Nil(t, view.Deletes)
			assert.NotNil(t, view)
			t.Log(view.Vecs[0].String())
			assert.Equal(t, bats[0].Vecs[0].Length(), view.Length())
		}
	}
	it.Close()
}

// 1. Txn1 create db, relation and append 10 rows. committed -- PASS
// 2. Txn2 append 10 rows. Get the 5th append row value -- PASS
// 3. Txn2 delete the 5th row value in uncommitted state -- PASS
// 4. Txn2 get the 5th row value -- NotFound
func TestMVCC2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	db := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer db.Close()
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 100
	schema.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows))
	defer bat.Close()
	bats := bat.Split(10)
	{
		txn, _, rel := testutil.CreateRelationNoCommit(t, db, "db", schema, true)
		err := rel.Append(context.Background(), bats[0])
		assert.NoError(t, err)
		val := bats[0].Vecs[schema.GetSingleSortKeyIdx()].Get(5)
		filter := handle.NewEQFilter(val)
		_, _, err = rel.GetByFilter(context.Background(), filter)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
	}
	{
		txn, rel := testutil.GetDefaultRelation(t, db, schema.Name)
		err := rel.Append(context.Background(), bats[1])
		assert.NoError(t, err)
		val := bats[1].Vecs[schema.GetSingleSortKeyIdx()].Get(5)
		filter := handle.NewEQFilter(val)
		err = rel.DeleteByFilter(context.Background(), filter)
		assert.NoError(t, err)

		_, _, err = rel.GetByFilter(context.Background(), filter)
		assert.Error(t, err)
		t.Log(err)
		assert.NoError(t, txn.Commit(context.Background()))
	}
	{
		txn, rel := testutil.GetDefaultRelation(t, db, schema.Name)
		it := rel.MakeObjectIt(false)
		for it.Next() {
			obj := it.GetObject()
			var view *containers.Batch
			err := obj.HybridScan(ctx, &view, 0, []int{schema.GetSingleSortKey().Idx}, common.DefaultAllocator)
			assert.Nil(t, err)
			assert.Nil(t, view.Deletes)
			assert.Equal(t, bats[1].Vecs[0].Length()*2-1, view.Length())
			// TODO: exclude deleted rows when apply appends
			view.Close()
		}
		assert.NoError(t, txn.Commit(context.Background()))
	}
}

func TestUnload1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := new(options.Options)
	db := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer db.Close()

	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2

	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*2))
	defer bat.Close()
	bats := bat.Split(int(schema.BlockMaxRows))
	testutil.CreateRelation(t, db, "db", schema, true)
	var wg sync.WaitGroup
	pool, err := ants.NewPool(1)
	assert.Nil(t, err)
	defer pool.Release()
	for _, data := range bats {
		wg.Add(1)
		err := pool.Submit(testutil.AppendClosure(t, data, schema.Name, db, &wg))
		assert.Nil(t, err)
	}
	wg.Wait()
	{
		txn, rel := testutil.GetDefaultRelation(t, db, schema.Name)
		for i := 0; i < 10; i++ {
			it := rel.MakeObjectIt(false)
			for it.Next() {
				blk := it.GetObject()
				for j := 0; j < blk.BlkCnt(); j++ {
					var view *containers.Batch
					err := blk.Scan(ctx, &view, uint16(j), []int{schema.GetSingleSortKey().Idx}, common.DefaultAllocator)
					assert.Nil(t, err)
					defer view.Close()
					assert.Equal(t, int(schema.BlockMaxRows), view.Length())
				}
			}
		}
		_ = txn.Commit(context.Background())
	}
}

func TestUnload2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := new(options.Options)
	db := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer db.Close()

	schema1 := catalog.MockSchemaAll(13, 2)
	schema1.BlockMaxRows = 10
	schema1.ObjectMaxBlocks = 2

	schema2 := catalog.MockSchemaAll(13, 2)
	schema2.BlockMaxRows = 10
	schema2.ObjectMaxBlocks = 2
	{
		txn, _ := db.StartTxn(nil)
		database, err := txn.CreateDatabase("db", "", "")
		assert.Nil(t, err)
		_, err = database.CreateRelation(schema1)
		assert.Nil(t, err)
		_, err = database.CreateRelation(schema2)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}

	bat := catalog.MockBatch(schema1, int(schema1.BlockMaxRows*5+5))
	defer bat.Close()
	bats := bat.Split(bat.Length())

	p, err := ants.NewPool(10)
	assert.Nil(t, err)
	defer p.Release()
	var wg sync.WaitGroup
	for i, data := range bats {
		wg.Add(1)
		name := schema1.Name
		if i%2 == 1 {
			name = schema2.Name
		}
		err := p.Submit(testutil.AppendClosure(t, data, name, db, &wg))
		assert.Nil(t, err)
	}
	wg.Wait()

	{
		txn, rel := testutil.GetDefaultRelation(t, db, schema1.Name)
		for i := 0; i < len(bats); i += 2 {
			data := bats[i]
			v := data.Vecs[schema1.GetSingleSortKeyIdx()].Get(0)
			filter := handle.NewEQFilter(v)
			_, _, err := rel.GetByFilter(context.Background(), filter)
			assert.NoError(t, err)
		}
		database, _ := txn.GetDatabase("db")
		rel, err = database.GetRelationByName(schema2.Name)
		assert.Nil(t, err)
		for i := 1; i < len(bats); i += 2 {
			data := bats[i]
			v := data.Vecs[schema1.GetSingleSortKeyIdx()].Get(0)
			filter := handle.NewEQFilter(v)
			_, _, err := rel.GetByFilter(context.Background(), filter)
			assert.NoError(t, err)
		}
		_ = txn.Commit(context.Background())
	}
}

func TestDelete1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()

	schema := catalog.MockSchemaAll(3, 2)
	schema.BlockMaxRows = 10
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows))
	defer bat.Close()
	testutil.CreateRelationAndAppend(t, 0, tae, "db", schema, bat, true)
	var id *common.ID
	var row uint32
	{
		txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
		pkCol := bat.Vecs[schema.GetSingleSortKeyIdx()]
		pkVal := pkCol.Get(5)
		filter := handle.NewEQFilter(pkVal)
		var err error
		id, row, err = rel.GetByFilter(context.Background(), filter)
		assert.NoError(t, err)
		err = rel.RangeDelete(id, row, row, handle.DT_Normal)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
	}
	{
		txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
		pkCol := bat.Vecs[schema.GetSingleSortKeyIdx()]
		pkVal := pkCol.Get(5)
		filter := handle.NewEQFilter(pkVal)
		_, _, err := rel.GetByFilter(context.Background(), filter)
		assert.Error(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
	}
	{
		txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
		blkMeta := testutil.GetOneBlockMeta(rel)
		task, err := jobs.NewFlushTableTailTask(nil, txn, []*catalog.ObjectEntry{blkMeta}, nil, tae.Runtime)
		assert.NoError(t, err)
		err = task.OnExec(context.Background())
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
	}
	{
		var view *containers.Batch
		txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
		blk := testutil.GetOneObject(rel)
		err := blk.HybridScan(ctx, &view, 0, []int{schema.GetSingleSortKeyIdx()}, common.DefaultAllocator)
		assert.NoError(t, err)
		assert.Nil(t, view.Deletes)
		assert.Equal(t, bat.Vecs[0].Length()-1, view.Length())
		view.Close()
		view = nil

		err = rel.RangeDelete(blk.Fingerprint(), 0, 0, handle.DT_Normal)
		assert.NoError(t, err)
		err = blk.HybridScan(ctx, &view, 0, []int{schema.GetSingleSortKeyIdx()}, common.DefaultAllocator)
		assert.NoError(t, err)
		assert.True(t, view.Deletes.Contains(0))
		view.Close()
		v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(0)
		filter := handle.NewEQFilter(v)
		_, _, err = rel.GetByFilter(context.Background(), filter)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound))
		assert.NoError(t, txn.Commit(context.Background()))
	}
	{
		txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
		blk := testutil.GetOneObject(rel)
		var view *containers.Batch
		err := blk.HybridScan(ctx, &view, 0, []int{schema.GetSingleSortKeyIdx()}, common.DefaultAllocator)
		assert.NoError(t, err)
		defer view.Close()
		assert.True(t, view.Deletes.Contains(0))
		assert.Equal(t, bat.Vecs[0].Length()-1, view.Length())
		v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(0)
		filter := handle.NewEQFilter(v)
		_, _, err = rel.GetByFilter(context.Background(), filter)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound))
		_ = txn.Rollback(context.Background())
	}
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestLogIndex1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 0)
	schema.BlockMaxRows = 10
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows))
	defer bat.Close()
	bats := bat.Split(int(schema.BlockMaxRows))
	testutil.CreateRelation(t, tae, "db", schema, true)
	txns := make([]txnif.AsyncTxn, 0)
	doAppend := func(data *containers.Batch) func() {
		return func() {
			txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
			err := rel.Append(context.Background(), data)
			assert.NoError(t, err)
			assert.NoError(t, txn.Commit(context.Background()))
			txns = append(txns, txn)
		}
	}
	for _, data := range bats {
		doAppend(data)()
	}
	var id *common.ID
	var offset uint32
	var err error
	{
		txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
		v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(3)
		filter := handle.NewEQFilter(v)
		id, offset, err = rel.GetByFilter(context.Background(), filter)
		assert.Nil(t, err)
		err = rel.RangeDelete(id, offset, offset, handle.DT_Normal)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	{
		txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
		blk := testutil.GetOneObject(rel)
		meta := blk.GetMeta().(*catalog.ObjectEntry)

		var view *containers.Batch
		err := blk.HybridScan(ctx, &view, 0, []int{schema.GetSingleSortKeyIdx()}, common.DefaultAllocator)
		assert.Nil(t, err)
		defer view.Close()
		assert.True(t, view.Deletes.Contains(uint64(offset)))
		task, err := jobs.NewFlushTableTailTask(nil, txn, []*catalog.ObjectEntry{meta}, nil, tae.Runtime)
		assert.Nil(t, err)
		err = task.OnExec(context.Background())
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
}

func TestCrossDBTxn(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()

	txn, _ := tae.StartTxn(nil)
	db1, err := txn.CreateDatabase("db1", "", "")
	assert.Nil(t, err)
	db2, err := txn.CreateDatabase("db2", "", "")
	assert.Nil(t, err)
	assert.NotNil(t, db1)
	assert.NotNil(t, db2)
	assert.Nil(t, txn.Commit(context.Background()))

	schema1 := catalog.MockSchema(2, 0)
	schema1.BlockMaxRows = 10
	schema1.ObjectMaxBlocks = 2
	schema2 := catalog.MockSchema(4, 0)
	schema2.BlockMaxRows = 10
	schema2.ObjectMaxBlocks = 2

	rows1 := schema1.BlockMaxRows * 5 / 2
	rows2 := schema1.BlockMaxRows * 3 / 2
	bat1 := catalog.MockBatch(schema1, int(rows1))
	bat2 := catalog.MockBatch(schema2, int(rows2))
	defer bat1.Close()
	defer bat2.Close()

	txn, _ = tae.StartTxn(nil)
	db1, err = txn.GetDatabase("db1")
	assert.Nil(t, err)
	db2, err = txn.GetDatabase("db2")
	assert.Nil(t, err)
	rel1, err := db1.CreateRelation(schema1)
	assert.Nil(t, err)
	rel2, err := db2.CreateRelation(schema2)
	assert.Nil(t, err)
	err = rel1.Append(context.Background(), bat1)
	assert.Nil(t, err)
	err = rel2.Append(context.Background(), bat2)
	assert.Nil(t, err)

	assert.Nil(t, txn.Commit(context.Background()))

	txn, _ = tae.StartTxn(nil)
	db1, err = txn.GetDatabase("db1")
	assert.NoError(t, err)
	db2, err = txn.GetDatabase("db2")
	assert.NoError(t, err)
	rel1, err = db1.GetRelationByName(schema1.Name)
	assert.NoError(t, err)
	rel2, err = db2.GetRelationByName(schema2.Name)
	assert.NoError(t, err)

	testutil.CheckAllColRowsByScan(t, rel1, int(rows1), false)
	testutil.CheckAllColRowsByScan(t, rel2, int(rows2), false)

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestSystemDB2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()

	txn, _ := tae.StartTxn(nil)
	sysDB, err := txn.GetDatabase(pkgcatalog.MO_CATALOG)
	assert.NoError(t, err)
	_, err = sysDB.DropRelationByName(pkgcatalog.MO_DATABASE)
	assert.Error(t, err)
	_, err = sysDB.DropRelationByName(pkgcatalog.MO_TABLES)
	assert.Error(t, err)
	_, err = sysDB.DropRelationByName(pkgcatalog.MO_COLUMNS)
	assert.Error(t, err)

	schema := catalog.MockSchema(2, 0)
	schema.BlockMaxRows = 100
	schema.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema, 1000)
	defer bat.Close()

	rel, err := sysDB.CreateRelation(schema)
	assert.NoError(t, err)
	assert.NotNil(t, rel)
	err = rel.Append(context.Background(), bat)
	assert.Nil(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, _ = tae.StartTxn(nil)
	sysDB, err = txn.GetDatabase(pkgcatalog.MO_CATALOG)
	assert.NoError(t, err)
	rel, err = sysDB.GetRelationByName(schema.Name)
	assert.NoError(t, err)
	testutil.CheckAllColRowsByScan(t, rel, 1000, false)
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestSystemDB3(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()
	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()
	txn, _ := tae.StartTxn(nil)
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 100
	schema.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema, 20)
	defer bat.Close()
	db, err := txn.GetDatabase(pkgcatalog.MO_CATALOG)
	assert.NoError(t, err)
	rel, err := db.CreateRelation(schema)
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bat)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestScan1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()
	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()

	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 100
	schema.ObjectMaxBlocks = 2

	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows-1))
	defer bat.Close()
	txn, _, rel := testutil.CreateRelationNoCommit(t, tae, testutil.DefaultTestDB, schema, true)
	err := rel.Append(context.Background(), bat)
	assert.NoError(t, err)
	testutil.CheckAllColRowsByScan(t, rel, bat.Length(), false)
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestDedup(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()

	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 100
	schema.ObjectMaxBlocks = 2

	bat := catalog.MockBatch(schema, 10)
	defer bat.Close()
	txn, _, rel := testutil.CreateRelationNoCommit(t, tae, testutil.DefaultTestDB, schema, true)
	err := rel.Append(context.Background(), bat)
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bat)
	t.Log(err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	testutil.CheckAllColRowsByScan(t, rel, 10, false)
	err = txn.Rollback(context.Background())
	assert.NoError(t, err)
}

func TestScan2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 10
	rows := schema.BlockMaxRows * 5 / 2
	bat := catalog.MockBatch(schema, int(rows))
	defer bat.Close()
	bats := bat.Split(2)

	txn, _, rel := testutil.CreateRelationNoCommit(t, tae, testutil.DefaultTestDB, schema, true)
	err := rel.Append(context.Background(), bats[0])
	assert.NoError(t, err)
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length(), false)

	err = rel.Append(context.Background(), bats[0])
	assert.Error(t, err)
	err = rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	testutil.CheckAllColRowsByScan(t, rel, int(rows), false)

	pkv := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(5)
	filter := handle.NewEQFilter(pkv)
	err = rel.DeleteByFilter(context.Background(), filter)
	assert.NoError(t, err)
	testutil.CheckAllColRowsByScan(t, rel, int(rows)-1, true)

	pkv = bat.Vecs[schema.GetSingleSortKeyIdx()].Get(8)
	filter = handle.NewEQFilter(pkv)
	updateV := int64(999)
	err = rel.UpdateByFilter(context.Background(), filter, 3, updateV, false)
	assert.NoError(t, err)

	v, _, err := rel.GetValueByFilter(context.Background(), filter, 3)
	assert.NoError(t, err)
	assert.Equal(t, updateV, v.(int64))
	testutil.CheckAllColRowsByScan(t, rel, int(rows)-1, true)
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestADA(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 1000
	bat := catalog.MockBatch(schema, 1)
	defer bat.Close()

	// Append to a block
	testutil.CreateRelationAndAppend(t, 0, tae, "db", schema, bat, true)

	// Delete a row from the block
	txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(0)
	filter := handle.NewEQFilter(v)
	id, row, err := rel.GetByFilter(context.Background(), filter)
	assert.NoError(t, err)
	err = rel.RangeDelete(id, row, row, handle.DT_Normal)
	assert.NoError(t, err)
	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.Error(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	// Append a row with the same primary key
	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.Error(t, err)
	err = rel.Append(context.Background(), bat)
	assert.NoError(t, err)
	id, row, err = rel.GetByFilter(context.Background(), filter)
	assert.NoError(t, err)
	testutil.CheckAllColRowsByScan(t, rel, 1, true)

	err = rel.RangeDelete(id, row, row, handle.DT_Normal)
	assert.NoError(t, err)
	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.Error(t, err)

	err = rel.Append(context.Background(), bat)
	assert.NoError(t, err)
	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.NoError(t, err)
	testutil.CheckAllColRowsByScan(t, rel, 1, true)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	err = rel.Append(context.Background(), bat)
	assert.Error(t, err)
	id, row, err = rel.GetByFilter(context.Background(), filter)
	assert.NoError(t, err)
	err = rel.RangeDelete(id, row, row, handle.DT_Normal)
	assert.NoError(t, err)
	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.Error(t, err)

	err = rel.Append(context.Background(), bat)
	assert.NoError(t, err)

	id, row, err = rel.GetByFilter(context.Background(), filter)
	assert.NoError(t, err)

	err = rel.Append(context.Background(), bat)
	assert.Error(t, err)

	err = rel.RangeDelete(id, row, row, handle.DT_Normal)
	assert.NoError(t, err)
	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.Error(t, err)
	err = rel.Append(context.Background(), bat)
	assert.NoError(t, err)

	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	err = rel.Append(context.Background(), bat)
	assert.Error(t, err)
	id, row, err = rel.GetByFilter(context.Background(), filter)
	assert.NoError(t, err)
	err = rel.RangeDelete(id, row, row, handle.DT_Normal)
	assert.NoError(t, err)
	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.Error(t, err)

	err = rel.Append(context.Background(), bat)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	it := rel.MakeObjectIt(false)
	for it.Next() {
		blk := it.GetObject()
		for j := 0; j < blk.BlkCnt(); j++ {
			var view *containers.Batch
			err := blk.HybridScan(ctx, &view, uint16(j), []int{schema.GetSingleSortKeyIdx()}, common.DefaultAllocator)
			assert.NoError(t, err)
			assert.Equal(t, 4, view.Length())
			assert.Equal(t, 3, view.Deletes.GetCardinality())
			view.Close()

		}
	}
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestUpdateByFilter(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 3)
	bat := catalog.MockBatch(schema, 100)
	defer bat.Close()

	testutil.CreateRelationAndAppend(t, 0, tae, "db", schema, bat, true)

	txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(2)
	filter := handle.NewEQFilter(v)
	err := rel.UpdateByFilter(context.Background(), filter, 2, int32(2222), false)
	assert.NoError(t, err)

	id, row, err := rel.GetByFilter(context.Background(), filter)
	assert.NoError(t, err)
	cv, _, err := rel.GetValue(id, row, 2, false)
	assert.NoError(t, err)
	assert.Equal(t, int32(2222), cv.(int32))

	v = bat.Vecs[schema.GetSingleSortKeyIdx()].Get(3)
	filter = handle.NewEQFilter(v)

	err = rel.UpdateByFilter(context.Background(), filter, uint16(schema.GetSingleSortKeyIdx()), int64(333333), false)
	assert.NoError(t, err)

	assert.NoError(t, txn.Commit(context.Background()))
}

// Test Steps
// 1. Create DB|Relation and append 10 rows. Commit
// 2. Make a equal filter with value of the pk of the second inserted row
// 3. Start Txn1. GetByFilter return PASS
// 4. Start Txn2. Delete row 2. Commit.
// 5. Txn1 call GetByFilter and should return PASS
func TestGetByFilter(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	bat := catalog.MockBatch(schema, 10)
	defer bat.Close()

	// Step 1
	testutil.CreateRelationAndAppend(t, 0, tae, "db", schema, bat, true)

	// Step 2
	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(2)
	filter := handle.NewEQFilter(v)

	// Step 3
	txn1, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
	id, row, err := rel.GetByFilter(context.Background(), filter)
	assert.NoError(t, err)

	// Step 4
	{
		txn2, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
		err := rel.RangeDelete(id, row, row, handle.DT_Normal)
		assert.NoError(t, err)
		assert.NoError(t, txn2.Commit(context.Background()))
	}

	// Step 5
	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.NoError(t, err)
	assert.NoError(t, txn1.Commit(context.Background()))
}

//  1. Set a big BlockMaxRows
//  2. Mock one row batch
//  3. Start tones of workers. Each work execute below routines:
//     3.1 GetByFilter a pk val
//     3.1.1 If found, go to 3.5
//     3.2 Append a row
//     3.3 err should not be duplicated(TODO: now is duplicated, should be W-W conflict)
//     (why not duplicated: previous GetByFilter had checked that there was no duplicate key)
//     3.4 If no error. try commit. If commit ok, inc appendedcnt. If error, rollback
//     3.5 Delete the row
//     3.5.1 If no error. try commit. commit should always pass
//     3.5.2 If error, should always be w-w conflict
//  4. Wait done all workers. Check the raw row count of table, should be same with appendedcnt.
func TestChaos1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 100000
	schema.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema, 1)
	defer bat.Close()

	testutil.CreateRelation(t, tae, "db", schema, true)

	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(0)
	filter := handle.NewEQFilter(v)
	var wg sync.WaitGroup
	appendCnt := uint32(0)
	deleteCnt := uint32(0)
	worker := func() {
		defer wg.Done()
		txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
		id, row, err := rel.GetByFilter(context.Background(), filter)
		// logutil.Infof("id=%v,row=%d,err=%v", id, row, err)
		if err == nil {
			err = rel.RangeDelete(id, row, row, handle.DT_Normal)
			if err != nil {
				t.Logf("delete: %v", err)
				// assert.Equal(t, txnif.ErrTxnWWConflict, err)
				assert.NoError(t, txn.Rollback(context.Background()))
				return
			}
			err := txn.Commit(context.Background())
			if err == nil {
				atomic.AddUint32(&deleteCnt, uint32(1))
			}
			return
		}
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound))
		err = rel.Append(context.Background(), bat)
		// TODO: enable below check later
		// assert.NotEqual(t, data.ErrDuplicate, err)
		if err == nil {
			err = txn.Commit(context.Background())
			// TODO: enable below check later
			// assert.NotEqual(t, data.ErrDuplicate, err)
			if err == nil {
				atomic.AddUint32(&appendCnt, uint32(1))
			} else {
				t.Logf("commit: %v", err)
			}
			return
		}
		_ = txn.Rollback(context.Background())
	}
	pool, _ := ants.NewPool(10)
	defer pool.Release()
	for i := 0; i < 50; i++ {
		wg.Add(1)
		err := pool.Submit(worker)
		assert.Nil(t, err)
	}
	wg.Wait()
	t.Logf("AppendCnt: %d", appendCnt)
	t.Logf("DeleteCnt: %d", deleteCnt)
	assert.True(t, appendCnt-deleteCnt <= 1)
	_, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
	blk := testutil.GetOneObject(rel)
	var view *containers.Batch
	err := blk.HybridScan(ctx, &view, 0, []int{schema.GetSingleSortKeyIdx()}, common.DefaultAllocator)
	assert.NoError(t, err)
	defer view.Close()
	assert.Equal(t, int(appendCnt), view.Length())
	mask := view.Deletes
	view.Compact()
	t.Log(view.String())
	assert.Equal(t, int(deleteCnt), mask.GetCardinality())
}

// Testing Steps
// 1. Append 10 rows
// 2. Start txn1
// 3. Start txn2. Update the 3rd row 3rd col to int64(2222) and commit. -- PASS
// 4. Txn1 try to update the 3rd row 3rd col to int64(1111). -- W-W Conflict.
// 5. Txn1 try to delete the 3rd row. W-W Conflict. Rollback
// 6. Start txn3 and try to update th3 3rd row 3rd col to int64(3333). -- PASS
func TestSnapshotIsolation1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 100
	bat := catalog.MockBatch(schema, 10)
	defer bat.Close()
	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(3)
	filter := handle.NewEQFilter(v)

	// Step 1
	testutil.CreateRelationAndAppend(t, 0, tae, "db", schema, bat, true)

	// Step 2
	txn1, rel1 := testutil.GetDefaultRelation(t, tae, schema.Name)

	// Step 3
	txn2, rel2 := testutil.GetDefaultRelation(t, tae, schema.Name)
	err := rel2.UpdateByFilter(context.Background(), filter, 3, int64(2222), false)
	assert.NoError(t, err)
	assert.NoError(t, txn2.Commit(context.Background()))

	// Step 4
	err = rel1.UpdateByFilter(context.Background(), filter, 3, int64(1111), false)
	t.Log(err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
	_ = txn1.Rollback(context.Background())

	// Step 5
	txn1, rel1 = testutil.GetDefaultRelation(t, tae, schema.Name)
	id, row, err := rel1.GetByFilter(context.Background(), filter)
	assert.NoError(t, err)
	err = rel1.RangeDelete(id, row, row, handle.DT_Normal)
	assert.NoError(t, err)
	_ = txn1.Rollback(context.Background())

	// Step 6
	txn3, rel3 := testutil.GetDefaultRelation(t, tae, schema.Name)
	err = rel3.UpdateByFilter(context.Background(), filter, 3, int64(3333), false)
	assert.NoError(t, err)
	assert.NoError(t, txn3.Commit(context.Background()))

	txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
	v, _, err = rel.GetValueByFilter(context.Background(), filter, 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(3333), v.(int64))
	err = rel.RangeDelete(id, row, row, handle.DT_Normal)
	assert.Error(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
}

// Testing Steps
// 1. Start txn1
// 2. Start txn2 and append one row and commit
// 3. Start txn3 and delete the row and commit
// 4. Txn1 try to append the row. (W-W). Rollback
func TestSnapshotIsolation2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 100
	bat := catalog.MockBatch(schema, 1)
	defer bat.Close()
	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(0)
	filter := handle.NewEQFilter(v)

	testutil.CreateRelation(t, tae, "db", schema, true)

	// Step 1
	txn1, rel1 := testutil.GetDefaultRelation(t, tae, schema.Name)

	// Step 2
	txn2, rel2 := testutil.GetDefaultRelation(t, tae, schema.Name)
	err := rel2.Append(context.Background(), bat)
	assert.NoError(t, err)
	assert.NoError(t, txn2.Commit(context.Background()))

	// Step 3
	txn3, rel3 := testutil.GetDefaultRelation(t, tae, schema.Name)
	err = rel3.DeleteByFilter(context.Background(), filter)
	assert.NoError(t, err)
	assert.NoError(t, txn3.Commit(context.Background()))

	// Step 4
	err = rel1.Append(context.Background(), bat)
	assert.NoError(t, err)
	err = txn1.Commit(context.Background())
	t.Log(err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
}

// Same as TestMergeBlocks
// no pkRow in schema, so merge will run reshape.
func TestReshapeBlocks(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(1, -1)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 3
	bat := catalog.MockBatch(schema, 30)
	defer bat.Close()

	testutil.CreateRelationAndAppend(t, 0, tae, "db", schema, bat, true)

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err := txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err := db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	it := rel.MakeObjectIt(false)
	it.Next()
	blkID := it.GetObject().Fingerprint()
	err = rel.RangeDelete(blkID, 5, 9, handle.DT_Normal)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, err = tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	it = rel.MakeObjectIt(false)
	for it.Next() {
		testutil.CheckAllColRowsByScan(t, rel, bat.Length(), false)
		obj := it.GetObject()
		var view *containers.Batch
		for j := 0; j < obj.BlkCnt(); j++ {
			err := obj.Scan(ctx, &view, uint16(j), []int{0}, common.DefaultAllocator)
			assert.NoError(t, err)
		}
		t.Log(view)
		view.Close()
	}
	assert.Nil(t, txn.Commit(context.Background()))

	testutil.CompactBlocks(t, 0, tae, "db", schema, false)
	testutil.MergeBlocks(t, 0, tae, "db", schema, false)

	txn, err = tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, uint64(25), rel.GetMeta().(*catalog.TableEntry).GetRows())
	it = rel.MakeObjectIt(false)
	for it.Next() {
		testutil.CheckAllColRowsByScan(t, rel, bat.Length()-5, false)
		obj := it.GetObject()
		var view *containers.Batch
		for j := 0; j < obj.BlkCnt(); j++ {
			err := obj.Scan(ctx, &view, uint16(j), []int{0}, common.DefaultAllocator)
			assert.NoError(t, err)
		}
		t.Log(view)
		view.Close()
	}
	assert.Nil(t, txn.Commit(context.Background()))
}

// 1. Append 3 blocks and delete last 5 rows of the 1st block
// 2. Merge blocks
// 3. Check rows and col[0]
func TestMergeBlocks(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(1, 0)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 3
	bat := catalog.MockBatch(schema, 30)
	defer bat.Close()

	testutil.CreateRelationAndAppend(t, 0, tae, "db", schema, bat, true)

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err := txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err := db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	blkID := testutil.GetOneObject(rel).Fingerprint()
	err = rel.RangeDelete(blkID, 5, 9, handle.DT_Normal)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, err = tae.StartTxn(nil)
	it := rel.MakeObjectIt(false)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	for it.Next() {
		testutil.CheckAllColRowsByScan(t, rel, bat.Length(), false)
		obj := it.GetObject()
		var view *containers.Batch
		for j := 0; j < obj.BlkCnt(); j++ {
			err := obj.Scan(ctx, &view, uint16(j), []int{0}, common.DefaultAllocator)
			assert.NoError(t, err)
		}
		t.Log(view)
		view.Close()
	}
	assert.Nil(t, txn.Commit(context.Background()))

	testutil.CompactBlocks(t, 0, tae, "db", schema, false)
	testutil.MergeBlocks(t, 0, tae, "db", schema, false)

	txn, err = tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, uint64(25), rel.GetMeta().(*catalog.TableEntry).GetRows())
	it = rel.MakeObjectIt(false)
	for it.Next() {
		testutil.CheckAllColRowsByScan(t, rel, bat.Length()-5, false)
		obj := it.GetObject()
		var view *containers.Batch
		for j := 0; j < obj.BlkCnt(); j++ {
			err := obj.Scan(ctx, &view, uint16(j), []int{0}, common.DefaultAllocator)
			assert.NoError(t, err)
		}
		t.Log(view)
		view.Close()
	}
	assert.Nil(t, txn.Commit(context.Background()))
}

type dummyCpkGetter struct{}

func (c *dummyCpkGetter) CollectCheckpointsInRange(ctx context.Context, start, end types.TS) (ckpLoc string, lastEnd types.TS, err error) {
	return "", types.TS{}, nil
}

func (c *dummyCpkGetter) FlushTable(ctx context.Context, dbID, tableID uint64, ts types.TS) error {
	return nil
}

func totsp(ts types.TS) *timestamp.Timestamp {
	x := ts.ToTimestamp()
	return &x
}

func TestSegDelLogtail(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, -1)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 3
	bat := catalog.MockBatch(schema, 30)
	defer bat.Close()

	testutil.CreateRelationAndAppend(t, 0, tae.DB, "db", schema, bat, true)

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err := txn.GetDatabase("db")
	did := db.GetID()
	assert.Nil(t, err)
	rel, err := db.GetRelationByName(schema.Name)
	tid := rel.ID()
	assert.Nil(t, err)
	blkID := testutil.GetOneObject(rel).Fingerprint()
	err = rel.RangeDelete(blkID, 5, 9, handle.DT_Normal)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	testutil.CompactBlocks(t, 0, tae.DB, "db", schema, false)
	testutil.MergeBlocks(t, 0, tae.DB, "db", schema, false)

	t.Log(tae.Catalog.SimplePPString(common.PPL3))
	resp, close, err := logtail.HandleSyncLogTailReq(context.TODO(), new(dummyCpkGetter), tae.LogtailMgr, tae.Catalog, api.SyncLogTailReq{
		CnHave: totsp(types.TS{}),
		CnWant: totsp(types.MaxTs()),
		Table:  &api.TableID{DbId: did, TbId: tid},
	}, false)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(resp.Commands)) // data object + tombstone object

	assert.Equal(t, api.Entry_Insert, resp.Commands[0].EntryType)
	assert.True(t, strings.HasSuffix(resp.Commands[0].TableName, "data_meta"))
	assert.Equal(t, uint32(9), resp.Commands[0].Bat.Vecs[0].Len) /* 5 create + 4 delete */
	// start ts should not be empty
	startTSVec := resp.Commands[0].Bat.Vecs[9]
	cnStartVec, err := vector.ProtoVectorToVector(startTSVec)
	assert.NoError(t, err)
	startTSs := vector.MustFixedCol[types.TS](cnStartVec)
	for _, ts := range startTSs {
		assert.False(t, ts.IsEmpty())
	}

	assert.Equal(t, api.Entry_Insert, resp.Commands[1].EntryType)
	assert.True(t, strings.HasSuffix(resp.Commands[1].TableName, "tombstone_meta"))
	assert.Equal(t, uint32(4), resp.Commands[1].Bat.Vecs[0].Len) /* 2 create + 2 delete */
	// start ts should not be empty
	startTSVec = resp.Commands[1].Bat.Vecs[9]
	cnStartVec, err = vector.ProtoVectorToVector(startTSVec)
	assert.NoError(t, err)
	startTSs = vector.MustFixedCol[types.TS](cnStartVec)
	for _, ts := range startTSs {
		assert.False(t, ts.IsEmpty())
	}

	close()

	txn, err = tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, uint64(25), rel.GetMeta().(*catalog.TableEntry).GetRows())
	testutil.CheckAllColRowsByScan(t, rel, bat.Length()-5, false)
	assert.Nil(t, txn.Commit(context.Background()))

	err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false)
	assert.NoError(t, err)

	check := func() {
		ckpEntries := tae.BGCheckpointRunner.GetAllIncrementalCheckpoints()
		assert.Equal(t, 1, len(ckpEntries))
		entry := ckpEntries[0]
		ins, del, dataObj, tombstoneObj, err := entry.GetByTableID(context.Background(), tae.Runtime.Fs, tid)
		assert.NoError(t, err)
		assert.Nil(t, ins)                              // 0 ins
		assert.Nil(t, del)                              // 0  del
		assert.Equal(t, uint32(9), dataObj.Vecs[0].Len) // 5 create + 4 delete
		assert.Equal(t, 10, len(dataObj.Vecs))
		assert.Equal(t, uint32(4), tombstoneObj.Vecs[0].Len) // 2 create + 2 delete
		assert.Equal(t, 10, len(tombstoneObj.Vecs))
	}
	check()

	tae.Restart(ctx)

	txn, err = tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, uint64(25), rel.GetMeta().(*catalog.TableEntry).GetRows())
	testutil.CheckAllColRowsByScan(t, rel, bat.Length()-5, false)
	assert.Nil(t, txn.Commit(context.Background()))

	check()

}

// delete
// merge but not commit
// delete
// commit merge
func TestMergeblocks2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(1, 0)
	schema.BlockMaxRows = 3
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 6)
	bats := bat.Split(2)
	defer bat.Close()

	tae.CreateRelAndAppend(bats[0], true)

	txn, rel := tae.GetRelation()
	_ = rel.Append(context.Background(), bats[1])
	assert.Nil(t, txn.Commit(context.Background()))

	// flush to nblk
	{
		tae.CompactBlocks(false)
	}

	{
		v := testutil.GetSingleSortKeyValue(bat, schema, 1)
		filter := handle.NewEQFilter(v)
		txn2, rel := tae.GetRelation()
		testutil.CheckAllColRowsByScan(t, rel, 6, true)
		_ = rel.DeleteByFilter(context.Background(), filter)
		assert.Nil(t, txn2.Commit(context.Background()))
	}

	_, rel = tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, 5, true)

	{
		txn, rel = tae.GetRelation()

		obj := testutil.GetOneObject(rel).GetMeta().(*catalog.ObjectEntry)
		objHandle, err := rel.GetObject(obj.ID(), false)
		assert.NoError(t, err)

		objsToMerge := []*catalog.ObjectEntry{objHandle.GetMeta().(*catalog.ObjectEntry)}
		task, err := jobs.NewMergeObjectsTask(nil, txn, objsToMerge, tae.Runtime, 0, false)
		assert.NoError(t, err)
		err = task.OnExec(context.Background())
		assert.NoError(t, err)

		{
			v := testutil.GetSingleSortKeyValue(bat, schema, 2)
			filter := handle.NewEQFilter(v)
			txn2, rel := tae.GetRelation()
			testutil.CheckAllColRowsByScan(t, rel, 5, true)
			_ = rel.DeleteByFilter(context.Background(), filter)
			assert.Nil(t, txn2.Commit(context.Background()))
		}
		err = txn.Commit(context.Background())
		assert.NoError(t, err)
	}

	_, rel = tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, 4, true)

	v := testutil.GetSingleSortKeyValue(bat, schema, 1)
	filter := handle.NewEQFilter(v)
	_, _, err := rel.GetByFilter(context.Background(), filter)
	assert.NotNil(t, err)

	v = testutil.GetSingleSortKeyValue(bat, schema, 2)
	filter = handle.NewEQFilter(v)
	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.NotNil(t, err)
}

// Object1: 1, 2, 3 | 4, 5, 6
// Object2: 7, 8, 9 | 10, 11, 12
// Now delete 1 and 10, then after merge:
// Object1: 2, 3, 4 | 5, 6, 7
// Object2: 8, 9, 11 | 12
// Delete map not nil on: [obj1, blk1] and [obj2, blk2]
func TestMergeBlocksIntoMultipleObjects(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(1, 0)
	schema.BlockMaxRows = 3
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 12)
	bats := bat.Split(2)
	defer bat.Close()

	tae.CreateRelAndAppend(bats[0], true)

	txn, rel := tae.GetRelation()
	_ = rel.Append(context.Background(), bats[1])
	assert.Nil(t, txn.Commit(context.Background()))

	// flush to nblk
	{
		txn, rel := tae.GetRelation()
		blkMetas := testutil.GetAllBlockMetas(rel, false)
		task, err := jobs.NewFlushTableTailTask(tasks.WaitableCtx, txn, blkMetas, nil, tae.DB.Runtime)
		assert.NoError(t, err)
		assert.NoError(t, task.OnExec(context.Background()))
		assert.NoError(t, txn.Commit(context.Background()))

		txn, rel = tae.GetRelation()
		testutil.CheckAllColRowsByScan(t, rel, 12, true)
		assert.NoError(t, txn.Commit(context.Background()))
	}

	{
		t.Log("************split one object into two objects************")

		txn, rel = tae.GetRelation()
		obj := testutil.GetOneObject(rel).GetMeta().(*catalog.ObjectEntry)
		objHandle, err := rel.GetObject(obj.ID(), false)
		assert.NoError(t, err)

		objsToMerge := []*catalog.ObjectEntry{objHandle.GetMeta().(*catalog.ObjectEntry)}
		task, err := jobs.NewMergeObjectsTask(nil, txn, objsToMerge, tae.Runtime, 0, false)
		assert.NoError(t, err)
		assert.NoError(t, task.OnExec(context.Background()))
		assert.NoError(t, txn.Commit(context.Background()))
	}

	{
		t.Log("************check del map************")
		it := rel.MakeObjectIt(false)
		for it.Next() {
			obj := it.GetObject()
			assert.Nil(t, tae.Runtime.TransferDelsMap.GetDelsForBlk(*objectio.NewBlockidWithObjectID(obj.GetID(), 0)))
			assert.Nil(t, tae.Runtime.TransferDelsMap.GetDelsForBlk(*objectio.NewBlockidWithObjectID(obj.GetID(), 1)))
		}
	}

	{
		t.Log("************delete during merge************")

		txn, rel = tae.GetRelation()
		objIt := rel.MakeObjectIt(false)
		objIt.Next()
		obj1 := objIt.GetObject().GetMeta().(*catalog.ObjectEntry)
		objHandle1, err := rel.GetObject(obj1.ID(), false)
		assert.NoError(t, err)
		objIt.Next()
		obj2 := objIt.GetObject().GetMeta().(*catalog.ObjectEntry)
		objHandle2, err := rel.GetObject(obj2.ID(), false)
		assert.NoError(t, err)
		objIt.Close()

		v := testutil.GetSingleSortKeyValue(bat, schema, 1)
		filter := handle.NewEQFilter(v)
		txn2, rel := tae.GetRelation()
		_ = rel.DeleteByFilter(context.Background(), filter)
		assert.NoError(t, txn2.Commit(context.Background()))
		_, rel = tae.GetRelation()
		testutil.CheckAllColRowsByScan(t, rel, 11, true)

		v = testutil.GetSingleSortKeyValue(bat, schema, 10)
		filter = handle.NewEQFilter(v)
		txn2, rel = tae.GetRelation()
		_ = rel.DeleteByFilter(context.Background(), filter)
		assert.NoError(t, txn2.Commit(context.Background()))
		_, rel = tae.GetRelation()
		testutil.CheckAllColRowsByScan(t, rel, 10, true)

		objsToMerge := []*catalog.ObjectEntry{objHandle1.GetMeta().(*catalog.ObjectEntry), objHandle2.GetMeta().(*catalog.ObjectEntry)}
		task, err := jobs.NewMergeObjectsTask(nil, txn, objsToMerge, tae.Runtime, 0, false)
		assert.NoError(t, err)
		assert.NoError(t, task.OnExec(context.Background()))
		assert.NoError(t, txn.Commit(context.Background()))
		{
			t.Log("************check del map again************")
			_, rel = tae.GetRelation()
			objCnt := 0
			for it := rel.MakeObjectIt(false); it.Next(); {
				obj := it.GetObject()
				if objCnt == 0 {
					assert.NotNil(t, tae.Runtime.TransferDelsMap.GetDelsForBlk(*objectio.NewBlockidWithObjectID(obj.GetID(), 0)))
				} else {
					assert.NotNil(t, tae.Runtime.TransferDelsMap.GetDelsForBlk(*objectio.NewBlockidWithObjectID(obj.GetID(), 1)))
				}
				objCnt++
			}
			assert.Equal(t, 2, objCnt)
		}
	}
}

func TestMergeEmptyBlocks(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(1, 0)
	schema.BlockMaxRows = 3
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 6)
	bats := bat.Split(2)
	defer bat.Close()

	tae.CreateRelAndAppend(bats[0], true)

	// flush to nblk
	{
		tae.CompactBlocks(false)
	}

	assert.NoError(t, tae.DeleteAll(true))

	{
		txn, rel := tae.GetRelation()
		assert.NoError(t, rel.Append(context.Background(), bats[1]))
		assert.NoError(t, txn.Commit(context.Background()))
	}

	{
		txn, rel := tae.GetRelation()

		obj := testutil.GetOneObject(rel).GetMeta().(*catalog.ObjectEntry)
		objHandle, err := rel.GetObject(obj.ID(), false)
		assert.NoError(t, err)

		objsToMerge := []*catalog.ObjectEntry{objHandle.GetMeta().(*catalog.ObjectEntry)}
		task, err := jobs.NewMergeObjectsTask(nil, txn, objsToMerge, tae.Runtime, 0, false)
		assert.NoError(t, err)
		err = task.OnExec(context.Background())
		assert.NoError(t, err)

		{
			v := testutil.GetSingleSortKeyValue(bat, schema, 4)
			filter := handle.NewEQFilter(v)
			txn2, rel := tae.GetRelation()
			assert.NoError(t, rel.DeleteByFilter(context.Background(), filter))
			assert.Nil(t, txn2.Commit(context.Background()))
		}
		err = txn.Commit(context.Background())
		assert.NoError(t, err)
	}
}
func TestDelete2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(18, 11)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 5)
	defer bat.Close()
	tae.CreateRelAndAppend(bat, true)

	txn, rel := tae.GetRelation()
	v := testutil.GetSingleSortKeyValue(bat, schema, 2)
	filter := handle.NewEQFilter(v)
	err := rel.DeleteByFilter(context.Background(), filter)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.CompactBlocks(false)
}

func TestNull1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(18, 9)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)

	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*3+1))
	defer bat.Close()
	bats := bat.Split(4)
	bats[0].Vecs[3].Update(2, nil, true)
	tae.CreateRelAndAppend(bats[0], true)

	txn, rel := tae.GetRelation()
	blk := testutil.GetOneObject(rel)
	var view *containers.Batch
	err := blk.Scan(ctx, &view, 0, []int{3}, common.DefaultAllocator)
	assert.NoError(t, err)
	//v := view.GetData().Get(2)
	assert.True(t, view.Vecs[0].IsNull(2))
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length(), false)
	assert.NoError(t, txn.Commit(context.Background()))
	view.Close()
	view = nil

	tae.Restart(ctx)
	txn, rel = tae.GetRelation()
	blk = testutil.GetOneObject(rel)
	blk.Scan(ctx, &view, 0, []int{3}, common.DefaultAllocator)
	assert.NoError(t, err)
	defer view.Close()
	//v = view.GetData().Get(2)
	assert.True(t, view.Vecs[0].IsNull(2))
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length(), false)

	v := testutil.GetSingleSortKeyValue(bats[0], schema, 2)
	filter_2 := handle.NewEQFilter(v)
	_, uv0_2_isNull, err := rel.GetValueByFilter(context.Background(), filter_2, 3)
	assert.NoError(t, err)
	assert.True(t, uv0_2_isNull)

	v0_4 := testutil.GetSingleSortKeyValue(bats[0], schema, 4)
	filter_4 := handle.NewEQFilter(v0_4)
	err = rel.UpdateByFilter(context.Background(), filter_4, 3, nil, true)
	assert.NoError(t, err)
	_, uv_isNull, err := rel.GetValueByFilter(context.Background(), filter_4, 3)
	assert.NoError(t, err)
	assert.True(t, uv_isNull)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length(), true)
	_, uv_isNull, err = rel.GetValueByFilter(context.Background(), filter_4, 3)
	assert.NoError(t, err)
	assert.True(t, uv_isNull)

	err = rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.CompactBlocks(false)
	txn, rel = tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, testutil.LenOfBats(bats[:2]), false)
	_, uv_isNull, err = rel.GetValueByFilter(context.Background(), filter_4, 3)
	assert.NoError(t, err)
	assert.True(t, uv_isNull)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.Restart(ctx)
	txn, rel = tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, testutil.LenOfBats(bats[:2]), false)
	_, uv_isNull, err = rel.GetValueByFilter(context.Background(), filter_4, 3)
	assert.NoError(t, err)
	assert.True(t, uv_isNull)

	v0_1 := testutil.GetSingleSortKeyValue(bats[0], schema, 1)
	filter0_1 := handle.NewEQFilter(v0_1)
	err = rel.UpdateByFilter(context.Background(), filter0_1, 12, nil, true)
	assert.NoError(t, err)
	_, uv0_1_isNull, err := rel.GetValueByFilter(context.Background(), filter0_1, 12)
	assert.NoError(t, err)
	assert.True(t, uv0_1_isNull)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	_, uv0_1_isNull, err = rel.GetValueByFilter(context.Background(), filter0_1, 12)
	assert.NoError(t, err)
	assert.True(t, uv0_1_isNull)
	err = rel.Append(context.Background(), bats[2])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.CompactBlocks(false)
	tae.MergeBlocks(false)

	txn, rel = tae.GetRelation()
	_, uv0_1_isNull, err = rel.GetValueByFilter(context.Background(), filter0_1, 12)
	assert.NoError(t, err)
	assert.True(t, uv0_1_isNull)
	_, uv0_2_isNull, err = rel.GetValueByFilter(context.Background(), filter_2, 3)
	assert.NoError(t, err)
	assert.True(t, uv0_2_isNull)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.Restart(ctx)

	txn, rel = tae.GetRelation()
	_, uv0_1_isNull, err = rel.GetValueByFilter(context.Background(), filter0_1, 12)
	assert.NoError(t, err)
	assert.True(t, uv0_1_isNull)
	_, uv0_2_isNull, err = rel.GetValueByFilter(context.Background(), filter_2, 3)
	assert.NoError(t, err)
	assert.True(t, uv0_2_isNull)
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestTruncate(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(18, 15)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*5+1))
	defer bat.Close()
	bats := bat.Split(20)
	tae.CreateRelAndAppend(bats[0], true)

	var wg sync.WaitGroup
	p, _ := ants.NewPool(10)
	defer p.Release()
	tryAppend := func(i int) func() {
		return func() {
			defer wg.Done()
			tae.TryAppend(bats[1+i])
		}
	}

	for i := range bats[1:] {
		if i == 10 {
			wg.Add(1)
			_ = p.Submit(func() {
				defer wg.Done()
				tae.Truncate()
				t.Log(tae.Catalog.SimplePPString(common.PPL1))
			})
		}
		wg.Add(1)
		_ = p.Submit(tryAppend(i))
		time.Sleep(time.Millisecond * 2)
	}
	wg.Wait()
	txn, _ := tae.GetRelation()
	assert.NoError(t, txn.Commit(context.Background()))
	tae.Truncate()
	txn, _ = tae.GetRelation()
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestGetColumnData(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(18, 13)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 39)
	bats := bat.Split(4)
	defer bat.Close()
	tae.CreateRelAndAppend(bats[0], true)
	txn, rel := tae.GetRelation()
	blk := testutil.GetOneObject(rel)
	var view *containers.Batch
	blk.Scan(ctx, &view, 0, []int{2}, common.DefaultAllocator)
	assert.Equal(t, bats[0].Length(), view.Length())
	assert.NotZero(t, view.Vecs[0].Allocated())
	view.Close()
	view = nil

	blk.Scan(ctx, &view, 0, []int{2}, common.DefaultAllocator)
	assert.Equal(t, bats[0].Length(), view.Length())
	assert.NotZero(t, view.Vecs[0].Allocated())
	assert.NoError(t, txn.Commit(context.Background()))
	view.Close()
	view = nil

	tae.CompactBlocks(false)
	txn, rel = tae.GetRelation()
	blk = testutil.GetOneObject(rel)
	blk.Scan(ctx, &view, 0, []int{2}, common.DefaultAllocator)
	assert.Equal(t, bats[0].Length(), view.Length())
	assert.NotZero(t, view.Vecs[0].Allocated())
	view.Close()
	view = nil

	blk.Scan(ctx, &view, 0, []int{2}, common.DefaultAllocator)
	assert.Equal(t, bats[0].Length(), view.Length())
	assert.NotZero(t, view.Vecs[0].Allocated())
	assert.NoError(t, txn.Commit(context.Background()))
	view.Close()
	view = nil

	txn, rel = tae.GetRelation()
	err := rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	blk = testutil.GetOneObject(rel)
	blk.Scan(ctx, &view, 0, []int{2}, common.DefaultAllocator)
	assert.NoError(t, err)
	assert.True(t, view.Vecs[0].Equals(bats[1].Vecs[2]))
	assert.NotZero(t, view.Vecs[0].Allocated())
	view.Close()
	view = nil
	blk.Scan(ctx, &view, 0, []int{2}, common.DefaultAllocator)
	assert.NoError(t, err)
	assert.True(t, view.Vecs[0].Equals(bats[1].Vecs[2]))
	assert.NotZero(t, view.Vecs[0].Allocated())
	view.Close()
	view = nil

	assert.NoError(t, txn.Commit(context.Background()))
}

func TestCompactBlk1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(3, 1)
	schema.BlockMaxRows = 5
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 5)
	bats := bat.Split(5)
	defer bat.Close()

	tae.CreateRelAndAppend(bats[2], true)

	txn, rel := tae.GetRelation()
	_ = rel.Append(context.Background(), bats[1])
	assert.Nil(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	_ = rel.Append(context.Background(), bats[3])
	assert.Nil(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	_ = rel.Append(context.Background(), bats[4])
	assert.Nil(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	_ = rel.Append(context.Background(), bats[0])
	assert.Nil(t, txn.Commit(context.Background()))

	{
		v := testutil.GetSingleSortKeyValue(bat, schema, 1)
		t.Logf("v is %v**********", v)
		filter := handle.NewEQFilter(v)
		txn2, rel := tae.GetRelation()
		t.Log("********before delete******************")
		testutil.CheckAllColRowsByScan(t, rel, 5, true)
		_ = rel.DeleteByFilter(context.Background(), filter)
		assert.Nil(t, txn2.Commit(context.Background()))
	}

	_, rel = tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, 4, true)

	{
		t.Log("************compact************")
		txn, rel = tae.GetRelation()
		blk := testutil.GetOneObject(rel)
		meta := blk.GetMeta().(*catalog.ObjectEntry)
		task, err := jobs.NewFlushTableTailTask(nil, txn, []*catalog.ObjectEntry{meta}, nil, tae.DB.Runtime)
		assert.NoError(t, err)
		err = task.OnExec(context.Background())
		assert.NoError(t, err)

		{
			v := testutil.GetSingleSortKeyValue(bat, schema, 2)
			t.Logf("v is %v**********", v)
			filter := handle.NewEQFilter(v)
			txn2, rel := tae.GetRelation()
			t.Log("********before delete******************")
			testutil.CheckAllColRowsByScan(t, rel, 4, true)
			_ = rel.DeleteByFilter(context.Background(), filter)
			assert.Nil(t, txn2.Commit(context.Background()))
		}

		err = txn.Commit(context.Background())
		assert.NoError(t, err)
	}

	_, rel = tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, 3, true)

	tae.Restart(ctx)
	_, rel = tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, 3, true)
}

func TestCompactBlk2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(3, 1)
	schema.BlockMaxRows = 5
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 5)
	bats := bat.Split(5)
	defer bat.Close()

	tae.CreateRelAndAppend(bats[2], true)

	txn, rel := tae.GetRelation()
	_ = rel.Append(context.Background(), bats[1])
	assert.Nil(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	_ = rel.Append(context.Background(), bats[3])
	assert.Nil(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	_ = rel.Append(context.Background(), bats[4])
	assert.Nil(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	_ = rel.Append(context.Background(), bats[0])
	assert.Nil(t, txn.Commit(context.Background()))

	v := testutil.GetSingleSortKeyValue(bat, schema, 1)
	filter := handle.NewEQFilter(v)
	txn2, rel1 := tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel1, 5, true)
	_ = rel1.DeleteByFilter(context.Background(), filter)
	assert.Nil(t, txn2.Commit(context.Background()))
	txn3, rel1 := tae.GetRelation()

	txn4, rel2 := tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel2, 4, true)

	txn, rel = tae.GetRelation()
	blk := testutil.GetOneObject(rel)
	meta := blk.GetMeta().(*catalog.ObjectEntry)
	task, err := jobs.NewFlushTableTailTask(nil, txn, []*catalog.ObjectEntry{meta}, nil, tae.DB.Runtime)
	assert.NoError(t, err)
	err = task.OnExec(context.Background())
	assert.NoError(t, err)
	err = txn.Commit(context.Background())
	assert.NoError(t, err)

	v = testutil.GetSingleSortKeyValue(bat, schema, 2)
	filter = handle.NewEQFilter(v)
	txn2, rel3 := tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel3, 4, true)
	_ = rel3.DeleteByFilter(context.Background(), filter)
	assert.Nil(t, txn2.Commit(context.Background()))

	v = testutil.GetSingleSortKeyValue(bat, schema, 4)
	filter = handle.NewEQFilter(v)
	txn2, rel4 := tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel4, 3, true)
	_ = rel4.DeleteByFilter(context.Background(), filter)
	assert.Nil(t, txn2.Commit(context.Background()))

	testutil.CheckAllColRowsByScan(t, rel1, 4, true)
	assert.Nil(t, txn3.Commit(context.Background()))
	testutil.CheckAllColRowsByScan(t, rel2, 4, true)
	assert.Nil(t, txn4.Commit(context.Background()))

	_, rel = tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, 2, true)

	v = testutil.GetSingleSortKeyValue(bat, schema, 2)
	filter = handle.NewEQFilter(v)
	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.NotNil(t, err)

	v = testutil.GetSingleSortKeyValue(bat, schema, 4)
	filter = handle.NewEQFilter(v)
	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.NotNil(t, err)

	tae.Restart(ctx)
}

func TestCompactblk3(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(3, 1)
	schema.BlockMaxRows = 5
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 3)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)

	v := testutil.GetSingleSortKeyValue(bat, schema, 1)
	filter := handle.NewEQFilter(v)
	txn2, rel1 := tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel1, 3, true)
	_ = rel1.DeleteByFilter(context.Background(), filter)
	assert.Nil(t, txn2.Commit(context.Background()))

	_, rel2 := tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel2, 2, true)

	txn, rel := tae.GetRelation()
	blk := testutil.GetOneObject(rel)
	meta := blk.GetMeta().(*catalog.ObjectEntry)
	task, err := jobs.NewFlushTableTailTask(nil, txn, []*catalog.ObjectEntry{meta}, nil, tae.DB.Runtime)
	assert.NoError(t, err)
	err = task.OnExec(context.Background())
	assert.NoError(t, err)
	err = txn.Commit(context.Background())
	assert.NoError(t, err)

	txn, err = tae.StartTxn(nil)
	assert.NoError(t, err)
	processor := &catalog.LoopProcessor{}
	processor.ObjectFn = func(be *catalog.ObjectEntry) error {
		if be.GetTable().GetDB().IsSystemDB() {
			return nil
		}
		tblEntry := be.GetTable()
		for j := 0; j < be.BlockCnt(); j++ {
			var view *containers.Batch
			blkID := objectio.NewBlockidWithObjectID(be.ID(), uint16(j))
			err := tables.HybridScanByBlock(ctx, tblEntry, txn, &view, schema, []int{0}, blkID, common.DefaultAllocator)
			assert.NoError(t, err)
			view.Compact()
			assert.Equal(t, 2, view.Length())
			view.Close()
		}
		return nil
	}
	err = tae.Catalog.RecurLoop(processor)
	assert.NoError(t, err)
}

func TestImmutableIndexInAblk(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(3, 1)
	schema.BlockMaxRows = 5
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 5)
	bats := bat.Split(5)
	defer bat.Close()

	tae.CreateRelAndAppend(bats[2], true)
	txn, rel := tae.GetRelation()
	_ = rel.Append(context.Background(), bats[1])
	assert.Nil(t, txn.Commit(context.Background()))
	txn, rel = tae.GetRelation()
	_ = rel.Append(context.Background(), bats[3])
	assert.Nil(t, txn.Commit(context.Background()))
	txn, rel = tae.GetRelation()
	_ = rel.Append(context.Background(), bats[4])
	assert.Nil(t, txn.Commit(context.Background()))
	txn, rel = tae.GetRelation()
	_ = rel.Append(context.Background(), bats[0])
	assert.Nil(t, txn.Commit(context.Background()))

	v := testutil.GetSingleSortKeyValue(bat, schema, 1)
	filter := handle.NewEQFilter(v)
	txn2, rel := tae.GetRelation()
	_ = rel.DeleteByFilter(context.Background(), filter)
	assert.Nil(t, txn2.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	blk := testutil.GetOneObject(rel)
	meta := blk.GetMeta().(*catalog.ObjectEntry)
	task, err := jobs.NewFlushTableTailTask(nil, txn, []*catalog.ObjectEntry{meta}, nil, tae.DB.Runtime)
	assert.NoError(t, err)
	err = task.OnExec(context.Background())
	assert.NoError(t, err)
	err = txn.Commit(context.Background())
	assert.NoError(t, err)

	txn, rel = tae.GetRelation()
	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.Error(t, err)
	v = testutil.GetSingleSortKeyValue(bat, schema, 2)
	filter = handle.NewEQFilter(v)
	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.NoError(t, err)

	rowIDs := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
	for i := 0; i < bat.Length(); i++ {
		rowIDs.Append(nil, true)
	}
	err = meta.GetObjectData().GetDuplicatedRows(
		context.Background(), txn, bat.Vecs[1], nil, false, true, false, rowIDs, common.DefaultAllocator,
	)
	assert.NoError(t, err)
	err = meta.GetObjectData().Contains(context.Background(), txn, false, rowIDs, nil, common.DebugAllocator)
	assert.NoError(t, err)
	duplicate := false
	rowIDs.Foreach(func(v any, isNull bool, row int) error {
		if !isNull {
			duplicate = true
		}
		return nil
	}, nil)
	assert.True(t, duplicate)
}

func TestDelete3(t *testing.T) {
	// t.Skip(any("This case crashes occasionally, is being fixed, skip it for now"))
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	// this task won't affect logic of TestAppend2, it just prints logs about dirty count
	forest := logtail.NewDirtyCollector(tae.LogtailMgr, opts.Clock, tae.Catalog, new(catalog.LoopProcessor))
	hb := ops.NewHeartBeaterWithFunc(5*time.Millisecond, func() {
		forest.Run(0)
		t.Log(forest.String())
	}, nil)
	hb.Start()
	defer hb.Stop()
	schema := catalog.MockSchemaAll(3, 2)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	// rows := int(schema.BlockMaxRows * 1)
	rows := int(schema.BlockMaxRows*3) + 1
	bat := catalog.MockBatch(schema, rows)

	tae.CreateRelAndAppend(bat, true)
	tae.CheckRowsByScan(rows, false)
	deleted := false
	for i := 0; i < 10; i++ {
		if deleted {
			tae.CheckRowsByScan(0, true)
			tae.DoAppend(bat)
			deleted = false
			tae.CheckRowsByScan(rows, true)
		} else {
			tae.CheckRowsByScan(rows, true)
			err := tae.DeleteAll(true)
			if err == nil {
				deleted = true
				tae.CheckRowsByScan(0, true)
				// assert.Zero(t, tae.getRows())
			} else {
				tae.CheckRowsByScan(rows, true)
				// assert.Equal(t, tae.getRows(), rows)
			}
		}
	}
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestDropCreated1(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	_, err = txn.CreateDatabase("db", "", "")
	assert.Nil(t, err)
	db, err := txn.DropDatabase("db")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	assert.Equal(t, txn.GetCommitTS(), db.GetMeta().(*catalog.DBEntry).GetCreatedAtLocked())
	assert.Equal(t, txn.GetCommitTS(), db.GetMeta().(*catalog.DBEntry).GetCreatedAtLocked())

	tae.Restart(ctx)
}

func TestDropCreated2(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	schema := catalog.MockSchemaAll(1, -1)
	defer tae.Close()

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err := txn.CreateDatabase("db", "", "")
	assert.Nil(t, err)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	_, err = db.DropRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	assert.Equal(t, txn.GetCommitTS(), rel.GetMeta().(*catalog.TableEntry).GetCreatedAtLocked())
	assert.Equal(t, txn.GetCommitTS(), rel.GetMeta().(*catalog.TableEntry).GetCreatedAtLocked())

	tae.Restart(ctx)
}

func TestDropCreated3(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	_, err = txn.CreateDatabase("db", "", "")
	assert.Nil(t, err)
	_, err = txn.DropDatabase("db")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false)
	assert.Nil(t, err)

	tae.Restart(ctx)
}

func TestRollbackCreateTable(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	schema := catalog.MockSchemaAll(1, -1)
	defer tae.Close()

	txn, _ := tae.StartTxn(nil)
	db, _ := txn.CreateDatabase("db", "sql", "")
	db.CreateRelationWithID(schema.Clone(), uint64(27200))
	txn.Commit(ctx)

	for i := 0; i < 10; i += 2 {
		tae.Catalog.GCByTS(ctx, tae.TxnMgr.Now())
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		db.DropRelationByID(uint64(i + 27200))
		db.CreateRelationWithID(schema.Clone(), uint64(i+27200+1))
		txn.Commit(ctx)

		txn, _ = tae.StartTxn(nil)
		db, _ = txn.GetDatabase("db")
		db.DropRelationByID(uint64(i + 27200 + 1))
		db.CreateRelationWithID(schema.Clone(), uint64(i+27200+2))
		txn.Rollback(ctx)
	}

	t.Log(tae.Catalog.SimplePPString(3))
}

func TestDropCreated4(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	schema := catalog.MockSchemaAll(1, -1)
	defer tae.Close()

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err := txn.CreateDatabase("db", "", "")
	assert.Nil(t, err)
	_, err = db.CreateRelation(schema)
	assert.Nil(t, err)
	_, err = db.DropRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false)
	assert.Nil(t, err)

	tae.Restart(ctx)
}

func TestTruncateZonemap(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	type Mod struct {
		offset int
		v      byte
	}
	mockBytes := func(init byte, size int, mods ...Mod) []byte {
		ret := make([]byte, size)
		for i := 0; i < size; i++ {
			ret[i] = init
		}
		for _, m := range mods {
			ret[m.offset] = m.v
		}
		return ret
	}
	testutils.EnsureNoLeak(t)
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(13, 12) // set varchar PK
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)

	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*2+9))        // 2.9 blocks
	minv := mockBytes(0, 35)                                              // 0x00000000
	trickyMinv := mockBytes(0, 33)                                        // smaller than minv, not in mut index but in immut index
	maxv := mockBytes(0xff, 35, Mod{0, 0x61}, Mod{1, 0x62}, Mod{2, 0x63}) // abc0xff0xff...
	trickyMaxv := []byte("abd")                                           // bigger than maxv, not in mut index but in immut index
	bat.Vecs[12].Update(8, maxv, false)
	bat.Vecs[12].Update(11, minv, false)
	bat.Vecs[12].Update(22, []byte("abcc"), false)
	defer bat.Close()

	checkMinMax := func(rel handle.Relation, minvOffset, maxvOffset uint32) {
		_, _, err := rel.GetByFilter(context.Background(), handle.NewEQFilter(trickyMinv))
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound))
		_, _, err = rel.GetByFilter(context.Background(), handle.NewEQFilter(trickyMaxv))
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound))
		_, row, err := rel.GetByFilter(context.Background(), handle.NewEQFilter(minv))
		assert.NoError(t, err)
		assert.Equal(t, minvOffset, row)
		_, row, err = rel.GetByFilter(context.Background(), handle.NewEQFilter(maxv))
		assert.NoError(t, err)
		assert.Equal(t, maxvOffset, row)
	}

	tae.CreateRelAndAppend(bat, true)

	// runtime check
	txn, rel := tae.GetRelation()
	checkMinMax(rel, 1, 8)
	assert.NoError(t, txn.Commit(context.Background()))

	// restart without compact
	tae.Restart(ctx)
	txn, rel = tae.GetRelation()
	checkMinMax(rel, 1, 8)
	assert.NoError(t, txn.Commit(context.Background()))

	// restart with compact
	tae.CompactBlocks(false)
	tae.MergeBlocks(false)
	tae.Restart(ctx)
	txn, rel = tae.GetRelation()
	checkMinMax(rel, 0, 8)
	assert.NoError(t, txn.Commit(context.Background()))

	// 3 NonAppendable Blocks
	txn, rel = tae.GetRelation()
	rel.UpdateByFilter(context.Background(), handle.NewEQFilter(maxv), 12, mockBytes(0xff, 35), false)
	assert.NoError(t, txn.Commit(context.Background()))
	tae.CompactBlocks(false)
	tae.MergeBlocks(false)
	tae.Restart(ctx)

	txn, rel = tae.GetRelation()
	_, row, err := rel.GetByFilter(context.Background(), handle.NewEQFilter(mockBytes(0xff, 35)))
	assert.NoError(t, err)
	assert.Equal(t, uint32(8), row)
	assert.NoError(t, txn.Commit(context.Background()))
}

func mustStartTxn(t *testing.T, tae *testutil.TestEngine, tenantID uint32) txnif.AsyncTxn {
	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	txn.BindAccessInfo(tenantID, 0, 0)
	return txn
}

func TestMultiTenantDBOps(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	var err error
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	txn11 := mustStartTxn(t, tae, 1)
	_, err = txn11.CreateDatabase("db", "", "")
	assert.NoError(t, err)
	txn12 := mustStartTxn(t, tae, 1)
	_, err = txn11.CreateDatabase("db", "", "")
	assert.Error(t, err)

	txn21 := mustStartTxn(t, tae, 2)
	_, err = txn21.CreateDatabase("db", "", "")
	assert.NoError(t, err)

	assert.NoError(t, txn11.Commit(context.Background()))
	assert.NoError(t, txn12.Commit(context.Background()))
	assert.NoError(t, txn21.Commit(context.Background()))

	txn22 := mustStartTxn(t, tae, 2)
	_, _ = txn22.CreateDatabase("db2", "", "")

	txn23 := mustStartTxn(t, tae, 2)
	// [mo_catalog, db]
	assert.Equal(t, 2, len(txn23.DatabaseNames()))
	assert.NoError(t, txn23.Commit(context.Background()))

	txn22.Commit(context.Background())
	tae.Restart(ctx)

	txn24 := mustStartTxn(t, tae, 2)
	// [mo_catalog, db, db2]
	assert.Equal(t, 3, len(txn24.DatabaseNames()))
	assert.NoError(t, txn24.Commit(context.Background()))

	txn13 := mustStartTxn(t, tae, 1)
	// [mo_catalog, db]
	assert.Equal(t, 2, len(txn13.DatabaseNames()))

	_, err = txn13.GetDatabase("db2")
	assert.Error(t, err)
	dbHdl, err := txn13.GetDatabase("db")
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), dbHdl.GetMeta().(*catalog.DBEntry).GetTenantID())

	_, err = txn13.DropDatabase("db2")
	assert.Error(t, err)
	_, err = txn13.DropDatabase("db")
	assert.NoError(t, err)
	assert.NoError(t, txn13.Commit(context.Background()))

	txn14 := mustStartTxn(t, tae, 1)
	// [mo_catalog]
	assert.Equal(t, 1, len(txn14.DatabaseNames()))
	assert.NoError(t, txn14.Commit(context.Background()))
}

func TestMultiTenantMoCatalogOps(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	var err error
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	s := catalog.MockSchemaAll(1, 0)
	s.Name = "mo_accounts"
	txn0, sysDB := tae.GetDB(pkgcatalog.MO_CATALOG)
	_, err = sysDB.CreateRelation(s)
	assert.NoError(t, err)
	assert.NoError(t, txn0.Commit(context.Background()))

	schema11 := catalog.MockSchemaAll(3, 0)
	schema11.BlockMaxRows = 10
	schema11.ObjectMaxBlocks = 2
	tae.BindSchema(schema11)
	tae.BindTenantID(1)

	bat1 := catalog.MockBatch(schema11, int(schema11.BlockMaxRows*2+9))
	tae.CreateRelAndAppend(bat1, true)
	// pretend 'mo_users'
	s = catalog.MockSchemaAll(1, 0)
	s.Name = "mo_users"
	txn11, sysDB := tae.GetDB(pkgcatalog.MO_CATALOG)
	_, err = sysDB.CreateRelation(s)
	assert.NoError(t, err)
	assert.NoError(t, txn11.Commit(context.Background()))

	tae.CompactBlocks(false)
	tae.MergeBlocks(false)

	schema21 := catalog.MockSchemaAll(2, 1)
	schema21.BlockMaxRows = 10
	schema21.ObjectMaxBlocks = 2
	tae.BindSchema(schema21)
	tae.BindTenantID(2)

	bat2 := catalog.MockBatch(schema21, int(schema21.BlockMaxRows*3+5))
	tae.CreateRelAndAppend(bat2, true)
	txn21, sysDB := tae.GetDB(pkgcatalog.MO_CATALOG)
	s = catalog.MockSchemaAll(1, 0)
	s.Name = "mo_users"
	_, err = sysDB.CreateRelation(s)
	assert.NoError(t, err)
	assert.NoError(t, txn21.Commit(context.Background()))

	tae.CompactBlocks(false)
	tae.MergeBlocks(false)

	tae.Restart(ctx)

	{
		// account 2
		// check data for good
		_, tbl := tae.GetRelation()
		testutil.CheckAllColRowsByScan(t, tbl, 35, false)
	}
	{
		// account 1
		tae.BindSchema(schema11)
		tae.BindTenantID(1)
		// check data for good
		_, tbl := tae.GetRelation()
		testutil.CheckAllColRowsByScan(t, tbl, 29, false)
	}
	{
		// sys account
		tae.BindSchema(nil)
		tae.BindTenantID(0)
		// [mo_catalog]
		assert.Equal(t, 1, len(mustStartTxn(t, tae, 0).DatabaseNames()))
	}

}

// txn1: create relation and append, half blk
// txn2: compact
// txn3: append, shouldn't get rw
func TestGetLastAppender(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(1, -1)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 14)
	bats := bat.Split(2)

	tae.CreateRelAndAppend(bats[0], true)
	t.Log(tae.Catalog.SimplePPString(3))

	tae.CompactBlocks(false)
	t.Log(tae.Catalog.SimplePPString(3))

	tae.Restart(ctx)

	txn, rel := tae.GetRelation()
	rel.Append(context.Background(), bats[1])
	assert.NoError(t, txn.Commit(context.Background()))
}

// txn1[s1,p1,e1] append1
// txn2[s2,p2,e2] append2
// txn3[s3,p3,e3] append3
// collect [0,p1] [0,p2] [p1+1,p2] [p1+1,p3]
// check data, row count, commit ts
// TODO 1. in2pc committs!=preparets; 2. abort
func TestCollectInsert(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(1, -1)
	schema.BlockMaxRows = 20
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 12)
	bats := bat.Split(4)

	tae.CreateRelAndAppend(bats[0], true)

	txn1, rel := tae.GetRelation()
	assert.NoError(t, rel.Append(context.Background(), bats[1]))
	assert.NoError(t, txn1.Commit(context.Background()))

	p1 := txn1.GetPrepareTS()
	t.Logf("p1= %v", p1.ToString())

	txn2, rel := tae.GetRelation()
	assert.NoError(t, rel.Append(context.Background(), bats[2]))
	assert.NoError(t, txn2.Commit(context.Background()))

	p2 := txn2.GetPrepareTS()
	t.Logf("p2= %v", p2.ToString())

	txn3, rel := tae.GetRelation()
	assert.NoError(t, rel.Append(context.Background(), bats[3]))
	assert.NoError(t, txn3.Commit(context.Background()))

	p3 := txn3.GetPrepareTS()
	t.Logf("p3= %v", p3.ToString())

	_, rel = tae.GetRelation()
	objEntry := testutil.GetOneObject(rel).GetMeta().(*catalog.ObjectEntry)

	batches := make(map[uint32]*containers.BatchWithVersion)
	err := tables.RangeScanInMemoryByObject(ctx, objEntry, batches, types.TS{}, p1, common.DefaultAllocator)
	assert.NoError(t, err)
	t.Log((batches[schema.Version].Attrs))
	for _, vec := range batches[schema.Version].Vecs {
		t.Log(vec)
		assert.Equal(t, 6, vec.Length())
	}
	batches[schema.Version].Close()

	batches = make(map[uint32]*containers.BatchWithVersion)
	err = tables.RangeScanInMemoryByObject(ctx, objEntry, batches, types.TS{}, p2, common.DefaultAllocator)
	assert.NoError(t, err)
	t.Log((batches[schema.Version].Attrs))
	for _, vec := range batches[schema.Version].Vecs {
		t.Log(vec)
		assert.Equal(t, 9, vec.Length())
	}
	batches[schema.Version].Close()

	batches = make(map[uint32]*containers.BatchWithVersion)
	err = tables.RangeScanInMemoryByObject(ctx, objEntry, batches, p1.Next(), p2, common.DefaultAllocator)
	assert.NoError(t, err)
	t.Log((batches[schema.Version].Attrs))
	for _, vec := range batches[schema.Version].Vecs {
		t.Log(vec)
		assert.Equal(t, 3, vec.Length())
	}
	batches[schema.Version].Close()

	batches = make(map[uint32]*containers.BatchWithVersion)
	err = tables.RangeScanInMemoryByObject(ctx, objEntry, batches, p1.Next(), p3, common.DefaultAllocator)
	assert.NoError(t, err)
	t.Log((batches[schema.Version].Attrs))
	for _, vec := range batches[schema.Version].Vecs {
		t.Log(vec)
		assert.Equal(t, 6, vec.Length())
	}
	batches[schema.Version].Close()
}

func TestAppendnode(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(1, 0)
	schema.BlockMaxRows = 10000
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	appendCnt := 20
	bat := catalog.MockBatch(schema, appendCnt)
	bats := bat.Split(appendCnt)

	tae.CreateRelAndAppend(bats[0], true)
	tae.CheckRowsByScan(1, false)

	var wg sync.WaitGroup
	pool, _ := ants.NewPool(5)
	defer pool.Release()
	worker := func(i int) func() {
		return func() {
			txn, rel := tae.GetRelation()
			row := testutil.GetColumnRowsByScan(t, rel, 0, true)
			err := tae.DoAppendWithTxn(bats[i], txn, true)
			assert.NoError(t, err)
			row2 := testutil.GetColumnRowsByScan(t, rel, 0, true)
			assert.Equal(t, row+1, row2)
			assert.NoError(t, txn.Commit(context.Background()))
			wg.Done()
		}
	}
	for i := 1; i < appendCnt; i++ {
		wg.Add(1)
		pool.Submit(worker(i))
	}
	wg.Wait()
	tae.CheckRowsByScan(appendCnt, true)

	tae.Restart(ctx)
	tae.CheckRowsByScan(appendCnt, true)
}

func TestTxnIdempotent(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(1, 0)
	schema.BlockMaxRows = 10000
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	appendCnt := 20
	bat := catalog.MockBatch(schema, appendCnt)
	bats := bat.Split(appendCnt)

	var wg sync.WaitGroup

	tae.CreateRelAndAppend(bats[0], true)
	for i := 0; i < 10; i++ {
		txn, _ := tae.GetRelation()
		wg.Add(1)
		assert.NoError(t, txn.Rollback(context.Background()))
		go func() {
			defer wg.Done()
			assert.True(t, moerr.IsMoErrCode(txn.Commit(context.Background()), moerr.ErrTxnNotFound))
			// txn.Commit(context.Background())
		}()
		wg.Wait()
	}
}

// insert 200 rows and do quick compaction
// expect that there are some dirty tables at first and then zero dirty table found
func TestWatchDirty(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	logMgr := tae.LogtailMgr

	visitor := &catalog.LoopProcessor{}
	watcher := logtail.NewDirtyCollector(logMgr, opts.Clock, tae.Catalog, visitor)

	tbl, obj := watcher.DirtyCount()
	assert.Zero(t, obj)
	assert.Zero(t, tbl)

	schema := catalog.MockSchemaAll(1, 0)
	schema.BlockMaxRows = 50
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	appendCnt := 200
	bat := catalog.MockBatch(schema, appendCnt)
	bats := bat.Split(appendCnt)

	tae.CreateRelAndAppend(bats[0], true)
	tae.CheckRowsByScan(1, false)

	wg := &sync.WaitGroup{}
	pool, _ := ants.NewPool(3)
	defer pool.Release()
	worker := func(i int) func() {
		return func() {
			txn, _ := tae.GetRelation()
			err := tae.DoAppendWithTxn(bats[i], txn, true)
			assert.NoError(t, err)
			assert.NoError(t, txn.Commit(context.Background()))
			wg.Done()
		}
	}
	for i := 1; i < appendCnt; i++ {
		wg.Add(1)
		pool.Submit(worker(i))
	}
	wg.Wait()

	timer := time.After(20 * time.Second)
	for {
		select {
		case <-timer:
			t.Errorf("timeout to wait zero")
			return
		default:
			watcher.Run(0)
			time.Sleep(5 * time.Millisecond)
			_, objCnt := watcher.DirtyCount()
			// find block zero
			if objCnt == 0 {
				return
			}
		}
	}
}

func TestDirtyWatchRace(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(2, -1)
	schema.Name = "test"
	schema.BlockMaxRows = 5
	schema.ObjectMaxBlocks = 5
	tae.BindSchema(schema)

	tae.CreateRelAndAppend(catalog.MockBatch(schema, 1), true)

	visitor := &catalog.LoopProcessor{}
	watcher := logtail.NewDirtyCollector(tae.LogtailMgr, opts.Clock, tae.Catalog, visitor)

	wg := &sync.WaitGroup{}

	addRow := func() {
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		tbl, _ := db.GetRelationByName("test")
		tbl.Append(context.Background(), catalog.MockBatch(schema, 1))
		assert.NoError(t, txn.Commit(context.Background()))
		wg.Done()
	}

	pool, _ := ants.NewPool(5)
	defer pool.Release()

	for i := 0; i < 50; i++ {
		wg.Add(1)
		pool.Submit(addRow)
	}

	// test race
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < 300; j++ {
				time.Sleep(5 * time.Millisecond)
				watcher.Run(0)
				// tbl, obj, blk := watcher.DirtyCount()
				// t.Logf("t%d: tbl %d, obj %d, blk %d", i, tbl, obj, blk)
				_, _ = watcher.DirtyCount()
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
}

type TestBlockReadDeltaSource struct {
	deltaLoc objectio.Location
	testTs   types.TS
}

func (b *TestBlockReadDeltaSource) SetTS(ts types.TS) {
	b.testTs = ts
}

func (b *TestBlockReadDeltaSource) GetDeltaLoc(bid objectio.Blockid) (objectio.Location, types.TS) {
	return b.deltaLoc, b.testTs
}

func NewTestBlockReadSource(deltaLoc objectio.Location) logtail.DeltaSource {
	return &TestBlockReadDeltaSource{
		deltaLoc: deltaLoc,
	}
}

func TestBlockRead(t *testing.T) {
	blockio.RunPipelineTest(
		func() {
			defer testutils.AfterTest(t)()
			ctx := context.Background()

			opts := config.WithLongScanAndCKPOpts(nil)
			tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
			tsAlloc := types.NewTsAlloctor(opts.Clock)
			defer tae.Close()
			schema := catalog.MockSchemaAll(2, 1)
			schema.BlockMaxRows = 20
			schema.ObjectMaxBlocks = 2
			tae.BindSchema(schema)
			bat := catalog.MockBatch(schema, 40)

			tae.CreateRelAndAppend(bat, true)

			_, rel := tae.GetRelation()
			blkEntry := testutil.GetOneObject(rel).GetMeta().(*catalog.ObjectEntry)
			blkID := blkEntry.AsCommonID()

			beforeDel := tsAlloc.Alloc()
			txn1, rel := tae.GetRelation()
			assert.NoError(t, rel.RangeDelete(blkID, 0, 0, handle.DT_Normal))
			assert.NoError(t, txn1.Commit(context.Background()))

			afterFirstDel := tsAlloc.Alloc()
			txn2, rel := tae.GetRelation()
			assert.NoError(t, rel.RangeDelete(blkID, 1, 3, handle.DT_Normal))
			assert.NoError(t, txn2.Commit(context.Background()))

			afterSecondDel := tsAlloc.Alloc()

			tae.CompactBlocks(false)
			_, rel = tae.GetRelation()
			tombstoneObjectEntry := testutil.GetOneTombstoneMeta(rel)
			objectEntry := testutil.GetOneBlockMeta(rel)
			objStats := tombstoneObjectEntry.ObjectMVCCNode
			testDS := NewTestBlockReadSource(objStats.ObjectLocation())
			ds := logtail.NewDeltaLocDataSource(ctx, tae.DB.Runtime.Fs.Service, beforeDel, testDS)
			bid, _ := blkEntry.ID(), blkEntry.ID()

			info := &objectio.BlockInfo{
				BlockID:    *objectio.NewBlockidWithObjectID(bid, 0),
				Appendable: false, // TODO: jxm
			}
			metaloc := objectEntry.ObjectLocation()
			metaloc.SetRows(schema.BlockMaxRows)
			info.SetMetaLocation(metaloc)

			columns := make([]string, 0)
			colIdxs := make([]uint16, 0)
			colTyps := make([]types.Type, 0)
			defs := schema.ColDefs[:]
			rand.Shuffle(len(defs), func(i, j int) { defs[i], defs[j] = defs[j], defs[i] })
			for _, col := range defs {
				columns = append(columns, col.Name)
				colIdxs = append(colIdxs, uint16(col.Idx))
				colTyps = append(colTyps, col.Type)
			}
			t.Log("read columns: ", columns)
			fs := tae.DB.Runtime.Fs.Service
			pool, err := mpool.NewMPool("test", 0, mpool.NoFixed)
			assert.NoError(t, err)
			infos := make([]*objectio.BlockInfo, 0)
			infos = append(infos, info)
			err = blockio.BlockPrefetch("", colIdxs, fs, infos, false)
			assert.NoError(t, err)

			var vp engine.VectorPool
			buildBatch := func(typs []types.Type) *batch.Batch {
				bat := batch.NewWithSize(len(typs))
				//bat.Attrs = append(bat.Attrs, cols...)

				for i := 0; i < len(typs); i++ {
					if vp == nil {
						bat.Vecs[i] = vector.NewVec(typs[i])
					} else {
						bat.Vecs[i] = vp.GetVector(typs[i])
					}
				}
				return bat
			}
			b1 := buildBatch(colTyps)
			err = blockio.BlockDataReadInner(
				context.Background(), "", info, ds, colIdxs, colTyps,
				beforeDel, nil, fs, pool, nil, fileservice.Policy(0), b1,
			)
			assert.NoError(t, err)
			assert.Equal(t, len(columns), len(b1.Vecs))
			assert.Equal(t, 20, b1.Vecs[0].Length())

			testDS.SetTS(afterFirstDel)

			b2 := buildBatch(colTyps)
			err = blockio.BlockDataReadInner(
				context.Background(), "", info, ds, colIdxs, colTyps,
				afterFirstDel, nil, fs, pool, nil, fileservice.Policy(0), b2,
			)
			assert.NoError(t, err)
			assert.Equal(t, 19, b2.Vecs[0].Length())

			testDS.SetTS(afterSecondDel)

			b3 := buildBatch(colTyps)
			err = blockio.BlockDataReadInner(
				context.Background(), "", info, ds, colIdxs, colTyps,
				afterSecondDel, nil, fs, pool, nil, fileservice.Policy(0), b3,
			)
			assert.NoError(t, err)
			assert.Equal(t, len(columns), len(b2.Vecs))
			assert.Equal(t, 16, b3.Vecs[0].Length())
			// read rowid column only
			b4 := buildBatch([]types.Type{types.T_Rowid.ToType()})
			err = blockio.BlockDataReadInner(
				context.Background(), "", info,
				ds,
				[]uint16{2},
				[]types.Type{types.T_Rowid.ToType()},
				afterSecondDel, nil, fs, pool, nil, fileservice.Policy(0), b4,
			)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(b4.Vecs))
			assert.Equal(t, 16, b4.Vecs[0].Length())

			// read rowid column only
			info.Appendable = false
			b5 := buildBatch([]types.Type{types.T_Rowid.ToType()})
			err = blockio.BlockDataReadInner(
				context.Background(), "", info,
				ds, []uint16{2},
				[]types.Type{types.T_Rowid.ToType()},
				afterSecondDel, nil, fs, pool, nil, fileservice.Policy(0), b5,
			)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(b5.Vecs))
			assert.Equal(t, 16, b5.Vecs[0].Length())
		},
	)
}

func TestCompactDeltaBlk(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(3, 1)
	schema.BlockMaxRows = 6
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 5)

	tae.CreateRelAndAppend(bat, true)

	{
		v := testutil.GetSingleSortKeyValue(bat, schema, 1)
		filter := handle.NewEQFilter(v)
		txn2, rel := tae.GetRelation()
		testutil.CheckAllColRowsByScan(t, rel, 5, true)
		_ = rel.DeleteByFilter(context.Background(), filter)
		assert.Nil(t, txn2.Commit(context.Background()))
	}

	_, rel := tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, 4, true)

	{
		txn, rel := tae.GetRelation()
		blk := testutil.GetOneObject(rel)
		meta := blk.GetMeta().(*catalog.ObjectEntry)
		task, err := jobs.NewFlushTableTailTask(nil, txn, []*catalog.ObjectEntry{meta}, nil, tae.DB.Runtime)
		assert.NoError(t, err)
		err = task.OnExec(context.Background())
		assert.NoError(t, err)
		assert.False(t, meta.GetLatestNode().IsEmpty())
		created := task.GetCreatedObjects().GetMeta().(*catalog.ObjectEntry)
		assert.False(t, created.GetLatestNode().IsEmpty())
		err = txn.Commit(context.Background())
		assert.Nil(t, err)
		err = meta.GetTable().RemoveEntry(meta)
		assert.Nil(t, err)
	}
	{
		v := testutil.GetSingleSortKeyValue(bat, schema, 2)
		filter := handle.NewEQFilter(v)
		txn2, rel := tae.GetRelation()
		testutil.CheckAllColRowsByScan(t, rel, 4, true)
		_ = rel.DeleteByFilter(context.Background(), filter)
		assert.Nil(t, txn2.Commit(context.Background()))
	}
	{
		txn, rel := tae.GetRelation()
		blk := testutil.GetOneObject(rel)
		meta := blk.GetMeta().(*catalog.ObjectEntry)
		tombstone := testutil.GetOneTombstoneMeta(rel)
		assert.False(t, meta.IsAppendable())
		task2, err := jobs.NewFlushTableTailTask(nil, txn, nil, []*catalog.ObjectEntry{tombstone}, tae.DB.Runtime)
		assert.NoError(t, err)
		err = task2.OnExec(context.Background())
		assert.NoError(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
		t.Log(tae.Catalog.SimplePPString(3))

		txn, _ = tae.GetRelation()
		task, err := jobs.NewMergeObjectsTask(nil, txn, []*catalog.ObjectEntry{meta}, tae.DB.Runtime, 0, false)
		assert.NoError(t, err)
		err = task.OnExec(context.Background())
		assert.NoError(t, err)
		t.Log(tae.Catalog.SimplePPString(3))
		assert.True(t, !meta.IsEmpty())
		created := task.GetCreatedObjects()[0]
		assert.False(t, created.IsEmpty())
		err = txn.Commit(context.Background())
		assert.Nil(t, err)
	}

	_, rel = tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, 3, true)

	tae.Restart(ctx)
	_, rel = tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, 3, true)
}

func TestFlushTable(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	tae.BGCheckpointRunner.DebugUpdateOptions(
		checkpoint.WithForceFlushCheckInterval(time.Millisecond * 5))

	schema := catalog.MockSchemaAll(3, 1)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 21)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)

	_, rel := tae.GetRelation()
	db, err := rel.GetDB()
	assert.Nil(t, err)
	table, err := db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	err = tae.FlushTable(
		context.Background(),
		0,
		db.GetID(),
		table.ID(),
		types.BuildTS(time.Now().UTC().UnixNano(), 0))
	assert.NoError(t, err)
	t.Log(tae.Catalog.SimplePPString(common.PPL1))

	txn, rel := tae.GetRelation()
	it := rel.MakeObjectIt(false)
	for it.Next() {
		blk := it.GetObject().GetMeta().(*catalog.ObjectEntry)
		assert.True(t, blk.HasPersistedData())
	}
	it.Close()
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestReadCheckpoint(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(3, 1)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 21)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)
	now := time.Now()
	testutils.WaitExpect(10000, func() bool {
		return tae.Runtime.Scheduler.GetPenddingLSNCnt() == 0
	})
	t.Log(time.Since(now))
	t.Logf("Checkpointed: %d", tae.Runtime.Scheduler.GetCheckpointedLSN())
	t.Logf("GetPenddingLSNCnt: %d", tae.Runtime.Scheduler.GetPenddingLSNCnt())
	assert.Equal(t, uint64(0), tae.Runtime.Scheduler.GetPenddingLSNCnt())
	tids := []uint64{
		pkgcatalog.MO_DATABASE_ID,
		pkgcatalog.MO_TABLES_ID,
		pkgcatalog.MO_COLUMNS_ID,
		1000,
	}

	now = time.Now()
	testutils.WaitExpect(10000, func() bool {
		return tae.Runtime.Scheduler.GetPenddingLSNCnt() == 0
	})
	t.Log(time.Since(now))
	assert.Equal(t, uint64(0), tae.Runtime.Scheduler.GetPenddingLSNCnt())

	now = time.Now()
	testutils.WaitExpect(10000, func() bool {
		return tae.BGCheckpointRunner.GetPenddingIncrementalCount() == 0
	})
	t.Log(time.Since(now))
	assert.Equal(t, 0, tae.BGCheckpointRunner.GetPenddingIncrementalCount())

	gcTS := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	err := tae.BGCheckpointRunner.GCByTS(context.Background(), gcTS)
	assert.NoError(t, err)
	now = time.Now()
	assert.Equal(t, uint64(0), tae.Wal.GetPenddingCnt())
	testutils.WaitExpect(10000, func() bool {
		tae.BGCheckpointRunner.ExistPendingEntryToGC()
		return !tae.BGCheckpointRunner.ExistPendingEntryToGC()
	})
	t.Log(time.Since(now))
	assert.False(t, tae.BGCheckpointRunner.ExistPendingEntryToGC())
	entries := tae.BGCheckpointRunner.GetAllGlobalCheckpoints()
	for _, entry := range entries {
		t.Log(entry.String())
	}
	for _, entry := range entries {
		for _, tid := range tids {
			ins, del, _, _, err := entry.GetByTableID(context.Background(), tae.Runtime.Fs, tid)
			assert.NoError(t, err)
			t.Logf("table %d", tid)
			if ins != nil {
				logutil.Infof("ins is %v", ins.Vecs[0].String())
				t.Log(common.ApiBatchToString(ins, 3))
			}
			if del != nil {
				t.Log(common.ApiBatchToString(del, 3))
			}
		}
	}
	tae.Restart(ctx)
	entries = tae.BGCheckpointRunner.GetAllGlobalCheckpoints()
	entry := entries[len(entries)-1]
	for _, tid := range tids {
		ins, del, _, _, err := entry.GetByTableID(context.Background(), tae.Runtime.Fs, tid)
		assert.NoError(t, err)
		t.Logf("table %d", tid)
		if ins != nil {
			t.Log(common.ApiBatchToString(ins, 3))
		}
		if del != nil {
			t.Log(common.ApiBatchToString(del, 3))
		}
	}
}

func TestDelete4(t *testing.T) {
	t.Skip(any("This case crashes occasionally, is being fixed, skip it for now"))
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.NewEmptySchema("xx")
	schema.AppendPKCol("name", types.T_varchar.ToType(), 0)
	schema.AppendCol("offset", types.T_uint32.ToType())
	schema.Finalize(false)
	schema.BlockMaxRows = 50
	schema.ObjectMaxBlocks = 5
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 1)
	bat.Vecs[1].Update(0, uint32(0), false)
	defer bat.Close()
	tae.CreateRelAndAppend(bat, true)

	filter := handle.NewEQFilter(bat.Vecs[0].Get(0))
	var wg sync.WaitGroup
	var count atomic.Uint32

	run := func() {
		defer wg.Done()
		time.Sleep(time.Duration(rand.Intn(20)+1) * time.Millisecond)
		cloneBat := bat.CloneWindow(0, 1)
		defer cloneBat.Close()
		txn, rel := tae.GetRelation()
		id, offset, err := rel.GetByFilter(context.Background(), filter)
		if err != nil {
			txn.Rollback(context.Background())
			return
		}
		v, _, err := rel.GetValue(id, offset, 1, false)
		if err != nil {
			txn.Rollback(context.Background())
			return
		}
		oldV := v.(uint32)
		newV := oldV + 1
		if err := rel.RangeDelete(id, offset, offset, handle.DT_Normal); err != nil {
			txn.Rollback(context.Background())
			return
		}
		cloneBat.Vecs[1].Update(0, newV, false)
		if err := rel.Append(context.Background(), cloneBat); err != nil {
			txn.Rollback(context.Background())
			return
		}
		if err := txn.Commit(context.Background()); err == nil {
			ok := count.CompareAndSwap(oldV, newV)
			for !ok {
				ok = count.CompareAndSwap(oldV, newV)
			}
			t.Logf("RangeDelete block-%d, offset-%d, old %d newV %d, %s", id.BlockID, offset, oldV, newV, txn.GetCommitTS().ToString())
		}
	}

	p, _ := ants.NewPool(20)
	defer p.Release()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		_ = p.Submit(run)
	}
	wg.Wait()

	t.Logf("count=%v", count.Load())

	getValueFn := func() {
		txn, rel := tae.GetRelation()
		v, _, err := rel.GetValueByFilter(context.Background(), filter, 1)
		assert.NoError(t, err)
		assert.Equal(t, int(count.Load()), int(v.(uint32)))
		assert.NoError(t, txn.Commit(context.Background()))
		t.Logf("GetV=%v, %s", v, txn.GetStartTS().ToString())
	}
	scanFn := func() {
		txn, rel := tae.GetRelation()
		it := rel.MakeObjectIt(false)
		for it.Next() {
			blk := it.GetObject()
			for j := 0; j < blk.BlkCnt(); j++ {
				var view *containers.Batch
				err := blk.HybridScan(ctx, &view, uint16(j), []int{0}, common.DefaultAllocator)
				assert.NoError(t, err)
				view.Compact()
				if view.Length() != 0 {
					t.Logf("block-%d, data=%s", j, logtail.ToStringTemplate(view.Vecs[0], -1))
				}
				view.Close()
			}
		}
		it.Close()
		txn.Commit(context.Background())
	}

	for i := 0; i < 20; i++ {
		getValueFn()
		scanFn()

		tae.Restart(ctx)

		getValueFn()
		scanFn()
		for j := 0; j < 100; j++ {
			wg.Add(1)
			p.Submit(run)
		}
		wg.Wait()
	}
	t.Log(tae.Catalog.SimplePPString(common.PPL3))
}

// append, delete, apppend, get start ts, compact, get active row
func TestGetActiveRow(t *testing.T) {
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(3, 1)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 1)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)

	txn, rel := tae.GetRelation()
	v := testutil.GetSingleSortKeyValue(bat, schema, 0)
	filter := handle.NewEQFilter(v)
	id, row, err := rel.GetByFilter(context.Background(), filter)
	assert.NoError(t, err)
	err = rel.RangeDelete(id, row, row, handle.DT_Normal)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	assert.NoError(t, rel.Append(context.Background(), bat))
	assert.NoError(t, txn.Commit(context.Background()))

	_, rel = tae.GetRelation()
	{
		txn2, rel2 := tae.GetRelation()
		blk := testutil.GetOneObject(rel2).GetMeta().(*catalog.ObjectEntry)
		task, err := jobs.NewFlushTableTailTask(nil, txn2, []*catalog.ObjectEntry{blk}, nil, tae.Runtime)
		assert.NoError(t, err)
		err = task.OnExec(context.Background())
		assert.NoError(t, err)
		assert.NoError(t, txn2.Commit(context.Background()))
	}
	filter = handle.NewEQFilter(v)
	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.NoError(t, err)
}
func TestTransfer(t *testing.T) {
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(5, 3)
	schema.BlockMaxRows = 100
	schema.ObjectMaxBlocks = 10
	tae.BindSchema(schema)

	bat := catalog.MockBatch(schema, 10)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)

	filter := handle.NewEQFilter(bat.Vecs[3].Get(3))

	txn1, rel1 := tae.GetRelation()
	err := rel1.DeleteByFilter(context.Background(), filter)
	assert.NoError(t, err)

	meta := rel1.GetMeta().(*catalog.TableEntry)
	err = tae.FlushTable(context.Background(), 0, meta.GetDB().ID, meta.ID,
		types.BuildTS(time.Now().UTC().UnixNano(), 0))
	assert.NoError(t, err)

	err = txn1.Commit(context.Background())
	// assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnRWConflict))
	assert.NoError(t, err)

	txn2, rel2 := tae.GetRelation()
	_, _, err = rel2.GetValueByFilter(context.Background(), filter, 3)
	t.Log(err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound))
	v, _, err := rel2.GetValueByFilter(context.Background(), handle.NewEQFilter(bat.Vecs[3].Get(4)), 2)
	expectV := bat.Vecs[2].Get(4)
	assert.Equal(t, expectV, v)
	assert.NoError(t, err)
	_ = txn2.Commit(context.Background())
}

func TestTransfer2(t *testing.T) {
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(5, 3)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 10
	tae.BindSchema(schema)

	bat := catalog.MockBatch(schema, 200)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)

	filter := handle.NewEQFilter(bat.Vecs[3].Get(3))

	txn1, rel1 := tae.GetRelation()
	err := rel1.DeleteByFilter(context.Background(), filter)
	assert.NoError(t, err)

	tae.CompactBlocks(false)
	tae.MergeBlocks(false)

	err = txn1.Commit(context.Background())
	// assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnRWConflict))
	assert.NoError(t, err)

	txn2, rel2 := tae.GetRelation()
	_, _, err = rel2.GetValueByFilter(context.Background(), filter, 3)
	t.Log(err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound))
	v, _, err := rel2.GetValueByFilter(context.Background(), handle.NewEQFilter(bat.Vecs[3].Get(4)), 2)
	expectV := bat.Vecs[2].Get(4)
	assert.Equal(t, expectV, v)
	assert.NoError(t, err)
	_ = txn2.Commit(context.Background())
}

func TestMergeBlocks3(t *testing.T) {
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(5, 3)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 5
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 100)
	defer bat.Close()
	tae.CreateRelAndAppend(bat, true)

	// flush to nblk
	{
		txn, rel := tae.GetRelation()
		blkMetas := testutil.GetAllBlockMetas(rel, false)
		tombstoneMetas := testutil.GetAllBlockMetas(rel, true)
		task, err := jobs.NewFlushTableTailTask(tasks.WaitableCtx, txn, blkMetas, tombstoneMetas, tae.DB.Runtime)
		assert.NoError(t, err)
		assert.NoError(t, task.OnExec(context.Background()))
		assert.NoError(t, txn.Commit(context.Background()))
	}

	filter15 := handle.NewEQFilter(bat.Vecs[3].Get(15))
	filter19 := handle.NewEQFilter(bat.Vecs[3].Get(19))
	filter18 := handle.NewEQFilter(bat.Vecs[3].Get(18))
	filter17 := handle.NewEQFilter(bat.Vecs[3].Get(17))
	// delete all rows in first blk in obj1 and the 5th,9th rows in blk2
	{
		txn, rel := tae.GetRelation()
		obj1 := testutil.GetOneObject(rel).GetMeta().(*catalog.ObjectEntry)
		objHandle, err := rel.GetObject(obj1.ID(), false)
		assert.NoError(t, err)

		var view *containers.Batch
		err = objHandle.Scan(ctx, &view, 0, []int{schema.GetColIdx(catalog.PhyAddrColumnName)}, common.DefaultAllocator)
		assert.NoError(t, err)
		assert.NotNil(t, *view)
		pkDef := schema.GetPrimaryKey()
		var pkView *containers.Batch
		err = objHandle.Scan(ctx, &pkView, 0, []int{pkDef.Idx}, common.DefaultAllocator)
		assert.NoError(t, err)
		err = rel.DeleteByPhyAddrKeys(view.Vecs[0], pkView.Vecs[0])
		assert.NoError(t, err)

		assert.NoError(t, rel.DeleteByFilter(context.Background(), filter15))
		assert.NoError(t, rel.DeleteByFilter(context.Background(), filter19))
		assert.NoError(t, txn.Commit(context.Background()))
	}

	// 1. merge first Object
	// 2. delete 7th row in blk2 during executing merge task
	// 3. delete 8th row in blk2 and commit that after merging, test transfer
	{
		del8txn, rel8 := tae.GetRelation()
		valrow8, null, err := rel8.GetValueByFilter(context.Background(), filter18, schema.GetColIdx(catalog.PhyAddrColumnName))
		assert.NoError(t, err)
		assert.False(t, null)

		del7txn, rel7 := tae.GetRelation()
		mergetxn, relm := tae.GetRelation()

		// merge first Object
		obj1 := testutil.GetOneObject(relm).GetMeta().(*catalog.ObjectEntry)
		assert.NoError(t, err)

		objsToMerge := []*catalog.ObjectEntry{obj1}
		task, err := jobs.NewMergeObjectsTask(nil, mergetxn, objsToMerge, tae.Runtime, 0, false)
		assert.NoError(t, err)
		assert.NoError(t, task.OnExec(context.Background()))

		// delete del7 after starting merge txn
		assert.NoError(t, rel7.DeleteByFilter(context.Background(), filter17))
		assert.NoError(t, del7txn.Commit(context.Background()))

		// commit merge, and it will carry del7 to the new block
		assert.NoError(t, mergetxn.Commit(context.Background()))

		// delete 8 row and it is expected to be transfered correctly
		rel8.DeleteByPhyAddrKey(valrow8)
		assert.NoError(t, del8txn.Commit(context.Background()))
	}

	// consistency check
	{
		var err error
		txn, rel := tae.GetRelation()
		_, _, err = rel.GetValueByFilter(context.Background(), filter15, 3)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound))
		_, _, err = rel.GetValueByFilter(context.Background(), filter17, 3)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound))
		_, _, err = rel.GetValueByFilter(context.Background(), filter18, 3)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound))
		_, _, err = rel.GetValueByFilter(context.Background(), filter19, 3)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound))

		testutil.CheckAllColRowsByScan(t, rel, 86, true)
		assert.NoError(t, txn.Commit(context.Background()))
	}
}

func TestTransfer3(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(5, 3)
	schema.BlockMaxRows = 100
	schema.ObjectMaxBlocks = 10
	tae.BindSchema(schema)

	bat := catalog.MockBatch(schema, 10)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)

	filter := handle.NewEQFilter(bat.Vecs[3].Get(3))

	txn1, rel1 := tae.GetRelation()

	var err error
	err = rel1.DeleteByFilter(context.Background(), filter)
	assert.NoError(t, err)

	meta := rel1.GetMeta().(*catalog.TableEntry)
	err = tae.FlushTable(context.Background(), 0, meta.GetDB().ID, meta.ID,
		types.BuildTS(time.Now().UTC().UnixNano(), 0))
	assert.NoError(t, err)

	err = rel1.Append(context.Background(), bat.Window(3, 1))
	assert.NoError(t, err)
	err = txn1.Commit(context.Background())
	assert.NoError(t, err)
}

func TestUpdate(t *testing.T) {
	t.Skip(any("This case crashes occasionally, is being fixed, skip it for now"))
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts2(nil, 5)
	// opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(5, 3)
	schema.BlockMaxRows = 100
	schema.ObjectMaxBlocks = 4
	tae.BindSchema(schema)

	bat := catalog.MockBatch(schema, 1)
	defer bat.Close()
	bat.Vecs[2].Update(0, int32(0), false)

	tae.CreateRelAndAppend(bat, true)

	var wg sync.WaitGroup

	var expectV atomic.Int32
	expectV.Store(bat.Vecs[2].Get(0).(int32))
	filter := handle.NewEQFilter(bat.Vecs[3].Get(0))
	updateFn := func() {
		defer wg.Done()
		txn, rel := tae.GetRelation()
		id, offset, err := rel.GetByFilter(context.Background(), filter)
		assert.NoError(t, err)
		v, _, err := rel.GetValue(id, offset, 2, false)
		assert.NoError(t, err)
		err = rel.RangeDelete(id, offset, offset, handle.DT_Normal)
		if err != nil {
			t.Logf("range delete %v, rollbacking", err)
			_ = txn.Rollback(context.Background())
			return
		}
		tuples := bat.CloneWindow(0, 1)
		defer tuples.Close()
		updatedV := v.(int32) + 1
		tuples.Vecs[2].Update(0, updatedV, false)
		err = rel.Append(context.Background(), tuples)
		assert.NoError(t, err)

		err = txn.Commit(context.Background())
		if err != nil {
			t.Logf("commit update %v", err)
		} else {
			expectV.CompareAndSwap(v.(int32), updatedV)
			t.Logf("%v committed", updatedV)
		}
	}
	p, _ := ants.NewPool(5)
	defer p.Release()
	loop := 1000
	for i := 0; i < loop; i++ {
		wg.Add(1)
		// updateFn()
		_ = p.Submit(updateFn)
	}
	wg.Wait()
	t.Logf("Final: %v", expectV.Load())
	{
		txn, rel := tae.GetRelation()
		v, _, err := rel.GetValueByFilter(context.Background(), filter, 2)
		assert.NoError(t, err)
		assert.Equal(t, v.(int32), expectV.Load())
		testutil.CheckAllColRowsByScan(t, rel, 1, true)
		assert.NoError(t, txn.Commit(context.Background()))
	}
}

func TestMergeMemsize(t *testing.T) {
	t.Skip("run it manully to observe memory heap")
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(18, 3)
	schema.Name = "testupdate"
	schema.BlockMaxRows = 8192
	schema.ObjectMaxBlocks = 200
	tae.BindSchema(schema)

	wholebat := catalog.MockBatch(schema, 8192*80)
	for _, col := range schema.ColDefs {
		t.Log(col.Type.DescString(), col.Type.Size)
	}
	t.Log(wholebat.ApproxSize())
	batCnt := 40
	bats := wholebat.Split(batCnt)
	// write only one block by apply metaloc
	objName1 := objectio.BuildObjectNameWithObjectID(objectio.NewObjectid())
	writer, err := blockio.NewBlockWriterNew(tae.Runtime.Fs.Service, objName1, 0, nil)
	assert.Nil(t, err)
	writer.SetPrimaryKey(3)
	for _, b := range bats {
		_, err = writer.WriteBatch(containers.ToCNBatch(b))
		assert.Nil(t, err)
	}
	blocks, _, err := writer.Sync(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, batCnt, len(blocks))
	statsVec := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	statsVec.Append(writer.GetObjectStats()[objectio.SchemaData][:], false)
	{
		txn, _ := tae.StartTxn(nil)
		txn.SetDedupType(txnif.DedupPolicy_CheckIncremental)
		db, err := txn.CreateDatabase("db", "", "")
		assert.NoError(t, err)
		tbl, err := db.CreateRelation(schema)
		assert.NoError(t, err)
		assert.NoError(t, tbl.AddObjsWithMetaLoc(context.Background(), statsVec))
		assert.NoError(t, txn.Commit(context.Background()))
	}
	statsVec.Close()

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	var metas []*catalog.ObjectEntry
	{
		txn, rel := tae.GetRelation()
		it := rel.MakeObjectIt(false)
		blkcnt := 0
		for it.Next() {
			obj := it.GetObject()
			defer obj.Close()
			meta := it.GetObject().GetMeta().(*catalog.ObjectEntry)
			stat := meta.GetObjectStats()
			blkcnt += int(stat.BlkCnt())
			metas = append(metas, meta)

		}
		it.Next()
		txn.Commit(ctx)
		assert.Equal(t, batCnt, blkcnt)
	}

	{
		txn, _ := tae.StartTxn(nil)
		task, err := jobs.NewMergeObjectsTask(nil, txn, metas, tae.Runtime, 0, false)
		assert.NoError(t, err)

		dbutils.PrintMemStats()
		err = task.OnExec(context.Background())
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))
		dbutils.PrintMemStats()
	}
}

func TestCollectDeletesAfterCKP(t *testing.T) {
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(5, 3)
	schema.Name = "testupdate"
	schema.BlockMaxRows = 8192
	schema.ObjectMaxBlocks = 20
	tae.BindSchema(schema)

	bat := catalog.MockBatch(schema, 400)
	// write only one block by apply metaloc
	objName1 := objectio.BuildObjectNameWithObjectID(objectio.NewObjectid())
	writer, err := blockio.NewBlockWriterNew(tae.Runtime.Fs.Service, objName1, 0, nil)
	assert.Nil(t, err)
	writer.SetPrimaryKey(3)
	_, err = writer.WriteBatch(containers.ToCNBatch(bat))
	assert.Nil(t, err)
	blocks, _, err := writer.Sync(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 1, len(blocks))
	statsVec := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	statsVec.Append(writer.GetObjectStats()[objectio.SchemaData][:], false)
	defer statsVec.Close()
	{
		txn, _ := tae.StartTxn(nil)
		txn.SetDedupType(txnif.DedupPolicy_CheckIncremental)
		db, err := txn.CreateDatabase("db", "", "")
		assert.NoError(t, err)
		tbl, err := db.CreateRelation(schema)
		assert.NoError(t, err)
		assert.NoError(t, tbl.AddObjsWithMetaLoc(context.Background(), statsVec))
		assert.NoError(t, txn.Commit(context.Background()))
	}

	updateFn := func(round, i, j int) {
		tuples := bat.CloneWindow(0, 1)
		defer tuples.Close()
		for x := i; x < j; x++ {
			txn, rel := tae.GetRelation()
			filter := handle.NewEQFilter(int64(x))
			id, offset, err := rel.GetByFilter(context.Background(), filter)
			assert.NoError(t, err)
			_, _, err = rel.GetValue(id, offset, 2, false)
			assert.NoError(t, err)
			err = rel.RangeDelete(id, offset, offset, handle.DT_Normal)
			if err != nil {
				t.Logf("range delete %v, rollbacking", err)
				_ = txn.Rollback(context.Background())
				return
			}
			tuples.Vecs[3].Update(0, int64(x), false)
			err = rel.Append(context.Background(), tuples)
			assert.NoError(t, err)
			assert.NoError(t, txn.Commit(context.Background()))
		}
		t.Logf("(%d, %d, %d) done", round, i, j)
	}
	updateFn(1, 100, 110)
	{
		txn, rel := tae.GetRelation()
		meta := testutil.GetOneTombstoneMeta(rel)
		batches := make(map[uint32]*containers.BatchWithVersion)
		err := tables.RangeScanInMemoryByObject(ctx, meta, batches, types.TS{}, types.MaxTs(), common.DefaultAllocator)
		assert.NoError(t, err)
		bat = batches[schema.Version].Batch
		assert.Equal(t, 10, bat.Length())
		bat.Close()
		assert.NoError(t, txn.Commit(ctx))
	}
	logutil.Info(tae.Catalog.SimplePPString(3))
	tae.ForceLongCheckpoint()
	{
		txn, rel := tae.GetRelation()
		meta := testutil.GetOneTombstoneMeta(rel)
		assert.Equal(t, 10, meta.GetRows())
		assert.NoError(t, txn.Commit(ctx))
	}
	logutil.Info(tae.Catalog.SimplePPString(3))
	tae.Restart(ctx)
	logutil.Info(tae.Catalog.SimplePPString(3))
	{
		txn, rel := tae.GetRelation()
		meta := testutil.GetOneTombstoneMeta(rel)
		assert.Equal(t, 10, meta.GetRows())
		assert.NoError(t, txn.Commit(ctx))
	}
}

// This is used to observe a lot of compactions to overflow a Object, it is not compulsory
func TestAlwaysUpdate(t *testing.T) {
	t.Skip("This is a long test, run it manully to observe what you want")
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	// opts := config.WithQuickScanAndCKPOpts2(nil, 10)
	// opts.GCCfg.ScanGCInterval = 3600 * time.Second
	// opts.CatalogCfg.GCInterval = 3600 * time.Second
	opts := config.WithQuickScanAndCKPAndGCOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(5, 3)
	schema.Name = "testupdate"
	schema.BlockMaxRows = 8192
	schema.ObjectMaxBlocks = 200
	tae.BindSchema(schema)

	bats := catalog.MockBatch(schema, 400*100).Split(100)
	statsVec := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	defer statsVec.Close()
	// write only one Object
	for i := 0; i < 1; i++ {
		objName1 := objectio.BuildObjectNameWithObjectID(objectio.NewObjectid())
		writer, err := blockio.NewBlockWriterNew(tae.Runtime.Fs.Service, objName1, 0, nil)
		assert.Nil(t, err)
		writer.SetPrimaryKey(3)
		for _, bat := range bats[i*25 : (i+1)*25] {
			_, err := writer.WriteBatch(containers.ToCNBatch(bat))
			assert.Nil(t, err)
		}
		blocks, _, err := writer.Sync(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, 25, len(blocks))
		statsVec.Append(writer.GetObjectStats()[objectio.SchemaData][:], false)
	}

	// var did, tid uint64
	txn, _ := tae.StartTxn(nil)
	txn.SetDedupType(txnif.DedupPolicy_CheckIncremental)
	db, err := txn.CreateDatabase("db", "", "")
	// did = db.GetID()
	assert.NoError(t, err)
	tbl, err := db.CreateRelation(schema)
	// tid = tbl.ID()
	assert.NoError(t, err)
	assert.NoError(t, tbl.AddObjsWithMetaLoc(context.Background(), statsVec))
	assert.NoError(t, txn.Commit(context.Background()))

	t.Log(tae.Catalog.SimplePPString(common.PPL1))

	wg := &sync.WaitGroup{}

	updateFn := func(round, i, j int) {
		defer wg.Done()
		tuples := bats[0].CloneWindow(0, 1)
		defer tuples.Close()
		for x := i; x < j; x++ {
			txn, rel := tae.GetRelation()
			filter := handle.NewEQFilter(int64(x))
			id, offset, err := rel.GetByFilter(context.Background(), filter)
			assert.NoError(t, err)
			_, _, err = rel.GetValue(id, offset, 2, false)
			assert.NoError(t, err)
			err = rel.RangeDelete(id, offset, offset, handle.DT_Normal)
			if err != nil {
				t.Logf("range delete %v, rollbacking", err)
				_ = txn.Rollback(context.Background())
				return
			}
			tuples.Vecs[3].Update(0, int64(x), false)
			err = rel.Append(context.Background(), tuples)
			assert.NoError(t, err)
			assert.NoError(t, txn.Commit(context.Background()))
		}
		t.Logf("(%d, %d, %d) done", round, i, j)
	}

	p, _ := ants.NewPool(20)
	defer p.Release()

	// ch := make(chan int, 1)
	// ticker := time.NewTicker(1 * time.Second)
	// ticker2 := time.NewTicker(100 * time.Millisecond)
	// go func() {
	// 	for {
	// 		select {
	// 		case <-ticker.C:
	// 			t.Log(tbl.SimplePPString(common.PPL1))
	// 		case <-ticker2.C:
	// 			_, _, _ = logtail.HandleSyncLogTailReq(ctx, new(dummyCpkGetter), tae.LogtailMgr, tae.Catalog, api.SyncLogTailReq{
	// 				CnHave: totsp(types.BuildTS(0, 0)),
	// 				CnWant: totsp(types.MaxTs()),
	// 				Table:  &api.TableID{DbId: did, TbId: tid},
	// 			}, true)
	// 		case <-ch:
	// 		}
	// 	}
	// }()

	for r := 0; r < 10; r++ {
		for i := 0; i < 40; i++ {
			wg.Add(1)
			start, end := i*200, (i+1)*200
			f := func() { updateFn(r, start, end) }
			p.Submit(f)
		}
		wg.Wait()
		tae.CheckRowsByScan(100*100, true)
	}
}

func TestInsertPerf(t *testing.T) {
	t.Skip(any("for debug"))
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(10, 2)
	schema.BlockMaxRows = 1000
	schema.ObjectMaxBlocks = 5
	tae.BindSchema(schema)

	cnt := 1000
	iBat := 1
	poolSize := 20

	bat := catalog.MockBatch(schema, cnt*iBat*poolSize*2)
	defer bat.Close()

	tae.CreateRelAndAppend(bat.Window(0, 1), true)
	var wg sync.WaitGroup
	run := func(start int) func() {
		return func() {
			defer wg.Done()
			for i := start; i < start+cnt*iBat; i += iBat {
				txn, rel := tae.GetRelation()
				_ = rel.Append(context.Background(), bat.Window(i, iBat))
				_ = txn.Commit(context.Background())
			}
		}
	}

	p, _ := ants.NewPool(poolSize)
	defer p.Release()
	now := time.Now()
	for i := 1; i <= poolSize; i++ {
		wg.Add(1)
		_ = p.Submit(run(i * cnt * iBat))
	}
	wg.Wait()
	t.Log(time.Since(now))
}

func TestUpdatePerf(t *testing.T) {
	t.Skip(any("for debug"))
	ctx := context.Background()

	totalCnt := 4000
	poolSize := 2
	cnt := totalCnt / poolSize

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(10, 2)
	schema.BlockMaxRows = 1000
	schema.ObjectMaxBlocks = 5
	tae.BindSchema(schema)

	bat := catalog.MockBatch(schema, poolSize)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)
	var wg sync.WaitGroup
	run := func(idx int) func() {
		return func() {
			defer wg.Done()
			v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(idx)
			filter := handle.NewEQFilter(v)
			for i := 0; i < cnt; i++ {
				txn, rel := tae.GetRelation()
				err := rel.UpdateByFilter(context.Background(), filter, 0, int8(0), false)
				assert.NoError(t, err)
				err = txn.Commit(context.Background())
				assert.NoError(t, err)
				if i%50 == 0 {
					t.Logf("lalala %d", i)
				}
			}
		}
	}

	p, _ := ants.NewPool(poolSize)
	defer p.Release()
	now := time.Now()
	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		_ = p.Submit(run(i))
	}
	wg.Wait()
	t.Log(time.Since(now))
}

func TestUpdatePerf2(t *testing.T) {
	t.Skip(any("for debug"))
	ctx := context.Background()

	totalCnt := 10000000
	deleteCnt := 1000000
	updateCnt := 100000
	poolSize := 200

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(3, 2)
	schema.BlockMaxRows = 1000
	schema.ObjectMaxBlocks = 5
	tae.BindSchema(schema)

	bat := catalog.MockBatch(schema, totalCnt)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)
	tae.CompactBlocks(true)

	txn, rel := tae.GetRelation()
	it := rel.MakeObjectIt(false)
	blkCnt := totalCnt / int(schema.BlockMaxRows)
	deleteEachBlock := deleteCnt / blkCnt
	t.Logf("%d blocks", blkCnt)
	blkIdx := 0
	for it.Next() {
		txn2, rel2 := tae.GetRelation()
		obj := it.GetObject()
		meta := obj.GetMeta().(*catalog.ObjectEntry)
		id := meta.AsCommonID()
		for i := 0; i < meta.BlockCnt(); i++ {
			id.SetBlockOffset(uint16(i))
			for j := 0; j < deleteEachBlock; j++ {
				idx := uint32(rand.Intn(int(schema.BlockMaxRows)))
				rel2.RangeDelete(id, idx, idx, handle.DT_Normal)
			}
			blkIdx++
			if blkIdx%50 == 0 {
				t.Logf("lalala %d blk", blkIdx)
			}
		}
		txn2.Commit(ctx)
		tae.CompactBlocks(true)
	}
	it.Close()
	txn.Commit(ctx)

	var wg sync.WaitGroup
	var now time.Time
	run := func(index int) func() {
		return func() {
			defer wg.Done()
			for i := 0; i < (updateCnt)/poolSize; i++ {
				idx := rand.Intn(totalCnt)
				v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(idx)
				filter := handle.NewEQFilter(v)
				txn, rel := tae.GetRelation()
				rel.UpdateByFilter(context.Background(), filter, 0, int8(0), false)
				txn.Commit(context.Background())
				if index == 0 && i%50 == 0 {
					logutil.Infof("lalala %d", i)
				}
			}
		}
	}
	t.Log("start update")
	now = time.Now()

	p, _ := ants.NewPool(poolSize)
	defer p.Release()
	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		_ = p.Submit(run(i))
	}
	wg.Wait()
	t.Log(time.Since(now))
}

func TestDeletePerf(t *testing.T) {
	t.Skip(any("for debug"))
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPAndGCOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(10, 2)
	schema.BlockMaxRows = 1000
	schema.ObjectMaxBlocks = 5
	tae.BindSchema(schema)

	totalCount := 50000
	poolSize := 20
	cnt := totalCount / poolSize

	bat := catalog.MockBatch(schema, totalCount)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)
	var wg sync.WaitGroup
	run := func(start int) func() {
		return func() {
			defer wg.Done()
			for i := start * cnt; i < start*cnt+cnt; i++ {
				v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(i)
				filter := handle.NewEQFilter(v)
				txn, rel := tae.GetRelation()
				err := rel.DeleteByFilter(context.Background(), filter)
				assert.NoError(t, err)
				err = txn.Commit(context.Background())
				assert.NoError(t, err)
			}
		}
	}

	p, _ := ants.NewPool(poolSize)
	defer p.Release()
	now := time.Now()
	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		_ = p.Submit(run(i))
	}
	wg.Wait()
	t.Log(time.Since(now))
	t.Log(tae.Catalog.SimplePPString(3))
}

func TestAppendBat(t *testing.T) {
	p, _ := ants.NewPool(100)
	defer p.Release()
	var wg sync.WaitGroup

	schema := catalog.MockSchema(7, 2)
	bat := catalog.MockBatch(schema, 1000)
	defer bat.Close()

	run := func() {
		defer wg.Done()
		b := containers.BuildBatch(schema.Attrs(), schema.Types(), containers.Options{
			Allocator: common.DefaultAllocator})
		defer b.Close()
		for i := 0; i < bat.Length(); i++ {
			w := bat.Window(i, 1)
			b.Extend(w)
		}
	}

	for i := 0; i < 200; i++ {
		wg.Add(1)
		_ = p.Submit(run)
	}
	wg.Wait()
}

func TestGCWithCheckpoint(t *testing.T) {
	blockio.RunPipelineTest(
		func() {
			defer testutils.AfterTest(t)()
			ctx := context.Background()

			opts := config.WithQuickScanAndCKPAndGCOpts(nil)
			tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
			defer tae.Close()
			cleaner := gc.NewCheckpointCleaner(context.Background(), "", tae.Runtime.Fs, tae.BGCheckpointRunner, false)
			manager := gc.NewDiskCleaner(cleaner)
			manager.Start()
			defer manager.Stop()

			schema := catalog.MockSchemaAll(3, 1)
			schema.BlockMaxRows = 10
			schema.ObjectMaxBlocks = 2
			tae.BindSchema(schema)
			bat := catalog.MockBatch(schema, 21)
			defer bat.Close()

			tae.CreateRelAndAppend(bat, true)
			now := time.Now()
			testutils.WaitExpect(10000, func() bool {
				return tae.Runtime.Scheduler.GetPenddingLSNCnt() == 0
			})
			t.Log(time.Since(now))
			t.Logf("Checkpointed: %d", tae.Runtime.Scheduler.GetCheckpointedLSN())
			t.Logf("GetPenddingLSNCnt: %d", tae.Runtime.Scheduler.GetPenddingLSNCnt())
			assert.Equal(t, uint64(0), tae.Runtime.Scheduler.GetPenddingLSNCnt())
			err := manager.GC(context.Background())
			assert.Nil(t, err)
			entries := tae.BGCheckpointRunner.GetAllIncrementalCheckpoints()
			num := len(entries)
			assert.Greater(t, num, 0)
			testutils.WaitExpect(5000, func() bool {
				if manager.GetCleaner().GetMaxConsumed() == nil {
					return false
				}
				end := entries[num-1].GetEnd()
				maxEnd := manager.GetCleaner().GetMaxConsumed().GetEnd()
				return end.Equal(&maxEnd)
			})
			end := entries[num-1].GetEnd()
			maxEnd := manager.GetCleaner().GetMaxConsumed().GetEnd()
			assert.True(t, end.Equal(&maxEnd))
			cleaner2 := gc.NewCheckpointCleaner(context.Background(), "", tae.Runtime.Fs, tae.BGCheckpointRunner, false)
			manager2 := gc.NewDiskCleaner(cleaner2)
			manager2.Start()
			defer manager2.Stop()
			testutils.WaitExpect(5000, func() bool {
				if manager2.GetCleaner().GetMaxConsumed() == nil {
					return false
				}
				end := entries[num-1].GetEnd()
				maxEnd := manager2.GetCleaner().GetMaxConsumed().GetEnd()
				return end.Equal(&maxEnd)
			})
			end = entries[num-1].GetEnd()
			maxEnd = manager2.GetCleaner().GetMaxConsumed().GetEnd()
			assert.True(t, end.Equal(&maxEnd))
			tables1 := manager.GetCleaner().GetInputs()
			tables2 := manager2.GetCleaner().GetInputs()
			assert.True(t, tables1.Compare(tables2))
		},
	)
}

func TestGCDropDB(t *testing.T) {
	blockio.RunPipelineTest(
		func() {
			defer testutils.AfterTest(t)()
			ctx := context.Background()

			opts := config.WithQuickScanAndCKPAndGCOpts(nil)
			tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
			defer tae.Close()
			cleaner := gc.NewCheckpointCleaner(context.Background(), "", tae.Runtime.Fs, tae.BGCheckpointRunner, false)
			manager := gc.NewDiskCleaner(cleaner)
			manager.Start()
			defer manager.Stop()
			schema := catalog.MockSchemaAll(3, 1)
			schema.BlockMaxRows = 10
			schema.ObjectMaxBlocks = 2
			tae.BindSchema(schema)
			bat := catalog.MockBatch(schema, 210)
			defer bat.Close()

			tae.CreateRelAndAppend(bat, true)
			txn, err := tae.StartTxn(nil)
			assert.Nil(t, err)
			db, err := txn.DropDatabase(testutil.DefaultTestDB)
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit(context.Background()))

			assert.Equal(t, txn.GetCommitTS(), db.GetMeta().(*catalog.DBEntry).GetDeleteAtLocked())
			now := time.Now()
			testutils.WaitExpect(10000, func() bool {
				return tae.Runtime.Scheduler.GetPenddingLSNCnt() == 0
			})
			t.Log(time.Since(now))
			err = manager.GC(context.Background())
			assert.Nil(t, err)
			entries := tae.BGCheckpointRunner.GetAllIncrementalCheckpoints()
			num := len(entries)
			assert.Greater(t, num, 0)
			testutils.WaitExpect(5000, func() bool {
				if manager.GetCleaner().GetMaxConsumed() == nil {
					return false
				}
				end := entries[num-1].GetEnd()
				maxEnd := manager.GetCleaner().GetMaxConsumed().GetEnd()
				return end.Equal(&maxEnd)
			})
			end := entries[num-1].GetEnd()
			maxEnd := manager.GetCleaner().GetMaxConsumed().GetEnd()
			assert.True(t, end.Equal(&maxEnd))
			cleaner2 := gc.NewCheckpointCleaner(context.Background(), "", tae.Runtime.Fs, tae.BGCheckpointRunner, false)
			manager2 := gc.NewDiskCleaner(cleaner2)
			manager2.Start()
			defer manager2.Stop()
			testutils.WaitExpect(5000, func() bool {
				if manager2.GetCleaner().GetMaxConsumed() == nil {
					return false
				}
				end := entries[num-1].GetEnd()
				maxEnd := manager2.GetCleaner().GetMaxConsumed().GetEnd()
				return end.Equal(&maxEnd)
			})
			end = entries[num-1].GetEnd()
			maxEnd = manager2.GetCleaner().GetMaxConsumed().GetEnd()
			assert.True(t, end.Equal(&maxEnd))
			tables1 := manager.GetCleaner().GetInputs()
			tables2 := manager2.GetCleaner().GetInputs()
			assert.True(t, tables1.Compare(tables2))
			tae.Restart(ctx)
		},
	)
}

func TestGCDropTable(t *testing.T) {
	blockio.RunPipelineTest(
		func() {
			defer testutils.AfterTest(t)()
			ctx := context.Background()

			opts := config.WithQuickScanAndCKPAndGCOpts(nil)
			tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
			defer tae.Close()
			cleaner := gc.NewCheckpointCleaner(context.Background(), "", tae.Runtime.Fs, tae.BGCheckpointRunner, false)
			manager := gc.NewDiskCleaner(cleaner)
			manager.Start()
			defer manager.Stop()
			schema := catalog.MockSchemaAll(3, 1)
			schema.BlockMaxRows = 10
			schema.ObjectMaxBlocks = 2
			tae.BindSchema(schema)
			bat := catalog.MockBatch(schema, 210)
			defer bat.Close()
			schema2 := catalog.MockSchemaAll(3, 1)
			schema2.BlockMaxRows = 10
			schema2.ObjectMaxBlocks = 2
			bat2 := catalog.MockBatch(schema2, 210)
			defer bat.Close()

			tae.CreateRelAndAppend(bat, true)
			txn, _ := tae.StartTxn(nil)
			db, err := txn.GetDatabase(testutil.DefaultTestDB)
			assert.Nil(t, err)
			rel, _ := db.CreateRelation(schema2)
			rel.Append(context.Background(), bat2)
			assert.Nil(t, txn.Commit(context.Background()))

			txn, err = tae.StartTxn(nil)
			assert.Nil(t, err)
			db, err = txn.GetDatabase(testutil.DefaultTestDB)
			assert.Nil(t, err)
			_, err = db.DropRelationByName(schema2.Name)
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit(context.Background()))

			now := time.Now()
			testutils.WaitExpect(10000, func() bool {
				return tae.Runtime.Scheduler.GetPenddingLSNCnt() == 0
			})
			assert.Equal(t, uint64(0), tae.Runtime.Scheduler.GetPenddingLSNCnt())
			assert.Equal(t, txn.GetCommitTS(), rel.GetMeta().(*catalog.TableEntry).GetDeleteAtLocked())
			t.Log(time.Since(now))
			err = manager.GC(context.Background())
			assert.Nil(t, err)
			entries := tae.BGCheckpointRunner.GetAllIncrementalCheckpoints()
			num := len(entries)
			assert.Greater(t, num, 0)
			testutils.WaitExpect(10000, func() bool {
				if manager.GetCleaner().GetMaxConsumed() == nil {
					return false
				}
				end := entries[num-1].GetEnd()
				maxEnd := manager.GetCleaner().GetMaxConsumed().GetEnd()
				return end.Equal(&maxEnd)
			})
			end := entries[num-1].GetEnd()
			maxEnd := manager.GetCleaner().GetMaxConsumed().GetEnd()
			assert.True(t, end.Equal(&maxEnd))
			cleaner2 := gc.NewCheckpointCleaner(context.Background(), "", tae.Runtime.Fs, tae.BGCheckpointRunner, false)
			manager2 := gc.NewDiskCleaner(cleaner2)
			manager2.Start()
			defer manager2.Stop()
			testutils.WaitExpect(5000, func() bool {
				if manager2.GetCleaner().GetMaxConsumed() == nil {
					return false
				}
				end := entries[num-1].GetEnd()
				maxEnd := manager2.GetCleaner().GetMaxConsumed().GetEnd()
				return end.Equal(&maxEnd)
			})
			end = entries[num-1].GetEnd()
			maxEnd = manager2.GetCleaner().GetMaxConsumed().GetEnd()
			assert.True(t, end.Equal(&maxEnd))
			tables1 := manager.GetCleaner().GetInputs()
			tables2 := manager2.GetCleaner().GetInputs()
			assert.True(t, tables1.Compare(tables2))
			tae.Restart(ctx)
		},
	)
}

func TestAlterRenameTbl(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(2, -1)
	schema.Name = "test"
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	schema.Constraint = []byte("start version")
	schema.Comment = "comment version"

	{
		var err error
		txn, _ := tae.StartTxn(nil)
		txn.CreateDatabase("xx", "", "")
		assert.NoError(t, txn.Commit(context.Background()))
		txn1, _ := tae.StartTxn(nil)
		txn2, _ := tae.StartTxn(nil)

		db, _ := txn1.GetDatabase("xx")
		_, err = db.CreateRelation(schema)
		assert.NoError(t, err)

		db1, _ := txn2.GetDatabase("xx")
		_, err = db1.CreateRelation(schema)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
		assert.NoError(t, txn1.Rollback(context.Background()))
		assert.NoError(t, txn2.Rollback(context.Background()))
	}

	txn, _ := tae.StartTxn(nil)
	db, _ := txn.CreateDatabase("db", "", "")
	created, _ := db.CreateRelation(schema)
	tid := created.ID()
	txn.Commit(context.Background())

	// concurrent create and in txn alter check
	txn0, _ := tae.StartTxn(nil)
	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	tbl, _ := db.GetRelationByName("test") // 1002
	assert.NoError(t, tbl.AlterTable(context.TODO(), api.NewRenameTableReq(0, 0, "test", "ultra-test")))
	_, err := db.GetRelationByName("test")
	assert.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))
	tbl, err = db.GetRelationByName("ultra-test")
	assert.NoError(t, err)
	assert.Equal(t, tid, tbl.ID())

	assert.NoError(t, tbl.AlterTable(context.TODO(), api.NewRenameTableReq(0, 0, "ultra-test", "ultraman-test")))
	_, err = db.GetRelationByName("test")
	assert.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))
	_, err = db.GetRelationByName("ultra-test")
	assert.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))
	tbl, err = db.GetRelationByName("ultraman-test")
	assert.NoError(t, err)
	assert.Equal(t, tid, tbl.ID())

	// concurrent txn should see test
	txn1, _ := tae.StartTxn(nil)
	db, err = txn1.GetDatabase("db")
	assert.NoError(t, err)
	tbl, err = db.GetRelationByName("test")
	assert.NoError(t, err)
	assert.Equal(t, tid, tbl.ID())
	_, err = db.GetRelationByName("ultraman-test")
	assert.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))
	assert.NoError(t, txn1.Commit(context.Background()))

	assert.NoError(t, txn.Commit(context.Background()))

	txn2, _ := tae.StartTxn(nil)
	db, err = txn2.GetDatabase("db")
	assert.NoError(t, err)
	_, err = db.GetRelationByName("test")
	assert.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))
	_, err = db.GetRelationByName("ultra-test")
	assert.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))
	tbl, err = db.GetRelationByName("ultraman-test")
	assert.NoError(t, err)
	assert.Equal(t, tid, tbl.ID())

	assert.NoError(t, txn2.Commit(context.Background()))

	// should see test, not newest name
	db, err = txn0.GetDatabase("db")
	assert.NoError(t, err)
	_, err = db.GetRelationByName("ultraman-test")
	assert.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))
	_, err = db.GetRelationByName("ultra-test")
	assert.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))
	tbl, err = db.GetRelationByName("test")
	assert.NoError(t, err)
	assert.Equal(t, tid, tbl.ID())

	txn3, _ := tae.StartTxn(nil)
	db, _ = txn3.GetDatabase("db")
	rel, err := db.CreateRelation(schema)
	assert.NoError(t, err)
	assert.NotEqual(t, rel.ID(), tid)
	assert.NoError(t, txn3.Commit(context.Background()))

	t.Log(1, db.GetMeta().(*catalog.DBEntry).PrettyNameIndex())
	{
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		tbl, _ := db.GetRelationByName("test")
		assert.Error(t, tbl.AlterTable(context.TODO(), api.NewRenameTableReq(0, 0, "unmatch", "yyyy")))
		assert.NoError(t, txn.Rollback(context.Background()))
	}
	// alter back to original schema
	{
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		tbl, _ := db.GetRelationByName("test")
		assert.NoError(t, tbl.AlterTable(context.TODO(), api.NewRenameTableReq(0, 0, "test", "xx")))
		assert.NoError(t, txn.Commit(context.Background()))

		t.Log(2, db.GetMeta().(*catalog.DBEntry).PrettyNameIndex())
		txn, _ = tae.StartTxn(nil)
		db, _ = txn.GetDatabase("db")
		tbl, _ = db.GetRelationByName("xx")
		assert.NoError(t, tbl.AlterTable(context.TODO(), api.NewRenameTableReq(0, 0, "xx", "test")))
		assert.NoError(t, txn.Commit(context.Background()))

		t.Log(3, db.GetMeta().(*catalog.DBEntry).PrettyNameIndex())
	}

	// rename duplicate and rollback
	{
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		schema.Name = "other"
		_, err := db.CreateRelation(schema)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))

		t.Log(4, db.GetMeta().(*catalog.DBEntry).PrettyNameIndex())
		txn, _ = tae.StartTxn(nil)
		db, _ = txn.GetDatabase("db")
		tbl, _ = db.GetRelationByName("test")
		assert.NoError(t, tbl.AlterTable(context.TODO(), api.NewRenameTableReq(0, 0, "test", "toBeRollback1")))
		assert.NoError(t, tbl.AlterTable(context.TODO(), api.NewRenameTableReq(0, 0, "toBeRollback1", "toBeRollback2")))
		assert.Error(t, tbl.AlterTable(context.TODO(), api.NewRenameTableReq(0, 0, "toBeRollback2", "other"))) // duplicate
		assert.NoError(t, txn.Rollback(context.Background()))

		t.Log(5, db.GetMeta().(*catalog.DBEntry).PrettyNameIndex())
	}

	// test checkpoint replay with txn nil
	{
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		tbl, _ := db.GetRelationByName("test")
		assert.NoError(t, tbl.AlterTable(context.TODO(), api.NewRenameTableReq(0, 0, "test", "newtest"))) // make test nodelist has no active node
		assert.NoError(t, txn.Commit(context.Background()))

		txn, _ = tae.StartTxn(nil)
		db, _ = txn.GetDatabase("db")
		tbl, _ = db.GetRelationByName("other")
		assert.NoError(t, tbl.AlterTable(context.TODO(), api.NewRenameTableReq(0, 0, "other", "test"))) // rename other to test, success
		assert.NoError(t, txn.Commit(context.Background()))
	}

	tae.Restart(ctx)

	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	dbentry := db.GetMeta().(*catalog.DBEntry)
	t.Log(dbentry.PrettyNameIndex())
	assert.NoError(t, txn.Commit(context.Background()))

	assert.NoError(t, tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false))
	tae.Restart(ctx)

	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	dbentry = db.GetMeta().(*catalog.DBEntry)
	t.Log(dbentry.PrettyNameIndex())
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestAlterRenameTbl2(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(2, -1)
	schema.Name = "t1"
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	schema.Constraint = []byte("start version")
	schema.Comment = "comment version"

	schema2 := schema.Clone()
	schema2.Name = "t1-copy-fefsfwafe"

	schema3 := schema.Clone()
	schema3.Name = "t1-copy-igmgibjtm"

	var oldId, newId uint64
	{
		var err error
		txn, _ := tae.StartTxn(nil)
		txn.CreateDatabase("xx", "", "")

		db, _ := txn.GetDatabase("xx")

		hdl, err := db.CreateRelation(schema)
		assert.NoError(t, err)
		oldId = hdl.ID()
		assert.NoError(t, txn.Commit(context.Background()))
	}

	{
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("xx")
		hdl, err := db.CreateRelation(schema2)
		assert.NoError(t, err)
		newId = hdl.ID()

		_, err = db.DropRelationByID(oldId)
		assert.NoError(t, err)

		newhdl, _ := db.GetRelationByID(newId)
		assert.NoError(t, newhdl.AlterTable(ctx, api.NewRenameTableReq(0, 0, "t1-copy-fefsfwafe", "t1")))
		assert.NoError(t, txn.Commit(context.Background()))

		dbentry := db.GetMeta().(*catalog.DBEntry)
		t.Log(dbentry.PrettyNameIndex())
	}

	{
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("xx")
		hdl, err := db.CreateRelation(schema3)
		assert.NoError(t, err)
		newId2 := hdl.ID()

		_, err = db.DropRelationByID(newId)
		assert.NoError(t, err)

		newhdl, _ := db.GetRelationByID(newId2)
		assert.NoError(t, newhdl.AlterTable(ctx, api.NewRenameTableReq(0, 0, "t1-copy-igmgibjtm", "t1")))
		assert.NoError(t, txn.Commit(context.Background()))

		dbentry := db.GetMeta().(*catalog.DBEntry)
		t.Log(dbentry.PrettyNameIndex())
		newId = newId2
	}

	tae.Restart(ctx)
	{
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("xx")
		dbentry := db.GetMeta().(*catalog.DBEntry)
		t.Log(dbentry.PrettyNameIndex())
		assert.NoError(t, txn.Commit(context.Background()))
	}

	{
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("xx")

		newhdl, _ := db.GetRelationByID(newId)
		assert.NoError(t, newhdl.AlterTable(ctx, api.NewRenameTableReq(0, 0, "t1", "t2")))
		assert.NoError(t, txn.Commit(context.Background()))

		dbentry := db.GetMeta().(*catalog.DBEntry)
		t.Log(dbentry.PrettyNameIndex())
	}

	assert.NoError(t, tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false))

	tae.Restart(ctx)
	{
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("xx")
		dbentry := db.GetMeta().(*catalog.DBEntry)
		t.Log(dbentry.PrettyNameIndex())
		assert.NoError(t, txn.Commit(context.Background()))
	}

}

func TestAlterFakePk(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(3, -1)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bats := catalog.MockBatch(schema, 12).Split(3)
	tae.CreateRelAndAppend(bats[0], true)

	var did, tid uint64
	var blkFp *common.ID
	{
		// add two cloumns
		txn, rel := tae.GetRelation()
		tid = rel.ID()
		d, _ := rel.GetDB()
		did = d.GetID()
		blkFp = testutil.GetOneObject(rel).Fingerprint()
		tblEntry := rel.GetMeta().(*catalog.TableEntry)
		err := rel.AlterTable(context.TODO(), api.NewAddColumnReq(0, 0, "add1", types.NewProtoType(types.T_int32), 1))
		assert.NoError(t, err)
		err = rel.AlterTable(context.TODO(), api.NewAddColumnReq(0, 0, "add2", types.NewProtoType(types.T_int64), 2))
		assert.NoError(t, err)
		t.Log(tblEntry.StringWithLevel(common.PPL2))
		assert.NoError(t, txn.Commit(context.Background()))
		assert.Equal(t, 2, tblEntry.MVCC.Depth())
	}

	{
		txn, rel := tae.GetRelation()
		obj, err := rel.GetObject(blkFp.ObjectID(), false)
		assert.NoError(t, err)
		err = rel.RangeDelete(obj.Fingerprint(), 1, 1, handle.DT_Normal)
		assert.NoError(t, err)
		err = rel.RangeDelete(obj.Fingerprint(), 3, 3, handle.DT_Normal)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
	}

	{
		txn, rel := tae.GetRelation()
		obj, err := rel.GetObject(blkFp.ObjectID(), false)
		assert.NoError(t, err)
		// check non-exist column foreach
		newSchema := obj.GetRelation().Schema(false)
		sels := &nulls.Nulls{}
		sels.Add(1)
		sels.Add(3)
		rows := make([]int, 0, 4)
		tbl := rel.GetMeta().(*catalog.TableEntry)
		var view *containers.Batch
		blkID := objectio.NewBlockidWithObjectID(obj.GetID(), 0)
		err = tables.HybridScanByBlock(ctx, tbl, txn, &view, newSchema.(*catalog.Schema), []int{1}, blkID, common.DefaultAllocator)
		view.Vecs[0].Foreach(func(v any, isNull bool, row int) error {
			assert.True(t, true)
			rows = append(rows, row)
			return nil
		}, sels)
		assert.Equal(t, []int{1, 3}, rows)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
	}

	t.Log(tae.Catalog.SimplePPString(3))
	resp, close, _ := logtail.HandleSyncLogTailReq(context.TODO(), new(dummyCpkGetter), tae.LogtailMgr, tae.Catalog, api.SyncLogTailReq{
		CnHave: totsp(types.BuildTS(0, 0)),
		CnWant: totsp(types.MaxTs()),
		Table:  &api.TableID{DbId: did, TbId: tid},
	}, true)

	defer close()
	assert.Equal(t, 4, len(resp.Commands)) // data object, tombstone object, date insert, data delete
	for i, cmd := range resp.Commands {
		t.Logf("command %d, table name %v, type %d", i, cmd.TableName, cmd.EntryType)
	}
	assert.Equal(t, api.Entry_Insert, resp.Commands[0].EntryType) // data insert
	assert.Equal(t, api.Entry_Insert, resp.Commands[1].EntryType) // data insert
	assert.Equal(t, api.Entry_Insert, resp.Commands[2].EntryType) // data insert
	assert.Equal(t, api.Entry_Delete, resp.Commands[3].EntryType) // data delete

	dataObjectBat, err := batch.ProtoBatchToBatch(resp.Commands[0].Bat)
	assert.NoError(t, err)
	tnDataObjectBat := containers.NewNonNullBatchWithSharedMemory(dataObjectBat, common.DefaultAllocator)
	t.Log(tnDataObjectBat.Attrs)
	assert.Equal(t, 10, len(tnDataObjectBat.Vecs))
	for _, v := range tnDataObjectBat.Vecs {
		assert.Equal(t, 1, v.Length())
	}

	tombstoneObjectBat, err := batch.ProtoBatchToBatch(resp.Commands[1].Bat)
	assert.NoError(t, err)
	tnTombstoneObjectBat := containers.NewNonNullBatchWithSharedMemory(tombstoneObjectBat, common.DefaultAllocator)
	t.Log(tnTombstoneObjectBat.Attrs)
	assert.Equal(t, 10, len(tnTombstoneObjectBat.Vecs)) // 1 fake pk + 1 rowid + 1 committs
	for _, v := range tnTombstoneObjectBat.Vecs {
		assert.Equal(t, 1, v.Length())
	}

	insBat, err := batch.ProtoBatchToBatch(resp.Commands[2].Bat)
	assert.NoError(t, err)
	tnInsBat := containers.NewNonNullBatchWithSharedMemory(insBat, common.DefaultAllocator)
	t.Log(tnInsBat.Attrs)
	assert.Equal(t, 6, len(tnInsBat.Vecs)) // 3 col + 1 fake pk + 1 rowid + 1 committs
	for _, v := range tnInsBat.Vecs {
		assert.Equal(t, 4, v.Length())
	}
	t.Log(tnInsBat.GetVectorByName(pkgcatalog.FakePrimaryKeyColName).PPString(10))

	delBat, err := batch.ProtoBatchToBatch(resp.Commands[3].Bat)
	assert.NoError(t, err)
	tnDelBat := containers.NewNonNullBatchWithSharedMemory(delBat, common.DefaultAllocator)
	t.Log(tnDelBat.Attrs)
	assert.Equal(t, 4, len(tnDelBat.Vecs)) // 1 fake pk + 1 rowid + 1 committs + tombstone rowID
	for _, v := range tnDelBat.Vecs {
		assert.Equal(t, 2, v.Length())
	}
	t.Log(tnDelBat.GetVectorByName(catalog.AttrPKVal).PPString(10))

}

func TestAlterColumnAndFreeze(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(10, 5)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bats := catalog.MockBatch(schema, 8).Split(2)
	tae.CreateRelAndAppend(bats[0], true)

	{
		// test error in alter
		txn, rel := tae.GetRelation()
		tblEntry := rel.GetMeta().(*catalog.TableEntry)
		err := rel.AlterTable(context.TODO(), api.NewRemoveColumnReq(0, 0, 1, 10))
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
		assert.Equal(t, 2, tblEntry.MVCC.Depth())
		t.Log(tblEntry.StringWithLevel(common.PPL2))
		assert.NoError(t, txn.Rollback(context.Background()))
		// new node is clean
		assert.Equal(t, 1, tblEntry.MVCC.Depth())
	}

	txn0, rel0 := tae.GetRelation()
	db, err := rel0.GetDB()
	assert.NoError(t, err)
	did, tid := db.GetID(), rel0.ID()

	assert.NoError(t, rel0.Append(context.Background(), bats[1])) // in localObject

	txn, rel := tae.GetRelation()
	assert.NoError(t, rel.AlterTable(context.TODO(), api.NewAddColumnReq(0, 0, "xyz", types.NewProtoType(types.T_int32), 0)))
	assert.NoError(t, txn.Commit(context.Background()))

	assert.Error(t, rel0.Append(context.Background(), nil)) // schema changed, error
	// Test variaous read on old schema
	testutil.CheckAllColRowsByScan(t, rel0, 8, false)

	filter := handle.NewEQFilter(uint16(3))
	id, row, err := rel0.GetByFilter(context.Background(), filter)
	filen, blkn := id.BlockID.Offsets() // first block
	assert.Equal(t, uint16(0), filen)
	assert.Equal(t, uint16(0), blkn)
	assert.Equal(t, uint32(3), row)
	assert.NoError(t, err)

	for _, col := range rel0.Schema(false).(*catalog.Schema).ColDefs {
		val, null, err := rel0.GetValue(id, 2, uint16(col.Idx), false)
		assert.NoError(t, err)
		assert.False(t, null)
		if col.IsPrimary() {
			assert.Equal(t, uint16(2), val.(uint16))
		}
	}
	assert.Error(t, txn0.Commit(context.Background())) // scheam change, commit failed

	// GetValueByFilter() is combination of GetByFilter and GetValue
	// GetValueByPhyAddrKey is GetValue

	tae.Restart(ctx)

	txn, rel = tae.GetRelation()
	schema1 := rel.Schema(false).(*catalog.Schema)
	bats = catalog.MockBatch(schema1, 16).Split(4)
	assert.Error(t, rel.Append(context.Background(), bats[0])) // dup error
	assert.NoError(t, rel.Append(context.Background(), bats[1]))
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, 8, false)
	it := rel.MakeObjectIt(false)
	cnt := 0
	var id2 *common.ID
	for it.Next() {
		cnt++
		id2 = it.GetObject().Fingerprint()
	}
	it.Close()
	assert.Equal(t, 2, cnt) // 2 blocks because the first is freezed

	for _, col := range rel.Schema(false).(*catalog.Schema).ColDefs {
		val, null, err := rel.GetValue(id, 3, uint16(col.Idx), false) // get first blk
		assert.NoError(t, err)
		if col.Name == "xyz" {
			assert.True(t, null) // fill null for the new column
		} else {
			assert.False(t, null)
		}
		if col.IsPrimary() {
			assert.Equal(t, uint16(3), val.(uint16))
		}

		val, null, err = rel.GetValue(id2, 3, uint16(col.Idx), false) // get second blk
		assert.NoError(t, err)
		assert.False(t, null)
		if col.IsPrimary() {
			assert.Equal(t, uint16(7), val.(uint16))
		}
	}
	txn.Commit(context.Background())

	// append to the second block
	txn, rel = tae.GetRelation()
	assert.NoError(t, rel.Append(context.Background(), bats[2]))
	assert.NoError(t, rel.Append(context.Background(), bats[3])) // new block and append 2 rows
	assert.NoError(t, txn.Commit(context.Background()))

	// remove and freeze
	txn, rel = tae.GetRelation()
	assert.NoError(t, rel.AlterTable(context.TODO(), api.NewRemoveColumnReq(0, 0, 9, 8))) // remove float mock_8
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	schema2 := rel.Schema(false).(*catalog.Schema)
	bats = catalog.MockBatch(schema2, 20).Split(5)
	assert.NoError(t, rel.Append(context.Background(), bats[4])) // new 4th block and append 4 blocks

	testutil.CheckAllColRowsByScan(t, rel, 20, true)
	assert.NoError(t, txn.Commit(context.Background()))

	t.Log(tae.Catalog.SimplePPString(3))

	resp, close, _ := logtail.HandleSyncLogTailReq(context.TODO(), new(dummyCpkGetter), tae.LogtailMgr, tae.Catalog, api.SyncLogTailReq{
		CnHave: totsp(types.BuildTS(0, 0)),
		CnWant: totsp(types.MaxTs()),
		Table:  &api.TableID{DbId: did, TbId: tid},
	}, true)

	assert.Equal(t, 4, len(resp.Commands)) // data object + 3 version insert
	bat0 := resp.Commands[1].Bat
	assert.Equal(t, 12, len(bat0.Attrs))
	assert.Equal(t, "mock_9", bat0.Attrs[2+schema.GetSeqnum("mock_9")])
	bat1 := resp.Commands[2].Bat
	assert.Equal(t, 13, len(bat1.Attrs))
	assert.Equal(t, "mock_9", bat1.Attrs[2+schema1.GetSeqnum("mock_9")])
	assert.Equal(t, "xyz", bat1.Attrs[2+schema1.GetSeqnum("xyz")])
	bat2 := resp.Commands[3].Bat
	assert.Equal(t, 13, len(bat2.Attrs))
	assert.Equal(t, "mock_9", bat2.Attrs[2+schema1.GetSeqnum("mock_9")])
	assert.Equal(t, "mock_9", bat2.Attrs[2+schema2.GetSeqnum("mock_9")])
	assert.Equal(t, "xyz", bat2.Attrs[2+schema1.GetSeqnum("xyz")])
	assert.Equal(t, "xyz", bat2.Attrs[2+schema2.GetSeqnum("xyz")])
	close()
	logutil.Info(tae.Catalog.SimplePPString(common.PPL1))
}

func TestGlobalCheckpoint1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	options.WithCheckpointGlobalMinCount(1)(opts)
	options.WithGlobalVersionInterval(time.Millisecond * 10)(opts)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(10, 2)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 400)

	tae.CreateRelAndAppend(bat, true)

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	tae.Restart(ctx)
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	tae.CheckRowsByScan(400, true)

	testutils.WaitExpect(4000, func() bool {
		return tae.Wal.GetPenddingCnt() == 0
	})

	tae.Restart(ctx)
	tae.CheckRowsByScan(400, true)
}

func TestAppendAndGC(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := new(options.Options)
	opts = config.WithQuickScanAndCKPOpts(opts)
	options.WithDisableGCCheckpoint()(opts)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	db := tae.DB
	db.DiskCleaner.GetCleaner().SetMinMergeCountForTest(2)

	schema1 := catalog.MockSchemaAll(13, 2)
	schema1.BlockMaxRows = 10
	schema1.ObjectMaxBlocks = 2

	schema2 := catalog.MockSchemaAll(13, 2)
	schema2.BlockMaxRows = 10
	schema2.ObjectMaxBlocks = 2
	{
		txn, _ := db.StartTxn(nil)
		database, err := txn.CreateDatabase("db", "", "")
		assert.Nil(t, err)
		_, err = database.CreateRelation(schema1)
		assert.Nil(t, err)
		_, err = database.CreateRelation(schema2)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	bat := catalog.MockBatch(schema1, int(schema1.BlockMaxRows*10-1))
	defer bat.Close()
	bats := bat.Split(bat.Length())

	pool, err := ants.NewPool(20)
	assert.Nil(t, err)
	defer pool.Release()
	var wg sync.WaitGroup

	for _, data := range bats {
		wg.Add(2)
		err = pool.Submit(testutil.AppendClosure(t, data, schema1.Name, db, &wg))
		assert.Nil(t, err)
		err = pool.Submit(testutil.AppendClosure(t, data, schema2.Name, db, &wg))
		assert.Nil(t, err)
	}
	wg.Wait()
	testutils.WaitExpect(10000, func() bool {
		return db.Runtime.Scheduler.GetPenddingLSNCnt() == 0
	})
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	if db.Runtime.Scheduler.GetPenddingLSNCnt() != 0 {
		return
	}
	assert.Equal(t, uint64(0), db.Runtime.Scheduler.GetPenddingLSNCnt())
	err = db.DiskCleaner.GetCleaner().CheckGC()
	assert.Nil(t, err)
	testutils.WaitExpect(5000, func() bool {
		return db.DiskCleaner.GetCleaner().GetMinMerged() != nil
	})
	testutils.WaitExpect(10000, func() bool {
		return db.DiskCleaner.GetCleaner().GetMinMerged() != nil
	})
	minMerged := db.DiskCleaner.GetCleaner().GetMinMerged()
	if minMerged == nil {
		return
	}
	assert.NotNil(t, minMerged)
	tae.Restart(ctx)
	db = tae.DB
	db.DiskCleaner.GetCleaner().SetMinMergeCountForTest(2)
	testutils.WaitExpect(5000, func() bool {
		if db.DiskCleaner.GetCleaner().GetMaxConsumed() == nil {
			return false
		}
		end := db.DiskCleaner.GetCleaner().GetMaxConsumed().GetEnd()
		minEnd := minMerged.GetEnd()
		return end.GreaterEq(&minEnd)
	})
	end := db.DiskCleaner.GetCleaner().GetMaxConsumed().GetEnd()
	minEnd := minMerged.GetEnd()
	assert.True(t, end.GreaterEq(&minEnd))
	err = db.DiskCleaner.GetCleaner().CheckGC()
	assert.Nil(t, err)

}

func TestSnapshotGC(t *testing.T) {
	t.Skip("FIXME: jiangwei")
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := new(options.Options)
	opts = config.WithQuickScanAndCKPOpts(opts)
	options.WithDisableGCCheckpoint()(opts)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	db := tae.DB
	db.DiskCleaner.GetCleaner().SetMinMergeCountForTest(1)

	snapshotSchema := catalog.MockSnapShotSchema()
	snapshotSchema.BlockMaxRows = 2
	snapshotSchema.ObjectMaxBlocks = 1
	schema1 := catalog.MockSchemaAll(13, 2)
	schema1.BlockMaxRows = 10
	schema1.ObjectMaxBlocks = 2

	schema2 := catalog.MockSchemaAll(13, 2)
	schema2.BlockMaxRows = 10
	schema2.ObjectMaxBlocks = 2
	var rel3 handle.Relation
	{
		txn, _ := db.StartTxn(nil)
		database, err := txn.CreateDatabase("db", "", "")
		assert.Nil(t, err)
		_, err = database.CreateRelation(schema1)
		assert.Nil(t, err)
		_, err = database.CreateRelation(schema2)
		assert.Nil(t, err)
		rel3, err = database.CreateRelation(snapshotSchema)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	db.DiskCleaner.GetCleaner().SetTid(rel3.ID())
	db.DiskCleaner.GetCleaner().DisableGCForTest()
	bat := catalog.MockBatch(schema1, int(schema1.BlockMaxRows*10-1))
	defer bat.Close()
	bats := bat.Split(bat.Length())

	pool, err := ants.NewPool(20)
	assert.Nil(t, err)
	defer pool.Release()
	snapshots := make([]int64, 0)
	var wg sync.WaitGroup
	var snapWG sync.WaitGroup
	snapWG.Add(1)
	go func() {
		i := 0
		for {
			if i > 3 {
				snapWG.Done()
				break
			}
			i++
			time.Sleep(200 * time.Millisecond)
			snapshot := time.Now().UTC().UnixNano()
			snapshots = append(snapshots, snapshot)
			attrs := []string{"col0", "col1", "ts", "col3", "col4", "col5", "col6", "id"}
			vecTypes := []types.Type{types.T_uint64.ToType(),
				types.T_uint64.ToType(), types.T_int64.ToType(),
				types.T_enum.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(),
				types.T_uint64.ToType(), types.T_uint64.ToType()}
			opt := containers.Options{}
			opt.Capacity = 0
			data1 := containers.BuildBatch(attrs, vecTypes, opt)
			data1.Vecs[0].Append(uint64(0), false)
			data1.Vecs[1].Append(uint64(0), false)
			data1.Vecs[2].Append(snapshot, false)
			data1.Vecs[3].Append(types.Enum(1), false)
			data1.Vecs[4].Append(uint64(0), false)
			data1.Vecs[5].Append(uint64(0), false)
			data1.Vecs[6].Append(uint64(0), false)
			data1.Vecs[7].Append(uint64(0), false)
			txn1, _ := db.StartTxn(nil)
			database, _ := txn1.GetDatabase("db")
			rel, _ := database.GetRelationByName(snapshotSchema.Name)
			err = rel.Append(context.Background(), data1)
			data1.Close()
			assert.Nil(t, err)
			assert.Nil(t, txn1.Commit(context.Background()))
		}
	}()
	for _, data := range bats {
		wg.Add(2)
		err := pool.Submit(testutil.AppendClosure(t, data, schema1.Name, db, &wg))
		assert.Nil(t, err)

		err = pool.Submit(testutil.AppendClosure(t, data, schema2.Name, db, &wg))
		assert.Nil(t, err)
	}
	snapWG.Wait()
	wg.Wait()
	testutils.WaitExpect(10000, func() bool {
		return db.Runtime.Scheduler.GetPenddingLSNCnt() == 0
	})
	if db.Runtime.Scheduler.GetPenddingLSNCnt() != 0 {
		return
	}
	db.DiskCleaner.GetCleaner().EnableGCForTest()
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	assert.Equal(t, uint64(0), db.Runtime.Scheduler.GetPenddingLSNCnt())
	testutils.WaitExpect(5000, func() bool {
		return db.DiskCleaner.GetCleaner().GetMinMerged() != nil
	})
	minMerged := db.DiskCleaner.GetCleaner().GetMinMerged()
	testutils.WaitExpect(5000, func() bool {
		return db.DiskCleaner.GetCleaner().GetMinMerged() != nil
	})
	if db.DiskCleaner.GetCleaner().GetMinMerged() == nil {
		return
	}
	assert.NotNil(t, minMerged)
	err = db.DiskCleaner.GetCleaner().CheckGC()
	assert.Nil(t, err)
	tae.RestartDisableGC(ctx)
	db = tae.DB
	db.DiskCleaner.GetCleaner().SetMinMergeCountForTest(1)
	testutils.WaitExpect(5000, func() bool {
		if db.DiskCleaner.GetCleaner().GetMaxConsumed() == nil {
			return false
		}
		end := db.DiskCleaner.GetCleaner().GetMaxConsumed().GetEnd()
		minEnd := minMerged.GetEnd()
		return end.GreaterEq(&minEnd)
	})
	end := db.DiskCleaner.GetCleaner().GetMaxConsumed().GetEnd()
	minEnd := minMerged.GetEnd()
	assert.True(t, end.GreaterEq(&minEnd))
	err = db.DiskCleaner.GetCleaner().CheckGC()
	assert.Nil(t, err)

}

func TestSnapshotMeta(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := new(options.Options)
	opts = config.WithQuickScanAndCKPOpts(opts)
	options.WithDisableGCCheckpoint()(opts)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	db := tae.DB
	db.DiskCleaner.GetCleaner().SetMinMergeCountForTest(1)

	snapshotSchema := catalog.MockSnapShotSchema()
	snapshotSchema.BlockMaxRows = 2
	snapshotSchema.ObjectMaxBlocks = 1
	snapshotSchema1 := catalog.MockSnapShotSchema()
	snapshotSchema1.BlockMaxRows = 2
	snapshotSchema1.ObjectMaxBlocks = 1
	snapshotSchema2 := catalog.MockSnapShotSchema()
	snapshotSchema2.BlockMaxRows = 2
	snapshotSchema2.ObjectMaxBlocks = 1
	var rel3, rel4, rel5 handle.Relation
	{
		txn, _ := db.StartTxn(nil)
		database, err := txn.CreateDatabase("db", "", "")
		assert.Nil(t, err)
		database2, err := txn.CreateDatabase("db2", "", "")
		assert.Nil(t, err)
		database3, err := txn.CreateDatabase("db3", "", "")
		assert.Nil(t, err)
		rel3, err = database.CreateRelation(snapshotSchema)
		assert.Nil(t, err)
		rel4, err = database2.CreateRelation(snapshotSchema1)
		assert.Nil(t, err)
		rel5, err = database3.CreateRelation(snapshotSchema2)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
	//db.DiskCleaner.GetCleaner().DisableGCForTest()

	snapshots := make([]int64, 0)
	for i := 0; i < 10; i++ {
		time.Sleep(20 * time.Millisecond)
		snapshot := time.Now().UTC().Unix()
		snapshots = append(snapshots, snapshot)
	}
	testutils.WaitExpect(10000, func() bool {
		return db.Runtime.Scheduler.GetPenddingLSNCnt() == 0
	})
	if db.Runtime.Scheduler.GetPenddingLSNCnt() != 0 {
		return
	}
	tae.Restart(ctx)
	db = tae.DB
	db.DiskCleaner.GetCleaner().DisableGCForTest()
	db.DiskCleaner.GetCleaner().SetMinMergeCountForTest(1)
	for i, snapshot := range snapshots {
		attrs := []string{"col0", "col1", "ts", "col3", "col4", "col5", "col6", "id"}
		vecTypes := []types.Type{types.T_uint64.ToType(),
			types.T_uint64.ToType(), types.T_int64.ToType(),
			types.T_enum.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(),
			types.T_uint64.ToType(), types.T_uint64.ToType()}
		opt := containers.Options{}
		opt.Capacity = 0
		data1 := containers.BuildBatch(attrs, vecTypes, opt)
		data1.Vecs[0].Append(uint64(0), false)
		data1.Vecs[1].Append(uint64(0), false)
		data1.Vecs[2].Append(snapshot, false)
		data1.Vecs[3].Append(types.Enum(1), false)
		data1.Vecs[4].Append(uint64(0), false)
		data1.Vecs[5].Append(uint64(0), false)
		data1.Vecs[6].Append(uint64(0), false)
		data1.Vecs[7].Append(uint64(0), false)
		txn1, _ := db.StartTxn(nil)
		var database handle.Database
		var id uint64
		if i%3 == 0 {
			id = rel3.ID()
			database, _ = txn1.GetDatabase("db")
		} else if i%3 == 1 {
			id = rel4.ID()
			database, _ = txn1.GetDatabase("db2")
		} else {
			id = rel5.ID()
			database, _ = txn1.GetDatabase("db3")
		}
		rel, _ := database.GetRelationByID(id)
		err := rel.Append(context.Background(), data1)
		data1.Close()
		assert.Nil(t, err)
		assert.Nil(t, txn1.Commit(context.Background()))
	}
	testutils.WaitExpect(10000, func() bool {
		return db.Runtime.Scheduler.GetPenddingLSNCnt() == 0
	})
	if db.Runtime.Scheduler.GetPenddingLSNCnt() != 0 {
		return
	}
	initMinMerged := db.DiskCleaner.GetCleaner().GetMinMerged()
	db.DiskCleaner.GetCleaner().EnableGCForTest()
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	assert.Equal(t, uint64(0), db.Runtime.Scheduler.GetPenddingLSNCnt())
	testutils.WaitExpect(3000, func() bool {
		if db.DiskCleaner.GetCleaner().GetMinMerged() == nil {
			return false
		}
		minEnd := db.DiskCleaner.GetCleaner().GetMinMerged().GetEnd()
		if minEnd.IsEmpty() {
			return false
		}
		if initMinMerged == nil {
			return true
		}
		initMinEnd := initMinMerged.GetEnd()
		return minEnd.Greater(&initMinEnd)
	})
	minMerged := db.DiskCleaner.GetCleaner().GetMinMerged()
	if minMerged == nil {
		return
	}
	minEnd := minMerged.GetEnd()
	if minEnd.IsEmpty() {
		return
	}
	if initMinMerged != nil {
		initMinEnd := initMinMerged.GetEnd()
		if !minEnd.Greater(&initMinEnd) {
			return
		}
	}

	assert.NotNil(t, minMerged)
	snaps, err := db.DiskCleaner.GetCleaner().GetSnapshots()
	assert.Nil(t, err)
	defer logtail.CloseSnapshotList(snaps)
	assert.Equal(t, 1, len(snaps))
	for _, snap := range snaps {
		assert.Equal(t, len(snapshots), snap.Length())
	}
	err = db.DiskCleaner.GetCleaner().CheckGC()
	assert.Nil(t, err)
	tae.RestartDisableGC(ctx)
	db = tae.DB
	db.DiskCleaner.GetCleaner().SetMinMergeCountForTest(1)
	testutils.WaitExpect(10000, func() bool {
		if db.DiskCleaner.GetCleaner().GetMaxConsumed() == nil {
			return false
		}
		end := db.DiskCleaner.GetCleaner().GetMaxConsumed().GetEnd()
		if db.DiskCleaner.GetCleaner().GetMinMerged() == nil {
			return false
		}
		minEnd := db.DiskCleaner.GetCleaner().GetMinMerged().GetEnd()
		return end.GreaterEq(&minEnd)
	})
	end := db.DiskCleaner.GetCleaner().GetMaxConsumed().GetEnd()
	minEnd = db.DiskCleaner.GetCleaner().GetMinMerged().GetEnd()
	assert.True(t, end.GreaterEq(&minEnd))
	snaps, err = db.DiskCleaner.GetCleaner().GetSnapshots()
	assert.Nil(t, err)
	defer logtail.CloseSnapshotList(snaps)
	assert.Equal(t, 1, len(snaps))
	for _, snap := range snaps {
		assert.Equal(t, len(snapshots), snap.Length())
	}
	err = db.DiskCleaner.GetCleaner().CheckGC()
	assert.Nil(t, err)
}

func TestGlobalCheckpoint2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	options.WithCheckpointGlobalMinCount(1)(opts)
	options.WithDisableGCCatalog()(opts)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	tae.BGCheckpointRunner.DisableCheckpoint()
	tae.BGCheckpointRunner.CleanPenddingCheckpoint()
	defer tae.Close()
	schema := catalog.MockSchemaAll(10, 2)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 40)

	_, firstRel := tae.CreateRelAndAppend(bat, true)

	tae.DropRelation(t)
	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	tae.IncrementalCheckpoint(txn.GetStartTS(), false, true, true)
	tae.GlobalCheckpoint(txn.GetStartTS(), 0, false)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.CreateRelAndAppend(bat, false)

	txn, rel := tae.GetRelation()
	assert.NoError(t, rel.AlterTable(context.Background(), api.NewRemoveColumnReq(0, 0, 3, 3)))
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	newschema := rel.Schema(false).(*catalog.Schema)
	assert.Equal(t, uint32(1), newschema.Version)
	assert.Equal(t, uint32(10), newschema.Extra.NextColSeqnum)
	assert.Equal(t, "mock_3", newschema.Extra.DroppedAttrs[0])
	assert.NoError(t, txn.Commit(context.Background()))

	currTs := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	assert.NoError(t, err)
	tae.IncrementalCheckpoint(currTs, false, true, true)
	tae.GlobalCheckpoint(currTs, time.Duration(1), false)

	p := &catalog.LoopProcessor{}
	tableExisted := false
	p.TableFn = func(te *catalog.TableEntry) error {
		if te.ID == firstRel.ID() {
			tableExisted = true
		}
		return nil
	}

	assert.NoError(t, tae.Catalog.RecurLoop(p))
	assert.True(t, tableExisted)

	t.Log(tae.Catalog.SimplePPString(3))
	tae.Restart(ctx)
	t.Log(tae.Catalog.SimplePPString(3))

	tableExisted = false
	assert.NoError(t, tae.Catalog.RecurLoop(p))
	assert.False(t, tableExisted)
	txn, rel = tae.GetRelation()
	newschema = rel.Schema(false).(*catalog.Schema)
	assert.Equal(t, uint32(1), newschema.Version)
	assert.Equal(t, uint32(10), newschema.Extra.NextColSeqnum)
	assert.Equal(t, "mock_3", newschema.Extra.DroppedAttrs[0])
	assert.NoError(t, txn.Commit(context.Background()))

}

func TestGlobalCheckpoint3(t *testing.T) {
	t.Skip("This case crashes occasionally, is being fixed, skip it for now")
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	options.WithCheckpointGlobalMinCount(1)(opts)
	options.WithGlobalVersionInterval(time.Nanosecond * 1)(opts)
	options.WithDisableGCCatalog()(opts)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(10, 2)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 40)

	_, rel := tae.CreateRelAndAppend(bat, true)
	testutils.WaitExpect(1000, func() bool {
		return tae.Wal.GetPenddingCnt() == 0
	})

	tae.DropRelation(t)
	testutils.WaitExpect(1000, func() bool {
		return tae.Wal.GetPenddingCnt() == 0
	})

	tae.CreateRelAndAppend(bat, false)
	testutils.WaitExpect(1000, func() bool {
		return tae.Wal.GetPenddingCnt() == 0
	})

	p := &catalog.LoopProcessor{}
	tableExisted := false
	p.TableFn = func(te *catalog.TableEntry) error {
		if te.ID == rel.ID() {
			tableExisted = true
		}
		return nil
	}

	assert.NoError(t, tae.Catalog.RecurLoop(p))
	assert.True(t, tableExisted)

	tae.Restart(ctx)

	tableExisted = false
	assert.NoError(t, tae.Catalog.RecurLoop(p))
	assert.False(t, tableExisted)
}

func TestGlobalCheckpoint4(t *testing.T) {
	t.Skip("This case crashes occasionally, is being fixed, skip it for now")
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	tae.BGCheckpointRunner.DisableCheckpoint()
	tae.BGCheckpointRunner.CleanPenddingCheckpoint()
	globalCkpInterval := time.Second

	schema := catalog.MockSchemaAll(18, 2)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 40)

	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	_, err = txn.CreateDatabase("db", "", "")
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	err = tae.IncrementalCheckpoint(txn.GetCommitTS(), false, true, true)
	assert.NoError(t, err)

	txn, err = tae.StartTxn(nil)
	assert.NoError(t, err)
	_, err = txn.DropDatabase("db")
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	err = tae.GlobalCheckpoint(txn.GetCommitTS(), globalCkpInterval, false)
	assert.NoError(t, err)

	tae.CreateRelAndAppend(bat, true)

	t.Log(tae.Catalog.SimplePPString(3))
	tae.Restart(ctx)
	tae.BGCheckpointRunner.DisableCheckpoint()
	tae.BGCheckpointRunner.CleanPenddingCheckpoint()
	t.Log(tae.Catalog.SimplePPString(3))

	// tae.CreateRelAndAppend(bat, false)

	txn, err = tae.StartTxn(nil)
	assert.NoError(t, err)
	db, err := txn.GetDatabase("db")
	assert.NoError(t, err)
	_, err = db.DropRelationByName(schema.Name)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	err = tae.GlobalCheckpoint(txn.GetCommitTS(), globalCkpInterval, false)
	assert.NoError(t, err)

	tae.CreateRelAndAppend(bat, false)

	t.Log(tae.Catalog.SimplePPString(3))
	tae.Restart(ctx)
	tae.BGCheckpointRunner.DisableCheckpoint()
	tae.BGCheckpointRunner.CleanPenddingCheckpoint()
	t.Log(tae.Catalog.SimplePPString(3))
}

func TestGlobalCheckpoint5(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	tae.BGCheckpointRunner.DisableCheckpoint()
	tae.BGCheckpointRunner.CleanPenddingCheckpoint()
	globalCkpInterval := time.Duration(0)

	schema := catalog.MockSchemaAll(18, 2)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 60)
	bats := bat.Split(3)

	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	err = tae.IncrementalCheckpoint(txn.GetStartTS(), false, true, true)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.CreateRelAndAppend(bats[0], true)

	txn, err = tae.StartTxn(nil)
	assert.NoError(t, err)
	err = tae.GlobalCheckpoint(txn.GetStartTS(), globalCkpInterval, false)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.DoAppend(bats[1])

	txn, err = tae.StartTxn(nil)
	assert.NoError(t, err)
	err = tae.GlobalCheckpoint(txn.GetStartTS(), globalCkpInterval, false)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.CheckRowsByScan(40, true)

	t.Log(tae.Catalog.SimplePPString(3))
	tae.Restart(ctx)
	tae.BGCheckpointRunner.DisableCheckpoint()
	tae.BGCheckpointRunner.CleanPenddingCheckpoint()
	t.Log(tae.Catalog.SimplePPString(3))

	tae.CheckRowsByScan(40, true)

	tae.DoAppend(bats[2])

	tae.CheckRowsByScan(60, true)
	txn, err = tae.StartTxn(nil)
	assert.NoError(t, err)
	err = tae.GlobalCheckpoint(txn.GetStartTS(), globalCkpInterval, false)
	assert.NoError(t, err)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestGlobalCheckpoint6(t *testing.T) {
	t.Skip("This case crashes occasionally, is being fixed, skip it for now")
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	tae.BGCheckpointRunner.DisableCheckpoint()
	tae.BGCheckpointRunner.CleanPenddingCheckpoint()
	globalCkpInterval := time.Duration(0)
	restartCnt := 10
	batchsize := 10

	schema := catalog.MockSchemaAll(18, 2)
	schema.BlockMaxRows = 5
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, batchsize*(restartCnt+1))
	bats := bat.Split(restartCnt + 1)

	tae.CreateRelAndAppend(bats[0], true)
	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	err = tae.IncrementalCheckpoint(txn.GetStartTS(), false, true, true)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	for i := 0; i < restartCnt; i++ {
		tae.DoAppend(bats[i+1])
		txn, err = tae.StartTxn(nil)
		assert.NoError(t, err)
		err = tae.GlobalCheckpoint(txn.GetStartTS(), globalCkpInterval, false)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))

		rows := (i + 2) * batchsize
		tae.CheckRowsByScan(rows, true)
		t.Log(tae.Catalog.SimplePPString(3))
		tae.Restart(ctx)
		tae.BGCheckpointRunner.DisableCheckpoint()
		tae.BGCheckpointRunner.CleanPenddingCheckpoint()
		t.Log(tae.Catalog.SimplePPString(3))
		tae.CheckRowsByScan(rows, true)
	}
}

func TestGCCheckpoint1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(18, 2)
	schema.BlockMaxRows = 5
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 50)

	tae.CreateRelAndAppend(bat, true)

	testutils.WaitExpect(4000, func() bool {
		return tae.Wal.GetPenddingCnt() == 0
	})
	assert.Equal(t, uint64(0), tae.Wal.GetPenddingCnt())

	testutils.WaitExpect(4000, func() bool {
		return tae.BGCheckpointRunner.GetPenddingIncrementalCount() == 0
	})
	assert.Equal(t, 0, tae.BGCheckpointRunner.GetPenddingIncrementalCount())

	testutils.WaitExpect(4000, func() bool {
		return tae.BGCheckpointRunner.MaxGlobalCheckpoint().IsFinished()
	})
	assert.True(t, tae.BGCheckpointRunner.MaxGlobalCheckpoint().IsFinished())

	tae.BGCheckpointRunner.DisableCheckpoint()

	gcTS := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	t.Log(gcTS.ToString())
	tae.BGCheckpointRunner.GCByTS(context.Background(), gcTS)

	maxGlobal := tae.BGCheckpointRunner.MaxGlobalCheckpoint()

	testutils.WaitExpect(4000, func() bool {
		tae.BGCheckpointRunner.ExistPendingEntryToGC()
		return !tae.BGCheckpointRunner.ExistPendingEntryToGC()
	})
	assert.False(t, tae.BGCheckpointRunner.ExistPendingEntryToGC())

	globals := tae.BGCheckpointRunner.GetAllGlobalCheckpoints()
	assert.Equal(t, 1, len(globals))
	end := maxGlobal.GetEnd()
	maxEnd := globals[0].GetEnd()
	assert.True(t, end.Equal(&maxEnd))
	for _, global := range globals {
		t.Log(global.String())
	}

	incrementals := tae.BGCheckpointRunner.GetAllIncrementalCheckpoints()
	prevEnd := maxGlobal.GetEnd().Prev()
	for _, incremental := range incrementals {
		startTS := incremental.GetStart()
		prevEndNextTS := prevEnd.Next()
		assert.True(t, startTS.Equal(&prevEndNextTS))
		t.Log(incremental.String())
	}
}

func TestGCCatalog1(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	txn1, _ := tae.StartTxn(nil)
	db, err := txn1.CreateDatabase("db1", "", "")
	assert.Nil(t, err)
	db2, err := txn1.CreateDatabase("db2", "", "")
	assert.Nil(t, err)

	schema := catalog.MockSchema(1, 0)
	schema.Name = "tb1"
	tb, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	schema2 := catalog.MockSchema(1, 0)
	schema2.Name = "tb2"
	tb2, err := db.CreateRelation(schema2)
	assert.Nil(t, err)
	schema3 := catalog.MockSchema(1, 0)
	schema3.Name = "tb3"
	tb3, err := db2.CreateRelation(schema3)
	assert.Nil(t, err)

	_, err = tb.CreateObject(false)
	assert.Nil(t, err)
	_, err = tb2.CreateObject(false)
	assert.Nil(t, err)
	obj3, err := tb2.CreateObject(false)
	assert.Nil(t, err)
	obj4, err := tb3.CreateObject(false)
	assert.Nil(t, err)

	err = txn1.Commit(context.Background())
	assert.Nil(t, err)

	p := &catalog.LoopProcessor{}
	var dbCnt, tableCnt, objCnt int
	p.DatabaseFn = func(d *catalog.DBEntry) error {
		if d.IsSystemDB() {
			return nil
		}
		dbCnt++
		return nil
	}
	p.TableFn = func(te *catalog.TableEntry) error {
		if te.GetDB().IsSystemDB() {
			return nil
		}
		tableCnt++
		return nil
	}
	p.ObjectFn = func(se *catalog.ObjectEntry) error {
		if se.GetTable().GetDB().IsSystemDB() {
			return nil
		}
		objCnt++
		return nil
	}
	resetCount := func() {
		dbCnt = 0
		tableCnt = 0
		objCnt = 0
	}

	err = tae.Catalog.RecurLoop(p)
	assert.NoError(t, err)
	assert.Equal(t, 2, dbCnt)
	assert.Equal(t, 3, tableCnt)
	assert.Equal(t, 4, objCnt)

	txn2, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	db2, err = txn2.GetDatabase("db2")
	assert.NoError(t, err)
	tb3, err = db2.GetRelationByName("tb3")
	assert.NoError(t, err)
	obj4, err = tb3.GetObject(obj4.GetID(), false)
	assert.NoError(t, err)
	err = txn2.Commit(context.Background())
	assert.NoError(t, err)

	t.Log(tae.Catalog.SimplePPString(3))
	commitTS := txn2.GetCommitTS()
	tae.Catalog.GCByTS(context.Background(), commitTS.Next())
	t.Log(tae.Catalog.SimplePPString(3))

	resetCount()
	err = tae.Catalog.RecurLoop(p)
	assert.NoError(t, err)
	assert.Equal(t, 2, dbCnt)
	assert.Equal(t, 3, tableCnt)
	assert.Equal(t, 4, objCnt)

	txn3, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	db2, err = txn3.GetDatabase("db2")
	assert.NoError(t, err)
	tb3, err = db2.GetRelationByName("tb3")
	assert.NoError(t, err)
	obj4, err = tb3.GetObject(obj4.GetID(), false)
	assert.NoError(t, err)
	err = tb3.SoftDeleteObject(obj4.GetID(), false)
	testutil.MockObjectStats(t, obj4)
	assert.NoError(t, err)

	db2, err = txn3.GetDatabase("db1")
	assert.NoError(t, err)
	tb3, err = db2.GetRelationByName("tb2")
	assert.NoError(t, err)
	obj3, err = tb3.GetObject(obj3.GetID(), false)
	assert.NoError(t, err)
	err = tb3.SoftDeleteObject(obj3.GetID(), false)
	testutil.MockObjectStats(t, obj3)
	assert.NoError(t, err)

	err = txn3.Commit(context.Background())
	assert.NoError(t, err)

	t.Log(tae.Catalog.SimplePPString(3))
	commitTS = txn3.GetCommitTS()
	tae.Catalog.GCByTS(context.Background(), commitTS.Next())
	t.Log(tae.Catalog.SimplePPString(3))

	resetCount()
	err = tae.Catalog.RecurLoop(p)
	assert.NoError(t, err)
	assert.Equal(t, 2, dbCnt)
	assert.Equal(t, 3, tableCnt)
	assert.Equal(t, 2, objCnt)

	txn4, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	db2, err = txn4.GetDatabase("db2")
	assert.NoError(t, err)
	_, err = db2.DropRelationByName("tb3")
	assert.NoError(t, err)

	db2, err = txn4.GetDatabase("db1")
	assert.NoError(t, err)
	_, err = db2.DropRelationByName("tb2")
	assert.NoError(t, err)

	err = txn4.Commit(context.Background())
	assert.NoError(t, err)

	t.Log(tae.Catalog.SimplePPString(3))
	commitTS = txn4.GetCommitTS()
	tae.Catalog.GCByTS(context.Background(), commitTS.Next())
	t.Log(tae.Catalog.SimplePPString(3))

	resetCount()
	err = tae.Catalog.RecurLoop(p)
	assert.NoError(t, err)
	assert.Equal(t, 2, dbCnt)
	assert.Equal(t, 1, tableCnt)
	assert.Equal(t, 1, objCnt)

	txn5, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	_, err = txn5.DropDatabase("db2")
	assert.NoError(t, err)

	_, err = txn5.DropDatabase("db1")
	assert.NoError(t, err)

	err = txn5.Commit(context.Background())
	assert.NoError(t, err)

	t.Log(tae.Catalog.SimplePPString(3))
	commitTS = txn5.GetCommitTS()
	tae.Catalog.GCByTS(context.Background(), commitTS.Next())
	t.Log(tae.Catalog.SimplePPString(3))

	resetCount()
	err = tae.Catalog.RecurLoop(p)
	assert.NoError(t, err)
	assert.Equal(t, 0, dbCnt)
	assert.Equal(t, 0, tableCnt)
	assert.Equal(t, 0, objCnt)
}

func TestGCCatalog2(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	options.WithCatalogGCInterval(10 * time.Millisecond)(opts)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchema(3, 2)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 33)

	checkCompactAndGCFn := func() bool {
		p := &catalog.LoopProcessor{}
		appendableCount := 0
		p.ObjectFn = func(be *catalog.ObjectEntry) error {
			if be.GetTable().GetDB().IsSystemDB() {
				return nil
			}
			if be.IsAppendable() {
				appendableCount++
			}
			return nil
		}
		err := tae.Catalog.RecurLoop(p)
		assert.NoError(t, err)
		return appendableCount == 0
	}

	tae.CreateRelAndAppend(bat, true)
	t.Log(tae.Catalog.SimplePPString(3))
	testutils.WaitExpect(10000, checkCompactAndGCFn)
	assert.True(t, checkCompactAndGCFn())
	t.Log(tae.Catalog.SimplePPString(3))
}
func TestGCCatalog3(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	options.WithCatalogGCInterval(10 * time.Millisecond)(opts)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchema(3, 2)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 33)

	checkCompactAndGCFn := func() bool {
		p := &catalog.LoopProcessor{}
		dbCount := 0
		p.DatabaseFn = func(be *catalog.DBEntry) error {
			if be.IsSystemDB() {
				return nil
			}
			dbCount++
			return nil
		}
		err := tae.Catalog.RecurLoop(p)
		assert.NoError(t, err)
		return dbCount == 0
	}

	tae.CreateRelAndAppend(bat, true)
	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	_, err = txn.DropDatabase("db")
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	t.Log(tae.Catalog.SimplePPString(3))
	testutils.WaitExpect(10000, checkCompactAndGCFn)
	assert.True(t, checkCompactAndGCFn())
	t.Log(tae.Catalog.SimplePPString(3))
}

func TestForceCheckpoint(t *testing.T) {
	fault.Enable()
	defer fault.Disable()
	err := fault.AddFaultPoint(context.Background(), "tae: flush timeout", ":::", "echo", 0, "mock flush timeout")
	assert.NoError(t, err)
	defer func() {
		err := fault.RemoveFaultPoint(context.Background(), "tae: flush timeout")
		assert.NoError(t, err)
	}()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(18, 2)
	schema.BlockMaxRows = 5
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 50)

	tae.CreateRelAndAppend(bat, true)

	err = tae.BGCheckpointRunner.ForceFlushWithInterval(tae.TxnMgr.Now(), context.Background(), time.Second*2, time.Millisecond*10)
	assert.Error(t, err)
	err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false)
	assert.NoError(t, err)
}

func TestLogailAppend(t *testing.T) {
	ctx := context.Background()
	tae := testutil.NewTestEngine(ctx, ModuleName, t, nil)
	defer tae.Close()
	tae.DB.LogtailMgr.RegisterCallback(logtail.MockCallback)
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	batch := catalog.MockBatch(schema, int(schema.BlockMaxRows*uint32(schema.ObjectMaxBlocks)-1))
	//create database, create table, append
	tae.CreateRelAndAppend(batch, true)
	//delete
	err := tae.DeleteAll(true)
	assert.NoError(t, err)
	//compact(metadata)
	tae.DoAppend(batch)
	tae.CompactBlocks(false)
	//drop table
	tae.DropRelation(t)
	//drop database
	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	txn.DropDatabase("db")
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestSnapshotLag1(t *testing.T) {
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(14, 3)
	schema.BlockMaxRows = 10000
	schema.ObjectMaxBlocks = 10
	tae.BindSchema(schema)

	data := catalog.MockBatch(schema, 20)
	defer data.Close()

	bats := data.Split(4)
	tae.CreateRelAndAppend(bats[0], true)

	txn1, rel1 := tae.GetRelation()
	assert.NoError(t, rel1.Append(context.Background(), bats[1]))
	txn2, rel2 := tae.GetRelation()
	assert.NoError(t, rel2.Append(context.Background(), bats[1]))

	{
		txn, rel := tae.GetRelation()
		assert.NoError(t, rel.Append(context.Background(), bats[1]))
		assert.NoError(t, txn.Commit(context.Background()))
	}

	txn1.MockStartTS(tae.TxnMgr.Now())
	err := txn1.Commit(context.Background())
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	err = txn2.Commit(context.Background())
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
}

func TestMarshalPartioned(t *testing.T) {
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(14, 3)
	schema.BlockMaxRows = 10000
	schema.ObjectMaxBlocks = 10
	schema.Partitioned = 1
	tae.BindSchema(schema)

	data := catalog.MockBatch(schema, 20)
	defer data.Close()

	bats := data.Split(4)
	tae.CreateRelAndAppend(bats[0], true)

	_, rel := tae.GetRelation()
	partioned := rel.Schema(false).(*catalog.Schema).Partitioned
	assert.Equal(t, int8(1), partioned)

	tae.Restart(ctx)

	_, rel = tae.GetRelation()
	partioned = rel.Schema(false).(*catalog.Schema).Partitioned
	assert.Equal(t, int8(1), partioned)

	err := tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false)
	assert.NoError(t, err)
	lsn := tae.BGCheckpointRunner.MaxLSNInRange(tae.TxnMgr.Now())
	entry, err := tae.Wal.RangeCheckpoint(1, lsn)
	assert.NoError(t, err)
	assert.NoError(t, entry.WaitDone())

	tae.Restart(ctx)

	_, rel = tae.GetRelation()
	partioned = rel.Schema(false).(*catalog.Schema).Partitioned
	assert.Equal(t, int8(1), partioned)
}

func TestDedup2(t *testing.T) {
	ctx := context.Background()
	opts := config.WithQuickScanAndCKPAndGCOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(14, 3)
	schema.BlockMaxRows = 2
	schema.ObjectMaxBlocks = 10
	schema.Partitioned = 1
	tae.BindSchema(schema)

	count := 50
	data := catalog.MockBatch(schema, count)
	datas := data.Split(count)

	tae.CreateRelAndAppend(datas[0], true)

	for i := 1; i < count; i++ {
		tae.DoAppend(datas[i])
		txn, rel := tae.GetRelation()
		for j := 0; j <= i; j++ {
			err := rel.Append(context.Background(), datas[j])
			assert.Error(t, err, "txn start at %v", txn.GetStartTS().ToString())
		}
		assert.NoError(t, txn.Commit(context.Background()))
	}
}

func TestCompactLargeTable(t *testing.T) {
	ctx := context.Background()
	opts := config.WithQuickScanAndCKPAndGCOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(600, 3)
	schema.BlockMaxRows = 2
	schema.ObjectMaxBlocks = 10
	schema.Partitioned = 1
	tae.BindSchema(schema)

	data := catalog.MockBatch(schema, 10)

	tae.CreateRelAndAppend(data, true)

	tae.Restart(ctx)

	tae.CheckRowsByScan(10, true)

	testutils.WaitExpect(10000, func() bool {
		return tae.Wal.GetPenddingCnt() == 0
	})

	tae.Restart(ctx)

	tae.CheckRowsByScan(10, true)
}

func TestCommitS3Blocks(t *testing.T) {
	ctx := context.Background()
	opts := config.WithQuickScanAndCKPAndGCOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(60, 3)
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 10
	schema.Partitioned = 1
	tae.BindSchema(schema)

	data := catalog.MockBatch(schema, 200)
	datas := data.Split(10)
	tae.CreateRelAndAppend(datas[0], true)
	datas = datas[1:]

	statsVecs := make([]containers.Vector, 0)
	for _, bat := range datas {
		name := objectio.BuildObjectNameWithObjectID(objectio.NewObjectid())
		writer, err := blockio.NewBlockWriterNew(tae.Runtime.Fs.Service, name, 0, nil)
		assert.Nil(t, err)
		writer.SetPrimaryKey(3)
		for i := 0; i < 50; i++ {
			_, err := writer.WriteBatch(containers.ToCNBatch(bat))
			assert.Nil(t, err)
			//offset++
		}
		blocks, _, err := writer.Sync(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, 50, len(blocks))
		statsVec := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
		defer statsVec.Close()
		statsVec.Append(writer.GetObjectStats()[objectio.SchemaData][:], false)
		statsVecs = append(statsVecs, statsVec)
	}

	for _, vec := range statsVecs {
		txn, rel := tae.GetRelation()
		rel.AddObjsWithMetaLoc(context.Background(), vec)
		assert.NoError(t, txn.Commit(context.Background()))
	}
	for _, vec := range statsVecs {
		txn, rel := tae.GetRelation()
		err := rel.AddObjsWithMetaLoc(context.Background(), vec)
		assert.Error(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
	}
}

func TestDedupSnapshot1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 3
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 10)
	tae.CreateRelAndAppend(bat, true)

	testutils.WaitExpect(10000, func() bool {
		return tae.Wal.GetPenddingCnt() == 0
	})
	assert.Equal(t, uint64(0), tae.Wal.GetPenddingCnt())

	txn, rel := tae.GetRelation()
	startTS := txn.GetStartTS()
	txn.SetSnapshotTS(startTS.Next())
	txn.SetDedupType(txnif.DedupPolicy_CheckIncremental)
	err := rel.Append(context.Background(), bat)
	assert.NoError(t, err)
	_ = txn.Commit(context.Background())
}

func TestDedupSnapshot2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 3
	tae.BindSchema(schema)
	data := catalog.MockBatch(schema, 200)
	testutil.CreateRelation(t, tae.DB, "db", schema, true)

	name := objectio.BuildObjectNameWithObjectID(objectio.NewObjectid())
	writer, err := blockio.NewBlockWriterNew(tae.Runtime.Fs.Service, name, 0, nil)
	assert.Nil(t, err)
	writer.SetPrimaryKey(3)
	_, err = writer.WriteBatch(containers.ToCNBatch(data))
	assert.Nil(t, err)
	blocks, _, err := writer.Sync(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 1, len(blocks))
	statsVec := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	defer statsVec.Close()
	statsVec.Append(writer.GetObjectStats()[objectio.SchemaData][:], false)

	name2 := objectio.BuildObjectNameWithObjectID(objectio.NewObjectid())
	writer, err = blockio.NewBlockWriterNew(tae.Runtime.Fs.Service, name2, 0, nil)
	assert.Nil(t, err)
	writer.SetPrimaryKey(3)
	_, err = writer.WriteBatch(containers.ToCNBatch(data))
	assert.Nil(t, err)
	blocks, _, err = writer.Sync(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 1, len(blocks))
	statsVec2 := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	defer statsVec2.Close()
	statsVec2.Append(writer.GetObjectStats()[objectio.SchemaData][:], false)

	txn, rel := tae.GetRelation()
	err = rel.AddObjsWithMetaLoc(context.Background(), statsVec)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	startTS := txn.GetStartTS()
	txn.SetSnapshotTS(startTS.Next())
	txn.SetDedupType(txnif.DedupPolicy_CheckIncremental)
	err = rel.AddObjsWithMetaLoc(context.Background(), statsVec2)
	assert.NoError(t, err)
	_ = txn.Commit(context.Background())
}

func TestDedupSnapshot3(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 3
	tae.BindSchema(schema)
	testutil.CreateRelation(t, tae.DB, "db", schema, true)

	totalRows := 100

	bat := catalog.MockBatch(schema, int(totalRows))
	bats := bat.Split(totalRows)
	var wg sync.WaitGroup
	pool, _ := ants.NewPool(80)
	defer pool.Release()

	appendFn := func(offset uint32) func() {
		return func() {
			defer wg.Done()
			txn, _ := tae.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)
			err := rel.BatchDedup(bats[offset].Vecs[3])
			txn.Commit(context.Background())
			if err != nil {
				logutil.Infof("err is %v", err)
				return
			}

			txn2, _ := tae.StartTxn(nil)
			txn2.SetDedupType(txnif.DedupPolicy_CheckIncremental)
			txn2.SetSnapshotTS(txn.GetStartTS())
			database, _ = txn2.GetDatabase("db")
			rel, _ = database.GetRelationByName(schema.Name)
			_ = rel.Append(context.Background(), bats[offset])
			_ = txn2.Commit(context.Background())
		}
	}

	for i := 0; i < totalRows; i++ {
		for j := 0; j < 5; j++ {
			wg.Add(1)
			err := pool.Submit(appendFn(uint32(i)))
			assert.Nil(t, err)
		}
	}
	wg.Wait()

	txn, rel := tae.GetRelation()
	for _, def := range schema.ColDefs {
		rows := testutil.GetColumnRowsByScan(t, rel, def.Idx, false)
		if totalRows != rows {
			t.Log(tae.Catalog.SimplePPString(common.PPL3))
			it := rel.MakeObjectIt(false)
			for it.Next() {
				obj := it.GetObject()
				t.Log(obj.GetMeta().(*catalog.ObjectEntry).GetObjectData().PPString(common.PPL3, 0, "", -1))
			}
		}
		assert.Equal(t, totalRows, rows)
	}
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestSoftDeleteRollback(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 20
	schema.Name = "testtable"
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 50)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)

	// flush the table
	txn2, rel := tae.GetRelation()
	metas := testutil.GetAllBlockMetas(rel, false)
	task, err := jobs.NewFlushTableTailTask(nil, txn2, metas, nil, tae.Runtime)
	assert.NoError(t, err)
	err = task.OnExec(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, txn2.Commit(context.Background()))

	txn, rel := tae.GetRelation()
	it := rel.MakeObjectIt(false)
	var obj *catalog.ObjectEntry
	for it.Next() {
		obj = it.GetObject().GetMeta().(*catalog.ObjectEntry)
		if obj.IsActive() && !obj.IsAppendable() {
			break
		}
	}
	t.Log(obj.ID().String())
	assert.NoError(t, txn.GetStore().SoftDeleteObject(false, obj.AsCommonID()))
	assert.NoError(t, txn.Rollback(ctx))

	tae.CheckRowsByScan(50, false)
}

func TestDeduplication(t *testing.T) {
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(60, 3)
	schema.BlockMaxRows = 2
	schema.ObjectMaxBlocks = 10
	tae.BindSchema(schema)
	testutil.CreateRelation(t, tae.DB, "db", schema, true)

	rows := 10
	bat := catalog.MockBatch(schema, rows)
	bats := bat.Split(rows)

	ObjectIDs := make([]*types.Objectid, 2)
	ObjectIDs[0] = objectio.NewObjectid()
	ObjectIDs[1] = objectio.NewObjectid()
	sort.Slice(ObjectIDs, func(i, j int) bool {
		return ObjectIDs[i].Le(*ObjectIDs[j])
	})

	blk1Name := objectio.BuildObjectNameWithObjectID(ObjectIDs[1])
	writer, err := blockio.NewBlockWriterNew(tae.Runtime.Fs.Service, blk1Name, 0, nil)
	assert.NoError(t, err)
	writer.SetPrimaryKey(3)
	writer.WriteBatch(containers.ToCNBatch(bats[0]))
	blocks, _, err := writer.Sync(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(blocks))

	statsVec := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	defer statsVec.Close()
	statsVec.Append(writer.GetObjectStats()[objectio.SchemaData][:], false)

	txn, rel := tae.GetRelation()
	err = rel.AddObjsWithMetaLoc(context.Background(), statsVec)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, err = tae.StartTxn(nil)
	assert.NoError(t, err)
	db, err := tae.Catalog.TxnGetDBEntryByName("db", txn)
	assert.NoError(t, err)
	tbl, err := db.TxnGetTableEntryByName(schema.Name, txn)
	assert.NoError(t, err)
	dataFactory := tables.NewDataFactory(
		tae.Runtime,
		tae.Dir)
	stats := objectio.NewObjectStatsWithObjectID(ObjectIDs[0], true, false, false)
	obj, err := tbl.CreateObject(
		txn,
		new(objectio.CreateObjOpt).WithObjectStats(stats).WithIsTombstone(false), dataFactory.MakeObjectFactory())
	assert.NoError(t, err)
	txn.GetStore().AddTxnEntry(txnif.TxnType_Normal, obj)
	txn.GetStore().IncreateWriteCnt()
	assert.NoError(t, txn.Commit(context.Background()))
	assert.NoError(t, obj.PrepareCommit())
	assert.NoError(t, obj.ApplyCommit(txn.GetID()))

	txns := make([]txnif.AsyncTxn, 0)
	for i := 0; i < 5; i++ {
		for j := 1; j < rows; j++ {
			txn, _ := tae.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)
			_ = rel.Append(context.Background(), bats[j])
			txns = append(txns, txn)
		}
	}
	for _, txn := range txns {
		txn.Commit(context.Background())
	}
	tae.CheckRowsByScan(rows, false)
	t.Log(tae.Catalog.SimplePPString(3))
}

func TestRW(t *testing.T) {
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	rows := 10
	schema := catalog.MockSchemaAll(5, 2)
	schema.BlockMaxRows = uint32(rows)
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, rows)
	defer bat.Close()
	tae.CreateRelAndAppend(bat, true)

	txn1, rel1 := tae.GetRelation()
	v := bat.Vecs[2].Get(2)
	filter := handle.NewEQFilter(v)
	id, row, err := rel1.GetByFilter(ctx, filter)
	assert.NoError(t, err)
	err = rel1.RangeDelete(id, row, row, handle.DT_Normal)
	assert.NoError(t, err)

	meta := rel1.GetMeta().(*catalog.TableEntry)

	cnt := 3
	for i := 0; i < cnt; i++ {
		txn2, rel2 := tae.GetRelation()
		v = bat.Vecs[2].Get(i + 3)
		filter = handle.NewEQFilter(v)
		id, row, err = rel2.GetByFilter(ctx, filter)
		assert.NoError(t, err)
		err = rel2.RangeDelete(id, row, row, handle.DT_Normal)
		assert.NoError(t, err)
		err = txn2.Commit(ctx)
		assert.NoError(t, err)

		err = tae.FlushTable(
			ctx, 0, meta.GetDB().ID, meta.ID,
			types.BuildTS(time.Now().UTC().UnixNano(), 0),
		)
		assert.NoError(t, err)
	}

	err = txn1.Commit(ctx)
	assert.NoError(t, err)

	{
		txn, rel := tae.GetRelation()
		rcnt := testutil.GetColumnRowsByScan(t, rel, 2, true)
		assert.Equal(t, rows-cnt-1, rcnt)
		assert.NoError(t, txn.Commit(ctx))
	}
}

func TestReplayDeletes(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	rows := 250
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 50
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, rows)
	defer bat.Close()
	bats := bat.Split(5)
	tae.CreateRelAndAppend(bats[0], true)
	//nablk
	tae.CompactBlocks(false)
	//deletes
	txn, rel := tae.GetRelation()
	blk := testutil.GetOneObject(rel)
	rel.RangeDelete(blk.Fingerprint(), 1, 49, handle.DT_Normal)
	assert.NoError(t, txn.Commit(context.Background()))
	//the next blk to compact
	tae.DoAppend(bats[1])
	//keep the Object appendable
	tae.DoAppend(bats[2])
	//compact nablk and its next blk
	txn2, rel := tae.GetRelation()
	blkEntry := testutil.GetOneTombstoneMeta(rel)
	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	task, err := jobs.NewFlushTableTailTask(nil, txn, nil, []*catalog.ObjectEntry{blkEntry}, tae.Runtime)
	assert.NoError(t, err)
	err = task.OnExec(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
	assert.NoError(t, txn2.Commit(context.Background()))
}
func TestApplyDeltalocation1(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	rows := 10
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 10
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, rows)
	defer bat.Close()
	tae.CreateRelAndAppend(bat, true)

	// apply deleteloc fails on ablk
	v1 := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(1)
	ok, err := tae.TryDeleteByDeltaloc([]any{v1})
	assert.NoError(t, err)
	assert.True(t, ok)

	tae.CompactBlocks(false)
	filter := handle.NewEQFilter(v1)
	txn, rel := tae.GetRelation()
	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.Error(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	// apply deltaloc fails if there're persisted deletes
	v2 := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(2)
	ok, err = tae.TryDeleteByDeltaloc([]any{v2})
	assert.NoError(t, err)
	assert.True(t, ok)

	// apply deltaloc fails if there're deletes in memory
	tae.CompactBlocks(false)
	v3 := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(3)
	filter = handle.NewEQFilter(v3)
	txn, rel = tae.GetRelation()
	id, offset, err := rel.GetByFilter(context.Background(), filter)
	assert.NoError(t, err)
	err = rel.RangeDelete(id, offset, offset, handle.DT_Normal)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	v4 := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(4)
	ok, err = tae.TryDeleteByDeltaloc([]any{v4})
	assert.NoError(t, err)
	assert.True(t, ok)

}

// test compact
func TestApplyDeltalocation2(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	rows := 10
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 10
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, rows)
	bats := bat.Split(10)
	defer bat.Close()
	tae.CreateRelAndAppend(bat, true)
	tae.CompactBlocks(false)

	v3 := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(3)
	v5 := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(5)
	filter3 := handle.NewEQFilter(v3)
	filter5 := handle.NewEQFilter(v5)

	// test logtail
	tae.LogtailMgr.RegisterCallback(logtail.MockCallback)
	tae.TryDeleteByDeltaloc([]any{v3, v5})
	t.Log(tae.Catalog.SimplePPString(3))

	txn, rel := tae.GetRelation()
	_, _, err := rel.GetByFilter(context.Background(), filter5)
	assert.Error(t, err)
	_, _, err = rel.GetByFilter(context.Background(), filter3)
	assert.Error(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
	tae.CheckRowsByScan(8, true)

	tae.Restart(context.Background())
	txn, rel = tae.GetRelation()
	_, _, err = rel.GetByFilter(context.Background(), filter5)
	assert.Error(t, err)
	_, _, err = rel.GetByFilter(context.Background(), filter3)
	assert.Error(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
	tae.CheckRowsByScan(8, true)

	// test dedup
	tae.DoAppend(bats[3])
	tae.CheckRowsByScan(9, true)

	// test compact
	tae.CompactBlocks(false)
	txn, rel = tae.GetRelation()
	_, _, err = rel.GetByFilter(context.Background(), filter5)
	assert.Error(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
	tae.CheckRowsByScan(9, true)

	tae.Restart(context.Background())
	txn, rel = tae.GetRelation()
	_, _, err = rel.GetByFilter(context.Background(), filter5)
	assert.Error(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
	tae.CheckRowsByScan(9, true)
}

func TestApplyDeltalocation3(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	rows := 10
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 10
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, rows)
	defer bat.Close()
	tae.CreateRelAndAppend(bat, true)
	tae.CompactBlocks(false)

	v3 := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(3)
	filter3 := handle.NewEQFilter(v3)

	// apply deltaloc failed if there're new deletes

	v5 := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(5)
	filter5 := handle.NewEQFilter(v5)
	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	ok, err := tae.TryDeleteByDeltalocWithTxn([]any{v3}, txn)
	assert.NoError(t, err)
	assert.True(t, ok)

	{
		// delete v5
		txn2, rel2 := tae.GetRelation()
		err = rel2.DeleteByFilter(context.Background(), filter5)
		assert.NoError(t, err)
		assert.NoError(t, txn2.Commit(context.Background()))
	}
	tae.CheckRowsByScan(9, true)

	assert.NoError(t, txn.Commit(context.Background()))
	tae.CheckRowsByScan(8, true)

	// delete v5
	v4 := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(4)
	filter4 := handle.NewEQFilter(v4)
	txn2, rel2 := tae.GetRelation()
	err = rel2.DeleteByFilter(context.Background(), filter4)
	assert.NoError(t, err)

	tae.CheckRowsByScan(8, true)

	assert.NoError(t, txn2.Commit(context.Background()))
	tae.CheckRowsByScan(7, true)

	txn, rel := tae.GetRelation()
	_, _, err = rel.GetByFilter(context.Background(), filter3)
	assert.Error(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

}

func TestApplyDeltalocation4(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	rows := 10
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 10
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, rows)
	defer bat.Close()
	bats := bat.Split(rows)
	tae.CreateRelAndAppend(bat, true)

	tae.CompactBlocks(false)

	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	v5 := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(5)
	tae.TryDeleteByDeltalocWithTxn([]any{v5}, txn)
	v1 := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(1)
	filter1 := handle.NewEQFilter(v1)
	db, err := txn.GetDatabase("db")
	assert.NoError(t, err)
	rel, err := db.GetRelationByName(schema.Name)
	assert.NoError(t, err)
	err = rel.DeleteByFilter(context.Background(), filter1)
	assert.NoError(t, err)
	tae.DoAppendWithTxn(bats[1], txn, false)
	tae.DoAppendWithTxn(bats[5], txn, false)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.CheckRowsByScan(rows, true)

	tae.Restart(ctx)

	tae.CheckRowsByScan(rows, true)
}

func TestReplayPersistedDelete(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	rows := 10
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 10
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, rows)
	defer bat.Close()
	tae.CreateRelAndAppend(bat, true)
	tae.CompactBlocks(false)

	v3 := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(3)
	filter3 := handle.NewEQFilter(v3)
	txn, rel := tae.GetRelation()
	id, offset, err := rel.GetByFilter(context.Background(), filter3)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	ok, err := tae.TryDeleteByDeltaloc([]any{v3})
	assert.NoError(t, err)
	assert.True(t, ok)

	tae.Restart(context.Background())

	txn, rel = tae.GetRelation()
	err = rel.RangeDelete(id, offset, offset, handle.DT_Normal)
	assert.Error(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestCheckpointReadWrite(t *testing.T) {
	t.Skip("TODO: find a new way to test three tables ckp")
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	db, err := txn.CreateDatabase("db", "create database db", "1")
	assert.NoError(t, err)
	schema1 := catalog.MockSchemaAll(2, 1)
	_, err = db.CreateRelation(schema1)
	assert.NoError(t, err)
	schema2 := catalog.MockSchemaAll(3, -1)
	_, err = db.CreateRelation(schema2)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	t1 := tae.TxnMgr.Now()
	testutil.CheckCheckpointReadWrite(t, types.TS{}, t1, tae.Catalog, smallCheckpointBlockRows, smallCheckpointSize, tae.Opts.Fs)

	txn, err = tae.StartTxn(nil)
	assert.NoError(t, err)
	db, err = txn.GetDatabase("db")
	assert.NoError(t, err)
	_, err = db.DropRelationByName(schema1.Name)
	assert.NoError(t, err)
	_, err = db.DropRelationByName(schema2.Name)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	t2 := tae.TxnMgr.Now()
	testutil.CheckCheckpointReadWrite(t, types.TS{}, t2, tae.Catalog, smallCheckpointBlockRows, smallCheckpointSize, tae.Opts.Fs)
	testutil.CheckCheckpointReadWrite(t, t1, t2, tae.Catalog, smallCheckpointBlockRows, smallCheckpointSize, tae.Opts.Fs)

	txn, err = tae.StartTxn(nil)
	assert.NoError(t, err)
	_, err = txn.DropDatabase("db")
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
	t3 := tae.TxnMgr.Now()
	testutil.CheckCheckpointReadWrite(t, types.TS{}, t3, tae.Catalog, smallCheckpointBlockRows, smallCheckpointSize, tae.Opts.Fs)
	testutil.CheckCheckpointReadWrite(t, t2, t3, tae.Catalog, smallCheckpointBlockRows, smallCheckpointSize, tae.Opts.Fs)

	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 1
	schema.ObjectMaxBlocks = 1
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 10)

	tae.CreateRelAndAppend(bat, true)
	t4 := tae.TxnMgr.Now()
	testutil.CheckCheckpointReadWrite(t, types.TS{}, t4, tae.Catalog, smallCheckpointBlockRows, smallCheckpointSize, tae.Opts.Fs)
	testutil.CheckCheckpointReadWrite(t, t3, t4, tae.Catalog, smallCheckpointBlockRows, smallCheckpointSize, tae.Opts.Fs)

	tae.CompactBlocks(false)
	t5 := tae.TxnMgr.Now()
	testutil.CheckCheckpointReadWrite(t, types.TS{}, t5, tae.Catalog, smallCheckpointBlockRows, smallCheckpointSize, tae.Opts.Fs)
	testutil.CheckCheckpointReadWrite(t, t4, t5, tae.Catalog, smallCheckpointBlockRows, smallCheckpointSize, tae.Opts.Fs)
}

func TestCheckpointReadWrite2(t *testing.T) {
	t.Skip("TODO: find a new way to test three tables ckp")
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	for i := 0; i < 10; i++ {
		schema := catalog.MockSchemaAll(i+1, i)
		schema.BlockMaxRows = 2
		bat := catalog.MockBatch(schema, rand.Intn(30))
		tae.BindSchema(schema)
		createDB := false
		if i == 0 {
			createDB = true
		}
		tae.CreateRelAndAppend(bat, createDB)
		tae.CompactBlocks(false)
	}

	t1 := tae.TxnMgr.Now()
	testutil.CheckCheckpointReadWrite(t, types.TS{}, t1, tae.Catalog, smallCheckpointBlockRows, smallCheckpointSize, tae.Opts.Fs)
}

func TestSnapshotCheckpoint(t *testing.T) {
	blockio.RunPipelineTest(
		func() {
			defer testutils.AfterTest(t)()
			testutils.EnsureNoLeak(t)
			ctx := context.Background()

			opts := new(options.Options)
			opts = config.WithLongScanAndCKPOpts(opts)
			options.WithDisableGCCheckpoint()(opts)
			tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
			defer tae.Close()
			db := tae.DB
			db.DiskCleaner.GetCleaner().SetMinMergeCountForTest(2)

			schema1 := catalog.MockSchemaAll(13, 2)
			schema1.BlockMaxRows = 10
			schema1.ObjectMaxBlocks = 2

			schema2 := catalog.MockSchemaAll(13, 2)
			schema2.BlockMaxRows = 10
			schema2.ObjectMaxBlocks = 2
			var rel1 handle.Relation
			{
				txn, _ := db.StartTxn(nil)
				database, err := txn.CreateDatabase("db", "", "")
				assert.Nil(t, err)
				rel1, err = database.CreateRelation(schema1)
				assert.Nil(t, err)
				_, err = database.CreateRelation(schema2)
				assert.Nil(t, err)
				assert.Nil(t, txn.Commit(context.Background()))
			}
			bat := catalog.MockBatch(schema1, int(schema1.BlockMaxRows*10-1))
			defer bat.Close()
			bats := bat.Split(bat.Length())

			pool, err := ants.NewPool(20)
			assert.Nil(t, err)
			defer pool.Release()
			var wg sync.WaitGroup

			for i := 0; i < len(bats)/2; i++ {
				wg.Add(2)
				err = pool.Submit(testutil.AppendClosure(t, bats[i], schema1.Name, db, &wg))
				assert.Nil(t, err)
				err = pool.Submit(testutil.AppendClosure(t, bats[i], schema2.Name, db, &wg))
				assert.Nil(t, err)
			}
			wg.Wait()
			ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
			db.ForceCheckpoint(ctx, ts, time.Minute)
			snapshot := types.BuildTS(time.Now().UTC().UnixNano(), 0)
			db.ForceCheckpoint(ctx, snapshot, time.Minute)
			tae.ForceCheckpoint()
			assert.Equal(t, uint64(0), db.Runtime.Scheduler.GetPenddingLSNCnt())
			var wg2 sync.WaitGroup
			for i := len(bats) / 2; i < len(bats); i++ {
				wg2.Add(2)
				err = pool.Submit(testutil.AppendClosure(t, bats[i], schema1.Name, db, &wg2))
				assert.Nil(t, err)
				err = pool.Submit(testutil.AppendClosure(t, bats[i], schema2.Name, db, &wg2))
				assert.Nil(t, err)
			}
			wg2.Wait()
			t.Log(tae.Catalog.SimplePPString(3))
			tae.ForceCheckpoint()
			tae.ForceCheckpoint()
			dataObject, tombstoneObject := testutil.GetUserTablesInsBatch(t, rel1.ID(), types.TS{}, snapshot, db.Catalog)
			ckps, err := checkpoint.ListSnapshotCheckpoint(ctx, "", db.Opts.Fs, snapshot, rel1.ID(), checkpoint.SpecifiedCheckpoint)
			assert.Nil(t, err)
			var inslen, dataObjectLen, tombstoneObjectLen int
			for _, ckp := range ckps {
				ins, _, dataObject, tombstoneObject, cbs := testutil.ReadSnapshotCheckpoint(t, rel1.ID(), ckp.GetLocation(), db.Opts.Fs)
				for _, cb := range cbs {
					if cb != nil {
						cb()
					}
				}
				if ins != nil {
					moIns, err := batch.ProtoBatchToBatch(ins)
					assert.NoError(t, err)
					inslen += moIns.Vecs[0].Length()
				}
				if dataObject != nil {
					moIns, err := batch.ProtoBatchToBatch(dataObject)
					assert.NoError(t, err)
					dataObjectLen += moIns.Vecs[0].Length()
				}
				if tombstoneObject != nil {
					moIns, err := batch.ProtoBatchToBatch(tombstoneObject)
					assert.NoError(t, err)
					tombstoneObjectLen += moIns.Vecs[0].Length()
				}
			}
			assert.Equal(t, dataObjectLen, dataObject.Length())
			assert.Equal(t, tombstoneObjectLen, tombstoneObject.Length())
			assert.Equal(t, int64(0), common.DebugAllocator.CurrNB())
		},
	)
}

func TestEstimateMemSize(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 50
	schemaBig := catalog.MockSchemaAll(14, 1)

	schema50rowSize := 0
	{
		tae.BindSchema(schema)
		bat := catalog.MockBatch(schema, 50)
		testutil.CreateRelationAndAppend(t, 0, tae.DB, "db", schema, bat, true)
		txn, rel := tae.GetRelation()
		blk := testutil.GetOneBlockMeta(rel)
		size1, ds1 := blk.GetObjectData().EstimateMemSize()
		schema50rowSize = size1

		blkID := objectio.NewBlockidWithObjectID(blk.ID(), 0)
		err := rel.DeleteByPhyAddrKey(*objectio.NewRowid(blkID, 1))
		assert.NoError(t, err)
		size2, ds2 := blk.GetObjectData().EstimateMemSize()

		err = rel.DeleteByPhyAddrKey(*objectio.NewRowid(blkID, 5))
		assert.NoError(t, err)
		size3, ds3 := blk.GetObjectData().EstimateMemSize()
		// assert.Less(t, size1, size2)
		// assert.Less(t, size2, size3)
		assert.NoError(t, txn.Rollback(ctx))
		size4, ds4 := blk.GetObjectData().EstimateMemSize()
		t.Log(size1, size2, size3, size4)
		t.Log(ds1, ds2, ds3, ds4)
	}

	{
		tae.BindSchema(schemaBig)
		bat := catalog.MockBatch(schemaBig, 50)
		testutil.CreateRelationAndAppend(t, 0, tae.DB, "db", schemaBig, bat, false)
		txn, rel := tae.GetRelation()
		blk := testutil.GetOneBlockMeta(rel)
		size1, d1 := blk.GetObjectData().EstimateMemSize()

		blkID := objectio.NewBlockidWithObjectID(blk.ID(), 0)
		err := rel.DeleteByPhyAddrKey(*objectio.NewRowid(blkID, 1))
		assert.NoError(t, err)

		size2, d2 := blk.GetObjectData().EstimateMemSize()

		err = rel.DeleteByPhyAddrKey(*objectio.NewRowid(blkID, 5))
		assert.NoError(t, err)
		size3, d3 := blk.GetObjectData().EstimateMemSize()

		assert.NoError(t, txn.Commit(ctx))

		txn, rel = tae.GetRelation()
		tombstone := testutil.GetOneTombstoneMeta(rel)
		size4, d4 := tombstone.GetObjectData().EstimateMemSize()

		t.Log(size1, size2, size3, size4)
		t.Log(d1, d2, d3, d4)
		assert.Equal(t, size1, size2)
		assert.Equal(t, size2, size3)
		assert.NotZero(t, size4)
		assert.Less(t, schema50rowSize, size1)
		assert.NoError(t, txn.Commit(ctx))
	}
}

func TestColumnCount(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 50
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 1)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)

	{
		txn, rel := tae.GetRelation()
		for i := 0; i < 500; i++ {
			colName := fmt.Sprintf("col %d", i)
			err := rel.AlterTable(context.TODO(), api.NewAddColumnReq(0, 0, colName, types.NewProtoType(types.T_char), 5))
			assert.NoError(t, err)
		}
		assert.Nil(t, txn.Commit(context.Background()))
	}

	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	db, err := txn.GetDatabase("db")
	assert.NoError(t, err)
	_, err = db.DropRelationByName(schema.Name)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	commitTS := txn.GetCommitTS()
	tae.Catalog.GCByTS(context.Background(), commitTS.Next())
}

func TestCollectDeletesInRange1(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 50
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 2)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)

	txn, rel := tae.GetRelation()
	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(0)
	filter := handle.NewEQFilter(v)
	err := rel.DeleteByFilter(context.Background(), filter)
	assert.NoError(t, err)
	err = txn.Commit(context.Background())
	assert.NoError(t, err)

	txn, rel = tae.GetRelation()
	v = bat.Vecs[schema.GetSingleSortKeyIdx()].Get(1)
	filter = handle.NewEQFilter(v)
	err = rel.DeleteByFilter(context.Background(), filter)
	assert.NoError(t, err)
	err = txn.Commit(context.Background())
	assert.NoError(t, err)

	tae.CheckCollectTombstoneInRange()
}

func TestCollectDeletesInRange2(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 50
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 50)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)
	tae.CompactBlocks(false)

	txn, rel := tae.GetRelation()
	blk := testutil.GetOneObject(rel)
	deltaLoc, err := testutil.MockCNDeleteInS3(tae.Runtime.Fs, blk.GetMeta().(*catalog.ObjectEntry).GetObjectData(), 0, schema, txn, []uint32{0, 1, 2, 3})
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	blk = testutil.GetOneObject(rel)
	ok, err := rel.TryDeleteByDeltaloc(blk.Fingerprint(), deltaLoc)
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	t.Log(tae.Catalog.SimplePPString(3))
	txn, rel = tae.GetRelation()
	blk = testutil.GetOneObject(rel)
	tableEntry := rel.GetMeta().(*catalog.TableEntry)
	deletes, err := tables.TombstoneRangeScanByObject(
		ctx, tableEntry, *blk.GetID(), types.TS{}, txn.GetStartTS(), common.DefaultAllocator, tae.Runtime.VectorPool.Small,
	)
	assert.NoError(t, err)
	assert.Equal(t, 4, deletes.Length())
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	v1 := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(4)
	filter := handle.NewEQFilter(v1)
	err = rel.DeleteByFilter(context.Background(), filter)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	blk = testutil.GetOneObject(rel)
	tableEntry = rel.GetMeta().(*catalog.TableEntry)
	deletes, err = tables.TombstoneRangeScanByObject(
		ctx, tableEntry, *blk.GetID(), types.TS{}, txn.GetStartTS(), common.DefaultAllocator, tae.Runtime.VectorPool.Small,
	)
	assert.NoError(t, err)
	assert.Equal(t, 5, deletes.Length())
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestGlobalCheckpoint7(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	options.WithCheckpointGlobalMinCount(3)(opts)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	_, err = txn.CreateDatabase("db1", "sql", "typ")
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	testutils.WaitExpect(10000, func() bool {
		return tae.Wal.GetPenddingCnt() == 0
	})

	entries := tae.BGCheckpointRunner.GetAllCheckpoints()
	for _, e := range entries {
		t.Logf("%s", e.String())
	}
	assert.Equal(t, 1, len(entries))

	tae.Restart(context.Background())

	txn, err = tae.StartTxn(nil)
	assert.NoError(t, err)
	_, err = txn.CreateDatabase("db2", "sql", "typ")
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	testutils.WaitExpect(10000, func() bool {
		return tae.Wal.GetPenddingCnt() == 0
	})

	entries = tae.BGCheckpointRunner.GetAllCheckpoints()
	for _, e := range entries {
		t.Logf("%s", e.String())
	}
	assert.Equal(t, 2, len(entries))

	tae.Restart(context.Background())

	txn, err = tae.StartTxn(nil)
	assert.NoError(t, err)
	_, err = txn.CreateDatabase("db3", "sql", "typ")
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	testutils.WaitExpect(10000, func() bool {
		return tae.Wal.GetPenddingCnt() == 0
	})

	testutils.WaitExpect(10000, func() bool {
		return len(tae.BGCheckpointRunner.GetAllGlobalCheckpoints()) == 1
	})

	entries = tae.BGCheckpointRunner.GetAllCheckpoints()
	for _, e := range entries {
		t.Logf("%s", e.String())
	}
	assert.Equal(t, 1, len(entries))

}

func TestSplitCommand(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	opts.MaxMessageSize = txnbase.CmdBufReserved + 2*1024
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 50
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 50)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)

	tae.CheckRowsByScan(50, false)
	t.Log(tae.Catalog.SimplePPString(3))
	tae.Restart(context.Background())
	t.Log(tae.Catalog.SimplePPString(3))
	tae.CheckRowsByScan(50, false)
}

func TestFlushAndAppend(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 50
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 50)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)

	txn, rel := tae.GetRelation()
	var metas []*catalog.ObjectEntry
	it := rel.MakeObjectIt(false)
	for it.Next() {
		blk := it.GetObject()
		meta := blk.GetMeta().(*catalog.ObjectEntry)
		metas = append(metas, meta)
	}
	it.Next()
	_ = txn.Commit(context.Background())

	txn, _ = tae.GetRelation()

	tae.DeleteAll(true)

	task, err := jobs.NewFlushTableTailTask(nil, txn, metas, nil, tae.Runtime)
	assert.NoError(t, err)
	_ = task.OnExec(context.Background())

	txn2, _ := tae.GetRelation()

	_ = txn.Commit(context.Background())

	tae.DoAppendWithTxn(bat, txn2, true)

	t.Log(tae.Catalog.SimplePPString(3))
	err = txn2.Commit(context.Background())
	assert.NoError(t, err)

}

func TestFlushAndAppend2(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 50
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 50)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)

	txn, rel := tae.GetRelation()
	var metas []*catalog.ObjectEntry
	it := rel.MakeObjectIt(false)
	for it.Next() {
		blk := it.GetObject()
		meta := blk.GetMeta().(*catalog.ObjectEntry)
		metas = append(metas, meta)
	}
	it.Close()
	_ = txn.Commit(context.Background())

	txn, _ = tae.GetRelation()

	task, err := jobs.NewFlushTableTailTask(nil, txn, metas, nil, tae.Runtime)
	assert.NoError(t, err)
	_ = task.OnExec(context.Background())

	tae.DeleteAll(true)

	txn2, _ := tae.GetRelation()

	_ = txn.Commit(context.Background())

	tae.DoAppendWithTxn(bat, txn2, true)

	t.Log(tae.Catalog.SimplePPString(3))
	err = txn2.Commit(context.Background())
	assert.NoError(t, err)

	p := &catalog.LoopProcessor{}
	p.TombstoneFn = func(oe *catalog.ObjectEntry) error {
		prepareTS := oe.GetLastMVCCNode().GetPrepare()
		assert.False(t, prepareTS.Equal(&txnif.UncommitTS), oe.ID().String())
		return nil
	}
	tae.Catalog.RecurLoop(p)
}

func TestDeletesInMerge(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 50
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 50)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)
	tae.CompactBlocks(true)

	txn, rel := tae.GetRelation()
	objID := testutil.GetOneObject(rel).Fingerprint()
	rel.RangeDelete(objID, 0, 0, handle.DT_Normal)
	txn.Commit(ctx)

	t.Log(tae.Catalog.SimplePPString(3))

	txn, _ = tae.StartTxn(nil)
	obj := testutil.GetOneBlockMeta(rel)
	task, _ := jobs.NewMergeObjectsTask(
		nil, txn, []*catalog.ObjectEntry{obj}, tae.Runtime,
		common.DefaultMaxOsizeObjMB*common.Const1MBytes, false)
	task.Execute(ctx)
	{
		txn, rel := tae.GetRelation()
		obj := testutil.GetOneTombstoneMeta(rel)
		task, _ := jobs.NewFlushTableTailTask(nil, txn, nil, []*catalog.ObjectEntry{obj}, tae.Runtime)
		task.Execute(ctx)
		txn.Commit(ctx)
	}

	txn.Commit(ctx)
}

func TestDedupAndFlush(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	options.WithGlobalVersionInterval(time.Microsecond)(opts)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 50
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 1)
	defer bat.Close()

	tae.CreateRelAndAppend(bat, true)

	flushTxn, rel := tae.GetRelation()

	obj := testutil.GetOneBlockMeta(rel)
	task, err := jobs.NewFlushTableTailTask(nil, flushTxn, []*catalog.ObjectEntry{obj}, nil, tae.Runtime)
	assert.NoError(t, err)
	err = task.OnExec(ctx)
	assert.NoError(t, err)

	{
		// mock update
		txn, rel := tae.GetRelation()
		v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(0)
		filter := handle.NewEQFilter(v)
		err := rel.DeleteByFilter(context.Background(), filter)
		assert.NoError(t, err)
		err = rel.Append(ctx, bat)
		assert.NoError(t, err)
		err = txn.Commit(context.Background())
		assert.NoError(t, err)
	}

	assert.NoError(t, flushTxn.Commit(ctx))
}

func TestTransferDeletes(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchema(2, 0)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 10
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 1)

	tae.CreateRelAndAppend(bat, true)

	txn, rel := testutil.GetDefaultRelation(t, tae.DB, schema.Name)
	blkMetas := testutil.GetAllBlockMetas(rel, false)
	task, err := jobs.NewFlushTableTailTask(tasks.WaitableCtx, txn, blkMetas, nil, tae.DB.Runtime)
	assert.NoError(t, err)
	err = task.OnExec(ctx)
	assert.NoError(t, err)

	txn2, rel := tae.GetRelation()
	filter := handle.NewEQFilter(bat.Vecs[0].Get(0))
	assert.NoError(t, rel.UpdateByFilter(ctx, filter, 1, int32(3), false))
	var wg sync.WaitGroup
	txn.SetApplyCommitFn(func(at txnif.AsyncTxn) error {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, txn2.PrePrepare(ctx))
		}()
		time.Sleep(time.Millisecond * 100)
		return txn.GetStore().ApplyCommit()
	})
	assert.NoError(t, txn.Commit(ctx))
	wg.Wait()
}

func TestGCKP(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchema(2, 0)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 10
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 1)

	tae.CreateRelAndAppend(bat, true)

	tae.DeleteAll(true)

	tae.CompactBlocks(true)
	time.Sleep(time.Millisecond * 200)

	tae.ForceGlobalCheckpoint(ctx, tae.TxnMgr.Now(), time.Minute, time.Millisecond*100)
	tae.Restart(ctx)
	t.Log(tae.Catalog.SimplePPString(3))
}

func TestCKPCollectObject(t *testing.T) {
	blockio.RunPipelineTest(
		func() {
			defer testutils.AfterTest(t)()
			testutils.EnsureNoLeak(t)
			ctx := context.Background()

			opts := config.WithLongScanAndCKPOpts(nil)
			tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
			defer tae.Close()

			schema := catalog.MockSchema(2, 0)
			schema.BlockMaxRows = 10
			schema.ObjectMaxBlocks = 10
			tae.BindSchema(schema)
			bat := catalog.MockBatch(schema, 1)

			tae.CreateRelAndAppend(bat, true)

			txn, rel := tae.GetRelation()
			blkMeta1 := testutil.GetOneBlockMeta(rel)
			task1, err := jobs.NewFlushTableTailTask(tasks.WaitableCtx, txn, []*catalog.ObjectEntry{blkMeta1}, nil, tae.Runtime)
			assert.NoError(t, err)
			assert.NoError(t, task1.Execute(ctx))

			collector := logtail.NewIncrementalCollector("", types.TS{}, tae.TxnMgr.Now())
			assert.NoError(t, tae.Catalog.RecurLoop(collector))
			ckpData := collector.OrphanData()
			objBatch := ckpData.GetObjectBatchs()
			assert.Equal(t, 1, objBatch.Length())
			assert.NoError(t, txn.Commit(ctx))
		},
	)
}

func TestGCCatalog4(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchema(2, 0)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 10
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 1)

	tae.CreateRelAndAppend(bat, true)

	tae.DeleteAll(true)

	tae.CompactBlocks(true)

	tae.Catalog.GCByTS(ctx, tae.TxnMgr.Now())

}

func TestPersistTransferTable(t *testing.T) {
	ctx := context.Background()
	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 3
	tae.BindSchema(schema)
	testutil.CreateRelation(t, tae.DB, "db", schema, true)

	sid := objectio.NewSegmentid()
	id1 := common.ID{BlockID: *objectio.NewBlockid(sid, 1, 0)}
	id2 := common.ID{BlockID: *objectio.NewBlockid(sid, 2, 0)}
	createdObjs := []*objectio.ObjectId{objectio.NewObjectidWithSegmentIDAndNum(sid, 2)}

	now := time.Now()
	page := model.NewTransferHashPage(&id1, now, false, tae.Runtime.LocalFs.Service, time.Second, time.Minute, createdObjs)
	ids := make([]types.Rowid, 10)
	transferMap := make(api.TransferMap)
	for i := 0; i < 10; i++ {
		transferMap[uint32(i)] = api.TransferDestPos{
			RowIdx: uint32(i),
		}
		rowID := *objectio.NewRowid(&id2.BlockID, uint32(i))
		ids[i] = rowID
	}
	page.Train(transferMap)
	tae.Runtime.TransferTable.AddPage(page)

	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	ioVector := fileservice.IOVector{
		FilePath: name.String(),
	}
	offset := int64(0)
	data := page.Marshal()
	ioEntry := fileservice.IOEntry{
		Offset: offset,
		Size:   int64(len(data)),
		Data:   data,
	}
	ioVector.Entries = append(ioVector.Entries, ioEntry)

	err := tae.Runtime.Fs.Service.Write(context.Background(), ioVector)
	if err != nil {
		return
	}

	path := model.Path{
		Name:   ioVector.FilePath,
		Offset: 0,
		Size:   int64(len(data)),
	}
	page.SetPath(path)

	time.Sleep(2 * time.Second)
	tae.Runtime.TransferTable.RunTTL()
	assert.True(t, page.IsPersist())
	for i := 0; i < 10; i++ {
		id, ok := page.Transfer(uint32(i))
		assert.True(t, ok)
		assert.Equal(t, ids[i], id)
	}
}

func TestClearPersistTransferTable(t *testing.T) {
	blockio.RunPipelineTest(
		func() {
			ctx := context.Background()
			opts := config.WithQuickScanAndCKPOpts(nil)
			tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
			defer tae.Close()
			schema := catalog.MockSchemaAll(13, 3)
			schema.BlockMaxRows = 10
			schema.ObjectMaxBlocks = 3
			tae.BindSchema(schema)
			testutil.CreateRelation(t, tae.DB, "db", schema, true)

			sid := objectio.NewSegmentid()

			id1 := common.ID{BlockID: *objectio.NewBlockid(sid, 1, 0)}
			id2 := common.ID{BlockID: *objectio.NewBlockid(sid, 2, 0)}
			createdObjs := []*objectio.ObjectId{objectio.NewObjectidWithSegmentIDAndNum(sid, 2)}

			now := time.Now()
			page := model.NewTransferHashPage(&id1, now, false, tae.Runtime.LocalFs.Service, time.Second, 2*time.Second, createdObjs)
			ids := make([]types.Rowid, 10)
			transferMap := make(api.TransferMap)
			for i := 0; i < 10; i++ {
				transferMap[uint32(i)] = api.TransferDestPos{
					BlkIdx: 0,
					RowIdx: uint32(i),
				}
				rowID := *objectio.NewRowid(&id2.BlockID, uint32(i))
				ids[i] = rowID
			}
			page.Train(transferMap)
			tae.Runtime.TransferTable.AddPage(page)

			name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
			ioVector := fileservice.IOVector{
				FilePath: name.String(),
			}
			offset := int64(0)
			data := page.Marshal()
			ioEntry := fileservice.IOEntry{
				Offset: offset,
				Size:   int64(len(data)),
				Data:   data,
			}
			ioVector.Entries = append(ioVector.Entries, ioEntry)

			err := tae.Runtime.Fs.Service.Write(context.Background(), ioVector)
			if err != nil {
				return
			}

			path := model.Path{
				Name:   ioVector.FilePath,
				Offset: 0,
				Size:   int64(len(data)),
			}
			page.SetPath(path)

			time.Sleep(2 * time.Second)
			tae.Runtime.TransferTable.RunTTL()
			_, err = tae.Runtime.TransferTable.Pin(*page.ID())
			assert.True(t, errors.Is(err, moerr.GetOkExpectedEOB()))
		},
	)
}

func TestTryDeleteByDeltaloc1(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	rows := 100
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 10
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, rows)
	defer bat.Close()
	tae.CreateRelAndAppend(bat, true)
	tae.CompactBlocks(true)

	// apply deleteloc fails on ablk
	txn, _ := tae.StartTxn(nil)
	v1 := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(1)
	ok, err := tae.TryDeleteByDeltalocWithTxn([]any{v1}, txn)
	assert.NoError(t, err)
	assert.True(t, ok)
	tae.MergeBlocks(true)
	t.Log(tae.Catalog.SimplePPString(3))

	err = txn.Commit(ctx)
	assert.NoError(t, err)

	tae.CheckRowsByScan(rows-1, true)
	t.Log(tae.Catalog.SimplePPString(3))
}

func TestTryDeleteByDeltaloc2(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	rows := 100
	schema := catalog.MockSchemaAll(2, 1)
	schema.BlockMaxRows = 10
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, rows)
	defer bat.Close()
	tae.CreateRelAndAppend(bat, true)
	tae.CompactBlocks(true)

	// apply deleteloc fails on ablk
	txn, rel := tae.GetRelation()
	v1 := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(1)
	filter := handle.NewEQFilter(v1)
	id, offset, err := rel.GetByFilter(ctx, filter)
	assert.NoError(t, err)
	obj, err := rel.GetMeta().(*catalog.TableEntry).GetObjectByID(id.ObjectID(), false)
	assert.NoError(t, err)
	_, blkOffset := id.BlockID.Offsets()
	deltaLoc, err := testutil.MockCNDeleteInS3(tae.Runtime.Fs, obj.GetObjectData(), blkOffset, schema, txn, []uint32{offset})
	assert.NoError(t, err)

	tae.MergeBlocks(true)
	t.Log(tae.Catalog.SimplePPString(3))

	ok, err := rel.TryDeleteByDeltaloc(id, deltaLoc)
	assert.NoError(t, err)
	assert.NoError(t, err)
	assert.True(t, ok)

	err = txn.Commit(ctx)
	assert.NoError(t, err)

	tae.CheckRowsByScan(rows-1, true)
	t.Log(tae.Catalog.SimplePPString(3))
}

func TestMergeBlocks4(t *testing.T) {
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(1, 0)
	schema.BlockMaxRows = 8
	schema.ObjectMaxBlocks = 5
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 10)
	defer bat.Close()
	tae.CreateRelAndAppend(bat, true)
	tae.CompactBlocks(true)

	txn, rel := tae.GetRelation()
	obj := testutil.GetOneBlockMeta(rel)
	task, err := jobs.NewMergeObjectsTask(nil, txn, []*catalog.ObjectEntry{obj}, tae.Runtime, 0, false)
	assert.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fault.Enable()
		defer fault.Disable()
		err := fault.AddFaultPoint(context.Background(), "tae: slow transfer deletes", ":::", "echo", 0, "mock flush timeout")
		assert.NoError(t, err)
		defer func() {
			err := fault.RemoveFaultPoint(context.Background(), "tae: slow transfer deletes")
			assert.NoError(t, err)
		}()
		tae.DeleteAll(true)
		tae.CompactBlocks(true)
		time.Sleep(time.Millisecond * 500)
		txn, rel := tae.GetRelation()
		obj := testutil.GetOneTombstoneMeta(rel)
		task, err := jobs.NewMergeObjectsTask(nil, txn, []*catalog.ObjectEntry{obj}, tae.Runtime, 0, true)
		assert.NoError(t, err)
		err = task.OnExec(context.Background())
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
		t.Log(tae.Catalog.SimplePPString(3))
	}()
	err = task.OnExec(context.Background())
	assert.NoError(t, err)
	wg.Wait()

	assert.NoError(t, txn.Commit(context.Background()))
	tae.CheckRowsByScan(0, true)
}
