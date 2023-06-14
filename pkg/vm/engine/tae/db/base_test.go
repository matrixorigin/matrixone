// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"

	checkpoint2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName    = "TAEDB"
	defaultTestDB = "db"
)

func init() {
	// workaround sca check
	_ = tryAppendClosure
	_ = dropDB
}

type testEngine struct {
	*DB
	t        *testing.T
	schema   *catalog.Schema
	tenantID uint32 // for almost tests, userID and roleID is not important
}

func newTestEngine(t *testing.T, opts *options.Options) *testEngine {
	blockio.Start()
	db := initDB(t, opts)
	return &testEngine{
		DB: db,
		t:  t,
	}
}

func (e *testEngine) bindSchema(schema *catalog.Schema) { e.schema = schema }

func (e *testEngine) bindTenantID(tenantID uint32) { e.tenantID = tenantID }

func (e *testEngine) restart() {
	_ = e.DB.Close()
	var err error
	e.DB, err = Open(e.Dir, e.Opts)
	// only ut executes this checker
	e.DB.DiskCleaner.AddChecker(
		func(item any) bool {
			min := e.DB.TxnMgr.MinTSForTest()
			checkpoint := item.(*checkpoint2.CheckpointEntry)
			//logutil.Infof("min: %v, checkpoint: %v", min.ToString(), checkpoint.GetStart().ToString())
			return !checkpoint.GetEnd().GreaterEq(min)
		})
	assert.NoError(e.t, err)
}

func (e *testEngine) Close() error {
	err := e.DB.Close()
	blockio.Stop()
	blockio.ResetPipeline()
	return err
}

func (e *testEngine) createRelAndAppend(bat *containers.Batch, createDB bool) (handle.Database, handle.Relation) {
	return createRelationAndAppend(e.t, e.tenantID, e.DB, defaultTestDB, e.schema, bat, createDB)
}

// func (e *testEngine) getRows() int {
// 	txn, rel := e.getRelation()
// 	rows := rel.Rows()
// 	assert.NoError(e.t, txn.Commit())
// 	return int(rows)
// }

func (e *testEngine) checkRowsByScan(exp int, applyDelete bool) {
	txn, rel := e.getRelation()
	checkAllColRowsByScan(e.t, rel, exp, applyDelete)
	assert.NoError(e.t, txn.Commit())
}
func (e *testEngine) dropRelation(t *testing.T) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	db, err := txn.GetDatabase(defaultTestDB)
	assert.NoError(t, err)
	_, err = db.DropRelationByName(e.schema.Name)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
}
func (e *testEngine) getRelation() (txn txnif.AsyncTxn, rel handle.Relation) {
	return getRelation(e.t, e.tenantID, e.DB, defaultTestDB, e.schema.Name)
}
func (e *testEngine) getRelationWithTxn(txn txnif.AsyncTxn) (rel handle.Relation) {
	return getRelationWithTxn(e.t, txn, defaultTestDB, e.schema.Name)
}

func (e *testEngine) compactBlocks(skipConflict bool) {
	compactBlocks(e.t, e.tenantID, e.DB, defaultTestDB, e.schema, skipConflict)
}

func (e *testEngine) mergeBlocks(skipConflict bool) {
	mergeBlocks(e.t, e.tenantID, e.DB, defaultTestDB, e.schema, skipConflict)
}

func (e *testEngine) getDB(name string) (txn txnif.AsyncTxn, db handle.Database) {
	txn, err := e.DB.StartTxn(nil)
	txn.BindAccessInfo(e.tenantID, 0, 0)
	assert.NoError(e.t, err)
	db, err = txn.GetDatabase(name)
	assert.NoError(e.t, err)
	return
}

func (e *testEngine) getTestDB() (txn txnif.AsyncTxn, db handle.Database) {
	return e.getDB(defaultTestDB)
}

func (e *testEngine) DoAppend(bat *containers.Batch) {
	txn, rel := e.getRelation()
	err := rel.Append(context.Background(), bat)
	assert.NoError(e.t, err)
	assert.NoError(e.t, txn.Commit())
}

func (e *testEngine) doAppendWithTxn(bat *containers.Batch, txn txnif.AsyncTxn, skipConflict bool) (err error) {
	rel := e.getRelationWithTxn(txn)
	err = rel.Append(context.Background(), bat)
	if !skipConflict {
		assert.NoError(e.t, err)
	}
	return
}

func (e *testEngine) tryAppend(bat *containers.Batch) {
	txn, err := e.DB.StartTxn(nil)
	txn.BindAccessInfo(e.tenantID, 0, 0)
	assert.NoError(e.t, err)
	db, err := txn.GetDatabase(defaultTestDB)
	assert.NoError(e.t, err)
	rel, err := db.GetRelationByName(e.schema.Name)
	if err != nil {
		_ = txn.Rollback()
		return
	}

	err = rel.Append(context.Background(), bat)
	if err != nil {
		_ = txn.Rollback()
		return
	}
	_ = txn.Commit()
}
func (e *testEngine) deleteAll(skipConflict bool) error {
	txn, rel := e.getRelation()
	it := rel.MakeBlockIt()
	for it.Valid() {
		blk := it.GetBlock()
		defer blk.Close()
		view, err := blk.GetColumnDataByName(context.Background(), catalog.PhyAddrColumnName)
		assert.NoError(e.t, err)
		defer view.Close()
		view.ApplyDeletes()
		err = rel.DeleteByPhyAddrKeys(view.GetData(), types.TS{})
		assert.NoError(e.t, err)
		it.Next()
	}
	// checkAllColRowsByScan(e.t, rel, 0, true)
	err := txn.Commit()
	if !skipConflict {
		checkAllColRowsByScan(e.t, rel, 0, true)
		assert.NoError(e.t, err)
	}
	return err
}

func (e *testEngine) truncate() {
	txn, db := e.getTestDB()
	_, err := db.TruncateByName(e.schema.Name)
	assert.NoError(e.t, err)
	assert.NoError(e.t, txn.Commit())
}
func (e *testEngine) globalCheckpoint(
	endTs types.TS,
	versionInterval time.Duration,
	enableAndCleanBGCheckpoint bool,
) error {
	if enableAndCleanBGCheckpoint {
		e.DB.BGCheckpointRunner.DisableCheckpoint()
		defer e.DB.BGCheckpointRunner.EnableCheckpoint()
		e.DB.BGCheckpointRunner.CleanPenddingCheckpoint()
	}
	if e.DB.BGCheckpointRunner.GetPenddingIncrementalCount() == 0 {
		testutils.WaitExpect(4000, func() bool {
			flushed := e.DB.BGCheckpointRunner.IsAllChangesFlushed(types.TS{}, endTs, false)
			return flushed
		})
		flushed := e.DB.BGCheckpointRunner.IsAllChangesFlushed(types.TS{}, endTs, true)
		assert.True(e.t, flushed)
	}
	err := e.DB.BGCheckpointRunner.ForceGlobalCheckpoint(endTs, versionInterval)
	assert.NoError(e.t, err)
	return nil
}

func (e *testEngine) incrementalCheckpoint(
	end types.TS,
	enableAndCleanBGCheckpoint bool,
	waitFlush bool,
	truncate bool,
) error {
	if enableAndCleanBGCheckpoint {
		e.DB.BGCheckpointRunner.DisableCheckpoint()
		defer e.DB.BGCheckpointRunner.EnableCheckpoint()
		e.DB.BGCheckpointRunner.CleanPenddingCheckpoint()
	}
	if waitFlush {
		testutils.WaitExpect(4000, func() bool {
			flushed := e.DB.BGCheckpointRunner.IsAllChangesFlushed(types.TS{}, end, false)
			return flushed
		})
		flushed := e.DB.BGCheckpointRunner.IsAllChangesFlushed(types.TS{}, end, true)
		assert.True(e.t, flushed)
	}
	err := e.DB.BGCheckpointRunner.ForceIncrementalCheckpoint(end)
	assert.NoError(e.t, err)
	if truncate {
		lsn := e.DB.BGCheckpointRunner.MaxLSNInRange(end)
		entry, err := e.DB.Wal.RangeCheckpoint(1, lsn)
		assert.NoError(e.t, err)
		assert.NoError(e.t, entry.WaitDone())
		testutils.WaitExpect(1000, func() bool {
			return e.Scheduler.GetPenddingLSNCnt() == 0
		})
	}
	return nil
}
func initDB(t *testing.T, opts *options.Options) *DB {
	dir := testutils.InitTestEnv(ModuleName, t)
	db, _ := Open(dir, opts)
	// only ut executes this checker
	db.DiskCleaner.AddChecker(
		func(item any) bool {
			min := db.TxnMgr.MinTSForTest()
			checkpoint := item.(*checkpoint2.CheckpointEntry)
			//logutil.Infof("min: %v, checkpoint: %v", min.ToString(), checkpoint.GetStart().ToString())
			return !checkpoint.GetEnd().GreaterEq(min)
		})
	return db
}

func withTestAllPKType(t *testing.T, tae *DB, test func(*testing.T, *DB, *catalog.Schema)) {
	var wg sync.WaitGroup
	pool, _ := ants.NewPool(100)
	defer pool.Release()
	for i := 0; i < 17; i++ {
		schema := catalog.MockSchemaAll(18, i)
		schema.BlockMaxRows = 10
		schema.SegmentMaxBlocks = 2
		wg.Add(1)
		_ = pool.Submit(func() {
			defer wg.Done()
			test(t, tae, schema)
		})
	}
	wg.Wait()
}

func lenOfBats(bats []*containers.Batch) int {
	rows := 0
	for _, bat := range bats {
		rows += bat.Length()
	}
	return rows
}

func printCheckpointStats(t *testing.T, tae *DB) {
	t.Logf("GetCheckpointedLSN: %d", tae.Wal.GetCheckpointed())
	t.Logf("GetPenddingLSNCnt: %d", tae.Wal.GetPenddingCnt())
	t.Logf("GetCurrSeqNum: %d", tae.Wal.GetCurrSeqNum())
}

func createDB(t *testing.T, e *DB, dbName string) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	_, err = txn.CreateDatabase(dbName, "", "")
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
}

func dropDB(t *testing.T, e *DB, dbName string) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	_, err = txn.DropDatabase(dbName)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
}

func createRelation(t *testing.T, e *DB, dbName string, schema *catalog.Schema, createDB bool) (db handle.Database, rel handle.Relation) {
	txn, db, rel := createRelationNoCommit(t, e, dbName, schema, createDB)
	assert.NoError(t, txn.Commit())
	return
}

func createRelationNoCommit(t *testing.T, e *DB, dbName string, schema *catalog.Schema, createDB bool) (txn txnif.AsyncTxn, db handle.Database, rel handle.Relation) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	if createDB {
		db, err = txn.CreateDatabase(dbName, "", "")
		assert.NoError(t, err)
	} else {
		db, err = txn.GetDatabase(dbName)
		assert.NoError(t, err)
	}
	rel, err = db.CreateRelation(schema)
	assert.NoError(t, err)
	return
}

func createRelationAndAppend(
	t *testing.T,
	tenantID uint32,
	e *DB,
	dbName string,
	schema *catalog.Schema,
	bat *containers.Batch,
	createDB bool) (db handle.Database, rel handle.Relation) {
	txn, err := e.StartTxn(nil)
	txn.BindAccessInfo(tenantID, 0, 0)
	assert.NoError(t, err)
	if createDB {
		db, err = txn.CreateDatabase(dbName, "", "")
		assert.NoError(t, err)
	} else {
		db, err = txn.GetDatabase(dbName)
		assert.NoError(t, err)
	}
	rel, err = db.CreateRelation(schema)
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bat)
	assert.NoError(t, err)
	assert.Nil(t, txn.Commit())
	return
}

func getRelation(t *testing.T, tenantID uint32, e *DB, dbName, tblName string) (txn txnif.AsyncTxn, rel handle.Relation) {
	txn, err := e.StartTxn(nil)
	txn.BindAccessInfo(tenantID, 0, 0)
	assert.NoError(t, err)
	db, err := txn.GetDatabase(dbName)
	assert.NoError(t, err)
	rel, err = db.GetRelationByName(tblName)
	assert.NoError(t, err)
	return
}

func getRelationWithTxn(t *testing.T, txn txnif.AsyncTxn, dbName, tblName string) (rel handle.Relation) {
	db, err := txn.GetDatabase(dbName)
	assert.NoError(t, err)
	rel, err = db.GetRelationByName(tblName)
	assert.NoError(t, err)
	return
}

func getDefaultRelation(t *testing.T, e *DB, name string) (txn txnif.AsyncTxn, rel handle.Relation) {
	return getRelation(t, 0, e, defaultTestDB, name)
}

func getOneBlock(rel handle.Relation) handle.Block {
	it := rel.MakeBlockIt()
	return it.GetBlock()
}

func getOneBlockMeta(rel handle.Relation) *catalog.BlockEntry {
	it := rel.MakeBlockIt()
	return it.GetBlock().GetMeta().(*catalog.BlockEntry)
}

func checkAllColRowsByScan(t *testing.T, rel handle.Relation, expectRows int, applyDelete bool) {
	schema := rel.Schema().(*catalog.Schema)
	for _, def := range schema.ColDefs {
		rows := getColumnRowsByScan(t, rel, def.Idx, applyDelete)
		assert.Equal(t, expectRows, rows)
	}
}

func getColumnRowsByScan(t *testing.T, rel handle.Relation, colIdx int, applyDelete bool) int {
	rows := 0
	forEachColumnView(rel, colIdx, func(view *model.ColumnView) (err error) {
		if applyDelete {
			view.ApplyDeletes()
		}
		rows += view.Length()
		// t.Log(view.String())
		return
	})
	return rows
}

func forEachColumnView(rel handle.Relation, colIdx int, fn func(view *model.ColumnView) error) {
	forEachBlock(rel, func(blk handle.Block) (err error) {
		view, err := blk.GetColumnDataById(context.Background(), colIdx)
		if view == nil {
			logutil.Warnf("blk %v", blk.String())
			return
		}
		if err != nil {
			return
		}
		defer view.Close()
		err = fn(view)
		return
	})
}

func forEachBlock(rel handle.Relation, fn func(blk handle.Block) error) {
	it := rel.MakeBlockIt()
	var err error
	for it.Valid() {
		blk := it.GetBlock()
		defer blk.Close()
		if err = fn(blk); err != nil {
			if errors.Is(err, handle.ErrIteratorEnd) {
				return
			} else {
				panic(err)
			}
		}
		it.Next()
	}
}

func forEachSegment(rel handle.Relation, fn func(seg handle.Segment) error) {
	it := rel.MakeSegmentIt()
	var err error
	for it.Valid() {
		seg := it.GetSegment()
		defer seg.Close()
		if err = fn(seg); err != nil {
			if errors.Is(err, handle.ErrIteratorEnd) {
				return
			} else {
				panic(err)
			}
		}
		it.Next()
	}
}

func appendFailClosure(t *testing.T, data *containers.Batch, name string, e *DB, wg *sync.WaitGroup) func() {
	return func() {
		if wg != nil {
			defer wg.Done()
		}
		txn, _ := e.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(name)
		err := rel.Append(context.Background(), data)
		assert.NotNil(t, err)
		assert.Nil(t, txn.Rollback())
	}
}

func appendClosure(t *testing.T, data *containers.Batch, name string, e *DB, wg *sync.WaitGroup) func() {
	return func() {
		if wg != nil {
			defer wg.Done()
		}
		txn, _ := e.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(name)
		err := rel.Append(context.Background(), data)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
}

func tryAppendClosure(t *testing.T, data *containers.Batch, name string, e *DB, wg *sync.WaitGroup) func() {
	return func() {
		if wg != nil {
			defer wg.Done()
		}
		txn, _ := e.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, err := database.GetRelationByName(name)
		if err != nil {
			_ = txn.Rollback()
			return
		}
		if err = rel.Append(context.Background(), data); err != nil {
			_ = txn.Rollback()
			return
		}
		_ = txn.Commit()
	}
}

func compactBlocks(t *testing.T, tenantID uint32, e *DB, dbName string, schema *catalog.Schema, skipConflict bool) {
	txn, rel := getRelation(t, tenantID, e, dbName, schema.Name)

	var metas []*catalog.BlockEntry
	it := rel.MakeBlockIt()
	for it.Valid() {
		blk := it.GetBlock()
		meta := blk.GetMeta().(*catalog.BlockEntry)
		if blk.Rows() < int(schema.BlockMaxRows) {
			it.Next()
			continue
		}
		metas = append(metas, meta)
		it.Next()
	}
	_ = txn.Commit()
	for _, meta := range metas {
		txn, _ := getRelation(t, tenantID, e, dbName, schema.Name)
		task, err := jobs.NewCompactBlockTask(nil, txn, meta, e.Scheduler)
		if skipConflict && err != nil {
			_ = txn.Rollback()
			continue
		}
		assert.NoError(t, err)
		err = task.OnExec(context.Background())
		if skipConflict {
			if err != nil {
				_ = txn.Rollback()
			} else {
				_ = txn.Commit()
			}
		} else {
			assert.NoError(t, err)
			assert.NoError(t, txn.Commit())
		}
	}
}

func mergeBlocks(t *testing.T, tenantID uint32, e *DB, dbName string, schema *catalog.Schema, skipConflict bool) {
	txn, _ := e.StartTxn(nil)
	txn.BindAccessInfo(tenantID, 0, 0)
	db, _ := txn.GetDatabase(dbName)
	rel, _ := db.GetRelationByName(schema.Name)

	var segs []*catalog.SegmentEntry
	segIt := rel.MakeSegmentIt()
	for segIt.Valid() {
		seg := segIt.GetSegment().GetMeta().(*catalog.SegmentEntry)
		if seg.GetAppendableBlockCnt() == int(seg.GetTable().GetLastestSchema().SegmentMaxBlocks) {
			segs = append(segs, seg)
		}
		segIt.Next()
	}
	_ = txn.Commit()
	for _, seg := range segs {
		txn, _ = e.StartTxn(nil)
		txn.BindAccessInfo(tenantID, 0, 0)
		db, _ = txn.GetDatabase(dbName)
		rel, _ = db.GetRelationByName(schema.Name)
		segHandle, err := rel.GetSegment(&seg.ID)
		if err != nil {
			if skipConflict {
				_ = txn.Rollback()
				continue
			}
			assert.NoErrorf(t, err, "Txn Ts=%d", txn.GetStartTS())
		}
		var metas []*catalog.BlockEntry
		it := segHandle.MakeBlockIt()
		for it.Valid() {
			meta := it.GetBlock().GetMeta().(*catalog.BlockEntry)
			metas = append(metas, meta)
			it.Next()
		}
		segsToMerge := []*catalog.SegmentEntry{segHandle.GetMeta().(*catalog.SegmentEntry)}
		task, err := jobs.NewMergeBlocksTask(nil, txn, metas, segsToMerge, nil, e.Scheduler)
		if skipConflict && err != nil {
			_ = txn.Rollback()
			continue
		}
		assert.NoError(t, err)
		err = task.OnExec(context.Background())
		if skipConflict {
			if err != nil {
				_ = txn.Rollback()
			} else {
				_ = txn.Commit()
			}
		} else {
			assert.NoError(t, err)
			assert.NoError(t, txn.Commit())
		}
	}
}

/*func compactSegs(t *testing.T, e *DB, schema *catalog.Schema) {
	txn, rel := getDefaultRelation(t, e, schema.Name)
	segs := make([]*catalog.SegmentEntry, 0)
	it := rel.MakeSegmentIt()
	for it.Valid() {
		seg := it.GetSegment().GetMeta().(*catalog.SegmentEntry)
		segs = append(segs, seg)
		it.Next()
	}
	for _, segMeta := range segs {
		seg := segMeta.GetSegmentData()
		factory, taskType, scopes, err := seg.BuildCompactionTaskFactory()
		assert.NoError(t, err)
		if factory == nil {
			continue
		}
		task, err := e.Scheduler.ScheduleMultiScopedTxnTask(tasks.WaitableCtx, taskType, scopes, factory)
		assert.NoError(t, err)
		err = task.WaitDone()
		assert.NoError(t, err)
	}
	assert.NoError(t, txn.Commit())
}*/

func getSingleSortKeyValue(bat *containers.Batch, schema *catalog.Schema, row int) (v any) {
	v = bat.Vecs[schema.GetSingleSortKeyIdx()].Get(row)
	return
}
