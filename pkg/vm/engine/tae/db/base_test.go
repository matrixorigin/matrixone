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
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/mockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName    = "TAEDB"
	defaultTestDB = "db"
)

type testEngine struct {
	*DB
	t      *testing.T
	schema *catalog.Schema
}

func newTestEngine(t *testing.T, opts *options.Options) *testEngine {
	db := initDB(t, opts)
	return &testEngine{
		DB: db,
		t:  t,
	}
}

func (e *testEngine) bindSchema(schema *catalog.Schema) { e.schema = schema }

func (e *testEngine) restart() {
	_ = e.DB.Close()
	var err error
	e.DB, err = Open(e.Dir, e.Opts)
	assert.NoError(e.t, err)
}

func (e *testEngine) Close() error {
	return e.DB.Close()
}

func (e *testEngine) createRelAndAppend(bat *containers.Batch, createDB bool) (handle.Database, handle.Relation) {
	return createRelationAndAppend(e.t, e.DB, defaultTestDB, e.schema, bat, createDB)
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

func (e *testEngine) getRelation() (txn txnif.AsyncTxn, rel handle.Relation) {
	return getDefaultRelation(e.t, e.DB, e.schema.Name)
}

func (e *testEngine) checkpointCatalog() {
	err := e.DB.Catalog.Checkpoint(e.DB.TxnMgr.StatSafeTS())
	assert.NoError(e.t, err)
}

func (e *testEngine) compactABlocks(skipConflict bool) {
	forceCompactABlocks(e.t, e.DB, defaultTestDB, e.schema, skipConflict)
}

func (e *testEngine) compactBlocks(skipConflict bool) {
	compactBlocks(e.t, e.DB, defaultTestDB, e.schema, skipConflict)
}

func (e *testEngine) mergeBlocks(skipConflict bool) {
	mergeBlocks(e.t, e.DB, defaultTestDB, e.schema, skipConflict)
}

func (e *testEngine) getDB() (txn txnif.AsyncTxn, db handle.Database) {
	txn, err := e.DB.StartTxn(nil)
	assert.NoError(e.t, err)
	db, err = txn.GetDatabase(defaultTestDB)
	assert.NoError(e.t, err)
	return
}

func (e *testEngine) doAppend(bat *containers.Batch) {
	txn, rel := e.getRelation()
	err := rel.Append(bat)
	assert.NoError(e.t, err)
	assert.NoError(e.t, txn.Commit())
}

func (e *testEngine) tryAppend(bat *containers.Batch) {
	txn, err := e.DB.StartTxn(nil)
	assert.NoError(e.t, err)
	db, err := txn.GetDatabase(defaultTestDB)
	assert.NoError(e.t, err)
	rel, err := db.GetRelationByName(e.schema.Name)
	if err != nil {
		_ = txn.Rollback()
		return
	}

	err = rel.Append(bat)
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
		view, err := blk.GetColumnDataByName(catalog.HiddenColumnName, nil)
		assert.NoError(e.t, err)
		defer view.Close()
		view.ApplyDeletes()
		err = rel.DeleteByHiddenKeys(view.GetData())
		assert.NoError(e.t, err)
		it.Next()
	}
	checkAllColRowsByScan(e.t, rel, 0, true)
	err := txn.Commit()
	if !skipConflict {
		assert.NoError(e.t, err)
	}
	return err
}

func (e *testEngine) truncate() {
	txn, db := e.getDB()
	_, err := db.TruncateByName(e.schema.Name)
	assert.NoError(e.t, err)
	assert.NoError(e.t, txn.Commit())
}

func initDB(t *testing.T, opts *options.Options) *DB {
	mockio.ResetFS()
	dir := testutils.InitTestEnv(ModuleName, t)
	db, _ := Open(dir, opts)
	return db
}

func withTestAllPKType(t *testing.T, tae *DB, test func(*testing.T, *DB, *catalog.Schema)) {
	var wg sync.WaitGroup
	pool, _ := ants.NewPool(100)
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

func getSegmentFileNames(e *DB) (names map[uint64]string) {
	names = make(map[uint64]string)
	files, err := os.ReadDir(e.Dir)
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		name := f.Name()
		id, err := e.FileFactory.DecodeName(name)
		if err != nil {
			continue
		}
		names[id] = name
	}
	return
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
	_, err = txn.CreateDatabase(dbName)
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

func dropRelation(t *testing.T, e *DB, dbName, name string) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	db, err := txn.GetDatabase(dbName)
	assert.NoError(t, err)
	_, err = db.DropRelationByName(name)
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
		db, err = txn.CreateDatabase(dbName)
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
	e *DB,
	dbName string,
	schema *catalog.Schema,
	bat *containers.Batch,
	createDB bool) (db handle.Database, rel handle.Relation) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	if createDB {
		db, err = txn.CreateDatabase(dbName)
		assert.NoError(t, err)
	} else {
		db, err = txn.GetDatabase(dbName)
		assert.NoError(t, err)
	}
	rel, err = db.CreateRelation(schema)
	assert.NoError(t, err)
	err = rel.Append(bat)
	assert.NoError(t, err)
	assert.Nil(t, txn.Commit())
	return
}

func getRelation(t *testing.T, e *DB, dbName, tblName string) (txn txnif.AsyncTxn, rel handle.Relation) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	db, err := txn.GetDatabase(dbName)
	assert.NoError(t, err)
	rel, err = db.GetRelationByName(tblName)
	assert.NoError(t, err)
	return
}

func getDefaultRelation(t *testing.T, e *DB, name string) (txn txnif.AsyncTxn, rel handle.Relation) {
	return getRelation(t, e, defaultTestDB, name)
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
	schema := rel.GetMeta().(*catalog.TableEntry).GetSchema()
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
		view, err := blk.GetColumnDataById(colIdx, nil)
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
		if err = fn(it.GetBlock()); err != nil {
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
		err := rel.Append(data)
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
		err := rel.Append(data)
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
		if err = rel.Append(data); err != nil {
			_ = txn.Rollback()
			return
		}
		_ = txn.Commit()
	}
}
func forceCompactABlocks(t *testing.T, e *DB, dbName string, schema *catalog.Schema, skipConflict bool) {
	txn, rel := getRelation(t, e, dbName, schema.Name)

	var metas []*catalog.BlockEntry
	it := rel.MakeBlockIt()
	for it.Valid() {
		blk := it.GetBlock()
		meta := blk.GetMeta().(*catalog.BlockEntry)
		// if blk.Rows() >= int(schema.BlockMaxRows) {
		if !meta.IsAppendable() {
			it.Next()
			continue
		}
		metas = append(metas, meta)
		it.Next()
	}
	_ = txn.Commit()
	for _, meta := range metas {
		err := meta.GetBlockData().ForceCompact()
		if !skipConflict {
			assert.NoError(t, err)
		}
	}
}

func compactBlocks(t *testing.T, e *DB, dbName string, schema *catalog.Schema, skipConflict bool) {
	txn, rel := getRelation(t, e, dbName, schema.Name)

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
		txn, _ := getRelation(t, e, dbName, schema.Name)
		task, err := jobs.NewCompactBlockTask(nil, txn, meta, e.Scheduler)
		if skipConflict && err != nil {
			_ = txn.Rollback()
			continue
		}
		assert.NoError(t, err)
		err = task.OnExec()
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

func mergeBlocks(t *testing.T, e *DB, dbName string, schema *catalog.Schema, skipConflict bool) {
	txn, _ := e.StartTxn(nil)
	db, _ := txn.GetDatabase(dbName)
	rel, _ := db.GetRelationByName(schema.Name)

	var segs []*catalog.SegmentEntry
	segIt := rel.MakeSegmentIt()
	for segIt.Valid() {
		seg := segIt.GetSegment().GetMeta().(*catalog.SegmentEntry)
		if seg.GetAppendableBlockCnt() == int(seg.GetTable().GetSchema().SegmentMaxBlocks) {
			segs = append(segs, seg)
		}
		segIt.Next()
	}
	_ = txn.Commit()
	for _, seg := range segs {
		txn, _ = e.StartTxn(nil)
		db, _ = txn.GetDatabase(dbName)
		rel, _ = db.GetRelationByName(schema.Name)
		segHandle, err := rel.GetSegment(seg.ID)
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
		err = task.OnExec()
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

func compactSegs(t *testing.T, e *DB, schema *catalog.Schema) {
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
}

func getSingleSortKeyValue(bat *containers.Batch, schema *catalog.Schema, row int) (v any) {
	v = bat.Vecs[schema.GetSingleSortKeyIdx()].Get(row)
	return
}
