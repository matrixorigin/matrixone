package db

import (
	"errors"
	"io/ioutil"
	"sync"
	"testing"

	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
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
	files, err := ioutil.ReadDir(e.Dir)
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

func printCheckpointStats(t *testing.T, tae *DB) {
	t.Logf("GetCheckpointedLSN: %d", tae.Wal.GetCheckpointed())
	t.Logf("GetPenddingLSNCnt: %d", tae.Wal.GetPenddingCnt())
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
	bat *gbat.Batch,
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
		view, err := blk.GetColumnDataById(colIdx, nil, nil)
		if err != nil {
			return
		}
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

func appendFailClosure(t *testing.T, data *gbat.Batch, name string, e *DB, wg *sync.WaitGroup) func() {
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

func appendClosure(t *testing.T, data *gbat.Batch, name string, e *DB, wg *sync.WaitGroup) func() {
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

func tryAppendClosure(t *testing.T, data *gbat.Batch, name string, e *DB, wg *sync.WaitGroup) func() {
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

func compactBlocks(t *testing.T, e *DB, dbName string, schema *catalog.Schema, skipConflict bool) {
	txn, _ := e.StartTxn(nil)
	db, _ := txn.GetDatabase(dbName)
	rel, _ := db.GetRelationByName(schema.Name)

	var metas []*catalog.BlockEntry
	it := rel.MakeBlockIt()
	for it.Valid() {
		blk := it.GetBlock()
		if blk.Rows() < int(schema.BlockMaxRows) {
			it.Next()
			continue
		}
		meta := blk.GetMeta().(*catalog.BlockEntry)
		metas = append(metas, meta)
		it.Next()
	}
	_ = txn.Commit()
	for _, meta := range metas {
		txn, _ = e.StartTxn(nil)
		db, _ = txn.GetDatabase(dbName)
		rel, _ = db.GetRelationByName(schema.Name)
		task, err := jobs.NewCompactBlockTask(nil, txn, meta, e.Scheduler)
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
