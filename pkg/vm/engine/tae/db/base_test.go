package db

import (
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/mockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "TAEDB"
)

func initDB(t *testing.T, opts *options.Options) *DB {
	mockio.ResetFS()
	dir := testutils.InitTestEnv(ModuleName, t)
	db, _ := Open(dir, opts)
	return db
}

func getSegmentFileNames(dir string) (names map[uint64]string) {
	names = make(map[uint64]string)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		name := f.Name()
		segName := strings.TrimSuffix(name, ".seg")
		if segName == name {
			continue
		}
		id, err := strconv.ParseUint(segName, 10, 64)
		if err != nil {
			panic(err)
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
	assert.NoError(t, txn.Commit())
	return
}

func createRelationAndAppend(
	t *testing.T,
	e *DB,
	dbName string,
	schema *catalog.Schema,
	bat *batch.Batch,
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
