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

package testutil

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func WithTestAllPKType(t *testing.T, tae *db.DB, test func(*testing.T, *db.DB, *catalog.Schema)) {
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

func LenOfBats(bats []*containers.Batch) int {
	rows := 0
	for _, bat := range bats {
		rows += bat.Length()
	}
	return rows
}

func PrintCheckpointStats(t *testing.T, tae *db.DB) {
	t.Logf("GetCheckpointedLSN: %d", tae.Wal.GetCheckpointed())
	t.Logf("GetPenddingLSNCnt: %d", tae.Wal.GetPenddingCnt())
	t.Logf("GetCurrSeqNum: %d", tae.Wal.GetCurrSeqNum())
}

func CreateDB(t *testing.T, e *db.DB, dbName string) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	_, err = txn.CreateDatabase(dbName, "", "")
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
}

func DropDB(t *testing.T, e *db.DB, dbName string) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	_, err = txn.DropDatabase(dbName)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
}

func CreateRelation(t *testing.T, e *db.DB, dbName string, schema *catalog.Schema, createDB bool) (db handle.Database, rel handle.Relation) {
	txn, db, rel := CreateRelationNoCommit(t, e, dbName, schema, createDB)
	assert.NoError(t, txn.Commit(context.Background()))
	return
}

func CreateRelationNoCommit(t *testing.T, e *db.DB, dbName string, schema *catalog.Schema, createDB bool) (txn txnif.AsyncTxn, db handle.Database, rel handle.Relation) {
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

func CreateRelationAndAppend(
	t *testing.T,
	tenantID uint32,
	e *db.DB,
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
	assert.Nil(t, txn.Commit(context.Background()))
	return
}

func GetRelation(t *testing.T, tenantID uint32, e *db.DB, dbName, tblName string) (txn txnif.AsyncTxn, rel handle.Relation) {
	txn, err := e.StartTxn(nil)
	txn.BindAccessInfo(tenantID, 0, 0)
	assert.NoError(t, err)
	db, err := txn.GetDatabase(dbName)
	assert.NoError(t, err)
	rel, err = db.GetRelationByName(tblName)
	assert.NoError(t, err)
	return
}

func GetRelationWithTxn(t *testing.T, txn txnif.AsyncTxn, dbName, tblName string) (rel handle.Relation) {
	db, err := txn.GetDatabase(dbName)
	assert.NoError(t, err)
	rel, err = db.GetRelationByName(tblName)
	assert.NoError(t, err)
	return
}

func GetDefaultRelation(t *testing.T, e *db.DB, name string) (txn txnif.AsyncTxn, rel handle.Relation) {
	return GetRelation(t, 0, e, DefaultTestDB, name)
}

func GetOneBlock(rel handle.Relation) handle.Block {
	it := rel.MakeBlockIt()
	return it.GetBlock()
}

func GetOneBlockMeta(rel handle.Relation) *catalog.BlockEntry {
	it := rel.MakeBlockIt()
	return it.GetBlock().GetMeta().(*catalog.BlockEntry)
}

func CheckAllColRowsByScan(t *testing.T, rel handle.Relation, expectRows int, applyDelete bool) {
	schema := rel.Schema().(*catalog.Schema)
	for _, def := range schema.ColDefs {
		rows := GetColumnRowsByScan(t, rel, def.Idx, applyDelete)
		assert.Equal(t, expectRows, rows)
	}
}

func GetColumnRowsByScan(t *testing.T, rel handle.Relation, colIdx int, applyDelete bool) int {
	rows := 0
	ForEachColumnView(rel, colIdx, func(view *containers.ColumnView) (err error) {
		if applyDelete {
			view.ApplyDeletes()
		}
		rows += view.Length()
		// t.Log(view.String())
		return
	})
	return rows
}

func ForEachColumnView(rel handle.Relation, colIdx int, fn func(view *containers.ColumnView) error) {
	ForEachBlock(rel, func(blk handle.Block) (err error) {
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

func ForEachBlock(rel handle.Relation, fn func(blk handle.Block) error) {
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

func ForEachSegment(rel handle.Relation, fn func(seg handle.Segment) error) {
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

func AppendFailClosure(t *testing.T, data *containers.Batch, name string, e *db.DB, wg *sync.WaitGroup) func() {
	return func() {
		if wg != nil {
			defer wg.Done()
		}
		txn, _ := e.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(name)
		err := rel.Append(context.Background(), data)
		assert.NotNil(t, err)
		assert.Nil(t, txn.Rollback(context.Background()))
	}
}

func AppendClosure(t *testing.T, data *containers.Batch, name string, e *db.DB, wg *sync.WaitGroup) func() {
	return func() {
		if wg != nil {
			defer wg.Done()
		}
		txn, _ := e.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(name)
		err := rel.Append(context.Background(), data)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
}

func CompactBlocks(t *testing.T, tenantID uint32, e *db.DB, dbName string, schema *catalog.Schema, skipConflict bool) {
	txn, rel := GetRelation(t, tenantID, e, dbName, schema.Name)

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
	_ = txn.Commit(context.Background())
	for _, meta := range metas {
		txn, _ := GetRelation(t, tenantID, e, dbName, schema.Name)
		task, err := jobs.NewCompactBlockTask(nil, txn, meta, e.Runtime)
		if skipConflict && err != nil {
			_ = txn.Rollback(context.Background())
			continue
		}
		assert.NoError(t, err)
		err = task.OnExec(context.Background())
		if skipConflict {
			if err != nil {
				_ = txn.Rollback(context.Background())
			} else {
				_ = txn.Commit(context.Background())
			}
		} else {
			assert.NoError(t, err)
			assert.NoError(t, txn.Commit(context.Background()))
		}
	}
}

func MergeBlocks(t *testing.T, tenantID uint32, e *db.DB, dbName string, schema *catalog.Schema, skipConflict bool) {
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
	_ = txn.Commit(context.Background())
	for _, seg := range segs {
		txn, _ = e.StartTxn(nil)
		txn.BindAccessInfo(tenantID, 0, 0)
		db, _ = txn.GetDatabase(dbName)
		rel, _ = db.GetRelationByName(schema.Name)
		segHandle, err := rel.GetSegment(&seg.ID)
		if err != nil {
			if skipConflict {
				_ = txn.Rollback(context.Background())
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
		task, err := jobs.NewMergeBlocksTask(nil, txn, metas, segsToMerge, nil, e.Runtime)
		if skipConflict && err != nil {
			_ = txn.Rollback(context.Background())
			continue
		}
		assert.NoError(t, err)
		err = task.OnExec(context.Background())
		if skipConflict {
			if err != nil {
				_ = txn.Rollback(context.Background())
			} else {
				_ = txn.Commit(context.Background())
			}
		} else {
			assert.NoError(t, err)
			assert.NoError(t, txn.Commit(context.Background()))
		}
	}
}

func GetSingleSortKeyValue(bat *containers.Batch, schema *catalog.Schema, row int) (v any) {
	v = bat.Vecs[schema.GetSingleSortKeyIdx()].Get(row)
	return
}

func MockCNDeleteInS3(
	fs *objectio.ObjectFS,
	blk data.Block,
	schema *catalog.Schema,
	txn txnif.AsyncTxn,
	deleteRows []uint32,
) (location objectio.Location, err error) {
	pkDef := schema.GetPrimaryKey()
	view, err := blk.GetColumnDataById(context.Background(), txn, schema, pkDef.Idx)
	pkVec := containers.MakeVector(pkDef.Type)
	rowIDVec := containers.MakeVector(types.T_Rowid.ToType())
	blkID := &blk.GetMeta().(*catalog.BlockEntry).ID
	if err != nil {
		return
	}
	for _, row := range deleteRows {
		pkVal := view.GetData().Get(int(row))
		pkVec.Append(pkVal, false)
		rowID := objectio.NewRowid(blkID, row)
		rowIDVec.Append(*rowID, false)
	}
	bat := containers.NewBatch()
	bat.AddVector(catalog.AttrRowID, rowIDVec)
	bat.AddVector("pk", pkVec)
	name := objectio.MockObjectName()
	writer, err := blockio.NewBlockWriterNew(fs.Service, name, 0, nil)
	if err != nil {
		return
	}
	_, err = writer.WriteBatchWithOutIndex(containers.ToCNBatch(bat))
	if err != nil {
		return
	}
	blks, _, err := writer.Sync(context.Background())
	location = blockio.EncodeLocation(name, blks[0].GetExtent(), uint32(bat.Length()), blks[0].GetID())
	return
}
