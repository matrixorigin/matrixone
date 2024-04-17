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
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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
		schema.ObjectMaxBlocks = 2
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

func GetOneObject(rel handle.Relation) handle.Object {
	it := rel.MakeObjectIt()
	return it.GetObject()
}

func GetOneBlockMeta(rel handle.Relation) *catalog.ObjectEntry {
	it := rel.MakeObjectIt()
	return it.GetObject().GetMeta().(*catalog.ObjectEntry)
}

func GetAllBlockMetas(rel handle.Relation) (metas []*catalog.ObjectEntry) {
	it := rel.MakeObjectIt()
	for ; it.Valid(); it.Next() {
		blk := it.GetObject()
		metas = append(metas, blk.GetMeta().(*catalog.ObjectEntry))
	}
	return
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
		fmt.Println(view.String())
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
	ForEachObject(rel, func(blk handle.Object) (err error) {
		blkCnt := blk.GetMeta().(*catalog.ObjectEntry).BlockCnt()
		for i := 0; i < blkCnt; i++ {
			view, err := blk.GetColumnDataById(context.Background(), uint16(i), colIdx, common.DefaultAllocator)
			if view == nil {
				logutil.Warnf("blk %v", blk.String())
				continue
			}
			if err != nil {
				return err
			}
			defer view.Close()
			err = fn(view)
			if err != nil {
				return err
			}
		}
		return
	})
}

func ForEachObject(rel handle.Relation, fn func(obj handle.Object) error) {
	it := rel.MakeObjectIt()
	var err error
	for it.Valid() {
		obj := it.GetObject()
		defer obj.Close()
		if err = fn(obj); err != nil {
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

	var metas []*catalog.ObjectEntry
	it := rel.MakeObjectIt()
	for it.Valid() {
		blk := it.GetObject()
		meta := blk.GetMeta().(*catalog.ObjectEntry)
		metas = append(metas, meta)
		it.Next()
	}
	_ = txn.Commit(context.Background())
	if len(metas) == 0 {
		return
	}
	txn, _ = GetRelation(t, tenantID, e, dbName, schema.Name)
	task, err := jobs.NewFlushTableTailTask(nil, txn, metas, e.Runtime, txn.GetStartTS())
	if skipConflict && err != nil {
		_ = txn.Rollback(context.Background())
		return
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

func MergeBlocks(t *testing.T, tenantID uint32, e *db.DB, dbName string, schema *catalog.Schema, skipConflict bool) {
	txn, _ := e.StartTxn(nil)
	txn.BindAccessInfo(tenantID, 0, 0)
	db, _ := txn.GetDatabase(dbName)
	rel, _ := db.GetRelationByName(schema.Name)

	var objs []*catalog.ObjectEntry
	objIt := rel.MakeObjectIt()
	for objIt.Valid() {
		obj := objIt.GetObject().GetMeta().(*catalog.ObjectEntry)
		if !obj.IsAppendable() {
			objs = append(objs, obj)
		}
		objIt.Next()
	}
	_ = txn.Commit(context.Background())
	metas := make([]*catalog.ObjectEntry, 0)
	for _, obj := range objs {
		txn, _ = e.StartTxn(nil)
		txn.BindAccessInfo(tenantID, 0, 0)
		db, _ = txn.GetDatabase(dbName)
		rel, _ = db.GetRelationByName(schema.Name)
		objHandle, err := rel.GetObject(&obj.ID)
		if err != nil {
			if skipConflict {
				continue
			} else {
				assert.NoErrorf(t, err, "Txn Ts=%d", txn.GetStartTS())
			}
		}
		metas = append(metas, objHandle.GetMeta().(*catalog.ObjectEntry))
	}
	task, err := jobs.NewMergeObjectsTask(nil, txn, metas, e.Runtime)
	if skipConflict && err != nil {
		_ = txn.Rollback(context.Background())
		return
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

func GetSingleSortKeyValue(bat *containers.Batch, schema *catalog.Schema, row int) (v any) {
	v = bat.Vecs[schema.GetSingleSortKeyIdx()].Get(row)
	return
}

func MockCNDeleteInS3(
	fs *objectio.ObjectFS,
	obj data.Object,
	blkOffset uint16,
	schema *catalog.Schema,
	txn txnif.AsyncTxn,
	deleteRows []uint32,
) (location objectio.Location, err error) {
	pkDef := schema.GetPrimaryKey()
	view, err := obj.GetColumnDataById(context.Background(), txn, schema, blkOffset, pkDef.Idx, common.DefaultAllocator)
	pkVec := containers.MakeVector(pkDef.Type, common.DefaultAllocator)
	rowIDVec := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
	objID := &obj.GetMeta().(*catalog.ObjectEntry).ID
	blkID := objectio.NewBlockidWithObjectID(objID, blkOffset)
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
	_, err = writer.WriteTombstoneBatch(containers.ToCNBatch(bat))
	if err != nil {
		return
	}
	blks, _, err := writer.Sync(context.Background())
	location = blockio.EncodeLocation(name, blks[0].GetExtent(), uint32(bat.Length()), blks[0].GetID())
	return
}
