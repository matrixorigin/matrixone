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

package moengine

import (
	"context"
	"testing"

	mobat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/mockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "TAEMOEngine"
)

func initDB(t *testing.T, opts *options.Options) *db.DB {
	mockio.ResetFS()
	dir := testutils.InitTestEnv(ModuleName, t)
	db, _ := db.Open(dir, opts)
	return db
}

func TestEngine(t *testing.T) {
	ctx := context.TODO()
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	e := NewEngine(tae)
	txn, err := e.StartTxn(nil)
	assert.Nil(t, err)
	err = e.Create(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	names, _ := e.Databases(ctx, engine.Snapshot(txn.GetCtx()))
	assert.Equal(t, 2, len(names))
	dbase, err := e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)

	schema := catalog.MockSchema(13, 12)
	defs, err := SchemaToDefs(schema)
	defs[5].(*engine.AttributeDef).Attr.Default = engine.MakeDefaultExpr(true, int32(3), false)
	defs[6].(*engine.AttributeDef).Attr.Default = engine.MakeDefaultExpr(true, nil, true)
	assert.NoError(t, err)
	err = dbase.Create(ctx, schema.Name, defs)
	assert.Nil(t, err)
	names, _ = dbase.Relations(ctx)
	assert.Equal(t, 1, len(names))

	rel, err := dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	rDefs, _ := rel.TableDefs(ctx)
	assert.Equal(t, 14, len(rDefs))
	rAttr := rDefs[5].(*engine.AttributeDef).Attr
	assert.Equal(t, int32(3), rAttr.Default.Value.(int32))
	rAttr = rDefs[6].(*engine.AttributeDef).Attr
	assert.Equal(t, true, rAttr.Default.IsNull)
	rAttr = rDefs[7].(*engine.AttributeDef).Attr
	assert.Equal(t, false, rAttr.Default.Exist)
	bat := catalog.MockBatch(schema, 100)
	defer bat.Close()

	newbat := mobat.New(true, bat.Attrs)
	newbat.Vecs = CopyToMoVectors(bat.Vecs)
	err = rel.Write(ctx, newbat)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	rel, err = dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	attr, _ := rel.GetPrimaryKeys(ctx)
	key := attr[0]
	bat = catalog.MockBatch(schema, 20)
	defer bat.Close()
	newbat = mobat.New(true, bat.Attrs)
	newbat.Vecs = CopyToMoVectors(bat.Vecs)
	err = rel.Delete(ctx, newbat.Vecs[12], key.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	rel, err = dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	readers, _ := rel.NewReader(ctx, 10, nil, nil)
	m := mheap.New(guest.New(1<<20, host.New(1<<20)))
	for _, reader := range readers {
		bat, err := reader.Read([]string{schema.ColDefs[1].Name}, nil, m)
		assert.Nil(t, err)
		if bat != nil {
			assert.Equal(t, 80, vector.Length(bat.Vecs[0]))
		}
	}
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestEngineAllType(t *testing.T) {
	ctx := context.TODO()
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	e := NewEngine(tae)
	txn, err := e.StartTxn(nil)
	assert.Nil(t, err)
	err = e.Create(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	names, _ := e.Databases(ctx, engine.Snapshot(txn.GetCtx()))
	assert.Equal(t, 2, len(names))
	dbase, err := e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)

	schema := catalog.MockSchemaAll(18, 12)
	defs, err := SchemaToDefs(schema)
	defs[5].(*engine.AttributeDef).Attr.Default = engine.MakeDefaultExpr(true, uint16(3), false)
	defs[6].(*engine.AttributeDef).Attr.Default = engine.MakeDefaultExpr(true, nil, true)
	assert.NoError(t, err)
	err = dbase.Create(ctx, schema.Name, defs)
	assert.Nil(t, err)
	names, _ = dbase.Relations(ctx)
	assert.Equal(t, 1, len(names))

	rel, err := dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	rDefs, _ := rel.TableDefs(ctx)
	assert.Equal(t, 19, len(rDefs))
	rAttr := rDefs[5].(*engine.AttributeDef).Attr
	assert.Equal(t, uint16(3), rAttr.Default.Value.(uint16))
	rAttr = rDefs[6].(*engine.AttributeDef).Attr
	assert.Equal(t, true, rAttr.Default.IsNull)
	basebat := catalog.MockBatch(schema, 100)
	defer basebat.Close()

	newbat := mobat.New(true, basebat.Attrs)
	newbat.Vecs = CopyToMoVectors(basebat.Vecs)
	err = rel.Write(ctx, newbat)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	rel, err = dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	attr, _ := rel.GetPrimaryKeys(ctx)
	key := attr[0]
	bat := catalog.MockBatch(schema, 20)
	defer bat.Close()
	newbat1 := mobat.New(true, bat.Attrs)
	newbat1.Vecs = CopyToMoVectors(bat.Vecs)
	err = rel.Delete(ctx, newbat1.Vecs[12], key.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	rel, err = dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	rows := rel.Rows()
	readers, _ := rel.NewReader(ctx, 10, nil, nil)
	m := mheap.New(guest.New(1<<20, host.New(1<<20)))
	for _, reader := range readers {
		bat, err := reader.Read(schema.Attrs(), nil, m)
		assert.Nil(t, err)
		if bat != nil {
			assert.Equal(t, 80, vector.Length(bat.Vecs[0]))
			vec := MOToVector(bat.Vecs[12], false)
			assert.Equal(t, vec.Get(0), basebat.Vecs[12].Get(20))
		}
	}
	delRows, err := rel.Truncate(ctx)
	assert.Nil(t, err)
	assert.Equal(t, rows, int64(delRows))
	assert.Nil(t, txn.Commit())
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	rel, err = dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	assert.Zero(t, rel.Rows())
	assert.Nil(t, txn.Commit())
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestTxnRelation_GetHideKey(t *testing.T) {
	ctx := context.TODO()
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	e := NewEngine(tae)
	txn, err := e.StartTxn(nil)
	assert.Nil(t, err)
	err = e.Create(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	names, _ := e.Databases(ctx, engine.Snapshot(txn.GetCtx()))
	assert.Equal(t, 2, len(names))
	dbase, err := e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)

	schema := catalog.MockSchema(13, 15)
	defs, err := SchemaToDefs(schema)
	assert.NoError(t, err)
	err = dbase.Create(ctx, schema.Name, defs)
	assert.Nil(t, err)
	names, _ = dbase.Relations(ctx)
	assert.Equal(t, 1, len(names))

	rel, err := dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	bat := catalog.MockBatch(schema, 100)
	defer bat.Close()

	newbat := mobat.New(true, bat.Attrs)
	newbat.Vecs = CopyToMoVectors(bat.Vecs)
	err = rel.Write(ctx, newbat)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	rel, err = dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	readers, _ := rel.NewReader(ctx, 10, nil, nil)
	delete := mobat.New(true, bat.Attrs)
	m := mheap.New(guest.New(1<<20, host.New(1<<20)))
	for _, reader := range readers {
		bat, err := reader.Read([]string{schema.ColDefs[13].Name}, nil, m)
		assert.Nil(t, err)
		if bat != nil {
			assert.Equal(t, 100, vector.Length(bat.Vecs[0]))
			delete = bat
		}
	}
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	rel, err = dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	attr, _ := rel.GetPrimaryKeys(ctx)
	assert.Nil(t, attr)
	keys, _ := rel.GetHideKeys(ctx)
	key := keys[0]
	err = rel.Delete(ctx, delete.Vecs[0], key.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	rel, err = dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	readers, _ = rel.NewReader(ctx, 1, nil, nil)
	m = mheap.New(guest.New(1<<20, host.New(1<<20)))
	for _, reader := range readers {
		bat, err := reader.Read([]string{schema.ColDefs[13].Name}, nil, m)
		assert.Nil(t, err)
		if bat != nil {
			assert.Equal(t, 0, vector.Length(bat.Vecs[0]))
		}
	}

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestTxnRelation_Update(t *testing.T) {
	ctx := context.TODO()
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	e := NewEngine(tae)
	txn, err := e.StartTxn(nil)
	assert.Nil(t, err)
	err = e.Create(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	names, _ := e.Databases(ctx, engine.Snapshot(txn.GetCtx()))
	assert.Equal(t, 2, len(names))
	dbase, err := e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)

	schema := catalog.MockSchema(13, 2)
	defs, err := SchemaToDefs(schema)
	assert.NoError(t, err)
	err = dbase.Create(ctx, schema.Name, defs)
	assert.Nil(t, err)
	dbase, err = e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	rel, err := dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	bat := catalog.MockBatch(schema, 4)
	defer bat.Close()

	newbat := mobat.New(true, bat.Attrs)
	newbat.Vecs = CopyToMoVectors(bat.Vecs)
	err = rel.Write(ctx, newbat)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	rel, err = dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	readers, _ := rel.NewReader(ctx, 10, nil, nil)
	update := newbat
	m := mheap.New(guest.New(1<<20, host.New(1<<20)))
	for _, reader := range readers {
		bat1, err := reader.Read([]string{schema.ColDefs[13].Name, schema.ColDefs[0].Name, schema.ColDefs[1].Name}, nil, m)
		assert.Nil(t, err)
		if bat1 != nil {
			assert.Equal(t, 4, vector.Length(bat1.Vecs[0]))
			update = bat1
		}
	}
	assert.Equal(t, newbat.Vecs[0].Col.([]int32)[0], update.Vecs[1].Col.([]int32)[0])
	assert.Equal(t, newbat.Vecs[0].Col.([]int32)[1], update.Vecs[1].Col.([]int32)[1])
	assert.Equal(t, newbat.Vecs[1].Col.([]int32)[0], update.Vecs[2].Col.([]int32)[0])
	assert.Equal(t, newbat.Vecs[1].Col.([]int32)[1], update.Vecs[2].Col.([]int32)[1])
	update.Vecs[1].Col.([]int32)[0] = 5
	update.Vecs[1].Col.([]int32)[1] = 6
	update.Vecs[1].Col.([]int32)[3] = 8
	update.Vecs[2].Col.([]int32)[0] = 9
	update.Vecs[2].Col.([]int32)[1] = 10
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	rel, err = dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	err = rel.Update(ctx, update)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	rel, err = dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	readers, _ = rel.NewReader(ctx, 10, nil, nil)
	m = mheap.New(guest.New(1<<20, host.New(1<<20)))
	for _, reader := range readers {
		bat, err := reader.Read([]string{schema.ColDefs[0].Name, schema.ColDefs[1].Name}, nil, m)
		assert.Nil(t, err)
		if bat != nil {
			assert.Equal(t, int32(5), bat.Vecs[0].Col.([]int32)[0])
			assert.Equal(t, int32(6), bat.Vecs[0].Col.([]int32)[1])
			assert.Equal(t, int32(8), bat.Vecs[0].Col.([]int32)[3])
			assert.Equal(t, update.Vecs[1].Col.([]int32)[2], bat.Vecs[0].Col.([]int32)[2])
			assert.Equal(t, int32(9), bat.Vecs[1].Col.([]int32)[0])
			assert.Equal(t, int32(10), bat.Vecs[1].Col.([]int32)[1])
			assert.Equal(t, update.Vecs[2].Col.([]int32)[2], bat.Vecs[1].Col.([]int32)[2])
		}
	}

	tae.Close()
	tae, err = db.Open(tae.Dir, nil)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, tae.Close()) }()
	e = NewEngine(tae)
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	rel, err = dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	readers, _ = rel.NewReader(ctx, 10, nil, nil)
	updatePK := newbat
	m = mheap.New(guest.New(1<<20, host.New(1<<20)))
	for _, reader := range readers {
		bat, err := reader.Read([]string{schema.ColDefs[0].Name,
			schema.ColDefs[1].Name, schema.ColDefs[2].Name,
			schema.ColDefs[13].Name}, nil, m)
		assert.Nil(t, err)
		if bat != nil {
			assert.Equal(t, int32(5), bat.Vecs[0].Col.([]int32)[0])
			assert.Equal(t, int32(6), bat.Vecs[0].Col.([]int32)[1])
			assert.Equal(t, int32(8), bat.Vecs[0].Col.([]int32)[3])
			assert.Equal(t, update.Vecs[1].Col.([]int32)[2], bat.Vecs[0].Col.([]int32)[2])
			assert.Equal(t, int32(9), bat.Vecs[1].Col.([]int32)[0])
			assert.Equal(t, int32(10), bat.Vecs[1].Col.([]int32)[1])
			assert.Equal(t, update.Vecs[2].Col.([]int32)[2], bat.Vecs[1].Col.([]int32)[2])
			updatePK = bat
		}
	}
	updatePK.Vecs[2].Col.([]int32)[0] = 20
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database(ctx, "db", engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	rel, err = dbase.Relation(ctx, schema.Name)
	assert.Nil(t, err)
	err = rel.Update(ctx, updatePK)
	assert.NotNil(t, err)
	assert.Equal(t, data.ErrUpdateUniqueKey, err)
}

func TestCopy1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	t1 := types.Type_VARCHAR.ToType()
	v1 := containers.MockVector(t1, 10, false, true, nil)
	defer v1.Close()
	v1.Update(5, types.Null{})
	mv1 := CopyToMoVector(v1)
	for i := 0; i < v1.Length(); i++ {
		assert.Equal(t, v1.Get(i), GetValue(mv1, uint32(i)))
	}

	t2 := types.Type_DATE.ToType()
	v2 := containers.MockVector(t2, 20, false, true, nil)
	defer v2.Close()
	v2.Update(6, types.Null{})
	mv2 := CopyToMoVector(v2)
	for i := 0; i < v2.Length(); i++ {
		assert.Equal(t, v2.Get(i), GetValue(mv2, uint32(i)))
	}

	v3 := MOToVector(mv2, true)
	t.Log(v3.String())
	for i := 0; i < v3.Length(); i++ {
		assert.Equal(t, v2.Get(i), v3.Get(i))
	}
}

func checkSysTable(t *testing.T, name string, dbase engine.Database, txn Txn, relcnt int, schema *catalog.Schema) {
	ctx := context.TODO()
	defs, err := SchemaToDefs(schema)
	defs[5].(*engine.AttributeDef).Attr.Default = engine.MakeDefaultExpr(true, int32(3), false)
	defs[6].(*engine.AttributeDef).Attr.Default = engine.MakeDefaultExpr(true, nil, true)
	assert.NoError(t, err)
	err = dbase.Create(ctx, name, defs)
	assert.Nil(t, err)
	names, _ := dbase.Relations(ctx)
	assert.Equal(t, relcnt, len(names))
	rel, err := dbase.Relation(ctx, name)
	assert.Nil(t, err)
	rDefs, _ := rel.TableDefs(ctx)
	rDefs = rDefs[:len(rDefs)-1]
	for i, def := range rDefs {
		assert.Equal(t, defs[i], def)
	}
	rAttr := rDefs[5].(*engine.AttributeDef).Attr
	assert.Equal(t, int32(3), rAttr.Default.Value.(int32))
	rAttr = rDefs[6].(*engine.AttributeDef).Attr
	assert.Equal(t, true, rAttr.Default.IsNull)
	rAttr = rDefs[7].(*engine.AttributeDef).Attr
	assert.Equal(t, false, rAttr.Default.Exist)
	bat := catalog.MockBatch(schema, 100)
	defer bat.Close()

	newbat := mobat.New(true, bat.Attrs)
	newbat.Vecs = CopyToMoVectors(bat.Vecs)
	err = rel.Write(ctx, newbat)
	assert.Equal(t, ErrReadOnly, err)
	attrs, _ := rel.GetPrimaryKeys(ctx)
	assert.NotNil(t, attrs)
	assert.Equal(t, schema.SortKey.Defs[0].Name, attrs[0].Name)
	attrs, _ = rel.GetHideKeys(ctx)
	attr := attrs[0]
	assert.NotNil(t, attr.Name, catalog.HiddenColumnName)
}

func TestSysRelation(t *testing.T) {
	ctx := context.TODO()
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	e := NewEngine(tae)
	txn, err := e.StartTxn(nil)
	assert.Nil(t, err)
	err = e.Create(ctx, catalog.SystemTable_DB_Name, engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	names, _ := e.Databases(ctx, engine.Snapshot(txn.GetCtx()))
	assert.Equal(t, 2, len(names))
	dbase, err := e.Database(ctx, catalog.SystemTable_DB_Name, engine.Snapshot(txn.GetCtx()))
	assert.Nil(t, err)
	schema := catalog.MockSchema(13, 12)
	checkSysTable(t, catalog.SystemTable_DB_Name, dbase, txn, 1, schema)
	schema = catalog.MockSchema(14, 13)
	checkSysTable(t, catalog.SystemTable_Table_Name, dbase, txn, 2, schema)
	schema = catalog.MockSchema(15, 14)
	checkSysTable(t, catalog.SystemTable_Columns_Name, dbase, txn, 3, schema)
	assert.Nil(t, txn.Commit())
}
