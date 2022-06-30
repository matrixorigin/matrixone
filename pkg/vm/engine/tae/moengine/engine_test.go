package moengine

import (
	mobat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"

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
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	e := NewEngine(tae)
	txn, err := e.StartTxn(nil)
	assert.Nil(t, err)
	err = e.Create(0, "db", 0, txn.GetCtx())
	assert.Nil(t, err)
	names := e.Databases(txn.GetCtx())
	assert.Equal(t, 2, len(names))
	dbase, err := e.Database("db", txn.GetCtx())
	assert.Nil(t, err)

	schema := catalog.MockSchema(13, 12)
	defs, err := SchemaToDefs(schema)
	defs[5].(*engine.AttributeDef).Attr.Default = engine.MakeDefaultExpr(true, int32(3), false)
	defs[6].(*engine.AttributeDef).Attr.Default = engine.MakeDefaultExpr(true, nil, true)
	assert.NoError(t, err)
	err = dbase.Create(0, schema.Name, defs, txn.GetCtx())
	assert.Nil(t, err)
	names = dbase.Relations(txn.GetCtx())
	assert.Equal(t, 1, len(names))

	rel, err := dbase.Relation(schema.Name, txn.GetCtx())
	assert.Nil(t, err)
	rDefs := rel.TableDefs(nil)
	assert.Equal(t, 14, len(rDefs))
	rAttr := rDefs[5].(*engine.AttributeDef).Attr
	assert.Equal(t, int32(3), rAttr.Default.Value.(int32))
	rAttr = rDefs[6].(*engine.AttributeDef).Attr
	assert.Equal(t, true, rAttr.Default.IsNull)
	rAttr = rDefs[7].(*engine.AttributeDef).Attr
	assert.Equal(t, false, rAttr.Default.Exist)
	assert.Equal(t, 1, len(rel.Nodes(nil)))
	assert.Equal(t, ADDR, rel.Nodes(nil)[0].Addr)
	bat := catalog.MockBatch(schema, 100)
	defer bat.Close()

	newbat := mobat.New(true, bat.Attrs)
	newbat.Vecs = CopyToMoVectors(bat.Vecs)
	err = rel.Write(0, newbat, txn.GetCtx())
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database("db", txn.GetCtx())
	assert.Nil(t, err)
	rel, err = dbase.Relation(schema.Name, txn.GetCtx())
	assert.Nil(t, err)
	attr := rel.GetPrimaryKeys(nil)
	key := attr[0]
	bat = catalog.MockBatch(schema, 20)
	defer bat.Close()
	newbat = mobat.New(true, bat.Attrs)
	newbat.Vecs = CopyToMoVectors(bat.Vecs)
	err = rel.Delete(0, newbat.Vecs[12], key.Name, nil)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database("db", txn.GetCtx())
	assert.Nil(t, err)
	rel, err = dbase.Relation(schema.Name, txn.GetCtx())
	assert.Nil(t, err)
	readers := rel.NewReader(10, nil, nil, nil)
	for _, reader := range readers {
		bat, err := reader.Read([]uint64{uint64(1)}, []string{schema.ColDefs[1].Name})
		assert.Nil(t, err)
		if bat != nil {
			assert.Equal(t, 80, vector.Length(bat.Vecs[0]))
		}
	}
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestEngineAllType(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	e := NewEngine(tae)
	txn, err := e.StartTxn(nil)
	assert.Nil(t, err)
	err = e.Create(0, "db", 0, txn.GetCtx())
	assert.Nil(t, err)
	names := e.Databases(txn.GetCtx())
	assert.Equal(t, 2, len(names))
	dbase, err := e.Database("db", txn.GetCtx())
	assert.Nil(t, err)

	schema := catalog.MockSchemaAll(18, 12)
	defs, err := SchemaToDefs(schema)
	defs[5].(*engine.AttributeDef).Attr.Default = engine.MakeDefaultExpr(true, uint16(3), false)
	defs[6].(*engine.AttributeDef).Attr.Default = engine.MakeDefaultExpr(true, nil, true)
	assert.NoError(t, err)
	err = dbase.Create(0, schema.Name, defs, txn.GetCtx())
	assert.Nil(t, err)
	names = dbase.Relations(txn.GetCtx())
	assert.Equal(t, 1, len(names))

	rel, err := dbase.Relation(schema.Name, txn.GetCtx())
	assert.Nil(t, err)
	rDefs := rel.TableDefs(nil)
	assert.Equal(t, 19, len(rDefs))
	rAttr := rDefs[5].(*engine.AttributeDef).Attr
	assert.Equal(t, uint16(3), rAttr.Default.Value.(uint16))
	rAttr = rDefs[6].(*engine.AttributeDef).Attr
	assert.Equal(t, true, rAttr.Default.IsNull)
	basebat := catalog.MockBatch(schema, 100)
	defer basebat.Close()

	newbat := mobat.New(true, basebat.Attrs)
	newbat.Vecs = CopyToMoVectors(basebat.Vecs)
	err = rel.Write(0, newbat, txn.GetCtx())
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database("db", txn.GetCtx())
	assert.Nil(t, err)
	rel, err = dbase.Relation(schema.Name, txn.GetCtx())
	assert.Nil(t, err)
	attr := rel.GetPrimaryKeys(nil)
	key := attr[0]
	bat := catalog.MockBatch(schema, 20)
	defer bat.Close()
	newbat1 := mobat.New(true, bat.Attrs)
	newbat1.Vecs = CopyToMoVectors(bat.Vecs)
	err = rel.Delete(0, newbat1.Vecs[12], key.Name, nil)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database("db", txn.GetCtx())
	assert.Nil(t, err)
	rel, err = dbase.Relation(schema.Name, txn.GetCtx())
	assert.Nil(t, err)
	refs := make([]uint64, len(schema.Attrs()))
	readers := rel.NewReader(10, nil, nil, nil)
	for _, reader := range readers {
		bat, err := reader.Read(refs, schema.Attrs())
		assert.Nil(t, err)
		if bat != nil {
			assert.Equal(t, 80, vector.Length(bat.Vecs[0]))
			vec := MOToVector(bat.Vecs[12], false)
			assert.Equal(t, vec.Get(0), basebat.Vecs[12].Get(20))
		}
	}
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestTxnRelation_GetHideKey(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	e := NewEngine(tae)
	txn, err := e.StartTxn(nil)
	assert.Nil(t, err)
	err = e.Create(0, "db", 0, txn.GetCtx())
	assert.Nil(t, err)
	names := e.Databases(txn.GetCtx())
	assert.Equal(t, 2, len(names))
	dbase, err := e.Database("db", txn.GetCtx())
	assert.Nil(t, err)

	schema := catalog.MockSchema(13, 15)
	defs, err := SchemaToDefs(schema)
	assert.NoError(t, err)
	err = dbase.Create(0, schema.Name, defs, txn.GetCtx())
	assert.Nil(t, err)
	names = dbase.Relations(txn.GetCtx())
	assert.Equal(t, 1, len(names))

	rel, err := dbase.Relation(schema.Name, txn.GetCtx())
	assert.Nil(t, err)
	assert.Equal(t, 1, len(rel.Nodes(nil)))
	assert.Equal(t, ADDR, rel.Nodes(nil)[0].Addr)
	bat := catalog.MockBatch(schema, 100)
	defer bat.Close()

	newbat := mobat.New(true, bat.Attrs)
	newbat.Vecs = CopyToMoVectors(bat.Vecs)
	err = rel.Write(0, newbat, txn.GetCtx())
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database("db", txn.GetCtx())
	assert.Nil(t, err)
	rel, err = dbase.Relation(schema.Name, txn.GetCtx())
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	readers := rel.NewReader(10, nil, nil, nil)
	delete := mobat.New(true, bat.Attrs)
	for _, reader := range readers {
		bat, err := reader.Read([]uint64{uint64(1)}, []string{schema.ColDefs[13].Name})
		assert.Nil(t, err)
		if bat != nil {
			assert.Equal(t, 100, vector.Length(bat.Vecs[0]))
			delete = bat
		}
	}
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database("db", txn.GetCtx())
	assert.Nil(t, err)
	rel, err = dbase.Relation(schema.Name, txn.GetCtx())
	assert.Nil(t, err)
	attr := rel.GetPrimaryKeys(nil)
	assert.Nil(t, attr)
	key := rel.GetHideKey(nil)
	err = rel.Delete(0, delete.Vecs[0], key.Name, nil)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database("db", txn.GetCtx())
	assert.Nil(t, err)
	rel, err = dbase.Relation(schema.Name, txn.GetCtx())
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	readers = rel.NewReader(1, nil, nil, nil)
	for _, reader := range readers {
		bat, err := reader.Read([]uint64{uint64(1)}, []string{schema.ColDefs[13].Name})
		assert.Nil(t, err)
		if bat != nil {
			assert.Equal(t, 0, vector.Length(bat.Vecs[0]))
		}
	}

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestTxnRelation_Update(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	e := NewEngine(tae)
	txn, err := e.StartTxn(nil)
	assert.Nil(t, err)
	err = e.Create(0, "db", 0, txn.GetCtx())
	assert.Nil(t, err)
	names := e.Databases(txn.GetCtx())
	assert.Equal(t, 2, len(names))
	dbase, err := e.Database("db", txn.GetCtx())
	assert.Nil(t, err)

	schema := catalog.MockSchema(13, 2)
	defs, err := SchemaToDefs(schema)
	assert.NoError(t, err)
	err = dbase.Create(0, schema.Name, defs, txn.GetCtx())
	assert.Nil(t, err)
	dbase, err = e.Database("db", txn.GetCtx())
	assert.Nil(t, err)
	rel, err := dbase.Relation(schema.Name, txn.GetCtx())
	assert.Nil(t, err)
	bat := catalog.MockBatch(schema, 4)
	defer bat.Close()

	newbat := mobat.New(true, bat.Attrs)
	newbat.Vecs = CopyToMoVectors(bat.Vecs)
	err = rel.Write(0, newbat, txn.GetCtx())
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database("db", txn.GetCtx())
	assert.Nil(t, err)
	rel, err = dbase.Relation(schema.Name, txn.GetCtx())
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	readers := rel.NewReader(10, nil, nil, nil)
	update := newbat
	for _, reader := range readers {
		bat1, err := reader.Read([]uint64{uint64(1), uint64(1), uint64(1)}, []string{schema.ColDefs[13].Name, schema.ColDefs[0].Name, schema.ColDefs[1].Name})
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
	dbase, err = e.Database("db", txn.GetCtx())
	assert.Nil(t, err)
	rel, err = dbase.Relation(schema.Name, txn.GetCtx())
	assert.Nil(t, err)
	err = rel.Update(0, update, nil)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database("db", txn.GetCtx())
	assert.Nil(t, err)
	rel, err = dbase.Relation(schema.Name, txn.GetCtx())
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	readers = rel.NewReader(10, nil, nil, nil)
	for _, reader := range readers {
		bat, err := reader.Read([]uint64{uint64(1), uint64(1)}, []string{schema.ColDefs[0].Name, schema.ColDefs[1].Name})
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
	defer tae.Close()
	assert.NoError(t, err)
	e = NewEngine(tae)
	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database("db", txn.GetCtx())
	assert.Nil(t, err)
	rel, err = dbase.Relation(schema.Name, txn.GetCtx())
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	readers = rel.NewReader(10, nil, nil, nil)
	updatePK := newbat
	for _, reader := range readers {
		bat, err := reader.Read([]uint64{uint64(1), uint64(1), uint64(1), uint64(1)},
			[]string{schema.ColDefs[0].Name,
				schema.ColDefs[1].Name, schema.ColDefs[2].Name,
				schema.ColDefs[13].Name})
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
	dbase, err = e.Database("db", txn.GetCtx())
	assert.Nil(t, err)
	rel, err = dbase.Relation(schema.Name, txn.GetCtx())
	assert.Nil(t, err)
	err = rel.Update(0, updatePK, nil)
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
