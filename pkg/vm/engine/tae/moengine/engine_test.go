package moengine

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/mockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"testing"
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
	bat := catalog.MockData(schema, 100)
	err = rel.Write(0, bat, txn.GetCtx())
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
	bat = catalog.MockData(schema, 20)
	err = rel.Delete(0, bat.Vecs[12], key.Name, nil)
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

func TestTxnRelation_GetHideKey(t *testing.T) {
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
	bat := catalog.MockData(schema, 100)
	err = rel.Write(0, bat, txn.GetCtx())
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
	delete := bat
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
	bat := catalog.MockData(schema, 4)
	err = rel.Write(0, bat, txn.GetCtx())
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
	update := bat
	for _, reader := range readers {
		bat1, err := reader.Read([]uint64{uint64(1), uint64(1), uint64(1)}, []string{schema.ColDefs[13].Name, schema.ColDefs[0].Name, schema.ColDefs[1].Name})
		assert.Nil(t, err)
		if bat1 != nil {
			assert.Equal(t, 4, vector.Length(bat1.Vecs[0]))
			update = bat1
		}
	}
	assert.Equal(t, bat.Vecs[0].Col.([]int32)[0], update.Vecs[1].Col.([]int32)[0])
	assert.Equal(t, bat.Vecs[0].Col.([]int32)[1], update.Vecs[1].Col.([]int32)[1])
	assert.Equal(t, bat.Vecs[1].Col.([]int32)[0], update.Vecs[2].Col.([]int32)[0])
	assert.Equal(t, bat.Vecs[1].Col.([]int32)[1], update.Vecs[2].Col.([]int32)[1])
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
	updatePK := bat
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

func TestTxn10(t *testing.T) {
	tdb := initDB(t, nil)
	tae := NewEngine(tdb)
	schema := catalog.MockSchemaAll(4, 2)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 4
	cnt := uint32(10)
	rows := schema.BlockMaxRows / 2 * cnt
	bat := catalog.MockData(schema, rows)
	bats := compute.SplitBatch(bat, int(cnt))
	{
		//Create DB -> Create Table -> Drop Table -> Drop DB
		txn, _ := tae.StartTxn(nil)
		err := tae.Create(0, "tae", 0, txn.GetCtx())
		assert.Nil(t, err)
		dbase, err := tae.Database("tae", txn.GetCtx())
		assert.Nil(t, err)
		defs, err := SchemaToDefs(schema)
		assert.NoError(t, err)
		err = dbase.Create(0, schema.Name, defs, txn.GetCtx())
		assert.Nil(t, err)
		rel, err := dbase.Relation(schema.Name, txn.GetCtx())
		assert.Nil(t, err)
		err = rel.Write(0, bats[0], txn.GetCtx())
		assert.Nil(t, err)
		err = rel.Write(0, bats[0], txn.GetCtx())
		assert.NotNil(t, err)
		err = dbase.Delete(0, schema.Name, txn.GetCtx())
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
		txn, err = tae.StartTxn(nil)
		assert.Nil(t, err)
		dbase, err = tae.Database("tae", txn.GetCtx())
		assert.Nil(t, err)
		_, err = dbase.Relation(schema.Name, txn.GetCtx())
		assert.NotNil(t, err)
		err = tae.Delete(0, "tae", txn.GetCtx())
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		//Create DB -> Create Table -> Drop Table
		tdb, err := db.Open(tdb.Dir, nil)
		assert.Nil(t, err)
		tae := NewEngine(tdb)
		txn, err := tae.StartTxn(nil)
		assert.Nil(t, err)
		dbase, err := tae.Database("tae", txn.GetCtx())
		assert.NotNil(t, err)
		err = tae.Create(0, "tae", 0, txn.GetCtx())
		assert.Nil(t, err)
		dbase, err = tae.Database("tae", txn.GetCtx())
		assert.Nil(t, err)
		defs, err := SchemaToDefs(schema)
		assert.NoError(t, err)
		err = dbase.Create(0, schema.Name, defs, txn.GetCtx())
		assert.Nil(t, err)
		rel, err := dbase.Relation(schema.Name, txn.GetCtx())
		assert.Nil(t, err)
		err = rel.Write(0, bats[0], txn.GetCtx())
		assert.Nil(t, err)
		err = dbase.Delete(0, schema.Name, txn.GetCtx())
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
		tdb.Close()
	}
	{
		//Create Table -> Drop Table -> Create Table
		tdb, err := db.Open(tdb.Dir, nil)
		assert.Nil(t, err)
		tae := NewEngine(tdb)
		txn, err := tae.StartTxn(nil)
		assert.Nil(t, err)
		dbase, err := tae.Database("tae", txn.GetCtx())
		assert.Nil(t, err)
		_, err = dbase.Relation(schema.Name, txn.GetCtx())
		assert.NotNil(t, err)
		defs, err := SchemaToDefs(schema)
		assert.NoError(t, err)
		err = dbase.Create(0, schema.Name, defs, txn.GetCtx())
		assert.Nil(t, err)
		rel, err := dbase.Relation(schema.Name, txn.GetCtx())
		assert.Nil(t, err)
		err = rel.Write(0, bats[0], txn.GetCtx())
		assert.Nil(t, err)
		err = dbase.Delete(0, schema.Name, txn.GetCtx())
		assert.Nil(t, err)
		err = dbase.Create(0, schema.Name, defs, txn.GetCtx())
		assert.Nil(t, err)
		rel, err = dbase.Relation(schema.Name, txn.GetCtx())
		assert.Nil(t, err)
		err = rel.Write(0, bats[0], txn.GetCtx())
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
		tdb.Close()
	}
	{
		//Drop Table -> Create Table -> Drop Table -> Create Table -> Drop Table
		tdb, err := db.Open(tdb.Dir, nil)
		assert.Nil(t, err)
		tae := NewEngine(tdb)
		txn, err := tae.StartTxn(nil)
		assert.Nil(t, err)
		dbase, err := tae.Database("tae", txn.GetCtx())
		assert.Nil(t, err)
		_, err = dbase.Relation(schema.Name, txn.GetCtx())
		assert.Nil(t, err)
		err = dbase.Delete(0, schema.Name, txn.GetCtx())
		assert.Nil(t, err)
		_, err = dbase.Relation(schema.Name, txn.GetCtx())
		assert.NotNil(t, err)
		defs, err := SchemaToDefs(schema)
		assert.NoError(t, err)
		err = dbase.Create(0, schema.Name, defs, txn.GetCtx())
		assert.Nil(t, err)
		rel, err := dbase.Relation(schema.Name, txn.GetCtx())
		assert.Nil(t, err)
		err = rel.Write(0, bats[0], txn.GetCtx())
		assert.Nil(t, err)
		err = dbase.Delete(0, schema.Name, txn.GetCtx())
		assert.Nil(t, err)
		_, err = dbase.Relation(schema.Name, txn.GetCtx())
		assert.NotNil(t, err)
		err = dbase.Create(0, schema.Name, defs, txn.GetCtx())
		assert.Nil(t, err)
		rel, err = dbase.Relation(schema.Name, txn.GetCtx())
		assert.Nil(t, err)
		err = rel.Write(0, bats[0], txn.GetCtx())
		assert.Nil(t, err)
		err = dbase.Delete(0, schema.Name, txn.GetCtx())
		assert.Nil(t, err)
		_, err = dbase.Relation(schema.Name, txn.GetCtx())
		assert.NotNil(t, err)
		assert.Nil(t, txn.Commit())
		tdb.Close()
	}
	{
		tdb, err := db.Open(tdb.Dir, nil)
		assert.Nil(t, err)
		tae := NewEngine(tdb)
		txn, err := tae.StartTxn(nil)
		assert.Nil(t, err)
		dbase, err := tae.Database("tae", txn.GetCtx())
		assert.Nil(t, err)
		_, err = dbase.Relation(schema.Name, txn.GetCtx())
		assert.NotNil(t, err)
		assert.Nil(t, txn.Commit())
		tdb.Close()
	}
	t.Log(tdb.Opts.Catalog.SimplePPString(common.PPL1))
}
