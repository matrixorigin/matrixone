package moengine

import (
	"testing"

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
