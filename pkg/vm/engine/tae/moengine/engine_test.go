package moengine

import (
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/helper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/adaptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
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

	mockTbl := adaptor.MockTableInfo(10)
	mockTbl.Name = fmt.Sprintf("%s%d", "xx", time.Now().Unix())
	_, _, _, _, defs, _ := helper.UnTransfer(*mockTbl)

	err = dbase.Create(0, mockTbl.Name, defs, txn.GetCtx())
	assert.Nil(t, err)
	names = dbase.Relations(txn.GetCtx())
	assert.Equal(t, 1, len(names))

	rel, err := dbase.Relation(mockTbl.Name, txn.GetCtx())
	assert.Nil(t, err)
	meta := rel.(*txnRelation).handle.GetMeta().(*catalog.TableEntry)
	bat := compute.MockBatch(meta.GetSchema().Types(), 100, int(meta.GetSchema().PrimaryKey), nil)
	err = rel.Write(0, bat, txn.GetCtx())
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, err = e.StartTxn(nil)
	assert.Nil(t, err)
	dbase, err = e.Database("db", txn.GetCtx())
	assert.Nil(t, err)
	rel, err = dbase.Relation(mockTbl.Name, txn.GetCtx())
	assert.Nil(t, err)
	readers := rel.NewReader(10, nil, nil, nil)
	for _, reader := range readers {
		bat, err := reader.Read([]uint64{uint64(1)}, []string{meta.GetSchema().ColDefs[1].Name})
		assert.Nil(t, err)
		if bat != nil {
			assert.Equal(t, 100, vector.Length(bat.Vecs[0]))
		}
	}
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}
