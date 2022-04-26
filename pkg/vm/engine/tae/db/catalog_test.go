package db

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/assert"
)

func TestCatalog1(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()

	txn := db.StartTxn(nil)
	schema := catalog.MockSchema(1)
	database, _ := txn.CreateDatabase("db")
	rel, _ := database.CreateRelation(schema)
	// relMeta := rel.GetMeta().(*catalog.TableEntry)
	seg, _ := rel.CreateSegment()
	blk, err := seg.CreateBlock()
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))

	txn = db.StartTxn(nil)
	database, _ = txn.GetDatabase("db")
	rel, _ = database.GetRelationByName(schema.Name)
	sseg, err := rel.GetSegment(seg.GetID())
	assert.Nil(t, err)
	t.Log(sseg.String())
	err = sseg.SoftDeleteBlock(blk.Fingerprint().BlockID)
	assert.Nil(t, err)

	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
	blk2, err := sseg.CreateBlock()
	assert.Nil(t, err)
	assert.NotNil(t, blk2)
	assert.Nil(t, txn.Commit())
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))

	{
		txn = db.StartTxn(nil)
		database, _ = txn.GetDatabase("db")
		rel, _ = database.GetRelationByName(schema.Name)
		t.Log(txn.String())
		it := rel.MakeBlockIt()
		cnt := 0
		for it.Valid() {
			block := it.GetBlock()
			cnt++
			t.Log(block.String())
			it.Next()
		}
		assert.Equal(t, 1, cnt)
	}
}
