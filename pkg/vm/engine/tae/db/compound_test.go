package db

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/stretchr/testify/assert"
)

func TestCompoundPK1(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockCompoundSchema(3, 2, 0)
	assert.Equal(t, 2, schema.GetSortKeyCnt())
	assert.Equal(t, 2, schema.SortKey.Defs[0].Idx)
	assert.Equal(t, 0, schema.SortKey.Defs[1].Idx)
	schema.BlockMaxRows = 5
	bat := catalog.MockData(schema, 8)
	c2 := []int32{1, 2, 1, 2, 1, 2, 1, 2}
	c0 := []int32{2, 3, 4, 5, 1, 2, 3, 2}
	vector.SetCol(bat.Vecs[2], c2)
	vector.SetCol(bat.Vecs[0], c0)

	txn, _ := tae.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	err := rel.Append(bat)
	assert.ErrorIs(t, err, data.ErrDuplicate)
	it := rel.MakeBlockIt()
	rows := 0
	for it.Valid() {
		blk := it.GetBlock()
		view, err := blk.GetColumnDataById(2, nil, nil)
		assert.NoError(t, err)
		rows += view.Length()
		it.Next()
	}
	assert.Equal(t, 0, rows)
	c0[7] = 4
	err = rel.Append(bat)
	assert.NoError(t, err)
	rows = 0
	it = rel.MakeBlockIt()
	for it.Valid() {
		blk := it.GetBlock()
		view, err := blk.GetColumnDataById(2, nil, nil)
		assert.NoError(t, err)
		rows += view.Length()
		it.Next()
	}
	assert.Equal(t, 8, rows)
	assert.NoError(t, txn.Commit())
}
