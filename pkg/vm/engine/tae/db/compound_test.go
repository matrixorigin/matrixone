package db

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
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

	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	rel, _ = db.GetRelationByName(schema.Name)
	err = rel.Append(bat)
	assert.ErrorIs(t, err, data.ErrDuplicate)

	filter := handle.NewEQFilter(model.EncodeTuple(nil, 2, bat.Vecs[2], bat.Vecs[0]))
	id, row, err := rel.GetByFilter(filter)
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), row)
	err = rel.Update(id, row, 1, int32(999))
	assert.NoError(t, err)

	filter = handle.NewEQFilter(model.EncodeTuple(nil, 3, bat.Vecs[2], bat.Vecs[0]))
	err = rel.DeleteByFilter(filter)
	assert.NoError(t, err)

	it = rel.MakeBlockIt()
	rows = 0
	for it.Valid() {
		blk := it.GetBlock()
		view, err := blk.GetColumnDataById(0, nil, nil)
		assert.NoError(t, err)
		view.ApplyDeletes()
		rows += view.Length()
		it.Next()
	}
	assert.Equal(t, 7, rows)
	_, _, err = rel.GetByFilter(filter)
	assert.ErrorIs(t, err, data.ErrNotFound)

	assert.NoError(t, txn.Commit())

	txn, err = tae.StartTxn(nil)
	assert.NoError(t, err)
	db, _ = txn.GetDatabase("db")
	rel, _ = db.GetRelationByName(schema.Name)
	assert.Equal(t, int64(7), rel.Rows())

	it = rel.MakeBlockIt()
	rows = 0
	for it.Valid() {
		blk := it.GetBlock()
		view, err := blk.GetColumnDataById(0, nil, nil)
		assert.NoError(t, err)
		view.ApplyDeletes()
		rows += view.Length()
		it.Next()
	}
	assert.Equal(t, 7, rows)

	bat2 := catalog.MockData(schema, 5)
	c2_1 := []int32{3, 4, 3, 4, 3}
	c0_1 := []int32{1, 2, 3, 1, 2}
	vector.SetCol(bat2.Vecs[2], c2_1)
	vector.SetCol(bat2.Vecs[0], c0_1)
	err = rel.Append(bat2)
	assert.NoError(t, err)

	it = rel.MakeBlockIt()
	rows = 0
	for it.Valid() {
		blk := it.GetBlock()
		view, err := blk.GetColumnDataById(0, nil, nil)
		assert.NoError(t, err)
		view.ApplyDeletes()
		rows += view.Length()
		it.Next()
	}
	assert.Equal(t, 12, rows)

	assert.NoError(t, txn.Commit())

	compactBlocks(t, tae, "db", schema, false)

	// TODO
	// txn, _ = tae.StartTxn(nil)
	// db, _ = txn.GetDatabase("db")
	// rel, _ = db.GetRelationByName(schema.Name)
	// assert.Equal(t, int64(12), rel.Rows())

	// id, row, err = rel.GetByFilter(filter)
	// assert.ErrorIs(t, err, data.ErrNotFound)
	// filter = handle.NewEQFilter(model.EncodeTuple(nil, 4, bat.Vecs[2], bat.Vecs[0]))
	// id, row, err = rel.GetByFilter(filter)
	// assert.NoError(t, err)
	// err = rel.Append(bat)
	// assert.ErrorIs(t, err, data.ErrDuplicate)

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}
