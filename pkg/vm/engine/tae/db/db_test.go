package db

import (
	"bytes"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "TAEDB"
)

func initDB(t *testing.T, opts *options.Options) *DB {
	dir := testutils.InitTestEnv(ModuleName, t)
	db, _ := Open(dir, nil)
	return db
}

func TestAppend(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()
	txn, err := db.StartTxn(nil)
	assert.Nil(t, err)
	schema := catalog.MockSchemaAll(14)
	schema.BlockMaxRows = options.DefaultBlockMaxRows
	schema.SegmentMaxBlocks = options.DefaultBlocksPerSegment
	schema.PrimaryKey = 3
	data := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows)*2, int(schema.PrimaryKey), nil)
	now := time.Now()
	bats := compute.SplitBatch(data, 4)
	database, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	rel, err := database.CreateRelation(schema)
	assert.Nil(t, err)
	err = rel.Append(bats[0])
	assert.Nil(t, err)
	t.Log(vector.Length(bats[0].Vecs[0]))
	assert.Nil(t, txn.Commit())
	t.Log(time.Since(now))
	t.Log(vector.Length(bats[0].Vecs[0]))

	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		{
			txn, _ := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)
			err = rel.Append(bats[1])
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit())
		}
		err = rel.Append(bats[2])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
}

func TestTableHandle(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()

	txn, _ := db.StartTxn(nil)
	database, _ := txn.CreateDatabase("db")
	schema := catalog.MockSchema(2)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2
	rel, _ := database.CreateRelation(schema)

	tableMeta := rel.GetMeta().(*catalog.TableEntry)
	t.Log(tableMeta.String())
	table := tableMeta.GetTableData()

	handle := table.GetHandle()
	appender, err := handle.GetAppender()
	assert.Nil(t, appender)
	assert.Equal(t, data.ErrAppendableSegmentNotFound, err)
}

func TestMVCC1(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()
	schema := catalog.MockSchemaAll(13)
	schema.BlockMaxRows = 40
	schema.SegmentMaxBlocks = 2
	schema.PrimaryKey = 2
	bat := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows)*10, int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, 40)

	txn, _ := db.StartTxn(nil)
	database, _ := txn.CreateDatabase("db")
	rel, _ := database.CreateRelation(schema)
	err := rel.Append(bats[0])
	assert.Nil(t, err)

	row := uint32(5)
	expectVal := compute.GetValue(bats[0].Vecs[schema.PrimaryKey], row)
	filter := &handle.Filter{
		Op:  handle.FilterEq,
		Val: expectVal,
	}
	id, offset, err := rel.GetByFilter(filter)
	assert.Nil(t, err)
	t.Logf("id=%s,offset=%d", id, offset)
	// Read uncommitted value
	actualVal, err := rel.GetValue(id, offset, uint16(schema.PrimaryKey))
	assert.Nil(t, err)
	assert.Equal(t, expectVal, actualVal)
	assert.Nil(t, txn.Commit())

	txn, _ = db.StartTxn(nil)
	database, _ = txn.GetDatabase("db")
	rel, _ = database.GetRelationByName(schema.Name)
	id, offset, err = rel.GetByFilter(filter)
	assert.Nil(t, err)
	t.Logf("id=%s,offset=%d", id, offset)
	// Read committed value
	actualVal, err = rel.GetValue(id, offset, uint16(schema.PrimaryKey))
	assert.Nil(t, err)
	assert.Equal(t, expectVal, actualVal)

	txn2, _ := db.StartTxn(nil)
	database2, _ := txn2.GetDatabase("db")
	rel2, _ := database2.GetRelationByName(schema.Name)

	err = rel2.Append(bats[1])
	assert.Nil(t, err)

	val2 := compute.GetValue(bats[1].Vecs[schema.PrimaryKey], row)
	filter.Val = val2
	id, offset, err = rel2.GetByFilter(filter)
	assert.Nil(t, err)
	actualVal, err = rel2.GetValue(id, offset, uint16(schema.PrimaryKey))
	assert.Nil(t, err)
	assert.Equal(t, val2, actualVal)

	assert.Nil(t, txn2.Commit())

	_, _, err = rel.GetByFilter(filter)
	assert.NotNil(t, err)

	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		id, offset, err = rel.GetByFilter(filter)
		t.Log(err)
		assert.Nil(t, err)
	}

	it := rel.MakeBlockIt()
	for it.Valid() {
		block := it.GetBlock()
		bid := block.Fingerprint()
		if bid.BlockID == id.BlockID {
			var comp bytes.Buffer
			var decomp bytes.Buffer
			vec, mask, err := block.GetVectorCopy(schema.ColDefs[schema.PrimaryKey].Name, &comp, &decomp)
			assert.Nil(t, err)
			assert.Nil(t, mask)
			assert.NotNil(t, vec)
			t.Log(vec.String())
			t.Log(offset)
			t.Log(val2)
			t.Log(vector.Length(bats[0].Vecs[0]))
			assert.Equal(t, vector.Length(bats[0].Vecs[0]), vector.Length(vec))
		}
		it.Next()
	}
}

// 1. Txn1 create db, relation and append 10 rows. committed -- PASS
// 2. Txn2 append 10 rows. Get the 5th append row value -- PASS
// 3. Txn2 delete the 5th row value in uncommited state -- PASS
// 4. Txn2 get the 5th row value -- NotFound
func TestMVCC2(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()
	schema := catalog.MockSchemaAll(13)
	schema.BlockMaxRows = 100
	schema.SegmentMaxBlocks = 2
	schema.PrimaryKey = 2
	bat := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows), int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, 10)
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		rel.Append(bats[0])
		val := compute.GetValue(bats[0].Vecs[schema.PrimaryKey], 5)
		filter := handle.Filter{
			Op:  handle.FilterEq,
			Val: val,
		}
		_, _, err := rel.GetByFilter(&filter)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		rel.Append(bats[1])
		val := compute.GetValue(bats[1].Vecs[schema.PrimaryKey], 5)
		filter := handle.Filter{
			Op:  handle.FilterEq,
			Val: val,
		}
		id, offset, err := rel.GetByFilter(&filter)
		assert.Nil(t, err)
		err = rel.RangeDelete(id, offset, offset)
		assert.Nil(t, err)

		_, _, err = rel.GetByFilter(&filter)
		assert.NotNil(t, err)

		t.Log(err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		var comp bytes.Buffer
		var decomp bytes.Buffer
		for it.Valid() {
			block := it.GetBlock()
			vec, mask, err := block.GetVectorCopy(schema.ColDefs[schema.PrimaryKey].Name, &comp, &decomp)
			assert.Nil(t, err)
			assert.Nil(t, mask)
			t.Log(vec.String())
			// TODO: exclude deleted rows when apply appends
			// assert.Equal(t, vector.Length(bats[1].Vecs[0])*2-1, int(vector.Length(vec)))
			it.Next()
		}
	}
}
