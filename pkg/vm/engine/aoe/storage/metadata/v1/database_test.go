package metadata

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
	"github.com/stretchr/testify/assert"
)

func TestDatabase1(t *testing.T) {
	dir := "/tmp/metadata/testdatabase"
	blockRows, segmentBlocks := uint64(100), uint64(2)
	catalog := initTest(dir, blockRows, segmentBlocks, true)

	dbName := "db1"
	idAlloc := common.IdAlloctor{}

	index := &LogIndex{
		Id: shard.SimpleIndexId(idAlloc.Alloc()),
	}
	db, err := catalog.SimpleCreateDatabase(dbName, index)
	assert.Nil(t, err)
	assert.Equal(t, dbName, db.Name)
	_, err = catalog.SimpleCreateDatabase(dbName, index)
	assert.NotNil(t, err)

	assert.Panics(t, func() {
		catalog.SimpleHardDeleteDatabase(db.Id)
	})

	dropIdx := &LogIndex{
		Id: shard.SimpleIndexId(idAlloc.Alloc()),
	}
	err = catalog.SimpleDropDatabaseByName(dbName, dropIdx)
	assert.Nil(t, err)

	assert.True(t, db.IsSoftDeleted())

	_, err = catalog.SimpleGetDatabaseByName(dbName)
	assert.NotNil(t, err)

	t.Log(db.PString(PPL1))
	db2, err := catalog.SimpleCreateDatabase(dbName, index)
	assert.Nil(t, err)

	schema := MockSchema(2)
	schema.Name = "t1"
	idx1 := &LogIndex{
		Id: shard.SimpleIndexId(idAlloc.Alloc()),
	}
	t1, err := db2.SimpleCreateTable(schema, idx1)
	assert.Nil(t, err)
	assert.Equal(t, schema.Name, t1.Schema.Name)
	found := db2.SimpleGetTableByName(t1.Schema.Name)
	assert.Equal(t, t1, found)

	idx2 := &LogIndex{
		Id: shard.SimpleIndexId(idAlloc.Alloc()),
	}
	err = db2.SimpleDropTableByName(t1.Schema.Name, idx2)
	assert.Nil(t, err)

	found = db2.SimpleGetTableByName(t1.Schema.Name)
	assert.Nil(t, found)

	err = db2.SimpleHardDeleteTable(t1.Id)
	assert.Nil(t, err)
	assert.True(t, t1.IsHardDeleted())

	nextId := func() *LogIndex {
		return &LogIndex{
			Id: shard.SimpleIndexId(idAlloc.Alloc()),
		}
	}
	idx1 = nextId()
	_, err = db2.SimpleCreateTable(schema, idx1)
	assert.Nil(t, err)
	// t.Log(db2.PString(PPL1))

	t.Log(catalog.PString(PPL0))
	catalog.Close()
	t.Log("----------")

	catalog2 := initTest(dir, blockRows, segmentBlocks, false)
	defer catalog2.Close()

	t.Log(catalog2.PString(PPL0))
}

func TestTxn(t *testing.T) {
	dir := "/tmp/metadata/testtxn"
	blockRows, segmentBlocks := uint64(100), uint64(2)
	catalog := initTest(dir, blockRows, segmentBlocks, true)
	defer catalog.Close()
	gen := shard.NewMockIndexAllocator()

	shardId := uint64(100)

	txn := catalog.StartTxn(gen.Next(shardId))
	assert.NotNil(t, txn)
	db1, err := catalog.CreateDatabaseInTxn(txn, "db1")
	assert.Nil(t, err)
	assert.False(t, db1.HasCommitted())
	schema := MockSchema(2)
	schema.Name = "t1"
	_, err = db1.CreateTableInTxn(txn, schema)
	assert.Nil(t, err)
	t.Log(db1.PString(PPL0))

	_, err = catalog.SimpleGetDatabaseByName(db1.Name)
	assert.NotNil(t, err)
	_, err = catalog.GetDatabaseByNameInTxn(txn, db1.Name)
	assert.Nil(t, err)

	err = txn.Commit()
	assert.Nil(t, err)
	assert.True(t, db1.HasCommitted())
	t.Log(db1.PString(PPL0))

	_, err = catalog.SimpleGetDatabaseByName(db1.Name)
	assert.Nil(t, err)

	txn = catalog.StartTxn(gen.Next(shardId))
	assert.NotNil(t, txn)
	db1, err = catalog.GetDatabaseByNameInTxn(txn, db1.Name)
	assert.Nil(t, err)
	assert.True(t, db1.HasCommitted())
	schema2 := MockSchema(3)
	schema2.Name = "t2"
	t2, err := db1.CreateTableInTxn(txn, schema2)
	assert.Nil(t, err)
	assert.False(t, t2.HasCommitted())
	foundT := db1.GetTableByNameInTxn(txn, schema2.Name)
	assert.NotNil(t, foundT)
	foundT = db1.SimpleGetTableByName(schema2.Name)
	assert.Nil(t, foundT)

	err = db1.DropTableByNameInTxn(txn, schema.Name)
	assert.Nil(t, err)

	err = db1.SimpleDropTableByName(schema.Name, gen.Next(shardId))
	assert.NotNil(t, err)
	err = db1.SimpleDropTableByName(schema2.Name, gen.Next(shardId))
	t.Log(err)
	assert.NotNil(t, err)

	_, err = db1.SimpleCreateTable(schema2, gen.Next(shardId))
	assert.NotNil(t, err)
	t.Log(err)

	t.Log(db1.PString(PPL0))
}
