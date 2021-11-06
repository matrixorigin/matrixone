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
