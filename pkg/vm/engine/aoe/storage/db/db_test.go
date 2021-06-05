package db

import (
	"fmt"
	"math/rand"
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	TEST_DB_DIR = "/tmp/db_test"
)

func initDBTest() {
	os.RemoveAll(TEST_DB_DIR)
}

func initDB() *DB {
	rand.Seed(time.Now().UnixNano())
	cfg := &md.Configuration{
		Dir:              TEST_DB_DIR,
		SegmentMaxBlocks: 10,
		BlockMaxRows:     10,
	}
	opts := &e.Options{}
	opts.Meta.Conf = cfg
	dbi, _ := Open(TEST_DB_DIR, opts)
	return dbi
}

func TestCreateTable(t *testing.T) {
	initDBTest()
	dbi := initDB()
	assert.NotNil(t, dbi)
	defer dbi.Close()
	tblCnt := rand.Intn(5) + 3
	prefix := "mocktbl_"
	var wg sync.WaitGroup
	names := make([]string, 0)
	for i := 0; i < tblCnt; i++ {
		schema := md.MockSchema(2)
		name := fmt.Sprintf("%s%d", prefix, i)
		schema.Name = name
		names = append(names, name)
		wg.Add(1)
		go func(w *sync.WaitGroup) {
			_, err := dbi.CreateTable(schema)
			assert.Nil(t, err)
			w.Done()
		}(&wg)
	}
	wg.Wait()
	ids, err := dbi.TableIDs()
	assert.Nil(t, err)
	assert.Equal(t, tblCnt, len(ids))
	for _, name := range names {
		assert.True(t, dbi.HasTable(name))
	}
	t.Log(dbi.store.MetaInfo.String())
}

func TestCreateDuplicateTable(t *testing.T) {
	initDBTest()
	dbi := initDB()
	defer dbi.Close()
	schema := md.MockSchema(2)
	schema.Name = "t1"

	_, err := dbi.CreateTable(schema)
	assert.Nil(t, err)
	_, err = dbi.CreateTable(schema)
	assert.NotNil(t, err)
}

func TestAppend(t *testing.T) {
	initDBTest()
	dbi := initDB()
	schema := md.MockSchema(2)
	schema.Name = "mocktbl"
	_, err := dbi.CreateTable(schema)
	assert.Nil(t, err)
	blkCnt := 2
	rows := dbi.store.MetaInfo.Conf.BlockMaxRows * uint64(blkCnt)
	ck := chunk.MockChunk(schema.Types(), rows)
	assert.Equal(t, uint64(rows), ck.GetCount())
	logIdx := &md.LogIndex{
		ID:       uint64(9),
		Capacity: ck.GetCount(),
	}
	invalidName := "xxx"
	err = dbi.Append(invalidName, ck, logIdx)
	assert.NotNil(t, err)
	err = dbi.Append(schema.Name, ck, logIdx)
	assert.Nil(t, err)

	cols := []int{0, 1}
	iterOpts := &e.IterOptions{
		TableName: schema.Name,
		All:       true,
		ColIdxes:  cols,
	}

	blkCount := 0
	segCount := 0
	segIt, err := dbi.NewSegmentIter(iterOpts)
	assert.Nil(t, err)
	assert.NotNil(t, segIt)

	for segIt.Valid() {
		segCount++
		segH := segIt.GetSegmentHandle()
		assert.NotNil(t, segH)
		blkIt := segH.NewIterator()
		assert.NotNil(t, blkIt)
		for blkIt.Valid() {
			blkCount++
			blkIt.Next()
		}
		segIt.Next()
	}
	assert.Equal(t, 1, segCount)
	assert.Equal(t, blkCnt, blkCount)

	blkIt, err := dbi.NewBlockIter(iterOpts)
	assert.Nil(t, err)
	assert.NotNil(t, blkIt)

	blkCount = 0
	for blkIt.Valid() {
		blkCount++
		h := blkIt.GetBlockHandle()
		assert.NotNil(t, h)
		t.Log(h.GetColumn(0).String())
		t.Log(h.GetColumn(1).String())
		blkIt.Next()
	}
	assert.Equal(t, blkCnt, blkCount)
	dbi.Close()
}
