package db

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"os"
	"sync"
	"testing"
	"time"
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

func TestIter(t *testing.T) {
	initDBTest()
	dbi := initDB()
	assert.NotNil(t, dbi)
	defer dbi.Close()
}
