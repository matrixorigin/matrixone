package db

import (
	"github.com/stretchr/testify/assert"
	// "io/ioutil"
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"os"
	// "strconv"
	"testing"
)

var (
	TEST_DB_DIR = "/tmp/db_test"
)

func initDBTest() {
	os.RemoveAll(TEST_DB_DIR)
}

func initDB() *DB {
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

func TestIter(t *testing.T) {
	initDBTest()
	dbi := initDB()
	assert.NotNil(t, dbi)
	defer dbi.Close()
}
