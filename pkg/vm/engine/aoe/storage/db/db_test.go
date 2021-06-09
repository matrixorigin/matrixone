package db

import (
	"context"
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
		SegmentMaxBlocks: 2,
		BlockMaxRows:     100,
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
		ID:       uint64(0),
		Capacity: ck.GetCount(),
	}
	invalidName := "xxx"
	err = dbi.Append(invalidName, ck, logIdx)
	assert.NotNil(t, err)
	insertCnt := 4
	for i := 0; i < insertCnt; i++ {
		err = dbi.Append(schema.Name, ck, logIdx)
		assert.Nil(t, err)
	}

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
		segH.Close()
		assert.NotNil(t, blkIt)
		for blkIt.Valid() {
			blkCount++
			blkIt.Next()
		}
		blkIt.Close()
		segIt.Next()
	}
	segIt.Close()
	assert.Equal(t, insertCnt, segCount)
	assert.Equal(t, blkCnt*insertCnt, blkCount)

	blkIt, err := dbi.NewBlockIter(iterOpts)
	assert.Nil(t, err)
	assert.NotNil(t, blkIt)

	blkCount = 0
	for blkIt.Valid() {
		blkCount++
		h := blkIt.GetBlockHandle()
		assert.NotNil(t, h)
		h.Close()
		blkIt.Next()
	}
	blkIt.Close()
	assert.Equal(t, blkCnt*insertCnt, blkCount)
	time.Sleep(time.Duration(10) * time.Millisecond)
	t.Log(dbi.MTBufMgr.String())
	t.Log(dbi.SSTBufMgr.String())
	dbi.Close()
}

type InsertReq struct {
	Name     string
	Data     *chunk.Chunk
	LogIndex *md.LogIndex
}

func TestConcurrency(t *testing.T) {
	initDBTest()
	dbi := initDB()
	schema := md.MockSchema(2)
	schema.Name = "mockcon"
	_, err := dbi.CreateTable(schema)
	assert.Nil(t, err)
	blkCnt := dbi.store.MetaInfo.Conf.SegmentMaxBlocks
	rows := dbi.store.MetaInfo.Conf.BlockMaxRows * blkCnt
	baseCk := chunk.MockChunk(schema.Types(), rows)
	insertCh := make(chan *InsertReq)
	searchCh := make(chan *e.IterOptions)

	reqCtx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-searchCh:
				wg.Add(1)
				go func() {
					defer wg.Done()
					{
						segIt, err := dbi.NewSegmentIter(req)
						assert.Nil(t, err)
						if segIt == nil {
							return
						}
						segCnt := 0
						blkCnt := 0
						for segIt.Valid() {
							segCnt++
							sh := segIt.GetSegmentHandle()
							blkIt := sh.NewIterator()
							for blkIt.Valid() {
								blkCnt++
								blkIt.Next()
							}
							blkIt.Close()
							segIt.Next()
						}
						segIt.Close()
						// t.Logf("segCnt = %d", segCnt)
						// t.Logf("blkCnt = %d", blkCnt)
					}
				}()
			case req := <-insertCh:
				wg.Add(1)
				go func() {
					err := dbi.Append(req.Name, req.Data, req.LogIndex)
					assert.Nil(t, err)
					wg.Done()
				}()
			}
		}
	}(reqCtx)

	insertCnt := 8
	var wg2 sync.WaitGroup

	wg2.Add(1)
	go func() {
		defer wg2.Done()
		for i := 0; i < insertCnt; i++ {
			insertReq := &InsertReq{
				Name:     schema.Name,
				Data:     baseCk,
				LogIndex: &md.LogIndex{ID: uint64(i), Capacity: baseCk.GetCount()},
			}
			insertCh <- insertReq
		}
	}()

	cols := make([]int, 0)
	for i := 0; i < len(schema.ColDefs); i++ {
		cols = append(cols, i)
	}
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		reqCnt := rand.Intn(200) + 200
		for i := 0; i < reqCnt; i++ {
			searchReq := &e.IterOptions{
				TableName: schema.Name,
				All:       true,
				ColIdxes:  cols,
			}
			searchCh <- searchReq
		}
	}()

	wg2.Wait()
	cancel()
	wg.Wait()
	opts := &e.IterOptions{
		TableName: schema.Name,
		All:       true,
		ColIdxes:  cols,
	}
	segIt, err := dbi.NewSegmentIter(opts)
	segCnt := 0
	tblkCnt := 0
	for segIt.Valid() {
		segCnt++
		h := segIt.GetSegmentHandle()
		blkIt := h.NewIterator()
		h.Close()
		for blkIt.Valid() {
			tblkCnt++
			blkIt.Next()
		}
		blkIt.Close()
		segIt.Next()
	}
	segIt.Close()
	assert.Equal(t, insertCnt*int(blkCnt), tblkCnt)
	assert.Equal(t, insertCnt, segCnt)

	blkIt, err := dbi.NewBlockIter(opts)
	assert.Nil(t, err)
	tblkCnt = 0
	for blkIt.Valid() {
		tblkCnt++
		blkIt.Next()
	}
	assert.Equal(t, insertCnt*int(blkCnt), tblkCnt)
	blkIt.Close()
	time.Sleep(time.Duration(200) * time.Millisecond)

	t.Log(dbi.WorkersStatsString())
	t.Log(dbi.MTBufMgr.String())
	t.Log(dbi.SSTBufMgr.String())
	dbi.Close()
}

func TestGC(t *testing.T) {
	initDBTest()
	dbi := initDB()
	schema := md.MockSchema(2)
	schema.Name = "mockcon"
	_, err := dbi.CreateTable(schema)
	assert.Nil(t, err)
	blkCnt := dbi.store.MetaInfo.Conf.SegmentMaxBlocks
	rows := dbi.store.MetaInfo.Conf.BlockMaxRows * blkCnt
	baseCk := chunk.MockChunk(schema.Types(), rows)

	logIdx := &md.LogIndex{
		ID:       uint64(0),
		Capacity: baseCk.GetCount(),
	}

	insertCnt := uint64(8)

	var wg sync.WaitGroup
	{
		for i := uint64(0); i < insertCnt; i++ {
			wg.Add(1)
			go func() {
				dbi.Append(schema.Name, baseCk, logIdx)
				wg.Done()
			}()
		}
	}
	wg.Wait()
	time.Sleep(time.Duration(40) * time.Millisecond)
	t.Log(dbi.MTBufMgr.String())
	t.Log(dbi.SSTBufMgr.String())
	assert.Equal(t, int(blkCnt*insertCnt*2), dbi.SSTBufMgr.NodeCount()+dbi.MTBufMgr.NodeCount())
	dbi.Close()
}
