// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and

package metadata

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/internal/invariants"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
	ops "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/worker"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

var (
	mockBlockSize   int64 = 100
	mockSegmentSize int64 = 150
	mockFactory           = new(mockNameFactory)
)

func initTest(dir string, blockRows, segmentBlocks uint64, hasWal bool, cleanup bool) (*Catalog, Wal) {
	if cleanup {
		os.RemoveAll(dir)
	}
	cfg := new(CatalogCfg)
	cfg.Dir = dir
	cfg.BlockMaxRows, cfg.SegmentMaxBlocks = blockRows, segmentBlocks
	cfg.RotationFileMaxSize = 100 * int(common.M)

	var catalog *Catalog
	var indexWal Wal
	var err error
	if hasWal {
		driver, _ := logstore.NewBatchStore(dir, "driver", nil)
		indexWal = shard.NewManagerWithDriver(driver, false, wal.BrokerRole)
		catalog, err = OpenCatalogWithDriver(new(sync.RWMutex), cfg, driver, indexWal)
		if err != nil {
			panic(err)
		}
	} else {
		catalog, _ = OpenCatalog(new(sync.RWMutex), cfg)
	}
	catalog.Start()
	catalog.Compact(nil, nil)
	return catalog, indexWal
}

type mockNameFactory struct{}

func (factory *mockNameFactory) Encode(shardId uint64, name string) string {
	return fmt.Sprintf("mock:%s:%d", name, shardId)
}

func (factory *mockNameFactory) Decode(name string) (shardId uint64, oname string) {
	arr := strings.Split(name, ":")
	oname = arr[1]
	shardId, _ = strconv.ParseUint(arr[2], 10, 64)
	return
}

func (factory *mockNameFactory) Rename(name string, shardId uint64) string {
	_, oname := factory.Decode(name)
	return factory.Encode(shardId, oname)
}

func upgradeSeg(t *testing.T, seg *Segment, wg *sync.WaitGroup) func() {
	return func() {
		defer wg.Done()
		err := seg.SimpleUpgrade(mockSegmentSize, nil)
		assert.Nil(t, err)
	}
}

func createBlock(t *testing.T, tables int, gen *shard.MockIndexAllocator, shardId uint64, db *Database, blocks int, wg *sync.WaitGroup, nextNode *ants.Pool) func() {
	return func() {
		defer wg.Done()
		for k := 0; k < tables; k++ {
			schema := MockSchema(2)
			index := gen.Next(shardId)
			name := fmt.Sprintf("t%d", index.Id.Id)
			schema.Name = mockFactory.Encode(shardId, name)
			tbl, err := db.SimpleCreateTable(schema, index)
			assert.Nil(t, err)
			var prev *Block
			for i := 0; i < blocks; i++ {
				blk := tbl.SimpleGetOrCreateNextBlock(prev)
				blk.SetCount(tbl.Schema.BlockMaxRows)
				blk.SetIndexLocked(LogIndex{
					ShardId:  shardId,
					Id:       shard.SimpleIndexId(gen.Alloc(shardId)),
					Count:    tbl.Schema.BlockMaxRows,
					Capacity: tbl.Schema.BlockMaxRows,
				})
				blk.GetCommit().SetSize(mockBlockSize)
				err := blk.SimpleUpgrade(nil)
				assert.Nil(t, err)
				if prev != nil && blk.Segment != prev.Segment {
					wg.Add(1)
					if nextNode != nil {
						nextNode.Submit(upgradeSeg(t, prev.Segment, wg))
					} else {
						upgradeSeg(t, prev.Segment, wg)()
					}
				}
				prev = blk
			}
		}
	}
}

func doCompareCatalog(t *testing.T, expected, actual *Catalog) {
	assert.Equal(t, len(expected.Databases), len(actual.Databases))
	for _, expectedDB := range expected.Databases {
		actualDB := actual.Databases[expectedDB.Id]
		assert.Equal(t, len(expectedDB.TableSet), len(actualDB.TableSet))
		for _, expectedTable := range expectedDB.TableSet {
			actualTable := actualDB.TableSet[expectedTable.Id]
			assert.Equal(t, len(expectedTable.SegmentSet), len(actualTable.SegmentSet))
		}
	}
}

func doCompare(t *testing.T, expected, actual *databaseLogEntry) {
	assert.Equal(t, expected.LogRange, actual.LogRange)
	assert.Equal(t, len(expected.Database.TableSet), len(actual.Database.TableSet))

	for _, expectedTable := range expected.Database.TableSet {
		actualTable := actual.Database.TableSet[expectedTable.Id]
		assert.Equal(t, len(expectedTable.SegmentSet), len(actualTable.SegmentSet))
	}
}

func TestTable(t *testing.T) {
	cfg := new(CatalogCfg)
	cfg.Dir = "/tmp/testtable"
	os.RemoveAll(cfg.Dir)
	catalog, err := OpenCatalog(new(sync.RWMutex), cfg)
	assert.Nil(t, err)
	catalog.Start()
	defer catalog.Close()

	gen := shard.NewMockIndexAllocator()
	idx := gen.Next(0)

	dbName := "db1"
	db, err := catalog.SimpleCreateDatabase(dbName, idx)
	assert.Nil(t, err)

	schema := MockSchema(2)
	e := NewTableEntry(db, schema, uint64(0), nil)
	buf, err := e.Marshal()
	assert.Nil(t, err)

	e2 := NewEmptyTableEntry(db)
	err = e2.Unmarshal(buf)
	assert.Nil(t, err)

	assert.Equal(t, e, e2)
}

func TestCreateTable(t *testing.T) {
	dir := "/tmp/createtable"
	os.RemoveAll(dir)
	cfg := new(CatalogCfg)
	cfg.Dir = dir
	catalog, err := OpenCatalog(new(sync.RWMutex), cfg)
	assert.Nil(t, err)
	catalog.Start()
	defer catalog.Close()
	tableCnt := 20

	gen := shard.NewMockIndexAllocator()
	idx := gen.Next(0)
	dbName := "db1"
	db, err := catalog.SimpleCreateDatabase(dbName, idx)

	pool, _ := ants.NewPool(10)
	var wg sync.WaitGroup
	f := func(i int) func() {
		return func() {
			defer wg.Done()
			schema := MockSchema(2)
			schema.Name = fmt.Sprintf("m%d", i)
			t1, err := db.SimpleCreateTable(schema, gen.Next(0))
			assert.Nil(t, err)
			assert.NotNil(t, t1)

			rt1 := db.SimpleGetTableByName(schema.Name)
			assert.NotNil(t, rt1)
		}
	}

	for i := 0; i < tableCnt; i++ {
		wg.Add(1)
		pool.Submit(f(i))
	}

	wg.Wait()

	err = catalog.Close()
	assert.Nil(t, err)
}

func TestTables(t *testing.T) {
	dir := "/tmp/testtables"
	os.RemoveAll(dir)
	cfg := new(CatalogCfg)
	cfg.Dir = dir
	catalog, err := OpenCatalog(new(sync.RWMutex), cfg)
	assert.Nil(t, err)
	catalog.Start()
	defer catalog.Close()

	gen := shard.NewMockIndexAllocator()
	idx := gen.Next(0)
	dbName := "db1"
	db, err := catalog.SimpleCreateDatabase(dbName, idx)

	m1Cnt := 10
	m2Cnt := 5

	for i := 0; i < m1Cnt; i++ {
		name := fmt.Sprintf("m1_%d", i)
		schema := MockSchema(2)
		schema.Name = name
		_, err := db.SimpleCreateTable(schema, gen.Next(0))
		assert.Nil(t, err)
	}

	for i := 0; i < m2Cnt; i++ {
		name := fmt.Sprintf("m2_%d", i)
		schema := MockSchema(2)
		schema.Name = name
		_, err := db.SimpleCreateTable(schema, gen.Next(0))
		assert.Nil(t, err)
	}
}

func TestDropTable(t *testing.T) {
	dir := "/tmp/droptable"
	os.RemoveAll(dir)
	cfg := new(CatalogCfg)
	cfg.Dir = dir
	catalog, err := OpenCatalog(new(sync.RWMutex), cfg)
	assert.Nil(t, err)
	catalog.Start()

	gen := shard.NewMockIndexAllocator()
	idx := gen.Next(0)
	dbName := "db1"
	db, err := catalog.SimpleCreateDatabase(dbName, idx)

	schema1 := MockSchema(2)
	schema1.Name = "m1"

	t1, err := db.SimpleCreateTable(schema1, gen.Next(0))
	assert.Nil(t, err)
	assert.NotNil(t, t1)

	assert.False(t, t1.IsSoftDeleted())
	assert.True(t, t1.HasCommitted())

	t1_1, err := db.SimpleCreateTable(schema1, gen.Next(0))
	assert.NotNil(t, err)
	assert.Nil(t, t1_1)

	err = db.SimpleDropTableByName(t1.Schema.Name, gen.Next(0))
	assert.Nil(t, err)

	err = db.SimpleDropTableByName(t1.Schema.Name, gen.Next(0))
	assert.NotNil(t, err)

	schema2 := MockSchema(3)
	schema2.Name = schema1.Name
	t2, err := db.SimpleCreateTable(schema2, gen.Next(0))
	assert.Nil(t, err)
	assert.NotNil(t, t2)

	schema3 := MockSchema(4)
	schema3.Name = schema1.Name
	t3, err := db.SimpleCreateTable(schema3, gen.Next(0))
	assert.NotNil(t, err)
	assert.Nil(t, t3)

	err = db.SimpleDropTableByName(t1.Schema.Name, gen.Next(0))
	assert.Nil(t, err)

	err = db.SimpleDropTableByName(t1.Schema.Name, gen.Next(0))
	assert.NotNil(t, err)

	db.SimpleHardDeleteTable(t2.Id)

	t3, err = db.SimpleCreateTable(schema3, gen.Next(0))
	assert.Nil(t, err)
	assert.NotNil(t, t3)

	versions := 0
	node := db.nameNodes[t1.Schema.Name].GetNext().(*nameNode)
	for node != nil {
		entry := node.GetTable()
		t.Log(entry.PString(PPL0, 0))
		snode := node.GetNext()
		if snode == nil {
			node = nil
		} else {
			node = snode.(*nameNode)
		}
		versions += 1
	}
	assert.Equal(t, 3, versions)

	t.Log(catalog.PString(PPL1, 0))
	nodes := db.nameNodes[t1.Schema.Name]
	assert.Equal(t, 3, nodes.Length())
	t.Log(nodes.PString(PPL1))

	db.Compact(nil, nil)
	assert.Equal(t, 2, nodes.Length())
	t.Log(nodes.PString(PPL1))

	err = db.SimpleDropTableByName(t1.Schema.Name, gen.Next(0))
	assert.Nil(t, err)
	err = db.SimpleHardDeleteTable(t3.Id)
	assert.Nil(t, err)
	assert.Equal(t, 2, nodes.Length())
	db.Compact(nil, nil)
	t.Log(nodes.PString(PPL1))
	assert.Equal(t, 1, nodes.Length())
	assert.Equal(t, 1, len(db.nameNodes))

	err = db.SimpleHardDeleteTable(t1.Id)
	assert.Nil(t, err)
	db.Compact(nil, nil)
	t.Log(nodes.PString(PPL1))
	assert.Equal(t, 0, len(db.nameNodes))
	assert.Equal(t, 0, len(db.TableSet))

	index := gen.Next(99)
	t4, err := db.SimpleCreateTable(schema1, index)
	assert.Nil(t, err)
	assert.NotNil(t, t4)

	writer := NewDBSSWriter(db, dir, index.Id.Id)
	err = writer.PrepareWrite()
	assert.Nil(t, err)
	err = writer.CommitWrite()
	assert.Nil(t, err)

	loader := NewDBSSLoader(catalog, writer.name)
	err = loader.PrepareLoad()
	assert.Nil(t, err)
	err = loader.CommitLoad()
	assert.Nil(t, err)

	db, err = catalog.SimpleGetDatabaseByName(db.Name)
	assert.Nil(t, err)

	nodes = db.nameNodes[schema1.Name]
	assert.NotNil(t, nodes)
	assert.Equal(t, 1, nodes.Length())

	// TODO: add catalog compact
	// catalog.Compact()
	// assert.Equal(t, 1, len(catalog.TableSet))
	// assert.Equal(t, 1, nodes.Length())
	// t.Log(nodes.PString(PPL1))

	t.Log(catalog.PString(PPL1, 0))
	catalog.Close()
}

func TestSegment(t *testing.T) {
	dir := "/tmp/testsegment"
	os.RemoveAll(dir)

	cfg := new(CatalogCfg)
	cfg.Dir = dir
	catalog, err := OpenCatalog(new(sync.RWMutex), cfg)
	assert.Nil(t, err)
	catalog.Start()
	defer catalog.Close()

	gen := shard.NewMockIndexAllocator()
	idx := gen.Next(0)
	dbName := "db1"
	db, err := catalog.SimpleCreateDatabase(dbName, idx)

	pool, _ := ants.NewPool(10)
	var wg sync.WaitGroup

	f := func(i int) func() {
		return func() {
			defer wg.Done()
			schema := MockSchema(2)
			schema.Name = fmt.Sprintf("m%d", i)

			t1, err := db.SimpleCreateTable(schema, gen.Next(0))
			assert.Nil(t, err)
			assert.NotNil(t, t1)

			t1.SimpleCreateSegment()
			// t.Log(segment.String())
			t1.SimpleCreateSegment()
			// t.Log(segment.String())

			schema2 := MockSchema(2)
			schema2.Name = fmt.Sprintf("m%d", i+100)
			t2, err := db.SimpleCreateTable(schema2, gen.Next(0))
			t2.SimpleCreateSegment()
			// t.Log(segment.String())

			err = db.SimpleDropTableByName(t1.Schema.Name, gen.Next(0))
			assert.Nil(t, err)
		}
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		pool.Submit(f(i))
	}
	wg.Wait()
}

func TestBlock(t *testing.T) {
	dir := "/tmp/testblock"
	os.RemoveAll(dir)

	cfg := new(CatalogCfg)
	cfg.BlockMaxRows = uint64(10)
	cfg.SegmentMaxBlocks = uint64(4)
	cfg.Dir = dir
	catalog, err := OpenCatalog(new(sync.RWMutex), cfg)
	assert.Nil(t, err)
	catalog.Start()
	defer catalog.Close()

	gen := shard.NewMockIndexAllocator()
	idx := gen.Next(0)
	dbName := "db1"
	db, err := catalog.SimpleCreateDatabase(dbName, idx)

	pool, _ := ants.NewPool(5)
	var wg sync.WaitGroup
	f := func(i int) func() {
		return func() {
			defer wg.Done()
			schema := MockSchema(2)
			schema.Name = fmt.Sprintf("m%d", i)

			t1, err := db.SimpleCreateTable(schema, gen.Next(0))
			assert.Nil(t, err)
			assert.NotNil(t, t1)

			s1 := t1.SimpleCreateSegment()
			s1.RLock()
			assert.True(t, s1.HasCommitted())
			s1.RUnlock()

			rt1 := db.SimpleGetTableByName(schema.Name)
			assert.NotNil(t, rt1)

			b1 := s1.SimpleCreateBlock()
			b1.RLock()
			assert.True(t, b1.HasCommitted())
			b1.RUnlock()
		}
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		pool.Submit(f(i))
	}
	wg.Wait()

	schema := MockSchema(2)
	schema.Name = "mm"
	t2, err := db.SimpleCreateTable(schema, gen.Next(0))
	assert.Nil(t, err)
	var prev *Block
	for i := 0; i < 2*int(cfg.SegmentMaxBlocks)+int(cfg.SegmentMaxBlocks)/2; i++ {
		blk := t2.SimpleGetOrCreateNextBlock(prev)
		assert.NotNil(t, blk)
		prev = blk
	}
	assert.Equal(t, 3, len(t2.SegmentSet))
}

type mockGetSegmentedHB struct {
	db *Database
	t  *testing.T
}

func (hb *mockGetSegmentedHB) OnStopped() {}
func (hb *mockGetSegmentedHB) OnExec() {
	hb.db.ForLoopTables(hb.processTable)
}
func (hb *mockGetSegmentedHB) processTable(tbl *Table) error {
	// tbl.RLock()
	// defer tbl.RUnlock()
	// hb.t.Log(tbl.PString(PPL0))
	return nil
}

func TestReplay(t *testing.T) {
	dir := "/tmp/testreplay"
	os.RemoveAll(dir)

	hbInterval := time.Duration(4) * time.Millisecond
	if invariants.RaceEnabled {
		hbInterval *= 3
	}

	cfg := new(CatalogCfg)
	cfg.Dir = dir
	cfg.BlockMaxRows, cfg.SegmentMaxBlocks = uint64(100), uint64(4)
	cfg.RotationFileMaxSize = 100 * int(common.K)
	catalog, _ := OpenCatalog(new(sync.RWMutex), cfg)
	catalog.Start()

	gen := shard.NewMockIndexAllocator()
	idx := gen.Next(0)
	dbName := "db1"
	db, _ := catalog.SimpleCreateDatabase(dbName, idx)

	mockShards := 5
	createBlkWorker, _ := ants.NewPool(mockShards)
	upgradeSegWorker, _ := ants.NewPool(4)

	getSegmentedIdWorker := ops.NewHeartBeater(hbInterval, &mockGetSegmentedHB{db: db, t: t})
	getSegmentedIdWorker.Start()

	var wg sync.WaitGroup

	mockBlocks := cfg.SegmentMaxBlocks*2 + cfg.SegmentMaxBlocks/2

	for i := 0; i < mockShards; i++ {
		wg.Add(1)
		createBlkWorker.Submit(createBlock(t, 1, gen, uint64(i), db, int(mockBlocks), &wg, upgradeSegWorker))
	}
	wg.Wait()
	getSegmentedIdWorker.Stop()
	t.Log(catalog.PString(PPL0, 0))

	catalog.Close()
}

func TestAppliedIndex(t *testing.T) {
	dir := "/tmp/testappliedindex"
	os.RemoveAll(dir)
	blkRows, segBlks := uint64(10), uint64(2)
	catalog, indexWal := MockCatalogAndWal(dir, blkRows, segBlks)
	defer indexWal.Close()
	defer catalog.Close()

	gen := shard.NewMockIndexAllocator()
	idx := gen.Next(0)
	dbName := "db1"
	db, err := catalog.SimpleCreateDatabase(dbName, idx)

	index := gen.Next(0)
	indexWal.SyncLog(index)
	tbl := MockTable(db, nil, 0, index)
	assert.NotNil(t, tbl)
	indexWal.Checkpoint(index)
	testutils.WaitExpect(10, func() bool {
		return indexWal.GetShardCheckpointId(0) == index.Id.Id
	})
	assert.Equal(t, index.Id.Id, indexWal.GetShardCheckpointId(0))

	blk, prevSeg := tbl.SimpleCreateBlock()
	assert.Nil(t, prevSeg)
	assert.NotNil(t, blk)
	opIdx := gen.Alloc(0)
	index = &LogIndex{
		Id:       shard.SimpleIndexId(opIdx),
		Count:    blkRows,
		Capacity: blkRows * 3 / 2,
	}
	blk.SetIndexLocked(*index)
	blk.SetCount(blkRows)
	err = blk.SimpleUpgrade(nil)
	assert.Nil(t, err)
	snip := blk.ConsumeSnippet(false)
	t.Log(snip.String())
	indexWal.Checkpoint(snip)

	blk, prevSeg = tbl.SimpleCreateBlock()
	assert.Nil(t, prevSeg)
	assert.NotNil(t, blk)

	index = &LogIndex{
		Id:       shard.SimpleIndexId(opIdx),
		Start:    blkRows,
		Count:    blkRows / 2,
		Capacity: blkRows * 3 / 2,
	}

	indexWal.SyncLog(index)
	blk.SetIndexLocked(*index)
	snip = blk.ConsumeSnippet(false)
	t.Log(snip.String())
	indexWal.Checkpoint(snip)
	testutils.WaitExpect(20, func() bool {
		return indexWal.GetShardCheckpointId(0) == index.Id.Id
	})
	assert.Equal(t, index.Id.Id, indexWal.GetShardCheckpointId(0))

	blk.SetCount(blkRows)
	opIdx = gen.Alloc(0)
	index = &LogIndex{
		Id:       shard.SimpleIndexId(opIdx),
		Start:    0,
		Count:    blkRows / 2,
		Capacity: blkRows / 2,
	}
	indexWal.SyncLog(index)
	blk.SetIndexLocked(*index)
	err = blk.SimpleUpgrade(nil)
	assert.Nil(t, err)
	snip = blk.ConsumeSnippet(false)
	indexWal.Checkpoint(snip)

	testutils.WaitExpect(20, func() bool {
		return index.Id.Id == indexWal.GetShardCheckpointId(0)
	})
	assert.Equal(t, index.Id.Id, indexWal.GetShardCheckpointId(0))

	seg := blk.Segment
	err = seg.SimpleUpgrade(mockSegmentSize, nil)
	assert.Nil(t, err)

	opIdx = gen.Alloc(0)
	index = &LogIndex{
		Id: shard.SimpleIndexId(opIdx),
	}
	indexWal.SyncLog(index)
	tbl.SimpleSoftDelete(index)
	snip = blk.ConsumeSnippet(false)
	indexWal.Checkpoint(index)

	testutils.WaitExpect(20, func() bool {
		return index.Id.Id == indexWal.GetShardCheckpointId(0)
	})
	assert.Equal(t, index.Id.Id, indexWal.GetShardCheckpointId(0))

	tbl.HardDelete()
	assert.Equal(t, index.Id.Id, tbl.LatestLogIndex().Id.Id)
}

func TestUpgrade(t *testing.T) {
	dir := "/tmp/testupgradeblock"
	os.RemoveAll(dir)
	delta := DefaultCheckpointDelta
	DefaultCheckpointDelta = uint64(10)
	defer func() {
		DefaultCheckpointDelta = delta
	}()

	cfg := new(CatalogCfg)
	cfg.Dir = dir
	cfg.BlockMaxRows, cfg.SegmentMaxBlocks = uint64(100), uint64(2)
	cfg.RotationFileMaxSize = 20 * int(common.K)
	catalog, _ := OpenCatalog(new(sync.RWMutex), cfg)
	catalog.Start()

	gen := shard.NewMockIndexAllocator()
	idx := gen.Next(0)
	dbName := "db1"
	db, err := catalog.SimpleCreateDatabase(dbName, idx)

	pool1, _ := ants.NewPool(2)
	pool2, _ := ants.NewPool(2)
	pool3, _ := ants.NewPool(2)
	pool4, _ := ants.NewPool(2)
	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup
	var wg3 sync.WaitGroup
	var wg4 sync.WaitGroup

	var mu sync.Mutex
	traceSegments := make(map[uint64]int)
	upgradedBlocks := uint64(0)
	upgradedSegments := uint64(0)
	segCnt, blockCnt := 20, int(cfg.SegmentMaxBlocks)

	updateTrace := func(tableId, segmentId uint64) func() {
		return func() {
			defer wg4.Done()

			mu.Lock()
			traceSegments[segmentId]++
			cnt := traceSegments[segmentId]
			mu.Unlock()
			if cnt == blockCnt {
				segment, err := db.SimpleGetSegment(tableId, segmentId)
				assert.Nil(t, err)
				segment.SimpleUpgrade(mockSegmentSize, nil)
				atomic.AddUint64(&upgradedSegments, uint64(1))
				// segment.RLock()
				// t.Log(segment.PString(PPL1))
				// defer segment.RUnlock()
			}
		}
	}

	upgradeBlk := func(tableId, segmentId, blockId uint64) func() {
		return func() {
			defer wg3.Done()
			block, err := db.SimpleGetBlock(tableId, segmentId, blockId)
			assert.Nil(t, err)

			block.RLock()
			assert.Equal(t, OpCreate, block.CommitInfo.Op)
			block.RUnlock()
			block.SimpleUpgrade(nil)
			atomic.AddUint64(&upgradedBlocks, uint64(1))
			// block.RLock()
			// t.Log(block.PString())
			// block.RUnlock()

			wg4.Add(1)
			pool4.Submit(updateTrace(block.Segment.Table.Id, block.Segment.Id))
		}
	}

	schema := MockSchema(2)
	schema.Name = "mock"

	t1, err := db.SimpleCreateTable(schema, gen.Next(0))
	assert.Nil(t, err)
	assert.NotNil(t, t1)

	createBlk := func(tableId, segmentId uint64) func() {
		return func() {
			defer wg2.Done()
			segment, err := db.SimpleGetSegment(tableId, segmentId)
			assert.Nil(t, err)
			block := segment.SimpleCreateBlock()
			assert.NotNil(t, block)
			wg3.Add(1)
			pool3.Submit(upgradeBlk(block.Segment.Table.Id, block.Segment.Id, block.Id))
		}
	}
	createSeg := func(tableId uint64) func() {
		return func() {
			defer wg1.Done()
			s := t1.SimpleCreateSegment()
			assert.NotNil(t, s)
			for i := 0; i < blockCnt; i++ {
				wg2.Add(1)
				pool2.Submit(createBlk(s.Table.Id, s.Id))
			}
		}
	}

	for i := 0; i < segCnt; i++ {
		wg1.Add(1)
		pool1.Submit(createSeg(t1.Id))
	}
	wg1.Wait()
	wg2.Wait()
	wg3.Wait()
	wg4.Wait()
	assert.Equal(t, segCnt*blockCnt, int(upgradedBlocks))
	assert.Equal(t, segCnt, int(upgradedSegments))

	catalog.Close()
	sequence := catalog.Sequence

	t.Log("Start replay")
	now := time.Now()
	catalog, err = OpenCatalog(new(sync.RWMutex), cfg)
	assert.Nil(t, err)
	t.Log(time.Since(now))

	assert.Equal(t, sequence.nextCommitId, catalog.Sequence.nextCommitId)
	assert.Equal(t, sequence.nextTableId, catalog.Sequence.nextTableId)
	assert.Equal(t, sequence.nextSegmentId, catalog.Sequence.nextSegmentId)
	assert.Equal(t, sequence.nextBlockId, catalog.Sequence.nextBlockId)

	catalog.Start()
	db, err = catalog.SimpleGetDatabaseByName(dbName)
	assert.Nil(t, err)

	tmp := db.SimpleGetTable(t1.Id)
	assert.NotNil(t, tmp)
	tmp = db.SimpleGetTableByName(t1.Schema.Name)
	assert.NotNil(t, tmp)

	err = db.SimpleDropTableByName(t1.Schema.Name, gen.Next(0))
	assert.Nil(t, err)
	err = db.SimpleDropTableByName(t1.Schema.Name, gen.Next(0))
	assert.NotNil(t, err)
	err = db.SimpleHardDeleteTable(t1.Id)
	assert.Nil(t, err)

	viewId := uint64(40)
	filter := new(Filter)
	filter.tableFilter = newCommitFilter()
	filter.tableFilter.AddChecker(createCommitIdChecker(viewId))
	filter.segmentFilter = filter.tableFilter
	filter.blockFilter = filter.tableFilter
	view := newDatabaseLogEntry(0, viewId)
	db.fillView(filter, view.Database)
	for _, tbl := range view.Database.TableSet {
		t.Log(len(tbl.SegmentSet))
	}
	view = db.LatestView()
	assert.Equal(t, 1, len(view.Database.TableSet))

	sequence = catalog.Sequence
	catalog.Close()
	return

	catalog, err = OpenCatalog(new(sync.RWMutex), cfg)
	assert.Nil(t, err)
	catalog.Start()
	// t.Logf("%d - %d", catalog.Store.GetSyncedId(), catalog.Store.GetCheckpointId())
	assert.Equal(t, sequence.nextCommitId, catalog.Sequence.nextCommitId)
	assert.Equal(t, sequence.nextTableId, catalog.Sequence.nextTableId)
	assert.Equal(t, sequence.nextSegmentId, catalog.Sequence.nextSegmentId)
	assert.Equal(t, sequence.nextBlockId, catalog.Sequence.nextBlockId)
	catalog.Close()
}

func TestOpen(t *testing.T) {
	dir := "/tmp/meta/testopen"
	os.RemoveAll(dir)
	err := os.MkdirAll(dir, os.FileMode(0755))
	assert.Nil(t, err)
	cfg := new(CatalogCfg)
	cfg.Dir = dir
	cfg.BlockMaxRows, cfg.SegmentMaxBlocks = uint64(100), uint64(100)
	catalog, err := OpenCatalog(new(sync.RWMutex), cfg)
	assert.Nil(t, err)
	catalog.Start()
	catalog.Close()

	catalog, err = OpenCatalog(new(sync.RWMutex), cfg)
	assert.Nil(t, err)
	catalog.Start()

	gen := shard.NewMockIndexAllocator()
	idx := gen.Next(0)
	dbName := "db1"
	db, err := catalog.SimpleCreateDatabase(dbName, idx)

	schema := MockSchema(2)
	_, err = db.SimpleCreateTable(schema, nil)
	assert.Nil(t, err)
	catalog.Close()
}

func TestCatalog2(t *testing.T) {
	dir := "/tmp/testcatalog2"
	os.RemoveAll(dir)
	delta := DefaultCheckpointDelta
	DefaultCheckpointDelta = uint64(400000)
	defer func() {
		DefaultCheckpointDelta = delta
	}()

	cfg := new(CatalogCfg)
	cfg.Dir = dir
	cfg.BlockMaxRows, cfg.SegmentMaxBlocks = uint64(100), uint64(20)
	cfg.RotationFileMaxSize = 10 * int(common.K)
	catalog := NewCatalog(new(sync.RWMutex), cfg)
	catalog.Start()

	gen := shard.NewMockIndexAllocator()
	dbName := "db1"
	db, err := catalog.SimpleCreateDatabase(dbName, gen.Next(0))
	assert.Nil(t, err)

	pool, _ := ants.NewPool(10)
	var wg sync.WaitGroup
	f := func(i int) func() {
		return func() {
			defer wg.Done()
			schema := MockSchema(2)
			schema.Name = fmt.Sprintf("m%d", i)

			t1, err := db.SimpleCreateTable(schema, gen.Next(0))
			assert.Nil(t, err)
			assert.NotNil(t, t1)

			s1 := t1.SimpleCreateSegment()
			s1.RLock()
			assert.True(t, s1.HasCommitted())
			s1.RUnlock()

			rt1 := db.SimpleGetTableByName(schema.Name)
			assert.NotNil(t, rt1)

			b1 := s1.SimpleCreateBlock()
			b1.RLock()
			assert.True(t, b1.HasCommitted())
			b1.RUnlock()
		}
	}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		pool.Submit(f(i))
	}
	wg.Wait()

	catalog.Close()
}

func TestDatabases1(t *testing.T) {
	dir := "/tmp/metadata/testdbs1"
	blockRows, segmentBlocks := uint64(100), uint64(2)
	catalog, _ := initTest(dir, blockRows, segmentBlocks, false, true)
	defer catalog.Close()

	mockShards := 4
	createBlkWorker, _ := ants.NewPool(mockShards)
	upgradeSegWorker, _ := ants.NewPool(20)

	now := time.Now()
	var wg sync.WaitGroup

	mockBlocks := segmentBlocks*2 + segmentBlocks/2

	gen := shard.NewMockIndexAllocator()
	for i := 0; i < mockShards; i++ {
		shardId := uint64(i)
		idx := gen.Next(shardId)
		dbName := fmt.Sprintf("db%d", shardId)
		db, err := catalog.SimpleCreateDatabase(dbName, idx)
		assert.Nil(t, err)
		wg.Add(1)
		createBlkWorker.Submit(createBlock(t, 2, gen, shardId, db, int(mockBlocks), &wg, upgradeSegWorker))
	}
	wg.Wait()
	t.Logf("mock metadata takes: %s", time.Since(now))
	t.Log(catalog.PString(PPL0, 0))

	now = time.Now()
	var viewsMu sync.Mutex
	views := make(map[uint64]*databaseLogEntry)
	for i := 0; i < mockShards; i++ {
		wg.Add(1)
		go func(shardId uint64) {
			defer wg.Done()
			dbName := fmt.Sprintf("db%d", shardId)
			db, err := catalog.SimpleGetDatabaseByName(dbName)
			assert.Nil(t, err)
			writer := NewDBSSWriter(db, dir, gen.Get(shardId))
			err = writer.PrepareWrite()
			assert.Nil(t, err)
			err = writer.CommitWrite()
			assert.Nil(t, err)
			viewsMu.Lock()
			views[shardId] = writer.view
			viewsMu.Unlock()
		}(uint64(i))
	}
	wg.Wait()

	files, err := ioutil.ReadDir(dir)
	assert.Nil(t, err)
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".meta") {
			continue
		}
		loader := NewDBSSLoader(catalog, filepath.Join(dir, file.Name()))
		err = loader.PrepareLoad()
		assert.Nil(t, err)
		err = loader.CommitLoad()
		assert.Nil(t, err)
		db, err := catalog.SimpleGetDatabaseByName(loader.view.Database.Name)
		assert.Nil(t, err)
		expected := db.View(gen.Get(db.GetShardId()))
		doCompare(t, expected, loader.view)
		// t.Logf("shardId-%d: %s", loader.View.LogRange.ShardId, loader.View.Catalog.PString(PPL0))
	}
	t.Logf("takes %s", time.Since(now))
	t.Log(catalog.PString(PPL0, 0))

	for shardId, allocator := range gen.Shards {
		dbName := fmt.Sprintf("db%d", shardId)
		db, err := catalog.SimpleGetDatabaseByName(dbName)
		assert.Nil(t, err)
		writer := NewDBSSWriter(db, dir, allocator.Get())
		err = writer.PrepareWrite()
		assert.Nil(t, err)
		checker := func(info *CommitInfo) bool {
			// logutil.Infof("shardId-%d, %s", shardId, info.PString(PPL0))
			assert.Equal(t, shardId, info.GetShardId())
			return true
		}
		processor := new(LoopProcessor)
		processor.BlockFn = func(block *Block) error {
			checker(block.GetCommit())
			return nil
		}
		processor.TableFn = func(table *Table) error {
			checker(table.GetCommit())
			return nil
		}
		writer.view.Database.RecurLoopLocked(processor)
		t.Log(writer.view.Database.PString(PPL0, 0))
	}
}

type testCfg struct {
	shardId uint64
	blocks  int
	tables  int
}

func TestDatabases2(t *testing.T) {
	dir := "/tmp/metadata/testdbs2"
	blockRows, segmentBlocks := uint64(100), uint64(2)
	catalog, _ := initTest(dir, blockRows, segmentBlocks, false, true)
	gen := shard.NewMockIndexAllocator()
	cfg1 := testCfg{
		shardId: uint64(77),
		blocks:  int(segmentBlocks*2 + segmentBlocks/2),
		tables:  2,
	}
	cfg2 := testCfg{
		shardId: uint64(88),
		blocks:  int(segmentBlocks) / 2,
		tables:  3,
	}
	db1, err := catalog.SimpleCreateDatabase("db1", gen.Next(cfg1.shardId))
	assert.Nil(t, err)
	db2, err := catalog.SimpleCreateDatabase("db2", gen.Next(cfg2.shardId))
	assert.Nil(t, err)

	wg := new(sync.WaitGroup)
	wg.Add(2)
	w1, _ := ants.NewPool(4)
	w2, _ := ants.NewPool(4)
	w1.Submit(createBlock(t, cfg1.tables, gen, cfg1.shardId, db1, cfg1.blocks, wg, w2))
	w1.Submit(createBlock(t, cfg2.tables, gen, cfg2.shardId, db2, cfg2.blocks, wg, w2))
	wg.Wait()

	blockCnt := 0
	blockCntFn := func(block *Block) error {
		blockCnt++
		return nil
	}
	blockCntOp := newBlockProcessor(blockCntFn)

	index1_0 := gen.Get(cfg1.shardId)
	view1 := db1.View(index1_0)
	assert.Equal(t, cfg1.tables, len(view1.Database.TableSet))
	err = view1.Database.RecurLoopLocked(blockCntOp)
	assert.Nil(t, err)
	assert.Equal(t, cfg1.blocks*cfg1.tables, blockCnt)

	blockCnt = 0
	view2 := db2.View(gen.Get(cfg2.shardId))
	assert.Equal(t, cfg2.tables, len(view2.Database.TableSet))
	err = view2.Database.RecurLoopLocked(blockCntOp)
	assert.Nil(t, err)
	assert.Equal(t, cfg2.blocks*cfg2.tables, blockCnt)

	cfg1_1 := testCfg{
		shardId: cfg1.shardId,
		blocks:  1,
		tables:  1,
	}
	wg.Add(1)
	w1.Submit(createBlock(t, cfg1_1.tables, gen, cfg1_1.shardId, db1, cfg1_1.blocks, wg, w2))
	wg.Wait()

	index1_1 := gen.Get(cfg1_1.shardId)
	view1_0 := db1.View(index1_0)
	doCompare(t, view1, view1_0)

	blockCnt = 0
	view1_1 := db1.View(index1_1)
	assert.Equal(t, cfg1.tables+cfg1_1.tables, len(view1_1.Database.TableSet))
	err = view1_1.Database.RecurLoopLocked(blockCntOp)
	assert.Nil(t, err)
	assert.Equal(t, cfg1.blocks*cfg1.tables+cfg1_1.blocks*cfg1_1.tables, blockCnt)

	ids := view1_0.Database.SimpleGetTableIds()

	table := db1.SimpleGetTable(ids[0])
	deleteIndex := new(LogIndex)
	deleteIndex.ShardId = cfg1.shardId
	deleteIndex.Id = shard.SimpleIndexId(gen.Alloc(cfg1.shardId))
	err = table.SimpleSoftDelete(deleteIndex)
	assert.Nil(t, err)

	view1_0_1 := db1.View(index1_0)
	doCompare(t, view1_0, view1_0_1)

	view1_1_1 := db1.View(index1_1)
	doCompare(t, view1_1, view1_1_1)

	index1_2 := gen.Get(cfg1.shardId)
	view1_2 := db1.View(index1_2)
	assert.Equal(t, len(view1_1_1.Database.TableSet)-1, len(view1_2.Database.TableSet))

	t.Log(catalog.PString(PPL0, 0))

	writer := NewDBSSWriter(db1, dir, index1_0)
	err = writer.PrepareWrite()
	assert.Nil(t, err)
	doCompare(t, view1_0, writer.view)
	err = writer.CommitWrite()
	assert.Nil(t, err)

	t.Log(db1.GetCount())
	loader := NewDBSSLoader(catalog, writer.name)
	err = loader.PrepareLoad()
	assert.Nil(t, err)
	err = loader.CommitLoad()
	assert.Nil(t, err)
	db1, err = catalog.SimpleGetDatabaseByName(db1.Name)
	assert.Nil(t, err)
	view1_0 = db1.View(index1_0)
	doCompare(t, view1_0, loader.view)

	db1, err = catalog.SimpleGetDatabaseByName("db1")
	assert.Nil(t, err)
	view := db1.View(index1_0)
	assert.Equal(t, view1.LogRange, view.LogRange)
	assert.Equal(t, len(view1.Database.TableSet), len(view.Database.TableSet))
	db1, err = catalog.SimpleGetDatabaseByName("db1")
	assert.Nil(t, err)
	t.Log(db1.GetCount())
	catalog.Compact(nil, nil)
	catalog.Close()

	catalog2, _ := initTest(dir, blockRows, segmentBlocks, false, false)
	db1, err = catalog2.SimpleGetDatabaseByName("db1")
	assert.Nil(t, err)
	t.Log(db1.GetCount())
	catalog2.Close()

	doCompareCatalog(t, catalog, catalog2)
}

func TestSplit(t *testing.T) {
	dir := "/tmp/metadata/testsplit"
	catalog, _ := initTest(dir, uint64(100), uint64(2), false, true)

	gen := shard.NewMockIndexAllocator()
	wg := new(sync.WaitGroup)
	w1, _ := ants.NewPool(4)
	w2, _ := ants.NewPool(4)

	shardId := uint64(66)
	db, err := catalog.SimpleCreateDatabase("db1", gen.Next(shardId))
	assert.Nil(t, err)
	wg.Add(4)
	w1.Submit(createBlock(t, 1, gen, shardId, db, 0, wg, w2))
	w1.Submit(createBlock(t, 1, gen, shardId, db, 1, wg, w2))
	w1.Submit(createBlock(t, 1, gen, shardId, db, 2, wg, w2))
	w1.Submit(createBlock(t, 1, gen, shardId, db, 3, wg, w2))

	wg.Wait()

	index := gen.Get(shardId)
	t.Logf("index=%d", index)
	assert.Equal(t, int64(550), db.GetSize())
	assert.Equal(t, int64(600), db.GetCount())

	_, _, keys, ctx, err := catalog.SplitCheck(uint64(250), index, db.Name)
	assert.Nil(t, err)
	t.Log(len(keys))
	// assert.Equal(t, 3, len(keys))
	spec := NewEmptyShardSplitSpec()
	err = spec.Unmarshal(ctx)
	assert.Nil(t, err)
	t.Log(spec.String())
	assert.Equal(t, spec.Name, db.Name)
	assert.Equal(t, spec.Index, index)
	assert.Equal(t, len(spec.Specs), 4)

	dbSpecs := make([]DBSpec, len(keys))
	for i, _ := range dbSpecs {
		dbSpec := DBSpec{}
		dbSpec.ShardId = uint64(100) + uint64(i)
		dbSpec.Name = fmt.Sprintf("db-%d", dbSpec.ShardId)
		dbSpecs[i] = dbSpec
	}
	splitIndex := gen.Next(shardId)

	splitter := NewShardSplitter(catalog, spec, dbSpecs, splitIndex, mockFactory)
	err = splitter.Prepare()
	assert.Nil(t, err)
	err = splitter.Commit()
	assert.Nil(t, err)
	processor := new(LoopProcessor)
	tables := 0
	processor.TableFn = func(table *Table) error {
		tables++
		return nil
	}
	err = catalog.RecurLoopLocked(processor)
	assert.Nil(t, err)
	assert.Equal(t, tables, 9)
	catalog.Compact(nil, nil)
	tables = 0
	err = catalog.RecurLoopLocked(processor)
	assert.Equal(t, tables, 9)
	t.Log(catalog.PString(PPL0, 0))
	catalog.Close()
	return

	t.Log("--------------------------------------")
	catalog2, _ := initTest(dir, uint64(100), uint64(2), false, false)
	tables = 0
	err = catalog2.RecurLoopLocked(processor)
	assert.Nil(t, err)
	t.Log(catalog2.PString(PPL0, 0))
	assert.Equal(t, tables, 9)
	doCompareCatalog(t, catalog, catalog2)
	catalog2.Close()
}
