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

type mockIdAllocator struct {
	sync.RWMutex
	driver map[uint64]*common.IdAlloctor
}

func newMockAllocator() *mockIdAllocator {
	return &mockIdAllocator{
		driver: make(map[uint64]*common.IdAlloctor),
	}
}

func (alloc *mockIdAllocator) get(shardId uint64) uint64 {
	alloc.RLock()
	defer alloc.RUnlock()
	shardAlloc := alloc.driver[shardId]
	if shardAlloc == nil {
		return 0
	}
	return shardAlloc.Get()
}

func (alloc *mockIdAllocator) alloc(shardId uint64) uint64 {
	alloc.Lock()
	defer alloc.Unlock()
	shardAlloc := alloc.driver[shardId]
	if shardAlloc == nil {
		shardAlloc = new(common.IdAlloctor)
		alloc.driver[shardId] = shardAlloc
	}
	return shardAlloc.Alloc()
}

func upgradeSeg(t *testing.T, seg *Segment, wg *sync.WaitGroup) func() {
	return func() {
		defer wg.Done()
		err := seg.SimpleUpgrade(mockSegmentSize, nil)
		assert.Nil(t, err)
	}
}

func createBlock(t *testing.T, tables int, idAlloc *mockIdAllocator, shardId uint64, catalog *Catalog, blocks int, wg *sync.WaitGroup, nextNode *ants.Pool) func() {
	return func() {
		defer wg.Done()
		for k := 0; k < tables; k++ {
			schema := MockSchema(2)
			id := idAlloc.alloc(shardId)
			name := fmt.Sprintf("t%d", id)
			schema.Name = mockFactory.Encode(shardId, name)
			index := shard.Index{
				ShardId: shardId,
				Id:      shard.SimpleIndexId(id),
			}
			tbl, err := catalog.SimpleCreateTable(schema, &index)
			assert.Nil(t, err)
			var prev *Block
			for i := 0; i < blocks; i++ {
				blk := tbl.SimpleGetOrCreateNextBlock(prev)
				blk.SetCount(tbl.Schema.BlockMaxRows)
				blk.SetIndexLocked(LogIndex{
					ShardId:  shardId,
					Id:       shard.SimpleIndexId(idAlloc.alloc(shardId)),
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
	assert.Equal(t, len(expected.TableSet), len(actual.TableSet))
	for _, expectedTable := range expected.TableSet {
		actualTable := actual.TableSet[expectedTable.Id]
		assert.Equal(t, len(expectedTable.SegmentSet), len(actualTable.SegmentSet))
	}
}

func doCompare(t *testing.T, expected, actual *catalogLogEntry) {
	assert.Equal(t, expected.LogRange, actual.LogRange)
	assert.Equal(t, len(expected.Catalog.TableSet), len(actual.Catalog.TableSet))
	for _, expectedTable := range expected.Catalog.TableSet {
		actualTable := actual.Catalog.TableSet[expectedTable.Id]
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
	schema := MockSchema(2)
	e := NewTableEntry(catalog, schema, uint64(0), nil)
	buf, err := e.Marshal()
	assert.Nil(t, err)

	e2 := NewEmptyTableEntry(catalog)
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

	pool, _ := ants.NewPool(10)
	var wg sync.WaitGroup
	f := func(i int) func() {
		return func() {
			defer wg.Done()
			schema := MockSchema(2)
			schema.Name = fmt.Sprintf("m%d", i)
			t1, err := catalog.SimpleCreateTable(schema, nil)
			assert.Nil(t, err)
			assert.NotNil(t, t1)

			rt1 := catalog.SimpleGetTableByName(schema.Name)
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

	m1Cnt := 10
	m2Cnt := 5

	for i := 0; i < m1Cnt; i++ {
		name := fmt.Sprintf("m1_%d", i)
		schema := MockSchema(2)
		schema.Name = name
		_, err := catalog.SimpleCreateTable(schema, nil)
		assert.Nil(t, err)
	}

	for i := 0; i < m2Cnt; i++ {
		name := fmt.Sprintf("m2_%d", i)
		schema := MockSchema(2)
		schema.Name = name
		_, err := catalog.SimpleCreateTable(schema, nil)
		assert.Nil(t, err)
	}

	tbls := catalog.SimpleGetTablesByPrefix("m1_")
	assert.Equal(t, m1Cnt, len(tbls))
	tbls = catalog.SimpleGetTablesByPrefix("m2_")
	assert.Equal(t, m2Cnt, len(tbls))

	for _, tbl := range tbls[1:] {
		tbl.SimpleSoftDelete(nil)
	}
	tbls = catalog.SimpleGetTablesByPrefix("m2_")
	assert.Equal(t, 1, len(tbls))
}

func TestDropTable(t *testing.T) {
	dir := "/tmp/droptable"
	os.RemoveAll(dir)
	cfg := new(CatalogCfg)
	cfg.Dir = dir
	catalog, err := OpenCatalog(new(sync.RWMutex), cfg)
	assert.Nil(t, err)
	catalog.Start()

	schema1 := MockSchema(2)
	schema1.Name = "m1"

	t1, err := catalog.SimpleCreateTable(schema1, nil)
	assert.Nil(t, err)
	assert.NotNil(t, t1)

	assert.False(t, t1.IsSoftDeleted())
	assert.True(t, t1.HasCommitted())

	t1_1, err := catalog.SimpleCreateTable(schema1, nil)
	assert.NotNil(t, err)
	assert.Nil(t, t1_1)

	err = catalog.SimpleDropTableByName(t1.Schema.Name, nil)
	assert.Nil(t, err)

	err = catalog.SimpleDropTableByName(t1.Schema.Name, nil)
	assert.NotNil(t, err)

	schema2 := MockSchema(3)
	schema2.Name = schema1.Name
	t2, err := catalog.SimpleCreateTable(schema2, nil)
	assert.Nil(t, err)
	assert.NotNil(t, t2)

	schema3 := MockSchema(4)
	schema3.Name = schema1.Name
	t3, err := catalog.SimpleCreateTable(schema3, nil)
	assert.NotNil(t, err)
	assert.Nil(t, t3)

	err = catalog.SimpleDropTableByName(t1.Schema.Name, nil)
	assert.Nil(t, err)

	err = catalog.SimpleDropTableByName(t1.Schema.Name, nil)
	assert.NotNil(t, err)

	catalog.HardDeleteTable(t2.Id)

	t3, err = catalog.SimpleCreateTable(schema3, nil)
	assert.Nil(t, err)
	assert.NotNil(t, t3)

	versions := 0
	node := catalog.nameNodes[t1.Schema.Name].GetNext().(*nameNode)
	for node != nil {
		entry := node.GetEntry()
		t.Log(entry.PString(PPL0))
		snode := node.GetNext()
		if snode == nil {
			node = nil
		} else {
			node = snode.(*nameNode)
		}
		versions += 1
	}
	assert.Equal(t, 3, versions)

	t.Log(catalog.PString(PPL1))
	tableNode := catalog.nameNodes[t1.Schema.Name]
	assert.Equal(t, 3, tableNode.Length())
	t.Log(tableNode.PString(PPL1))

	catalog.Compact()
	assert.Equal(t, 2, tableNode.Length())
	t.Log(tableNode.PString(PPL1))

	err = catalog.SimpleDropTableByName(t1.Schema.Name, nil)
	assert.Nil(t, err)
	err = catalog.HardDeleteTable(t3.Id)
	assert.Nil(t, err)
	assert.Equal(t, 2, tableNode.Length())
	catalog.Compact()
	t.Log(tableNode.PString(PPL1))
	assert.Equal(t, 1, tableNode.Length())

	assert.Equal(t, 1, len(catalog.nameNodes))
	assert.Equal(t, 1, catalog.nameIndex.Len())

	err = catalog.HardDeleteTable(t1.Id)
	assert.Nil(t, err)
	catalog.Compact()
	t.Log(tableNode.PString(PPL1))
	assert.Equal(t, 0, len(catalog.nameNodes))
	assert.Equal(t, 0, catalog.nameIndex.Len())
	assert.Equal(t, 0, len(catalog.TableSet))

	index := new(LogIndex)
	index.ShardId = uint64(99)
	index.Id = shard.SimpleIndexId(uint64(1))
	t4, err := catalog.SimpleCreateTable(schema1, index)
	assert.Nil(t, err)
	assert.NotNil(t, t4)

	writer := NewShardSSWriter(catalog, dir, index.ShardId, index.Id.Id)
	err = writer.PrepareWrite()
	assert.Nil(t, err)

	err = writer.ReAllocId(&catalog.Sequence, writer.view.Catalog)
	assert.Nil(t, err)

	err = catalog.SimpleReplaceShard(writer.view, writer.tranId)
	assert.Nil(t, err)

	tableNode = catalog.nameNodes[schema1.Name]
	assert.Equal(t, 2, tableNode.Length())
	err = catalog.HardDeleteTable(t4.Id)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(catalog.TableSet))

	catalog.Compact()
	assert.Equal(t, 1, len(catalog.TableSet))
	assert.Equal(t, 1, tableNode.Length())
	t.Log(tableNode.PString(PPL1))

	t.Log(catalog.PString(PPL1))
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

	pool, _ := ants.NewPool(10)
	var wg sync.WaitGroup

	f := func(i int) func() {
		return func() {
			defer wg.Done()
			schema := MockSchema(2)
			schema.Name = fmt.Sprintf("m%d", i)

			t1, err := catalog.SimpleCreateTable(schema, nil)
			assert.Nil(t, err)
			assert.NotNil(t, t1)

			t1.SimpleCreateSegment()
			// t.Log(segment.String())
			t1.SimpleCreateSegment()
			// t.Log(segment.String())

			schema2 := MockSchema(2)
			schema2.Name = fmt.Sprintf("m%d", i+100)
			t2, err := catalog.SimpleCreateTable(schema2, nil)
			t2.SimpleCreateSegment()
			// t.Log(segment.String())

			err = catalog.SimpleDropTableByName(t1.Schema.Name, nil)
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

	pool, _ := ants.NewPool(5)
	var wg sync.WaitGroup
	f := func(i int) func() {
		return func() {
			defer wg.Done()
			schema := MockSchema(2)
			schema.Name = fmt.Sprintf("m%d", i)

			t1, err := catalog.SimpleCreateTable(schema, nil)
			assert.Nil(t, err)
			assert.NotNil(t, t1)

			s1 := t1.SimpleCreateSegment()
			s1.RLock()
			assert.True(t, s1.HasCommitted())
			s1.RUnlock()

			rt1 := catalog.SimpleGetTableByName(schema.Name)
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
	t2, err := catalog.SimpleCreateTable(schema, nil)
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
	catalog *Catalog
	t       *testing.T
}

func (hb *mockGetSegmentedHB) OnStopped() {}
func (hb *mockGetSegmentedHB) OnExec() {
	hb.catalog.ForLoopTables(hb.processTable)
}
func (hb *mockGetSegmentedHB) processTable(tbl *Table) error {
	// tbl.RLock()
	// defer tbl.RUnlock()
	id, _ := tbl.GetAppliedIndex(nil)
	hb.t.Logf("table %d segmented id: %d", tbl.Id, id)
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

	mockShards := 5
	createBlkWorker, _ := ants.NewPool(mockShards)
	upgradeSegWorker, _ := ants.NewPool(4)

	getSegmentedIdWorker := ops.NewHeartBeater(hbInterval, &mockGetSegmentedHB{catalog: catalog, t: t})
	getSegmentedIdWorker.Start()

	var wg sync.WaitGroup

	mockBlocks := cfg.SegmentMaxBlocks*2 + cfg.SegmentMaxBlocks/2
	idAlloc := newMockAllocator()

	for i := 0; i < mockShards; i++ {
		wg.Add(1)
		createBlkWorker.Submit(createBlock(t, 1, idAlloc, uint64(i), catalog, int(mockBlocks), &wg, upgradeSegWorker))
	}
	wg.Wait()
	getSegmentedIdWorker.Stop()
	t.Log(catalog.PString(PPL0))

	catalog.Close()
}

func TestAppliedIndex(t *testing.T) {
	dir := "/tmp/testappliedindex"
	os.RemoveAll(dir)
	blkRows, segBlks := uint64(10), uint64(2)
	driver, err := logstore.NewBatchStore(dir, "driver", nil)
	assert.Nil(t, err)
	indexWal := shard.NewManagerWithDriver(driver, false, wal.BrokerRole)
	defer indexWal.Close()
	catalog := MockCatalog(dir, blkRows, segBlks, driver, indexWal)
	defer catalog.Close()

	idAlloc := common.IdAlloctor{}
	index := new(LogIndex)
	index.Id = shard.SimpleIndexId(idAlloc.Alloc())

	// blkCnt := segBlks*2 + segBlks/2
	indexWal.SyncLog(index)
	tbl := MockTable(catalog, nil, 0, index)
	indexWal.Checkpoint(index)
	testutils.WaitExpect(10, func() bool {
		return indexWal.GetShardCheckpointId(0) == index.Id.Id
	})
	assert.Equal(t, index.Id.Id, indexWal.GetShardCheckpointId(0))

	blk, prevSeg := tbl.SimpleCreateBlock()
	assert.Nil(t, prevSeg)
	assert.NotNil(t, blk)
	opIdx := idAlloc.Alloc()
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
	opIdx = idAlloc.Alloc()
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

	opIdx = idAlloc.Alloc()
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
				segment, err := catalog.SimpleGetSegment(tableId, segmentId)
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
			block, err := catalog.SimpleGetBlock(tableId, segmentId, blockId)
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

	t1, err := catalog.SimpleCreateTable(schema, nil)
	assert.Nil(t, err)
	assert.NotNil(t, t1)

	createBlk := func(tableId, segmentId uint64) func() {
		return func() {
			defer wg2.Done()
			segment, err := catalog.SimpleGetSegment(tableId, segmentId)
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
	// t.Logf("%d - %d", catalog.Store.GetSyncedId(), catalog.Store.GetCheckpointId())
	t.Log(time.Since(now))

	assert.Equal(t, sequence.nextCommitId, catalog.Sequence.nextCommitId)
	assert.Equal(t, sequence.nextTableId, catalog.Sequence.nextTableId)
	assert.Equal(t, sequence.nextSegmentId, catalog.Sequence.nextSegmentId)
	assert.Equal(t, sequence.nextBlockId, catalog.Sequence.nextBlockId)

	// t.Logf("r - %d", catalog.Sequence.nextCommitId)
	// t.Logf("r - %d", catalog.Sequence.nextTableId)
	// t.Logf("r - %d", catalog.Sequence.nextSegmentId)
	// t.Logf("r - %d", catalog.Sequence.nextBlockId)

	catalog.Start()

	tmp := catalog.SimpleGetTable(t1.Id)
	assert.NotNil(t, tmp)
	tmp = catalog.SimpleGetTableByName(t1.Schema.Name)
	assert.NotNil(t, tmp)

	err = catalog.SimpleDropTableByName(t1.Schema.Name, nil)
	assert.Nil(t, err)
	err = catalog.SimpleDropTableByName(t1.Schema.Name, nil)
	assert.NotNil(t, err)
	err = catalog.HardDeleteTable(t1.Id)
	assert.Nil(t, err)

	viewId := uint64(40)
	filter := new(Filter)
	filter.tableFilter = newCommitFilter()
	filter.tableFilter.AddChecker(createCommitIdChecker(viewId))
	filter.segmentFilter = filter.tableFilter
	filter.blockFilter = filter.tableFilter
	view := newCatalogLogEntry(viewId)
	catalog.fillView(filter, view.Catalog)
	for _, tbl := range view.Catalog.TableSet {
		t.Log(len(tbl.SegmentSet))
	}
	view = catalog.LatestView()
	assert.Equal(t, 1, len(view.Catalog.TableSet))

	sequence = catalog.Sequence
	catalog.Close()

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

	schema := MockSchema(2)
	_, err = catalog.SimpleCreateTable(schema, nil)
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

	pool, _ := ants.NewPool(10)
	var wg sync.WaitGroup
	f := func(i int) func() {
		return func() {
			defer wg.Done()
			schema := MockSchema(2)
			schema.Name = fmt.Sprintf("m%d", i)

			t1, err := catalog.SimpleCreateTable(schema, nil)
			assert.Nil(t, err)
			assert.NotNil(t, t1)

			s1 := t1.SimpleCreateSegment()
			s1.RLock()
			assert.True(t, s1.HasCommitted())
			s1.RUnlock()

			rt1 := catalog.SimpleGetTableByName(schema.Name)
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

func TestShard(t *testing.T) {
	dir := "/tmp/metadata/testshard"
	blockRows, segmentBlocks := uint64(100), uint64(2)
	catalog := initTest(dir, blockRows, segmentBlocks, true)
	defer catalog.Close()

	mockShards := 4
	createBlkWorker, _ := ants.NewPool(mockShards)
	upgradeSegWorker, _ := ants.NewPool(20)

	now := time.Now()
	var wg sync.WaitGroup

	mockBlocks := segmentBlocks*2 + segmentBlocks/2

	idAlloc := newMockAllocator()
	for i := 0; i < mockShards; i++ {
		wg.Add(1)
		createBlkWorker.Submit(createBlock(t, 2, idAlloc, uint64(i), catalog, int(mockBlocks), &wg, upgradeSegWorker))
	}
	wg.Wait()
	t.Logf("mock metadata takes: %s", time.Since(now))
	t.Log(catalog.PString(PPL0))

	now = time.Now()
	var viewsMu sync.Mutex
	views := make(map[uint64]*catalogLogEntry)
	for i := 0; i < mockShards; i++ {
		wg.Add(1)
		go func(shardId uint64) {
			defer wg.Done()
			writer := NewShardSSWriter(catalog, dir, shardId, idAlloc.get(shardId))
			err := writer.PrepareWrite()
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
		loader := NewShardSSLoader(catalog, filepath.Join(dir, file.Name()))
		err = loader.PrepareLoad()
		assert.Nil(t, err)
		expected := views[loader.view.LogRange.ShardId]
		doCompare(t, expected, loader.view)
		err = loader.CommitLoad()
		assert.Nil(t, err)
		// t.Logf("shardId-%d: %s", loader.View.LogRange.ShardId, loader.View.Catalog.PString(PPL0))
	}
	t.Logf("takes %s", time.Since(now))
	t.Log(catalog.PString(PPL0))

	for shardId, allocator := range idAlloc.driver {
		writer := NewShardSSWriter(catalog, dir, shardId, allocator.Get())
		err = writer.PrepareWrite()
		assert.Nil(t, err)
		checker := func(info *CommitInfo) bool {
			// logutil.Infof("shardId-%d, %s", shardId, info.PString(PPL0))
			assert.Equal(t, shardId, info.GetShardId())
			return true
		}
		processor := new(loopProcessor)
		processor.BlockFn = func(block *Block) error {
			checker(block.GetCommit())
			return nil
		}
		processor.TableFn = func(table *Table) error {
			checker(table.GetCommit())
			return nil
		}
		writer.view.Catalog.RecurLoop(processor)
		t.Log(writer.view.Catalog.PString(PPL0))
	}
}

func initTest(dir string, blockRows, segmentBlocks uint64, cleanup bool) *Catalog {
	if cleanup {
		os.RemoveAll(dir)
	}
	cfg := new(CatalogCfg)
	cfg.Dir = dir
	cfg.BlockMaxRows, cfg.SegmentMaxBlocks = blockRows, segmentBlocks
	cfg.RotationFileMaxSize = 100 * int(common.M)
	catalog, _ := OpenCatalog(new(sync.RWMutex), cfg)
	catalog.Start()
	return catalog
}

type testCfg struct {
	shardId uint64
	blocks  int
	tables  int
}

func TestShard2(t *testing.T) {
	dir := "/tmp/metadata/testshard2"
	blockRows, segmentBlocks := uint64(100), uint64(2)
	catalog := initTest(dir, blockRows, segmentBlocks, true)
	idAlloc := newMockAllocator()
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
	wg := new(sync.WaitGroup)
	wg.Add(2)
	w1, _ := ants.NewPool(4)
	w2, _ := ants.NewPool(4)
	w1.Submit(createBlock(t, cfg1.tables, idAlloc, cfg1.shardId, catalog, cfg1.blocks, wg, w2))
	w1.Submit(createBlock(t, cfg2.tables, idAlloc, cfg2.shardId, catalog, cfg2.blocks, wg, w2))
	wg.Wait()

	blockCnt := 0
	blockCntFn := func(block *Block) error {
		blockCnt++
		return nil
	}
	blockCntOp := newBlockProcessor(blockCntFn)

	index1_0 := idAlloc.get(cfg1.shardId)
	view1 := catalog.ShardView(cfg1.shardId, index1_0)
	assert.Equal(t, cfg1.tables, len(view1.Catalog.TableSet))
	err := view1.Catalog.RecurLoop(blockCntOp)
	assert.Nil(t, err)
	assert.Equal(t, cfg1.blocks*cfg1.tables, blockCnt)

	blockCnt = 0
	view2 := catalog.ShardView(cfg2.shardId, idAlloc.get(cfg2.shardId))
	assert.Equal(t, cfg1.tables, len(view1.Catalog.TableSet))
	err = view2.Catalog.RecurLoop(blockCntOp)
	assert.Nil(t, err)
	assert.Equal(t, cfg2.blocks*cfg2.tables, blockCnt)

	cfg1_1 := testCfg{
		shardId: cfg1.shardId,
		blocks:  1,
		tables:  1,
	}
	wg.Add(1)
	w1.Submit(createBlock(t, cfg1_1.tables, idAlloc, cfg1_1.shardId, catalog, cfg1_1.blocks, wg, w2))
	wg.Wait()

	index1_1 := idAlloc.get(cfg1_1.shardId)
	view1_0 := catalog.ShardView(cfg1.shardId, index1_0)
	doCompare(t, view1, view1_0)

	blockCnt = 0
	view1_1 := catalog.ShardView(cfg1_1.shardId, index1_1)
	assert.Equal(t, cfg1.tables+cfg1_1.tables, len(view1_1.Catalog.TableSet))
	err = view1_1.Catalog.RecurLoop(blockCntOp)
	assert.Nil(t, err)
	assert.Equal(t, cfg1.blocks*cfg1.tables+cfg1_1.blocks*cfg1_1.tables, blockCnt)

	ids := view1_0.Catalog.SimpleGetTableIds()

	table := catalog.SimpleGetTable(ids[0])
	deleteIndex := new(LogIndex)
	deleteIndex.ShardId = cfg1.shardId
	deleteIndex.Id = shard.SimpleIndexId(idAlloc.alloc(cfg1.shardId))
	err = table.SimpleSoftDelete(deleteIndex)
	assert.Nil(t, err)

	view1_0_1 := catalog.ShardView(cfg1.shardId, index1_0)
	doCompare(t, view1_0, view1_0_1)

	view1_1_1 := catalog.ShardView(cfg1.shardId, index1_1)
	doCompare(t, view1_1, view1_1_1)

	index1_2 := idAlloc.get(cfg1.shardId)
	view1_2 := catalog.ShardView(cfg1.shardId, index1_2)
	assert.Equal(t, len(view1_1_1.Catalog.TableSet)-1, len(view1_2.Catalog.TableSet))

	t.Log(catalog.PString(PPL0))

	writer := NewShardSSWriter(catalog, dir, cfg1.shardId, index1_0)
	err = writer.PrepareWrite()
	assert.Nil(t, err)
	doCompare(t, view1_0, writer.view)
	err = writer.CommitWrite()
	assert.Nil(t, err)

	loader := NewShardSSLoader(catalog, writer.name)
	err = loader.PrepareLoad()
	assert.Nil(t, err)
	doCompare(t, view1_0, loader.view)
	err = loader.CommitLoad()
	assert.Nil(t, err)

	view := catalog.ShardView(cfg1.shardId, index1_0)
	assert.Equal(t, view1.LogRange, view.LogRange)
	assert.Equal(t, len(view1.Catalog.TableSet), len(view.Catalog.TableSet))
	catalog.Close()

	catalog2 := initTest(dir, blockRows, segmentBlocks, false)
	t.Log(catalog.PString(PPL2))
	catalog2.Close()

	doCompareCatalog(t, catalog, catalog2)
}

func TestSplit(t *testing.T) {
	dir := "/tmp/metadata/testsplit"
	catalog := initTest(dir, uint64(100), uint64(2), true)

	idAlloc := newMockAllocator()
	wg := new(sync.WaitGroup)
	w1, _ := ants.NewPool(4)
	w2, _ := ants.NewPool(4)

	shardId := uint64(66)
	wg.Add(4)
	w1.Submit(createBlock(t, 1, idAlloc, shardId, catalog, 0, wg, w2))
	w1.Submit(createBlock(t, 1, idAlloc, shardId, catalog, 1, wg, w2))
	w1.Submit(createBlock(t, 1, idAlloc, shardId, catalog, 2, wg, w2))
	w1.Submit(createBlock(t, 1, idAlloc, shardId, catalog, 3, wg, w2))

	wg.Wait()

	index := idAlloc.get(shardId)
	t.Logf("index=%d", index)
	stat := catalog.GetShardStats(shardId)
	assert.Equal(t, int64(550), stat.GetSize())
	assert.Equal(t, int64(600), stat.GetCount())

	_, _, keys, ctx, err := catalog.SplitCheck(uint64(250), shardId, index)
	assert.Nil(t, err)
	// t.Log(len(keys))
	// assert.Equal(t, 3, len(keys))
	spec := NewEmptyShardSplitSpec()
	err = spec.Unmarshal(ctx)
	assert.Nil(t, err)
	// t.Log(spec.String())
	assert.Equal(t, spec.ShardId, shardId)
	assert.Equal(t, spec.Index, index)
	assert.Equal(t, len(spec.Specs), 4)

	newShards := make([]uint64, len(keys))
	for i, _ := range newShards {
		newShards[i] = uint64(100) + uint64(i)
	}

	splitIndex := &LogIndex{
		ShardId: shardId,
		Id:      shard.SimpleIndexId(idAlloc.alloc(shardId)),
	}
	splitter := NewShardSplitter(catalog, spec, newShards, splitIndex, mockFactory)
	err = splitter.Prepare()
	assert.Nil(t, err)
	err = splitter.Commit()
	assert.Nil(t, err)
	assert.Equal(t, len(catalog.TableSet), 9)
	t.Log(catalog.PString(PPL1))
	catalog.Close()

	t.Log("--------------------------------------")
	catalog2 := initTest(dir, uint64(100), uint64(2), false)

	doCompareCatalog(t, catalog, catalog2)

	catalog2.Close()
}
