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

package db

import (
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"

	"github.com/stretchr/testify/assert"
)

func TestReplay1(t *testing.T) {
	initDBTest()
	inst, gen, database := initDB2(wal.BrokerRole, "db1")
	schema := metadata.MockSchema(2)
	schema.Name = "mockcon"
	shardId := database.GetShardId()
	tid, err := inst.CreateTable(database.Name, schema, gen.Next(shardId))
	assert.Nil(t, err)

	meta := database.SimpleGetTable(tid)
	assert.NotNil(t, meta)
	rel, err := inst.Relation(database.Name, meta.Schema.Name)
	assert.Nil(t, err)

	irows := inst.Store.Catalog.Cfg.BlockMaxRows / 2
	ibat := mock.MockBatch(meta.Schema.Types(), irows)

	insertFn := func() {
		err = rel.Write(dbi.AppendCtx{
			ShardId:   database.GetShardId(),
			OpIndex:   gen.Alloc(shardId),
			OpSize:    1,
			Data:      ibat,
			TableName: meta.Schema.Name,
			DBName:    database.Name,
		})
		assert.Nil(t, err)
	}

	insertFn()
	assert.Equal(t, irows, uint64(rel.Rows()))
	err = inst.FlushTable(database.Name, schema.Name)
	assert.Nil(t, err)

	insertFn()
	assert.Equal(t, irows*2, uint64(rel.Rows()))
	err = inst.FlushTable(database.Name, schema.Name)
	assert.Nil(t, err)

	insertFn()
	assert.Equal(t, irows*3, uint64(rel.Rows()))
	err = inst.FlushTable(database.Name, schema.Name)
	assert.Nil(t, err)
	err = inst.FlushTable(database.Name, schema.Name)
	assert.Nil(t, err)

	testutils.WaitExpect(200, func() bool {
		return gen.Get(shardId) == inst.GetShardCheckpointId(shardId)
	})
	time.Sleep(time.Duration(10) * time.Millisecond)

	rel.Close()
	inst.Close()

	inst, _ = initDB(wal.BrokerRole)

	t.Log(inst.Store.Catalog.PString(metadata.PPL1, 0))
	segmentedIdx := inst.GetShardCheckpointId(shardId)
	assert.Equal(t, gen.Get(shardId), segmentedIdx)

	meta, err = inst.Opts.Meta.Catalog.SimpleGetTableByName(database.Name, schema.Name)
	assert.Nil(t, err)

	rel, err = inst.Relation(database.Name, meta.Schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, int(irows*3), int(rel.Rows()))
	t.Log(rel.Rows())

	insertFn()
	t.Log(rel.Rows())
	insertFn()
	time.Sleep(time.Duration(10) * time.Millisecond)
	t.Log(rel.Rows())
	t.Log(inst.MutationBufMgr.String())

	err = inst.FlushTable(database.Name, meta.Schema.Name)
	assert.Nil(t, err)
	insertFn()
	t.Log(rel.Rows())
	insertFn()
	t.Log(rel.Rows())

	rel.Close()
	inst.Close()
}

func mockSegFile(id common.ID, dir string, t *testing.T) string {
	name := id.ToSegmentFileName()
	fname := common.MakeSegmentFileName(dir, name, id.TableID, false)
	f, err := os.Create(fname)
	assert.Nil(t, err)
	defer f.Close()
	t.Log(fname)
	return fname
}

func mockBlkFile(id common.ID, dir string, t *testing.T) string {
	name := id.ToBlockFileName()
	fname := common.MakeBlockFileName(dir, name, id.TableID, false)
	f, err := os.Create(fname)
	assert.Nil(t, err)
	defer f.Close()
	return fname
}

func mockTBlkFile(id common.ID, version uint32, dir string, t *testing.T) string {
	fname := dataio.MakeTblockFileName(dir, "xx", uint64(version), id, false)
	f, err := os.Create(fname)
	assert.Nil(t, err)
	defer f.Close()
	return fname
}

func initDataAndMetaDir(dir string) {
	dataDir := common.MakeDataDir(dir)
	os.MkdirAll(dataDir, os.ModePerm)
	metaDir := common.MakeMetaDir(dir)
	os.MkdirAll(metaDir, os.ModePerm)
}

type replayObserver struct {
	removed []string
}

func (o *replayObserver) OnRemove(name string) {
	o.removed = append(o.removed, name)
}

func TestReplay2(t *testing.T) {
	dir := "/tmp/testreplay2"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)

	mu := &sync.RWMutex{}
	colCnt := 2
	blkRowCount, segBlkCount := uint64(16), uint64(4)
	cfg := &metadata.CatalogCfg{
		Dir:              dir,
		SegmentMaxBlocks: segBlkCount,
		BlockMaxRows:     blkRowCount,
	}
	catalog, err := metadata.OpenCatalog(mu, cfg)
	assert.Nil(t, err)
	catalog.Start()
	schema := metadata.MockSchema(colCnt)
	totalBlks := segBlkCount

	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, totalBlks, gen.Shard(0))
	opts := new(storage.Options)
	opts.Meta.Catalog = catalog
	opts.FillDefaults(dir)

	blkfiles := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := len(seg.BlockSet) - 2; i >= 0; i-- {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}

	catalog.Close()
	catalog, err = metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg)
	assert.Nil(t, err)
	catalog.Start()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	replayHandle.Replay()
	replayHandle.Cleanup()

	assert.Equal(t, 0, len(observer.removed))
	catalog.Close()
}

func buildOpts(dir string, createCatalog bool) *storage.Options {
	blkRowCount, segBlkCount := uint64(16), uint64(4)
	cfg := &storage.MetaCfg{
		BlockMaxRows:     blkRowCount,
		SegmentMaxBlocks: segBlkCount,
	}
	opts := new(storage.Options)
	opts.Meta.Conf = cfg
	opts.FillDefaults(dir)
	if createCatalog {
		opts.Meta.Catalog, _ = metadata.OpenCatalog(&opts.Mu, &metadata.CatalogCfg{
			Dir:              dir,
			BlockMaxRows:     opts.Meta.Conf.BlockMaxRows,
			SegmentMaxBlocks: opts.Meta.Conf.SegmentMaxBlocks,
		})
		opts.Meta.Catalog.Start()
	}
	return opts
}

func TestReplay3(t *testing.T) {
	dir := "/tmp/testreplay3"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, totalBlks, gen.Shard(0))
	blkfiles := make([]string, 0)
	tblkfiles := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet)-1; i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	tblk := seg.BlockSet[len(seg.BlockSet)-1]
	name := mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)
	tblkfiles = append(tblkfiles, name)

	catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg)
	assert.Nil(t, err)
	catalog.Start()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	replayHandle.Replay()
	replayHandle.Cleanup()

	assert.Equal(t, 0, len(observer.removed))
	catalog.Close()
}

func TestReplay4(t *testing.T) {
	dir := "/tmp/testreplay4"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	totalBlks := opts.Meta.Catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(opts.Meta.Catalog, "db1", schema, totalBlks, gen.Shard(0))
	blkfiles := make([]string, 0)
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet)-2; i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	unblk := seg.BlockSet[len(seg.BlockSet)-2]
	name := mockBlkFile(*unblk.AsCommonID(), dir, t)
	toRemove = append(blkfiles, name)

	unblk = seg.BlockSet[len(seg.BlockSet)-1]
	name = mockBlkFile(*unblk.AsCommonID(), dir, t)
	toRemove = append(blkfiles, name)
	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})

	opts.Meta.Catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), opts.Meta.Catalog.Cfg)
	assert.Nil(t, err)
	catalog.Start()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, opts.Meta.Catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	err = replayHandle.Replay()
	assert.Nil(t, err)
	replayHandle.Cleanup()

	assert.Equal(t, 2, len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	catalog.Close()
}

func TestReplay5(t *testing.T) {
	dir := "/tmp/testreplay5"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, totalBlks, gen.Shard(0))
	blkfiles := make([]string, 0)
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet)-2; i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	tblk := seg.BlockSet[len(seg.BlockSet)-3]
	name := mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*tblk.AsCommonID(), uint32(1), dir, t)
	toRemove = append(toRemove, name)

	unblk := seg.BlockSet[len(seg.BlockSet)-2]
	name = mockBlkFile(*unblk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)
	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})
	t.Log(toRemove)

	catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg)
	assert.Nil(t, err)
	catalog.Start()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	err = replayHandle.Replay()
	assert.Nil(t, err)
	replayHandle.Cleanup()
	tbl2, err := catalog.SimpleGetTableByName(tbl.Database.Name, tbl.Schema.Name)
	assert.Nil(t, err)
	assert.True(t, tbl2.SegmentSet[0].BlockSet[1].IsFullLocked())
	assert.False(t, tbl2.SegmentSet[0].BlockSet[2].IsFullLocked())

	assert.Equal(t, 3, len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	assert.Equal(t, toRemove, observer.removed)

	blk := tbl2.SimpleGetOrCreateNextBlock(nil)
	t.Log(blk.PString(metadata.PPL0))
	t.Log(tbl2.PString(metadata.PPL1, 0))
	assert.Equal(t, tbl2.SegmentSet[0].BlockSet[2], blk)

	catalog.Close()
}

func TestReplay6(t *testing.T) {
	dir := "/tmp/testreplay6"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, totalBlks, gen.Shard(0))
	blkfiles := make([]string, 0)
	tblkfiles := make([]string, 0)
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet)-2; i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	tblk := seg.BlockSet[len(seg.BlockSet)-3]
	name := mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*tblk.AsCommonID(), uint32(1), dir, t)
	toRemove = append(toRemove, name)

	unblk := seg.BlockSet[len(seg.BlockSet)-2]
	name = mockBlkFile(*unblk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*unblk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*unblk.AsCommonID(), uint32(1), dir, t)
	tblkfiles = append(tblkfiles, name)

	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})
	t.Log(toRemove)

	catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg)
	assert.Nil(t, err)
	catalog.Start()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	err = replayHandle.Replay()
	assert.Nil(t, err)
	replayHandle.Cleanup()
	tbl2, err := catalog.SimpleGetTableByName(tbl.Database.Name, tbl.Schema.Name)
	assert.Nil(t, err)
	assert.True(t, tbl2.SegmentSet[0].BlockSet[1].IsFullLocked())
	assert.False(t, tbl2.SegmentSet[0].BlockSet[2].IsFullLocked())

	assert.Equal(t, 4, len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	assert.Equal(t, toRemove, observer.removed)

	blk := tbl2.SimpleGetOrCreateNextBlock(nil)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[0].BlockSet[2], blk)

	catalog.Close()
}

func TestReplay7(t *testing.T) {
	dir := "/tmp/testreplay7"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, totalBlks, gen.Shard(0))
	blkfiles := make([]string, 0)
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet)-3; i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	tblk := seg.BlockSet[len(seg.BlockSet)-4]
	name := mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)

	blk := seg.BlockSet[len(seg.BlockSet)-3]
	name = mockBlkFile(*blk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)

	blk = seg.BlockSet[len(seg.BlockSet)-2]
	name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*blk.AsCommonID(), uint32(1), dir, t)
	toRemove = append(toRemove, name)

	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})
	t.Log(toRemove)

	catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg)
	assert.Nil(t, err)
	catalog.Start()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	err = replayHandle.Replay()
	assert.Nil(t, err)
	replayHandle.Cleanup()
	tbl2, err := catalog.SimpleGetTableByName(tbl.Database.Name, tbl.Schema.Name)
	assert.Nil(t, err)
	assert.True(t, tbl2.SegmentSet[0].BlockSet[0].IsFullLocked())
	assert.False(t, tbl2.SegmentSet[0].BlockSet[1].IsFullLocked())

	assert.Equal(t, 4, len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	assert.Equal(t, toRemove, observer.removed)

	blk = tbl2.SimpleGetOrCreateNextBlock(nil)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[0].BlockSet[1], blk)

	catalog.Close()
}

func TestReplay8(t *testing.T) {
	dir := "/tmp/testreplay8"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, totalBlks, gen.Shard(0))
	blkfiles := make([]string, 0)
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet)-3; i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	tblk := seg.BlockSet[len(seg.BlockSet)-4]
	name := mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)

	blk := seg.BlockSet[len(seg.BlockSet)-3]
	name = mockBlkFile(*blk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)

	blk = seg.BlockSet[len(seg.BlockSet)-2]
	name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*blk.AsCommonID(), uint32(1), dir, t)
	toRemove = append(toRemove, name)

	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})
	t.Log(toRemove)
	t.Log(tbl.PString(metadata.PPL1, 0))

	catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg)
	assert.Nil(t, err)
	catalog.Start()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	replayHandle.Replay()
	replayHandle.Cleanup()
	tbl2, err := catalog.SimpleGetTableByName(tbl.Database.Name, tbl.Schema.Name)
	assert.Nil(t, err)
	t.Log(tbl2.PString(metadata.PPL1, 0))
	assert.True(t, tbl2.SegmentSet[0].BlockSet[0].IsFullLocked())
	assert.False(t, tbl2.SegmentSet[0].BlockSet[1].IsFullLocked())

	assert.Equal(t, 4, len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	assert.Equal(t, toRemove, observer.removed)

	blk = tbl2.SimpleGetOrCreateNextBlock(nil)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[0].BlockSet[1], blk)

	catalog.Close()
}

func TestReplay9(t *testing.T) {
	dir := "/tmp/testreplay9"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks * 2

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, totalBlks, gen.Shard(0))
	blkfiles := make([]string, 0)
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet)-3; i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
	}
	tblk := seg.BlockSet[len(seg.BlockSet)-4]
	name := mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)

	blk := seg.BlockSet[len(seg.BlockSet)-3]
	name = mockBlkFile(*blk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)

	blk = seg.BlockSet[len(seg.BlockSet)-2]
	name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*blk.AsCommonID(), uint32(1), dir, t)
	toRemove = append(toRemove, name)

	blk = seg.BlockSet[len(seg.BlockSet)-1]
	name = mockBlkFile(*blk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)

	seg = tbl.SegmentSet[1]
	for i := 0; i < len(seg.BlockSet); i++ {
		blk := seg.BlockSet[i]
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		toRemove = append(toRemove, name)
		name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)
		toRemove = append(toRemove, name)
	}

	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})
	t.Log(toRemove)

	catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg)
	assert.Nil(t, err)
	catalog.Start()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	replayHandle.Replay()
	replayHandle.Cleanup()
	tbl2, err := catalog.SimpleGetTableByName(tbl.Database.Name, tbl.Schema.Name)
	assert.Nil(t, err)
	assert.True(t, tbl2.SegmentSet[0].BlockSet[0].IsFullLocked())
	assert.False(t, tbl2.SegmentSet[0].BlockSet[1].IsFullLocked())

	assert.Equal(t, len(toRemove), len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	assert.Equal(t, toRemove, observer.removed)

	t.Log(tbl2.PString(metadata.PPL1, 0))
	blk = tbl2.SimpleGetOrCreateNextBlock(nil)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[0].BlockSet[1], blk)

	blk = tbl2.SimpleGetOrCreateNextBlock(blk)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[0].BlockSet[2], blk)

	blk = tbl2.SimpleGetOrCreateNextBlock(blk)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[0].BlockSet[3], blk)

	blk = tbl2.SimpleGetOrCreateNextBlock(blk)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[1].BlockSet[0], blk)

	blk = tbl2.SimpleGetOrCreateNextBlock(blk)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[1].BlockSet[1], blk)

	tmp := blk

	blk = tbl2.SimpleGetOrCreateNextBlock(blk)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[1].BlockSet[2], blk)

	blk = tbl2.SimpleGetOrCreateNextBlock(blk)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[1].BlockSet[3], blk)

	blk = tbl2.SimpleGetOrCreateNextBlock(blk)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[2].BlockSet[0], blk)

	blk = tbl2.SimpleGetOrCreateNextBlock(tmp)
	t.Log(blk.PString(metadata.PPL0))
	assert.Equal(t, tbl2.SegmentSet[1].BlockSet[2], blk)

	catalog.Close()
}

func TestReplay10(t *testing.T) {
	dir := "/tmp/testreplay10"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks * 2

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, totalBlks, gen.Shard(0))
	blkfiles := make([]string, 0)
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet); i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		blkfiles = append(blkfiles, name)
		name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)
		toRemove = append(toRemove, name)
	}

	seg = tbl.SegmentSet[1]

	tblk := seg.BlockSet[len(seg.BlockSet)-4]
	name := mockBlkFile(*tblk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)

	name = mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)

	blk := seg.BlockSet[len(seg.BlockSet)-2]
	name = mockBlkFile(*blk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)

	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})
	t.Log(toRemove)

	catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg)
	assert.Nil(t, err)
	catalog.Start()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	replayHandle.Replay()
	replayHandle.Cleanup()
	tbl2, err := catalog.SimpleGetTableByName(tbl.Database.Name, tbl.Schema.Name)
	assert.Nil(t, err)
	assert.True(t, tbl2.SegmentSet[0].BlockSet[3].IsFullLocked())
	assert.False(t, tbl2.SegmentSet[1].BlockSet[1].IsFullLocked())
	t.Log(tbl2.PString(metadata.PPL1, 0))

	assert.Equal(t, len(toRemove), len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	assert.Equal(t, toRemove, observer.removed)
	catalog.Close()
}

func TestReplay11(t *testing.T) {
	dir := "/tmp/testreplay11"
	os.RemoveAll(dir)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks * 2

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, totalBlks, gen.Shard(0))
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet); i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(opts.Meta.Catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
		name := mockBlkFile(*blk.AsCommonID(), dir, t)
		toRemove = append(toRemove, name)
		name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)
		toRemove = append(toRemove, name)
		name = mockTBlkFile(*blk.AsCommonID(), uint32(1), dir, t)
		toRemove = append(toRemove, name)
	}
	mockSegFile(*seg.AsCommonID(), dir, t)

	seg = tbl.SegmentSet[1]

	tblk := seg.BlockSet[len(seg.BlockSet)-4]
	name := mockBlkFile(*tblk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)

	name = mockTBlkFile(*tblk.AsCommonID(), uint32(0), dir, t)

	blk := seg.BlockSet[len(seg.BlockSet)-2]
	name = mockBlkFile(*blk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)
	name = mockTBlkFile(*blk.AsCommonID(), uint32(0), dir, t)
	toRemove = append(toRemove, name)

	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})
	// t.Log(toRemove)

	catalog.Close()
	catalog, err := metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg)
	assert.Nil(t, err)
	catalog.Start()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	replayHandle.Replay()
	replayHandle.Cleanup()
	tbl2, err := catalog.SimpleGetTableByName(tbl.Database.Name, tbl.Schema.Name)
	assert.Nil(t, err)
	t.Log(tbl2.PString(metadata.PPL1, 0))
	assert.True(t, tbl2.SegmentSet[0].BlockSet[3].IsFullLocked())
	assert.False(t, tbl2.SegmentSet[1].BlockSet[1].IsFullLocked())

	assert.Equal(t, len(toRemove), len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	assert.Equal(t, toRemove, observer.removed)
	catalog.Close()
}

func TestReplay12(t *testing.T) {
	initDBTest()
	inst, gen, database := initDB2(wal.BrokerRole, "db1")
	schema := metadata.MockSchema(2)
	schema.Name = "mockcon"
	shardId := database.GetShardId()
	tid, err := inst.CreateTable(database.Name, schema, gen.Next(shardId))
	assert.Nil(t, err)

	meta := database.SimpleGetTable(tid)
	assert.NotNil(t, meta)
	rel, err := inst.Relation(database.Name, meta.Schema.Name)
	assert.Nil(t, err)

	irows := inst.Store.Catalog.Cfg.BlockMaxRows / 2
	ibat := mock.MockBatch(meta.Schema.Types(), irows)

	insertFn := func() {
		err = rel.Write(dbi.AppendCtx{
			ShardId:   database.GetShardId(),
			OpIndex:   gen.Alloc(shardId),
			OpSize:    1,
			Data:      ibat,
			TableName: meta.Schema.Name,
			DBName:    database.Name,
		})
		assert.Nil(t, err)
	}

	insertFn()
	assert.Equal(t, irows, uint64(rel.Rows()))
	err = inst.FlushTable(database.Name, schema.Name)
	assert.Nil(t, err)

	insertFn()
	assert.Equal(t, irows*2, uint64(rel.Rows()))
	err = inst.FlushTable(database.Name, schema.Name)
	assert.Nil(t, err)

	ibat2 := mock.MockBatch(meta.Schema.Types(), inst.Store.Catalog.Cfg.BlockMaxRows)
	insertFn2 := func() {
		err = rel.Write(dbi.AppendCtx{
			OpIndex:   gen.Alloc(shardId),
			OpOffset:  0,
			OpSize:    2,
			Data:      ibat,
			TableName: meta.Schema.Name,
			DBName:    database.Name,
			ShardId:   shardId,
		})
		err = rel.Write(dbi.AppendCtx{
			OpIndex:   gen.Get(shardId),
			OpOffset:  1,
			OpSize:    2,
			Data:      ibat2,
			TableName: meta.Schema.Name,
			DBName:    database.Name,
			ShardId:   shardId,
		})
		assert.Nil(t, err)
	}

	insertFn2()
	assert.Equal(t, irows*5, uint64(rel.Rows()))
	t.Log(rel.Rows())
	testutils.WaitExpect(200, func() bool {
		safeId := inst.GetShardCheckpointId(shardId)
		return gen.Get(shardId)-1 == safeId
	})
	assert.Equal(t, gen.Get(shardId)-1, inst.GetShardCheckpointId(shardId))
	time.Sleep(time.Duration(50) * time.Millisecond)

	rel.Close()
	inst.Close()

	inst, _ = initDB(wal.BrokerRole)
	replayDatabase, err := inst.Store.Catalog.SimpleGetDatabaseByName(database.Name)
	assert.Nil(t, err)

	assert.Equal(t, database.GetSize(), replayDatabase.GetSize())
	assert.Equal(t, database.GetCount(), replayDatabase.GetCount())

	segmentedIdx := inst.GetShardCheckpointId(shardId)
	assert.Equal(t, gen.Get(shardId)-1, segmentedIdx)

	rel, err = inst.Relation(database.Name, meta.Schema.Name)
	t.Log(rel.Rows())
	assert.Nil(t, err)
	assert.Equal(t, int64(irows*4), rel.Rows())

	insertFn3 := func() {
		err = rel.Write(dbi.AppendCtx{
			ShardId:   shardId,
			OpIndex:   segmentedIdx + 1,
			OpOffset:  0,
			OpSize:    2,
			Data:      ibat,
			TableName: meta.Schema.Name,
			DBName:    database.Name,
		})
		err = rel.Write(dbi.AppendCtx{
			OpIndex:   segmentedIdx + 1,
			OpOffset:  1,
			OpSize:    2,
			Data:      ibat2,
			TableName: meta.Schema.Name,
			DBName:    database.Name,
			ShardId:   shardId,
		})
		assert.Nil(t, err)
	}

	insertFn3()
	t.Log(rel.Rows())
	insertFn()
	time.Sleep(time.Duration(10) * time.Millisecond)
	t.Log(rel.Rows())
	t.Log(inst.MutationBufMgr.String())

	err = inst.FlushTable(meta.Database.Name, meta.Schema.Name)
	assert.Nil(t, err)
	insertFn()
	t.Log(rel.Rows())
	insertFn()
	t.Log(rel.Rows())

	rel.Close()
	inst.Close()
}

func TestReplay13(t *testing.T) {
	// dir := "/tmp/replay/test13"
	// os.RemoveAll(dir)
	dir := TEST_DB_DIR
	initDBTest()
	initDataAndMetaDir(dir)
	inst, gen := initDB(wal.BrokerRole)
	shardId := uint64(100)
	catalog := inst.Store.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks * 2
	schema := metadata.MockSchema(2)
	tbl := metadata.MockDBTable(catalog, "db1", schema, totalBlks, gen.Shard(shardId))
	toRemove := make([]string, 0)
	seg := tbl.SegmentSet[0]
	for i := 0; i < len(seg.BlockSet); i++ {
		blk := seg.BlockSet[i]
		blk.SetCount(catalog.Cfg.BlockMaxRows)
		err := blk.SimpleUpgrade(nil)
		assert.Nil(t, err)
	}
	name := mockSegFile(*seg.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)
	indice := make([]*metadata.LogIndex, 1)
	indice[0] = gen.Next(shardId)
	err := seg.SimpleUpgrade(100, indice)
	assert.Nil(t, err)

	catalog.IndexWal.SyncLog(gen.Next(shardId))
	err = tbl.SimpleSoftDelete(gen.Curr(shardId))
	assert.Nil(t, err)
	assert.True(t, tbl.IsSoftDeleted())
	catalog.IndexWal.Checkpoint(gen.Curr(shardId))

	testutils.WaitExpect(100, func() bool {
		return tbl.GetCommit().GetIndex() == tbl.Database.GetCheckpointId()
	})
	assert.Equal(t, tbl.GetCommit().GetIndex(), tbl.Database.GetCheckpointId())

	schema = metadata.MockSchema(3)
	tbl2 := metadata.MockTable(tbl.Database, schema, 1, gen.Next(shardId))
	blk := tbl2.SegmentSet[0].BlockSet[0]
	blk.SetCount(catalog.Cfg.BlockMaxRows)
	indice[0] = gen.Next(shardId)
	catalog.IndexWal.SyncLog(gen.Curr(shardId))
	err = blk.SimpleUpgrade(indice)
	assert.Nil(t, err)
	catalog.IndexWal.Checkpoint(gen.Curr(shardId))
	name = mockBlkFile(*blk.AsCommonID(), dir, t)
	toRemove = append(toRemove, name)
	catalog.IndexWal.SyncLog(gen.Next(shardId))
	err = tbl2.SimpleSoftDelete(gen.Curr(shardId))
	assert.Nil(t, err)

	inst.Close()

	catalog, err = metadata.OpenCatalog(new(sync.RWMutex), catalog.Cfg)
	assert.Nil(t, err)
	catalog.Start()

	observer := &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	replayHandle.Replay()
	sort.Slice(toRemove, func(i, j int) bool {
		return toRemove[i] < toRemove[j]
	})
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	assert.Equal(t, toRemove, observer.removed)

	db1 := catalog.Databases[tbl.Database.Id]
	assert.True(t, db1.TableSet[tbl.Id].IsHardDeleted())
	assert.True(t, db1.TableSet[tbl2.Id].IsHardDeleted())
	catalog.Compact(nil, nil)
	t.Log(catalog.PString(metadata.PPL0, 0))

	catalog.Close()
}
