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

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"

	"github.com/stretchr/testify/assert"
)

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
	dir := initTestEnv(t)
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
	tbl := metadata.MockDBTable(catalog, "db1", schema, nil, totalBlks, gen.Shard(0))
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
	dir := initTestEnv(t)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, nil, totalBlks, gen.Shard(0))
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
	dir := initTestEnv(t)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	totalBlks := opts.Meta.Catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(opts.Meta.Catalog, "db1", schema, nil, totalBlks, gen.Shard(0))
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
	dir := initTestEnv(t)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, nil, totalBlks, gen.Shard(0))
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
	dir := initTestEnv(t)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, nil, totalBlks, gen.Shard(0))
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
	dir := initTestEnv(t)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, nil, totalBlks, gen.Shard(0))
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
	dir := initTestEnv(t)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, nil, totalBlks, gen.Shard(0))
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
	dir := initTestEnv(t)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks * 2

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, nil, totalBlks, gen.Shard(0))
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
	dir := initTestEnv(t)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks * 2

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, nil, totalBlks, gen.Shard(0))
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
	dir := initTestEnv(t)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	catalog := opts.Meta.Catalog
	totalBlks := catalog.Cfg.SegmentMaxBlocks * 2

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(catalog, "db1", schema, nil, totalBlks, gen.Shard(0))
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

func TestReplay15(t *testing.T) {
	// Delete part of the data of the last entry
	ReplayTruncate(2290, t)
	// Only keep the meta data of the last entry
	ReplayTruncate(2280, t)
	// Only keep the first 8 bytes of the meta of the last entry
	ReplayTruncate(2270, t)
}
func ReplayTruncate(size int64, t *testing.T) {
	dir := initTestEnv(t)
	initDataAndMetaDir(dir)
	opts := buildOpts(dir, true)
	totalBlks := opts.Meta.Catalog.Cfg.SegmentMaxBlocks

	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	tbl := metadata.MockDBTable(opts.Meta.Catalog, "db1", schema, nil, totalBlks, gen.Shard(0))
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
	replayHandle := NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	err = replayHandle.Replay()
	assert.Nil(t, err)
	replayHandle.Cleanup()

	tbl2, err := catalog.SimpleGetTableByName(tbl.Database.Name, tbl.Schema.Name)
	assert.Nil(t, err)
	assert.True(t, tbl2.SegmentSet[0].BlockSet[0].IsFullLocked())
	assert.True(t, tbl2.SegmentSet[0].BlockSet[1].IsFullLocked())
	assert.Equal(t, 2, len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})
	err = catalog.Store.TryTruncate(size)
	logutil.Infof("err is %v", err)
	t.Log(catalog.PString(3,0))
	catalog.Close()

	catalog, err = metadata.OpenCatalog(new(sync.RWMutex), opts.Meta.Catalog.Cfg)
	assert.Nil(t, err)
	catalog.Start()
	t.Log(catalog.PString(3,0))

	observer = &replayObserver{
		removed: make([]string, 0),
	}
	replayHandle = NewReplayHandle(dir, catalog, nil, observer)
	assert.NotNil(t, replayHandle)
	err = replayHandle.Replay()
	assert.Nil(t, err)
	replayHandle.Cleanup()

	tbl2, err = catalog.SimpleGetTableByName(tbl.Database.Name, tbl.Schema.Name)
	assert.Nil(t, err)
	assert.True(t, tbl2.SegmentSet[0].BlockSet[0].IsFullLocked())
	assert.False(t, tbl2.SegmentSet[0].BlockSet[1].IsFullLocked())
	assert.Equal(t, 1, len(observer.removed))
	sort.Slice(observer.removed, func(i, j int) bool {
		return observer.removed[i] < observer.removed[j]
	})

	catalog.Close()
}