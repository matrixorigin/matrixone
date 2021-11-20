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

package table

import (
	"bytes"
	"os"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	bm "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/factories"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	ldio "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation"
	mb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"

	"github.com/stretchr/testify/assert"
)

type testProcessor struct {
	t  *testing.T
	fn func(batch.IBatch)
}

func (p *testProcessor) execute(bat batch.IBatch) {
	p.fn(bat)
}

func TestTBlock(t *testing.T) {
	dir := "/tmp/table/tblk"
	os.RemoveAll(dir)
	rowCount, blkCount := uint64(30), uint64(4)
	catalog, indexWal := metadata.MockCatalogAndWal(dir, rowCount, blkCount)
	defer indexWal.Close()
	defer catalog.Close()
	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	shardId := uint64(100)
	tablemeta := metadata.MockDBTable(catalog, "db1", schema, 2, gen.Shard(shardId))
	seg1 := tablemeta.SimpleGetSegment(uint64(1))
	assert.NotNil(t, seg1)
	meta1 := seg1.SimpleGetBlock(uint64(1))
	assert.NotNil(t, meta1)
	meta2 := seg1.SimpleGetBlock(uint64(2))
	assert.NotNil(t, meta2)

	capacity := uint64(4096)
	fsMgr := ldio.NewManager(dir, false)
	indexBufMgr := bm.NewBufferManager(dir, capacity)
	mtBufMgr := bm.NewBufferManager(dir, capacity)
	sstBufMgr := bm.NewBufferManager(dir, capacity)
	tables := NewTables(new(storage.Options), new(sync.RWMutex), fsMgr, mtBufMgr, sstBufMgr, indexBufMgr)
	tabledata, err := tables.RegisterTable(tablemeta)
	assert.Nil(t, err)

	segmeta := meta1.Segment
	segdata, err := tabledata.RegisterSegment(segmeta)
	assert.Nil(t, err)
	assert.NotNil(t, segdata)

	maxsize := uint64(140)
	evicter := bm.NewSimpleEvictHolder()
	mgr := buffer.NewNodeManager(maxsize, evicter)
	factory := factories.NewMutFactory(mgr, nil)
	nodeFactory := factory.GetNodeFactroy(tabledata)

	mockSize := mb.NewMockSize(uint64(0))
	blk1, err := newTBlock(segdata, meta1, nodeFactory, mockSize)
	assert.Nil(t, err)
	assert.NotNil(t, blk1)
	assert.False(t, blk1.node.IsLoaded())
	fn := func(bat batch.IBatch) error {
		assert.True(t, blk1.node.IsLoaded())
		return nil
	}
	blk1.ProcessData(fn)

	rows := uint64(10)
	factor := uint64(4)

	insertBat := mock.MockBatch(schema.Types(), rows)

	insertFn := func(n *mutation.MutableBlockNode, idx *shard.Index) func() error {
		return func() error {
			var na int
			for idx, attr := range n.Data.GetAttrs() {
				vec, err := n.Data.GetVectorByAttr(attr)
				assert.Nil(t, err)
				if na, err = vec.AppendVector(insertBat.Vecs[idx], 0); err != nil {
					assert.NotNil(t, err)
				}
			}
			num := uint64(na)
			idx.Count = num
			assert.Nil(t, n.Meta.CommitInfo.SetIndex(*idx))
			_, err = n.Meta.AddCountLocked(num)
			assert.Nil(t, err)
			n.Meta.SetIndexLocked(idx.AsSlice())
			return nil
		}
	}

	appendFn := func(idx *shard.Index) func(mb.IMutableBlock) error {
		return func(node mb.IMutableBlock) error {
			n := node.(*mutation.MutableBlockNode)
			return n.Expand(rows*factor, insertFn(n, idx))
		}
	}

	idx1 := gen.Next(shardId)
	idx1.Capacity = uint64(insertBat.Vecs[0].Length())
	indexWal.SyncLog(idx1)
	err = blk1.WithPinedContext(appendFn(idx1))
	assert.Nil(t, err)

	testutils.WaitExpect(100, func() bool {
		return blk1.GetMeta().Segment.Table.Database.GetCheckpointId() == blk1.GetMeta().Segment.Table.GetCommit().GetIndex()
	})
	assert.Equal(t, blk1.GetMeta().Segment.Table.Database.GetCheckpointId(), blk1.GetMeta().Segment.Table.GetCommit().GetIndex())

	idx2 := gen.Next(shardId)
	idx2.Capacity = uint64(insertBat.Vecs[0].Length())
	indexWal.SyncLog(idx2)
	err = blk1.WithPinedContext(appendFn(idx2))
	assert.Nil(t, err)
	assert.Equal(t, rows*factor*2, mgr.Total())

	blk2, err := newTBlock(segdata, meta2, nodeFactory, mockSize)
	assert.Nil(t, err)
	assert.NotNil(t, blk2)
	assert.False(t, blk2.node.IsLoaded())

	idx3 := gen.Next(shardId)
	idx3.Capacity = uint64(insertBat.Vecs[0].Length())
	indexWal.SyncLog(idx3)
	err = blk2.WithPinedContext(appendFn(idx3))
	assert.Nil(t, err)

	idx4 := gen.Next(shardId)
	idx4.Capacity = uint64(insertBat.Vecs[0].Length())
	indexWal.SyncLog(idx4)
	err = blk2.WithPinedContext(appendFn(idx4))
	assert.Nil(t, err)

	testutils.WaitExpect(100, func() bool {
		return idx2.Id.Id == blk1.GetMeta().Segment.Table.Database.GetCheckpointId()
	})
	assert.Equal(t, idx2.Id.Id, blk1.GetMeta().Segment.Table.Database.GetCheckpointId())

	err = blk1.WithPinedContext(func(node mb.IMutableBlock) error {
		n := node.(*mutation.MutableBlockNode)
		assert.Equal(t, int(rows*2), n.Data.Length())
		return nil
	})
	assert.Nil(t, err)

	idx5 := gen.Next(shardId)
	idx5.Capacity = uint64(insertBat.Vecs[0].Length())
	indexWal.SyncLog(idx5)
	err = blk1.WithPinedContext(appendFn(idx5))
	assert.Nil(t, err)

	t.Log(mgr.String())

	err = blk1.WithPinedContext(func(node mb.IMutableBlock) error {
		n := node.(*mutation.MutableBlockNode)
		return n.Flush()
	})
	assert.Nil(t, err)

	t.Log(common.GPool.String())

	testutils.WaitExpect(100, func() bool {
		return idx5.Id.Id == blk1.GetMeta().Segment.Table.Database.GetCheckpointId()
	})
	assert.Equal(t, idx5.Id.Id, blk1.GetMeta().Segment.Table.Database.GetCheckpointId())

	blk1.WithPinedContext(func(node mb.IMutableBlock) error {
		n := node.(*mutation.MutableBlockNode)
		n.Meta.SimpleUpgrade(nil)
		var vecs []*vector.Vector
		for attri, _ := range n.Data.GetAttrs() {
			v, err := n.Data.GetVectorByAttr(attri)
			if err != nil {
				return err
			}
			vc, err := v.CopyToVector()
			if err != nil {
				return err
			}
			vecs = append(vecs, vc)
		}

		bw := dataio.NewBlockWriter(vecs, n.Meta, n.Meta.Segment.Table.Database.Catalog.Cfg.Dir)
		bw.SetPreExecutor(func() {
			logutil.Infof(" %s | Memtable | Flushing", bw.GetFileName())
		})
		bw.SetPostExecutor(func() {
			logutil.Infof(" %s | Memtable | Flushed", bw.GetFileName())
		})
		return bw.Execute()
	})

	t.Logf(mtBufMgr.String())
	nblk1, err := blk1.CloneWithUpgrade(blk1.host, blk1.meta)
	assert.Nil(t, err)
	t.Logf("Reference count %d", nblk1.RefCount())

	t.Logf(mtBufMgr.String())
	t.Logf(sstBufMgr.String())
	for _, colDef := range blk1.meta.Segment.Table.Schema.ColDefs {
		t.Logf("col %s size= %d", colDef.Name, blk1.Size(colDef.Name))
	}

	attrs := []string{}
	for _, colDef := range tablemeta.Schema.ColDefs {
		attrs = append(attrs, colDef.Name)
	}
	cds := make([]*bytes.Buffer, len(attrs))
	dds := make([]*bytes.Buffer, len(attrs))
	for i, attr := range attrs {
		cds[i] = bytes.NewBuffer(make([]byte, 0))
		dds[i] = bytes.NewBuffer(make([]byte, 0))
		vec, err := blk1.GetVectorCopy(attr, cds[i], dds[i])
		assert.Nil(t, err)
		assert.NotNil(t, vec)
	}
	t.Logf(blk1.String())
	blk1.Unref()
	blk2.Unref()
	t.Log(indexWal.String())
}
