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

package mutation

import (
	"os"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/compress"
	bm "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	ldio "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"

	"github.com/stretchr/testify/assert"
)

var (
	moduleName = "mutation"
)

func getTestPath(t *testing.T) string {
	return testutils.GetDefaultTestPath(moduleName, t)
}

func initTestEnv(t *testing.T) string {
	testutils.RemoveDefaultTestPath(moduleName, t)
	return testutils.MakeDefaultTestPath(moduleName, t)
}

func TestMutableBlockNode(t *testing.T) {
	dir := initTestEnv(t)
	opts := config.NewOptions(dir, config.CST_None, config.BST_S, config.SST_S)
	os.RemoveAll(dir)
	rowCount, blkCount := uint64(30), uint64(4)
	catalog := metadata.MockCatalog(dir, rowCount, blkCount, nil, nil)
	opts.Meta.Catalog = catalog
	opts.Wal = shard.NewNoopWal()
	defer catalog.Close()
	opts.Scheduler = sched.NewScheduler(opts, nil)
	schema := metadata.MockSchema(2)
	gen := shard.NewMockIndexAllocator()
	shardId := uint64(100)
	tablemeta := metadata.MockDBTable(catalog, "db1", schema, nil, 2, gen.Shard(shardId))

	meta1, err := tablemeta.SimpleGetBlock(uint64(1), uint64(1))
	assert.Nil(t, err)
	meta2, err := tablemeta.SimpleGetBlock(uint64(1), uint64(2))
	assert.Nil(t, err)

	segfile := dataio.NewUnsortedSegmentFile(dir, *meta1.Segment.AsCommonID())
	tblkfile := dataio.NewTBlockFile(segfile, *meta1.AsCommonID())
	assert.NotNil(t, tblkfile)
	capacity := uint64(4096)
	fsMgr := ldio.NewManager(dir, false)
	indexBufMgr := bm.NewBufferManager(dir, capacity)
	mtBufMgr := bm.NewBufferManager(dir, capacity)
	sstBufMgr := bm.NewBufferManager(dir, capacity)
	tables := table.NewTables(opts, new(sync.RWMutex), fsMgr, mtBufMgr, sstBufMgr, indexBufMgr, nil)
	tabledata, err := tables.RegisterTable(tablemeta)

	maxsize := uint64(140)
	evicter := bm.NewSimpleEvictHolder()
	mgr := buffer.NewNodeManager(maxsize, evicter)
	node1 := NewMutableBlockNode(mgr, tblkfile, tabledata, meta1, nil, uint64(0))
	mgr.RegisterNode(node1)
	h1 := mgr.Pin(node1)
	assert.NotNil(t, h1)
	rows := uint64(10)
	factor := uint64(4)

	bat := mock.MockBatch(schema.Types(), rows)
	insert := func(n *MutableBlockNode) func() error {
		return func() error {
			for idx, attr := range n.Data.GetAttrs() {
				vec, err := n.Data.GetVectorByAttr(attr)
				if err != nil {
					return err
				}
				if _, err = vec.AppendVector(bat.Vecs[idx], 0); err != nil {
					return err
				}
				// assert.Nil(t, err)
			}
			return nil
		}
	}
	err = node1.Expand(rows*factor, insert(node1))
	assert.Nil(t, err)
	t.Logf("length=%d", node1.Data.Length())
	err = node1.Expand(rows*factor, insert(node1))
	assert.Nil(t, err)
	t.Logf("length=%d", node1.Data.Length())
	assert.Equal(t, rows*factor*2, mgr.Total())

	blkmeta2, err := tablemeta.SimpleGetBlock(uint64(1), uint64(2))
	assert.Nil(t, err)
	tblkfile2 := dataio.NewTBlockFile(segfile, *meta2.AsCommonID())
	node2 := NewMutableBlockNode(mgr, tblkfile2, tabledata, blkmeta2, nil, uint64(0))
	mgr.RegisterNode(node2)
	h2 := mgr.Pin(node2)
	assert.NotNil(t, h2)

	err = node2.Expand(rows*factor, insert(node2))
	assert.Nil(t, err)
	err = node2.Expand(rows*factor, insert(node2))
	assert.NotNil(t, err)

	h1.Close()

	err = node2.Expand(rows*factor, insert(node2))
	assert.Nil(t, err)

	h2.Close()
	h1 = mgr.Pin(node1)
	assert.Equal(t, int(rows*2), node1.Data.Length())

	err = node1.Expand(rows*factor, insert(node1))
	assert.Nil(t, err)
	h1.Close()
	t.Log(mgr.String())
	h2 = mgr.Pin(node2)
	assert.NotNil(t, h2)

	err = node2.Expand(rows*factor, insert(node2))
	assert.Nil(t, err)
	t.Log(mgr.String())
	err = node2.Flush()
	assert.Nil(t, err)
	t.Log(mgr.String())

	h2.Close()
	h1 = mgr.Pin(node1)

	t.Log(mgr.String())
	t.Log(common.GPool.String())

	bufs := make([][]byte, 2)
	for i, _ := range bufs {
		sz := tblkfile2.PartSize(uint64(i), *meta2.AsCommonID(), false)
		osz := tblkfile2.PartSize(uint64(i), *meta2.AsCommonID(), true)
		node := common.GPool.Alloc(uint64(sz))
		defer common.GPool.Free(node)
		buf := node.Buf[:sz]
		tblkfile2.ReadPart(uint64(i), *meta2.AsCommonID(), buf)
		obuf := make([]byte, osz)
		_, err = compress.Decompress(buf, obuf, compress.Lz4)
		assert.Nil(t, err)
		vec := vector.NewEmptyStrVector()
		err = vec.Unmarshal(obuf)
		assert.Nil(t, err)
		assert.Equal(t, int(rowCount), vec.Length())
	}
}
