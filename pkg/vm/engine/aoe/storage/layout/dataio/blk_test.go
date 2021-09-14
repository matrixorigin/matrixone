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
// limitations under the License.

package dataio

import (
	"encoding/binary"
	"fmt"
	"matrixone/pkg/compress"
	gbatch "matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	workDir = "/tmp/dataio_test"
)

func mockUnSortedSegmentFile(t *testing.T, dirname string, id common.ID, indices []index.Index, blkCnt int) base.ISegmentFile {
	baseid := id
	var dir string
	for i := 0; i < blkCnt; i++ {
		id.BlockID = uint64(i)
		name := e.MakeBlockFileName(dirname, id.ToBlockFileName(), id.TableID, false)
		dir = filepath.Dir(name)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			err = os.MkdirAll(dir, 0755)
			if err != nil {
				panic(fmt.Sprintf("err: %s", err))
			}
		}
		t.Log(name)
		w, err := os.Create(name)
		if err != nil {
			panic(err)
		}
		//buf, err := index.DefaultRWHelper.WriteIndices(indices)
		//if err != nil {
		//	panic(err)
		//}
		//_, err = w.Write(buf)
		//if err != nil {
		//	panic(err)
		//}
		algo := uint8(0)
		cols := uint16(0)
		count := uint64(0)
		err = binary.Write(w, binary.BigEndian, &algo)
		assert.Nil(t, err)
		err = binary.Write(w, binary.BigEndian, &cols)
		assert.Nil(t, err)
		err = binary.Write(w, binary.BigEndian, &count)
		assert.Nil(t, err)
		prevIdx := md.LogIndex{
			ID:       uint64(0),
			Start:    uint64(0),
			Count:    uint64(0),
			Capacity: uint64(0),
		}
		buf, err := prevIdx.Marshall()
		assert.Nil(t, err)
		var sz int32
		sz = int32(len(buf))
		err = binary.Write(w, binary.BigEndian, &sz)
		assert.Nil(t, err)
		err = binary.Write(w, binary.BigEndian, &buf)
		assert.Nil(t, err)
		idx := md.LogIndex{
			ID:       uint64(0),
			Start:    uint64(0),
			Count:    uint64(0),
			Capacity: uint64(0),
		}
		buf, err = idx.Marshall()
		assert.Nil(t, err)
		var sz_ int32
		sz_ = int32(len(buf))
		err = binary.Write(w, binary.BigEndian, &sz_)
		assert.Nil(t, err)
		err = binary.Write(w, binary.BigEndian, &buf)
		assert.Nil(t, err)
		w.Close()
	}
	segFile := NewUnsortedSegmentFile(dirname, baseid)
	id = baseid
	for i := 0; i < blkCnt; i++ {
		id.BlockID = uint64(i)
		segFile.RefBlock(id)
	}

	return segFile
}

func TestAll(t *testing.T) {
	bufMgr := bmgr.MockBufMgr(26 * 4)
	colCnt := 2
	indices := index.MockInt32ZmIndices(colCnt)
	id := common.ID{}
	blkCnt := 4
	droppedBlocks := []uint64{}
	blkCB := func(v interface{}) {
		droppedBlocks = append(droppedBlocks, v.(*index.BlockHolder).ID.BlockID)
	}
	segFile := mockUnSortedSegmentFile(t, workDir, id, indices, blkCnt)
	tblHolder := index.NewTableHolder(bufMgr, id.TableID)
	segHolder := tblHolder.RegisterSegment(id, base.UNSORTED_SEG, nil)
	segHolder.Unref()
	for i := 0; i < blkCnt; i++ {
		blkId := id
		blkId.BlockID = uint64(i)
		blkHolder := segHolder.RegisterBlock(blkId, base.PERSISTENT_BLK, blkCB)
		blkHolder.Unref()
		blkHolder.Init(segFile)
	}
	t.Log(bufMgr.String())
	t.Log(tblHolder.String())
	//assert.Equal(t, colCnt*blkCnt, bufMgr.NodeCount())
	for bidx := 0; bidx < blkCnt; bidx++ {
		blk := segHolder.StrongRefBlock(uint64(bidx))
		for i, _ := range blk.Indices {
			node := blk.GetIndexNode(i)
			mnode := node.GetManagedNode()
			zm := mnode.DataNode.(*index.ZoneMapIndex)
			assert.Equal(t, zm.MinV.(int32), int32(i*100+1))
			assert.Equal(t, zm.MaxV.(int32), int32(i*100+99))
			t.Logf("zm index: min=%v,max=%v", zm.MinV, zm.MaxV)
			mnode.Close()
			node.Unref()
		}
		blk.Unref()
	}
	t.Log(bufMgr.String())
	t.Log(tblHolder.String())

	dropblkid := uint64(1)
	droppBlk := segHolder.DropBlock(dropblkid)
	t.Log(droppBlk.RefCount())
	droppBlk.Unref()
	t.Log(bufMgr.String())
	t.Log(tblHolder.String())
	t.Log(droppBlk.RefCount())
	t.Log(droppedBlocks)
	assert.Equal(t, 1, len(droppedBlocks))
	assert.Equal(t, droppedBlocks[0], dropblkid)
}

func TestSegmentWriter(t *testing.T) {
	mu := &sync.RWMutex{}
	rowCount, blkCount := uint64(10), uint64(4)
	info := md.MockInfo(mu, rowCount, blkCount)
	schema := md.MockSchema(2)
	segCnt, blkCnt := uint64(4), uint64(4)
	table := md.MockTable(info, schema, segCnt*blkCnt)
	segment, err := table.CreateSegment()
	assert.Nil(t, err)
	err = table.RegisterSegment(segment)
	assert.Nil(t, err)
	batches := make([]*gbatch.Batch, 0)
	for i := 0; i < int(blkCount); i++ {
		block, err := segment.CreateBlock()
		assert.Nil(t, err)
		block.SetCount(rowCount)
		err = segment.RegisterBlock(block)
		assert.Nil(t, err)
		batches = append(batches, chunk.MockBatch(schema.Types(), rowCount))
	}
	path := "/tmp/testwriter"
	writer := NewSegmentWriter(batches, segment, path)
	err = writer.Execute()
	assert.Nil(t, err)
	// name := writer.GetFileName()
	segFile := NewSortedSegmentFile(path, *segment.AsCommonID())
	assert.NotNil(t, segFile)
	tblHolder := index.NewTableHolder(bmgr.MockBufMgr(1000), table.ID)
	segHolder := tblHolder.RegisterSegment(*segment.AsCommonID(), base.SORTED_SEG, nil)
	segHolder.Unref()
	id := common.ID{}
	for i := 0; i < int(blkCount); i++ {
		id.BlockID = uint64(i)
		blkHolder := segHolder.RegisterBlock(id, base.PERSISTENT_BLK, nil)
		blkHolder.Unref()
		blkHolder.Init(segFile)
	}
	segHolder.Init(segFile)
	t.Log(tblHolder.String())
	t.Log(segHolder.CollectMinMax(0))
	t.Log(segHolder.CollectMinMax(1))
	t.Log(segHolder.GetBlockCount())
	col0Blk := segment.Blocks[0].AsCommonID().AsBlockID()
	col1Blk := col0Blk
	col1Blk.Idx = uint16(1)
	col0Vf := segFile.MakeVirtualPartFile(&col0Blk)
	col1Vf := segFile.MakeVirtualPartFile(&col1Blk)
	assert.NotNil(t, col0Vf)
	assert.NotNil(t, col1Vf)
	stat0 := col0Vf.Stat()
	stat1 := col1Vf.Stat()
	t.Log(stat0.Name())
	t.Log(stat1.Name())
}

func TestIVectorNodeWriter(t *testing.T) {
	dir := "/tmp/blktest"
	os.RemoveAll(dir)
	vecType := types.Type{types.T_int32, 4, 4, 0}
	capacity := uint64(40)
	vec0 := vector.NewStdVector(vecType, capacity)
	defer vec0.Close()
	vec1 := vector.NewStrVector(types.Type{types.T(types.T_varchar), 24, 0, 0}, capacity)
	defer vec1.Close()
	err := vec0.Append(4, []int32{int32(0), int32(1), int32(2), int32(3)})
	assert.Nil(t, err)
	str0 := "str0"
	str1 := "str1"
	str2 := "str2"
	str3 := "str3"
	strs := [][]byte{[]byte(str0), []byte(str1), []byte(str2), []byte(str3)}
	err = vec1.Append(len(strs), strs)

	info := md.MockInfo(&sync.RWMutex{}, capacity, uint64(10))
	schema := md.MockSchema(2)
	tblMeta := md.MockTable(info, schema, 1)
	meta, err := tblMeta.ReferenceBlock(uint64(1), uint64(1))
	assert.Nil(t, err)
	assert.NotNil(t, meta)
	bat := batch.NewBatch([]int{0, 1}, []vector.IVector{vec0, vec1})
	w := NewIBatchWriter(bat, meta, dir)
	err = w.Execute()
	assert.Nil(t, err)

	id := *meta.AsCommonID()
	segFile := NewUnsortedSegmentFile(dir, *meta.Segment.AsCommonID())
	f := NewBlockFile(segFile, id, nil)
	bufs := make([][]byte, 2)
	for i, _ := range bufs {
		sz := f.PartSize(uint64(i), id, false)
		osz := f.PartSize(uint64(i), id, true)
		node := common.GPool.Alloc(uint64(sz))
		defer common.GPool.Free(node)
		buf := node.Buf[:sz]
		f.ReadPart(uint64(i), id, buf)
		obuf := make([]byte, osz)
		_, err = compress.Decompress(buf, obuf, compress.Lz4)
		assert.Nil(t, err)
		if i == 0 {
			vec := vector.NewEmptyStdVector()
			err = vec.Unmarshall(obuf)
			assert.Nil(t, err)
			t.Log(vec.GetLatestView().CopyToVector().String())
			assert.Equal(t, 4, vec.Length())
		} else {
			vec := vector.NewEmptyStrVector()
			err = vec.Unmarshall(obuf)
			assert.Nil(t, err)
			t.Log(vec.GetLatestView().CopyToVector().String())
			assert.Equal(t, 4, vec.Length())
		}
	}
}

func TestTransientBlock(t *testing.T) {
	mu := &sync.RWMutex{}
	rowCount, blkCount := uint64(10), uint64(4)
	info := md.MockInfo(mu, rowCount, blkCount)
	info.Conf.Dir = "/tmp/tblktest"
	os.RemoveAll(info.Conf.Dir)
	schema := md.MockSchema(2)
	tbl := md.MockTable(info, schema, 1)

	blkMeta, err := tbl.ReferenceBlock(uint64(1), uint64(1))
	assert.Nil(t, err)

	segFile := NewUnsortedSegmentFile(info.Conf.Dir, *blkMeta.Segment.AsCommonID())

	tblk := NewTBlockFile(segFile, *blkMeta.AsCommonID())
	// defer tblk.Destory()
	defer tblk.Unref()
	t.Log(tblk.nextVersion())

	// rows := uint64(2)
	// bat1 := chunk.MockBatch(schema.Types(), rows)
	// bat2 := chunk.MockBatch(schema.Types(), rowCount)

	// ok := tblk.PreSync(uint32(bat1.Vecs[0].Length()))
	// assert.True(t, ok)
	// err = tblk.Sync(bat1.Vecs, blkMeta, blkMeta.Segment.Table.Conf.Dir)
	// assert.Nil(t, err)
	// ok = tblk.PreSync(uint32(bat2.Vecs[0].Length()))
	// assert.True(t, ok)
	// err = tblk.Sync(bat2.Vecs, blkMeta, blkMeta.Segment.Table.Conf.Dir)
	// assert.Nil(t, err)
	// ok = tblk.PreSync(uint32(bat2.Vecs[0].Length()))
	// assert.False(t, ok)
}
