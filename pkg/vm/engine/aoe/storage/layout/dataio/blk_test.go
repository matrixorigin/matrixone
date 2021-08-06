package dataio

import (
	"encoding/binary"
	"fmt"
	"matrixone/pkg/container/batch"
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"os"
	"path/filepath"
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
		name := e.MakeBlockFileName(dirname, id.ToBlockFileName(), id.TableID)
		dir = filepath.Dir(name)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			err = os.MkdirAll(dir, 0755)
			if err != nil {
				panic(fmt.Sprintf("err: %s", err))
			}
		}
		t.Log(name)
		w, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			panic(err)
		}
		buf, err := index.DefaultRWHelper.WriteIndices(indices)
		if err != nil {
			panic(err)
		}
		_, err = w.Write(buf)
		if err != nil {
			panic(err)
		}
		algo := uint8(0)
		cols := uint16(0)
		err = binary.Write(w, binary.BigEndian, &algo)
		assert.Nil(t, err)
		err = binary.Write(w, binary.BigEndian, &cols)
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
	assert.Equal(t, colCnt*blkCnt, bufMgr.NodeCount())
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
	rowCount, blkCount := uint64(10), uint64(4)
	info := md.MockInfo(rowCount, blkCount)
	schema := md.MockSchema(2)
	segCnt, blkCnt := uint64(4), uint64(4)
	table := md.MockTable(info, schema, segCnt*blkCnt)
	segment, err := table.CreateSegment()
	assert.Nil(t, err)
	err = table.RegisterSegment(segment)
	assert.Nil(t, err)
	batches := make([]*batch.Batch, 0)
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
