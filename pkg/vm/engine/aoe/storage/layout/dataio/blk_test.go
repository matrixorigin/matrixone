package dataio

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"os"
	"path/filepath"
	"testing"
)

var (
	workDir = "/tmp/dataio_test"
)

func mockUnSortedSegmentFile(t *testing.T, dirname string, id common.ID, indexes []index.Index, blkCnt int) base.ISegmentFile {
	baseid := id
	var dir string
	for i := 0; i < blkCnt; i++ {
		id.BlockID = uint64(i)
		name := e.MakeFilename(dirname, e.FTBlock, id.ToBlockFileName(), false)
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
		buf, err := index.DefaultRWHelper.WriteIndexes(indexes)
		if err != nil {
			panic(err)
		}
		_, err = w.Write(buf)
		if err != nil {
			panic(err)
		}
		twoBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(twoBytes, uint16(0))
		_, err = w.Write(twoBytes)
		if err != nil {
			panic(err)
		}
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
	indexes := index.MockInt32ZmIndexes(colCnt)
	id := common.ID{}
	blkCnt := 4
	droppedBlocks := []uint64{}
	blkCB := func(v interface{}) {
		droppedBlocks = append(droppedBlocks, v.(*index.BlockHolder).ID.BlockID)
	}
	segFile := mockUnSortedSegmentFile(t, workDir, id, indexes, blkCnt)
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
		for i, _ := range blk.Indexes {
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
