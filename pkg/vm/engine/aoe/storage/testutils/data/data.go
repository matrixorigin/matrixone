package data

import (
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	mgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	w "matrixone/pkg/vm/engine/aoe/storage/worker"
	"testing"

	"github.com/stretchr/testify/assert"
	// log "github.com/sirupsen/logrus"
)

func MakeBufMagr(capacity uint64) mgrif.IBufferManager {
	flusher := w.NewOpWorker("Mock Flusher")
	bufMgr := bmgr.NewBufferManager(capacity, flusher)
	return bufMgr
}

func MakeSegment(mtBufMgr, sstBufMgr mgrif.IBufferManager, colIdx int, meta *md.Segment, t *testing.T) col.IColumnSegment {
	seg := col.NewColumnSegment(mtBufMgr, sstBufMgr, colIdx, meta)
	for _, blkMeta := range meta.Blocks {
		blk, err := seg.RegisterBlock(blkMeta)
		assert.Nil(t, err)
		blk.UnRef()
	}
	return seg
}

func MakeSegments(mtBufMgr, sstBufMgr mgrif.IBufferManager, meta *md.Table, tblData table.ITableData, t *testing.T) []common.ID {
	var segIDs []common.ID
	for _, segMeta := range meta.Segments {
		var colSegs []col.IColumnSegment
		for colIdx, _ := range segMeta.Schema.ColDefs {
			colSeg := MakeSegment(mtBufMgr, sstBufMgr, colIdx, segMeta, t)
			colSegs = append(colSegs, colSeg)
		}
		tblData.AppendColSegments(colSegs)
		segIDs = append(segIDs, *segMeta.AsCommonID())
	}
	return segIDs
}
