package tables

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type dataSegment struct {
	meta   *catalog.SegmentEntry
	file   dataio.SegmentFile
	aBlk   data.Block
	bufMgr base.INodeManager
}

func newSegment(meta *catalog.SegmentEntry, factory dataio.SegmentFileFactory, bufMgr base.INodeManager) *dataSegment {
	segFile := factory("xxx", meta.GetID())
	blkMeta := meta.LastAppendableBlock()
	var blk data.Block
	if blkMeta != nil {
		blk = newBlock(blkMeta, segFile, bufMgr)
	}
	seg := &dataSegment{
		meta:   meta,
		file:   segFile,
		bufMgr: bufMgr,
		aBlk:   blk,
	}
	return seg
}

func (segment *dataSegment) GetSegmentFile() dataio.SegmentFile {
	return segment.file
}

func (segment *dataSegment) IsAppendable() bool {
	if !segment.meta.IsAppendable() {
		return false
	}
	if segment.aBlk != nil {
		if blkAppendable := segment.aBlk.IsAppendable(); blkAppendable {
			return true
		}
	}
	blkCnt := segment.meta.GetAppendableBlockCnt()
	if blkCnt >= int(segment.meta.GetTable().GetSchema().SegmentMaxBlocks) {
		return false
	}
	return true
}

func (segment *dataSegment) GetID() uint64 { return segment.meta.GetID() }

func (segment *dataSegment) GetAppender() (id *common.ID, appender data.BlockAppender, err error) {
	id = segment.meta.AsCommonID()
	if segment.aBlk == nil {
		if !segment.meta.IsAppendable() {
			err = data.ErrAppendableSegmentNotFound
			return
		}
		err = data.ErrAppendableBlockNotFound
		return
	}
	appender, err = segment.aBlk.MakeAppender()
	if err != nil {
		if segment.meta.GetAppendableBlockCnt() >= int(segment.meta.GetTable().GetSchema().SegmentMaxBlocks) {
			err = data.ErrAppendableSegmentNotFound
		} else {
			err = data.ErrAppendableBlockNotFound
		}
	}
	return
}

func (segment *dataSegment) SetAppender(blkId uint64) (appender data.BlockAppender, err error) {
	blk, err := segment.meta.GetBlockEntryByID(blkId)
	if err != nil {
		panic(err)
	}
	// TODO: Push to flush queue
	if segment.aBlk != nil {
	}
	segment.aBlk = blk.GetBlockData()
	appender, err = segment.aBlk.MakeAppender()
	return
}

func (segment *dataSegment) BatchDedup(txn txnif.AsyncTxn, pks *vector.Vector) (err error) {
	return
}
