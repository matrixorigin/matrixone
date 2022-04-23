package tables

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type dataSegment struct {
	meta   *catalog.SegmentEntry
	file   file.Segment
	bufMgr base.INodeManager
}

func newSegment(meta *catalog.SegmentEntry, factory file.SegmentFileFactory, bufMgr base.INodeManager) *dataSegment {
	segFile := factory("xxx", meta.GetID())
	seg := &dataSegment{
		meta:   meta,
		file:   segFile,
		bufMgr: bufMgr,
	}
	return seg
}

func (segment *dataSegment) GetSegmentFile() file.Segment {
	return segment.file
}

func (segment *dataSegment) GetID() uint64 { return segment.meta.GetID() }

func (segment *dataSegment) BatchDedup(txn txnif.AsyncTxn, pks *vector.Vector) (err error) {
	// TODO: segment level index
	return data.ErrPossibleDuplicate
	// blkIt := segment.meta.MakeBlockIt(false)
	// for blkIt.Valid() {
	// 	block := blkIt.Get().GetPayload().(*catalog.BlockEntry)
	// 	if err = block.GetBlockData().BatchDedup(txn, pks); err != nil {
	// 		return
	// 	}
	// 	blkIt.Next()
	// }
	// return nil
}
