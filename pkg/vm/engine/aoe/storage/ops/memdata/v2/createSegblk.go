package memdata

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	// log "github.com/sirupsen/logrus"
)

func NewCreateSegBlkOp(ctx *OpCtx, newSeg bool, meta *md.Block, tableData iface.ITableData) *CreateSegBlkOp {
	op := &CreateSegBlkOp{TableData: tableData, NewSegment: newSeg, BlkMeta: meta}
	op.Op = *NewOp(op, ctx, ctx.Opts.MemData.Updater)
	return op
}

type CreateSegBlkOp struct {
	Op
	TableData  iface.ITableData
	BlkMeta    *md.Block
	NewSegment bool
	Block      iface.IBlock
}

func (op *CreateSegBlkOp) Execute() error {
	if op.NewSegment {
		seg, err := op.TableData.RegisterSegment(op.BlkMeta.Segment)
		if err != nil {
			panic("should not happend")
		}
		seg.Unref()
	}
	blk, err := op.TableData.RegisterBlock(op.BlkMeta)
	if err != nil {
		panic(err)
	}
	op.Block = blk

	return nil
}
