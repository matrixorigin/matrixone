package memdata

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	// log "github.com/sirupsen/logrus"
)

func NewCreateSegBlkOp(ctx *OpCtx, newSeg bool, meta *md.Block, tableData table.ITableData) *CreateSegBlkOp {
	op := &CreateSegBlkOp{TableData: tableData, NewSegment: newSeg, BlkMeta: meta}
	op.Op = *NewOp(op, ctx, ctx.Opts.MemData.Updater)
	return op
}

type CreateSegBlkOp struct {
	Op
	TableData  table.ITableData
	BlkMeta    *md.Block
	NewSegment bool
	ColBlocks  []col.IColumnBlock
}

func (op *CreateSegBlkOp) Execute() error {
	for _, column := range op.TableData.GetCollumns() {
		if op.NewSegment {
			seg, err := column.RegisterSegment(op.BlkMeta.Segment)
			if err != nil {
				panic("should not happend")
			}
			seg.UnRef()
		}
		colBlk, err := column.RegisterBlock(op.BlkMeta)
		if err != nil {
			panic("should not happend")
		}
		op.ColBlocks = append(op.ColBlocks, colBlk)
	}

	return nil
}
