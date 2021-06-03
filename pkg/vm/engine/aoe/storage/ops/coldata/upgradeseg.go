package coldata

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	// log "github.com/sirupsen/logrus"
)

func NewUpgradeSegOp(ctx *OpCtx, segID common.ID, td table.ITableData) *UpgradeSegOp {
	op := &UpgradeSegOp{
		SegmentID: segID,
		TableData: td,
	}
	op.Op = *NewOp(op, ctx, ctx.Opts.MemData.Updater)
	return op
}

type UpgradeSegOp struct {
	Op
	SegmentID common.ID
	TableData table.ITableData
	Segments  []col.IColumnSegment
}

func (op *UpgradeSegOp) Execute() error {
	op.Segments = op.TableData.UpgradeSegment(op.SegmentID)
	return nil
}
