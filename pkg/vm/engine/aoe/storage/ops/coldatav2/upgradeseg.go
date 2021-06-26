package coldata

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table2/iface"
	// log "github.com/sirupsen/logrus"
)

func NewUpgradeSegOp(ctx *OpCtx, segID uint64, td iface.ITableData) *UpgradeSegOp {
	op := &UpgradeSegOp{
		SegmentID: segID,
		TableData: td,
	}
	op.Op = *NewOp(op, ctx, ctx.Opts.MemData.Updater)
	return op
}

type UpgradeSegOp struct {
	Op
	SegmentID uint64
	TableData iface.ITableData
	Segment   iface.ISegment
}

func (op *UpgradeSegOp) Execute() error {
	var err error
	op.Segment, err = op.TableData.UpgradeSegment(op.SegmentID)
	return err
}
