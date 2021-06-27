package coldata

import (
	table "matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/col"
	// log "github.com/sirupsen/logrus"
)

func NewUpgradeBlkOp(ctx *OpCtx, td table.ITableData) *UpgradeBlkOp {
	op := &UpgradeBlkOp{
		TableData: td,
	}
	op.Op = *NewOp(op, ctx, ctx.Opts.MemData.Updater)
	return op
}

type UpgradeBlkOp struct {
	Op
	TableData table.ITableData
	Blocks    []col.IColumnBlock
}

func (op *UpgradeBlkOp) Execute() error {
	op.Blocks = op.TableData.UpgradeBlock(op.Ctx.BlkMeta)
	return nil
}
