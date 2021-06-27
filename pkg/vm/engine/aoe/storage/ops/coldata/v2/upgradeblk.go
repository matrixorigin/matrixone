package coldata

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	// log "github.com/sirupsen/logrus"
)

func NewUpgradeBlkOp(ctx *OpCtx, td iface.ITableData) *UpgradeBlkOp {
	op := &UpgradeBlkOp{
		TableData: td,
	}
	op.Op = *NewOp(op, ctx, ctx.Opts.MemData.Updater)
	return op
}

type UpgradeBlkOp struct {
	Op
	TableData iface.ITableData
	Block     iface.IBlock
}

func (op *UpgradeBlkOp) Execute() error {
	var err error
	op.Block, err = op.TableData.UpgradeBlock(op.Ctx.BlkMeta)
	return err
}
