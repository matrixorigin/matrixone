package memdata

import (
	// log "github.com/sirupsen/logrus"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
)

func NewDropTblOp(ctx *OpCtx, id uint64) *DropTblOp {
	op := &DropTblOp{Id: id}
	op.Op = *NewOp(op, ctx, ctx.Opts.MemData.Updater)
	return op
}

type DropTblOp struct {
	Op
	Id    uint64
	Table iface.ITableData
}

func (op *DropTblOp) Execute() error {
	tbl, err := op.Ctx.Tables.DropTable(op.Id)
	op.Table = tbl
	return err
}
