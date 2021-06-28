package memdata

import (
// log "github.com/sirupsen/logrus"
)

func NewDropTblOp(ctx *OpCtx, id uint64) *DropTblOp {
	op := &DropTblOp{Id: id}
	op.Op = *NewOp(op, ctx, ctx.Opts.MemData.Updater)
	return op
}

type DropTblOp struct {
	Op
	Id uint64
}

func (op *DropTblOp) Execute() error {
	return op.Ctx.Tables.DropTable(op.Id)
}
