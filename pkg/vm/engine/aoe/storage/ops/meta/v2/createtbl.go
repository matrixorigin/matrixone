package meta

import (
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	// log "github.com/sirupsen/logrus"
)

func NewCreateTblOp(ctx *OpCtx, localCtx dbi.TableOpCtx) *CreateTblOp {
	op := &CreateTblOp{LocalCtx: localCtx}
	op.Op = *NewOp(op, ctx, ctx.Opts.Meta.Updater)
	return op
}

type CreateTblOp struct {
	Op
	LocalCtx dbi.TableOpCtx
}

func (op *CreateTblOp) GetTable() *md.Table {
	tbl := op.Result.(*md.Table)
	return tbl
}

func (op *CreateTblOp) Execute() error {
	tbl, err := op.Ctx.Opts.Meta.Info.CreateTableFromTableInfo(op.Ctx.TableInfo)
	if err != nil {
		return err
	}
	op.Result = tbl
	return err
}
