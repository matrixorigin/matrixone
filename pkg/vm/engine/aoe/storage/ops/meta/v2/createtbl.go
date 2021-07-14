package meta

import (
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	// log "github.com/sirupsen/logrus"
)

func NewCreateTblOp(ctx *OpCtx) *CreateTblOp {
	op := &CreateTblOp{}
	op.Op = *NewOp(op, ctx, ctx.Opts.Meta.Updater)
	return op
}

type CreateTblOp struct {
	Op
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
