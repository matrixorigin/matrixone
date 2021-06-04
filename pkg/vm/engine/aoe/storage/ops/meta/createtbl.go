package meta

import (
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
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
	tbl, err := op.Ctx.Opts.Meta.Info.CreateTable()
	if err != nil {
		return err
	}

	err = op.Ctx.Opts.Meta.Info.RegisterTable(tbl)
	if err == nil {
		op.Result = tbl
	}
	return err
}
