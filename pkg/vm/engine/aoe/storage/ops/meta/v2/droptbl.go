package meta

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	// mmop "matrixone/pkg/vm/engine/aoe/storage/ops/memdata/v2"
	// log "github.com/sirupsen/logrus"
)

func NewDropTblOp(ctx *OpCtx, name string, tables *table.Tables) *DropTblOp {
	op := &DropTblOp{Name: name, Tables: tables}
	op.Op = *NewOp(op, ctx, ctx.Opts.Meta.Updater)
	return op
}

type DropTblOp struct {
	Op
	Name   string
	Id     uint64
	Tables *table.Tables
}

func (op *DropTblOp) Execute() error {
	id, err := op.Ctx.Opts.Meta.Info.SoftDeleteTable(op.Name)
	if err != nil {
		return err
	}
	op.Id = id
	return err
	// TODO: gc the data should be in async way, will change it later
	// ctx := new(mmop.OpCtx)
	// ctx.Opts = op.Ctx.Opts
	// ctx.Tables = op.Tables
	// dropOp := mmop.NewDropTblOp(ctx, id)
	// dropOp.Push()
	// err = dropOp.WaitDone()
	// if err != nil {
	// 	panic(err)
	// }
	// op.Id = id
	// return err
}
