package meta

import (
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	// mmop "matrixone/pkg/vm/engine/aoe/storage/ops/memdata/v2"
	// log "github.com/sirupsen/logrus"
)

func NewDropTblOp(ctx *OpCtx, dropCtx dbi.DropTableCtx, tables *table.Tables) *DropTblOp {
	op := &DropTblOp{LocalCtx: dropCtx, Tables: tables}
	op.Op = *NewOp(op, ctx, ctx.Opts.Meta.Updater)
	return op
}

type DropTblOp struct {
	Op
	LocalCtx dbi.DropTableCtx
	Name     string
	Id       uint64
	Tables   *table.Tables
}

func (op *DropTblOp) Execute() error {
	id, err := op.Ctx.Opts.Meta.Info.SoftDeleteTable(op.LocalCtx.TableName, op.LocalCtx.OpIndex)
	if err != nil {
		return err
	}
	op.Id = id
	{
		ctx := md.CopyCtx{Ts: md.NowMicro() + 1, Attached: true}
		info := op.Ctx.Opts.Meta.Info.Copy(ctx)
		opCtx := OpCtx{Opts: op.Ctx.Opts}
		flushOp := NewFlushInfoOp(&opCtx, info)
		flushOp.Push()
		go func() {
			flushOp.WaitDone()
		}()
	}
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
