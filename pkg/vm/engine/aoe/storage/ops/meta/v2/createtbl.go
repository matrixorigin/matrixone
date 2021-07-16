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
	tbl, err := op.Ctx.Opts.Meta.Info.CreateTableFromTableInfo(op.Ctx.TableInfo, op.LocalCtx)
	if err != nil {
		return err
	}
	var table *md.Table
	{
		op.Result = tbl
		ctx := md.CopyCtx{Ts: md.NowMicro() + 1, Attached: true}
		info := op.Ctx.Opts.Meta.Info.Copy(ctx)
		table, _ = info.ReferenceTable(tbl.ID)
		opCtx := OpCtx{Opts: op.Ctx.Opts}
		flushOp := NewFlushInfoOp(&opCtx, info)
		flushOp.Push()
		go func() {
			flushOp.WaitDone()
		}()
	}
	{
		ctx := OpCtx{Opts: op.Ctx.Opts}
		flushOp := NewFlushTblOp(&ctx, table)
		flushOp.Push()
		go func() {
			flushOp.WaitDone()
		}()
	}
	return err
}
