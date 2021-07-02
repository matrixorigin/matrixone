package memdata

import (
	table "matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	// log "github.com/sirupsen/logrus"
)

func NewCreateTableOp(ctx *OpCtx) *CreateTableOp {
	op := &CreateTableOp{}
	op.Op = *NewOp(op, ctx, ctx.Opts.MemData.Updater)
	return op
}

type CreateTableOp struct {
	Op
	Collection imem.ICollection
}

func (op *CreateTableOp) Execute() error {
	collection := op.Ctx.MTManager.StrongRefCollection(op.Ctx.TableMeta.ID)
	if collection != nil {
		op.Collection = collection
		return nil
	}
	meta := op.Ctx.TableMeta

	tableData, err := op.Ctx.Tables.StrongRefTable(meta.ID)
	if err != nil {
		tableData = table.NewTableData(op.Ctx.FsMgr, op.Ctx.IndexBufMgr, op.Ctx.MTBufMgr, op.Ctx.SSTBufMgr, meta)
		err = op.Ctx.Tables.CreateTable(tableData)
		if err != nil {
			return err
		}
	}
	tableData.Ref()
	collection, err = op.Ctx.MTManager.RegisterCollection(tableData)
	if err != nil {
		return err
	}

	op.Collection = collection

	return nil
}
