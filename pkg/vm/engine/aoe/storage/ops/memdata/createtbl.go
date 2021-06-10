package memdata

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table"
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
	collection := op.Ctx.MTManager.GetCollection(op.Ctx.TableMeta.ID)
	if collection != nil {
		op.Collection = collection
		return nil
	}
	meta := op.Ctx.TableMeta
	colTypes := make([]types.Type, 0, len(meta.Schema.ColDefs))
	for _, colDef := range meta.Schema.ColDefs {
		colTypes = append(colTypes, colDef.Type)
	}
	tableData := table.NewTableData(op.Ctx.MTBufMgr, op.Ctx.SSTBufMgr, meta.ID, colTypes)
	err := op.Ctx.Tables.CreateTable(tableData)
	if err != nil {
		return err
	}
	collection, err = op.Ctx.MTManager.RegisterCollection(tableData)
	if err != nil {
		return err
	}

	op.Collection = collection

	return nil
}
