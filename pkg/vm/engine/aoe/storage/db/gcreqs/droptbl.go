package gcreqs

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/gc"
	// "matrixone/pkg/vm/engine/aoe/storage/gc/gci"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	"matrixone/pkg/vm/engine/aoe/storage/ops"
	mdops "matrixone/pkg/vm/engine/aoe/storage/ops/memdata/v2"
)

type dropTblRequest struct {
	gc.BaseRequest
	Tables  *table.Tables
	TableId uint64
	Opts    *e.Options
}

func NewDropTblRequest(opts *e.Options, id uint64, tables *table.Tables) *dropTblRequest {
	req := new(dropTblRequest)
	req.TableId = id
	req.Tables = tables
	req.Opts = opts
	req.Op = ops.Op{
		Impl:   req,
		ErrorC: make(chan error),
	}
	return req
}

func (req *dropTblRequest) Execute() error {
	ctx := mdops.OpCtx{Opts: req.Opts, Tables: req.Tables}
	op := mdops.NewDropTblOp(&ctx, req.TableId)
	op.Push()
	err := op.WaitDone()
	if err != nil && err != table.NotExistErr {
		return err
	} else {
		err = nil
	}
	return err
}
