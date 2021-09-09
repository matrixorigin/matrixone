package gcreqs

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/events/memdata"
	"matrixone/pkg/vm/engine/aoe/storage/gc"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	mtif "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	"matrixone/pkg/vm/engine/aoe/storage/ops"
)

type dropTblRequest struct {
	gc.BaseRequest
	Tables      *table.Tables
	TableId     uint64
	MemTableMgr mtif.IManager
	Opts        *e.Options
	CB          dbi.OnTableDroppedCB
}

func NewDropTblRequest(opts *e.Options, id uint64, tables *table.Tables, mtMgr mtif.IManager, cb dbi.OnTableDroppedCB) *dropTblRequest {
	req := new(dropTblRequest)
	req.TableId = id
	req.Tables = tables
	req.MemTableMgr = mtMgr
	req.Opts = opts
	req.CB = cb
	req.Op = ops.Op{
		Impl:   req,
		ErrorC: make(chan error),
	}
	return req
}

func (req *dropTblRequest) Execute() error {
	ctx := &memdata.Context{
		Opts:     req.Opts,
		Tables:   req.Tables,
		Waitable: true,
	}
	e := memdata.NewDropTableEvent(ctx, req.TableId)
	err := req.Opts.Scheduler.Schedule(e)
	if err != nil {
		return err
	}
	err = e.WaitDone()
	if err != nil && err != table.NotExistErr {
		return err
	} else if err != nil {
		err = nil
	} else {
		e.Data.Unref()
	}
	c, err := req.MemTableMgr.UnregisterCollection(req.TableId)
	if err != nil {
		if req.Iteration < 3 {
			return err
		}
		err = nil
	}
	if c != nil {
		c.Unref()
	}
	if req.CB != nil {
		req.CB(nil)
	}
	return err
}
