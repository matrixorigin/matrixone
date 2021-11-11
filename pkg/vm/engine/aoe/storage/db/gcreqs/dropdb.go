package gcreqs

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	mtif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/memtable/v1/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/ops"
)

type dropDBRequest struct {
	gc.BaseRequest
	Tables      *table.Tables
	DB          *metadata.Database
	MemTableMgr mtif.IManager
	Opts        *storage.Options
	Cleaner     *metadata.Cleaner
}

func NewDropDBRequest(opts *storage.Options, meta *metadata.Database, tables *table.Tables, mtMgr mtif.IManager) *dropDBRequest {
	req := new(dropDBRequest)
	req.DB = meta
	req.Tables = tables
	req.Opts = opts
	req.Cleaner = metadata.NewCleaner(opts.Meta.Catalog)
	req.Op = ops.Op{
		Impl:   req,
		ErrorC: make(chan error),
	}
	return req
}

func (req *dropDBRequest) IncIteration() {}

func (req *dropDBRequest) dropTable(meta *metadata.Table) error {
	task := NewDropTblRequest(req.Opts, meta, req.Tables, req.MemTableMgr, nil)
	if err := task.Execute(); err != nil {
		logutil.Warn(err.Error())
	}
	return nil
}

func (req *dropDBRequest) Execute() error {
	err := req.Cleaner.TryCompactDB(req.DB, req.dropTable)
	if err != nil {
		req.Next = req
	}
	return nil
}
