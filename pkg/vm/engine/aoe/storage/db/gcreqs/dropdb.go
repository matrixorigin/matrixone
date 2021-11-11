package gcreqs

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	mtif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/memtable/v1/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type dropDBRequest struct {
	gc.BaseRequest
	Tables      *table.Tables
	DB          *metadata.Database
	MemTableMgr mtif.IManager
	Opts        *storage.Options
}

func (req *dropDBRequest) IncIteration() {}

func (req *dropDBRequest) dropTable(meta *metadata.Table) error {
	task := NewDropTblRequest(req.Opts, meta, req.Tables, req.MemTableMgr, nil)
	return task.Execute()
}

func (req *dropDBRequest) Execute() error {
	tables := make([]*metadata.Table, 0)
	processor := new(metadata.LoopProcessor)
	processor.TableFn = func(t *metadata.Table) error {
		var err error
		if t.IsHardDeleted() {
			return err
		}
		tables = append(tables, t)
		return err
	}

	req.DB.RLock()
	req.DB.LoopLocked(processor)
	req.DB.RUnlock()

	if len(tables) == 0 {
		if err := req.DB.SimpleHardDelete(); err != nil {
			panic(err)
		}
		req.Next = nil
		return nil
	}
	for _, t := range tables {
		if t.IsDeleted() {
			continue
		}
		if err := req.dropTable(t); err != nil {
			logutil.Warn(err.Error())
		}
	}
	req.Next = req
	return nil
}
