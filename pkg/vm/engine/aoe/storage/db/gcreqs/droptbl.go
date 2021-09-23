// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gcreqs

import (
	"matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/events/memdata"
	"matrixone/pkg/vm/engine/aoe/storage/gc"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	mtif "matrixone/pkg/vm/engine/aoe/storage/memtable/v1/base"
	"matrixone/pkg/vm/engine/aoe/storage/ops"
)

type dropTblRequest struct {
	gc.BaseRequest
	// Tables' meta
	Tables *table.Tables
	// Table id of the dropped table
	TableId     uint64
	MemTableMgr mtif.IManager
	Opts        *storage.Options
	CB          dbi.OnTableDroppedCB
}

func NewDropTblRequest(opts *storage.Options, id uint64, tables *table.Tables, mtMgr mtif.IManager, cb dbi.OnTableDroppedCB) *dropTblRequest {
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
