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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/events/memdata"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/muthandle/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/ops"
)

type dropTblRequest struct {
	gc.BaseRequest
	// Tables' meta
	Tables *table.Tables
	// Table id of the dropped table
	Meta        *metadata.Table
	MemTableMgr base.IManager
	Opts        *storage.Options
	CB          dbi.OnTableDroppedCB
}

func NewDropTblRequest(opts *storage.Options, meta *metadata.Table, tables *table.Tables, mtMgr base.IManager, cb dbi.OnTableDroppedCB) *dropTblRequest {
	req := new(dropTblRequest)
	req.Meta = meta
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
	ctx := &sched.Context{
		Opts:     req.Opts,
		Waitable: true,
	}
	e := memdata.NewDropTableEvent(ctx, req.Meta.Id, req.Tables)
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
	c, err := req.MemTableMgr.UnregisterTable(req.Meta.Id)
	if err != nil {
		segIds := req.Meta.SimpleGetSegmentIds()
		if len(segIds) == 0 {
			err = nil
		} else if c = req.MemTableMgr.WeakRefTable(req.Meta.Id); c == nil {
			err = nil
		} else {
			if req.Iteration < 3 {
				return err
			}
			err = nil
		}
	}
	if c != nil {
		c.Unref()
	}
	if err = req.Meta.Database.SimpleHardDeleteTable(req.Meta.Id); err != nil {
		panic(err)
	}
	if req.CB != nil {
		req.CB(nil)
	}
	return err
}
