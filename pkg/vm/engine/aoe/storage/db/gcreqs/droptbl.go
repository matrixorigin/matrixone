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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/ops"
)

type dropTblRequest struct {
	gc.BaseRequest
	// Tables' meta
	Tables *table.Tables
	// Table id of the dropped table
	Meta *metadata.Table
	Opts *storage.Options
	CB   dbi.OnTableDroppedCB
}

func NewDropTblRequest(opts *storage.Options, meta *metadata.Table, tables *table.Tables, cb dbi.OnTableDroppedCB) *dropTblRequest {
	req := new(dropTblRequest)
	req.Meta = meta
	req.Tables = tables
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
	e := sched.NewUninstallTableEvent(ctx, req.Meta.Id, req.Tables)
	err := req.Opts.Scheduler.Schedule(e)
	if err != nil {
		return err
	}
	err = e.WaitDone()
	if err != nil && err != table.ErrNotExist {
		return err
	} else if err == nil {
		e.Data.Unref()
	}
	if err = req.Meta.Database.SimpleHardDeleteTable(req.Meta.Id); err != nil {
		panic(err)
	}
	if req.CB != nil {
		req.CB(nil)
	}
	return err
}
