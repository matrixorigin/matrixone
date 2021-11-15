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
	"fmt"
	"time"

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
	Tables         *table.Tables
	DB             *metadata.Database
	MemTableMgr    mtif.IManager
	Opts           *storage.Options
	Cleaner        *metadata.Cleaner
	needReschedule bool
	startTime      time.Time
}

func NewDropDBRequest(opts *storage.Options, meta *metadata.Database, tables *table.Tables, mtMgr mtif.IManager) *dropDBRequest {
	req := new(dropDBRequest)
	req.DB = meta
	req.Tables = tables
	req.Opts = opts
	req.MemTableMgr = mtMgr
	req.StartTime = time.Now()
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
		req.needReschedule = true
	}
	return nil
}

func (req *dropDBRequest) Execute() error {
	err := req.Cleaner.TryCompactDB(req.DB, req.dropTable)
	if err != nil {
		return err
	}
	if time.Since(req.StartTime) >= time.Duration(10)*time.Second {
		panic(fmt.Sprintf("cannot gc db %d", req.DB.Id))
	}
	if req.needReschedule {
		req.needReschedule = false
		req.Next = req
	}

	return err
}
