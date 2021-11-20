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

package memdata

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/muthandle/base"
	mtif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/muthandle/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type createTableEvent struct {
	sched.BaseEvent

	// Handle manages the memTable data of a table(creates Block and
	// creates memTable) and provides Append() interface externally
	Handle base.MutableTable
	MTMgr  mtif.IManager
	Tables *table.Tables
	Meta   *metadata.Table
}

func NewCreateTableEvent(ctx *sched.Context, meta *metadata.Table, mtMgr mtif.IManager, tables *table.Tables) *createTableEvent {
	e := &createTableEvent{
		MTMgr:  mtMgr,
		Meta:   meta,
		Tables: tables,
	}
	e.BaseEvent = *sched.NewBaseEvent(e, sched.MemdataUpdateEvent, ctx)
	return e
}

// 1. Create a Handle
// 2. Create and register a TableData
// 3. Register Handle to the memTable manager
func (e *createTableEvent) Execute() error {
	handle := e.MTMgr.StrongRefTable(e.Meta.Id)
	if handle != nil {
		e.Handle = handle
		return nil
	}

	// FIXME: table is dropped by another thread
	data, err := e.Tables.StrongRefTable(e.Meta.Id)
	if err != nil {
		data, err = e.Tables.RegisterTable(e.Meta)
		if err != nil {
			return err
		}
		data.Ref()
	}
	handle, err = e.MTMgr.RegisterTable(data)
	if err != nil {
		data.Unref()
		return err
	}

	e.Handle = handle

	return nil
}
