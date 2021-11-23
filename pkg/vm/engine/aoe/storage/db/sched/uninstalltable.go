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

package sched

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
)

type uninstallTableEvent struct {
	BaseEvent

	// Table's id, aoe is generated when the table is created
	TableId uint64

	// Data is Table's metadata in memory
	Data   iface.ITableData
	Tables *table.Tables
}

func NewUninstallTableEvent(ctx *Context, tableId uint64, tables *table.Tables) *uninstallTableEvent {
	e := &uninstallTableEvent{
		TableId: tableId,
		Tables:  tables,
	}
	e.BaseEvent = *NewBaseEvent(e, MemdataUpdateEvent, ctx)
	return e
}

// Remove and release a table from Tables
func (e *uninstallTableEvent) Execute() error {
	tbl, err := e.Tables.DropTable(e.TableId)
	e.Data = tbl
	return err
}
