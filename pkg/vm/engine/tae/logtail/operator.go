// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtail

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

// BoundTableOperator holds a read only reader, knows how to iterate catalog entries.
type BoundTableOperator struct {
	catalog *catalog.Catalog
	reader  *Reader
	visitor catalog.Processor
	dbID    uint64
	tableID uint64
}

func NewBoundTableOperator(catalog *catalog.Catalog,
	reader *Reader,
	dbID, tableID uint64,
	visitor catalog.Processor) *BoundTableOperator {
	return &BoundTableOperator{
		catalog: catalog,
		reader:  reader,
		visitor: visitor,
		tableID: tableID,
		dbID:    dbID,
	}
}

// Run takes a RespBuilder to visit every table/Object/block touched by all txn
// in the Reader. During the visiting, RespBuiler will fetch information to return logtail entry
func (c *BoundTableOperator) Run() error {
	return c.processTableData()
}

// For normal user table, pick out all dirty blocks and call OnBlock
func (c *BoundTableOperator) processTableData() error {
	db, err := c.catalog.GetDatabaseByID(c.dbID)
	if err != nil {
		return err
	}
	tbl, err := db.GetTableEntryByID(c.tableID)
	if err != nil {
		return err
	}
	dirty := c.reader.GetDirtyByTable(c.dbID, c.tableID)
	for _, dirtyObj := range dirty.Objs {
		obj, err := tbl.GetObjectByID(dirtyObj.ID, false)
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
				continue
			}
			return err
		}
		if err = c.visitor.OnObject(obj); err != nil {
			return err
		}

	}
	for _, dirtyObj := range dirty.Tombstones {
		obj, err := tbl.GetObjectByID(dirtyObj.ID, true)
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
				continue
			}
			return err
		}
		if err = c.visitor.OnTombstone(obj); err != nil {
			return err
		}

	}
	return nil
}
