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
	scope   Scope
}

func NewBoundTableOperator(catalog *catalog.Catalog,
	reader *Reader,
	scope Scope,
	dbID, tableID uint64,
	visitor catalog.Processor) *BoundTableOperator {
	return &BoundTableOperator{
		catalog: catalog,
		reader:  reader,
		visitor: visitor,
		tableID: tableID,
		dbID:    dbID,
		scope:   scope,
	}
}

// Run takes a RespBuilder to visit every table/segment/block touched by all txn
// in the Reader. During the visiting, RespBuiler will fetch information to return logtail entry
func (c *BoundTableOperator) Run() error {
	switch c.scope {
	case ScopeDatabases:
		return c.processDatabases()
	case ScopeTables, ScopeColumns:
		return c.processTables()
	case ScopeUserTables:
		return c.processTableData()
	default:
		panic("unknown logtail collect scope")
	}
}

// For normal user table, pick out all dirty blocks and call OnBlock
func (c *BoundTableOperator) processTableData() (err error) {
	var (
		db  *catalog.DBEntry
		tbl *catalog.TableEntry
		seg *catalog.SegmentEntry
		blk *catalog.BlockEntry
	)
	if db, err = c.catalog.GetDatabaseByID(c.dbID); err != nil {
		return
	}
	if tbl, err = db.GetTableEntryByID(c.tableID); err != nil {
		return
	}
	dirty := c.reader.GetDirtyByTable(c.dbID, c.tableID)
	for _, dirtySeg := range dirty.Segs {
		if seg, err = tbl.GetSegmentByID(dirtySeg.ID); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
				err = nil
				continue
			}
			return
		}
		if err = c.visitor.OnSegment(seg); err != nil {
			return err
		}
		for id := range dirtySeg.Blks {
			if blk, err = seg.GetBlockEntryByID(id); err != nil {
				if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
					err = nil
					continue
				}
				return
			}
			if err = c.visitor.OnBlock(blk); err != nil {
				return err
			}
		}
	}
	return nil
}

// For mo_database, iterate over all database and call OnBlock.
// TODO: avoid iterating all. For now it is acceptable because all catalog is in
// memory and ddl is much smaller than dml
func (c *BoundTableOperator) processDatabases() error {
	if !c.reader.HasCatalogChanges() {
		return nil
	}
	dbIt := c.catalog.MakeDBIt(true)
	for ; dbIt.Valid(); dbIt.Next() {
		dbentry := dbIt.Get().GetPayload()
		if err := c.visitor.OnDatabase(dbentry); err != nil {
			return err
		}
	}
	return nil
}

// For mo_table and mo_columns, iterate over all tables and call OnTable
// TODO: avoid iterating all. For now it is acceptable because all catalog is in
// memory and ddl is much smaller than dml
func (c *BoundTableOperator) processTables() error {
	if !c.reader.HasCatalogChanges() {
		return nil
	}
	dbIt := c.catalog.MakeDBIt(true)
	for ; dbIt.Valid(); dbIt.Next() {
		db := dbIt.Get().GetPayload()
		tblIt := db.MakeTableIt(true)
		for ; tblIt.Valid(); tblIt.Next() {
			if err := c.visitor.OnTable(tblIt.Get().GetPayload()); err != nil {
				return err
			}
		}
	}
	return nil
}
