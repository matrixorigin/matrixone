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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

// BoundTableOperator holds a read only reader, knows how to iter entry.
// Drive a entry visitor, which acts as an api resp builder
type BaseOperator struct {
	catalog *catalog.Catalog
	reader  *LogtailReader
}

type BoundOperator struct {
	*BaseOperator
	visitor catalog.Processor
}

func NewBoundOperator(catalog *catalog.Catalog,
	reader *LogtailReader,
	visitor catalog.Processor) *BoundOperator {
	return &BoundOperator{
		BaseOperator: &BaseOperator{
			catalog: catalog,
			reader:  reader,
		},
		visitor: visitor,
	}
}

func (op *BoundOperator) Run() (err error) {
	var (
		db  *catalog.DBEntry
		tbl *catalog.TableEntry
		seg *catalog.SegmentEntry
		blk *catalog.BlockEntry
	)
	dirty, _ := op.reader.GetDirty()
	for _, tblDirty := range dirty.Tables {
		if db, err = op.catalog.GetDatabaseByID(tblDirty.DbID); err != nil {
			return
		}
		if tbl, err = db.GetTableEntryByID(tblDirty.ID); err != nil {
			return
		}
		for _, dirtySeg := range tblDirty.Segs {
			if seg, err = tbl.GetSegmentByID(dirtySeg.ID); err != nil {
				return err
			}
			if err = op.visitor.OnSegment(seg); err != nil {
				return err
			}
			for id := range dirtySeg.Blks {
				if blk, err = seg.GetBlockEntryByID(id); err != nil {
					return err
				}
				if err = op.visitor.OnBlock(blk); err != nil {
					return err
				}
			}
		}
	}
	return
}

type BoundTableOperator struct {
	*BoundOperator
	dbID    uint64
	tableID uint64
	scope   Scope
}

func NewBoundTableOperator(catalog *catalog.Catalog,
	reader *LogtailReader,
	scope Scope,
	dbID, tableID uint64,
	visitor catalog.Processor) *BoundTableOperator {
	return &BoundTableOperator{
		BoundOperator: NewBoundOperator(catalog, reader, visitor),
		tableID:       tableID,
		dbID:          dbID,
		scope:         scope,
	}
}

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
			return err
		}
		if err = c.visitor.OnSegment(seg); err != nil {
			return err
		}
		for id := range dirtySeg.Blks {
			if blk, err = seg.GetBlockEntryByID(id); err != nil {
				return err
			}
			if err = c.visitor.OnBlock(blk); err != nil {
				return err
			}
		}
	}
	return nil
}

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
