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

// LogtailCollector holds a read only reader, knows how to iter entry.
// Drive a entry visitor, which acts as an api resp builder
type LogtailCollector struct {
	scope   Scope
	did     uint64
	tid     uint64
	catalog *catalog.Catalog
	reader  *LogtailReader
	visitor catalog.Processor
}

// BindInfo set collect env args and these args don't affect dirty seg/blk ids
func (c *LogtailCollector) BindCollectEnv(scope Scope, catalog *catalog.Catalog, visitor catalog.Processor) {
	c.scope, c.catalog, c.visitor = scope, catalog, visitor
}

func (c *LogtailCollector) Collect() error {
	switch c.scope {
	case ScopeDatabases:
		return c.collectCatalogDB()
	case ScopeTables, ScopeColumns:
		return c.collectCatalogTbl()
	case ScopeUserTables:
		return c.collectUserTbl()
	default:
		panic("unknown logtail collect scope")
	}
}

func (c *LogtailCollector) collectUserTbl() (err error) {
	var (
		db  *catalog.DBEntry
		tbl *catalog.TableEntry
		seg *catalog.SegmentEntry
		blk *catalog.BlockEntry
	)
	if db, err = c.catalog.GetDatabaseByID(c.did); err != nil {
		return
	}
	if tbl, err = db.GetTableEntryByID(c.tid); err != nil {
		return
	}
	dirty := c.reader.GetDirtyByTable(c.tid)
	for _, dirtySeg := range dirty.Segs {
		if seg, err = tbl.GetSegmentByID(dirtySeg.Sig); err != nil {
			return err
		}
		if err = c.visitor.OnSegment(seg); err != nil {
			return err
		}
		for _, blkid := range dirtySeg.Blks {
			if blk, err = seg.GetBlockEntryByID(blkid); err != nil {
				return err
			}
			if err = c.visitor.OnBlock(blk); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *LogtailCollector) collectCatalogDB() error {
	if !c.reader.HasCatalogChanges() {
		return nil
	}
	dbIt := c.catalog.MakeDBIt(true)
	for ; dbIt.Valid(); dbIt.Next() {
		dbentry := dbIt.Get().GetPayload()
		if dbentry.IsSystemDB() {
			continue
		}
		if err := c.visitor.OnDatabase(dbentry); err != nil {
			return err
		}
	}
	return nil
}

func (c *LogtailCollector) collectCatalogTbl() error {
	if !c.reader.HasCatalogChanges() {
		return nil
	}
	dbIt := c.catalog.MakeDBIt(true)
	for ; dbIt.Valid(); dbIt.Next() {
		db := dbIt.Get().GetPayload()
		if db.IsSystemDB() {
			continue
		}
		tblIt := db.MakeTableIt(true)
		for ; tblIt.Valid(); tblIt.Next() {
			if err := c.visitor.OnTable(tblIt.Get().GetPayload()); err != nil {
				return err
			}
		}
	}
	return nil
}
