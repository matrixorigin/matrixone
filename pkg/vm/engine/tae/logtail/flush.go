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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/tidwall/btree"
)

/*

an application on logtail mgr: flush table data

*/

type TSRangedTree struct {
	start, end types.TS
	tree       *common.Tree
}

type FlushMgr struct {
	mgr        *LogtailMgr
	catalog    *catalog.Catalog
	visitor    catalog.Processor
	dirtyTrees *btree.Generic[*TSRangedTree]
}

func NewFlushMgr(logtail *LogtailMgr, catalog *catalog.Catalog, visitor catalog.Processor) *FlushMgr {
	return &FlushMgr{
		mgr:        logtail,
		catalog:    catalog,
		visitor:    visitor,
		dirtyTrees: btree.NewGeneric(func(a, b *TSRangedTree) bool { return a.start.Less(b.start) && a.end.Less(b.end) }),
	}
}

func (f *FlushMgr) produceDirty(start, end types.TS) *TSRangedTree {
	reader := f.mgr.GetReader(start, end)
	tree := reader.GetDirty()
	return &TSRangedTree{
		start: start,
		end:   end,
		tree:  tree,
	}
}

func (f *FlushMgr) AppendDirtyTree(start, end types.TS) {
	dirty := f.produceDirty(start, end)
	f.dirtyTrees.Set(dirty)
}

func (f *FlushMgr) TrySchedFlush() {
	dels := make([]*TSRangedTree, 0)
	f.dirtyTrees.Scan(func(item *TSRangedTree) bool {
		if item.tree.TableCount() == 0 {
			dels = append(dels, item)
			return true
		}
		if err := f.driveVisitorOnTree(f.visitor, item.tree); err != nil {
			logutil.Infof("visitor on tree: %v", err)
		}
		return true
	})

	for _, del := range dels {
		f.dirtyTrees.Delete(del)
	}
}

// the tree maybe getting smaller after driving
func (f *FlushMgr) driveVisitorOnTree(visitor catalog.Processor, tree *common.Tree) (err error) {
	var (
		db  *catalog.DBEntry
		tbl *catalog.TableEntry
		seg *catalog.SegmentEntry
		blk *catalog.BlockEntry
	)
	for id, tblDirty := range tree.Tables {
		// remove empty tables
		if len(tblDirty.Segs) == 0 {
			delete(tree.Tables, id)
			return
		}
		if db, err = f.catalog.GetDatabaseByID(tblDirty.DbID); err != nil {
			return
		}
		if tbl, err = db.GetTableEntryByID(tblDirty.ID); err != nil {
			return
		}

		for id, dirtySeg := range tblDirty.Segs {
			// remove empty segs
			if len(dirtySeg.Blks) == 0 {
				delete(tblDirty.Segs, id)
				continue
			}

			if seg, err = tbl.GetSegmentByID(dirtySeg.ID); err != nil {
				return
			}
			for id := range dirtySeg.Blks {
				if blk, err = seg.GetBlockEntryByID(id); err != nil {
					return
				}
				// if blk has been flushed, remove it
				if blk.GetMetaLoc() != "" {
					delete(dirtySeg.Blks, id)
					continue
				}
				if err = visitor.OnBlock(blk); err != nil {
					return
				}
			}
		}
	}
	return
}

type FlushDirtyVisitor struct {
	*catalog.LoopProcessor
	ckpDriver checkpoint.Driver
}

func NewFlushDirtyVisitor(ckpDriver checkpoint.Driver) *FlushDirtyVisitor {
	v := &FlushDirtyVisitor{ckpDriver: ckpDriver}
	v.BlockFn = v.visitBlock
	return v
}

func (v *FlushDirtyVisitor) visitBlock(entry *catalog.BlockEntry) (err error) {
	data := entry.GetBlockData()

	// Run calibration and estimate score for checkpoint
	if data.RunCalibration() > 0 {
		v.ckpDriver.EnqueueCheckpointUnit(data)
	}
	return
}
