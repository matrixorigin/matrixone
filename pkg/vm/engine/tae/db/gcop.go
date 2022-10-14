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

package db

import (
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type GCType int16

const (
	GCType_Block GCType = iota
	GCType_Segment
	GCType_Table
	GCType_DB
)

// Destroy is not thread-safe
func gcBlockClosure(entry *catalog.BlockEntry, gct GCType) tasks.FuncT {
	return func() (err error) {
		logutil.Debugf("[GCBLK] | %s | Started", entry.Repr())
		defer func() {
			if err == nil {
				logutil.Debugf("[GCBLK] | %s | Removed", entry.Repr())
			} else {
				logutil.Warnf("Cannot remove block %s, maybe removed before", entry.String())
			}
		}()
		segment := entry.GetSegment()

		if err = entry.DestroyData(); err != nil {
			return
		}
		// For appendable segment, keep all soft-deleted blocks until the segment is soft-deleted
		if gct == GCType_Block && entry.IsAppendable() {
			return
		}
		err = segment.RemoveEntry(entry)
		return
	}
}

// Destroy is not thread-safe
func gcSegmentClosure(entry *catalog.SegmentEntry, gct GCType) tasks.FuncT {
	return func() (err error) {
		scopes := make([]common.ID, 0)
		logutil.Debugf("[GCSEG] | %s | Started", entry.Repr())
		defer func() {
			if err != nil {
				logutil.Warnf("Cannot remove segment %s, maybe removed before: %v", entry.String(), err)
			} else {
				logutil.Debugf("[GCSEG] | %s | BLKS=%s | Removed", entry.Repr(), common.IDArraryString(scopes))
			}
		}()
		table := entry.GetTable()
		it := entry.MakeBlockIt(false)
		for it.Valid() {
			blk := it.Get().GetPayload()
			scopes = append(scopes, *blk.AsCommonID())
			err = gcBlockClosure(blk, gct)()
			if err != nil {
				return
			}
			it.Next()
		}
		if err = entry.DestroyData(); err != nil {
			return
		}
		err = table.RemoveEntry(entry)
		return
	}
}

// TODO
func gcTableClosure(entry *catalog.TableEntry, gct GCType) tasks.FuncT {
	return func() (err error) {
		scopes := make([]common.ID, 0)
		logutil.Debugf("[GCTABLE] | %s | Started", entry.String())
		defer func() {
			logutil.Debugf("[GCTABLE] | %s | Ended: %v | SEGS=%s", entry.String(), err, common.IDArraryString(scopes))
		}()
		dbEntry := entry.GetDB()
		it := entry.MakeSegmentIt(false)
		for it.Valid() {
			seg := it.Get().GetPayload()
			scopes = append(scopes, *seg.AsCommonID())
			if err = gcSegmentClosure(seg, gct)(); err != nil {
				return
			}
			it.Next()
		}
		err = dbEntry.RemoveEntry(entry)
		return
	}
}

// TODO
func gcDatabaseClosure(entry *catalog.DBEntry) tasks.FuncT {
	return func() (err error) {
		return
		scopes := make([]common.ID, 0)
		logutil.Debugf("[GCDB] | %s | Started", entry.String())
		defer func() {
			logutil.Debugf("[GCDB] | %s | Ended: %v | TABLES=%s", entry.String(), err, common.IDArraryString(scopes))
		}()
		it := entry.MakeTableIt(false)
		for it.Valid() {
			table := it.Get().GetPayload()
			scopes = append(scopes, *table.AsCommonID())
			if err = gcTableClosure(table, GCType_DB)(); err != nil {
				return
			}
			it.Next()
		}
		err = entry.GetCatalog().RemoveEntry(entry)
		return
	}
}

type garbageCollector struct {
	*catalog.LoopProcessor
	db              *DB
	epoch           types.TS
	runTs           types.TS
	checkpointedLsn uint64
	clock           *types.TsAlloctor
	tree            *common.Tree
	minInterval     time.Duration
}

func newGarbageCollector(
	db *DB,
	minInterval time.Duration) *garbageCollector {
	ckp := &garbageCollector{
		LoopProcessor: new(catalog.LoopProcessor),
		db:            db,
		minInterval:   minInterval,
		clock:         types.NewTsAlloctor(db.Opts.Clock),
		tree:          common.NewTree(),
	}
	ckp.BlockFn = ckp.onBlock
	ckp.SegmentFn = ckp.onSegment
	ckp.TableFn = ckp.onTable
	ckp.DatabaseFn = ckp.onDatabase
	ckp.epoch = types.BuildTS(time.Now().UTC().UnixNano()-minInterval.Nanoseconds(), 0)
	return ckp
}

func (ckp *garbageCollector) PreExecute() (err error) {
	ckp.runTs = ckp.clock.Alloc()
	ckp.checkpointedLsn = ckp.db.Scheduler.GetCheckpointedLSN()
	// logutil.Infof("epoch=%s, lsn=%d", ckp.epoch.ToString(), ckp.checkpointedLsn)
	return
}

func (ckp *garbageCollector) PostExecute() (err error) {
	if ckp.tree.IsEmpty() {
		ckp.epoch = types.BuildTS(time.Now().UTC().UnixNano()-ckp.minInterval.Nanoseconds(), 0)
	}
	return
}

func (ckp *garbageCollector) isEntryCheckpointed(entry catalog.BaseEntry) bool {
	node := entry.GetLatestNodeLocked()
	index := node.GetLogIndex()
	if index == nil {
		return false
	}
	if index.LSN <= ckp.checkpointedLsn {
		return true
	}
	return false
}

func (ckp *garbageCollector) isCandidate(
	entry catalog.BaseEntry,
	terminated bool,
	rwlocker *sync.RWMutex) (ok bool) {
	entry.RLock()
	defer entry.RUnlock()
	ok = false
	if terminated {
		ok = ckp.isEntryCheckpointed(entry)
		return
	}
	if !entry.HasDropCommittedLocked() {
		return
	}
	if visible, _ := entry.IsVisible(ckp.epoch, rwlocker); visible {
		return
	}
	ok = ckp.isEntryCheckpointed(entry)
	return
}

func (ckp *garbageCollector) onBlock(entry *catalog.BlockEntry) (err error) {
	var ts types.TS
	var terminated bool
	if ts, terminated = entry.GetTerminationTS(); terminated {
		if ts.Greater(ckp.epoch) {
			terminated = false
		}
	}
	if ckp.isCandidate(entry.MetaBaseEntry, terminated, entry.RWMutex) {
		id := entry.AsCommonID()
		ckp.tree.AddBlock(entry.GetSegment().GetTable().GetDB().ID,
			id.TableID,
			id.SegmentID,
			id.BlockID)
	}
	return
}

func (ckp *garbageCollector) onSegment(entry *catalog.SegmentEntry) (err error) {
	// var ts types.TS
	// var terminated bool
	// if ts, terminated = entry.TryGetTerminatedTS(); terminated {
	// 	if ts.Greater(ckp.epoch) {
	// 		terminated = false
	// 	}
	// }
	// if ckp.isCandidate(entry.MetaBaseEntry, terminated, entry.RWMutex) {
	// 	id := entry.AsCommonID()
	// 	ckp.tree.AddSegment(entry.GetTable().GetDB().ID,
	// 		id.TableID,
	// 		id.SegmentID)
	// }
	return
}

func (ckp *garbageCollector) onTable(entry *catalog.TableEntry) (err error) {
	return
}

func (ckp *garbageCollector) onDatabase(entry *catalog.DBEntry) (err error) {
	return
}
