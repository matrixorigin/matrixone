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
			blk := it.Get().GetPayload().(*catalog.BlockEntry)
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
			seg := it.Get().GetPayload().(*catalog.SegmentEntry)
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
		scopes := make([]common.ID, 0)
		logutil.Debugf("[GCDB] | %s | Started", entry.String())
		defer func() {
			logutil.Debugf("[GCDB] | %s | Ended: %v | TABLES=%s", entry.String(), err, common.IDArraryString(scopes))
		}()
		it := entry.MakeTableIt(false)
		for it.Valid() {
			table := it.Get().GetPayload().(*catalog.TableEntry)
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
