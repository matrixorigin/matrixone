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

// Destroy is not thread-safe
func gcBlockClosure(entry *catalog.BlockEntry) tasks.FuncT {
	return func() error {
		logutil.Debugf("[GCBLK] | %s | Started", entry.Repr())
		segment := entry.GetSegment()
		segment.RLock()
		segDropped := segment.IsDroppedCommitted()
		segment.RUnlock()

		entry.DestroyData()
		if !segDropped && entry.IsAppendable() {
			return nil
		}
		err := segment.RemoveEntry(entry)
		logutil.Infof("[GCBLK] | %s | Removed", entry.Repr())
		if err != nil {
			logutil.Warnf("Cannot remove block %s, maybe removed before", entry.String())
			return err
		}
		return nil
	}
}

// Destroy is not thread-safe
func gcSegmentClosure(entry *catalog.SegmentEntry) tasks.FuncT {
	return func() error {
		logutil.Debugf("[GCSEG] | %s | Started", entry.Repr())
		table := entry.GetTable()
		scopes := make([]common.ID, 0)
		it := entry.MakeBlockIt(false)
		for it.Valid() {
			blk := it.Get().GetPayload().(*catalog.BlockEntry)
			scopes = append(scopes, *blk.AsCommonID())
			gcBlockClosure(blk)()
			it.Next()
		}
		err := table.RemoveEntry(entry)
		logutil.Infof("[GCSEG] | %s | BLKS=%s | Removed", entry.Repr(), common.IDArraryString(scopes))
		if err != nil {
			logutil.Warnf("Cannot remove segment %s, maybe removed before", entry.String())
			return err
		}
		return nil
	}
}
