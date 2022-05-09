package db

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

// Destory is not thread-safe
func gcBlockClosure(entry *catalog.BlockEntry) tasks.FuncT {
	return func() error {
		// logutil.Infof("[GC] | Block | %s", entry.String())
		// return nil
		segment := entry.GetSegment()
		segment.RLock()
		segDropped := segment.IsDroppedCommitted()
		segment.RUnlock()

		entry.DestroyData()
		if !segDropped && entry.IsAppendable() {
			return nil
		}
		err := segment.RemoveEntry(entry)
		if err != nil {
			logutil.Warnf("Cannot remove block %s, maybe removed before", entry.String())
			return err
		}
		return nil
	}
}

// Destory is not thread-safe
func gcSegmentClosure(entry *catalog.SegmentEntry) tasks.FuncT {
	return func() error {
		logutil.Infof("[GC] | Segment | %s", entry.String())
		table := entry.GetTable()
		it := entry.MakeBlockIt(true)
		if it.Valid() {
			blk := it.Get().GetPayload().(*catalog.BlockEntry)
			gcBlockClosure(blk)()
			it.Next()
		}
		err := table.RemoveEntry(entry)
		if err != nil {
			logutil.Warnf("Cannot remove segment %s, maybe removed before", entry.String())
			return err
		}
		return nil
	}
}
