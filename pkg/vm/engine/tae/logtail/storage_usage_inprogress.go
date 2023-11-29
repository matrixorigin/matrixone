// Copyright 2023 Matrix Origin
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

package logtail

import (
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/tidwall/btree"
	"sync"
	"time"
)

type UsageData_ struct {
	accId uint32
	dbId  uint64
	tblId uint64
	size  int64
}

func usageLess(a UsageData_, b UsageData_) bool {
	return a.accId < b.accId && a.dbId < b.dbId && a.tblId < b.tblId
}

type StorageUsageCache struct {
	// when two requests happens within [firstRequest, firstRequest + lazyThreshold],
	// it will reuse the cached result, no new query to TN.
	lazyThreshold time.Duration
	// the first requested time after an update
	firstRequest time.Time
	// accId -> dbId -> [tblId, size]
	data *btree.BTreeG[UsageData_]
}

type StorageUsageCacheOption = func(c *StorageUsageCache)

// WithLazyThreshold sets lazyThreshold to lazy seconds
func WithLazyThreshold(lazy int) StorageUsageCacheOption {
	return StorageUsageCacheOption(func(c *StorageUsageCache) {
		c.lazyThreshold = time.Second * time.Duration(lazy)
	})
}

func NewStorageUsageCache(opts ...StorageUsageCacheOption) *StorageUsageCache {
	cache := new(StorageUsageCache)
	cache.data = btree.NewBTreeG[UsageData_](usageLess)
	for _, opt := range opts {
		opt(cache)
	}
	return cache
}

type PendingUpdateType = int

const (
	// no pending update
	NUpdate PendingUpdateType = 0
	// incremental ckp pending
	IUpdate PendingUpdateType = 1
	// global ckp pending
	GUpdate PendingUpdateType = 2
)

type TNUsageMemo struct {
	sync.RWMutex
	cache *StorageUsageCache
	//pending PendingUpdateType
}

func NewTNUsageMemo() *TNUsageMemo {
	memo := new(TNUsageMemo)
	//memo.pending = NUpdate
	memo.cache = NewStorageUsageCache()
	return memo
}

var tnUsageMemo = NewTNUsageMemo()

func (m *TNUsageMemo) EnterProcessing() {
	m.Lock()
}

func (m *TNUsageMemo) LeaveProcessing() {
	m.Unlock()
}

func (m *TNUsageMemo) Update(usage UsageData_, del bool) {
	size := int64(0)
	if old, found := m.cache.data.Get(usage); found {
		size = old.size
	}

	if del {
		usage.size = size - usage.size
	} else {
		usage.size = size + usage.size
	}

	m.cache.data.Set(usage)
}

func checkSegment_(entry *catalog.SegmentEntry, collector *BaseCollector) bool {
	if !entry.IsSorted() || entry.IsAppendable() {
		return false
	}

	// the incremental ckp should consider the time range.
	// we only collect the segments which updates happened in [start, end]
	entry.RLock()
	cnt := len(entry.ClonePreparedInRange(collector.start, collector.end))
	entry.RUnlock()
	if !collector.isGlobal && cnt == 0 {
		return false
	}

	return true
}

// the returned order:
// [account_id_vec, db_id_vec, tbl_id_vec, size_vec]
func getStorageUsageBatVectors_(bat *containers.Batch) []*vector.Vector {
	accIDVec := bat.GetVectorByName(pkgcatalog.SystemColAttr_AccID).GetDownstreamVector()
	dbIDVec := bat.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector()
	tblIDVec := bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	sizeVec := bat.GetVectorByName(CheckpointMetaAttr_ObjectSize).GetDownstreamVector()

	return []*vector.Vector{
		accIDVec, dbIDVec, tblIDVec, sizeVec,
	}
}

func appendToStorageUsageBat_(positiveSize int64, negativeSize int64,
	data *CheckpointData, usage UsageData_, mp *mpool.MPool) {

	appendFunc := func(vecs []*vector.Vector, size int64) {
		vector.AppendFixed[int64](vecs[UsageSize], size, false, mp)
		vector.AppendFixed[uint32](vecs[UsageAccID], usage.accId, false, mp)
		vector.AppendFixed[uint64](vecs[UsageDBID], usage.dbId, false, mp)
		vector.AppendFixed[uint64](vecs[UsageTblID], usage.tblId, false, mp)
	}

	if positiveSize != 0 {
		insVecs := getStorageUsageBatVectors_(data.bats[StorageUsageInsIDX])
		appendFunc(insVecs, positiveSize)
		tnUsageMemo.Update(UsageData_{
			usage.accId, usage.dbId,
			usage.tblId, positiveSize}, false)
	}

	if negativeSize != 0 {
		delVecs := getStorageUsageBatVectors_(data.bats[StorageUsageDelIDX])
		appendFunc(delVecs, negativeSize)
		tnUsageMemo.Update(UsageData_{
			usage.accId, usage.dbId,
			usage.tblId, positiveSize}, true)
	}
}

func segEntryToUsageData(entry *catalog.SegmentEntry) UsageData_ {
	return UsageData_{
		size:  0,
		tblId: entry.GetTable().GetID(),
		dbId:  entry.GetTable().GetDB().GetID(),
		accId: entry.GetTable().GetDB().GetTenantID(),
	}
}

func traverseCatalog_(
	c *catalog.Catalog,
	collector *BaseCollector,
	mp *mpool.MPool) {

	var lastEntry *catalog.SegmentEntry
	positiveSize := int64(0)
	negativeSize := int64(0)

	isSameTable := func(current *catalog.SegmentEntry) bool {
		if lastEntry == nil {
			lastEntry = current
			return true
		}
		return lastEntry.GetTable().GetID() == current.GetTable().GetID()
	}

	processor := new(catalog.LoopProcessor)
	processor.SegmentFn = func(entry *catalog.SegmentEntry) error {
		// only the non-appendable can pass
		if !checkSegment_(entry, collector) {
			return nil
		}

		// table changed, append the previous table's usage info to batch
		if !isSameTable(entry) {
			appendToStorageUsageBat_(
				positiveSize, negativeSize, collector.data,
				segEntryToUsageData(entry), mp)
			lastEntry = entry
			negativeSize = 0
			positiveSize = 0
		}

		entry.RLock()
		node := entry.GetLatestNodeLocked()
		entry.RUnlock()

		if node.HasDropCommitted() {
			// deleted segment
			negativeSize += int64(node.BaseNode.ObjectStats.Size())
		} else if node.IsCommitted() {
			// flushed already
			positiveSize += int64(node.BaseNode.ObjectStats.Size())
		}

		return nil
	}

	c.RecurLoop(processor)

	// deal with the last table's usage info
	appendToStorageUsageBat_(
		positiveSize, negativeSize, collector.data,
		segEntryToUsageData(lastEntry), mp)
}

func FillUsageBatOfIncremental_(c *catalog.Catalog, collector *IncrementalCollector) {
	start := time.Now()

	tnUsageMemo.EnterProcessing()
	defer func() {
		tnUsageMemo.LeaveProcessing()
		v2.TaskICkpCollectUsageDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	traverseCatalog_(c, collector.BaseCollector, collector.Allocator())
}

func applyDeletesOnTNUsageMemo(collector *GlobalCollector) {
	var dbs []*catalog.DBEntry
	for _, del := range collector.deletes_ {
		switch e := del.(type) {
		case *catalog.DBEntry:
			dbs = append(dbs, e)
		case *catalog.TableEntry:
			tnUsageMemo.cache.data.Delete(UsageData_{
				e.GetDB().GetTenantID(),
				e.GetDB().GetID(), e.GetID(), 0})
		case *catalog.SegmentEntry:
			e.RLock()
			node := e.GetLatestNodeLocked()
			e.RUnlock()

			tnUsageMemo.Update(UsageData_{
				e.GetTable().GetDB().GetTenantID(),
				e.GetTable().GetDB().GetID(),
				e.GetTable().GetID(),
				int64(node.BaseNode.ObjectStats.Size())}, true)
		}
	}

	iter := tnUsageMemo.cache.data.Iter()
	var tbls []uint64
	for _, db := range dbs {
		if found := iter.Seek(UsageData_{
			db.GetTenantID(), db.ID, 0, 0}); !found {
			continue
		}

		tbls = append(tbls, iter.Item().tblId)
		for iter.Next() {
			tbls = append(tbls, iter.Item().tblId)
		}

		for idx := 0; idx < len(tbls); idx++ {
			tnUsageMemo.cache.data.Delete(UsageData_{
				db.GetTenantID(), db.ID, tbls[idx], 0,
			})
		}

		tbls = tbls[:0]
	}
}

func replayTNUsageMemoOnGCKP(collector *GlobalCollector) {
	iter := tnUsageMemo.cache.data.Iter()
	for iter.Next() {
		usage := iter.Item()
		appendToStorageUsageBat_(
			usage.size, 0,
			collector.data, usage, collector.Allocator())
	}
}

func FillUsageBatOfGlobal_(
	c *catalog.Catalog,
	collector *GlobalCollector) {
	start := time.Now()

	tnUsageMemo.EnterProcessing()
	defer func() {
		tnUsageMemo.LeaveProcessing()
		v2.TaskGCkpCollectUsageDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	applyDeletesOnTNUsageMemo(collector)
	replayTNUsageMemoOnGCKP(collector)

}
