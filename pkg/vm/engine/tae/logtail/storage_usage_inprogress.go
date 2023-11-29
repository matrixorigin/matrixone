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
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"time"
)

type StorageUsageCache struct {
	// when two requests happens within [firstRequest, firstRequest + lazyThreshold],
	// it will reuse the cached result, no new query to TN.
	lazyThreshold time.Duration
	// the first requested time after an update
	firstRequest time.Time
	// accId -> dbId -> [tblId, size]
	data map[uint32]map[uint64][2]uint64
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
	cache.data = make(map[uint32]map[uint64][2]uint64)
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
	cache   *StorageUsageCache
	pending PendingUpdateType
}

func NewTNUsageMemo() *TNUsageMemo {
	memo := new(TNUsageMemo)
	memo.pending = NUpdate
	memo.cache = NewStorageUsageCache()
	return memo
}

var tnUsageMemo = NewTNUsageMemo()

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

func appendToStorageUsageBat_(positiveSize uint64, negativeSize uint64,
	data *CheckpointData, entry *catalog.SegmentEntry, mp *mpool.MPool) {

	tblId := entry.GetTable().GetID()
	dbId := entry.GetTable().GetDB().GetID()
	accId := uint64(entry.GetTable().GetDB().GetTenantID())

	appendFunc := func(vecs []*vector.Vector, size uint64) {
		vector.AppendFixed[uint64](vecs[UsageAccID], accId, false, mp)
		vector.AppendFixed[uint64](vecs[UsageDBID], dbId, false, mp)
		vector.AppendFixed[uint64](vecs[UsageTblID], tblId, false, mp)
		vector.AppendFixed[uint64](vecs[UsageSize], size, false, mp)
	}

	if positiveSize != 0 {
		insVecs := getStorageUsageBatVectors_(data.bats[StorageUsageInsIDX])
		appendFunc(insVecs, positiveSize)
	}

	if negativeSize != 0 {
		delVecs := getStorageUsageBatVectors_(data.bats[StorageUsageDelIDX])
		appendFunc(delVecs, negativeSize)
	}
}

func traverseCatalog_(
	c *catalog.Catalog,
	collector *BaseCollector,
	fs fileservice.FileService,
	mp *mpool.MPool) {

	var lastEntry *catalog.SegmentEntry
	positiveSize := uint64(0)
	negativeSize := uint64(0)

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
			appendToStorageUsageBat_(positiveSize, negativeSize, collector.data, entry, mp)
			lastEntry = entry
			negativeSize = 0
			positiveSize = 0
		}

		entry.RLock()
		node := entry.GetLatestNodeLocked()
		entry.RUnlock()

		if node.HasDropCommitted() {
			// deleted segment
			negativeSize += uint64(node.BaseNode.ObjectStats.Size())
		} else if node.IsCommitted() {
			// flushed already
			positiveSize += uint64(node.BaseNode.ObjectStats.Size())
		}

		return nil
	}

	c.RecurLoop(processor)

	// deal with the last table's usage info
	appendToStorageUsageBat_(positiveSize, negativeSize, collector.data, lastEntry, mp)
}
