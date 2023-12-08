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
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/tidwall/btree"
	"math"
	"sync"
	"time"
	"unsafe"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

// 1. show accounts
//
//	internal `show accounts`       --------\
//										    |<=====> cn cache <====> (missed or expired) ===> tn cache
//  mysql client `show accounts`   --------/
//
//
// 2. collecting storage usage
//
// when doing incremental ckp
//
//	1. collect phase:
//		1. collect database deletes
//  	2. collect table deletes
//		3. collect segment deletes and inserts
//
//  2. apply phase
//		1. apply all deletes to tn cache and ckp batch[StorageUsageDel]
//		2. apply segment inserts to cache and batch[StorageUsageIns]
//
//
// when doing global checkpoint
//
//	1. replay all changes stored in tn cache into the global ckp batch
//
//
// when tn restart
//
//	1. replay all changes stored in ckp batches into the tn cache
//
//

const (
	UsageAccID uint8 = iota
	UsageDBID
	UsageTblID
	UsageObjID
	UsageSize

	UsageMAX
)

const StorageUsageMagic uint64 = 0x1A2B3C4D5E6F

type UsageData_ struct {
	AccId uint32
	DbId  uint64
	TblId uint64
	Size  int64
}

func (u UsageData_) String() string {
	return fmt.Sprintf("account id = %d; database id = %d; table id = %d; size = %d",
		u.AccId, u.DbId, u.TblId, u.Size)
}

func usageLess(a UsageData_, b UsageData_) bool {
	if a.AccId != b.AccId {
		return a.AccId < b.AccId
	}

	if a.DbId != b.DbId {
		return a.DbId < b.DbId
	}

	return a.TblId < b.TblId
}

type StorageUsageCache struct {
	// when two requests happens within [lastUpdate, lastUpdate + lazyThreshold],
	// it will reuse the cached result, no new query to TN.
	lazyThreshold time.Duration
	lastUpdate    time.Time
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
	cache.data.Clear()
	return cache
}

// MemUsed returns the memory used in megabytes
func (c *StorageUsageCache) MemUsed() float64 {
	itemCnt := uint64(c.data.Len())

	treeCost := uint64(unsafe.Sizeof(btree.BTreeG[UsageData_]{})) + itemCnt/2*12
	itemCost := itemCnt * uint64(unsafe.Sizeof(UsageData_{}))

	return math.Round(float64(treeCost+itemCost)/1048576.0*1e6) / 10e6
}

func (c *StorageUsageCache) Iter() btree.IterG[UsageData_] {
	return c.data.Iter()
}

func (c *StorageUsageCache) IsExpired() bool {
	if c.lastUpdate.IsZero() || c.lazyThreshold == 0 {
		return true
	}

	return time.Since(c.lastUpdate).Seconds() >= c.lazyThreshold.Seconds()
}

func (c *StorageUsageCache) String() string {
	return fmt.Sprintf("lazy threshold = %f s, last update = %s",
		c.lazyThreshold.Seconds(), c.lastUpdate.String())
}

func (c *StorageUsageCache) SetUpdateTime(t time.Time) {
	c.lastUpdate = t
}

func (c *StorageUsageCache) Update(usage UsageData_) {
	c.data.Set(usage)
}

func (c *StorageUsageCache) ClearForUpdate() {
	c.data.Clear()
}

func (c *StorageUsageCache) GatherAllAccSize() (usages map[uint32]int64) {
	usages = make(map[uint32]int64)
	c.data.Scan(func(item UsageData_) bool {
		usages[item.AccId] += item.Size
		return true
	})

	return
}

func (c *StorageUsageCache) GatherAccountSize(id uint32) (size int64, exist bool) {
	iter := c.data.Iter()
	defer iter.Release()

	piovt := UsageData_{AccId: id}

	if found := iter.Seek(piovt); !found {
		return
	}

	if iter.Item().AccId != id {
		return
	}

	size += iter.Item().Size
	for iter.Next() && iter.Item().AccId == id {
		size += iter.Item().Size
	}

	exist = true
	return
}

func (c *StorageUsageCache) Get(usage UsageData_) (ret UsageData_, exist bool) {
	return c.data.Get(usage)
}

func (c *StorageUsageCache) Delete(usage UsageData_) {
	c.data.Delete(usage)
}

type TNUsageMemo struct {
	sync.Mutex
	cache   *StorageUsageCache
	pending bool
}

func NewTNUsageMemo() *TNUsageMemo {
	memo := new(TNUsageMemo)
	memo.cache = NewStorageUsageCache()
	return memo
}

var tnUsageMemo = NewTNUsageMemo()

func GetTNUsageMemo() *TNUsageMemo {
	return tnUsageMemo
}

func (m *TNUsageMemo) MemoryUsed() float64 {
	return m.cache.MemUsed()
}

func (m *TNUsageMemo) EnterProcessing() {
	m.Lock()
}

func (m *TNUsageMemo) LeaveProcessing() {
	m.Unlock()
}

func (m *TNUsageMemo) HasUpdate() bool {
	return m.pending
}

func (m *TNUsageMemo) GatherAccountSize(id uint32) (size int64, exist bool) {
	return m.cache.GatherAccountSize(id)
}

func (m *TNUsageMemo) GatherAllAccSize() (usages map[uint32]int64) {
	return m.cache.GatherAllAccSize()
}

// Update does setting or updating
func (m *TNUsageMemo) Update(usage UsageData_, del bool) {
	m.pending = true
	size := int64(0)
	if old, found := m.cache.Get(usage); found {
		size = old.Size
	}

	if del {
		if usage.Size > size {
			panic("what the A !")
		}
		usage.Size = size - usage.Size
	} else {
		if size < 0 {
			panic("what the B !")
		}
		usage.Size = size + usage.Size
	}

	m.cache.Update(usage)
}

func (m *TNUsageMemo) Delete(usage UsageData_) {
	m.pending = true
	m.cache.data.Delete(usage)
}

func (m *TNUsageMemo) applyDeletes(
	deletes []interface{},
	ckpData *CheckpointData,
	mp *mpool.MPool) {

	var dbs []*catalog.DBEntry

	for _, del := range deletes {
		switch e := del.(type) {
		case *catalog.DBEntry:
			dbs = append(dbs, e)
		case *catalog.TableEntry:
			piovt := UsageData_{
				e.GetDB().GetTenantID(),
				e.GetDB().GetID(), e.GetID(), 0}
			if usage, exist := tnUsageMemo.cache.Get(piovt); exist {
				appendToStorageUsageBat_(ckpData, usage, true, mp)
				tnUsageMemo.Delete(usage)
			}
		}
	}

	iter := tnUsageMemo.cache.data.Iter()
	var tbls []uint64
	for _, db := range dbs {

		if found := iter.Seek(UsageData_{
			db.GetTenantID(), db.ID, 0, 0}); !found {
			continue
		}

		if iter.Item().DbId != db.ID {
			continue
		}

		tbls = append(tbls, iter.Item().TblId)
		for iter.Next() && iter.Item().DbId == db.ID {
			tbls = append(tbls, iter.Item().TblId)
			appendToStorageUsageBat_(ckpData, iter.Item(), true, mp)
		}

		for idx := 0; idx < len(tbls); idx++ {
			tnUsageMemo.cache.data.Delete(UsageData_{
				db.GetTenantID(), db.ID, tbls[idx], 0,
			})
		}

		tbls = tbls[:0]
	}

	iter.Release()
}

func (m *TNUsageMemo) applySegInserts(inserts []UsageData_, ckpData *CheckpointData, mp *mpool.MPool) {
	for _, usage := range inserts {
		appendToStorageUsageBat_(ckpData, usage, false, mp)
		tnUsageMemo.Update(usage, false)
	}
}

func (m *TNUsageMemo) applySegDeletes(deletes []UsageData_, ckpData *CheckpointData, mp *mpool.MPool) {
	for _, usage := range deletes {
		// can not delete a non-exist usage, right?
		if _, exist := tnUsageMemo.cache.Get(usage); exist {
			appendToStorageUsageBat_(ckpData, usage, true, mp)
			tnUsageMemo.Update(usage, true)
		}
	}
}

func (m *TNUsageMemo) replayIntoGCKP(collector *GlobalCollector) {
	iter := tnUsageMemo.cache.data.Iter()
	for iter.Next() {
		usage := iter.Item()
		appendToStorageUsageBat_(collector.data, usage, false, collector.Allocator())
	}
	iter.Release()
}

// EstablishFromCKPs replays usage info which stored in ckps into the tn cache
func (m *TNUsageMemo) EstablishFromCKPs(entries []*CheckpointData, vers []uint32) {
	m.EnterProcessing()
	defer m.LeaveProcessing()

	for x := range entries {
		if vers[x] < CheckpointVersion9 {
			// haven't StorageUsageIns batch
			continue
		}

		insVecs := getStorageUsageBatVectors_(entries[x].bats[StorageUsageInsIDX])
		accCol, dbCol, tblCol, sizeCol := getStorageUsageVectorCols(insVecs)

		for y := 0; y < insVecs[UsageAccID].Length(); y++ {
			m.Update(UsageData_{
				accCol[y], dbCol[y], tblCol[y], sizeCol[y],
			}, false)
		}

		if vers[x] < CheckpointVersion10 {
			// haven't StorageUsageDel batch
			continue
		}

		delVecs := getStorageUsageBatVectors_(entries[x].bats[StorageUsageDelIDX])
		accCol, dbCol, tblCol, sizeCol = getStorageUsageVectorCols(delVecs)

		for y := 0; y < delVecs[UsageAccID].Length(); y++ {
			m.Update(UsageData_{
				accCol[y], dbCol[y], tblCol[y], sizeCol[y],
			}, true)
		}
	}

}

// the returned order:
// [account_id_vec, db_id_vec, tbl_id_vec, size_vec]
func getStorageUsageBatVectors_(bat *containers.Batch) []*vector.Vector {
	accIDVec := bat.GetVectorByName(pkgcatalog.SystemColAttr_AccID).GetDownstreamVector()
	dbIDVec := bat.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector()
	tblIDVec := bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	sizeVec := bat.GetVectorByName(CheckpointMetaAttr_ObjectSize).GetDownstreamVector()

	return []*vector.Vector{
		accIDVec, dbIDVec, tblIDVec, nil, sizeVec,
	}
}

func getStorageUsageVectorCols(vecs []*vector.Vector) (
	accCol []uint32, dbCol []uint64, tblCol []uint64, sizeCol []int64) {

	dbCol = vector.MustFixedCol[uint64](vecs[UsageDBID])
	accCol = vector.MustFixedCol[uint32](vecs[UsageAccID])
	tblCol = vector.MustFixedCol[uint64](vecs[UsageTblID])
	sizeCol = vector.MustFixedCol[int64](vecs[UsageSize])

	return
}

const UsageBatMetaTableId uint64 = StorageUsageMagic

func appendToStorageUsageBat_(data *CheckpointData, usage UsageData_, del bool, mp *mpool.MPool) {
	start, end := int32(0), int32(0)

	defer func() {
		updateStorageUsageMeta(data, UsageBatMetaTableId, start, end, del)
	}()

	appendFunc := func(vecs []*vector.Vector) {
		vector.AppendFixed[int64](vecs[UsageSize], usage.Size, false, mp)
		vector.AppendFixed[uint32](vecs[UsageAccID], usage.AccId, false, mp)
		vector.AppendFixed[uint64](vecs[UsageDBID], usage.DbId, false, mp)
		vector.AppendFixed[uint64](vecs[UsageTblID], usage.TblId, false, mp)
	}

	if !del {
		insVecs := getStorageUsageBatVectors_(data.bats[StorageUsageInsIDX])
		start = int32(insVecs[0].Length())
		appendFunc(insVecs)
		end = int32(insVecs[0].Length())
		return
	}

	delVecs := getStorageUsageBatVectors_(data.bats[StorageUsageDelIDX])
	start = int32(delVecs[0].Length())
	appendFunc(delVecs)
	end = int32(delVecs[0].Length())
}

func segments2Usages(segs []*catalog.SegmentEntry) (usages []UsageData_, loaded int) {
	toUsage := func(seg *catalog.SegmentEntry) UsageData_ {
		return UsageData_{
			DbId:  seg.GetTable().GetDB().GetID(),
			Size:  int64(seg.Stat.GetCompSize()),
			TblId: seg.GetTable().GetID(),
			AccId: seg.GetTable().GetDB().GetTenantID(),
		}
	}

	// prefetch with batch, need to consider the capacity
	// of the prefetch cache
	batchCnt := 100
	i := 0
	for idx := 1; idx <= len(segs); idx++ {
		// prefetch obj meta
		blk := segs[idx-1].GetFirstBlkEntry()
		if blk != nil && len(blk.GetMetaLoc()) != 0 {
			loaded++
			blockio.PrefetchMeta(blk.GetBlockData().GetFs().Service, blk.GetMetaLoc())
		}

		// deal with the previously prefetched batch
		for idx%batchCnt == 0 && i < idx {
			segs[i].LoadObjectInfo()
			usages = append(usages, toUsage(segs[i]))
			i++
		}
	}

	// deal with the left segments
	for ; i < len(segs); i++ {
		segs[i].LoadObjectInfo()
		usages = append(usages, toUsage(segs[i]))
	}

	return
}

// prepare for storing the storage usage bat location into ckp table meta
func updateStorageUsageMeta(data *CheckpointData, tid uint64, start, end int32, del bool) {
	if del {
		data.updateTableMeta(tid, StorageUsageDel, start, end)
	} else {
		data.updateTableMeta(tid, StorageUsageIns, start, end)
	}
}

func applyChanges(collector *BaseCollector) (loaded int) {
	// must apply seg insert first
	// step 1: apply seg insert (non-appendable, committed)
	usage, l := segments2Usages(collector.Usage.SegInserts)
	tnUsageMemo.applySegInserts(usage, collector.data, collector.Allocator())
	loaded += l

	// step 2: apply db, tbl deletes
	tnUsageMemo.applyDeletes(collector.Usage.Deletes, collector.data, collector.Allocator())

	// step 3: apply seg deletes
	usage, l = segments2Usages(collector.Usage.SegDeletes)
	tnUsageMemo.applySegDeletes(usage, collector.data, collector.Allocator())
	loaded += l

	return loaded
}

func FillUsageBatOfGlobal_(collector *GlobalCollector) {
	start := time.Now()
	//var loaded int

	tnUsageMemo.EnterProcessing()
	defer func() {
		tnUsageMemo.LeaveProcessing()
		v2.TaskGCkpCollectUsageDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	//loaded = applyChanges(collector.BaseCollector)
	//logutil.Info(fmt.Sprintf("[storage usage] CKP[G]: load object: %d, current usage cache size: %f",
	//	loaded, tnUsageMemo.MemoryUsed()))
	tnUsageMemo.replayIntoGCKP(collector)
}

func FillUsageBatOfIncremental_(collector *IncrementalCollector) {
	start := time.Now()
	var loaded int

	tnUsageMemo.EnterProcessing()
	defer func() {
		tnUsageMemo.LeaveProcessing()
		v2.TaskICkpLoadObjectCounter.Add(float64(loaded))
		v2.TaskICkpCollectUsageDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	loaded = applyChanges(collector.BaseCollector)
	logutil.Info(fmt.Sprintf("[storage usage] CKP[I]: load object: %d, current usage cache size: %f",
		loaded, tnUsageMemo.MemoryUsed()))
}

// GetStorageUsageHistory is for debug to show these storage usage changes.
//
// 1. load each ckp meta batch.
//
// 2. load the specified storage usage ins/del batch using locations storing in meta batch.
func GetStorageUsageHistory(
	ctx context.Context,
	locations []objectio.Location, versions []uint32,
	fs fileservice.FileService, mp *mpool.MPool) ([][]UsageData_, [][]UsageData_, error) {

	var err error

	// 1. load each ckp meta batch
	datas, err := loadMetaBat(ctx, versions, locations, fs, mp)
	if err != nil {
		return nil, nil, err
	}

	usageInsData := make([][]UsageData_, len(locations))
	usageDelData := make([][]UsageData_, len(locations))

	var usageInsBat []*batch.Batch
	var usageDelBat []*batch.Batch

	for idx := 0; idx < len(datas); idx++ {
		datas[idx].GetTableMeta(UsageBatMetaTableId, versions[idx], locations[idx])
		usageMeta := datas[idx].meta[UsageBatMetaTableId]

		// 2.1. load storage usage ins bat
		if usageInsBat, err = loadBatch(
			ctx, usageMeta.tables[StorageUsageIns].locations,
			versions[idx], StorageUsageInsIDX, fs, mp); err != nil {
			return nil, nil, err
		}

		// 2.2. load storage usage del bat
		if usageDelBat, err = loadBatch(
			ctx, usageMeta.tables[StorageUsageDel].locations,
			versions[idx], StorageUsageDelIDX, fs, mp); err != nil {
			return nil, nil, err
		}

		// 3. collect usage data from these batches
		for _, bat := range usageInsBat {
			if ret := cnBatchToUsageDatas(bat); len(ret) != 0 {
				usageInsData[idx] = append(usageInsData[idx], ret...)
			}
		}

		for _, bat := range usageDelBat {
			if ret := cnBatchToUsageDatas(bat); len(ret) != 0 {
				usageDelData[idx] = append(usageDelData[idx], ret...)
			}
		}
	}

	return usageInsData, usageDelData, nil
}

func cnBatchToUsageDatas(bat *batch.Batch) []UsageData_ {
	accCol := vector.MustFixedCol[uint32](bat.GetVector(2))
	dbCol := vector.MustFixedCol[uint64](bat.GetVector(3))
	tblCol := vector.MustFixedCol[uint64](bat.GetVector(4))
	sizeCol := vector.MustFixedCol[int64](bat.GetVector(6))

	var usages []UsageData_

	for idx := range accCol {
		usages = append(usages, UsageData_{
			accCol[idx],
			dbCol[idx],
			tblCol[idx],
			sizeCol[idx],
		})
	}
	return usages
}

func loadMetaBat(
	ctx context.Context,
	versions []uint32, locations []objectio.Location,
	fs fileservice.FileService, mp *mpool.MPool) ([]*CNCheckpointData, error) {

	datas := make([]*CNCheckpointData, len(locations))
	var idxes []uint16

	for idx := 0; idx < len(locations); idx++ {
		if versions[idx] < CheckpointVersion10 {
			continue
		}

		data := NewCNCheckpointData()

		// 1.1. prefetch meta bat
		meteIdxSchema := checkpointDataReferVersions[versions[idx]][MetaIDX]
		for attrIdx := range meteIdxSchema.attrs {
			idxes = append(idxes, uint16(attrIdx))
		}

		data.PrefetchMetaIdx(ctx, versions[idx], idxes, locations[idx], fs)

		// 1.2. read meta bat
		reader, err := blockio.NewObjectReader(fs, locations[idx])
		if err != nil {
			return nil, err
		}

		data.InitMetaIdx(ctx, versions[idx], reader, locations[idx], mp)

		idxes = idxes[:0]
		datas[idx] = data
	}

	return datas, nil
}

func loadBatch(
	ctx context.Context, locations BlockLocations, version uint32,
	batIdx uint16, fs fileservice.FileService, mp *mpool.MPool) ([]*batch.Batch, error) {

	var bats []*batch.Batch

	it := locations.MakeIterator()
	for it.HasNext() {
		block := it.Next()
		schema := checkpointDataReferVersions[version][uint32(batIdx)]
		reader, err := blockio.NewObjectReader(fs, block.GetLocation())
		if err != nil {
			return nil, err
		}

		if bat, err := LoadCNSubBlkColumnsByMetaWithId(
			ctx, schema.types, schema.attrs, batIdx,
			block.GetID(), version, reader, mp); err != nil {
			return nil, err
		} else {
			bats = append(bats, bat)
		}
	}
	return bats, nil
}
