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
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/tidwall/btree"
	"go.uber.org/zap"
)

// 1. show accounts
//
//	internal `show accounts`       -------\
//									       |<=====> cn cache <====> (missed or expired) ===> tn cache
//  mysql client `show accounts`   -------/													 ^
//																			 ________________|
//         																	|	update       |
//																			|				 |
//                                                                  incremental ckp	         |
//																							 |
//  								            when tn restart --> replay from ckps --------|
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

type UsageData struct {
	AccId uint64
	DbId  uint64
	TblId uint64
	Size  uint64
}

var zeroUsageData UsageData = UsageData{math.MaxUint32, math.MaxUint64, math.MaxUint64, math.MaxInt64}

// MockUsageData generates accCnt * dbCnt * tblCnt UsageDatas.
// the accIds, dbIds and tblIds are random produced.
// this func ensure that all ids are different.
func MockUsageData(accCnt, dbCnt, tblCnt int, allocator *atomic.Uint64) (result []UsageData) {
	for x := 0; x < accCnt; x++ {
		accId := allocator.Add(1)

		for y := 0; y < dbCnt; y++ {
			dbId := allocator.Add(1)

			for z := 0; z < tblCnt; z++ {
				result = append(result, UsageData{
					AccId: accId,
					DbId:  dbId,
					TblId: allocator.Add(1),
					Size:  uint64(rand.Int63() % 0x3fff),
				})
			}
		}
	}

	return
}

func (u UsageData) String() string {
	return fmt.Sprintf("account id = %d; database id = %d; table id = %d; size = %d",
		u.AccId, u.DbId, u.TblId, u.Size)
}

func (u UsageData) IsZero() bool {
	return u == zeroUsageData
}

func usageLess(a UsageData, b UsageData) bool {
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
	sync.Mutex
	lazyThreshold time.Duration
	lastUpdate    time.Time
	// accId -> dbId -> [tblId, size]
	data     *btree.BTreeG[UsageData]
	lessFunc func(a UsageData, b UsageData) bool
}

type StorageUsageCacheOption = func(c *StorageUsageCache)

// WithLazyThreshold sets lazyThreshold to lazy seconds
func WithLazyThreshold(lazy int) StorageUsageCacheOption {
	return StorageUsageCacheOption(func(c *StorageUsageCache) {
		c.lazyThreshold = time.Second * time.Duration(lazy)
	})
}

func WithLessFunc(less func(a UsageData, b UsageData) bool) StorageUsageCacheOption {
	return StorageUsageCacheOption(func(c *StorageUsageCache) {
		c.lessFunc = less
	})
}

func NewStorageUsageCache(opts ...StorageUsageCacheOption) *StorageUsageCache {
	cache := new(StorageUsageCache)

	cache.fillDefault()

	for _, opt := range opts {
		opt(cache)
	}

	cache.data = btree.NewBTreeG[UsageData](cache.lessFunc)
	cache.data.Clear()

	return cache
}

func (c *StorageUsageCache) fillDefault() {
	c.lessFunc = usageLess
	c.lazyThreshold = 0
}

func (c *StorageUsageCache) LessFunc() func(a UsageData, b UsageData) bool {
	return c.lessFunc
}

func (c *StorageUsageCache) CacheLen() int {
	return c.data.Len()
}

// MemUsed returns the memory used in megabytes
func (c *StorageUsageCache) MemUsed() float64 {
	itemCnt := uint64(c.data.Len())

	treeCost := uint64(unsafe.Sizeof(btree.BTreeG[UsageData]{})) + itemCnt/2*12
	itemCost := itemCnt * uint64(unsafe.Sizeof(UsageData{}))

	return math.Round(float64(treeCost+itemCost)/1048576.0*10e6) / 10e6
}

func (c *StorageUsageCache) Iter() btree.IterG[UsageData] {
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

func (c *StorageUsageCache) setUpdateTime(t time.Time) {
	c.lastUpdate = t
}

func (c *StorageUsageCache) SetOrReplace(usage UsageData) {
	c.data.Set(usage)
	c.setUpdateTime(time.Now())
}

func (c *StorageUsageCache) ClearForUpdate() {
	c.data.Clear()
}

func (c *StorageUsageCache) GatherAllAccSize() (usages map[uint64]uint64) {
	usages = make(map[uint64]uint64)
	c.data.Scan(func(item UsageData) bool {
		usages[item.AccId] += item.Size
		return true
	})

	return
}

func (c *StorageUsageCache) GatherAccountSize(id uint64) (size uint64, exist bool) {
	iter := c.data.Iter()
	defer iter.Release()

	piovt := UsageData{AccId: id}

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

func (c *StorageUsageCache) Get(usage UsageData) (ret UsageData, exist bool) {
	return c.data.Get(usage)
}

func (c *StorageUsageCache) Delete(usage UsageData) {
	c.data.Delete(usage)
	c.setUpdateTime(time.Now())
}

type TNUsageMemo struct {
	sync.Mutex
	cache *StorageUsageCache
	// has update
	pending  bool
	reqTrace []struct {
		hint      string
		timeStamp time.Time
		accountId uint64
		totalSize uint64
	}

	newAccCache *StorageUsageCache

	pendingReplay struct {
		datas   []*CheckpointData
		vers    []uint32
		delayed map[uint64]UsageData
	}
}

func NewTNUsageMemo() *TNUsageMemo {
	memo := new(TNUsageMemo)
	memo.cache = NewStorageUsageCache()
	memo.newAccCache = NewStorageUsageCache()
	return memo
}

func (m *TNUsageMemo) PrepareReplay(datas []*CheckpointData, vers []uint32) {
	m.pendingReplay.datas = datas
	m.pendingReplay.vers = vers
}

func (m *TNUsageMemo) GetDelayed() map[uint64]UsageData {
	return m.pendingReplay.delayed
}

func (m *TNUsageMemo) GetNewAccCacheLatestUpdate() types.TS {
	return types.BuildTS(m.newAccCache.lastUpdate.UnixNano(), 0)
}

func (m *TNUsageMemo) UpdateNewAccCache(usage UsageData, del bool) {
	m.updateHelper(m.newAccCache, usage, del)
}

func (m *TNUsageMemo) ClearNewAccCache() {
	m.newAccCache.ClearForUpdate()
}

func (m *TNUsageMemo) AddReqTrace(accountId uint64, tSize uint64, t time.Time, hint string) {
	m.reqTrace = append(m.reqTrace,
		struct {
			hint      string
			timeStamp time.Time
			accountId uint64
			totalSize uint64
		}{
			hint:      hint,
			timeStamp: t,
			accountId: accountId,
			totalSize: tSize,
		})
}

func (m *TNUsageMemo) GetAllReqTrace() (accountIds []uint64, timestamps []time.Time, sizes []uint64, hints []string) {
	m.EnterProcessing()
	defer m.LeaveProcessing()

	for idx := range m.reqTrace {
		timestamps = append(timestamps, m.reqTrace[idx].timeStamp)
		accountIds = append(accountIds, m.reqTrace[idx].accountId)
		sizes = append(sizes, m.reqTrace[idx].totalSize)
		hints = append(hints, m.reqTrace[idx].hint)
	}
	return
}

func (m *TNUsageMemo) Clear() {
	m.EnterProcessing()
	defer m.LeaveProcessing()

	m.reqTrace = m.reqTrace[:0]
	m.cache.ClearForUpdate()
	m.pending = false
	m.cache.setUpdateTime(time.Time{})
	m.ClearNewAccCache()
}

func (m *TNUsageMemo) GetCache() *StorageUsageCache {
	return m.cache
}

func (m *TNUsageMemo) Get(usage UsageData) (old UsageData, exist bool) {
	return m.cache.Get(usage)
}

func (m *TNUsageMemo) CacheLen() int {
	return m.cache.CacheLen()
}

func (m *TNUsageMemo) MemoryUsed() float64 {
	cacheUsed := m.cache.MemUsed() + m.newAccCache.MemUsed()
	memoUsed := int(unsafe.Sizeof(TNUsageMemo{})) + len(m.reqTrace)*(12+int(unsafe.Sizeof(time.Time{})))
	extraUsed := int(unsafe.Sizeof(UsageData{})) * (len(summaryLog[0]) + len(summaryLog[1]))

	memoUsed += extraUsed
	return cacheUsed + (float64(memoUsed)/1048576.0*1e6)/1e6
}

func (m *TNUsageMemo) EnterProcessing() {
	m.cache.Lock()
	m.Lock()
}

func (m *TNUsageMemo) LeaveProcessing() {
	m.cache.Unlock()
	m.Unlock()
}

func (m *TNUsageMemo) HasUpdate() bool {
	return m.pending
}

func (m *TNUsageMemo) gatherAccountSizeHelper(cache *StorageUsageCache, id uint64) (size uint64, exist bool) {
	return cache.GatherAccountSize(id)
}

func (m *TNUsageMemo) GatherAccountSize(id uint64) (size uint64, exist bool) {
	return m.gatherAccountSizeHelper(m.cache, id)
}

func (m *TNUsageMemo) GatherNewAccountSize(id uint64) (size uint64, exist bool) {
	return m.gatherAccountSizeHelper(m.newAccCache, id)
}

func (m *TNUsageMemo) GatherAllAccSize() (usages map[uint64]uint64) {
	return m.cache.GatherAllAccSize()
}

func (m *TNUsageMemo) updateHelper(cache *StorageUsageCache, usage UsageData, del bool) {
	size := uint64(0)
	if old, found := cache.Get(usage); found {
		size = old.Size
	}

	if del {
		if usage.Size > size {
			//panic("what the A !")
			usage.Size = size
		}
		usage.Size = size - usage.Size
	} else {
		//if size < 0 {
		//	panic("what the B !")
		//}
		usage.Size = size + usage.Size
	}

	cache.SetOrReplace(usage)
}

// DeltaUpdate does setting or updating with delta size (delta.Size)
func (m *TNUsageMemo) DeltaUpdate(delta UsageData, del bool) {
	m.pending = true
	m.updateHelper(m.cache, delta, del)
}

// Replace replaces the old usage with newUsage
func (m *TNUsageMemo) Replace(new UsageData) {
	m.pending = true
	m.cache.SetOrReplace(new)
}

func (m *TNUsageMemo) Delete(usage UsageData) {
	m.pending = true
	m.cache.data.Delete(usage)
}

func (m *TNUsageMemo) applyDeletes(
	deletes []interface{},
	ckpData *CheckpointData,
	mp *mpool.MPool) string {

	var buf bytes.Buffer
	var dbs []*catalog.DBEntry

	for _, del := range deletes {
		switch e := del.(type) {
		case *catalog.DBEntry:
			dbs = append(dbs, e)
		case *catalog.TableEntry:
			piovt := UsageData{
				uint64(e.GetDB().GetTenantID()),
				e.GetDB().GetID(), e.GetID(), 0}
			if usage, exist := m.cache.Get(piovt); exist {
				appendToStorageUsageBat(ckpData, usage, true, mp)
				m.Delete(usage)
				buf.WriteString(fmt.Sprintf("[d-tbl]%s_%d_%d_%d_%d; ",
					e.GetFullName(), usage.AccId, usage.DbId, usage.TblId, usage.Size))
			}
		}
	}

	isSameDBFunc := func(a UsageData, db *catalog.DBEntry) bool {
		return a.AccId == uint64(db.GetTenantID()) && a.DbId == db.ID
	}

	usages := make([]UsageData, 0)
	for _, db := range dbs {
		iter := m.cache.Iter()
		iter.Seek(UsageData{uint64(db.GetTenantID()), db.ID, 0, 0})

		if !isSameDBFunc(iter.Item(), db) {
			iter.Release()
			// db not found in cache
			buf.WriteString(fmt.Sprintf("[n-db]%s_%d_%d_%d; ",
				db.GetFullName(), db.GetTenantID(), db.GetID(), 0))
			continue
		}

		usages = append(usages, iter.Item())
		for iter.Next() && isSameDBFunc(iter.Item(), db) {
			usages = append(usages, iter.Item())
		}

		iter.Release()

		totalSize := uint64(0)
		for idx := 0; idx < len(usages); idx++ {
			m.cache.Delete(usages[idx])
			appendToStorageUsageBat(ckpData, usages[idx], true, mp)
			totalSize += usages[idx].Size
		}

		buf.WriteString(fmt.Sprintf("[d-db]%s_%d_%d_%d; ",
			db.GetFullName(), db.GetTenantID(), db.GetID(), totalSize))

		usages = usages[:0]
	}

	return buf.String()
}

func (m *TNUsageMemo) applySegInserts(inserts []UsageData, ckpData *CheckpointData, mp *mpool.MPool) {
	for _, usage := range inserts {
		appendToStorageUsageBat(ckpData, usage, false, mp)
		m.DeltaUpdate(usage, false)
	}
}

func (m *TNUsageMemo) applySegDeletes(deletes []UsageData, ckpData *CheckpointData, mp *mpool.MPool) {
	for _, usage := range deletes {
		// can not delete a non-exist usage, right?
		if _, exist := m.cache.Get(usage); exist {
			appendToStorageUsageBat(ckpData, usage, true, mp)
			m.DeltaUpdate(usage, true)
		}
	}
}

func (m *TNUsageMemo) replayIntoGCKP(collector *GlobalCollector) {
	iter := m.cache.data.Iter()
	for iter.Next() {
		usage := iter.Item()
		appendToStorageUsageBat(collector.data, usage, false, collector.Allocator())
	}
	iter.Release()
}

func try2RemoveStaleData(usage UsageData, c *catalog.Catalog) (UsageData, string, bool) {
	if c == nil {
		return usage, "", false
	}

	var err error
	var dbEntry *catalog.DBEntry
	var tblEntry *catalog.TableEntry

	dbEntry, err = c.GetDatabaseByID(usage.DbId)
	if err != nil || dbEntry.HasDropCommitted() {
		// the db has been deleted
		name := "deleted"
		if dbEntry != nil {
			name = dbEntry.GetName()
		}
		log := fmt.Sprintf("[d-db]%s_%d_%d_%d; ", name, usage.AccId, usage.DbId, usage.Size)
		return usage, log, true
	}

	tblEntry, err = dbEntry.GetTableEntryByID(usage.TblId)
	if err != nil || tblEntry.HasDropCommitted() {
		// the tbl has been deleted
		name := "deleted"
		if tblEntry != nil {
			name = tblEntry.GetFullName()
		}
		log := fmt.Sprintf("[d-tbl]%s_%d_%d_%d_%d; ", name, usage.AccId, usage.DbId, usage.TblId, usage.Size)
		return usage, log, true
	}

	return usage, "", false
}

func (m *TNUsageMemo) deleteAccount(accId uint64) (size uint64) {
	trash := make([]UsageData, 0)
	povit := UsageData{accId, 0, 0, 0}

	iter := m.cache.Iter()

	iter.Seek(povit)

	if iter.Item().AccId != accId {
		iter.Release()
		return
	}

	trash = append(trash, iter.Item())
	for iter.Next() {
		if iter.Item().AccId != accId {
			break
		}

		trash = append(trash, iter.Item())
	}

	iter.Release()

	for idx := range trash {
		size += trash[idx].Size
		m.Delete(trash[idx])
	}
	return
}

func (m *TNUsageMemo) ClearDroppedAccounts(reserved map[uint64]struct{}) string {
	if reserved == nil {
		return ""
	}

	var buf bytes.Buffer

	usages := m.GatherAllAccSize()
	for accId := range usages {
		if _, ok := reserved[accId]; !ok {
			// this account has been deleted
			size := m.deleteAccount(accId)
			buf.WriteString(fmt.Sprintf("%d_%d; ", accId, size))
		}
	}
	return buf.String()
}

// EstablishFromCKPs replays usage info which stored in ckps into the tn cache
func (m *TNUsageMemo) EstablishFromCKPs(c *catalog.Catalog) {
	m.EnterProcessing()
	defer m.LeaveProcessing()

	var buf bytes.Buffer

	defer func() {
		for idx := range m.pendingReplay.datas {
			if m.pendingReplay.datas[idx] != nil {
				m.pendingReplay.datas[idx].Close()
			}
			m.pendingReplay.datas[idx] = nil
		}
		logutil.Info("[storage usage] replay:",
			zap.String("remove old deleted db/tbl", buf.String()),
			zap.Int("delayed tbl", len(m.pendingReplay.delayed)))
	}()

	for x := range m.pendingReplay.datas {
		if m.pendingReplay.vers[x] < CheckpointVersion9 {
			// haven't StorageUsageIns batch
			// haven't StorageUsageDel batch
			continue
		}

		insVecs := getStorageUsageBatVectors(m.pendingReplay.datas[x].bats[StorageUsageInsIDX])
		accCol, dbCol, tblCol, sizeCol := getStorageUsageVectorCols(insVecs)

		var skip bool
		var log string
		for y := 0; y < len(accCol); y++ {
			usage := UsageData{accCol[y], dbCol[y], tblCol[y], sizeCol[y]}

			// these ckps, older than version 11, haven't del bat, we need clear the
			// usage data which belongs the deleted databases or tables.
			//
			// (if a table or db recreate, it's id will change)
			//
			if m.pendingReplay.vers[x] < CheckpointVersion11 {
				// here only remove the deleted db and table.
				// if table has deletes, we update it in gckp
				usage, log, skip = try2RemoveStaleData(usage, c)
				if skip {
					buf.WriteString(log)
					continue
				}
				if m.pendingReplay.delayed == nil {
					m.pendingReplay.delayed = make(map[uint64]UsageData)
				}
				if old, ok := m.pendingReplay.delayed[usage.TblId]; !ok {
					m.pendingReplay.delayed[usage.TblId] = usage
				} else {
					old.Size += usage.Size
					m.pendingReplay.delayed[usage.TblId] = old
				}
			}
			m.DeltaUpdate(usage, false)
		}

		if m.pendingReplay.vers[x] < CheckpointVersion11 {
			// haven't StorageUsageDel batch
			continue
		}

		delVecs := getStorageUsageBatVectors(m.pendingReplay.datas[x].bats[StorageUsageDelIDX])
		accCol, dbCol, tblCol, sizeCol = getStorageUsageVectorCols(delVecs)

		for y := 0; y < len(accCol); y++ {
			usage := UsageData{accCol[y], dbCol[y], tblCol[y], sizeCol[y]}
			m.DeltaUpdate(usage, true)
		}
	}

}

// the returned order:
// [account_id_vec, db_id_vec, tbl_id_vec, size_vec]
func getStorageUsageBatVectors(bat *containers.Batch) []*vector.Vector {
	accIDVec := bat.GetVectorByName(pkgcatalog.SystemColAttr_AccID).GetDownstreamVector()
	dbIDVec := bat.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector()
	tblIDVec := bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	sizeVec := bat.GetVectorByName(CheckpointMetaAttr_ObjectSize).GetDownstreamVector()

	return []*vector.Vector{
		accIDVec, dbIDVec, tblIDVec, nil, sizeVec,
	}
}

func getStorageUsageVectorCols(vecs []*vector.Vector) (
	accCol []uint64, dbCol []uint64, tblCol []uint64, sizeCol []uint64) {

	dbCol = vector.MustFixedCol[uint64](vecs[UsageDBID])
	accCol = vector.MustFixedCol[uint64](vecs[UsageAccID])
	tblCol = vector.MustFixedCol[uint64](vecs[UsageTblID])
	sizeCol = vector.MustFixedCol[uint64](vecs[UsageSize])

	return
}

const UsageBatMetaTableId uint64 = StorageUsageMagic

var lastInsUsage UsageData = zeroUsageData
var lastDelUsage UsageData = zeroUsageData

// 0: insert, 1: delete
var summaryLog [2][]UsageData

// this function will accumulate all size of one table into one row.
// [acc1, db1, table1, size1]  \
// [acc1, db1, table1, size2]    ===> [acc1, db1, table1, size1 + size2 + size3]
// [acc1, db1, table1, size3]  /
func appendToStorageUsageBat(data *CheckpointData, usage UsageData, del bool, mp *mpool.MPool) {
	appendFunc := func(vecs []*vector.Vector) {
		vector.AppendFixed[uint64](vecs[UsageSize], usage.Size, false, mp)
		vector.AppendFixed[uint64](vecs[UsageAccID], usage.AccId, false, mp)
		vector.AppendFixed[uint64](vecs[UsageDBID], usage.DbId, false, mp)
		vector.AppendFixed[uint64](vecs[UsageTblID], usage.TblId, false, mp)

		// new table
		if del {
			summaryLog[1] = append(summaryLog[1], usage)
		} else {
			summaryLog[0] = append(summaryLog[0], usage)
		}
	}

	updateFunc := func(vecs []*vector.Vector, size uint64) {
		vector.SetFixedAt[uint64](vecs[UsageSize], vecs[UsageSize].Length()-1, size)

		if del {
			summaryLog[1][len(summaryLog[1])-1].Size = size
		} else {
			summaryLog[0][len(summaryLog[0])-1].Size = size
		}
	}

	tableChanged := func(last UsageData) bool {
		changed := !(last.AccId == usage.AccId && last.DbId == usage.DbId && last.TblId == usage.TblId)
		return changed
	}

	entranceFunc := func(last *UsageData, batIdx uint16) {
		vecs := getStorageUsageBatVectors(data.bats[batIdx])

		start := vecs[UsageSize].Length()

		// append new row to batch only when table changed
		if last.IsZero() || tableChanged(*last) {
			*last = usage
			appendFunc(vecs)
		} else {
			// table not change, only updates the size val
			last.Size += usage.Size
			updateFunc(vecs, last.Size)
		}

		end := vecs[UsageSize].Length()
		updateStorageUsageMeta(data, UsageBatMetaTableId, int32(start), int32(end), del)
	}

	if del {
		entranceFunc(&lastDelUsage, StorageUsageDelIDX)
	} else {
		entranceFunc(&lastInsUsage, StorageUsageInsIDX)
	}
}

func objects2Usages(objs []*catalog.ObjectEntry) (usages []UsageData) {
	toUsage := func(obj *catalog.ObjectEntry) UsageData {
		return UsageData{
			DbId:  obj.GetTable().GetDB().GetID(),
			Size:  uint64(obj.GetCompSize()),
			TblId: obj.GetTable().GetID(),
			AccId: uint64(obj.GetTable().GetDB().GetTenantID()),
		}
	}

	for idx := range objs {
		usages = append(usages, toUsage(objs[idx]))
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

// putCacheBack2Track correct the margin of error happened in incremental checkpoint
func putCacheBack2Track(collector *BaseCollector) (string, int) {
	memo := collector.UsageMemo

	var buf bytes.Buffer

	tblChanges := make(map[[3]uint64]int64)

	usages := objects2Usages(collector.Usage.ObjDeletes)
	for idx := range usages {
		uniqueTbl := [3]uint64{usages[idx].AccId, usages[idx].DbId, usages[idx].TblId}
		tblChanges[uniqueTbl] -= int64(usages[idx].Size)
	}

	usages = objects2Usages(collector.Usage.ObjInserts)
	for idx := range usages {
		uniqueTbl := [3]uint64{usages[idx].AccId, usages[idx].DbId, usages[idx].TblId}
		tblChanges[uniqueTbl] += int64(usages[idx].Size)
	}

	delDbs := make(map[uint64]struct{})
	delTbls := make(map[uint64]struct{})
	for _, del := range collector.Usage.Deletes {
		switch e := del.(type) {
		case *catalog.DBEntry:
			delDbs[e.ID] = struct{}{}
		case *catalog.TableEntry:
			delTbls[e.ID] = struct{}{}
		}
	}

	if len(tblChanges) == 0 {
		return "", 0
	}

	memo.GetCache().ClearForUpdate()

	for uniqueTbl, size := range tblChanges {
		if size <= 0 {
			size = 0
		}

		if _, ok := delDbs[uniqueTbl[1]]; ok {
			continue
		}

		if _, ok := delTbls[uniqueTbl[2]]; ok {
			continue
		}

		memo.Replace(UsageData{
			Size:  uint64(size),
			TblId: uniqueTbl[2],
			DbId:  uniqueTbl[1],
			AccId: uniqueTbl[0],
		})

		if len(memo.pendingReplay.delayed) == 0 {
			continue
		}

		if usage, ok := memo.pendingReplay.delayed[uniqueTbl[2]]; ok {
			buf.WriteString(fmt.Sprintf("[u-tbl]%d_%d_%d_(o)%d_(n)%d; ",
				usage.AccId, usage.DbId, usage.TblId, usage.Size, size))

			delete(memo.pendingReplay.delayed, uniqueTbl[2])
		}
	}

	return buf.String(), memo.CacheLen()
}

func applyChanges(collector *BaseCollector, tnUsageMemo *TNUsageMemo) string {
	tnUsageMemo.newAccCache.ClearForUpdate()

	// must apply seg insert first
	// step 1: apply seg insert (non-appendable, committed)
	usage := objects2Usages(collector.Usage.ObjInserts)
	tnUsageMemo.applySegInserts(usage, collector.data, collector.Allocator())

	// step 2: apply db, tbl deletes
	log := tnUsageMemo.applyDeletes(collector.Usage.Deletes, collector.data, collector.Allocator())

	// step 3: apply seg deletes
	usage = objects2Usages(collector.Usage.ObjDeletes)
	tnUsageMemo.applySegDeletes(usage, collector.data, collector.Allocator())

	return log
}

func doSummary(ckp string, fields ...zap.Field) {
	defer func() {
		summaryLog[0] = summaryLog[0][:0]
		summaryLog[1] = summaryLog[1][:0]

		lastInsUsage = zeroUsageData
		lastDelUsage = zeroUsageData
	}()

	sort.Slice(summaryLog[0], func(i, j int) bool { return usageLess(summaryLog[0][i], summaryLog[0][j]) })
	sort.Slice(summaryLog[1], func(i, j int) bool { return usageLess(summaryLog[1][i], summaryLog[1][j]) })

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("\nCKP[%s]\t%s\n", ckp, time.Now().UTC().String()))

	format := "\t%19d\t%19d\t%19d\t%19.6fmb"
	accumulated := int64(0)

	for idx := range summaryLog[0] {
		buf.WriteString(fmt.Sprintf(format+" -> i\n",
			summaryLog[0][idx].AccId,
			summaryLog[0][idx].DbId,
			summaryLog[0][idx].TblId,
			float64(summaryLog[0][idx].Size)/(1024*1024)))

		accumulated += int64(summaryLog[0][idx].Size)
	}

	for idx := range summaryLog[1] {
		buf.WriteString(fmt.Sprintf(format+" -> d\n",
			summaryLog[1][idx].AccId,
			summaryLog[1][idx].DbId,
			summaryLog[1][idx].TblId,
			float64(summaryLog[1][idx].Size)/(1024*1024)))

		accumulated -= int64(summaryLog[1][idx].Size)
	}

	buf.WriteString(fmt.Sprintf("accumulated size in this ckp: %19.6fmb, ",
		float64(accumulated)/(1024*1024)))

	fields = append(fields, zap.String("storage usage summary when ckp", buf.String()))
	logutil.Info(fmt.Sprintf("storage usage [%s]", ckp), fields...)
}

func FillUsageBatOfGlobal(collector *GlobalCollector) {
	start := time.Now()

	collector.UsageMemo.EnterProcessing()
	defer func() {
		v2.TaskGCkpCollectUsageDurationHistogram.Observe(time.Since(start).Seconds())
		collector.UsageMemo.LeaveProcessing()
	}()

	log1, cnt := putCacheBack2Track(collector.BaseCollector)
	log2 := collector.UsageMemo.ClearDroppedAccounts(collector.Usage.ReservedAccIds)
	collector.UsageMemo.replayIntoGCKP(collector)

	doSummary("G",
		zap.String("update old data", log1),
		zap.Int("tables back to track", cnt),
		zap.String("accounts cleaned", log2))
}

func FillUsageBatOfIncremental(collector *IncrementalCollector) {
	start := time.Now()
	var memoryUsed float64

	collector.UsageMemo.EnterProcessing()
	defer func() {
		v2.TaskStorageUsageCacheMemUsedGauge.Set(memoryUsed)
		v2.TaskICkpCollectUsageDurationHistogram.Observe(time.Since(start).Seconds())
		collector.UsageMemo.LeaveProcessing()
	}()

	log1 := applyChanges(collector.BaseCollector, collector.UsageMemo)
	//log2 := applyTransfer(collector.BaseCollector, collector.UsageMemo)

	memoryUsed = collector.UsageMemo.MemoryUsed()
	doSummary("I",
		zap.Float64("cache mem used", memoryUsed),
		zap.String("applied deletes", log1))
}

// GetStorageUsageHistory is for debug to show these storage usage changes.
//
// 1. load each ckp meta batch.
//
// 2. load the specified storage usage ins/del batch using locations storing in meta batch.
func GetStorageUsageHistory(
	ctx context.Context,
	locations []objectio.Location, versions []uint32,
	fs fileservice.FileService, mp *mpool.MPool) ([][]UsageData, [][]UsageData, error) {

	var err error

	// 1. load each ckp meta batch
	datas, selectedVers, selectedLocs, err := loadMetaBat(ctx, versions, locations, fs, mp)
	if err != nil {
		return nil, nil, err
	}

	usageInsData := make([][]UsageData, len(selectedVers))
	usageDelData := make([][]UsageData, len(selectedVers))

	var usageInsBat []*batch.Batch
	var usageDelBat []*batch.Batch

	for idx := 0; idx < len(datas); idx++ {
		datas[idx].GetTableMeta(UsageBatMetaTableId, selectedVers[idx], selectedLocs[idx])
		usageMeta := datas[idx].meta[UsageBatMetaTableId]

		if usageMeta == nil {
			continue
		}

		// 2.1. load storage usage ins bat
		if usageInsBat, err = loadStorageUsageBatch(
			ctx, usageMeta.tables[StorageUsageIns].locations,
			selectedVers[idx], StorageUsageInsIDX, fs, mp); err != nil {
			return nil, nil, err
		}

		// 2.2. load storage usage del bat
		if usageDelBat, err = loadStorageUsageBatch(
			ctx, usageMeta.tables[StorageUsageDel].locations,
			selectedVers[idx], StorageUsageDelIDX, fs, mp); err != nil {
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

	// release memory
	for idx := range datas {
		datas[idx].bats[MetaIDX].Clean(mp)
	}

	for idx := range usageInsBat {
		usageInsBat[idx].Clean(mp)
	}

	for idx := range usageDelBat {
		usageDelBat[idx].Clean(mp)
	}

	return usageInsData, usageDelData, nil
}

func cnBatchToUsageDatas(bat *batch.Batch) []UsageData {
	accCol := vector.MustFixedCol[uint64](bat.GetVector(2))
	dbCol := vector.MustFixedCol[uint64](bat.GetVector(3))
	tblCol := vector.MustFixedCol[uint64](bat.GetVector(4))
	sizeCol := vector.MustFixedCol[uint64](bat.GetVector(6))

	usages := make([]UsageData, 0)

	for idx := range accCol {
		usages = append(usages, UsageData{
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
	fs fileservice.FileService, mp *mpool.MPool) (
	datas []*CNCheckpointData,
	selectedVers []uint32,
	selectedLocs []objectio.Location,
	err error) {

	var idxes []uint16

	for idx := 0; idx < len(locations); idx++ {
		if versions[idx] < CheckpointVersion11 {
			// start with version 11, storage usage ins/del bat's locations is recorded in meta bat.
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
			return nil, nil, nil, err
		}

		data.InitMetaIdx(ctx, versions[idx], reader, locations[idx], mp)

		idxes = idxes[:0]
		datas = append(datas, data)
		selectedVers = append(selectedVers, versions[idx])
		selectedLocs = append(selectedLocs, locations[idx])
	}

	return datas, selectedVers, selectedLocs, nil
}

func loadStorageUsageBatch(
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

func PairAccountVsDB(c *catalog.Catalog) map[uint64]uint64 {
	pairs := make(map[uint64]uint64)

	processor := new(catalog.LoopProcessor)
	processor.DatabaseFn = func(entry *catalog.DBEntry) error {
		pairs[entry.GetID()] = uint64(entry.GetTenantID())
		return nil
	}

	c.RecurLoop(processor)
	return pairs
}

func CorrectUsageWrongPlacement(c *catalog.Catalog) (int, float64, error) {
	memo := c.GetUsageMemo().(*TNUsageMemo)
	if memo == nil {
		return 0, 0, moerr.NewInternalErrorNoCtx("tn usage cache is nil")
	}

	var buf bytes.Buffer

	pairs := PairAccountVsDB(c)

	memo.EnterProcessing()
	defer memo.LeaveProcessing()

	usages := memo.cache.data.Items()

	anyTransferred := int(0)
	transferredSize := float64(0)
	for idx := range usages {
		if pairs[usages[idx].DbId] != usages[idx].AccId {
			anyTransferred++
			transferredSize += float64(usages[idx].Size)

			memo.DeltaUpdate(usages[idx], true)
			buf.WriteString(fmt.Sprintf("[td-tbl]%d_%d_%d_%d; ",
				usages[idx].AccId, usages[idx].DbId, usages[idx].TblId, usages[idx].Size))
			//memo.pendingTransfer.deletes = append(memo.pendingTransfer.deletes, usages[idx])

			usages[idx].AccId = pairs[usages[idx].DbId]
			memo.DeltaUpdate(usages[idx], false)
			buf.WriteString(fmt.Sprintf("[ti-tbl]%d_%d_%d_%d; ",
				usages[idx].AccId, usages[idx].DbId, usages[idx].TblId, usages[idx].Size))
			//memo.pendingTransfer.inserts = append(memo.pendingTransfer.inserts, usages[idx])
		}
	}

	transferredSize /= 1024 * 1024
	logutil.Info("[storage usage] apply transfer: ",
		zap.String(fmt.Sprintf("transferred %d tbl, %f mb", anyTransferred, transferredSize), buf.String()))

	return anyTransferred, transferredSize, nil
}

func EliminateErrorsOnCache(c *catalog.Catalog, end types.TS) int {
	collector := BaseCollector{}
	loop := catalog.LoopProcessor{}
	loop.ObjectFn = func(entry *catalog.ObjectEntry) error {
		if entry.GetTable().GetDB().HasDropCommitted() || entry.GetTable().HasDropCommitted() {
			return nil
		}

		// PXU TODO
		if entry.IsAppendable() || !entry.IsCommittedLocked() {
			return nil
		}

		entry.Lock()
		createTS := entry.GetCreatedAtLocked()
		if createTS.GreaterEq(&end) {
			entry.Unlock()
			return nil
		}
		entry.Unlock()

		if entry.HasDropCommitted() {
			collector.Usage.ObjDeletes = append(collector.Usage.ObjDeletes, entry)
		} else {
			collector.Usage.ObjInserts = append(collector.Usage.ObjInserts, entry)
		}

		return nil
	}

	c.RecurLoop(&loop)

	collector.UsageMemo = c.GetUsageMemo().(*TNUsageMemo)

	collector.UsageMemo.EnterProcessing()
	defer collector.UsageMemo.LeaveProcessing()
	_, cnt := putCacheBack2Track(&collector)

	return cnt
}
