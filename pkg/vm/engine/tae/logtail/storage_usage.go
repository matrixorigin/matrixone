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
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
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

const cluster = "cluster"
const StorageUsageMagic uint64 = 0x1A2B3C4D5E6F

type triode int

const (
	unknown triode = 0
	yeah    triode = 1
	nope    triode = 2
)

type UsageData struct {
	AccId        uint64
	DbId         uint64
	TblId        uint64
	Size         uint64
	SnapshotSize uint64

	special triode

	// this will not persist
	// only global ckp will update
	ObjectAbstract
}

type ObjectAbstract struct {
	TotalObjCnt  int
	TotalObjSize int
	TotalBlkCnt  int
	TotalRowCnt  int
}

var zeroUsageData UsageData = UsageData{
	AccId:        math.MaxUint32,
	DbId:         math.MaxUint64,
	TblId:        math.MaxUint64,
	Size:         math.MaxInt64,
	SnapshotSize: math.MaxInt64,
	special:      unknown,
}

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
	return fmt.Sprintf("account id = %d; database id = %d; table id = %d; size = %d; snapshot size = %d",
		u.AccId, u.DbId, u.TblId, u.Size, u.SnapshotSize)
}

func (u *UsageData) Merge(other UsageData, delete bool) {
	if delete {
		if u.Size <= other.Size {
			u.Size = 0
			u.TotalObjSize = 0
		} else {
			u.Size -= other.Size
			u.TotalObjSize -= other.TotalObjSize
		}

		u.TotalObjCnt -= other.TotalObjCnt
		u.TotalRowCnt -= other.TotalRowCnt
		u.TotalBlkCnt -= other.TotalBlkCnt

	} else {
		u.Size += other.Size
		u.TotalObjSize += other.TotalObjSize
		u.TotalObjCnt += other.TotalObjCnt
		u.TotalRowCnt += other.TotalRowCnt
		u.TotalBlkCnt += other.TotalBlkCnt
	}
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

func (c *StorageUsageCache) GatherAllAccSize() (usages map[uint64][]uint64) {
	usages = make(map[uint64][]uint64)
	c.data.Scan(func(item UsageData) bool {
		if len(usages[item.AccId]) == 0 {
			usages[item.AccId] = make([]uint64, 2)
		}
		usages[item.AccId][0] += item.Size
		usages[item.AccId][1] += item.SnapshotSize
		return true
	})

	return
}

func (c *StorageUsageCache) GatherObjectAbstractForAccounts() (abstract map[uint64]ObjectAbstract) {
	abstract = make(map[uint64]ObjectAbstract)
	c.data.Scan(func(item UsageData) bool {
		a := abstract[item.AccId]
		a.TotalObjCnt += item.TotalObjCnt
		a.TotalObjSize += item.TotalObjSize
		a.TotalBlkCnt += item.TotalBlkCnt
		a.TotalRowCnt += item.TotalRowCnt
		abstract[item.AccId] = a
		return true
	})

	return
}

func (c *StorageUsageCache) GatherAccountSize(id uint64) (size, snapshotSize uint64, exist bool) {
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
	snapshotSize += iter.Item().SnapshotSize
	for iter.Next() && iter.Item().AccId == id {
		size += iter.Item().Size
		snapshotSize += iter.Item().SnapshotSize
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

	C *catalog.Catalog
}

func NewTNUsageMemo(c *catalog.Catalog) *TNUsageMemo {
	memo := new(TNUsageMemo)
	memo.cache = NewStorageUsageCache()
	memo.newAccCache = NewStorageUsageCache()
	memo.C = c
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
	m.Lock()
	m.cache.Lock()
}

func (m *TNUsageMemo) LeaveProcessing() {
	m.cache.Unlock()
	m.Unlock()
}

func (m *TNUsageMemo) HasUpdate() bool {
	return m.pending
}

func (m *TNUsageMemo) gatherAccountSizeHelper(cache *StorageUsageCache, id uint64) (size, snapshotSize uint64, exist bool) {
	return cache.GatherAccountSize(id)
}

func (m *TNUsageMemo) GatherObjectAbstractForAllAccount() map[uint64]ObjectAbstract {
	return m.cache.GatherObjectAbstractForAccounts()
}

func (m *TNUsageMemo) GatherAccountSize(id uint64) (size, snapshotSize uint64, exist bool) {
	return m.gatherAccountSizeHelper(m.cache, id)
}

var specialNameRegexp = regexp.MustCompile(fmt.Sprintf("%s|%s|%s", pkgcatalog.MO_DATABASE, pkgcatalog.MO_TABLES, pkgcatalog.MO_COLUMNS))
var indexNameRegexp = regexp.MustCompile("__mo_index.*")

func (m *TNUsageMemo) checkSpecial(usage UsageData, tbl *catalog.TableEntry) triode {
	if usage.special != unknown {
		return usage.special
	}

	if tbl == nil {
		return unknown
	}

	name := strings.ToLower(tbl.GetFullName())
	if indexNameRegexp.MatchString(name) {
		// we don't know if an index table is special or not
		return nope
	}

	if specialNameRegexp.MatchString(name) {
		return yeah
	}

	schema := tbl.GetLastestSchema(false)
	if strings.ToLower(schema.Relkind) == cluster {
		return yeah
	}

	return nope
}

func (m *TNUsageMemo) GatherSpecialTableSize() (size uint64) {
	dbEntry, err := m.C.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	if err != nil {
		logutil.Errorf("[storage]: get mo_catalog from catalog failed: %v", err)
		return 0
	}

	usage := UsageData{AccId: uint64(pkgcatalog.System_Account), DbId: pkgcatalog.MO_CATALOG_ID}
	iter := m.cache.Iter()

	var updates []UsageData

	ok := iter.Seek(usage)
	for ok {
		item := iter.Item()
		if item.AccId != uint64(pkgcatalog.System_Account) || item.DbId != pkgcatalog.MO_CATALOG_ID {
			break
		}
		//fmt.Println(item.String(), item.special)
		var ret triode
		if ret = m.checkSpecial(item, nil); ret != unknown {
			if ret == yeah {
				size += item.Size
			}
			ok = iter.Next()
			continue
		}

		tbl, err := dbEntry.GetTableEntryByID(item.TblId)
		if err != nil {
			logutil.Errorf("[storage]: get table %d from db %s failed: %v", item.TblId, dbEntry.GetName(), err)
			ok = iter.Next()
			continue
		}

		if ret = m.checkSpecial(item, tbl); ret == yeah {
			size += item.Size
		}

		item.special = ret
		updates = append(updates, item)

		ok = iter.Next()
	}

	iter.Release()

	for _, item := range updates {
		m.Replace(item)
	}

	return size
}

func (m *TNUsageMemo) GatherNewAccountSize(id uint64) (size, snapshotSize uint64, exist bool) {
	return m.gatherAccountSizeHelper(m.newAccCache, id)
}

func (m *TNUsageMemo) GatherAllAccSize() (usages map[uint64][]uint64) {
	return m.cache.GatherAllAccSize()
}

func (m *TNUsageMemo) updateHelper(cache *StorageUsageCache, usage UsageData, del bool) {
	size := uint64(0)
	special := unknown
	if old, found := cache.Get(usage); found {
		size = old.Size
		special = old.special
		usage.SnapshotSize = old.SnapshotSize

		usage.ObjectAbstract = old.ObjectAbstract
	}
	usage.special = special

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

func (m *TNUsageMemo) deleteAccount(accId uint64) (size uint64) {
	trash := make([]UsageData, 0)
	povit := UsageData{AccId: accId, special: unknown}

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
func (m *TNUsageMemo) EstablishFromCKPs() {
	m.EnterProcessing()
	defer m.LeaveProcessing()

	var buf bytes.Buffer

	defer func() {
		for idx := range m.pendingReplay.datas {
			m.pendingReplay.datas[idx] = nil
		}
		logutil.Info("[storage usage] replay:",
			zap.String("remove old deleted db/tbl", buf.String()),
			zap.Int("delayed tbl", len(m.pendingReplay.delayed)))
	}()

	for x := range m.pendingReplay.datas {

		insVecs := getStorageUsageBatVectors(m.pendingReplay.datas[x].bats[StorageUsageInsIDX])
		accCol, dbCol, tblCol, sizeCol := getStorageUsageVectorCols(insVecs)

		// var skip bool
		// var log string
		for y := 0; y < len(accCol); y++ {
			usage := UsageData{AccId: accCol[y], DbId: dbCol[y], TblId: tblCol[y], Size: sizeCol[y], special: unknown}
			m.DeltaUpdate(usage, false)
		}

		// if m.pendingReplay.vers[x] < CheckpointVersion11 {
		// 	// haven't StorageUsageDel batch
		// 	continue
		// }

		delVecs := getStorageUsageBatVectors(m.pendingReplay.datas[x].bats[StorageUsageDelIDX])
		accCol, dbCol, tblCol, sizeCol = getStorageUsageVectorCols(delVecs)

		for y := 0; y < len(accCol); y++ {
			usage := UsageData{AccId: accCol[y], DbId: dbCol[y], TblId: tblCol[y], Size: sizeCol[y], special: unknown}
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

	dbCol = vector.MustFixedColWithTypeCheck[uint64](vecs[UsageDBID])
	accCol = vector.MustFixedColWithTypeCheck[uint64](vecs[UsageAccID])
	tblCol = vector.MustFixedColWithTypeCheck[uint64](vecs[UsageTblID])
	sizeCol = vector.MustFixedColWithTypeCheck[uint64](vecs[UsageSize])

	return
}

const UsageBatMetaTableId uint64 = StorageUsageMagic

// 0: insert, 1: delete
var summaryLog [2][]UsageData

func Objects2Usages(objs []*catalog.ObjectEntry, isGlobal bool) (usages []UsageData) {
	toUsage := func(obj *catalog.ObjectEntry) UsageData {
		usage := UsageData{
			DbId:  obj.GetTable().GetDB().GetID(),
			Size:  uint64(obj.Size()),
			TblId: obj.GetTable().GetID(),
			AccId: uint64(obj.GetTable().GetDB().GetTenantID()),
		}

		if isGlobal {
			usage.ObjectAbstract = ObjectAbstract{
				TotalBlkCnt:  int(obj.BlkCnt()),
				TotalObjCnt:  1,
				TotalObjSize: int(obj.Size()),
				TotalRowCnt:  int(obj.Rows()),
			}
		}

		return usage
	}

	for idx := range objs {
		usages = append(usages, toUsage(objs[idx]))
	}

	return
}

func FillUsageBatOfCompacted(
	ctx context.Context,
	usage *TNUsageMemo,
	data *batch.Batch,
	meta *SnapshotMeta,
	accountSnapshots map[uint32][]types.TS,
	pitrs *PitrInfo,
	_ int,
) {
	start := time.Now()
	var memoryUsed float64
	usage.EnterProcessing()
	defer func() {
		v2.TaskStorageUsageCacheMemUsedGauge.Set(memoryUsed)
		v2.TaskCompactedCollectUsageDurationHistogram.Observe(time.Since(start).Seconds())
		usage.LeaveProcessing()
	}()
	usageData := make(map[[3]uint64]UsageData)
	tableSnapshots, tablePitrs := meta.AccountToTableSnapshots(
		accountSnapshots,
		pitrs,
	)
	objectsName := make(map[string]struct{})
	scan := func(bat *batch.Batch) {
		insDeleteTSVec := vector.MustFixedColWithTypeCheck[types.TS](
			bat.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx])
		insCreateTSVec := vector.MustFixedColWithTypeCheck[types.TS](
			bat.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx])
		dbid := vector.MustFixedColNoTypeCheck[uint64](
			bat.Vecs[ckputil.TableObjectsAttr_DB_Idx])
		tableID := vector.MustFixedColNoTypeCheck[uint64](
			bat.Vecs[ckputil.TableObjectsAttr_Table_Idx])
		for i := 0; i < bat.RowCount(); i++ {
			if insDeleteTSVec[i].IsEmpty() {
				dropTs, ok := meta.GetTableDropAt(tableID[i])
				if !ok || dropTs.IsEmpty() {
					continue
				}
			}
			id, ok := meta.GetAccountId(tableID[i])
			if !ok {
				continue
			}
			accountId := uint64(id)
			if len(tableSnapshots[tableID[i]]) == 0 &&
				(tablePitrs[tableID[i]] == nil ||
					tablePitrs[tableID[i]].IsEmpty()) {
				continue
			}

			buf := bat.Vecs[ckputil.TableObjectsAttr_ID_Idx].GetRawBytesAt(i)
			stats := (objectio.ObjectStats)(buf)
			if !ObjectIsSnapshotRefers(
				&stats,
				tablePitrs[tableID[i]],
				&insCreateTSVec[i],
				&insDeleteTSVec[i],
				tableSnapshots[tableID[i]]) {
				continue
			}
			// skip the same object
			if _, ok = objectsName[stats.ObjectName().String()]; ok {
				continue
			}
			key := [3]uint64{accountId, dbid[i], tableID[i]}
			snapSize := usageData[key].SnapshotSize
			snapSize += uint64(stats.Size())
			usageData[key] = UsageData{
				AccId:        accountId,
				DbId:         dbid[i],
				TblId:        tableID[i],
				SnapshotSize: snapSize,
			}

			objectsName[stats.ObjectName().String()] = struct{}{}
		}
	}
	scan(data)
	iter := usage.cache.data.Iter()
	update := make(map[[3]uint64]UsageData)
	for iter.Next() {
		val := iter.Item()
		key := [3]uint64{val.AccId, val.DbId, val.TblId}
		ud, ok := usageData[key]
		if val.SnapshotSize == 0 && !ok {
			continue
		}
		val.SnapshotSize = ud.SnapshotSize
		update[key] = val
		delete(usageData, key)
	}
	iter.Release()

	for _, v := range update {
		usage.cache.SetOrReplace(v)
	}

	// table has been dropped
	for _, v := range usageData {
		usage.cache.SetOrReplace(v)
	}
	memoryUsed = usage.MemoryUsed()
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
