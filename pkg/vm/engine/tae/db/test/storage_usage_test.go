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

package test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	// "github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

func Test_StorageUsageCache(t *testing.T) {
	// new cache with no option
	cache := logtail.NewStorageUsageCache(logtail.WithLazyThreshold(1))
	cache.Lock()
	defer cache.Unlock()

	require.True(t, cache.IsExpired())

	allocator := atomic.Uint64{}

	accCnt, dbCnt, tblCnt := 10, 10, 10
	usages := logtail.MockUsageData(accCnt, dbCnt, tblCnt, &allocator)
	for idx := range usages {
		cache.SetOrReplace(usages[idx])
	}

	fmt.Println(cache.String())

	// 1. test expired
	{
		require.Equal(t, len(usages), cache.CacheLen())
		require.False(t, cache.IsExpired())

		time.Sleep(time.Second * 1)
		require.True(t, cache.IsExpired())
	}

	// 2. test the less func
	{
		sort.Slice(usages, func(i, j int) bool {
			return cache.LessFunc()(usages[i], usages[j])
		})

		for idx := 1; idx < len(usages); idx++ {
			require.True(t, usages[idx].AccId >= usages[idx-1].AccId)

			if usages[idx].AccId == usages[idx-1].AccId {
				require.True(t, usages[idx].DbId >= usages[idx-1].DbId)

				if usages[idx].DbId == usages[idx-1].DbId {
					require.True(t, usages[idx].TblId >= usages[idx-1].TblId)
				}
			}
		}
	}

	// 3. test gather account size
	{
		totalSize := uint64(0)
		for idx := 0; idx < len(usages); idx++ {
			totalSize += usages[idx].Size
		}

		gathered := cache.GatherAllAccSize()
		for accId := range gathered {
			totalSize -= gathered[accId]

			size, exist := cache.GatherAccountSize(accId)
			require.True(t, exist)
			require.Equal(t, gathered[accId], size)
		}

		require.Equal(t, uint64(0), totalSize)

		size := uint64(0)
		preAccId := usages[0].AccId
		for idx := 0; idx < len(usages); idx++ {
			if usages[idx].AccId == preAccId {
				size += usages[idx].Size
				continue
			}

			gsize, exist := cache.GatherAccountSize(preAccId)
			require.True(t, exist)
			require.Equal(t, size, gsize)

			size = usages[idx].Size
			preAccId = usages[idx].AccId
		}
	}

	// 4. test mem used
	{
		require.Equal(t, len(usages), cache.CacheLen())
		used := float64(int(unsafe.Sizeof(logtail.UsageData{})) * len(usages))
		require.True(t, cache.MemUsed() > math.Round(used/1048576.0*1e6)/10e6)
	}

	// 5. test delete and get
	{
		for idx := 0; usages[idx].AccId == usages[0].AccId; idx++ {
			cache.Delete(usages[idx])
			_, exist := cache.Get(usages[idx])
			require.False(t, exist)
		}

		require.False(t, cache.IsExpired())

		_, exist := cache.GatherAccountSize(usages[0].AccId)
		require.False(t, exist)
	}

}

func mockDeletesAndInserts(
	usages []logtail.UsageData,
	delDbIds, delTblIds map[uint64]int,
	delSegIdxes, insSegIdxes map[int]struct{}) (
	[]interface{}, []*catalog.ObjectEntry, []*catalog.ObjectEntry) {
	var deletes []interface{}
	var segInserts []*catalog.ObjectEntry
	var segDeletes []*catalog.ObjectEntry

	// mock deletes, inserts
	{
		// db deletes
		for idx := range usages {
			if _, ok := delDbIds[usages[idx].DbId]; !ok {
				continue
			}
			deletes = append(deletes,
				catalog.MockDBEntryWithAccInfo(usages[idx].AccId, usages[idx].DbId))
		}

		// tbl deletes
		for idx := range usages {
			if _, ok := delTblIds[usages[idx].TblId]; !ok {
				continue
			}
			deletes = append(deletes,
				catalog.MockTableEntryWithDB(
					catalog.MockDBEntryWithAccInfo(
						usages[idx].AccId, usages[idx].DbId), usages[idx].TblId))
		}

		// segment deletes
		for idx := range usages {
			if _, ok := delSegIdxes[idx]; !ok {
				continue
			}
			segDeletes = append(segDeletes,
				catalog.MockObjEntryWithTbl(
					catalog.MockTableEntryWithDB(
						catalog.MockDBEntryWithAccInfo(usages[idx].AccId, usages[idx].DbId),
						usages[idx].TblId), usages[idx].Size))
		}

		// segment inserts
		for idx := range usages {
			if _, ok := insSegIdxes[idx]; !ok {
				continue
			}
			segInserts = append(segInserts,
				catalog.MockObjEntryWithTbl(
					catalog.MockTableEntryWithDB(
						catalog.MockDBEntryWithAccInfo(usages[idx].AccId, usages[idx].DbId),
						usages[idx].TblId), usages[idx].Size))
		}
	}

	return deletes, segDeletes, segInserts
}

func Test_FillUsageBatOfIncremental(t *testing.T) {
	blockio.RunPipelineTest(
		func() {
			allocator := atomic.Uint64{}
			allocator.Store(pkgcatalog.MO_RESERVED_MAX + 1)

			accCnt, dbCnt, tblCnt := 10, 10, 10
			usages := logtail.MockUsageData(accCnt, dbCnt, tblCnt, &allocator)

			memo := logtail.NewTNUsageMemo(nil)
			memo.Clear()

			sort.Slice(usages, func(i, j int) bool {
				return memo.GetCache().LessFunc()(usages[i], usages[j])
			})

			delDbCnt, delTblCnt, delSegCnt := 2, 3, 7
			delDbIds := make(map[uint64]int)
			delTblIds := make(map[uint64]int)
			delSegIdxes := make(map[int]struct{})
			insSegIdxes := make(map[int]struct{})

			// generate deletes
			{
				for i := 0; i < delDbCnt; i++ {
					idx := rand.Int() % len(usages)
					delDbIds[usages[idx].DbId] = idx
				}

				for i := 0; i < delTblCnt; i++ {
					idx := rand.Int() % len(usages)
					delTblIds[usages[idx].TblId] = idx
				}

				for i := 0; i < delSegCnt; i++ {
					delSegIdxes[rand.Int()%len(usages)] = struct{}{}
				}

				for i := 0; i < len(usages); i++ {
					insSegIdxes[i] = struct{}{}
				}
			}

			deletes, segDeletes, segInserts := mockDeletesAndInserts(
				usages, delDbIds, delTblIds, delSegIdxes, insSegIdxes)

			iCollector := logtail.NewIncrementalCollector("", types.TS{}, types.MaxTs())
			iCollector.UsageMemo = memo
			defer iCollector.Close()

			iCollector.Usage.Deletes = deletes
			iCollector.Usage.ObjDeletes = segDeletes
			iCollector.Usage.ObjInserts = segInserts

			logtail.FillUsageBatOfIncremental(iCollector)

			var delUsages []logtail.UsageData

			// test apply inserts and deletes
			{
				for idx := range usages {
					old, exist := memo.Get(usages[idx])
					_, ok1 := delDbIds[usages[idx].DbId]
					_, ok2 := delTblIds[usages[idx].TblId]
					if ok1 || ok2 {
						require.Equal(t, logtail.UsageData{}, old)
						require.False(t, exist)
						continue
					}

					if _, ok := delSegIdxes[idx]; ok {
						require.Equal(t, old.Size, uint64(0))
						require.True(t, exist)
					}
				}

				// gather all deletes
				for idx := range usages {
					if _, ok := delTblIds[usages[idx].TblId]; ok {
						delUsages = append(delUsages, usages[idx])
					}
				}

				for idx := range usages {
					if _, ok := delTblIds[usages[idx].TblId]; ok {
						continue
					}
					if _, ok := delDbIds[usages[idx].DbId]; ok {
						delUsages = append(delUsages, usages[idx])
					}
				}

				for idx := range usages {
					_, ok1 := delDbIds[usages[idx].DbId]
					_, ok2 := delTblIds[usages[idx].TblId]
					if ok1 || ok2 {
						continue
					}
					if _, ok := delSegIdxes[idx]; ok {
						last := &delUsages[len(delUsages)-1]

						if last.TblId == usages[idx].TblId &&
							last.AccId == usages[idx].AccId &&
							last.DbId == usages[idx].DbId {
							last.Size += usages[idx].Size
						} else {
							delUsages = append(delUsages, usages[idx])

						}
					}

				}
			}

			// test append to checkpoint
			{
				ckpData := iCollector.OrphanData()
				defer ckpData.Close()

				delBat := ckpData.GetBatches()[logtail.StorageUsageDelIDX]
				//insBat := ckpData.GetBatches()[logtail.StorageUsageInsIDX]

				accCol := vector.MustFixedCol[uint64](delBat.GetVectorByName(pkgcatalog.SystemColAttr_AccID).GetDownstreamVector())
				dbCol := vector.MustFixedCol[uint64](delBat.GetVectorByName(catalog.SnapshotAttr_DBID).GetDownstreamVector())
				tblCol := vector.MustFixedCol[uint64](delBat.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
				sizeCol := vector.MustFixedCol[uint64](delBat.GetVectorByName(logtail.CheckpointMetaAttr_ObjectSize).GetDownstreamVector())

				require.Equal(t, len(accCol), len(delUsages))

				for idx := range accCol {
					require.Equal(t, accCol[idx], delUsages[idx].AccId)
					require.Equal(t, dbCol[idx], delUsages[idx].DbId)
					require.Equal(t, tblCol[idx], delUsages[idx].TblId)
					require.Equal(t, sizeCol[idx], delUsages[idx].Size)
				}
			}
		},
	)
}

func Test_FillUsageBatOfGlobal(t *testing.T) {
	blockio.RunPipelineTest(
		func() {
			allocator := atomic.Uint64{}
			allocator.Store(pkgcatalog.MO_RESERVED_MAX + 1)

			accCnt, dbCnt, tblCnt := 10, 10, 10
			usages := logtail.MockUsageData(accCnt, dbCnt, tblCnt, &allocator)

			memo := logtail.NewTNUsageMemo(nil)
			memo.Clear()

			gCollector := logtail.NewGlobalCollector("", types.TS{}, time.Second)
			gCollector.UsageMemo = memo
			defer gCollector.Close()

			for idx := range usages {
				memo.DeltaUpdate(usages[idx], false)
				gCollector.Usage.ReservedAccIds[usages[idx].AccId] = struct{}{}
			}

			// test memo reply to global ckp
			{

				logtail.FillUsageBatOfGlobal(gCollector)

				ckpData := gCollector.OrphanData()
				defer ckpData.Close()

				delBat := ckpData.GetBatches()[logtail.StorageUsageDelIDX]
				require.Equal(t, delBat.GetVectorByName(pkgcatalog.SystemColAttr_AccID).Length(), 0)

				insBat := ckpData.GetBatches()[logtail.StorageUsageInsIDX]
				require.Equal(t, insBat.GetVectorByName(pkgcatalog.SystemColAttr_AccID).Length(), len(usages))

				// usage datas in memo ordered
				sort.Slice(usages, func(i, j int) bool {
					return memo.GetCache().LessFunc()(usages[i], usages[j])
				})

				accCol := vector.MustFixedCol[uint64](insBat.GetVectorByName(pkgcatalog.SystemColAttr_AccID).GetDownstreamVector())
				dbCol := vector.MustFixedCol[uint64](insBat.GetVectorByName(catalog.SnapshotAttr_DBID).GetDownstreamVector())
				tblCol := vector.MustFixedCol[uint64](insBat.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
				sizeCol := vector.MustFixedCol[uint64](insBat.GetVectorByName(logtail.CheckpointMetaAttr_ObjectSize).GetDownstreamVector())

				for idx := 0; idx < len(accCol); idx++ {
					require.Equal(t, accCol[idx], usages[idx].AccId)
					require.Equal(t, dbCol[idx], usages[idx].DbId)
					require.Equal(t, tblCol[idx], usages[idx].TblId)
					require.Equal(t, sizeCol[idx], usages[idx].Size)
				}
			}
		},
	)
}

func appendUsageToBatch(bat *containers.Batch, usage logtail.UsageData) {
	accVec := bat.GetVectorByName(pkgcatalog.SystemColAttr_AccID).GetDownstreamVector()
	dbVec := bat.GetVectorByName(catalog.SnapshotAttr_DBID).GetDownstreamVector()
	tblVec := bat.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector()
	sizeVec := bat.GetVectorByName(logtail.CheckpointMetaAttr_ObjectSize).GetDownstreamVector()

	vector.AppendFixed(accVec, usage.AccId, false, common.DebugAllocator)
	vector.AppendFixed(dbVec, usage.DbId, false, common.DebugAllocator)
	vector.AppendFixed(tblVec, usage.TblId, false, common.DebugAllocator)
	vector.AppendFixed(sizeVec, usage.Size, false, common.DebugAllocator)

}

func Test_EstablishFromCheckpoints(t *testing.T) {
	version11Cnt := 5
	allocator := atomic.Uint64{}
	allocator.Store(pkgcatalog.MO_RESERVED_MAX + 1)

	ckps := make([]*logtail.CheckpointData, 0)
	vers := make([]uint32, 0)

	var usageIns, usageDel []logtail.UsageData

	for idx := 0; idx < version11Cnt; idx++ {
		data := logtail.NewCheckpointDataWithVersion(logtail.CheckpointVersion12, common.DebugAllocator)
		insBat := data.GetBatches()[logtail.StorageUsageInsIDX]
		delBat := data.GetBatches()[logtail.StorageUsageDelIDX]

		usages := logtail.MockUsageData(10, 10, 10, &allocator)
		usageIns = append(usageIns, usages...)
		for xx := range usages {
			appendUsageToBatch(insBat, usages[xx])
		}

		usages = logtail.MockUsageData(10, 10, 10, &allocator)
		usageDel = append(usageDel, usages...)
		for xx := range usages {
			appendUsageToBatch(delBat, usages[xx])
		}

		ckps = append(ckps, data)
		vers = append(vers, logtail.CheckpointVersion12)
	}

	memo := logtail.NewTNUsageMemo(nil)
	memo.Clear()

	memo.PrepareReplay(ckps, vers)
	memo.EstablishFromCKPs(nil)

	memoShadow := logtail.NewTNUsageMemo(nil)
	for idx := range usageIns {
		memoShadow.DeltaUpdate(usageIns[idx], false)
	}

	for idx := range usageDel {
		memoShadow.DeltaUpdate(usageDel[idx], true)
	}

	require.Equal(t, memo.CacheLen(), memoShadow.CacheLen())

	iter := memoShadow.GetCache().Iter()
	for iter.Next() {
		usage, exist := memo.Get(iter.Item())
		require.True(t, exist)
		//fmt.Println(usage)
		//fmt.Println(iter.Item())
		//fmt.Println()
		require.Equal(t, usage, iter.Item())
	}
	iter.Release()
}

func Test_RemoveStaleAccounts(t *testing.T) {
	blockio.RunPipelineTest(
		func() {
			// clear stale accounts happens in global ckp
			allocator := atomic.Uint64{}
			allocator.Store(pkgcatalog.MO_RESERVED_MAX + 1)

			accCnt, dbCnt, tblCnt := 10000, 2, 2
			usages := logtail.MockUsageData(accCnt, dbCnt, tblCnt, &allocator)

			gCollector := logtail.NewGlobalCollector("", types.TS{}, time.Second)
			gCollector.UsageMemo = logtail.NewTNUsageMemo(nil)
			defer gCollector.Close()

			for idx := range usages {
				gCollector.UsageMemo.DeltaUpdate(usages[idx], false)
				if rand.Int()%3 == 0 {
					// mock the accounts deletion
					continue
				}
				gCollector.Usage.ReservedAccIds[usages[idx].AccId] = struct{}{}
			}

			logtail.FillUsageBatOfGlobal(gCollector)

			sizes := gCollector.UsageMemo.GatherAllAccSize()

			require.Equal(t, len(gCollector.Usage.ReservedAccIds), len(sizes))

			for accId := range sizes {
				_, ok := gCollector.Usage.ReservedAccIds[accId]
				require.True(t, ok)
			}
		},
	)
}

func Test_GatherSpecialSize(t *testing.T) {
	blockio.RunPipelineTest(
		func() {
			cc := catalog.MockCatalog()
			memo := logtail.NewTNUsageMemo(cc)
			memo.Clear()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(cc), catalog.MockTxnFactory(cc), types.NewMockHLCClock(1))
			txnMgr.Start(context.Background())
			defer txnMgr.Stop()

			txn, _ := txnMgr.StartTxn(nil)

			hdb, err := txn.GetDatabase(pkgcatalog.MO_CATALOG)
			require.Nil(t, err)
			require.NotNil(t, hdb)

			stats := objectio.NewObjectStats()
			objectio.SetObjectStatsSize(stats, 123*1024*1024)

			var rel handle.Relation
			var relId uint64 = 0x11235
			{
				ss := catalog.MockSchema(10, 0)
				ss.Relkind = "cluster"
				rel, err = hdb.CreateRelationWithID(ss, relId)
				require.Nil(t, err)

				vec := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
				vec.Append(stats.Clone().Marshal(), false)

				rel.AddObjsWithMetaLoc(ctx, vec)
				vec.Close()
			}

			txn.Commit(ctx)

			iCollector := logtail.NewIncrementalCollector("", types.TS{}, types.MaxTs())
			iCollector.UsageMemo = memo
			defer iCollector.Close()

			size := uint64(1024 * 1024)
			usages := []logtail.UsageData{
				{AccId: uint64(pkgcatalog.System_Account), DbId: pkgcatalog.MO_CATALOG_ID, TblId: pkgcatalog.MO_TABLES_ID, Size: size},
				{AccId: uint64(pkgcatalog.System_Account), DbId: pkgcatalog.MO_CATALOG_ID, TblId: pkgcatalog.MO_DATABASE_ID, Size: size},
				{AccId: uint64(pkgcatalog.System_Account), DbId: pkgcatalog.MO_CATALOG_ID, TblId: pkgcatalog.MO_COLUMNS_ID, Size: size},
				{AccId: uint64(pkgcatalog.System_Account), DbId: pkgcatalog.MO_CATALOG_ID, TblId: relId, Size: size},
			}

			expected := size * 4

			_, _, ins := mockDeletesAndInserts(
				usages, map[uint64]int{}, map[uint64]int{},
				map[int]struct{}{}, map[int]struct{}{0: {}, 1: {}, 2: {}, 3: {}})

			iCollector.Usage.ObjInserts = ins
			logtail.FillUsageBatOfIncremental(iCollector)

			actual := memo.GatherSpecialTableSize()

			require.Equal(t, expected, actual)
		},
	)
}
