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
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"

	// "github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
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
			totalSize -= gathered[accId][0]

			size, _, exist := cache.GatherAccountSize(accId)
			require.True(t, exist)
			require.Equal(t, gathered[accId][0], size)
		}

		require.Equal(t, uint64(0), totalSize)

		size := uint64(0)
		preAccId := usages[0].AccId
		for idx := 0; idx < len(usages); idx++ {
			if usages[idx].AccId == preAccId {
				size += usages[idx].Size
				continue
			}

			gsize, _, exist := cache.GatherAccountSize(preAccId)
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

		_, _, exist := cache.GatherAccountSize(usages[0].AccId)
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
				catalog.MockObjEntryWithTbl(catalog.MockTableEntryWithDB(
					catalog.MockDBEntryWithAccInfo(usages[idx].AccId, usages[idx].DbId),
					usages[idx].TblId), usages[idx].Size, false))
		}

		// segment inserts
		for idx := range usages {
			if _, ok := insSegIdxes[idx]; !ok {
				continue
			}
			segInserts = append(segInserts,
				catalog.MockObjEntryWithTbl(catalog.MockTableEntryWithDB(
					catalog.MockDBEntryWithAccInfo(usages[idx].AccId, usages[idx].DbId),
					usages[idx].TblId), usages[idx].Size, false))
		}
	}

	return deletes, segDeletes, segInserts
}

func Test_UsageDataMerge(t *testing.T) {
	a := logtail.UsageData{
		Size: 90,
		ObjectAbstract: logtail.ObjectAbstract{
			TotalObjCnt: 1,
			TotalBlkCnt: 2,
			TotalRowCnt: 1,
		},
	}

	b := logtail.UsageData{
		Size: 20,
		ObjectAbstract: logtail.ObjectAbstract{
			TotalObjCnt: 1,
			TotalBlkCnt: 2,
			TotalRowCnt: 1,
		},
	}

	c := logtail.UsageData{
		Size: 10,
		ObjectAbstract: logtail.ObjectAbstract{
			TotalObjCnt: 2,
			TotalBlkCnt: 4,
			TotalRowCnt: 2,
		},
	}

	a.Merge(b, false)
	a.Merge(c, true)

	require.Equal(t, uint64(100), a.Size)
	require.Equal(t, 0, a.TotalObjCnt)
	require.Equal(t, 0, a.TotalBlkCnt)
	require.Equal(t, 0, a.TotalRowCnt)
}

func Test_Objects2Usages(t *testing.T) {
	allocator := atomic.Uint64{}
	allocator.Store(pkgcatalog.MO_RESERVED_MAX + 1)

	accCnt, dbCnt, tblCnt := 10, 10, 10
	usages := logtail.MockUsageData(accCnt, dbCnt, tblCnt, &allocator)

	insertIndexes := make(map[int]struct{})
	for i := 0; i < len(usages); i++ {
		insertIndexes[i] = struct{}{}
	}

	_, _, inserts := mockDeletesAndInserts(usages, nil, nil, nil, insertIndexes)

	turnA := logtail.Objects2Usages(inserts[:len(inserts)/2], false)
	for i := range turnA {
		require.Equal(t, uint64(inserts[i].Size()), turnA[i].Size)
		require.Equal(t, 0, turnA[i].TotalObjCnt)
		require.Equal(t, 0, turnA[i].TotalBlkCnt)
		require.Equal(t, 0, turnA[i].TotalRowCnt)
	}

	turnB := logtail.Objects2Usages(inserts[len(inserts)/2:], true)
	offset := len(inserts) / 2
	for i := range turnB {
		require.Equal(t, uint64(inserts[i+offset].Size()), turnB[i].Size)
		require.Equal(t, 1, turnB[i].TotalObjCnt)
		require.Equal(t, int(inserts[i+offset].BlkCnt()), turnB[i].TotalBlkCnt)
		require.Equal(t, int(inserts[i+offset].Rows()), turnB[i].TotalRowCnt)
	}
}
