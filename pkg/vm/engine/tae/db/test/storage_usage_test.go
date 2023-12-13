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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"
	"unsafe"
)

func createAndWriteSingleNASegment(t *testing.T, ctx context.Context,
	rel handle.Relation, fs *objectio.ObjectFS) {
	segHandle, err := rel.CreateNonAppendableSegment(false)
	require.Nil(t, err)

	segEntry := segHandle.GetMeta().(*catalog.SegmentEntry)
	segEntry.SetSorted()

	schema := rel.Schema().(*catalog.Schema)
	vecs := make([]containers.Vector, len(schema.ColDefs))
	seqNums := make([]uint16, len(schema.ColDefs))
	// mock data for segment
	writtenBatches := make([]*containers.Batch, 0, len(schema.ColDefs))
	for idx, def := range schema.ColDefs {
		vecs[idx] = containers.MockVector(types.T_uint64.ToType(), 100, false, nil)
		seqNums[idx] = def.SeqNum
		writtenBatches = append(writtenBatches, containers.NewBatch())
	}

	for idx := range vecs {
		writtenBatches[idx].AddVector(schema.ColDefs[idx].Name, vecs[idx])
	}

	blk, err := segHandle.CreateNonAppendableBlock(new(objectio.CreateBlockOpt).WithFileIdx(0).WithBlkIdx(uint16(0)))
	require.Nil(t, err)

	name := objectio.BuildObjectName(&segEntry.ID, 0)
	writer, err := blockio.NewBlockWriterNew(fs.Service, name, 0, []uint16{0})
	require.Nil(t, err)

	for _, bat := range writtenBatches {
		_, err = writer.WriteBatch(containers.ToCNBatch(bat))
		require.Nil(t, err)
	}

	writtenBlocks, _, err := writer.Sync(ctx)
	require.Nil(t, err)

	for i, block := range writtenBlocks {
		metaLoc := blockio.EncodeLocation(name, block.GetExtent(), uint32(writtenBatches[i].Length()), block.GetID())
		err = blk.UpdateMetaLoc(metaLoc)
		require.Nil(t, err)

		err = blk.GetMeta().(*catalog.BlockEntry).GetBlockData().Init()
		require.Nil(t, err)
	}
}

// create segments for tables
func createAndWriteBatchNASegment(t *testing.T, ctx context.Context, segCnts []int,
	rels []handle.Relation, fs *objectio.ObjectFS) {

	for idx, rel := range rels {
		for i := 0; i < segCnts[idx]; i++ {
			createAndWriteSingleNASegment(t, ctx, rel, fs)
		}
	}
}

func createTables(t *testing.T, txn txnif.AsyncTxn, dbName string, colCnt int, tblCnt int) []handle.Relation {
	db, err := txn.CreateDatabase(dbName, "", "")
	assert.Nil(t, err)

	rels := make([]handle.Relation, tblCnt)
	for i := 0; i < tblCnt; i++ {
		schema := catalog.MockSchemaAll(colCnt, 0)
		rel, err := db.CreateRelation(schema)
		assert.Nil(t, err)
		rels[i] = rel
	}

	return rels
}

func createTAE(t *testing.T, ctx context.Context) *db.DB {
	return testutil.InitTestDB(ctx, "logtail", t, nil)
}

func createTablesAndSegments(
	t *testing.T, ctx context.Context,
	tae *db.DB, dbName string, tblCnt int) ([]int, error) {
	// table count
	relCnt := tblCnt
	naSegCnts := make([]int, relCnt)
	for i := 0; i < relCnt; i++ {
		// generating the count of non appendable segment for each table
		naSegCnts[i] = rand.Int()%50 + 1
	}

	txn, err := tae.StartTxn(nil)
	if err != nil {
		return nil, err
	}

	defer txn.Commit(context.Background())

	rels := createTables(t, txn, dbName, 10, relCnt)
	createAndWriteBatchNASegment(t, ctx, naSegCnts, rels, tae.Runtime.Fs)

	return naSegCnts, nil
}

// test plan:
//  1. test if the `fillSEGStorageUsageBat` work as expected
//  2. benchmark the `fillSEGStorageUsageBat`
func Test_FillUsageBatOfIncrement(t *testing.T) {
	ctx := context.Background()

	tae := createTAE(t, ctx)
	defer tae.Close()

	tblCnt := 10
	segCnts, err := createTablesAndSegments(t, ctx, tae, "testdb", tblCnt)
	require.Nil(t, err)

	collector := logtail.NewIncrementalCollector(types.TS{}, types.MaxTs())
	collector.BlockFn = nil
	collector.DatabaseFn = nil
	collector.TableFn = nil
	collector.SegmentFn = collector.VisitSeg

	tae.Catalog.RecurLoop(collector)

	totalSegCnt := 0
	for i := range segCnts {
		totalSegCnt += segCnts[i]
	}

	// should collect all segment insert events
	require.Equal(t, totalSegCnt, len(collector.Usage.SegInserts))
	require.Equal(t, 0, len(collector.Usage.Deletes))
	require.Equal(t, 0, len(collector.Usage.SegDeletes))

	logtail.FillUsageBatOfIncremental(collector)

	// after FillUsageBatOfIncremental done, all non appendable segments should
	// be loaded
	collector.SegmentFn = func(segment *catalog.SegmentEntry) error {
		require.NotNil(t, segment.GetFirstBlkEntry())
		if !segment.IsAppendable() {
			require.Equal(t, true, segment.Stat.GetLoaded())
			require.NotEqual(t, int(0), segment.Stat.GetOriginSize())
			require.NotEqual(t, int(0), segment.Stat.GetCompSize())
			require.Equal(t, 0, segment.Stat.GetRows())
			require.Equal(t, 0, segment.Stat.GetRemainingRows())
		}

		return nil
	}

	tae.Catalog.RecurLoop(collector)

	storageUsageBat := collector.OrphanData().GetBatches()[logtail.StorageUsageInsIDX]
	sizeVec := storageUsageBat.GetVectorByName(logtail.CheckpointMetaAttr_ObjectSize)

	logutil.Info(storageUsageBat.String())

	// should generate one size record for each table
	require.Equal(t, tblCnt, sizeVec.Length())

	require.Equal(t, tblCnt, logtail.GetTNUsageMemo().CacheLen())

}

// Benchmark_FillSEGStorageUsageBatOfIncrement-12  2  635291604 ns/op
func Benchmark_FillSEGStorageUsageBatOfIncrement(b *testing.B) {
	ctx := context.Background()

	t := &testing.T{}
	tae := createTAE(t, ctx)
	defer tae.Close()

	txn, err := tae.StartTxn(nil)
	require.Nil(t, err)

	rels := createTables(t, txn, "testdb", 10, 1)

	collector := logtail.NewIncrementalCollector(types.TS{}, types.MaxTs())

	for i := 0; i < b.N; i++ {
		createAndWriteBatchNASegment(t, ctx, []int{100}, rels, tae.Runtime.Fs)
		logtail.FillUsageBatOfIncremental(collector)
	}
}

func createCkpAndWriteDown(t *testing.T, ctx context.Context, tae *db.DB, cnt int, oldVersion bool) (
	[]*checkpoint.CheckpointEntry, []handle.Relation) {
	txn, err := tae.StartTxn(nil)
	require.Nil(t, err)

	defer txn.Commit(context.Background())

	// create 3 tables, and each table has 10 columns
	rels := createTables(t, txn, "testdb", 10, 3)
	var entries []*checkpoint.CheckpointEntry
	for i := 0; i < cnt; i++ {
		// 3 tables, each table has 10 non-appendable segment
		createAndWriteBatchNASegment(t, ctx, []int{10, 10, 10}, rels, tae.Runtime.Fs)
		collector := logtail.NewIncrementalCollector(types.TS{}, types.MaxTs())

		logtail.FillUsageBatOfIncremental(collector)

		tae.Catalog.RecurLoop(collector)
		incrCkpData := collector.OrphanData()
		defer incrCkpData.Close()

		cnLocation, tnLocation, _, err := incrCkpData.WriteTo(tae.Runtime.Fs.Service,
			logtail.DefaultCheckpointBlockRows, logtail.DefaultCheckpointSize)
		require.Nil(t, err)

		entry := checkpoint.NewCheckpointEntry(types.TS{}, types.TS{}, checkpoint.ET_Incremental)
		entry.SetLocation(cnLocation, tnLocation)

		if oldVersion {
			entry.SetVersion(logtail.CheckpointVersion1)
		} else {
			entry.SetVersion(logtail.CheckpointVersion9)
		}

		entries = append(entries, entry)
	}
	return entries, rels
}

func Test_FillSEGStorageUsageBatOfGlobal(t *testing.T) {
	ctx := context.Background()

	t.Run("current version", func(t *testing.T) {
		tae := createTAE(t, ctx)
		defer tae.Close()

		_, rels := createCkpAndWriteDown(t, ctx, tae, 10, false)
		collector := logtail.NewGlobalCollector(types.TS{}, 0)

		// mark the first table as deleted
		//deletes := collector.GetDeletes()
		//deletes[logtail.UsageTblID][rels[0].ID()] = struct{}{}

		logtail.FillUsageBatOfGlobal(collector)

		data := collector.OrphanData()
		defer data.Close()

		bat := data.GetBatches()[logtail.StorageUsageInsIDX]
		sizeVec := bat.GetVectorByName(logtail.CheckpointMetaAttr_ObjectSize)

		require.Equal(t, len(rels)-1, sizeVec.Length())
	})

	t.Run("old version", func(t *testing.T) {
		tae := createTAE(t, ctx)
		defer tae.Close()

		_, rels := createCkpAndWriteDown(t, ctx, tae, 10, true)
		collector := logtail.NewGlobalCollector(types.TS{}, 0)

		// mark all tables as deleted
		//deletes := collector.GetDeletes()
		//for i := 0; i < len(rels); i++ {
		//	deletes[logtail.UsageTblID][rels[i].ID()] = struct{}{}
		//}

		logtail.FillUsageBatOfGlobal(collector)

		data := collector.OrphanData()
		defer data.Close()

		bat := data.GetBatches()[logtail.StorageUsageInsIDX]
		sizeVec := bat.GetVectorByName(logtail.CheckpointMetaAttr_ObjectSize)

		// when existing old checkpoints, it will traverse the catalog
		// so the finial number of records should be len(rels)
		require.Equal(t, len(rels), sizeVec.Length())
	})

}

func Test_StorageUsageCache(t *testing.T) {
	// new cache with no option
	cache := logtail.NewStorageUsageCache(logtail.WithLazyThreshold(1))
	require.True(t, cache.IsExpired())

	accCnt, dbCnt, tblCnt := 10, 10, 10
	usages := logtail.MockUsageData(accCnt, dbCnt, tblCnt)
	for idx := range usages {
		cache.Update(usages[idx])
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
					require.True(t, usages[idx].TblId >= usages[idx].TblId)
				}
			}
		}
	}

	// 3. test gather account size
	{
		totalSize := int64(0)
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

		require.Equal(t, int64(0), totalSize)

		size := int64(0)
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
	dbDelRanges []int, tblDelRanges []int,
	segDelRanges []int, segInsRanges []int) (
	[]interface{}, []*catalog.SegmentEntry, []*catalog.SegmentEntry) {
	var deletes []interface{}
	var segInserts []*catalog.SegmentEntry
	var segDeletes []*catalog.SegmentEntry

	// mock deletes, inserts
	{
		// db deletes
		for idx := dbDelRanges[0]; idx < dbDelRanges[1]; idx++ {
			deletes = append(deletes,
				catalog.MockDBEntryWithAccInfo(usages[idx].AccId, usages[idx].DbId))
		}

		// tbl deletes
		for idx := tblDelRanges[0]; idx < tblDelRanges[1]; idx++ {
			deletes = append(deletes,
				catalog.MockTableEntryWithDB(
					catalog.MockDBEntryWithAccInfo(
						usages[idx].AccId, usages[idx].DbId), usages[idx].TblId))
		}

		// segment deletes
		for idx := segDelRanges[0]; idx < segDelRanges[1]; idx++ {
			segDeletes = append(segDeletes,
				catalog.MockSegEntryWithTbl(
					catalog.MockTableEntryWithDB(
						catalog.MockDBEntryWithAccInfo(usages[idx].AccId, usages[idx].DbId),
						usages[idx].TblId), usages[idx].Size))
		}

		// segment inserts
		for idx := segInsRanges[0]; idx < segInsRanges[1]; idx++ {
			segInserts = append(segInserts,
				catalog.MockSegEntryWithTbl(
					catalog.MockTableEntryWithDB(
						catalog.MockDBEntryWithAccInfo(usages[idx].AccId, usages[idx].DbId),
						usages[idx].TblId), usages[idx].Size))
		}
	}

	return deletes, segDeletes, segInserts
}

func Test_FillUsageBatOfIncremental(t *testing.T) {
	accCnt, dbCnt, tblCnt := 10, 10, 10
	usages := logtail.MockUsageData(accCnt, dbCnt, tblCnt)

	dbDelRanges, tblDelRanges := []int{0, 5}, []int{2, 7}
	segDelRanges, segInsRanges := []int{4, 9}, []int{0, len(usages)}

	deletes, segDeletes, segInserts := mockDeletesAndInserts(
		usages, dbDelRanges, tblDelRanges, segDelRanges, segInsRanges)

	iCollector := logtail.NewIncrementalCollector(types.TS{}, types.MaxTs())
	iCollector.Usage.Deletes = deletes
	iCollector.Usage.SegDeletes = segDeletes
	iCollector.Usage.SegInserts = segInserts

	logtail.FillUsageBatOfIncremental(iCollector)

	memo := logtail.GetTNUsageMemo()

	// test exist
	{
	}

}
