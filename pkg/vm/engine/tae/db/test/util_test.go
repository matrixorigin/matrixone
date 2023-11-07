// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/stretchr/testify/assert"
)

type testRows struct {
	id int
}

func (r *testRows) Length() int               { return 1 }
func (r *testRows) Window(_, _ int) *testRows { return nil }

func createBlockFn[R any](_ R) *model.TimedSliceBlock[R] {
	ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	return model.NewTimedSliceBlock[R](ts)
}

func TestAOT1(t *testing.T) {
	aot := model.NewAOT(
		10,
		createBlockFn[*testRows],
		func(a, b *model.TimedSliceBlock[*testRows]) bool {
			return a.BornTS.Less(b.BornTS)
		})
	for i := 0; i < 30; i++ {
		rows := &testRows{id: i}
		err := aot.Append(rows)
		assert.NoError(t, err)
	}
	t.Log(aot.BlockCount())
}

func TestAOT2(t *testing.T) {
	schema := catalog.MockSchemaAll(14, 3)
	factory := func(_ *containers.Batch) *model.BatchBlock {
		id := common.NextGlobalSeqNum()
		return model.NewBatchBlock(id, schema.Attrs(), schema.Types(), containers.Options{})
	}
	aot := model.NewAOT(
		10,
		factory,
		func(a, b *model.BatchBlock) bool {
			return a.ID < b.ID
		})
	defer aot.Close()

	bat := catalog.MockBatch(schema, 42)
	defer bat.Close()

	assert.NoError(t, aot.Append(bat))
	t.Log(aot.BlockCount())
	assert.Equal(t, 5, aot.BlockCount())
	rows := 0
	fn := func(block *model.BatchBlock) bool {
		rows += block.Length()
		return true
	}
	aot.Scan(fn)
	assert.Equal(t, 42, rows)
}

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

func createTables(t *testing.T, tae *db.DB, dbName string, colCnt int, tblCnt int) []handle.Relation {
	txn, _ := tae.StartTxn(nil)
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

func createTablesAndSegments(t *testing.T, ctx context.Context, tae *db.DB, dbName string, tblCnt int) {
	// table count
	relCnt := tblCnt
	naSegCnts := make([]int, relCnt)
	for i := 0; i < relCnt; i++ {
		// generating the count of non appendable segment for each table
		naSegCnts[i] = rand.Int()%50 + 1
	}

	rels := createTables(t, tae, dbName, 10, relCnt)
	createAndWriteBatchNASegment(t, ctx, naSegCnts, rels, tae.Runtime.Fs)
}

// test plan:
//  1. test if the `fillSEGStorageUsageBat` work as expected
//  2. benchmark the `fillSEGStorageUsageBat`
func Test_FillUsageBatOfIncrement(t *testing.T) {
	ctx := context.Background()

	tae := createTAE(t, ctx)
	defer tae.Close()

	createTablesAndSegments(t, ctx, tae, "testdb", 10)
	collector := logtail.NewIncrementalCollector(types.TS{}, types.MaxTs())
	collector.BlockFn = nil
	collector.DatabaseFn = nil
	collector.TableFn = nil
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

	logtail.FillUsageBatOfIncremental(tae.Catalog, collector, tae.Runtime.Fs.Service)

	// after `FillUsageBatOfIncremental`, all non-appendable segment should be loaded
	tae.Catalog.RecurLoop(collector)

	storageUsageBat := collector.OrphanData().GetBatches()[logtail.SEGStorageUsageIDX]
	sizeVec := storageUsageBat.GetVectorByName(logtail.CheckpointMetaAttr_ObjectSize)

	logutil.Info(storageUsageBat.String())

	// should generate one size record for each table
	require.Equal(t, 10, sizeVec.Length())
}

// Benchmark_FillSEGStorageUsageBatOfIncrement-12  2  635291604 ns/op
func Benchmark_FillSEGStorageUsageBatOfIncrement(b *testing.B) {
	ctx := context.Background()

	t := &testing.T{}
	tae := createTAE(t, ctx)
	defer tae.Close()
	rels := createTables(t, tae, "testdb", 10, 1)

	collector := logtail.NewIncrementalCollector(types.TS{}, types.MaxTs())

	for i := 0; i < b.N; i++ {
		createAndWriteBatchNASegment(t, ctx, []int{100}, rels, tae.Runtime.Fs)
		logtail.FillUsageBatOfIncremental(tae.Catalog, collector, tae.Runtime.Fs.Service)
	}
}

func createCkpAndWriteDown(t *testing.T, ctx context.Context, tae *db.DB, cnt int, oldVersion bool) (
	[]*checkpoint.CheckpointEntry, []handle.Relation) {
	// create 3 tables, and each table has 10 columns
	rels := createTables(t, tae, "testdb", 10, 3)
	var entries []*checkpoint.CheckpointEntry
	for i := 0; i < cnt; i++ {
		// 3 tables, each table has 10 non-appendable segment
		createAndWriteBatchNASegment(t, ctx, []int{10, 10, 10}, rels, tae.Runtime.Fs)
		collector := logtail.NewIncrementalCollector(types.TS{}, types.MaxTs())

		logtail.FillUsageBatOfIncremental(tae.Catalog, collector, tae.Runtime.Fs.Service)

		tae.Catalog.RecurLoop(collector)
		incrCkpData := collector.OrphanData()
		defer incrCkpData.Close()

		cnLocation, tnLocation, err := incrCkpData.WriteTo(tae.Runtime.Fs.Service,
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

func checkpointMetaInfoFactory(entries []*checkpoint.CheckpointEntry) []*logtail.CkpLocVers {
	ret := make([]*logtail.CkpLocVers, 0)
	for idx := range entries {
		ret = append(ret, &logtail.CkpLocVers{
			Location: entries[idx].GetLocation(),
			Version:  entries[idx].GetVersion(),
		})
	}
	return ret
}

func Test_FillSEGStorageUsageBatOfGlobal(t *testing.T) {
	ctx := context.Background()

	t.Run("current version", func(t *testing.T) {
		tae := createTAE(t, ctx)
		defer tae.Close()

		entries, rels := createCkpAndWriteDown(t, ctx, tae, 10, false)
		collector := logtail.NewGlobalCollector(types.TS{}, 0)

		// mark the first table as deleted
		deletes := collector.GetDeletes()
		deletes[logtail.UsageTblID][rels[0].ID()] = struct{}{}

		logtail.FillUsageBatOfGlobal(tae.Catalog, collector, tae.Runtime.Fs.Service, checkpointMetaInfoFactory(entries))

		data := collector.OrphanData()
		defer data.Close()

		bat := data.GetBatches()[logtail.SEGStorageUsageIDX]
		sizeVec := bat.GetVectorByName(logtail.CheckpointMetaAttr_ObjectSize)

		require.Equal(t, len(rels)-1, sizeVec.Length())
	})

	t.Run("old version", func(t *testing.T) {
		tae := createTAE(t, ctx)
		defer tae.Close()

		entries, rels := createCkpAndWriteDown(t, ctx, tae, 10, true)
		collector := logtail.NewGlobalCollector(types.TS{}, 0)

		// mark all tables as deleted
		deletes := collector.GetDeletes()
		for i := 0; i < len(rels); i++ {
			deletes[logtail.UsageTblID][rels[i].ID()] = struct{}{}
		}

		logtail.FillUsageBatOfGlobal(tae.Catalog, collector, tae.Runtime.Fs.Service, checkpointMetaInfoFactory(entries))

		data := collector.OrphanData()
		defer data.Close()

		bat := data.GetBatches()[logtail.SEGStorageUsageIDX]
		sizeVec := bat.GetVectorByName(logtail.CheckpointMetaAttr_ObjectSize)

		// when existing old checkpoints, it will traverse the catalog
		// so the finial number of records should be len(rels)
		require.Equal(t, len(rels), sizeVec.Length())
	})

}

func Test_LoadSpecifiedCkpData(t *testing.T) {
	ctx := context.Background()
	tae := createTAE(t, ctx)
	defer tae.Close()

	entries, _ := createCkpAndWriteDown(t, ctx, tae, 1, true)
	require.Equal(t, 1, len(entries))

	// out of bound
	data, err := logtail.LoadSpecifiedCkpBatch(ctx, entries[0].GetLocation(),
		tae.Runtime.Fs.Service, logtail.CheckpointVersion9, logtail.MaxIDX)
	require.NotNil(t, err)
	require.Nil(t, data)

	// invalid version
	data, err = logtail.LoadSpecifiedCkpBatch(ctx, entries[0].GetLocation(),
		tae.Runtime.Fs.Service, logtail.CheckpointVersion9, logtail.MaxIDX)
	require.NotNil(t, err)
	require.Nil(t, data)

	// invalid location
	data, err = logtail.LoadSpecifiedCkpBatch(ctx, objectio.BuildLocation([]byte("test"), objectio.Extent{}, 0, 0),
		tae.Runtime.Fs.Service, logtail.CheckpointVersion9, logtail.MaxIDX)
	require.NotNil(t, err)
	require.Nil(t, data)

	data, err = logtail.LoadSpecifiedCkpBatch(ctx, entries[0].GetLocation(),
		tae.Runtime.Fs.Service, logtail.CheckpointVersion9, logtail.SEGStorageUsageIDX)
	require.Nil(t, err)
	require.NotNil(t, data)

	data.Close()
}
