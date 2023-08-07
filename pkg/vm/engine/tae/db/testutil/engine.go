// Copyright 2021 Matrix Origin
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

package testutil

import (
	"context"
	"testing"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

const (
	DefaultTestDB = "db"
)

type TestEngine struct {
	*db.DB
	t        *testing.T
	schema   *catalog.Schema
	tenantID uint32 // for almost tests, userID and roleID is not important
}

func NewTestEngine(
	ctx context.Context,
	moduleName string,
	t *testing.T,
	opts *options.Options,
) *TestEngine {
	blockio.Start()
	db := InitTestDB(ctx, moduleName, t, opts)
	return &TestEngine{
		DB: db,
		t:  t,
	}
}

func (e *TestEngine) BindSchema(schema *catalog.Schema) { e.schema = schema }

func (e *TestEngine) BindTenantID(tenantID uint32) { e.tenantID = tenantID }

func (e *TestEngine) Restart(ctx context.Context) {
	_ = e.DB.Close()
	var err error
	e.DB, err = db.Open(ctx, e.Dir, e.Opts)
	// only ut executes this checker
	e.DB.DiskCleaner.AddChecker(
		func(item any) bool {
			min := e.DB.TxnMgr.MinTSForTest()
			ckp := item.(*checkpoint.CheckpointEntry)
			//logutil.Infof("min: %v, checkpoint: %v", min.ToString(), checkpoint.GetStart().ToString())
			return !ckp.GetEnd().GreaterEq(min)
		})
	assert.NoError(e.t, err)
}

func (e *TestEngine) Close() error {
	err := e.DB.Close()
	blockio.Stop()
	blockio.ResetPipeline()
	return err
}

func (e *TestEngine) CreateRelAndAppend(bat *containers.Batch, createDB bool) (handle.Database, handle.Relation) {
	return CreateRelationAndAppend(e.t, e.tenantID, e.DB, DefaultTestDB, e.schema, bat, createDB)
}

func (e *TestEngine) CheckRowsByScan(exp int, applyDelete bool) {
	txn, rel := e.GetRelation()
	CheckAllColRowsByScan(e.t, rel, exp, applyDelete)
	assert.NoError(e.t, txn.Commit(context.Background()))
}
func (e *TestEngine) DropRelation(t *testing.T) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	db, err := txn.GetDatabase(DefaultTestDB)
	assert.NoError(t, err)
	_, err = db.DropRelationByName(e.schema.Name)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
}
func (e *TestEngine) GetRelation() (txn txnif.AsyncTxn, rel handle.Relation) {
	return GetRelation(e.t, e.tenantID, e.DB, DefaultTestDB, e.schema.Name)
}
func (e *TestEngine) GetRelationWithTxn(txn txnif.AsyncTxn) (rel handle.Relation) {
	return GetRelationWithTxn(e.t, txn, DefaultTestDB, e.schema.Name)
}

func (e *TestEngine) CompactBlocks(skipConflict bool) {
	CompactBlocks(e.t, e.tenantID, e.DB, DefaultTestDB, e.schema, skipConflict)
}

func (e *TestEngine) MergeBlocks(skipConflict bool) {
	MergeBlocks(e.t, e.tenantID, e.DB, DefaultTestDB, e.schema, skipConflict)
}

func (e *TestEngine) GetDB(name string) (txn txnif.AsyncTxn, db handle.Database) {
	txn, err := e.DB.StartTxn(nil)
	txn.BindAccessInfo(e.tenantID, 0, 0)
	assert.NoError(e.t, err)
	db, err = txn.GetDatabase(name)
	assert.NoError(e.t, err)
	return
}

func (e *TestEngine) GetTestDB() (txn txnif.AsyncTxn, db handle.Database) {
	return e.GetDB(DefaultTestDB)
}

func (e *TestEngine) DoAppend(bat *containers.Batch) {
	txn, rel := e.GetRelation()
	err := rel.Append(context.Background(), bat)
	assert.NoError(e.t, err)
	assert.NoError(e.t, txn.Commit(context.Background()))
}

func (e *TestEngine) DoAppendWithTxn(bat *containers.Batch, txn txnif.AsyncTxn, skipConflict bool) (err error) {
	rel := e.GetRelationWithTxn(txn)
	err = rel.Append(context.Background(), bat)
	if !skipConflict {
		assert.NoError(e.t, err)
	}
	return
}

func (e *TestEngine) TryAppend(bat *containers.Batch) {
	txn, err := e.DB.StartTxn(nil)
	txn.BindAccessInfo(e.tenantID, 0, 0)
	assert.NoError(e.t, err)
	db, err := txn.GetDatabase(DefaultTestDB)
	assert.NoError(e.t, err)
	rel, err := db.GetRelationByName(e.schema.Name)
	if err != nil {
		_ = txn.Rollback(context.Background())
		return
	}

	err = rel.Append(context.Background(), bat)
	if err != nil {
		_ = txn.Rollback(context.Background())
		return
	}
	_ = txn.Commit(context.Background())
}
func (e *TestEngine) DeleteAll(skipConflict bool) error {
	txn, rel := e.GetRelation()
	it := rel.MakeBlockIt()
	for it.Valid() {
		blk := it.GetBlock()
		defer blk.Close()
		view, err := blk.GetColumnDataByName(context.Background(), catalog.PhyAddrColumnName)
		assert.NoError(e.t, err)
		defer view.Close()
		view.ApplyDeletes()
		err = rel.DeleteByPhyAddrKeys(view.GetData())
		assert.NoError(e.t, err)
		it.Next()
	}
	// CheckAllColRowsByScan(e.t, rel, 0, true)
	err := txn.Commit(context.Background())
	if !skipConflict {
		CheckAllColRowsByScan(e.t, rel, 0, true)
		assert.NoError(e.t, err)
	}
	return err
}

func (e *TestEngine) Truncate() {
	txn, db := e.GetTestDB()
	_, err := db.TruncateByName(e.schema.Name)
	assert.NoError(e.t, err)
	assert.NoError(e.t, txn.Commit(context.Background()))
}
func (e *TestEngine) GlobalCheckpoint(
	endTs types.TS,
	versionInterval time.Duration,
	enableAndCleanBGCheckpoint bool,
) error {
	if enableAndCleanBGCheckpoint {
		e.DB.BGCheckpointRunner.DisableCheckpoint()
		defer e.DB.BGCheckpointRunner.EnableCheckpoint()
		e.DB.BGCheckpointRunner.CleanPenddingCheckpoint()
	}
	if e.DB.BGCheckpointRunner.GetPenddingIncrementalCount() == 0 {
		testutils.WaitExpect(4000, func() bool {
			flushed := e.DB.BGCheckpointRunner.IsAllChangesFlushed(types.TS{}, endTs, false)
			return flushed
		})
		flushed := e.DB.BGCheckpointRunner.IsAllChangesFlushed(types.TS{}, endTs, true)
		assert.True(e.t, flushed)
	}
	err := e.DB.BGCheckpointRunner.ForceGlobalCheckpoint(endTs, versionInterval)
	assert.NoError(e.t, err)
	return nil
}

func (e *TestEngine) IncrementalCheckpoint(
	end types.TS,
	enableAndCleanBGCheckpoint bool,
	waitFlush bool,
	truncate bool,
) error {
	if enableAndCleanBGCheckpoint {
		e.DB.BGCheckpointRunner.DisableCheckpoint()
		defer e.DB.BGCheckpointRunner.EnableCheckpoint()
		e.DB.BGCheckpointRunner.CleanPenddingCheckpoint()
	}
	if waitFlush {
		testutils.WaitExpect(4000, func() bool {
			flushed := e.DB.BGCheckpointRunner.IsAllChangesFlushed(types.TS{}, end, false)
			return flushed
		})
		flushed := e.DB.BGCheckpointRunner.IsAllChangesFlushed(types.TS{}, end, true)
		assert.True(e.t, flushed)
	}
	err := e.DB.BGCheckpointRunner.ForceIncrementalCheckpoint(end)
	assert.NoError(e.t, err)
	if truncate {
		lsn := e.DB.BGCheckpointRunner.MaxLSNInRange(end)
		entry, err := e.DB.Wal.RangeCheckpoint(1, lsn)
		assert.NoError(e.t, err)
		assert.NoError(e.t, entry.WaitDone())
		testutils.WaitExpect(1000, func() bool {
			return e.Runtime.Scheduler.GetPenddingLSNCnt() == 0
		})
	}
	return nil
}

func (e *TestEngine) TryDeleteByDeltaloc(vals []any) (ok bool, err error) {
	txn, err := e.StartTxn(nil)
	assert.NoError(e.t, err)
	ok, err = e.TryDeleteByDeltalocWithTxn(vals, txn)
	if ok {
		assert.NoError(e.t, txn.Commit(context.Background()))
	} else {
		assert.NoError(e.t, txn.Rollback(context.Background()))
	}
	return
}

func (e *TestEngine) TryDeleteByDeltalocWithTxn(vals []any, txn txnif.AsyncTxn) (ok bool, err error) {
	rel := e.GetRelationWithTxn(txn)

	idOffsetsMap := make(map[common.ID][]uint32)
	for _, val := range vals {
		filter := handle.NewEQFilter(val)
		id, offset, err := rel.GetByFilter(context.Background(), filter)
		assert.NoError(e.t, err)
		offsets, ok := idOffsetsMap[*id]
		if !ok {
			offsets = make([]uint32, 0)
		}
		offsets = append(offsets, offset)
		idOffsetsMap[*id] = offsets
	}

	for id, offsets := range idOffsetsMap {
		seg, err := rel.GetMeta().(*catalog.TableEntry).GetSegmentByID(id.SegmentID())
		assert.NoError(e.t, err)
		blk, err := seg.GetBlockEntryByID(&id.BlockID)
		assert.NoError(e.t, err)
		deltaLoc, err := MockCNDeleteInS3(e.Runtime.Fs, blk.GetBlockData(), e.schema, txn, offsets)
		assert.NoError(e.t, err)
		ok, err = rel.TryDeleteByDeltaloc(&id, deltaLoc)
		assert.NoError(e.t, err)
		if !ok {
			return ok, err
		}
	}
	ok = true
	return
}

func InitTestDB(
	ctx context.Context,
	moduleName string,
	t *testing.T,
	opts *options.Options,
) *db.DB {
	dir := testutils.InitTestEnv(moduleName, t)
	db, _ := db.Open(ctx, dir, opts)
	// only ut executes this checker
	db.DiskCleaner.AddChecker(
		func(item any) bool {
			min := db.TxnMgr.MinTSForTest()
			ckp := item.(*checkpoint.CheckpointEntry)
			//logutil.Infof("min: %v, checkpoint: %v", min.ToString(), checkpoint.GetStart().ToString())
			return !ckp.GetEnd().GreaterEq(min)
		})
	return db
}

func writeIncrementalCheckpoint(t *testing.T, start, end types.TS, c *catalog.Catalog, fs fileservice.FileService) objectio.Location {
	factory := logtail.IncrementalCheckpointDataFactory(start, end)
	data, err := factory(c)
	assert.NoError(t, err)
	defer data.Close()
	location, err := data.WriteTo(fs)
	assert.NoError(t, err)
	return location
}

func dnReadCheckpoint(t *testing.T, location objectio.Location, fs fileservice.FileService) *logtail.CheckpointData {
	reader, err := blockio.NewObjectReader(fs, location)
	assert.NoError(t, err)
	data := logtail.NewCheckpointData()
	err = data.ReadFrom(
		context.Background(),
		logtail.CheckpointCurrentVersion,
		location,
		reader,
		fs,
		common.DefaultAllocator,
	)
	assert.NoError(t, err)
	return data
}

func cnReadCheckpoint(t *testing.T, tid uint64, location objectio.Location, fs fileservice.FileService) (ins, del, cnIns, segDel *api.Batch) {
	reader, err := blockio.NewObjectReader(fs, location)
	assert.NoError(t, err)
	data := logtail.NewCNCheckpointData()
	bats, err := data.ReadFromData(
		context.Background(),
		tid,
		location,
		reader,
		logtail.CheckpointCurrentVersion,
		common.DefaultAllocator,
	)
	assert.NoError(t, err)
	ins, del, cnIns, segDel, err = data.GetTableDataFromBats(tid, bats)
	assert.NoError(t, err)
	return
}

func checkDNCheckpointData(t *testing.T, data *logtail.CheckpointData, start, end types.TS, c *catalog.Catalog) {
	factory := logtail.IncrementalCheckpointDataFactory(start, end)
	data2, err := factory(c)
	assert.NoError(t, err)
	defer data2.Close()

	bats1 := data.GetBatches()
	bats2 := data2.GetBatches()
	assert.Equal(t, len(bats1), len(bats2))
	for i, bat := range bats1 {
		// skip metabatch
		if i == 0 {
			continue
		}
		bat2 := bats2[i]
		// t.Logf("check bat %d", i)
		isBatchEqual(t, bat, bat2)
	}
}

func getBatchLength(bat *containers.Batch) int {
	length := 0
	for _, vec := range bat.Vecs {
		if vec.Length() > length {
			length = vec.Length()
		}
	}
	return length
}

func isBatchEqual(t *testing.T, bat1, bat2 *containers.Batch) {
	assert.Equal(t, getBatchLength(bat1), getBatchLength(bat2))
	assert.Equal(t, len(bat1.Vecs), len(bat2.Vecs))
	for i := 0; i < getBatchLength(bat1); i++ {
		for j, vec1 := range bat1.Vecs {
			vec2 := bat2.Vecs[j]
			// for commitTS and rowid in checkpoint
			if vec1.Length() == 0 || vec2.Length() == 0 {
				// logutil.Warnf("empty vec attr %v", bat1.Attrs[j])
				continue
			}
			// t.Logf("attr %v, row %d", bat1.Attrs[j], i)
			assert.Equal(t, vec1.Get(i), vec2.Get(i))
		}
	}
}

func isProtoDNBatchEqual(t *testing.T, bat1 *api.Batch, bat2 *containers.Batch) {
	if bat1 == nil {
		if bat2 == nil {
			return
		}
		assert.Equal(t, 0, getBatchLength(bat2))
	} else {
		moIns, err := batch.ProtoBatchToBatch(bat1)
		assert.NoError(t, err)
		dnIns := containers.ToDNBatch(moIns)
		isBatchEqual(t, dnIns, bat2)
	}
}

func checkCNCheckpointData(t *testing.T, tid uint64, ins, del, cnIns, segDel *api.Batch, start, end types.TS, c *catalog.Catalog) {
	if tid == pkgcatalog.MO_DATABASE_ID {
		checkMODatabase(t, ins, del, cnIns, segDel, start, end, c)
	} else if tid == pkgcatalog.MO_TABLES_ID {
		checkMOTables(t, ins, del, cnIns, segDel, start, end, c)
	} else if tid == pkgcatalog.MO_COLUMNS_ID {
		checkMOColumns(t, ins, del, cnIns, segDel, start, end, c)
	} else {
		checkUserTables(t, tid, ins, del, cnIns, segDel, start, end, c)
	}
}

func checkMODatabase(t *testing.T, ins, del, cnIns, segDel *api.Batch, start, end types.TS, c *catalog.Catalog) {
	collector := logtail.NewIncrementalCollector(start, end)
	p := &catalog.LoopProcessor{}
	p.DatabaseFn = collector.VisitDB
	err := c.RecurLoop(p)
	assert.NoError(t, err)
	data2 := collector.OrphanData()
	defer data2.Close()
	ins2, _, del2, _ := data2.GetDBBatchs()

	isProtoDNBatchEqual(t, ins, ins2)
	isProtoDNBatchEqual(t, del, del2)
	assert.Nil(t, cnIns)
	assert.Nil(t, segDel)
}

func checkMOTables(t *testing.T, ins, del, cnIns, segDel *api.Batch, start, end types.TS, c *catalog.Catalog) {
	collector := logtail.NewIncrementalCollector(start, end)
	p := &catalog.LoopProcessor{}
	p.TableFn = collector.VisitTable
	err := c.RecurLoop(p)
	assert.NoError(t, err)
	data2 := collector.OrphanData()
	defer data2.Close()
	ins2, _, _, del2, _ := data2.GetTblBatchs()

	isProtoDNBatchEqual(t, ins, ins2)
	isProtoDNBatchEqual(t, del, del2)
	assert.Nil(t, cnIns)
	assert.Nil(t, segDel)
}

func checkMOColumns(t *testing.T, ins, del, cnIns, segDel *api.Batch, start, end types.TS, c *catalog.Catalog) {
	collector := logtail.NewIncrementalCollector(start, end)
	p := &catalog.LoopProcessor{}
	p.TableFn = collector.VisitTable
	err := c.RecurLoop(p)
	assert.NoError(t, err)
	data2 := collector.OrphanData()
	bats := data2.GetBatches()
	ins2 := bats[logtail.TBLColInsertIDX]
	del2 := bats[logtail.TBLColDeleteIDX]

	isProtoDNBatchEqual(t, ins, ins2)
	isProtoDNBatchEqual(t, del, del2)
	assert.Nil(t, cnIns)
	assert.Nil(t, segDel)
}

func checkUserTables(t *testing.T, tid uint64, ins, del, cnIns, segDel *api.Batch, start, end types.TS, c *catalog.Catalog) {
	collector := logtail.NewIncrementalCollector(start, end)
	p := &catalog.LoopProcessor{}
	p.BlockFn = func(be *catalog.BlockEntry) error {
		if be.GetSegment().GetTable().ID != tid {
			return nil
		}
		return collector.VisitBlk(be)
	}
	p.SegmentFn = func(se *catalog.SegmentEntry) error {
		if se.GetTable().ID != tid {
			return nil
		}
		return collector.VisitSeg(se)
	}
	err := c.RecurLoop(p)
	assert.NoError(t, err)
	data2 := collector.OrphanData()
	bats := data2.GetBatches()
	ins2 := bats[logtail.TBLColInsertIDX]
	del2 := bats[logtail.TBLColDeleteIDX]
	cnIns2 := bats[logtail.BLKCNMetaInsertIDX]
	segDel2 := bats[logtail.SEGDeleteIDX]

	isProtoDNBatchEqual(t, ins, ins2)
	isProtoDNBatchEqual(t, del, del2)
	isProtoDNBatchEqual(t, cnIns, cnIns2)
	isProtoDNBatchEqual(t, segDel, segDel2)
}

func CheckCheckpointReadWrite(t *testing.T, start, end types.TS, c *catalog.Catalog, fs fileservice.FileService) {
	location := writeIncrementalCheckpoint(t, start, end, c, fs)
	dnData := dnReadCheckpoint(t, location, fs)

	checkDNCheckpointData(t, dnData, start, end, c)
	p := &catalog.LoopProcessor{}

	ins, del, cnIns, seg := cnReadCheckpoint(t, pkgcatalog.MO_DATABASE_ID, location, fs)
	checkCNCheckpointData(t, pkgcatalog.MO_DATABASE_ID, ins, del, cnIns, seg, start, end, c)
	ins, del, cnIns, seg = cnReadCheckpoint(t, pkgcatalog.MO_TABLES_ID, location, fs)
	checkCNCheckpointData(t, pkgcatalog.MO_TABLES_ID, ins, del, cnIns, seg, start, end, c)
	ins, del, cnIns, seg = cnReadCheckpoint(t, pkgcatalog.MO_COLUMNS_ID, location, fs)
	checkCNCheckpointData(t, pkgcatalog.MO_COLUMNS_ID, ins, del, cnIns, seg, start, end, c)

	p.TableFn = func(te *catalog.TableEntry) error {
		ins, del, cnIns, seg := cnReadCheckpoint(t, te.ID, location, fs)
		checkCNCheckpointData(t, te.ID, ins, del, cnIns, seg, start, end, c)
		return nil
	}
}
