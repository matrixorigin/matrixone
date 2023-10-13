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
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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
	"github.com/stretchr/testify/require"
)

const (
	DefaultTestDB = "db"
)

type CtxOldVersion struct{}

type TestEngine struct {
	*db.DB
	t        *testing.T
	schema   *catalog.Schema
	tenantID uint32 // for almost tests, userID and roleID is not important
}

func NewTestEngineWithDir(
	ctx context.Context,
	dir string,
	t *testing.T,
	opts *options.Options,
) *TestEngine {
	blockio.Start()
	db := InitTestDBWithDir(ctx, dir, t, opts)
	return &TestEngine{
		DB: db,
		t:  t,
	}
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
func (e *TestEngine) ForceCheckpoint() {
	err := e.BGCheckpointRunner.ForceFlushWithInterval(e.TxnMgr.StatMaxCommitTS(), context.Background(), time.Second*2, time.Millisecond*10)
	assert.NoError(e.t, err)
	err = e.BGCheckpointRunner.ForceIncrementalCheckpoint(e.TxnMgr.StatMaxCommitTS(), false)
	assert.NoError(e.t, err)
}

func (e *TestEngine) ForceLongCheckpoint() {
	err := e.BGCheckpointRunner.ForceFlush(e.TxnMgr.StatMaxCommitTS(), context.Background(), 20*time.Second)
	assert.NoError(e.t, err)
	err = e.BGCheckpointRunner.ForceIncrementalCheckpoint(e.TxnMgr.StatMaxCommitTS(), false)
	assert.NoError(e.t, err)
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
	schema := rel.GetMeta().(*catalog.TableEntry).GetLastestSchema()
	pkName := schema.GetPrimaryKey().Name
	it := rel.MakeBlockIt()
	for it.Valid() {
		blk := it.GetBlock()
		defer blk.Close()
		view, err := blk.GetColumnDataByName(context.Background(), catalog.PhyAddrColumnName)
		assert.NoError(e.t, err)
		defer view.Close()
		view.ApplyDeletes()
		pkView, err := blk.GetColumnDataByName(context.Background(), pkName)
		assert.NoError(e.t, err)
		defer pkView.Close()
		pkView.ApplyDeletes()
		err = rel.DeleteByPhyAddrKeys(view.GetData(), pkView.GetData())
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
	err := e.DB.BGCheckpointRunner.ForceIncrementalCheckpoint(end, false)
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

func InitTestDBWithDir(
	ctx context.Context,
	dir string,
	t *testing.T,
	opts *options.Options,
) *db.DB {
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

func writeIncrementalCheckpoint(
	t *testing.T,
	start, end types.TS,
	c *catalog.Catalog,
	checkpointBlockRows int,
	fs fileservice.FileService,
) (objectio.Location, objectio.Location) {
	factory := logtail.IncrementalCheckpointDataFactory(start, end)
	data, err := factory(c)
	assert.NoError(t, err)
	defer data.Close()
	cnLocation, tnLocation, err := data.WriteTo(fs, checkpointBlockRows)
	assert.NoError(t, err)
	return cnLocation, tnLocation
}

func tnReadCheckpoint(t *testing.T, location objectio.Location, fs fileservice.FileService) *logtail.CheckpointData {
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
func cnReadCheckpoint(t *testing.T, tid uint64, location objectio.Location, fs fileservice.FileService) (ins, del, cnIns, segDel *api.Batch, cb []func()) {
	ins, del, cnIns, segDel, cb = cnReadCheckpointWithVersion(t, tid, location, fs, logtail.CheckpointCurrentVersion)
	return
}
func cnReadCheckpointWithVersion(t *testing.T, tid uint64, location objectio.Location, fs fileservice.FileService, ver uint32) (ins, del, cnIns, segDel *api.Batch, cb []func()) {
	locs := make([]string, 0)
	locs = append(locs, location.String())
	locs = append(locs, strconv.Itoa(int(ver)))
	locations := strings.Join(locs, ";")
	entries, cb, err := logtail.LoadCheckpointEntries(
		context.Background(),
		locations,
		tid,
		"tbl",
		0,
		"db",
		common.DefaultAllocator,
		fs,
	)
	assert.NoError(t, err)
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		if e.TableName == fmt.Sprintf("_%d_seg", tid) {
			segDel = e.Bat
		} else if e.EntryType == api.Entry_Delete {
			del = e.Bat
			if tid != pkgcatalog.MO_DATABASE_ID && tid != pkgcatalog.MO_TABLES_ID && tid != pkgcatalog.MO_COLUMNS_ID {
				cnIns = entries[i-1].Bat
				i--
			}
		} else {
			ins = e.Bat
		}
	}
	for _, c := range cb {
		c()
	}
	return
}

func checkTNCheckpointData(ctx context.Context, t *testing.T, data *logtail.CheckpointData, start, end types.TS, c *catalog.Catalog) {
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
		isBatchEqual(ctx, t, bat, bat2)
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

func isBatchEqual(ctx context.Context, t *testing.T, bat1, bat2 *containers.Batch) {
	oldver := -1
	if ver := ctx.Value(CtxOldVersion{}); ver != nil {
		oldver = ver.(int)
	}
	require.Equal(t, getBatchLength(bat1), getBatchLength(bat2))
	require.Equal(t, len(bat1.Vecs), len(bat2.Vecs))
	for i := 0; i < getBatchLength(bat1); i++ {
		for j, vec1 := range bat1.Vecs {
			vec2 := bat2.Vecs[j]
			if vec1.Length() == 0 || vec2.Length() == 0 {
				// for commitTS and rowid in checkpoint
				// logutil.Warnf("empty vec attr %v", bat1.Attrs[j])
				continue
			}
			if oldver >= 0 && oldver <= int(logtail.CheckpointVersion5) && // read old version checkpoint
				logtail.CheckpointCurrentVersion > logtail.CheckpointVersion5 && // check on new version
				bat1.Attrs[j] == pkgcatalog.BlockMeta_MemTruncPoint {
				// memTruncatePoint vector is committs vec in old checkpoint
				// it can't be the same with newly collected on new version checkpoint, just skip it
				logutil.Infof("isBatchEqual skip attr %v for ver.%d on ver.%d", bat1.Attrs[j], oldver, logtail.CheckpointCurrentVersion)
				continue
			}
			// t.Logf("attr %v, row %d", bat1.Attrs[j], i)
			require.Equal(t, vec1.Get(i), vec2.Get(i), "name is \"%v\"", bat1.Attrs[j])
		}
	}
}

func isProtoTNBatchEqual(ctx context.Context, t *testing.T, bat1 *api.Batch, bat2 *containers.Batch) {
	if bat1 == nil {
		if bat2 == nil {
			return
		}
		assert.Equal(t, 0, getBatchLength(bat2))
	} else {
		moIns, err := batch.ProtoBatchToBatch(bat1)
		assert.NoError(t, err)
		tnIns := containers.ToTNBatch(moIns)
		isBatchEqual(ctx, t, tnIns, bat2)
	}
}

func checkCNCheckpointData(ctx context.Context, t *testing.T, tid uint64, ins, del, cnIns, segDel *api.Batch, start, end types.TS, c *catalog.Catalog) {
	if tid == pkgcatalog.MO_DATABASE_ID {
		checkMODatabase(ctx, t, ins, del, cnIns, segDel, start, end, c)
	} else if tid == pkgcatalog.MO_TABLES_ID {
		checkMOTables(ctx, t, ins, del, cnIns, segDel, start, end, c)
	} else if tid == pkgcatalog.MO_COLUMNS_ID {
		checkMOColumns(ctx, t, ins, del, cnIns, segDel, start, end, c)
	} else {
		checkUserTables(ctx, t, tid, ins, del, cnIns, segDel, start, end, c)
	}
}

func checkMODatabase(ctx context.Context, t *testing.T, ins, del, cnIns, segDel *api.Batch, start, end types.TS, c *catalog.Catalog) {
	collector := logtail.NewIncrementalCollector(start, end)
	p := &catalog.LoopProcessor{}
	p.DatabaseFn = collector.VisitDB
	err := c.RecurLoop(p)
	assert.NoError(t, err)
	data2 := collector.OrphanData()
	defer data2.Close()
	ins2, _, del2, _ := data2.GetDBBatchs()

	isProtoTNBatchEqual(ctx, t, ins, ins2)
	isProtoTNBatchEqual(ctx, t, del, del2)
	assert.Nil(t, cnIns)
	assert.Nil(t, segDel)
}

func checkMOTables(ctx context.Context, t *testing.T, ins, del, cnIns, segDel *api.Batch, start, end types.TS, c *catalog.Catalog) {
	collector := logtail.NewIncrementalCollector(start, end)
	p := &catalog.LoopProcessor{}
	p.TableFn = collector.VisitTable
	err := c.RecurLoop(p)
	assert.NoError(t, err)
	data2 := collector.OrphanData()
	defer data2.Close()
	ins2, _, _, del2, _ := data2.GetTblBatchs()

	isProtoTNBatchEqual(ctx, t, ins, ins2)
	isProtoTNBatchEqual(ctx, t, del, del2)
	assert.Nil(t, cnIns)
	assert.Nil(t, segDel)
}

func checkMOColumns(ctx context.Context, t *testing.T, ins, del, cnIns, segDel *api.Batch, start, end types.TS, c *catalog.Catalog) {
	collector := logtail.NewIncrementalCollector(start, end)
	p := &catalog.LoopProcessor{}
	p.TableFn = collector.VisitTable
	err := c.RecurLoop(p)
	assert.NoError(t, err)
	data2 := collector.OrphanData()
	bats := data2.GetBatches()
	ins2 := bats[logtail.TBLColInsertIDX]
	del2 := bats[logtail.TBLColDeleteIDX]

	isProtoTNBatchEqual(ctx, t, ins, ins2)
	isProtoTNBatchEqual(ctx, t, del, del2)
	assert.Nil(t, cnIns)
	assert.Nil(t, segDel)
}

func checkUserTables(ctx context.Context, t *testing.T, tid uint64, ins, del, cnIns, segDel *api.Batch, start, end types.TS, c *catalog.Catalog) {
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
	ins2 := bats[logtail.BLKMetaInsertIDX]
	del2 := bats[logtail.BLKMetaDeleteIDX]
	cnIns2 := bats[logtail.BLKCNMetaInsertIDX]
	segDel2 := bats[logtail.SEGDeleteIDX]

	isProtoTNBatchEqual(ctx, t, ins, ins2)
	isProtoTNBatchEqual(ctx, t, del, del2)
	isProtoTNBatchEqual(ctx, t, cnIns, cnIns2)
	isProtoTNBatchEqual(ctx, t, segDel, segDel2)
}

func CheckCheckpointReadWrite(
	t *testing.T,
	start, end types.TS,
	c *catalog.Catalog,
	checkpointBlockRows int,
	fs fileservice.FileService,
) {
	location, _ := writeIncrementalCheckpoint(t, start, end, c, checkpointBlockRows, fs)
	tnData := tnReadCheckpoint(t, location, fs)

	checkTNCheckpointData(context.Background(), t, tnData, start, end, c)
	p := &catalog.LoopProcessor{}

	ins, del, cnIns, seg, cbs := cnReadCheckpoint(t, pkgcatalog.MO_DATABASE_ID, location, fs)
	checkCNCheckpointData(context.Background(), t, pkgcatalog.MO_DATABASE_ID, ins, del, cnIns, seg, start, end, c)
	for _, cb := range cbs {
		if cb != nil {
			cb()
		}
	}
	ins, del, cnIns, seg, cbs = cnReadCheckpoint(t, pkgcatalog.MO_TABLES_ID, location, fs)
	checkCNCheckpointData(context.Background(), t, pkgcatalog.MO_TABLES_ID, ins, del, cnIns, seg, start, end, c)
	for _, cb := range cbs {
		if cb != nil {
			cb()
		}
	}
	ins, del, cnIns, seg, cbs = cnReadCheckpoint(t, pkgcatalog.MO_COLUMNS_ID, location, fs)
	checkCNCheckpointData(context.Background(), t, pkgcatalog.MO_COLUMNS_ID, ins, del, cnIns, seg, start, end, c)
	for _, cb := range cbs {
		if cb != nil {
			cb()
		}
	}

	p.TableFn = func(te *catalog.TableEntry) error {
		ins, del, cnIns, seg, cbs := cnReadCheckpoint(t, te.ID, location, fs)
		checkCNCheckpointData(context.Background(), t, te.ID, ins, del, cnIns, seg, start, end, c)
		for _, cb := range cbs {
			if cb != nil {
				cb()
			}
		}
		return nil
	}
}

func (e *TestEngine) CheckReadCNCheckpoint() {
	tids := []uint64{1, 2, 3}
	p := &catalog.LoopProcessor{}
	p.TableFn = func(te *catalog.TableEntry) error {
		tids = append(tids, te.ID)
		return nil
	}
	err := e.Catalog.RecurLoop(p)
	assert.NoError(e.t, err)
	ckps := e.BGCheckpointRunner.GetAllIncrementalCheckpoints()
	for _, ckp := range ckps {
		for _, tid := range tids {
			ins, del, cnIns, seg, cbs := cnReadCheckpointWithVersion(e.t, tid, ckp.GetLocation(), e.Opts.Fs, ckp.GetVersion())
			ctx := context.Background()
			ctx = context.WithValue(ctx, CtxOldVersion{}, int(ckp.GetVersion()))
			checkCNCheckpointData(ctx, e.t, tid, ins, del, cnIns, seg, ckp.GetStart(), ckp.GetEnd(), e.Catalog)
			for _, cb := range cbs {
				if cb != nil {
					cb()
				}
			}
		}
	}
}

func (e *TestEngine) CheckCollectDeleteInRange() {
	txn, rel := e.GetRelation()
	ForEachBlock(rel, func(blk handle.Block) error {
		meta := blk.GetMeta().(*catalog.BlockEntry)
		deleteBat, err := meta.GetBlockData().CollectDeleteInRange(context.Background(), types.TS{}, txn.GetStartTS(), false)
		assert.NoError(e.t, err)
		pkDef := e.schema.GetPrimaryKey()
		deleteRowIDs := deleteBat.GetVectorByName(catalog.AttrRowID)
		deletePKs := deleteBat.GetVectorByName(pkDef.Name)
		pks, err := meta.GetBlockData().GetColumnDataById(context.Background(), txn, e.schema, pkDef.Idx)
		assert.NoError(e.t, err)
		rowIDs, err := meta.GetBlockData().GetColumnDataById(context.Background(), txn, e.schema, e.schema.PhyAddrKey.Idx)
		assert.NoError(e.t, err)
		for i := 0; i < deleteBat.Length(); i++ {
			rowID := deleteRowIDs.Get(i).(types.Rowid)
			offset := rowID.GetRowOffset()
			appendRowID := rowIDs.GetData().Get(int(offset)).(types.Rowid)
			e.t.Logf("delete rowID %v pk %v, append rowID %v pk %v", rowID.String(), deletePKs.Get(i), appendRowID.String(), pks.GetData().Get(int(offset)))
			assert.Equal(e.t, pks.GetData().Get(int(offset)), deletePKs.Get(i))
		}
		return nil
	})
	err := txn.Commit(context.Background())
	assert.NoError(e.t, err)
}
