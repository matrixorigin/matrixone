package testutil

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

const (
	defaultTestDB = "db"
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
	return CreateRelationAndAppend(e.t, e.tenantID, e.DB, defaultTestDB, e.schema, bat, createDB)
}

func (e *TestEngine) CheckRowsByScan(exp int, applyDelete bool) {
	txn, rel := e.GetRelation()
	CheckAllColRowsByScan(e.t, rel, exp, applyDelete)
	assert.NoError(e.t, txn.Commit(context.Background()))
}
func (e *TestEngine) DropRelation(t *testing.T) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	db, err := txn.GetDatabase(defaultTestDB)
	assert.NoError(t, err)
	_, err = db.DropRelationByName(e.schema.Name)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
}
func (e *TestEngine) GetRelation() (txn txnif.AsyncTxn, rel handle.Relation) {
	return GetRelation(e.t, e.tenantID, e.DB, defaultTestDB, e.schema.Name)
}
func (e *TestEngine) GetRelationWithTxn(txn txnif.AsyncTxn) (rel handle.Relation) {
	return GetRelationWithTxn(e.t, txn, defaultTestDB, e.schema.Name)
}

func (e *TestEngine) CompactBlocks(skipConflict bool) {
	CompactBlocks(e.t, e.tenantID, e.DB, defaultTestDB, e.schema, skipConflict)
}

func (e *TestEngine) MergeBlocks(skipConflict bool) {
	MergeBlocks(e.t, e.tenantID, e.DB, defaultTestDB, e.schema, skipConflict)
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
	return e.GetDB(defaultTestDB)
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
	db, err := txn.GetDatabase(defaultTestDB)
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
		deltaLoc, err := mockCNDeleteInS3(e.Runtime.Fs, blk.GetBlockData(), e.schema, txn, offsets)
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

func WithTestAllPKType(t *testing.T, tae *db.DB, test func(*testing.T, *db.DB, *catalog.Schema)) {
	var wg sync.WaitGroup
	pool, _ := ants.NewPool(100)
	defer pool.Release()
	for i := 0; i < 17; i++ {
		schema := catalog.MockSchemaAll(18, i)
		schema.BlockMaxRows = 10
		schema.SegmentMaxBlocks = 2
		wg.Add(1)
		_ = pool.Submit(func() {
			defer wg.Done()
			test(t, tae, schema)
		})
	}
	wg.Wait()
}

func LenOfBats(bats []*containers.Batch) int {
	rows := 0
	for _, bat := range bats {
		rows += bat.Length()
	}
	return rows
}

func PrintCheckpointStats(t *testing.T, tae *db.DB) {
	t.Logf("GetCheckpointedLSN: %d", tae.Wal.GetCheckpointed())
	t.Logf("GetPenddingLSNCnt: %d", tae.Wal.GetPenddingCnt())
	t.Logf("GetCurrSeqNum: %d", tae.Wal.GetCurrSeqNum())
}

func CreateDB(t *testing.T, e *db.DB, dbName string) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	_, err = txn.CreateDatabase(dbName, "", "")
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
}

func DropDB(t *testing.T, e *db.DB, dbName string) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	_, err = txn.DropDatabase(dbName)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
}

func CreateRelation(t *testing.T, e *db.DB, dbName string, schema *catalog.Schema, createDB bool) (db handle.Database, rel handle.Relation) {
	txn, db, rel := CreateRelationNoCommit(t, e, dbName, schema, createDB)
	assert.NoError(t, txn.Commit(context.Background()))
	return
}

func CreateRelationNoCommit(t *testing.T, e *db.DB, dbName string, schema *catalog.Schema, createDB bool) (txn txnif.AsyncTxn, db handle.Database, rel handle.Relation) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	if createDB {
		db, err = txn.CreateDatabase(dbName, "", "")
		assert.NoError(t, err)
	} else {
		db, err = txn.GetDatabase(dbName)
		assert.NoError(t, err)
	}
	rel, err = db.CreateRelation(schema)
	assert.NoError(t, err)
	return
}

func CreateRelationAndAppend(
	t *testing.T,
	tenantID uint32,
	e *db.DB,
	dbName string,
	schema *catalog.Schema,
	bat *containers.Batch,
	createDB bool) (db handle.Database, rel handle.Relation) {
	txn, err := e.StartTxn(nil)
	txn.BindAccessInfo(tenantID, 0, 0)
	assert.NoError(t, err)
	if createDB {
		db, err = txn.CreateDatabase(dbName, "", "")
		assert.NoError(t, err)
	} else {
		db, err = txn.GetDatabase(dbName)
		assert.NoError(t, err)
	}
	rel, err = db.CreateRelation(schema)
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bat)
	assert.NoError(t, err)
	assert.Nil(t, txn.Commit(context.Background()))
	return
}

func GetRelation(t *testing.T, tenantID uint32, e *db.DB, dbName, tblName string) (txn txnif.AsyncTxn, rel handle.Relation) {
	txn, err := e.StartTxn(nil)
	txn.BindAccessInfo(tenantID, 0, 0)
	assert.NoError(t, err)
	db, err := txn.GetDatabase(dbName)
	assert.NoError(t, err)
	rel, err = db.GetRelationByName(tblName)
	assert.NoError(t, err)
	return
}

func GetRelationWithTxn(t *testing.T, txn txnif.AsyncTxn, dbName, tblName string) (rel handle.Relation) {
	db, err := txn.GetDatabase(dbName)
	assert.NoError(t, err)
	rel, err = db.GetRelationByName(tblName)
	assert.NoError(t, err)
	return
}

func GetDefaultRelation(t *testing.T, e *db.DB, name string) (txn txnif.AsyncTxn, rel handle.Relation) {
	return GetRelation(t, 0, e, defaultTestDB, name)
}

func GetOneBlock(rel handle.Relation) handle.Block {
	it := rel.MakeBlockIt()
	return it.GetBlock()
}

func GetOneBlockMeta(rel handle.Relation) *catalog.BlockEntry {
	it := rel.MakeBlockIt()
	return it.GetBlock().GetMeta().(*catalog.BlockEntry)
}

func CheckAllColRowsByScan(t *testing.T, rel handle.Relation, expectRows int, applyDelete bool) {
	schema := rel.Schema().(*catalog.Schema)
	for _, def := range schema.ColDefs {
		rows := GetColumnRowsByScan(t, rel, def.Idx, applyDelete)
		assert.Equal(t, expectRows, rows)
	}
}

func GetColumnRowsByScan(t *testing.T, rel handle.Relation, colIdx int, applyDelete bool) int {
	rows := 0
	ForEachColumnView(rel, colIdx, func(view *containers.ColumnView) (err error) {
		if applyDelete {
			view.ApplyDeletes()
		}
		rows += view.Length()
		// t.Log(view.String())
		return
	})
	return rows
}

func ForEachColumnView(rel handle.Relation, colIdx int, fn func(view *containers.ColumnView) error) {
	ForEachBlock(rel, func(blk handle.Block) (err error) {
		view, err := blk.GetColumnDataById(context.Background(), colIdx)
		if view == nil {
			logutil.Warnf("blk %v", blk.String())
			return
		}
		if err != nil {
			return
		}
		defer view.Close()
		err = fn(view)
		return
	})
}

func ForEachBlock(rel handle.Relation, fn func(blk handle.Block) error) {
	it := rel.MakeBlockIt()
	var err error
	for it.Valid() {
		blk := it.GetBlock()
		defer blk.Close()
		if err = fn(blk); err != nil {
			if errors.Is(err, handle.ErrIteratorEnd) {
				return
			} else {
				panic(err)
			}
		}
		it.Next()
	}
}

func ForEachSegment(rel handle.Relation, fn func(seg handle.Segment) error) {
	it := rel.MakeSegmentIt()
	var err error
	for it.Valid() {
		seg := it.GetSegment()
		defer seg.Close()
		if err = fn(seg); err != nil {
			if errors.Is(err, handle.ErrIteratorEnd) {
				return
			} else {
				panic(err)
			}
		}
		it.Next()
	}
}

func AppendFailClosure(t *testing.T, data *containers.Batch, name string, e *db.DB, wg *sync.WaitGroup) func() {
	return func() {
		if wg != nil {
			defer wg.Done()
		}
		txn, _ := e.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(name)
		err := rel.Append(context.Background(), data)
		assert.NotNil(t, err)
		assert.Nil(t, txn.Rollback(context.Background()))
	}
}

func AppendClosure(t *testing.T, data *containers.Batch, name string, e *db.DB, wg *sync.WaitGroup) func() {
	return func() {
		if wg != nil {
			defer wg.Done()
		}
		txn, _ := e.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(name)
		err := rel.Append(context.Background(), data)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
}

func tryAppendClosure(t *testing.T, data *containers.Batch, name string, e *db.DB, wg *sync.WaitGroup) func() {
	return func() {
		if wg != nil {
			defer wg.Done()
		}
		txn, _ := e.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, err := database.GetRelationByName(name)
		if err != nil {
			_ = txn.Rollback(context.Background())
			return
		}
		if err = rel.Append(context.Background(), data); err != nil {
			_ = txn.Rollback(context.Background())
			return
		}
		_ = txn.Commit(context.Background())
	}
}

func CompactBlocks(t *testing.T, tenantID uint32, e *db.DB, dbName string, schema *catalog.Schema, skipConflict bool) {
	txn, rel := GetRelation(t, tenantID, e, dbName, schema.Name)

	var metas []*catalog.BlockEntry
	it := rel.MakeBlockIt()
	for it.Valid() {
		blk := it.GetBlock()
		meta := blk.GetMeta().(*catalog.BlockEntry)
		if blk.Rows() < int(schema.BlockMaxRows) {
			it.Next()
			continue
		}
		metas = append(metas, meta)
		it.Next()
	}
	_ = txn.Commit(context.Background())
	for _, meta := range metas {
		txn, _ := GetRelation(t, tenantID, e, dbName, schema.Name)
		task, err := jobs.NewCompactBlockTask(nil, txn, meta, e.Runtime)
		if skipConflict && err != nil {
			_ = txn.Rollback(context.Background())
			continue
		}
		assert.NoError(t, err)
		err = task.OnExec(context.Background())
		if skipConflict {
			if err != nil {
				_ = txn.Rollback(context.Background())
			} else {
				_ = txn.Commit(context.Background())
			}
		} else {
			assert.NoError(t, err)
			assert.NoError(t, txn.Commit(context.Background()))
		}
	}
}

func MergeBlocks(t *testing.T, tenantID uint32, e *db.DB, dbName string, schema *catalog.Schema, skipConflict bool) {
	txn, _ := e.StartTxn(nil)
	txn.BindAccessInfo(tenantID, 0, 0)
	db, _ := txn.GetDatabase(dbName)
	rel, _ := db.GetRelationByName(schema.Name)

	var segs []*catalog.SegmentEntry
	segIt := rel.MakeSegmentIt()
	for segIt.Valid() {
		seg := segIt.GetSegment().GetMeta().(*catalog.SegmentEntry)
		if seg.GetAppendableBlockCnt() == int(seg.GetTable().GetLastestSchema().SegmentMaxBlocks) {
			segs = append(segs, seg)
		}
		segIt.Next()
	}
	_ = txn.Commit(context.Background())
	for _, seg := range segs {
		txn, _ = e.StartTxn(nil)
		txn.BindAccessInfo(tenantID, 0, 0)
		db, _ = txn.GetDatabase(dbName)
		rel, _ = db.GetRelationByName(schema.Name)
		segHandle, err := rel.GetSegment(&seg.ID)
		if err != nil {
			if skipConflict {
				_ = txn.Rollback(context.Background())
				continue
			}
			assert.NoErrorf(t, err, "Txn Ts=%d", txn.GetStartTS())
		}
		var metas []*catalog.BlockEntry
		it := segHandle.MakeBlockIt()
		for it.Valid() {
			meta := it.GetBlock().GetMeta().(*catalog.BlockEntry)
			metas = append(metas, meta)
			it.Next()
		}
		segsToMerge := []*catalog.SegmentEntry{segHandle.GetMeta().(*catalog.SegmentEntry)}
		task, err := jobs.NewMergeBlocksTask(nil, txn, metas, segsToMerge, nil, e.Runtime)
		if skipConflict && err != nil {
			_ = txn.Rollback(context.Background())
			continue
		}
		assert.NoError(t, err)
		err = task.OnExec(context.Background())
		if skipConflict {
			if err != nil {
				_ = txn.Rollback(context.Background())
			} else {
				_ = txn.Commit(context.Background())
			}
		} else {
			assert.NoError(t, err)
			assert.NoError(t, txn.Commit(context.Background()))
		}
	}
}

/*func compactSegs(t *testing.T, e *DB, schema *catalog.Schema) {
	txn, rel := GetDefaultRelation(t, e, schema.Name)
	segs := make([]*catalog.SegmentEntry, 0)
	it := rel.MakeSegmentIt()
	for it.Valid() {
		seg := it.GetSegment().GetMeta().(*catalog.SegmentEntry)
		segs = append(segs, seg)
		it.Next()
	}
	for _, segMeta := range segs {
		seg := segMeta.GetSegmentData()
		factory, taskType, scopes, err := seg.BuildCompactionTaskFactory()
		assert.NoError(t, err)
		if factory == nil {
			continue
		}
		task, err := e.Scheduler.ScheduleMultiScopedTxnTask(tasks.WaitableCtx, taskType, scopes, factory)
		assert.NoError(t, err)
		err = task.WaitDone()
		assert.NoError(t, err)
	}
	assert.NoError(t, txn.Commit(context.Background()))
}*/

func getSingleSortKeyValue(bat *containers.Batch, schema *catalog.Schema, row int) (v any) {
	v = bat.Vecs[schema.GetSingleSortKeyIdx()].Get(row)
	return
}

func mockCNDeleteInS3(fs *objectio.ObjectFS, blk data.Block, schema *catalog.Schema, txn txnif.AsyncTxn, deleteRows []uint32) (location objectio.Location, err error) {
	pkDef := schema.GetPrimaryKey()
	view, err := blk.GetColumnDataById(context.Background(), txn, schema, pkDef.Idx)
	pkVec := containers.MakeVector(pkDef.Type)
	rowIDVec := containers.MakeVector(types.T_Rowid.ToType())
	blkID := &blk.GetMeta().(*catalog.BlockEntry).ID
	if err != nil {
		return
	}
	for _, row := range deleteRows {
		pkVal := view.GetData().Get(int(row))
		pkVec.Append(pkVal, false)
		rowID := objectio.NewRowid(blkID, row)
		rowIDVec.Append(*rowID, false)
	}
	bat := containers.NewBatch()
	bat.AddVector(catalog.AttrRowID, rowIDVec)
	bat.AddVector("pk", pkVec)
	name := objectio.MockObjectName()
	writer, err := blockio.NewBlockWriterNew(fs.Service, name, 0, nil)
	if err != nil {
		return
	}
	_, err = writer.WriteBatchWithOutIndex(containers.ToCNBatch(bat))
	if err != nil {
		return
	}
	blks, _, err := writer.Sync(context.Background())
	location = blockio.EncodeLocation(name, blks[0].GetExtent(), uint32(bat.Length()), blks[0].GetID())
	return
}
