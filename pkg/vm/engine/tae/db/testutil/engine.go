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
	gc "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v2"
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
	T        *testing.T
	schema   *catalog.Schema
	tenantID uint32 // for almost tests, userID and roleID is not important
}

func NewTestEngineWithDir(
	ctx context.Context,
	dir string,
	t *testing.T,
	opts *options.Options,
) *TestEngine {
	blockio.Start("")
	db := InitTestDBWithDir(ctx, dir, t, opts)
	return &TestEngine{
		DB: db,
		T:  t,
	}
}

func NewTestEngine(
	ctx context.Context,
	moduleName string,
	t *testing.T,
	opts *options.Options,
) *TestEngine {
	blockio.Start("")
	db := InitTestDB(ctx, moduleName, t, opts)
	return &TestEngine{
		DB: db,
		T:  t,
	}
}

func (e *TestEngine) BindSchema(schema *catalog.Schema) { e.schema = schema }

func (e *TestEngine) BindTenantID(tenantID uint32) { e.tenantID = tenantID }

func (e *TestEngine) Restart(ctx context.Context) {
	_ = e.DB.Close()
	var err error
	e.DB, err = db.Open(ctx, e.Dir, e.Opts)
	// only ut executes this checker
	e.DB.DiskCleaner.GetCleaner().AddChecker(
		func(item any) bool {
			min := e.DB.TxnMgr.MinTSForTest()
			ckp := item.(*checkpoint.CheckpointEntry)
			//logutil.Infof("min: %v, checkpoint: %v", min.ToString(), checkpoint.GetStart().ToString())
			end := ckp.GetEnd()
			return !end.GE(&min)
		}, gc.CheckerKeyMinTS)
	assert.NoError(e.T, err)
}
func (e *TestEngine) RestartDisableGC(ctx context.Context) {
	_ = e.DB.Close()
	var err error
	e.Opts.GCCfg.GCTTL = 100 * time.Second
	e.DB, err = db.Open(ctx, e.Dir, e.Opts)
	// only ut executes this checker
	e.DB.DiskCleaner.GetCleaner().AddChecker(
		func(item any) bool {
			min := e.DB.TxnMgr.MinTSForTest()
			ckp := item.(*checkpoint.CheckpointEntry)
			//logutil.Infof("min: %v, checkpoint: %v", min.ToString(), checkpoint.GetStart().ToString())
			end := ckp.GetEnd()
			return !end.GE(&min)
		}, gc.CheckerKeyMinTS)
	assert.NoError(e.T, err)
}

func (e *TestEngine) Close() error {
	blockio.Stop("")
	err := e.DB.Close()
	return err
}

func (e *TestEngine) CreateRelAndAppend(bat *containers.Batch, createDB bool) {
	clonedSchema := e.schema.Clone()
	CreateRelationAndAppend(e.T, e.tenantID, e.DB, DefaultTestDB, clonedSchema, bat, createDB)
}

func (e *TestEngine) CreateRelAndAppend2(bat *containers.Batch, createDB bool) {
	clonedSchema := e.schema.Clone()
	CreateRelationAndAppend2(e.T, e.tenantID, e.DB, DefaultTestDB, clonedSchema, bat, createDB)
}

func (e *TestEngine) CheckRowsByScan(exp int, applyDelete bool) {
	txn, rel := e.GetRelation()
	CheckAllColRowsByScan(e.T, rel, exp, applyDelete)
	assert.NoError(e.T, txn.Commit(context.Background()))
}
func (e *TestEngine) ForceCheckpoint() {
	err := e.BGCheckpointRunner.ForceFlushWithInterval(e.TxnMgr.Now(), context.Background(), time.Second*2, time.Millisecond*10)
	assert.NoError(e.T, err)
	err = e.BGCheckpointRunner.ForceIncrementalCheckpoint(e.TxnMgr.Now(), false)
	assert.NoError(e.T, err)
}

func (e *TestEngine) ForceLongCheckpoint() {
	err := e.BGCheckpointRunner.ForceFlush(e.TxnMgr.Now(), context.Background(), 20*time.Second)
	assert.NoError(e.T, err)
	err = e.BGCheckpointRunner.ForceIncrementalCheckpoint(e.TxnMgr.Now(), false)
	assert.NoError(e.T, err)
}

func (e *TestEngine) ForceLongCheckpointTruncate() {
	err := e.BGCheckpointRunner.ForceFlush(e.TxnMgr.Now(), context.Background(), 20*time.Second)
	assert.NoError(e.T, err)
	err = e.BGCheckpointRunner.ForceIncrementalCheckpoint(e.TxnMgr.Now(), true)
	assert.NoError(e.T, err)
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
	return GetRelation(e.T, e.tenantID, e.DB, DefaultTestDB, e.schema.Name)
}
func (e *TestEngine) GetRelationWithTxn(txn txnif.AsyncTxn) (rel handle.Relation) {
	return GetRelationWithTxn(e.T, txn, DefaultTestDB, e.schema.Name)
}

func (e *TestEngine) CompactBlocks(skipConflict bool) {
	CompactBlocks(e.T, e.tenantID, e.DB, DefaultTestDB, e.schema, skipConflict)
}

func (e *TestEngine) MergeBlocks(skipConflict bool) {
	MergeBlocks(e.T, e.tenantID, e.DB, DefaultTestDB, e.schema, skipConflict)
}

func (e *TestEngine) GetDB(name string) (txn txnif.AsyncTxn, db handle.Database) {
	txn, err := e.DB.StartTxn(nil)
	txn.BindAccessInfo(e.tenantID, 0, 0)
	assert.NoError(e.T, err)
	db, err = txn.GetDatabase(name)
	assert.NoError(e.T, err)
	return
}

func (e *TestEngine) GetTestDB() (txn txnif.AsyncTxn, db handle.Database) {
	return e.GetDB(DefaultTestDB)
}

func (e *TestEngine) DoAppend(bat *containers.Batch) {
	txn, rel := e.GetRelation()
	err := rel.Append(context.Background(), bat)
	assert.NoError(e.T, err)
	assert.NoError(e.T, txn.Commit(context.Background()))
}

func (e *TestEngine) DoAppendWithTxn(bat *containers.Batch, txn txnif.AsyncTxn, skipConflict bool) (err error) {
	rel := e.GetRelationWithTxn(txn)
	err = rel.Append(context.Background(), bat)
	if !skipConflict {
		assert.NoError(e.T, err)
	}
	return
}

func (e *TestEngine) TryAppend(bat *containers.Batch) {
	txn, err := e.DB.StartTxn(nil)
	txn.BindAccessInfo(e.tenantID, 0, 0)
	assert.NoError(e.T, err)
	db, err := txn.GetDatabase(DefaultTestDB)
	assert.NoError(e.T, err)
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
	schema := rel.GetMeta().(*catalog.TableEntry).GetLastestSchemaLocked(false)
	pkIdx := schema.GetPrimaryKey().Idx
	rowIDIdx := schema.GetColIdx(catalog.PhyAddrColumnName)
	it := rel.MakeObjectIt(false)
	for it.Next() {
		blk := it.GetObject()
		defer blk.Close()
		blkCnt := uint16(blk.BlkCnt())
		for i := uint16(0); i < blkCnt; i++ {
			var view *containers.Batch
			err := blk.HybridScan(context.Background(), &view, i, []int{rowIDIdx, pkIdx}, common.DefaultAllocator)
			assert.NoError(e.T, err)
			defer view.Close()
			view.Compact()
			err = rel.DeleteByPhyAddrKeys(view.Vecs[0], view.Vecs[1], handle.DT_Normal)
			assert.NoError(e.T, err)
		}
	}
	// CheckAllColRowsByScan(e.t, rel, 0, true)
	err := txn.Commit(context.Background())
	if !skipConflict {
		CheckAllColRowsByScan(e.T, rel, 0, true)
		assert.NoError(e.T, err)
	}
	return err
}

func (e *TestEngine) Truncate() {
	txn, db := e.GetTestDB()
	_, err := db.TruncateByName(e.schema.Name)
	assert.NoError(e.T, err)
	assert.NoError(e.T, txn.Commit(context.Background()))
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
		assert.True(e.T, flushed)
	}
	err := e.DB.BGCheckpointRunner.ForceGlobalCheckpoint(endTs, versionInterval)
	assert.NoError(e.T, err)
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
		require.True(e.T, flushed)
	}
	err := e.DB.BGCheckpointRunner.ForceIncrementalCheckpoint(end, false)
	require.NoError(e.T, err)
	if truncate {
		lsn := e.DB.BGCheckpointRunner.MaxLSNInRange(end)
		entry, err := e.DB.Wal.RangeCheckpoint(1, lsn)
		require.NoError(e.T, err)
		require.NoError(e.T, entry.WaitDone())
		testutils.WaitExpect(1000, func() bool {
			return e.Runtime.Scheduler.GetPenddingLSNCnt() == 0
		})
	}
	return nil
}

func (e *TestEngine) TryDeleteByDeltaloc(vals []any) (ok bool, err error) {
	txn, err := e.StartTxn(nil)
	assert.NoError(e.T, err)
	ok, err = e.TryDeleteByDeltalocWithTxn(vals, txn)
	if ok {
		assert.NoError(e.T, txn.Commit(context.Background()))
	} else {
		assert.NoError(e.T, txn.Rollback(context.Background()))
	}
	return
}

func (e *TestEngine) TryDeleteByDeltalocWithTxn(vals []any, txn txnif.AsyncTxn) (ok bool, err error) {
	rel := e.GetRelationWithTxn(txn)

	rowIDs := containers.MakeVector(types.T_Rowid.ToType(), common.DebugAllocator)
	pks := containers.MakeVector(e.schema.GetPrimaryKey().Type, common.DebugAllocator)
	var firstID *common.ID // TODO use table.AsCommonID
	for i, val := range vals {
		filter := handle.NewEQFilter(val)
		id, offset, err := rel.GetByFilter(context.Background(), filter)
		if i == 0 {
			firstID = id
		}
		assert.NoError(e.T, err)
		objID := id.ObjectID()
		_, blkOffset := id.BlockID.Offsets()
		rowID := types.NewRowIDWithObjectIDBlkNumAndRowID(*objID, blkOffset, offset)
		rowIDs.Append(rowID, false)
		pks.Append(val, false)
	}

	s3stats, err := MockCNDeleteInS3(e.Runtime.Fs, rowIDs, pks, e.schema, txn)
	stats := objectio.NewObjectStatsWithObjectID(s3stats.ObjectName().ObjectId(), false, true, true)
	objectio.SetObjectStats(stats, &s3stats)
	pks.Close()
	rowIDs.Close()
	assert.NoError(e.T, err)
	require.False(e.T, stats.IsZero())
	ok, err = rel.TryDeleteByStats(firstID, *stats)
	assert.NoError(e.T, err)
	if !ok {
		return ok, err
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
	db.DiskCleaner.GetCleaner().AddChecker(
		func(item any) bool {
			min := db.TxnMgr.MinTSForTest()
			ckp := item.(*checkpoint.CheckpointEntry)
			//logutil.Infof("min: %v, checkpoint: %v", min.ToString(), checkpoint.GetStart().ToString())
			end := ckp.GetEnd()
			return !end.GE(&min)
		}, gc.CheckerKeyMinTS)
	return db
}

func InitTestDB(
	ctx context.Context,
	moduleName string,
	t *testing.T,
	opts *options.Options,
) *db.DB {
	blockio.Start("")
	dir := testutils.InitTestEnv(moduleName, t)
	db, _ := db.Open(ctx, dir, opts)
	// only ut executes this checker
	db.DiskCleaner.GetCleaner().AddChecker(
		func(item any) bool {
			min := db.TxnMgr.MinTSForTest()
			ckp := item.(*checkpoint.CheckpointEntry)
			//logutil.Infof("min: %v, checkpoint: %v", min.ToString(), checkpoint.GetStart().ToString())
			end := ckp.GetEnd()
			return !end.GE(&min)
		}, gc.CheckerKeyMinTS)
	return db
}

func writeIncrementalCheckpoint(
	t *testing.T,
	start, end types.TS,
	c *catalog.Catalog,
	checkpointBlockRows int,
	checkpointSize int,
	fs fileservice.FileService,
) (objectio.Location, objectio.Location) {
	factory := logtail.IncrementalCheckpointDataFactory("", start, end, false)
	data, err := factory(c)
	assert.NoError(t, err)
	defer data.Close()
	cnLocation, tnLocation, _, err := data.WriteTo(fs, checkpointBlockRows, checkpointSize)
	assert.NoError(t, err)
	return cnLocation, tnLocation
}

func tnReadCheckpoint(t *testing.T, location objectio.Location, fs fileservice.FileService) *logtail.CheckpointData {
	reader, err := blockio.NewObjectReader("", fs, location)
	assert.NoError(t, err)
	data := logtail.NewCheckpointData("", common.CheckpointAllocator)
	err = data.ReadFrom(
		context.Background(),
		logtail.CheckpointCurrentVersion,
		location,
		reader,
		fs,
	)
	assert.NoError(t, err)
	return data
}
func cnReadCheckpoint(t *testing.T, tid uint64, location objectio.Location, fs fileservice.FileService) (ins, del, cnIns, segDel *api.Batch, cb []func()) {
	ins, del, cnIns, segDel, cb = cnReadCheckpointWithVersion(t, tid, location, fs, logtail.CheckpointCurrentVersion)
	return
}

func ReadSnapshotCheckpoint(t *testing.T, tid uint64, location objectio.Location, fs fileservice.FileService) (ins, del, cnIns, segDel *api.Batch, cb []func()) {
	ins, del, cnIns, segDel, cb = cnReadCheckpointWithVersion(t, tid, location, fs, logtail.CheckpointCurrentVersion)
	return
}

func cnReadCheckpointWithVersion(t *testing.T, tid uint64, location objectio.Location, fs fileservice.FileService, ver uint32) (ins, del, dataObj, tombstoneObj *api.Batch, cb []func()) {
	locs := make([]string, 0)
	locs = append(locs, location.String())
	locs = append(locs, strconv.Itoa(int(ver)))
	locations := strings.Join(locs, ";")
	entries, cb, err := logtail.LoadCheckpointEntries(
		context.Background(),
		"",
		locations,
		tid,
		"tbl",
		0,
		"db",
		common.CheckpointAllocator,
		fs,
	)
	assert.NoError(t, err)
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		if e.TableName == fmt.Sprintf("_%d_data_meta", tid) {
			dataObj = e.Bat
		} else if e.TableName == fmt.Sprintf("_%d_tombstone_meta", tid) {
			tombstoneObj = e.Bat
		} else if e.EntryType == api.Entry_Insert {
			ins = e.Bat
		} else {
			del = e.Bat
		}
	}
	for _, c := range cb {
		if c != nil {
			c()
		}
	}
	return
}

func checkTNCheckpointData(ctx context.Context, t *testing.T, data *logtail.CheckpointData,
	start, end types.TS, c *catalog.Catalog) {
	factory := logtail.IncrementalCheckpointDataFactory("", start, end, false)
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
		tnIns := containers.ToTNBatch(moIns, common.DefaultAllocator)
		isBatchEqual(ctx, t, tnIns, bat2)
	}
}

func checkCNCheckpointData(ctx context.Context, t *testing.T, tid uint64, ins, del, cnIns, segDel *api.Batch, start, end types.TS, c *catalog.Catalog) {
	checkUserTables(ctx, t, tid, ins, del, cnIns, segDel, start, end, c)
}

func checkUserTables(ctx context.Context, t *testing.T, tid uint64, ins, del, dataObject, tombstoneObject *api.Batch, start, end types.TS, c *catalog.Catalog) {
	collector := logtail.NewIncrementalCollector("", start, end)
	p := &catalog.LoopProcessor{}
	p.TombstoneFn = func(be *catalog.ObjectEntry) error {
		if be.GetTable().ID != tid {
			return nil
		}
		return collector.VisitObj(be)
	}
	p.ObjectFn = func(se *catalog.ObjectEntry) error {
		if se.GetTable().ID != tid {
			return nil
		}
		return collector.VisitObj(se)
	}
	err := c.RecurLoop(p)
	assert.NoError(t, err)
	data2 := collector.OrphanData()
	bats := data2.GetBatches()
	seg2 := bats[logtail.ObjectInfoIDX]
	tombstone2 := bats[logtail.TombstoneObjectInfoIDX]

	isProtoTNBatchEqual(ctx, t, dataObject, seg2)
	isProtoTNBatchEqual(ctx, t, tombstoneObject, tombstone2)
}

func GetUserTablesInsBatch(t *testing.T, tid uint64, start, end types.TS, c *catalog.Catalog) (dataObject, tombstoneObject *containers.Batch) {
	collector := logtail.NewIncrementalCollector("", start, end)
	p := &catalog.LoopProcessor{}
	p.TombstoneFn = func(be *catalog.ObjectEntry) error {
		if be.GetTable().ID != tid {
			return nil
		}
		return collector.VisitObj(be)
	}
	p.ObjectFn = func(se *catalog.ObjectEntry) error {
		if se.GetTable().ID != tid {
			return nil
		}
		return collector.VisitObj(se)
	}
	err := c.RecurLoop(p)
	assert.NoError(t, err)
	data := collector.OrphanData()
	bats := data.GetBatches()
	return bats[logtail.ObjectInfoIDX], bats[logtail.TombstoneObjectInfoIDX]
}

func CheckCheckpointReadWrite(
	t *testing.T,
	start, end types.TS,
	c *catalog.Catalog,
	checkpointBlockRows int,
	checkpointSize int,
	fs fileservice.FileService,
) {
	location, _ := writeIncrementalCheckpoint(t, start, end, c, checkpointBlockRows, checkpointSize, fs)
	tnData := tnReadCheckpoint(t, location, fs)

	checkTNCheckpointData(context.Background(), t, tnData, start, end, c)
	p := &catalog.LoopProcessor{}

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
	assert.NoError(e.T, err)
	ckps := e.BGCheckpointRunner.GetAllIncrementalCheckpoints()
	for _, ckp := range ckps {
		for _, tid := range tids {
			ins, del, cnIns, seg, cbs := cnReadCheckpointWithVersion(e.T, tid, ckp.GetLocation(), e.Opts.Fs, ckp.GetVersion())
			ctx := context.Background()
			ctx = context.WithValue(ctx, CtxOldVersion{}, int(ckp.GetVersion()))
			checkCNCheckpointData(ctx, e.T, tid, ins, del, cnIns, seg, ckp.GetStart(), ckp.GetEnd(), e.Catalog)
			for _, cb := range cbs {
				if cb != nil {
					cb()
				}
			}
		}
	}
}

func (e *TestEngine) CheckCollectTombstoneInRange() {
	txn, rel := e.GetRelation()
	ForEachTombstone(e.T, rel, func(obj handle.Object) error {
		meta := obj.GetMeta().(*catalog.ObjectEntry)
		blkCnt := obj.BlkCnt()
		for i := 0; i < blkCnt; i++ {
			var deleteBatch *containers.Batch
			err := meta.GetObjectData().Scan(
				context.Background(), &deleteBatch, txn, e.schema, uint16(i), []int{0, 1}, common.DefaultAllocator,
			)
			assert.NoError(e.T, err)
			pkDef := e.schema.GetPrimaryKey()
			deleteRowIDs := deleteBatch.Vecs[0]
			deletePKs := deleteBatch.Vecs[1]
			for i := 0; i < deleteRowIDs.Length(); i++ {
				rowID := deleteRowIDs.Get(i).(types.Rowid)
				offset := rowID.GetRowOffset()
				id := obj.Fingerprint()
				id.BlockID = *rowID.BorrowBlockID()
				val, _, err := rel.GetValue(id, offset, uint16(pkDef.Idx), true)
				assert.NoError(e.T, err)
				e.T.Logf("delete rowID %v pk %v, append rowID %v pk %v", rowID.String(), deletePKs.Get(i), rowID.String(), val)
				assert.Equal(e.T, val, deletePKs.Get(i))
			}
		}
		return nil
	})
	err := txn.Commit(context.Background())
	assert.NoError(e.T, err)
}
