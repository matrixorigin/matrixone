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

	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
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
	ioutil.Start("")
	db := InitTestDBWithDir(ctx, dir, t, opts)
	return &TestEngine{
		DB: db,
		T:  t,
	}
}

func NewReplayTestEngine(
	ctx context.Context,
	moduleName string,
	t *testing.T,
	opts *options.Options,
) *TestEngine {
	return NewTestEngine(
		ctx,
		moduleName,
		t,
		opts,
		db.WithTxnMode(db.DBTxnMode_Replay),
	)
}

func NewTestEngine(
	ctx context.Context,
	moduleName string,
	t *testing.T,
	opts *options.Options,
	dbOpts ...db.DBOption,
) *TestEngine {
	ioutil.Start("")
	db := InitTestDB(ctx, moduleName, t, opts, dbOpts...)
	return &TestEngine{
		DB: db,
		T:  t,
	}
}

func (e *TestEngine) BindSchema(schema *catalog.Schema) { e.schema = schema }

func (e *TestEngine) BindTenantID(tenantID uint32) { e.tenantID = tenantID }

func (e *TestEngine) Restart(ctx context.Context, opts ...*options.Options) {
	_ = e.DB.Close()
	var err error
	if len(opts) > 0 {
		e.Opts = opts[0]
	}
	e.DB, err = db.Open(ctx, e.Dir, e.Opts)
	// only ut executes this checker
	e.DB.DiskCleaner.GetCleaner().AddChecker(
		func(item any) bool {
			min := e.DB.TxnMgr.MinTSForTest()
			ckp := item.(*checkpoint.CheckpointEntry)
			//logutil.Infof("min: %v, checkpoint: %v", min.ToString(), checkpoint.GetStart().ToString())
			end := ckp.GetEnd()
			return !end.GE(&min)
		}, cmd_util.CheckerKeyMinTS)
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
		}, cmd_util.CheckerKeyMinTS)
	assert.NoError(e.T, err)
}

func (e *TestEngine) Close() error {
	ioutil.Stop("")
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err := e.DB.ForceCheckpoint(ctx, e.TxnMgr.Now())
	assert.NoError(e.T, err)
}

func (e *TestEngine) ForceLongCheckpoint() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	err := e.DB.ForceCheckpoint(ctx, e.TxnMgr.Now())
	assert.NoError(e.T, err)
}

func (e *TestEngine) ForceLongCheckpointTruncate() {
	e.ForceLongCheckpoint()
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

func (e *TestEngine) AllFlushExpected(ts types.TS, timeoutMS int) {
	testutils.WaitExpect(timeoutMS, func() bool {
		flushed := e.DB.BGFlusher.IsAllChangesFlushed(types.TS{}, ts, false)
		return flushed
	})
	flushed := e.DB.BGFlusher.IsAllChangesFlushed(types.TS{}, ts, true)
	require.True(e.T, flushed)
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
	ok, err = rel.AddPersistedTombstoneFile(firstID, *stats)
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
	var (
		err error
		tae *db.DB
	)
	if tae, err = db.Open(ctx, dir, opts); err != nil {
		panic(err)
	}
	// only ut executes this checker
	tae.DiskCleaner.GetCleaner().AddChecker(
		func(item any) bool {
			min := tae.TxnMgr.MinTSForTest()
			ckp := item.(*checkpoint.CheckpointEntry)
			//logutil.Infof("min: %v, checkpoint: %v", min.ToString(), checkpoint.GetStart().ToString())
			end := ckp.GetEnd()
			return !end.GE(&min)
		}, cmd_util.CheckerKeyMinTS)
	return tae
}

func InitTestDB(
	ctx context.Context,
	moduleName string,
	t *testing.T,
	opts *options.Options,
	dbOpts ...db.DBOption,
) *db.DB {
	ioutil.Start("")
	dir := testutils.InitTestEnv(moduleName, t)
	db, _ := db.Open(ctx, dir, opts, dbOpts...)
	// only ut executes this checker
	db.DiskCleaner.GetCleaner().AddChecker(
		func(item any) bool {
			min := db.TxnMgr.MinTSForTest()
			ckp := item.(*checkpoint.CheckpointEntry)
			//logutil.Infof("min: %v, checkpoint: %v", min.ToString(), checkpoint.GetStart().ToString())
			end := ckp.GetEnd()
			return !end.GE(&min)
		}, cmd_util.CheckerKeyMinTS)
	return db
}

func writeIncrementalCheckpoint(
	ctx context.Context,
	t *testing.T,
	start, end types.TS,
	c *catalog.Catalog,
	checkpointSize int,
	fs fileservice.FileService,
) (objectio.Location, objectio.Location) {
	factory := logtail.IncrementalCheckpointDataFactory(start, end, checkpointSize, fs)
	data, err := factory(c)
	assert.NoError(t, err)
	defer data.Close()
	cnLocation, tnLocation, _, err := data.WriteTo(ctx, fs)
	assert.NoError(t, err)
	return cnLocation, tnLocation
}

func checkTNCheckpointData(
	ctx context.Context,
	t *testing.T,
	loc1 objectio.Location,
	end types.TS,
	e *TestEngine,
) {

	c2 := mockAndReplayCatalog(
		ctx, t, loc1, e,
	)
	checkCatalog(t, e.Catalog, c2, end)
}

func checkCatalog(
	t *testing.T, c1, c2 *catalog.Catalog, end types.TS,
) {
	p := &catalog.LoopProcessor{}
	objFn := func(oe *catalog.ObjectEntry) error {
		createAt := oe.GetCreatedAt()
		if createAt.GT(&end) {
			return nil
		}
		db, err := c2.GetDatabaseByID(oe.GetTable().GetDB().ID)
		assert.NoError(t, err)
		tbl, err := db.GetTableEntryByID(oe.GetTable().ID)
		assert.NoError(t, err)
		oe2, err := tbl.GetObjectByID(oe.ID(), oe.IsTombstone)
		assert.NoError(t, err)
		createAt2 := oe2.GetCreatedAt()
		assert.True(t, createAt.EQ(&createAt2))
		delete := oe.GetDeleteAt()
		if delete.GT(&end) {
			return nil
		}
		delete2 := oe2.GetDeleteAt()
		assert.True(t, delete.EQ(&delete2))
		return nil
	}
	p.ObjectFn = objFn
	p.TombstoneFn = objFn
	err := c1.RecurLoop(p)
	assert.NoError(t, err)
}

type objlistReplayer struct{}

func (r *objlistReplayer) Submit(_ uint64, fn func()) {
	fn()
}

func mockAndReplayCatalog(
	ctx context.Context,
	t *testing.T,
	loc objectio.Location,
	e *TestEngine,
) *catalog.Catalog {
	dataFactory := tables.NewDataFactory(e.Runtime, e.Dir)
	c := catalog.MockCatalog(dataFactory)
	reader := logtail.NewCKPReader(
		logtail.CheckpointCurrentVersion,
		loc,
		common.DebugAllocator,
		e.Opts.Fs,
	)
	err := reader.ReadMeta(ctx)
	assert.NoError(t, err)
	err = logtail.ReplayCheckpoint(ctx, c, true, reader)
	assert.NoError(t, err)

	readTxn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	closeFn := c.RelayFromSysTableObjects(
		ctx, readTxn, tables.ReadSysTableBatch, func(cols []containers.Vector, pkidx int) (err2 error) {
			_, err2 = mergesort.SortBlockColumns(cols, pkidx, e.Runtime.VectorPool.Transient)
			return
		}, &objlistReplayer{},
	)
	for _, fn := range closeFn {
		fn()
	}
	assert.NoError(t, readTxn.Commit(ctx))

	reader = logtail.NewCKPReader(
		logtail.CheckpointCurrentVersion,
		loc,
		common.DebugAllocator,
		e.Opts.Fs,
	)
	err = reader.ReadMeta(ctx)
	assert.NoError(t, err)
	err = logtail.ReplayCheckpoint(ctx, c, false, reader)
	assert.NoError(t, err)

	return c
}

func checkCheckpointDataByTableID(
	ctx context.Context,
	t *testing.T,
	tbl *catalog.TableEntry,
	end types.TS,
	loc objectio.Location,
	e *TestEngine,
) {
	objCount := 0
	reader := logtail.NewCKPReaderWithTableID_V2(
		logtail.CheckpointCurrentVersion,
		loc,
		tbl.ID,
		common.DebugAllocator,
		e.Opts.Fs,
	)
	err := reader.ReadMeta(ctx)
	assert.NoError(t, err)
	err = reader.ConsumeCheckpointWithTableID(
		ctx,
		func(
			ctx context.Context, obj objectio.ObjectEntry, isTombstone bool,
		) (err error) {
			objCount++
			obj2, err := tbl.GetObjectByID(
				obj.ObjectName().ObjectId(), isTombstone,
			)
			assert.NoError(t, err)
			create2 := obj2.CreatedAt
			assert.True(t, create2.EQ(&obj.CreateTime))
			delete2 := obj2.DeletedAt
			if delete2.GT(&end) {
				return nil
			}
			assert.True(t, delete2.EQ(&obj.DeleteTime))
			return
		},
	)
	assert.NoError(t, err)
	objInCatalogCount := 0
	p := &catalog.LoopProcessor{}
	objFn := func(oe *catalog.ObjectEntry) error {
		if oe.CreatedAt.LE(&end) {
			objInCatalogCount++
		}
		return nil
	}
	p.ObjectFn = objFn
	p.TombstoneFn = objFn
	err = tbl.RecurLoop(p)
	assert.NoError(t, err)
	assert.Equal(t, objCount, objInCatalogCount)
}

// TODO: use ctx
func CheckCheckpointReadWrite(
	t *testing.T,
	end types.TS,
	c *catalog.Catalog,
	checkpointSize int,
	e *TestEngine,
) {
	start := types.TS{}
	ctx := context.Background()
	location, _ := writeIncrementalCheckpoint(ctx, t, start, end, c, checkpointSize, e.Opts.Fs)

	checkTNCheckpointData(ctx, t, location, end, e)
	p := &catalog.LoopProcessor{}

	p.TableFn = func(te *catalog.TableEntry) error {
		checkCheckpointDataByTableID(ctx, t, te, end, location, e)
		return nil
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
