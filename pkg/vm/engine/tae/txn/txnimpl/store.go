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

package txnimpl

import (
	"context"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/moprobe"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"

	"go.uber.org/zap"
)

var (
	_tracerPool = sync.Pool{
		New: func() any {
			return &txnTracer{}
		},
	}
)

func getTracer() *txnTracer {
	return _tracerPool.Get().(*txnTracer)
}

func putTracer(tracer *txnTracer) {
	tracer.task = nil
	tracer.state = 0
	_tracerPool.Put(tracer)
}

type txnTracer struct {
	state uint8
	task  *trace.Task
	stamp time.Time
}

func (tracer *txnTracer) Trigger(state uint8) {
	switch state {
	case 0: // start preparing wait
		_, tracer.task = trace.NewTask(context.Background(), "1-PreparingWait")
		tracer.stamp = time.Now()
		tracer.state = 0

	case 1: // end preparing wait and start preparing
		if tracer.task != nil && tracer.state == 0 {
			tracer.task.End()
			v2.TxnPreparingWaitDurationHistogram.Observe(time.Since(tracer.stamp).Seconds())
		}
		_, tracer.task = trace.NewTask(context.Background(), "2-Preparing")
		tracer.stamp = time.Now()
		tracer.state = 1

	case 2: // end preparing and start prepare wal wait
		if tracer.task != nil && tracer.state == 1 {
			tracer.task.End()
			v2.TxnPreparingDurationHistogram.Observe(time.Since(tracer.stamp).Seconds())
		}
		_, tracer.task = trace.NewTask(context.Background(), "3-PrepareWalWait")
		tracer.stamp = time.Now()
		tracer.state = 2

	case 3: // end prepare wal wait and start prepare wal
		if tracer.task != nil && tracer.state == 2 {
			tracer.task.End()
			v2.TxnPrepareWalWaitDurationHistogram.Observe(time.Since(tracer.stamp).Seconds())
		}
		_, tracer.task = trace.NewTask(context.Background(), "4-PrepareWal")
		tracer.stamp = time.Now()
		tracer.state = 3

	case 4: // end prepare wal and start prepared wait
		if tracer.task != nil && tracer.state == 3 {
			tracer.task.End()
			v2.TxnPrepareWalDurationHistogram.Observe(time.Since(tracer.stamp).Seconds())
		}
		_, tracer.task = trace.NewTask(context.Background(), "5-PreparedWait")
		tracer.stamp = time.Now()
		tracer.state = 4

	case 5: // end prepared wait and start prepared
		if tracer.task != nil && tracer.state == 4 {
			tracer.task.End()
			v2.TxnPreparedWaitDurationHistogram.Observe(time.Since(tracer.stamp).Seconds())
		}
		_, tracer.task = trace.NewTask(context.Background(), "6-Prepared")
		tracer.stamp = time.Now()
		tracer.state = 5
	}
}

func (tracer *txnTracer) Stop() {
	if tracer.task != nil && tracer.state == 5 {
		tracer.task.End()
		v2.TxnPreparedDurationHistogram.Observe(time.Since(tracer.stamp).Seconds())
	}
	tracer.task = nil
	tracer.state = 0
}

type txnStore struct {
	ctx context.Context
	txnbase.NoopTxnStore
	mu          sync.RWMutex
	rt          *dbutils.Runtime
	dbs         map[uint64]*txnDB
	driver      wal.Driver
	txn         txnif.AsyncTxn
	catalog     *catalog.Catalog
	cmdMgr      *commandManager
	logs        []entry.Entry
	warChecker  *warChecker
	dataFactory *tables.DataFactory
	writeOps    atomic.Uint32
	tracer      *txnTracer

	wg sync.WaitGroup
}

var TxnStoreFactory = func(
	ctx context.Context,
	catalog *catalog.Catalog,
	driver wal.Driver,
	rt *dbutils.Runtime,
	dataFactory *tables.DataFactory,
	maxMessageSize uint64) txnbase.TxnStoreFactory {
	return func() txnif.TxnStore {
		return newStore(ctx, catalog, driver, rt, dataFactory, maxMessageSize)
	}
}

func newStore(
	ctx context.Context,
	catalog *catalog.Catalog,
	driver wal.Driver,
	rt *dbutils.Runtime,
	dataFactory *tables.DataFactory,
	maxMessageSize uint64) *txnStore {
	return &txnStore{
		ctx:         ctx,
		rt:          rt,
		dbs:         make(map[uint64]*txnDB),
		catalog:     catalog,
		cmdMgr:      newCommandManager(driver, maxMessageSize),
		driver:      driver,
		logs:        make([]entry.Entry, 0),
		dataFactory: dataFactory,
		wg:          sync.WaitGroup{},
	}
}

func (store *txnStore) StartTrace() {
	if store.IsReadonly() || store.GetTransactionType() == txnif.TxnType_Heartbeat {
		return
	}
	store.tracer = getTracer()
	store.tracer.Trigger(txnif.TraceStart)
}

func (store *txnStore) EndTrace() {
	if store.tracer == nil {
		return
	}
	tracer := store.tracer
	store.tracer = nil
	tracer.Stop()
	putTracer(tracer)
}

func (store *txnStore) TriggerTrace(state uint8) {
	if store.tracer == nil {
		return
	}
	store.tracer.Trigger(state)
}

func (store *txnStore) GetContext() context.Context    { return store.ctx }
func (store *txnStore) SetContext(ctx context.Context) { store.ctx = ctx }

func (store *txnStore) IsReadonly() bool {
	return store.writeOps.Load() == 0
}

func (store *txnStore) IncreateWriteCnt() int {
	return int(store.writeOps.Add(1))
}

func (store *txnStore) LogTxnEntry(dbId uint64, tableId uint64, entry txnif.TxnEntry, readedObject, readedTombstone []*common.ID) (err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return
	}
	return db.LogTxnEntry(tableId, entry, readedObject, readedTombstone)
}

func (store *txnStore) LogTxnState(sync bool) (logEntry entry.Entry, err error) {
	cmd := txnbase.NewTxnStateCmd(
		store.txn.GetID(),
		store.txn.GetTxnState(false),
		store.txn.GetCommitTS(),
	)
	var buf []byte
	if buf, err = cmd.MarshalBinary(); err != nil {
		return
	}
	logEntry = entry.GetBase()
	logEntry.SetType(IOET_WALEntry_TxnRecord)
	if err = logEntry.SetPayload(buf); err != nil {
		return
	}
	info := &entry.Info{
		Group: wal.GroupC,
	}
	logEntry.SetInfo(info)
	var lsn uint64
	lsn, err = store.driver.AppendEntry(wal.GroupC, logEntry)
	if err != nil {
		return
	}
	if sync {
		err = logEntry.WaitDone()
	}
	logutil.Debugf("LogTxnState LSN=%d, Size=%d", lsn, len(buf))
	return
}

func (store *txnStore) Close() error {
	var err error
	for _, db := range store.dbs {
		if err = db.Close(); err != nil {
			break
		}
	}
	store.dbs = nil
	store.cmdMgr = nil
	store.logs = nil
	store.warChecker = nil
	return err
}

func (store *txnStore) BindTxn(txn txnif.AsyncTxn) {
	store.txn = txn
}

func (store *txnStore) BatchDedup(dbId, id uint64, pk containers.Vector) (err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return err
	}
	// if table.IsDeleted() {
	// 	return txnbase.ErrNotFound
	// }

	return db.BatchDedup(id, pk)
}

func (store *txnStore) Append(ctx context.Context, dbId, id uint64, data *containers.Batch) error {
	store.IncreateWriteCnt()
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return err
	}
	// if db.IsDeleted() {
	// 	return txnbase.ErrNotFound
	// }
	return db.Append(ctx, id, data)
}

func (store *txnStore) AddObjsWithMetaLoc(
	ctx context.Context,
	dbId, tid uint64,
	stats containers.Vector,
) error {
	store.IncreateWriteCnt()
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return err
	}
	return db.AddObjsWithMetaLoc(ctx, tid, stats)
}

func (store *txnStore) RangeDelete(
	id *common.ID, start, end uint32,
	pkVec containers.Vector, dt handle.DeleteType,
) (err error) {
	store.IncreateWriteCnt()
	db, err := store.getOrSetDB(id.DbID)
	if err != nil {
		return err
	}
	return db.RangeDelete(id, start, end, pkVec, dt)
}

func (store *txnStore) TryDeleteByDeltaloc(
	id *common.ID, deltaloc objectio.Location,
) (ok bool, err error) {
	store.IncreateWriteCnt()
	db, err := store.getOrSetDB(id.DbID)
	if err != nil {
		return
	}
	return db.TryDeleteByDeltaloc(id, deltaloc)
}

func (store *txnStore) GetByFilter(ctx context.Context, dbId, tid uint64, filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return
	}
	// if table.IsDeleted() {
	// 	err = txnbase.ErrNotFound
	// 	return
	// }
	return db.GetByFilter(ctx, tid, filter)
}

func (store *txnStore) GetValue(id *common.ID, row uint32, colIdx uint16, skipCheckDelete bool) (v any, isNull bool, err error) {
	db, err := store.getOrSetDB(id.DbID)
	if err != nil {
		return
	}
	// if table.IsDeleted() {
	// 	err = txnbase.ErrNotFound
	// 	return
	// }
	return db.GetValue(id, row, colIdx, skipCheckDelete)
}

func (store *txnStore) DatabaseNames() (names []string) {
	it := newDBIt(store.txn, store.catalog)
	for it.Valid() {
		names = append(names, it.GetCurr().GetName())
		it.Next()
	}
	return
}

func (store *txnStore) UseDatabase(name string) (err error) {
	return err
}

func (store *txnStore) UnsafeGetDatabase(id uint64) (h handle.Database, err error) {
	meta, err := store.catalog.GetDatabaseByID(id)
	if err != nil {
		return
	}
	var db *txnDB
	if db, err = store.getOrSetDB(meta.GetID()); err != nil {
		return
	}
	h = buildDB(db)
	return
}

func (store *txnStore) GetDatabase(name string) (h handle.Database, err error) {
	defer func() {
		if err == moerr.GetOkExpectedEOB() {
			err = moerr.NewBadDBNoCtx(name)
		}
	}()
	meta, err := store.catalog.TxnGetDBEntryByName(name, store.txn)
	if err != nil {
		return
	}
	var db *txnDB
	if db, err = store.getOrSetDB(meta.GetID()); err != nil {
		return
	}
	h = buildDB(db)
	return
}

func (store *txnStore) GetDatabaseByID(id uint64) (h handle.Database, err error) {
	meta, err := store.catalog.TxnGetDBEntryByID(id, store.txn)
	if err != nil {
		return
	}
	var db *txnDB
	if db, err = store.getOrSetDB(meta.GetID()); err != nil {
		return
	}
	h = buildDB(db)
	return
}

func (store *txnStore) CreateDatabase(name, createSql, datTyp string) (h handle.Database, err error) {
	id := store.catalog.NextDB()
	return store.CreateDatabaseWithID(context.Background(), name, createSql, datTyp, id)
}

func (store *txnStore) CreateDatabaseWithID(ctx context.Context, name, createSql, datTyp string, id uint64) (h handle.Database, err error) {
	meta, err := store.catalog.CreateDBEntryWithID(name, createSql, datTyp, id, store.txn)
	if err != nil {
		return nil, err
	}
	var db *txnDB
	if db, err = store.getOrSetDB(meta.GetID()); err != nil {
		return
	}
	if err = db.SetCreateEntry(meta); err != nil {
		return
	}

	// TODO(aptend): write ddl row
	// if err = store.Append(ctx, pkgcatalog.MO_CATALOG_ID, pkgcatalog.MO_DATABASE_ID, nil); err != nil {
	// 	return
	// }
	h = buildDB(db)
	return
}

func (store *txnStore) DropDatabase(name string) (h handle.Database, err error) {
	hasNewEntry, meta, err := store.catalog.DropDBEntryByName(name, store.txn)
	if err != nil {
		return
	}
	db, err := store.getOrSetDB(meta.GetID())
	if err != nil {
		return
	}
	if hasNewEntry {
		if err = db.SetDropEntry(meta); err != nil {
			return
		}
	}
	h = buildDB(db)
	return
}

func (store *txnStore) ObserveTxn(
	visitDatabase func(db any),
	visitTable func(tbl any),
	rotateTable func(aid uint32, dbName, tblName string, dbid, tid uint64, pkSeqnum uint16),
	visitObject func(obj any),
	visitAppend func(bat any, isTombstone bool)) {
	for _, db := range store.dbs {
		if db.createEntry != nil || db.dropEntry != nil {
			visitDatabase(db.entry)
		}
		dbName := db.entry.GetName()
		dbid := db.entry.ID
		for _, tbl := range db.tables {
			schema := tbl.GetLocalSchema(false)
			pkseq := uint16(60001)
			if schema.HasPKOrFakePK() { // view table has no pk or fake pk
				pkseq = schema.GetPrimaryKey().SeqNum
			}
			rotateTable(schema.AcInfo.TenantID,
				dbName, schema.Name,
				dbid, tbl.entry.ID,
				pkseq)
			if tbl.createEntry != nil || tbl.dropEntry != nil {
				visitTable(tbl.entry)
			}
			for _, iTxnEntry := range tbl.txnEntries.entries {
				switch txnEntry := iTxnEntry.(type) {
				case *catalog.ObjectEntry:
					visitObject(txnEntry)
				case *catalog.TableEntry:
					if tbl.createEntry != nil || tbl.dropEntry != nil {
						continue
					}
					visitTable(txnEntry)
				}
			}
			if tbl.dataTable.tableSpace != nil && tbl.dataTable.tableSpace.node != nil {
				anode := tbl.dataTable.tableSpace.node
				schema := anode.table.GetLocalSchema(false)
				bat := &containers.BatchWithVersion{
					Version:    schema.Version,
					NextSeqnum: uint16(schema.Extra.NextColSeqnum),
					Seqnums:    schema.AllSeqnums(),
					Batch:      anode.data,
				}
				visitAppend(bat, false)
			}
			if tbl.tombstoneTable != nil && tbl.tombstoneTable.tableSpace != nil && tbl.tombstoneTable.tableSpace.node != nil {
				anode := tbl.tombstoneTable.tableSpace.node
				schema := anode.table.GetLocalSchema(true)
				bat := &containers.BatchWithVersion{
					Version:    schema.Version,
					NextSeqnum: uint16(schema.Extra.NextColSeqnum),
					Seqnums:    schema.AllSeqnums(),
					Batch:      anode.data,
				}
				visitAppend(bat, true)
			}
		}
	}
}
func (store *txnStore) AddWaitEvent(cnt int) {
	store.wg.Add(cnt)
}
func (store *txnStore) DoneWaitEvent(cnt int) {
	store.wg.Add(-cnt)
}
func (store *txnStore) DropDatabaseByID(id uint64) (h handle.Database, err error) {
	hasNewEntry, meta, err := store.catalog.DropDBEntryByID(id, store.txn)
	if err != nil {
		return
	}
	db, err := store.getOrSetDB(meta.GetID())
	if err != nil {
		return
	}
	if hasNewEntry {
		if err = db.SetDropEntry(meta); err != nil {
			return
		}
	}
	h = buildDB(db)
	return
}

func (store *txnStore) CreateRelation(dbId uint64, def any) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return
	}
	return db.CreateRelation(def)
}

func (store *txnStore) CreateRelationWithTableId(dbId uint64, tableId uint64, def any) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return
	}
	return db.CreateRelationWithTableId(tableId, def)
}

func (store *txnStore) DropRelationByName(dbId uint64, name string) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return nil, err
	}
	return db.DropRelationByName(name)
}

func (store *txnStore) DropRelationByID(dbId uint64, id uint64) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return nil, err
	}
	return db.DropRelationByID(id)
}

func (store *txnStore) UnsafeGetRelation(dbId, id uint64) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return nil, err
	}
	return db.UnsafeGetRelation(id)
}

func (store *txnStore) GetRelationByName(dbId uint64, name string) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return nil, err
	}
	return db.GetRelationByName(name)
}

func (store *txnStore) GetRelationByID(dbId uint64, id uint64) (relation handle.Relation, err error) {
	db, err := store.getOrSetDB(dbId)
	if err != nil {
		return nil, err
	}
	return db.GetRelationByID(id)
}

func (store *txnStore) GetObject(id *common.ID, isTombstone bool) (obj handle.Object, err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(id.DbID); err != nil {
		return
	}
	return db.GetObject(id, isTombstone)
}

func (store *txnStore) CreateObject(dbId, tid uint64, isTombstone bool) (obj handle.Object, err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(dbId); err != nil {
		return
	}
	return db.CreateObject(tid, isTombstone)
}

func (store *txnStore) CreateNonAppendableObject(dbId, tid uint64, isTombstone bool, opt *objectio.CreateObjOpt) (obj handle.Object, err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(dbId); err != nil {
		return
	}
	return db.CreateNonAppendableObject(tid, opt, isTombstone)
}

func (store *txnStore) getOrSetDB(id uint64) (db *txnDB, err error) {
	store.mu.RLock()
	db = store.dbs[id]
	store.mu.RUnlock()
	if db != nil {
		return
	}
	var entry *catalog.DBEntry
	if entry, err = store.catalog.GetDatabaseByID(id); err != nil {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	db = store.dbs[id]
	if db != nil {
		return
	}
	db = newTxnDB(store, entry)
	db.idx = len(store.dbs)
	store.dbs[id] = db
	return
}
func (store *txnStore) UpdateObjectStats(id *common.ID, stats *objectio.ObjectStats, isTombstone bool) error {
	db, err := store.getOrSetDB(id.DbID)
	if err != nil {
		return err
	}
	db.UpdateObjectStats(id, stats, isTombstone)
	return nil
}

func (store *txnStore) SoftDeleteObject(isTombstone bool, id *common.ID) (err error) {
	var db *txnDB
	if db, err = store.getOrSetDB(id.DbID); err != nil {
		return
	}
	perfcounter.Update(store.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Object.SoftDelete.Add(1)
	})
	return db.SoftDeleteObject(id, isTombstone)
}

func (store *txnStore) ApplyRollback() (err error) {
	if store.cmdMgr.GetCSN() != 0 {
		for _, db := range store.dbs {
			if err = db.ApplyRollback(); err != nil {
				break
			}
		}
	}
	store.CleanUp()
	return
}

func (store *txnStore) WaitPrepared(ctx context.Context) (err error) {
	for _, db := range store.dbs {
		if err = db.WaitPrepared(); err != nil {
			return
		}
	}
	moprobe.WithRegion(ctx, moprobe.TxnStoreWaitWALFlush, func() {
		for _, e := range store.logs {
			if err = e.WaitDone(); err != nil {
				break
			}
			e.Free()
		}
	})
	store.wg.Wait()
	return
}

func (store *txnStore) ApplyCommit() (err error) {
	for _, db := range store.dbs {
		if err = db.ApplyCommit(); err != nil {
			break
		}
	}
	store.CleanUp()
	return
}

func (store *txnStore) Freeze() (err error) {
	for _, db := range store.dbs {
		if db.NeedRollback() {
			if err = db.PrepareRollback(); err != nil {
				return
			}
			delete(store.dbs, db.entry.GetID())
		}
		if err = db.Freeze(); err != nil {
			return
		}
	}
	return
}

func (store *txnStore) PrePrepare(ctx context.Context) (err error) {
	for _, db := range store.dbs {
		if err = db.PrePrepare(ctx); err != nil {
			return
		}
	}
	return
}

func (store *txnStore) PrepareCommit() (err error) {
	if store.warChecker != nil {
		if err = store.warChecker.checkAll(
			store.txn.GetPrepareTS()); err != nil {
			return err
		}
	}
	for _, db := range store.dbs {
		if err = db.PrepareCommit(); err != nil {
			break
		}
	}

	return
}

func (store *txnStore) PreApplyCommit() (err error) {
	for _, db := range store.dbs {
		if err = db.PreApplyCommit(); err != nil {
			return
		}
	}
	// logutil.Debugf("Txn-%X PrepareCommit Takes %s", store.txn.GetID(), time.Since(now))
	return
}

func (store *txnStore) PrepareWAL() (err error) {
	if err = store.CollectCmd(); err != nil {
		return
	}

	if store.cmdMgr.GetCSN() == 0 {
		return
	}

	// Apply the record from the command list.
	// Split the commands by max message size.
	for store.cmdMgr.cmd.MoreCmds() {
		logEntry, err := store.cmdMgr.ApplyTxnRecord(store.txn)
		if err != nil {
			return err
		}
		if logEntry != nil {
			store.logs = append(store.logs, logEntry)
		}
	}

	t1 := time.Now()
	t2 := time.Now()
	if t2.Sub(t1) > time.Millisecond*500 {
		logutil.Warn(
			"SLOW-LOG",
			zap.String("txn", store.txn.String()),
			zap.Duration("apply-1pc-commit-duration", t2.Sub(t1)),
		)
	}
	return
}

func (store *txnStore) CollectCmd() (err error) {
	dbs := make([]*txnDB, len(store.dbs))
	for _, db := range store.dbs {
		dbs[db.idx] = db
	}
	for _, db := range dbs {
		if err = db.CollectCmd(store.cmdMgr); err != nil {
			return
		}
	}
	return
}

func (store *txnStore) AddTxnEntry(t txnif.TxnEntryType, entry txnif.TxnEntry) {
	// TODO
}

func (store *txnStore) PrepareRollback() error {
	var err error
	for _, db := range store.dbs {
		if err = db.PrepareRollback(); err != nil {
			break
		}
	}

	return err
}

func (store *txnStore) GetLSN() uint64 { return store.cmdMgr.lsn }

func (store *txnStore) CleanUp() {
	for _, db := range store.dbs {
		db.CleanUp()
	}
}
func (store *txnStore) FillInWorkspaceDeletes(id *common.ID, deletes **nulls.Nulls) error {
	db, err := store.getOrSetDB(id.DbID)
	if err != nil {
		return err
	}
	return db.FillInWorkspaceDeletes(id, deletes)
}

func (store *txnStore) IsDeletedInWorkSpace(id *common.ID, row uint32) (bool, error) {
	db, err := store.getOrSetDB(id.DbID)
	if err != nil {
		return false, err
	}
	return db.IsDeletedInWorkSpace(id, row)
}
