package txnimpl

import (
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
	"github.com/sirupsen/logrus"
)

type txnStore struct {
	txnbase.NoopTxnStore
	tables      map[uint64]Table
	driver      wal.Driver
	nodesMgr    base.INodeManager
	dbIndex     map[string]uint64
	tableIndex  map[string]uint64
	txn         txnif.AsyncTxn
	catalog     *catalog.Catalog
	database    handle.Database
	createEntry txnif.TxnEntry
	dropEntry   txnif.TxnEntry
	cmdMgr      *commandManager
	logs        []entry.Entry
	warChecker  *warChecker
	dataFactory *tables.DataFactory
	writeOps    uint32
	ddlCSN      uint32
}

var TxnStoreFactory = func(catalog *catalog.Catalog, driver wal.Driver, txnBufMgr base.INodeManager, dataFactory *tables.DataFactory) txnbase.TxnStoreFactory {
	return func() txnif.TxnStore {
		return newStore(catalog, driver, txnBufMgr, dataFactory)
	}
}

func newStore(catalog *catalog.Catalog, driver wal.Driver, txnBufMgr base.INodeManager, dataFactory *tables.DataFactory) *txnStore {
	return &txnStore{
		tables:      make(map[uint64]Table),
		catalog:     catalog,
		cmdMgr:      newCommandManager(driver),
		driver:      driver,
		logs:        make([]entry.Entry, 0),
		dataFactory: dataFactory,
		nodesMgr:    txnBufMgr,
	}
}

func (store *txnStore) IsReadonly() bool {
	return atomic.LoadUint32(&store.writeOps) == 0
}

func (store *txnStore) IncreateWriteCnt() int {
	return int(atomic.AddUint32(&store.writeOps, uint32(1)))
}

func (store *txnStore) LogTxnEntry(tableId uint64, entry txnif.TxnEntry, readed []*common.ID) (err error) {
	table, err := store.getOrSetTable(tableId)
	if err != nil {
		return
	}
	return table.LogTxnEntry(entry, readed)
}

func (store *txnStore) LogSegmentID(tid, sid uint64) {
	table, _ := store.getOrSetTable(tid)
	table.LogSegmentID(sid)
}

func (store *txnStore) LogBlockID(tid, bid uint64) {
	table, _ := store.getOrSetTable(tid)
	table.LogBlockID(bid)
}

func (store *txnStore) Close() error {
	var err error
	for _, table := range store.tables {
		if err = table.Close(); err != nil {
			break
		}
	}
	store.tables = nil
	store.dbIndex = nil
	store.tableIndex = nil
	store.createEntry = nil
	store.database = nil
	store.dropEntry = nil
	store.cmdMgr = nil
	store.logs = nil
	store.warChecker = nil
	return err
}

func (store *txnStore) BindTxn(txn txnif.AsyncTxn) {
	store.txn = txn
}

func (store *txnStore) BatchDedup(id uint64, pks *vector.Vector) (err error) {
	table, err := store.getOrSetTable(id)
	if err != nil {
		return err
	}
	if table.IsDeleted() {
		return txnbase.ErrNotFound
	}

	return table.BatchDedup(pks)
}

func (store *txnStore) Append(id uint64, data *batch.Batch) error {
	store.IncreateWriteCnt()
	table, err := store.getOrSetTable(id)
	if err != nil {
		return err
	}
	if table.IsDeleted() {
		return txnbase.ErrNotFound
	}
	return table.Append(data)
}

func (store *txnStore) RangeDelete(id *common.ID, start, end uint32) (err error) {
	store.IncreateWriteCnt()
	table, err := store.getOrSetTable(id.TableID)
	if err != nil {
		return err
	}
	if table.IsDeleted() {
		return txnbase.ErrNotFound
	}
	return table.RangeDelete(id.PartID, id.SegmentID, id.BlockID, start, end)
}

func (store *txnStore) GetByFilter(tid uint64, filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	table, err := store.getOrSetTable(tid)
	if err != nil {
		return
	}
	if table.IsDeleted() {
		err = txnbase.ErrNotFound
		return
	}
	return table.GetByFilter(filter)
}

func (store *txnStore) GetValue(id *common.ID, row uint32, colIdx uint16) (v interface{}, err error) {
	table, err := store.getOrSetTable(id.TableID)
	if err != nil {
		return
	}
	if table.IsDeleted() {
		err = txnbase.ErrNotFound
		return
	}
	return table.GetValue(id, row, colIdx)
}

func (store *txnStore) Update(id *common.ID, row uint32, colIdx uint16, v interface{}) (err error) {
	store.IncreateWriteCnt()
	table, err := store.getOrSetTable(id.TableID)
	if err != nil {
		return err
	}
	if table.IsDeleted() {
		return txnbase.ErrNotFound
	}
	return table.Update(id.PartID, id.SegmentID, id.BlockID, row, colIdx, v)
}

func (store *txnStore) CurrentDatabase() (db handle.Database) {
	return store.database
}

func (store *txnStore) UseDatabase(name string) (err error) {
	if err = store.checkDatabase(name); err != nil {
		return
	}
	store.database, err = store.txn.GetDatabase(name)
	return err
}

func (store *txnStore) checkDatabase(name string) (err error) {
	if store.database != nil {
		if store.database.GetName() != name {
			return txnbase.ErrTxnDifferentDatabase
		}
	}
	return
}

func (store *txnStore) GetDatabase(name string) (db handle.Database, err error) {
	if err = store.checkDatabase(name); err != nil {
		return
	}
	meta, err := store.catalog.GetDBEntry(name, store.txn)
	if err != nil {
		return
	}
	db = newDatabase(store.txn, meta)
	store.database = db
	return
}

func (store *txnStore) CreateDatabase(name string) (handle.Database, error) {
	store.IncreateWriteCnt()
	if store.database != nil {
		return nil, txnbase.ErrTxnDifferentDatabase
	}
	meta, err := store.catalog.CreateDBEntry(name, store.txn)
	if err != nil {
		return nil, err
	}
	store.createEntry = meta
	store.database = newDatabase(store.txn, meta)
	return store.database, nil
}

func (store *txnStore) DropDatabase(name string) (db handle.Database, err error) {
	if store.createEntry != nil {
		err = txnbase.ErrDDLDropCreated
		return
	}
	store.IncreateWriteCnt()
	if err = store.checkDatabase(name); err != nil {
		return
	}
	meta, err := store.catalog.DropDBEntry(name, store.txn)
	if err != nil {
		return
	}
	store.dropEntry = meta
	store.database = newDatabase(store.txn, meta)
	return store.database, err
}

func (store *txnStore) CreateRelation(def interface{}) (relation handle.Relation, err error) {
	store.IncreateWriteCnt()
	schema := def.(*catalog.Schema)
	db := store.database.GetMeta().(*catalog.DBEntry)
	var factory catalog.TableDataFactory
	if store.dataFactory != nil {
		factory = store.dataFactory.MakeTableFactory()
	}
	meta, err := db.CreateTableEntry(schema, store.txn, factory)
	if err != nil {
		return
	}
	table, err := store.getOrSetTable(meta.GetID())
	if err != nil {
		return
	}
	relation = newRelation(store.txn, meta)
	table.SetCreateEntry(meta)
	return
}

func (store *txnStore) DropRelationByName(name string) (relation handle.Relation, err error) {
	store.IncreateWriteCnt()
	db := store.database.GetMeta().(*catalog.DBEntry)
	meta, err := db.DropTableEntry(name, store.txn)
	if err != nil {
		return nil, err
	}
	table, err := store.getOrSetTable(meta.GetID())
	if err != nil {
		return nil, err
	}
	relation = newRelation(store.txn, meta)
	err = table.SetDropEntry(meta)
	return
}

func (store *txnStore) GetRelationByName(name string) (relation handle.Relation, err error) {
	db := store.database.GetMeta().(*catalog.DBEntry)
	meta, err := db.GetTableEntry(name, store.txn)
	if err != nil {
		return
	}
	relation = newRelation(store.txn, meta)
	return
}

func (store *txnStore) GetSegment(id *common.ID) (seg handle.Segment, err error) {
	var table Table
	if table, err = store.getOrSetTable(id.TableID); err != nil {
		return
	}
	return table.GetSegment(id.SegmentID)
}

func (store *txnStore) CreateSegment(tid uint64) (seg handle.Segment, err error) {
	store.IncreateWriteCnt()
	var table Table
	if table, err = store.getOrSetTable(tid); err != nil {
		return
	}
	return table.CreateSegment()
}

func (store *txnStore) CreateNonAppendableSegment(tid uint64) (seg handle.Segment, err error) {
	store.IncreateWriteCnt()
	var table Table
	if table, err = store.getOrSetTable(tid); err != nil {
		return
	}
	return table.CreateNonAppendableSegment()
}

func (store *txnStore) getOrSetTable(id uint64) (table Table, err error) {
	table = store.tables[id]
	if table == nil {
		var entry *catalog.TableEntry
		if entry, err = store.database.GetMeta().(*catalog.DBEntry).GetTableEntryByID(id); err != nil {
			return
		}
		relation := newRelation(store.txn, entry)
		if store.warChecker == nil {
			store.warChecker = newWarChecker(store.txn, entry.GetDB())
		}
		table = newTxnTable(store, relation)
		store.tables[id] = table
	}
	return
}

func (store *txnStore) CreateNonAppendableBlock(id *common.ID) (blk handle.Block, err error) {
	store.IncreateWriteCnt()
	var table Table
	if table, err = store.getOrSetTable(id.TableID); err != nil {
		return
	}
	return table.CreateNonAppendableBlock(id.SegmentID)
}

func (store *txnStore) GetBlock(id *common.ID) (blk handle.Block, err error) {
	var table Table
	if table, err = store.getOrSetTable(id.TableID); err != nil {
		return
	}
	return table.GetBlock(id)
}

func (store *txnStore) CreateBlock(tid, sid uint64) (blk handle.Block, err error) {
	store.IncreateWriteCnt()
	var table Table
	if table, err = store.getOrSetTable(tid); err != nil {
		return
	}
	return table.CreateBlock(sid)
}

func (store *txnStore) SoftDeleteBlock(id *common.ID) (err error) {
	store.IncreateWriteCnt()
	var table Table
	if table, err = store.getOrSetTable(id.TableID); err != nil {
		return
	}
	return table.SoftDeleteBlock(id)
}

func (store *txnStore) ApplyRollback() (err error) {
	if store.createEntry != nil {
		if err = store.createEntry.ApplyRollback(); err != nil {
			return
		}
	}
	for _, table := range store.tables {
		if err = table.ApplyRollback(); err != nil {
			break
		}
	}
	if store.dropEntry != nil {
		if err = store.dropEntry.ApplyRollback(); err != nil {
			return
		}
	}
	return
}

func (store *txnStore) ApplyCommit() (err error) {
	now := time.Now()
	for _, e := range store.logs {
		e.WaitDone()
		e.Free()
	}
	for _, table := range store.tables {
		table.WaitSynced()
	}
	if store.createEntry != nil {
		if err = store.createEntry.ApplyCommit(store.cmdMgr.MakeLogIndex(store.ddlCSN)); err != nil {
			return
		}
	}
	for _, table := range store.tables {
		if err = table.ApplyCommit(); err != nil {
			break
		}
	}
	if store.dropEntry != nil {
		if err = store.dropEntry.ApplyCommit(store.cmdMgr.MakeLogIndex(store.ddlCSN)); err != nil {
			return
		}
	}
	logrus.Debugf("Txn-%d ApplyCommit Takes %s", store.txn.GetID(), time.Since(now))
	return
}

func (store *txnStore) PreCommit() (err error) {
	for _, table := range store.tables {
		if err = table.PreCommitDededup(); err != nil {
			return
		}
	}
	for _, table := range store.tables {
		if err = table.PreCommit(); err != nil {
			panic(err)
		}
	}
	return
}

func (store *txnStore) PrepareCommit() (err error) {
	now := time.Now()
	if store.warChecker != nil {
		if err = store.warChecker.check(); err != nil {
			return err
		}
	}
	if store.createEntry != nil {
		if err = store.createEntry.PrepareCommit(); err != nil {
			return
		}
	}
	for _, table := range store.tables {
		if err = table.PrepareCommit(); err != nil {
			break
		}
	}
	for _, table := range store.tables {
		table.ApplyAppend()
	}
	if store.dropEntry != nil {
		if err = store.dropEntry.PrepareCommit(); err != nil {
			return
		}
	}

	store.CollectCmd()

	logEntry, err := store.cmdMgr.ApplyTxnRecord()
	if err != nil {
		panic(err)
	}
	if logEntry != nil {
		store.logs = append(store.logs, logEntry)
	}
	logrus.Debugf("Txn-%d PrepareCommit Takes %s", store.txn.GetID(), time.Since(now))

	return
}

func (store *txnStore) CollectCmd() (err error) {
	if store.createEntry != nil {
		csn := store.cmdMgr.GetCSN()
		cmd, err := store.createEntry.MakeCommand(csn)
		if err != nil {
			panic(err)
		}
		store.cmdMgr.AddCmd(cmd)
		store.ddlCSN = csn
	}
	for _, table := range store.tables {
		if err = table.CollectCmd(store.cmdMgr); err != nil {
			panic(err)
		}
	}
	if store.dropEntry != nil {
		csn := store.cmdMgr.GetCSN()
		cmd, err := store.dropEntry.MakeCommand(csn)
		if err != nil {
			panic(err)
		}
		store.cmdMgr.AddCmd(cmd)
		store.ddlCSN = csn
	}
	return
}

func (store *txnStore) AddTxnEntry(t txnif.TxnEntryType, entry txnif.TxnEntry) {
	// TODO
}

func (store *txnStore) PrepareRollback() error {
	var err error
	if store.createEntry != nil {
		if err := store.createEntry.PrepareRollback(); err != nil {
			return err
		}
	}
	for _, table := range store.tables {
		if err = table.PrepareRollback(); err != nil {
			break
		}
	}
	if store.dropEntry != nil {
		if err := store.dropEntry.PrepareRollback(); err != nil {
			return err
		}
	}

	return err
}
