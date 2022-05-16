package txnimpl

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type txnDB struct {
	store       *txnStore
	tables      map[uint64]Table
	database    handle.Database
	createEntry txnif.TxnEntry
	dropEntry   txnif.TxnEntry
	ddlCSN      uint32
}

func newTxnDB(store *txnStore, handle handle.Database) *txnDB {
	db := &txnDB{
		store:    store,
		tables:   make(map[uint64]Table),
		database: handle,
	}
	return db
}

func (db *txnDB) SetCreateEntry(e txnif.TxnEntry) error {
	if db.createEntry != nil {
		panic("logic error")
	}
	db.createEntry = e
	return nil
}

func (db *txnDB) SetDropEntry(e txnif.TxnEntry) error {
	if db.dropEntry != nil {
		panic("logic error")
	}
	if db.createEntry != nil {
		return txnbase.ErrDDLDropCreated
	}
	db.dropEntry = e
	return nil
}

func (db *txnDB) LogTxnEntry(tableId uint64, entry txnif.TxnEntry, readed []*common.ID) (err error) {
	table, err := db.getOrSetTable(tableId)
	if err != nil {
		return
	}
	return table.LogTxnEntry(entry, readed)
}

func (db *txnDB) LogSegmentID(tid, sid uint64) {
	table, _ := db.getOrSetTable(tid)
	table.LogSegmentID(sid)
}

func (db *txnDB) LogBlockID(tid, bid uint64) {
	table, _ := db.getOrSetTable(tid)
	table.LogBlockID(bid)
}

func (db *txnDB) Close() error {
	var err error
	for _, table := range db.tables {
		if err = table.Close(); err != nil {
			break
		}
	}
	db.tables = nil
	db.createEntry = nil
	db.database = nil
	db.dropEntry = nil
	return err
}

func (db *txnDB) BatchDedup(id uint64, pks *vector.Vector) (err error) {
	table, err := db.getOrSetTable(id)
	if err != nil {
		return err
	}
	if table.IsDeleted() {
		return txnbase.ErrNotFound
	}

	return table.BatchDedup(pks)
}

func (db *txnDB) Append(id uint64, data *batch.Batch) error {
	table, err := db.getOrSetTable(id)
	if err != nil {
		return err
	}
	if table.IsDeleted() {
		return txnbase.ErrNotFound
	}
	return table.Append(data)
}

func (db *txnDB) RangeDelete(id *common.ID, start, end uint32) (err error) {
	table, err := db.getOrSetTable(id.TableID)
	if err != nil {
		return err
	}
	if table.IsDeleted() {
		return txnbase.ErrNotFound
	}
	return table.RangeDelete(id.PartID, id.SegmentID, id.BlockID, start, end)
}

func (db *txnDB) GetByFilter(tid uint64, filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	table, err := db.getOrSetTable(tid)
	if err != nil {
		return
	}
	if table.IsDeleted() {
		err = txnbase.ErrNotFound
		return
	}
	return table.GetByFilter(filter)
}

func (db *txnDB) GetValue(id *common.ID, row uint32, colIdx uint16) (v interface{}, err error) {
	table, err := db.getOrSetTable(id.TableID)
	if err != nil {
		return
	}
	if table.IsDeleted() {
		err = txnbase.ErrNotFound
		return
	}
	return table.GetValue(id, row, colIdx)
}

func (db *txnDB) Update(id *common.ID, row uint32, colIdx uint16, v interface{}) (err error) {
	table, err := db.getOrSetTable(id.TableID)
	if err != nil {
		return err
	}
	if table.IsDeleted() {
		return txnbase.ErrNotFound
	}
	return table.Update(id.PartID, id.SegmentID, id.BlockID, row, colIdx, v)
}

func (db *txnDB) CreateRelation(def interface{}) (relation handle.Relation, err error) {
	db.store.IncreateWriteCnt()
	schema := def.(*catalog.Schema)
	dbMeta := db.database.GetMeta().(*catalog.DBEntry)
	var factory catalog.TableDataFactory
	if db.store.dataFactory != nil {
		factory = db.store.dataFactory.MakeTableFactory()
	}
	meta, err := dbMeta.CreateTableEntry(schema, db.store.txn, factory)
	if err != nil {
		return
	}
	table, err := db.getOrSetTable(meta.GetID())
	if err != nil {
		return
	}
	relation = newRelation(db.store.txn, meta)
	table.SetCreateEntry(meta)
	return
}

func (db *txnDB) DropRelationByName(name string) (relation handle.Relation, err error) {
	db.store.IncreateWriteCnt()
	dbMeta := db.database.GetMeta().(*catalog.DBEntry)
	meta, err := dbMeta.DropTableEntry(name, db.store.txn)
	if err != nil {
		return nil, err
	}
	table, err := db.getOrSetTable(meta.GetID())
	if err != nil {
		return nil, err
	}
	relation = newRelation(db.store.txn, meta)
	err = table.SetDropEntry(meta)
	return
}

func (db *txnDB) GetRelationByName(name string) (relation handle.Relation, err error) {
	dbMeta := db.database.GetMeta().(*catalog.DBEntry)
	meta, err := dbMeta.GetTableEntry(name, db.store.txn)
	if err != nil {
		return
	}
	_, err = db.getOrSetTable(meta.GetID())
	if err != nil {
		return
	}
	relation = newRelation(db.store.txn, meta)
	return
}

func (db *txnDB) GetSegment(id *common.ID) (seg handle.Segment, err error) {
	var table Table
	if table, err = db.getOrSetTable(id.TableID); err != nil {
		return
	}
	return table.GetSegment(id.SegmentID)
}

func (db *txnDB) CreateSegment(tid uint64) (seg handle.Segment, err error) {
	var table Table
	if table, err = db.getOrSetTable(tid); err != nil {
		return
	}
	return table.CreateSegment()
}

func (db *txnDB) CreateNonAppendableSegment(tid uint64) (seg handle.Segment, err error) {
	var table Table
	if table, err = db.getOrSetTable(tid); err != nil {
		return
	}
	return table.CreateNonAppendableSegment()
}

func (db *txnDB) getOrSetTable(id uint64) (table Table, err error) {
	table = db.tables[id]
	if table == nil {
		var entry *catalog.TableEntry
		if entry, err = db.database.GetMeta().(*catalog.DBEntry).GetTableEntryByID(id); err != nil {
			return
		}
		relation := newRelation(db.store.txn, entry)
		if db.store.warChecker == nil {
			db.store.warChecker = newWarChecker(db.store.txn, db.store.catalog)
		}
		table = newTxnTable(db.store, relation)
		db.tables[id] = table
	}
	return
}

func (db *txnDB) CreateNonAppendableBlock(id *common.ID) (blk handle.Block, err error) {
	var table Table
	if table, err = db.getOrSetTable(id.TableID); err != nil {
		return
	}
	return table.CreateNonAppendableBlock(id.SegmentID)
}

func (db *txnDB) GetBlock(id *common.ID) (blk handle.Block, err error) {
	var table Table
	if table, err = db.getOrSetTable(id.TableID); err != nil {
		return
	}
	return table.GetBlock(id)
}

func (db *txnDB) CreateBlock(tid, sid uint64) (blk handle.Block, err error) {
	var table Table
	if table, err = db.getOrSetTable(tid); err != nil {
		return
	}
	return table.CreateBlock(sid)
}

func (db *txnDB) SoftDeleteBlock(id *common.ID) (err error) {
	var table Table
	if table, err = db.getOrSetTable(id.TableID); err != nil {
		return
	}
	return table.SoftDeleteBlock(id)
}

func (db *txnDB) SoftDeleteSegment(id *common.ID) (err error) {
	var table Table
	if table, err = db.getOrSetTable(id.TableID); err != nil {
		return
	}
	return table.SoftDeleteSegment(id.SegmentID)
}

func (db *txnDB) ApplyRollback() (err error) {
	if db.createEntry != nil {
		if err = db.createEntry.ApplyRollback(); err != nil {
			return
		}
	}
	for _, table := range db.tables {
		if err = table.ApplyRollback(); err != nil {
			break
		}
	}
	if db.dropEntry != nil {
		if err = db.dropEntry.ApplyRollback(); err != nil {
			return
		}
	}
	return
}

func (db *txnDB) ApplyCommit() (err error) {
	now := time.Now()
	for _, table := range db.tables {
		table.WaitSynced()
	}
	if db.createEntry != nil {
		if err = db.createEntry.ApplyCommit(db.store.cmdMgr.MakeLogIndex(db.ddlCSN)); err != nil {
			return
		}
	}
	for _, table := range db.tables {
		if err = table.ApplyCommit(); err != nil {
			break
		}
	}
	if db.dropEntry != nil {
		if err = db.dropEntry.ApplyCommit(db.store.cmdMgr.MakeLogIndex(db.ddlCSN)); err != nil {
			return
		}
	}
	logutil.Debugf("Txn-%d ApplyCommit Takes %s", db.store.txn.GetID(), time.Since(now))
	return
}

func (db *txnDB) PreCommit() (err error) {
	for _, table := range db.tables {
		if err = table.PreCommitDededup(); err != nil {
			return
		}
	}
	for _, table := range db.tables {
		if err = table.PreCommit(); err != nil {
			panic(err)
		}
	}
	return
}

func (db *txnDB) PrepareCommit() (err error) {
	now := time.Now()
	if db.createEntry != nil {
		if err = db.createEntry.PrepareCommit(); err != nil {
			return
		}
	}
	for _, table := range db.tables {
		if err = table.PrepareCommit(); err != nil {
			break
		}
	}
	for _, table := range db.tables {
		table.ApplyAppend()
	}
	if db.dropEntry != nil {
		if err = db.dropEntry.PrepareCommit(); err != nil {
			return
		}
	}

	logutil.Debugf("Txn-%d PrepareCommit Takes %s", db.store.txn.GetID(), time.Since(now))

	return
}

func (db *txnDB) CollectCmd(cmdMgr *commandManager) (err error) {
	if db.createEntry != nil {
		csn := cmdMgr.GetCSN()
		cmd, err := db.createEntry.MakeCommand(csn)
		if err != nil {
			panic(err)
		}
		cmdMgr.AddCmd(cmd)
		db.ddlCSN = csn
	}
	for _, table := range db.tables {
		if err = table.CollectCmd(cmdMgr); err != nil {
			panic(err)
		}
	}
	if db.dropEntry != nil {
		csn := cmdMgr.GetCSN()
		cmd, err := db.dropEntry.MakeCommand(csn)
		if err != nil {
			panic(err)
		}
		cmdMgr.AddCmd(cmd)
		db.ddlCSN = csn
	}
	return
}

func (db *txnDB) AddTxnEntry(t txnif.TxnEntryType, entry txnif.TxnEntry) {
	// TODO
}

func (db *txnDB) PrepareRollback() error {
	var err error
	if db.createEntry != nil {
		if err := db.createEntry.PrepareRollback(); err != nil {
			return err
		}
	}
	for _, table := range db.tables {
		if err = table.PrepareRollback(); err != nil {
			break
		}
	}
	if db.dropEntry != nil {
		if err := db.dropEntry.PrepareRollback(); err != nil {
			return err
		}
	}

	return err
}
