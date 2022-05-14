package txnimpl

import (
	"bytes"

	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type txnSysBlock struct {
	*txnBlock
	table   *catalog.TableEntry
	catalog *catalog.Catalog
}

func newSysBlock(txn txnif.AsyncTxn, meta *catalog.BlockEntry) *txnSysBlock {
	blk := &txnSysBlock{
		txnBlock: newBlock(txn, meta),
		table:    meta.GetSegment().GetTable(),
		catalog:  meta.GetSegment().GetTable().GetCatalog(),
	}
	return blk
}

func (blk *txnSysBlock) GetTotalChanges() int                      { panic("not supported") }
func (blk *txnSysBlock) BatchDedup(pks *gvec.Vector) (err error)   { panic("not supported") }
func (blk *txnSysBlock) RangeDelete(start, end uint32) (err error) { panic("not supported") }
func (blk *txnSysBlock) Update(row uint32, col uint16, v interface{}) (err error) {
	panic("not supported")
}

func (blk *txnSysBlock) dbRows() int {
	return blk.catalog.CoarseDBCnt()
}

func (blk *txnSysBlock) tableRows() int {
	rows := 0
	dbIt := blk.catalog.MakeDBIt(true)
	canRead := false
	for dbIt.Valid() {
		db := dbIt.Get().GetPayload().(*catalog.DBEntry)
		db.RLock()
		canRead = db.TxnCanRead(blk.Txn, db.RWMutex)
		db.RUnlock()
		if canRead {
			rows += db.CoarseTableCnt()
		}
		dbIt.Next()
	}
	return rows
}

func (blk *txnSysBlock) processDB(entry *catalog.DBEntry, fn func(*catalog.TableEntry)) {
	canRead := false
	tableIt := entry.MakeTableIt(true)
	for tableIt.Valid() {
		table := tableIt.Get().GetPayload().(*catalog.TableEntry)
		table.RLock()
		canRead = table.TxnCanRead(blk.Txn, table.RWMutex)
		table.RUnlock()
		if canRead {
			fn(table)
		}
		tableIt.Next()
	}
}

func (blk *txnSysBlock) columnRows() int {
	rows := 0
	fn := func(table *catalog.TableEntry) {
		rows += len(table.GetSchema().ColDefs)
	}
	dbIt := blk.catalog.MakeDBIt(true)
	canRead := false
	for dbIt.Valid() {
		db := dbIt.Get().GetPayload().(*catalog.DBEntry)
		db.RLock()
		canRead = db.TxnCanRead(blk.Txn, db.RWMutex)
		db.RUnlock()
		if canRead {
			blk.processDB(db, fn)
		}
		dbIt.Next()
	}
	return rows
}

func (blk *txnSysBlock) Rows() int {
	if blk.table.GetID() == catalog.SystemTable_DB_ID {
		return blk.dbRows()
	} else if blk.table.GetID() == catalog.SystemTable_Table_ID {
		return blk.tableRows()
	} else if blk.table.GetID() == catalog.SystemTable_Columns_ID {
		return blk.columnRows()
	} else {
		panic("not supported")
	}
}

func (blk *txnSysBlock) GetColumnDataById(colIdx int, compressed, decompressed *bytes.Buffer) (view *model.ColumnView, err error) {
	return
}

func (blk *txnSysBlock) GetColumnDataByName(attr string, compressed, decompressed *bytes.Buffer) (view *model.ColumnView, err error) {
	colIdx := blk.entry.GetSchema().GetColIdx(attr)
	return blk.GetColumnDataById(colIdx, compressed, decompressed)
}

func (blk *txnSysBlock) LogTxnEntry(entry txnif.TxnEntry, readed []*common.ID) (err error) {
	panic("not supported")
}
