package txnimpl

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type txnSysBlock struct {
	*txnBlock
	table   *txnTable
	catalog *catalog.Catalog
}

func newSysBlock(table *txnTable, meta *catalog.BlockEntry) *txnSysBlock {
	blk := &txnSysBlock{
		txnBlock: newBlock(table, meta),
		table:    table,
		catalog:  meta.GetSegment().GetTable().GetCatalog(),
	}
	return blk
}

func (blk *txnSysBlock) isSysTable() bool {
	return sysTableNames[blk.table.entry.GetSchema().Name]
}

func (blk *txnSysBlock) GetTotalChanges() int {
	if blk.isSysTable() {
		panic("not supported")
	}
	return blk.txnBlock.GetTotalChanges()
}

func (blk *txnSysBlock) BatchDedup(pks *movec.Vector, invisibility *roaring.Bitmap) (err error) {
	if blk.isSysTable() {
		panic("not supported")
	}
	return blk.txnBlock.BatchDedup(pks, invisibility)
}

func (blk *txnSysBlock) RangeDelete(start, end uint32) (err error) {
	if blk.isSysTable() {
		panic("not supported")
	}
	return blk.txnBlock.RangeDelete(start, end)
}

func (blk *txnSysBlock) Update(row uint32, col uint16, v any) (err error) {
	if blk.isSysTable() {
		panic("not supported")
	}
	return blk.txnBlock.Update(row, col, v)
}

func (blk *txnSysBlock) dbRows() int {
	return blk.catalog.CoarseDBCnt()
}

func (blk *txnSysBlock) tableRows() int {
	rows := 0
	fn := func(db *catalog.DBEntry) error {
		rows += db.CoarseTableCnt()
		return nil
	}
	_ = blk.processDB(fn, true)
	return rows
}

func (blk *txnSysBlock) processDB(fn func(*catalog.DBEntry) error, ignoreErr bool) (err error) {
	canRead := false
	dbIt := blk.catalog.MakeDBIt(true)
	for dbIt.Valid() {
		db := dbIt.Get().GetPayload().(*catalog.DBEntry)
		db.RLock()
		canRead, err = db.TxnCanRead(blk.Txn, db.RWMutex)
		db.RUnlock()
		if err != nil && !ignoreErr {
			break
		}
		if canRead {
			err = fn(db)
			if err != nil && !ignoreErr {
				break
			}
		}
		dbIt.Next()
	}
	return
}

func (blk *txnSysBlock) processTable(entry *catalog.DBEntry, fn func(*catalog.TableEntry) error, ignoreErr bool) (err error) {
	canRead := false
	tableIt := entry.MakeTableIt(true)
	for tableIt.Valid() {
		table := tableIt.Get().GetPayload().(*catalog.TableEntry)
		table.RLock()
		canRead, err = table.TxnCanRead(blk.Txn, table.RWMutex)
		table.RUnlock()
		if err != nil && !ignoreErr {
			break
		}
		if canRead {
			err = fn(table)
			if err != nil && ignoreErr {
				break
			}
		}
		tableIt.Next()
	}
	return
}

func (blk *txnSysBlock) columnRows() int {
	rows := 0
	fn := func(table *catalog.TableEntry) error {
		rows += len(table.GetSchema().ColDefs)
		return nil
	}
	dbFn := func(db *catalog.DBEntry) error {
		_ = blk.processTable(db, fn, true)
		return nil
	}
	_ = blk.processDB(dbFn, true)
	return rows
}

func (blk *txnSysBlock) Rows() int {
	if !blk.isSysTable() {
		return blk.txnBlock.Rows()
	}
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

func (blk *txnSysBlock) isPrimaryKey(schema *catalog.Schema, colIdx int) bool {
	attrName := schema.ColDefs[colIdx].Name
	switch schema.Name {
	case catalog.SystemTable_Columns_Name:
		return attrName == catalog.SystemColAttr_Name || attrName == catalog.SystemColAttr_DBName || attrName == catalog.SystemColAttr_RelName
	case catalog.SystemTable_Table_Name:
		return attrName == catalog.SystemRelAttr_DBName || attrName == catalog.SystemRelAttr_Name
	case catalog.SystemTable_DB_Name:
		return attrName == catalog.SystemDBAttr_Name
	}
	return schema.IsPartOfPK(colIdx)
}

func (blk *txnSysBlock) getColumnTableData(colIdx int) (view *model.ColumnView, err error) {
	view = model.NewColumnView(blk.Txn.GetStartTS(), colIdx)
	col := catalog.SystemColumnSchema.ColDefs[colIdx]
	colData := movec.New(col.Type)
	tableFn := func(table *catalog.TableEntry) error {
		for i, colDef := range table.GetSchema().ColDefs {
			switch col.Name {
			case catalog.SystemColAttr_Name:
				compute.AppendValue(colData, []byte(colDef.Name))
			case catalog.SystemColAttr_Num:
				compute.AppendValue(colData, int32(i+1))
			case catalog.SystemColAttr_Type:
				compute.AppendValue(colData, int32(colDef.Type.Oid))
			case catalog.SystemColAttr_DBName:
				compute.AppendValue(colData, []byte(table.GetDB().GetName()))
			case catalog.SystemColAttr_RelName:
				compute.AppendValue(colData, []byte(table.GetSchema().Name))
			case catalog.SystemColAttr_ConstraintType:
				if blk.isPrimaryKey(table.GetSchema(), i) {
					compute.AppendValue(colData, []byte(catalog.SystemColPKConstraint))
				} else {
					compute.AppendValue(colData, []byte(catalog.SystemColNoConstraint))
				}
			case catalog.SystemColAttr_Length:
				compute.AppendValue(colData, int32(colDef.Type.Size))
			case catalog.SystemColAttr_NullAbility:
				compute.AppendValue(colData, colDef.NullAbility) // TODO
			case catalog.SystemColAttr_HasExpr:
				compute.AppendValue(colData, int8(0)) // TODO
			case catalog.SystemColAttr_DefaultExpr:
				compute.AppendValue(colData, []byte("")) // TODO
			case catalog.SystemColAttr_IsDropped:
				compute.AppendValue(colData, int8(0)) // TODO
			case catalog.SystemColAttr_IsHidden:
				compute.AppendValue(colData, colDef.Hidden) // TODO
			case catalog.SystemColAttr_IsUnsigned:
				v := int8(0)
				switch colDef.Type.Oid {
				case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
					v = int8(1)
				}
				compute.AppendValue(colData, v) // TODO
			case catalog.SystemColAttr_IsAutoIncrement:
				compute.AppendValue(colData, colDef.AutoIncrement) // TODO
			case catalog.SystemColAttr_Comment:
				compute.AppendValue(colData, []byte(colDef.Comment)) // TODO
			default:
				panic("unexpected")
			}
		}
		return nil
	}
	dbFn := func(db *catalog.DBEntry) error {
		return blk.processTable(db, tableFn, false)
	}
	err = blk.processDB(dbFn, false)
	if err != nil {
		return
	}
	view.AppliedVec = colData
	return
}

func (blk *txnSysBlock) getRelTableData(colIdx int) (view *model.ColumnView, err error) {
	view = model.NewColumnView(blk.Txn.GetStartTS(), colIdx)
	colDef := catalog.SystemTableSchema.ColDefs[colIdx]
	colData := movec.New(colDef.Type)
	tableFn := func(table *catalog.TableEntry) error {
		switch colDef.Name {
		case catalog.SystemRelAttr_Name:
			compute.AppendValue(colData, []byte(table.GetSchema().Name))
		case catalog.SystemRelAttr_DBName:
			compute.AppendValue(colData, []byte(table.GetDB().GetName()))
		case catalog.SystemRelAttr_Comment:
			compute.AppendValue(colData, []byte(table.GetSchema().Comment))
		case catalog.SystemRelAttr_Persistence:
			compute.AppendValue(colData, []byte(catalog.SystemPersistRel))
		case catalog.SystemRelAttr_Kind:
			compute.AppendValue(colData, []byte(catalog.SystemOrdinaryRel))
		case catalog.SystemRelAttr_CreateSQL:
			compute.AppendValue(colData, []byte("todosql"))
		default:
			panic("unexpected")
		}
		return nil
	}
	dbFn := func(db *catalog.DBEntry) error {
		return blk.processTable(db, tableFn, false)
	}
	if err = blk.processDB(dbFn, false); err != nil {
		return
	}
	view.AppliedVec = colData
	return
}

func (blk *txnSysBlock) getDBTableData(colIdx int) (view *model.ColumnView, err error) {
	view = model.NewColumnView(blk.Txn.GetStartTS(), colIdx)
	colDef := catalog.SystemDBSchema.ColDefs[colIdx]
	colData := movec.New(colDef.Type)
	fn := func(db *catalog.DBEntry) error {
		switch colDef.Name {
		case catalog.SystemDBAttr_Name:
			compute.AppendValue(colData, []byte(db.GetName()))
		case catalog.SystemDBAttr_CatalogName:
			compute.AppendValue(colData, []byte(catalog.SystemCatalogName))
		case catalog.SystemDBAttr_CreateSQL:
			compute.AppendValue(colData, []byte("todosql"))
		default:
			panic("unexpected")
		}
		return nil
	}
	if err = blk.processDB(fn, false); err != nil {
		return
	}
	view.AppliedVec = colData
	return
}

func (blk *txnSysBlock) GetColumnDataById(colIdx int, compressed, decompressed *bytes.Buffer) (view *model.ColumnView, err error) {
	if !blk.isSysTable() {
		return blk.txnBlock.GetColumnDataById(colIdx, compressed, decompressed)
	}
	if blk.table.GetID() == catalog.SystemTable_DB_ID {
		return blk.getDBTableData(colIdx)
	} else if blk.table.GetID() == catalog.SystemTable_Table_ID {
		return blk.getRelTableData(colIdx)
	} else if blk.table.GetID() == catalog.SystemTable_Columns_ID {
		return blk.getColumnTableData(colIdx)
	} else {
		panic("not supported")
	}
}

func (blk *txnSysBlock) GetColumnDataByName(attr string, compressed, decompressed *bytes.Buffer) (view *model.ColumnView, err error) {
	colIdx := blk.entry.GetSchema().GetColIdx(attr)
	return blk.GetColumnDataById(colIdx, compressed, decompressed)
}

func (blk *txnSysBlock) LogTxnEntry(entry txnif.TxnEntry, readed []*common.ID) (err error) {
	if !blk.isSysTable() {
		return blk.txnBlock.LogTxnEntry(entry, readed)
	}
	panic("not supported")
}
