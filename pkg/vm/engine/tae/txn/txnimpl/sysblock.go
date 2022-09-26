// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnimpl

import (
	"bytes"
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
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

func bool2i8(v bool) int8 {
	if v {
		return int8(1)
	} else {
		return int8(0)
	}
}

func (blk *txnSysBlock) isSysTable() bool {
	return isSysTable(blk.table.entry.GetSchema().Name)
}

func (blk *txnSysBlock) GetTotalChanges() int {
	if blk.isSysTable() {
		panic("not supported")
	}
	return blk.txnBlock.GetTotalChanges()
}

func (blk *txnSysBlock) BatchDedup(pks containers.Vector, invisibility *roaring.Bitmap) (err error) {
	if blk.isSysTable() {
		panic("not supported")
	}
	return blk.txnBlock.BatchDedup(pks, invisibility)
}

func (blk *txnSysBlock) RangeDelete(start, end uint32, dt handle.DeleteType) (err error) {
	if blk.isSysTable() {
		panic("not supported")
	}
	return blk.txnBlock.RangeDelete(start, end, dt)
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
	it := newDBIt(blk.Txn, blk.catalog)
	for it.linkIt.Valid() {
		if err = it.GetError(); err != nil && !ignoreErr {
			break
		}
		db := it.GetCurr()
		if err = fn(db); err != nil && !ignoreErr {
			break
		}
		it.Next()
	}
	return
}

func (blk *txnSysBlock) processTable(entry *catalog.DBEntry, fn func(*catalog.TableEntry) error, ignoreErr bool) (err error) {
	txnDB, err := blk.table.store.getOrSetDB(entry.GetID())
	if err != nil {
		return
	}
	it := newRelationIt(txnDB)
	for it.linkIt.Valid() {
		if err = it.GetError(); err != nil && !ignoreErr {
			break
		}
		table := it.GetCurr()
		if err = fn(table); err != nil && !ignoreErr {
			break
		}
		it.Next()
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

func isPrimaryKey(schema *catalog.Schema, colIdx int) bool {
	attrName := schema.ColDefs[colIdx].Name
	switch schema.Name {
	case catalog.SystemTable_Columns_Name:
		return attrName == catalog.SystemColAttr_UniqName
	case catalog.SystemTable_Table_Name:
		return attrName == catalog.SystemRelAttr_ID
	case catalog.SystemTable_DB_Name:
		return attrName == catalog.SystemDBAttr_ID
	}
	return schema.IsPartOfPK(colIdx)
}

func FillColumnRow(table *catalog.TableEntry, attr string, colData containers.Vector) {
	schema := table.GetSchema()
	tableID := table.GetID()
	for i, colDef := range table.GetSchema().ColDefs {
		switch attr {
		case catalog.SystemColAttr_UniqName:
			colData.Append([]byte(fmt.Sprintf("%d-%s", tableID, colDef.Name)))
		case catalog.SystemColAttr_AccID:
			colData.Append(schema.AcInfo.TenantID)
		case catalog.SystemColAttr_Name:
			colData.Append([]byte(colDef.Name))
		case catalog.SystemColAttr_Num:
			colData.Append(int32(i + 1))
		case catalog.SystemColAttr_Type:
			colData.Append(int32(colDef.Type.Oid))
		case catalog.SystemColAttr_DBID:
			colData.Append(table.GetDB().GetID())
		case catalog.SystemColAttr_DBName:
			colData.Append([]byte(table.GetDB().GetName()))
		case catalog.SystemColAttr_RelID:
			colData.Append(tableID)
		case catalog.SystemColAttr_RelName:
			colData.Append([]byte(table.GetSchema().Name))
		case catalog.SystemColAttr_ConstraintType:
			if isPrimaryKey(table.GetSchema(), i) {
				colData.Append([]byte(catalog.SystemColPKConstraint))
			} else {
				colData.Append([]byte(catalog.SystemColNoConstraint))
			}
		case catalog.SystemColAttr_Length:
			colData.Append(int32(colDef.Type.Width))
		case catalog.SystemColAttr_NullAbility:
			colData.Append(bool2i8(colDef.NullAbility))
		case catalog.SystemColAttr_HasExpr:
			colData.Append(int8(0)) // TODO
		case catalog.SystemColAttr_DefaultExpr:
			colData.Append([]byte("")) // TODO
		case catalog.SystemColAttr_IsDropped:
			colData.Append(int8(0)) // TODO
		case catalog.SystemColAttr_IsHidden:
			colData.Append(bool2i8(colDef.Hidden))
		case catalog.SystemColAttr_IsUnsigned:
			v := int8(0)
			switch colDef.Type.Oid {
			case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
				v = int8(1)
			}
			colData.Append(v)
		case catalog.SystemColAttr_IsAutoIncrement:
			colData.Append(bool2i8(colDef.AutoIncrement))
		case catalog.SystemColAttr_Comment:
			colData.Append([]byte(colDef.Comment))
		default:
			panic("unexpected")
		}
	}
}

func (blk *txnSysBlock) getColumnTableData(colIdx int) (view *model.ColumnView, err error) {
	view = model.NewColumnView(blk.Txn.GetStartTS(), colIdx)
	col := catalog.SystemColumnSchema.ColDefs[colIdx]
	colData := containers.MakeVector(col.Type, col.Nullable())
	tableFn := func(table *catalog.TableEntry) error {
		FillColumnRow(table, col.Name, colData)
		return nil
	}
	dbFn := func(db *catalog.DBEntry) error {
		return blk.processTable(db, tableFn, false)
	}
	err = blk.processDB(dbFn, false)
	if err != nil {
		return
	}
	view.SetData(colData)
	return
}

func FillTableRow(table *catalog.TableEntry, attr string, colData containers.Vector) {
	schema := table.GetSchema()
	switch attr {
	case catalog.SystemRelAttr_ID:
		colData.Append(table.GetID())
	case catalog.SystemRelAttr_Name:
		colData.Append([]byte(schema.Name))
	case catalog.SystemRelAttr_DBName:
		colData.Append([]byte(table.GetDB().GetName()))
	case catalog.SystemRelAttr_DBID:
		colData.Append(table.GetDB().GetID())
	case catalog.SystemRelAttr_Comment:
		colData.Append([]byte(table.GetSchema().Comment))
	case catalog.SystemRelAttr_Partition:
		colData.Append([]byte(table.GetSchema().Partition))
	case catalog.SystemRelAttr_Persistence:
		colData.Append([]byte(catalog.SystemPersistRel))
	case catalog.SystemRelAttr_Kind:
		colData.Append([]byte(table.GetSchema().Relkind))
	case catalog.SystemRelAttr_CreateSQL:
		colData.Append([]byte(table.GetSchema().Createsql))
	case catalog.SystemRelAttr_Owner:
		colData.Append(schema.AcInfo.RoleID)
	case catalog.SystemRelAttr_Creator:
		colData.Append(schema.AcInfo.UserID)
	case catalog.SystemRelAttr_CreateAt:
		colData.Append(schema.AcInfo.CreateAt)
	case catalog.SystemRelAttr_AccID:
		colData.Append(schema.AcInfo.TenantID)
	default:
		panic("unexpected")
	}
}

func (blk *txnSysBlock) getRelTableData(colIdx int) (view *model.ColumnView, err error) {
	view = model.NewColumnView(blk.Txn.GetStartTS(), colIdx)
	colDef := catalog.SystemTableSchema.ColDefs[colIdx]
	colData := containers.MakeVector(colDef.Type, colDef.Nullable())
	tableFn := func(table *catalog.TableEntry) error {
		FillTableRow(table, colDef.Name, colData)
		return nil
	}
	dbFn := func(db *catalog.DBEntry) error {
		return blk.processTable(db, tableFn, false)
	}
	if err = blk.processDB(dbFn, false); err != nil {
		return
	}
	view.SetData(colData)
	return
}

func FillDBRow(db *catalog.DBEntry, attr string, colData containers.Vector) {
	switch attr {
	case catalog.SystemDBAttr_ID:
		colData.Append(db.GetID())
	case catalog.SystemDBAttr_Name:
		colData.Append([]byte(db.GetName()))
	case catalog.SystemDBAttr_CatalogName:
		colData.Append([]byte(catalog.SystemCatalogName))
	case catalog.SystemDBAttr_CreateSQL:
		colData.Append([]byte("todosql"))
	case catalog.SystemDBAttr_Owner:
		colData.Append(db.GetRoleID())
	case catalog.SystemDBAttr_Creator:
		colData.Append(db.GetUserID())
	case catalog.SystemDBAttr_CreateAt:
		colData.Append(db.GetCreateAt())
	case catalog.SystemDBAttr_AccID:
		colData.Append(db.GetTenantID())
	default:
		panic("unexpected")
	}
}

func (blk *txnSysBlock) getDBTableData(colIdx int) (view *model.ColumnView, err error) {
	view = model.NewColumnView(blk.Txn.GetStartTS(), colIdx)
	colDef := catalog.SystemDBSchema.ColDefs[colIdx]
	colData := containers.MakeVector(colDef.Type, colDef.Nullable())
	fn := func(db *catalog.DBEntry) error {
		FillDBRow(db, colDef.Name, colData)
		return nil
	}
	if err = blk.processDB(fn, false); err != nil {
		return
	}
	view.SetData(colData)
	return
}

func (blk *txnSysBlock) GetColumnDataById(colIdx int, buffer *bytes.Buffer) (view *model.ColumnView, err error) {
	if !blk.isSysTable() {
		return blk.txnBlock.GetColumnDataById(colIdx, buffer)
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

func (blk *txnSysBlock) GetColumnDataByName(attr string, buffer *bytes.Buffer) (view *model.ColumnView, err error) {
	colIdx := blk.entry.GetSchema().GetColIdx(attr)
	return blk.GetColumnDataById(colIdx, buffer)
}

func (blk *txnSysBlock) LogTxnEntry(entry txnif.TxnEntry, readed []*common.ID) (err error) {
	if !blk.isSysTable() {
		return blk.txnBlock.LogTxnEntry(entry, readed)
	}
	panic("not supported")
}
