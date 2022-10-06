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
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
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
	if blk.table.GetID() == pkgcatalog.MO_DATABASE_ID {
		return blk.dbRows()
	} else if blk.table.GetID() == pkgcatalog.MO_TABLES_ID {
		return blk.tableRows()
	} else if blk.table.GetID() == pkgcatalog.MO_COLUMNS_ID {
		return blk.columnRows()
	} else {
		panic("not supported")
	}
}

func FillColumnRow(table *catalog.TableEntry, attr string, colData containers.Vector) {
	schema := table.GetSchema()
	tableID := table.GetID()
	for i, colDef := range table.GetSchema().ColDefs {
		switch attr {
		case pkgcatalog.SystemColAttr_UniqName:
			colData.Append([]byte(fmt.Sprintf("%d-%s", tableID, colDef.Name)))
		case pkgcatalog.SystemColAttr_AccID:
			colData.Append(schema.AcInfo.TenantID)
		case pkgcatalog.SystemColAttr_Name:
			colData.Append([]byte(colDef.Name))
		case pkgcatalog.SystemColAttr_Num:
			colData.Append(int32(i + 1))
		case pkgcatalog.SystemColAttr_Type:
			//colData.Append(int32(colDef.Type.Oid))
			data, _ := types.Encode(colDef.Type)
			colData.Append(data)
		case pkgcatalog.SystemColAttr_DBID:
			colData.Append(table.GetDB().GetID())
		case pkgcatalog.SystemColAttr_DBName:
			colData.Append([]byte(table.GetDB().GetName()))
		case pkgcatalog.SystemColAttr_RelID:
			colData.Append(tableID)
		case pkgcatalog.SystemColAttr_RelName:
			colData.Append([]byte(table.GetSchema().Name))
		case pkgcatalog.SystemColAttr_ConstraintType:
			if table.GetSchema().IsPartOfPK(i) {
				colData.Append([]byte(pkgcatalog.SystemColPKConstraint))
			} else {
				colData.Append([]byte(pkgcatalog.SystemColNoConstraint))
			}
		case pkgcatalog.SystemColAttr_Length:
			colData.Append(int32(colDef.Type.Width))
		case pkgcatalog.SystemColAttr_NullAbility:
			colData.Append(bool2i8(colDef.NullAbility))
		case pkgcatalog.SystemColAttr_HasExpr:
			colData.Append(bool2i8(len(colDef.Default.Expr) > 0))
		case pkgcatalog.SystemColAttr_DefaultExpr:
			colData.Append(colDef.Default.Expr)
		case pkgcatalog.SystemColAttr_IsDropped:
			colData.Append(int8(0))
		case pkgcatalog.SystemColAttr_IsHidden:
			colData.Append(bool2i8(colDef.Hidden))
		case pkgcatalog.SystemColAttr_IsUnsigned:
			v := int8(0)
			switch colDef.Type.Oid {
			case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
				v = int8(1)
			}
			colData.Append(v)
		case pkgcatalog.SystemColAttr_IsAutoIncrement:
			colData.Append(bool2i8(colDef.AutoIncrement))
		case pkgcatalog.SystemColAttr_Comment:
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
	case pkgcatalog.SystemRelAttr_ID:
		colData.Append(table.GetID())
	case pkgcatalog.SystemRelAttr_Name:
		colData.Append([]byte(schema.Name))
	case pkgcatalog.SystemRelAttr_DBName:
		colData.Append([]byte(table.GetDB().GetName()))
	case pkgcatalog.SystemRelAttr_DBID:
		colData.Append(table.GetDB().GetID())
	case pkgcatalog.SystemRelAttr_Comment:
		colData.Append([]byte(table.GetSchema().Comment))
	case pkgcatalog.SystemRelAttr_Partition:
		colData.Append([]byte(table.GetSchema().Partition))
	case pkgcatalog.SystemRelAttr_Persistence:
		colData.Append([]byte(pkgcatalog.SystemPersistRel))
	case pkgcatalog.SystemRelAttr_Kind:
		colData.Append([]byte(table.GetSchema().Relkind))
	case pkgcatalog.SystemRelAttr_CreateSQL:
		colData.Append([]byte(table.GetSchema().Createsql))
	case pkgcatalog.SystemRelAttr_Owner:
		colData.Append(schema.AcInfo.RoleID)
	case pkgcatalog.SystemRelAttr_Creator:
		colData.Append(schema.AcInfo.UserID)
	case pkgcatalog.SystemRelAttr_CreateAt:
		colData.Append(schema.AcInfo.CreateAt)
	case pkgcatalog.SystemRelAttr_AccID:
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
	case pkgcatalog.SystemDBAttr_ID:
		colData.Append(db.GetID())
	case pkgcatalog.SystemDBAttr_Name:
		colData.Append([]byte(db.GetName()))
	case pkgcatalog.SystemDBAttr_CatalogName:
		colData.Append([]byte(pkgcatalog.SystemCatalogName))
	case pkgcatalog.SystemDBAttr_CreateSQL:
		colData.Append([]byte("todosql"))
	case pkgcatalog.SystemDBAttr_Owner:
		colData.Append(db.GetRoleID())
	case pkgcatalog.SystemDBAttr_Creator:
		colData.Append(db.GetUserID())
	case pkgcatalog.SystemDBAttr_CreateAt:
		colData.Append(db.GetCreateAt())
	case pkgcatalog.SystemDBAttr_AccID:
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
	if blk.table.GetID() == pkgcatalog.MO_DATABASE_ID {
		return blk.getDBTableData(colIdx)
	} else if blk.table.GetID() == pkgcatalog.MO_TABLES_ID {
		return blk.getRelTableData(colIdx)
	} else if blk.table.GetID() == pkgcatalog.MO_COLUMNS_ID {
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
