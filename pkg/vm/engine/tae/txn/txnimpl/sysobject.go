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
	"context"
	"fmt"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

type txnSysObject struct {
	*txnObject
	table   *txnTable
	catalog *catalog.Catalog
}

func newSysObject(table *txnTable, meta *catalog.ObjectEntry) *txnSysObject {
	obj := &txnSysObject{
		txnObject: newObject(table, meta),
		table:     table,
		catalog:   meta.GetTable().GetCatalog(),
	}
	return obj
}

func bool2i8(v bool) int8 {
	if v {
		return int8(1)
	} else {
		return int8(0)
	}
}

func (obj *txnSysObject) isSysTable() bool {
	return isSysTable(obj.table.entry.GetLastestSchemaLocked().Name)
}

func (obj *txnSysObject) GetTotalChanges() int {
	if obj.isSysTable() {
		panic("not supported")
	}
	return obj.txnObject.GetTotalChanges()
}

func (obj *txnSysObject) RangeDelete(blkID uint16, start, end uint32, dt handle.DeleteType, mp *mpool.MPool) (err error) {
	if obj.isSysTable() {
		panic("not supported")
	}
	return obj.txnObject.RangeDelete(blkID, start, end, dt, mp)
}

func (obj *txnSysObject) processDB(fn func(*catalog.DBEntry) error, ignoreErr bool) (err error) {
	it := newDBIt(obj.Txn, obj.catalog)
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

func (obj *txnSysObject) processTable(entry *catalog.DBEntry, fn func(*catalog.TableEntry) error, ignoreErr bool) (err error) {
	txnDB, err := obj.table.store.getOrSetDB(entry.GetID())
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

func FillColumnRow(table *catalog.TableEntry, node *catalog.MVCCNode[*catalog.TableMVCCNode], attr string, colData containers.Vector) {
	schema := node.BaseNode.Schema
	tableID := table.GetID()
	for i, colDef := range schema.ColDefs {
		switch attr {
		case pkgcatalog.SystemColAttr_UniqName:
			colData.Append([]byte(fmt.Sprintf("%d-%s", tableID, colDef.Name)), false)
		case pkgcatalog.SystemColAttr_AccID:
			colData.Append(schema.AcInfo.TenantID, false)
		case pkgcatalog.SystemColAttr_Name:
			colData.Append([]byte(colDef.Name), false)
		case pkgcatalog.SystemColAttr_Num:
			colData.Append(int32(i+1), false)
		case pkgcatalog.SystemColAttr_Type:
			//colData.Append(int32(colDef.Type.Oid))
			data, _ := types.Encode(&colDef.Type)
			colData.Append(data, false)
		case pkgcatalog.SystemColAttr_DBID:
			colData.Append(table.GetDB().GetID(), false)
		case pkgcatalog.SystemColAttr_DBName:
			colData.Append([]byte(table.GetDB().GetName()), false)
		case pkgcatalog.SystemColAttr_RelID:
			colData.Append(tableID, false)
		case pkgcatalog.SystemColAttr_RelName:
			colData.Append([]byte(schema.Name), false)
		case pkgcatalog.SystemColAttr_ConstraintType:
			if colDef.Primary {
				colData.Append([]byte(pkgcatalog.SystemColPKConstraint), false)
			} else {
				colData.Append([]byte(pkgcatalog.SystemColNoConstraint), false)
			}
		case pkgcatalog.SystemColAttr_Length:
			colData.Append(int32(colDef.Type.Width), false)
		case pkgcatalog.SystemColAttr_NullAbility:
			colData.Append(bool2i8(!colDef.NullAbility), false)
		case pkgcatalog.SystemColAttr_HasExpr:
			colData.Append(bool2i8(len(colDef.Default) > 0), false) // @imlinjunhong says always has Default, expect row_id
		case pkgcatalog.SystemColAttr_DefaultExpr:
			colData.Append(colDef.Default, false)
		case pkgcatalog.SystemColAttr_IsDropped:
			colData.Append(int8(0), false)
		case pkgcatalog.SystemColAttr_IsHidden:
			colData.Append(bool2i8(colDef.Hidden), false)
		case pkgcatalog.SystemColAttr_IsUnsigned:
			colData.Append(bool2i8(colDef.Type.IsUInt()), false)
		case pkgcatalog.SystemColAttr_IsAutoIncrement:
			colData.Append(bool2i8(colDef.AutoIncrement), false)
		case pkgcatalog.SystemColAttr_Comment:
			colData.Append([]byte(colDef.Comment), false)
		case pkgcatalog.SystemColAttr_HasUpdate:
			colData.Append(bool2i8(len(colDef.OnUpdate) > 0), false)
		case pkgcatalog.SystemColAttr_IsClusterBy:
			colData.Append(bool2i8(colDef.IsClusterBy()), false)
		case pkgcatalog.SystemColAttr_Update:
			colData.Append(colDef.OnUpdate, false)
		case pkgcatalog.SystemColAttr_Seqnum:
			colData.Append(colDef.SeqNum, false)
		case pkgcatalog.SystemColAttr_EnumValues:
			colData.Append([]byte(colDef.EnumValues), false)
		default:
			panic("unexpected colname. if add new catalog def, fill it in this switch")
		}
	}
}

// func (obj *txnSysObject) GetDeltaPersistedTS() types.TS {
// 	return types.TS{}.Next()
// }

func (obj *txnSysObject) getColumnTableVec(
	ts types.TS, colIdx int, mp *mpool.MPool,
) (colData containers.Vector, err error) {
	col := catalog.SystemColumnSchema.ColDefs[colIdx]
	colData = containers.MakeVector(col.Type, mp)
	tableFn := func(table *catalog.TableEntry) error {
		table.RLock()
		node := table.GetVisibleNodeLocked(obj.Txn)
		table.RUnlock()
		FillColumnRow(table, node, col.Name, colData)
		return nil
	}
	dbFn := func(db *catalog.DBEntry) error {
		return obj.processTable(db, tableFn, false)
	}
	err = obj.processDB(dbFn, false)
	if err != nil {
		return
	}
	return
}
func (obj *txnSysObject) getColumnTableData(
	colIdx int, mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	ts := obj.Txn.GetStartTS()
	view = containers.NewColumnView(colIdx)
	colData, err := obj.getColumnTableVec(ts, colIdx, mp)
	view.SetData(colData)
	return
}

func FillTableRow(table *catalog.TableEntry, node *catalog.MVCCNode[*catalog.TableMVCCNode], attr string, colData containers.Vector) {
	schema := node.BaseNode.Schema
	switch attr {
	case pkgcatalog.SystemRelAttr_ID:
		colData.Append(table.GetID(), false)
	case pkgcatalog.SystemRelAttr_Name:
		colData.Append([]byte(schema.Name), false)
	case pkgcatalog.SystemRelAttr_DBName:
		colData.Append([]byte(table.GetDB().GetName()), false)
	case pkgcatalog.SystemRelAttr_DBID:
		colData.Append(table.GetDB().GetID(), false)
	case pkgcatalog.SystemRelAttr_Comment:
		colData.Append([]byte(schema.Comment), false)
	case pkgcatalog.SystemRelAttr_Partitioned:
		colData.Append(schema.Partitioned, false)
	case pkgcatalog.SystemRelAttr_Partition:
		colData.Append([]byte(schema.Partition), false)
	case pkgcatalog.SystemRelAttr_Persistence:
		colData.Append([]byte(pkgcatalog.SystemPersistRel), false)
	case pkgcatalog.SystemRelAttr_Kind:
		colData.Append([]byte(schema.Relkind), false)
	case pkgcatalog.SystemRelAttr_CreateSQL:
		colData.Append([]byte(schema.Createsql), false)
	case pkgcatalog.SystemRelAttr_ViewDef:
		colData.Append([]byte(schema.View), false)
	case pkgcatalog.SystemRelAttr_Owner:
		colData.Append(schema.AcInfo.RoleID, false)
	case pkgcatalog.SystemRelAttr_Creator:
		colData.Append(schema.AcInfo.UserID, false)
	case pkgcatalog.SystemRelAttr_CreateAt:
		colData.Append(schema.AcInfo.CreateAt, false)
	case pkgcatalog.SystemRelAttr_AccID:
		colData.Append(schema.AcInfo.TenantID, false)
	case pkgcatalog.SystemRelAttr_Constraint:
		colData.Append(schema.Constraint, false)
	case pkgcatalog.SystemRelAttr_Version:
		colData.Append(schema.Version, false)
	case pkgcatalog.SystemRelAttr_CatalogVersion:
		colData.Append(schema.CatalogVersion, false)
	case catalog.AccountIDDbNameTblName:
		packer := types.NewPacker(common.WorkspaceAllocator)
		packer.EncodeUint32(schema.AcInfo.TenantID)
		packer.EncodeStringType([]byte(table.GetDB().GetName()))
		packer.EncodeStringType([]byte(schema.Name))
		colData.Append(packer.Bytes(), false)
		packer.FreeMem()
	default:
		panic("unexpected colname. if add new catalog def, fill it in this switch")
	}
}

func (obj *txnSysObject) getRelTableVec(ts types.TS, colIdx int, mp *mpool.MPool) (colData containers.Vector, err error) {
	colDef := catalog.SystemTableSchema.ColDefs[colIdx]
	colData = containers.MakeVector(colDef.Type, mp)
	tableFn := func(table *catalog.TableEntry) error {
		table.RLock()
		node := table.GetVisibleNodeLocked(obj.Txn)
		table.RUnlock()
		FillTableRow(table, node, colDef.Name, colData)
		return nil
	}
	dbFn := func(db *catalog.DBEntry) error {
		return obj.processTable(db, tableFn, false)
	}
	if err = obj.processDB(dbFn, false); err != nil {
		return
	}
	return
}

func (obj *txnSysObject) getRelTableData(colIdx int, mp *mpool.MPool) (view *containers.ColumnView, err error) {
	ts := obj.Txn.GetStartTS()
	view = containers.NewColumnView(colIdx)
	colData, err := obj.getRelTableVec(ts, colIdx, mp)
	view.SetData(colData)
	return
}

func FillDBRow(db *catalog.DBEntry, _ *catalog.MVCCNode[*catalog.EmptyMVCCNode], attr string, colData containers.Vector) {
	switch attr {
	case pkgcatalog.SystemDBAttr_ID:
		colData.Append(db.GetID(), false)
	case pkgcatalog.SystemDBAttr_Name:
		colData.Append([]byte(db.GetName()), false)
	case pkgcatalog.SystemDBAttr_CatalogName:
		colData.Append([]byte(pkgcatalog.SystemCatalogName), false)
	case pkgcatalog.SystemDBAttr_CreateSQL:
		colData.Append([]byte(db.GetCreateSql()), false)
	case pkgcatalog.SystemDBAttr_Owner:
		colData.Append(db.GetRoleID(), false)
	case pkgcatalog.SystemDBAttr_Creator:
		colData.Append(db.GetUserID(), false)
	case pkgcatalog.SystemDBAttr_CreateAt:
		colData.Append(db.GetCreateAt(), false)
	case pkgcatalog.SystemDBAttr_AccID:
		colData.Append(db.GetTenantID(), false)
	case pkgcatalog.SystemDBAttr_Type:
		colData.Append([]byte(db.GetDatType()), false)
	case catalog.AccountIDDbName:
		packer := types.NewPacker(common.WorkspaceAllocator)
		packer.EncodeUint32(db.GetTenantID())
		packer.EncodeStringType([]byte(db.GetName()))
		colData.Append(packer.Bytes(), false)
		packer.FreeMem()
	default:
		panic("unexpected colname. if add new catalog def, fill it in this switch")
	}
}
func (obj *txnSysObject) getDBTableVec(colIdx int, mp *mpool.MPool) (colData containers.Vector, err error) {
	colDef := catalog.SystemDBSchema.ColDefs[colIdx]
	colData = containers.MakeVector(colDef.Type, mp)
	fn := func(db *catalog.DBEntry) error {
		FillDBRow(db, nil, colDef.Name, colData)
		return nil
	}
	if err = obj.processDB(fn, false); err != nil {
		return
	}
	return
}
func (obj *txnSysObject) getDBTableData(
	colIdx int, mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	view = containers.NewColumnView(colIdx)
	colData, err := obj.getDBTableVec(colIdx, mp)
	view.SetData(colData)
	return
}

func (obj *txnSysObject) GetColumnDataById(
	ctx context.Context, blkID uint16, colIdx int, mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	if !obj.isSysTable() {
		return obj.txnObject.GetColumnDataById(ctx, blkID, colIdx, mp)
	}
	if obj.table.GetID() == pkgcatalog.MO_DATABASE_ID {
		return obj.getDBTableData(colIdx, mp)
	} else if obj.table.GetID() == pkgcatalog.MO_TABLES_ID {
		return obj.getRelTableData(colIdx, mp)
	} else if obj.table.GetID() == pkgcatalog.MO_COLUMNS_ID {
		return obj.getColumnTableData(colIdx, mp)
	} else {
		panic("not supported")
	}
}

func (obj *txnSysObject) Prefetch(idxes []int) error {
	return nil
}

func (obj *txnSysObject) GetColumnDataByName(
	ctx context.Context, blkID uint16, attr string, mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	colIdx := obj.entry.GetSchema().GetColIdx(attr)
	return obj.GetColumnDataById(ctx, blkID, colIdx, mp)
}

func (obj *txnSysObject) GetColumnDataByNames(
	ctx context.Context, blkID uint16, attrs []string, mp *mpool.MPool,
) (view *containers.BlockView, err error) {
	if !obj.isSysTable() {
		return obj.txnObject.GetColumnDataByNames(ctx, blkID, attrs, mp)
	}
	view = containers.NewBlockView()
	ts := obj.Txn.GetStartTS()
	switch obj.table.GetID() {
	case pkgcatalog.MO_DATABASE_ID:
		for _, attr := range attrs {
			colIdx := obj.entry.GetSchema().GetColIdx(attr)
			vec, err := obj.getDBTableVec(colIdx, mp)
			view.SetData(colIdx, vec)
			if err != nil {
				return view, err
			}
		}
	case pkgcatalog.MO_TABLES_ID:
		for _, attr := range attrs {
			colIdx := obj.entry.GetSchema().GetColIdx(attr)
			vec, err := obj.getRelTableVec(ts, colIdx, mp)
			view.SetData(colIdx, vec)
			if err != nil {
				return view, err
			}
		}
	case pkgcatalog.MO_COLUMNS_ID:
		for _, attr := range attrs {
			colIdx := obj.entry.GetSchema().GetColIdx(attr)
			vec, err := obj.getColumnTableVec(ts, colIdx, mp)
			view.SetData(colIdx, vec)
			if err != nil {
				return view, err
			}
		}
	default:
		panic("not supported")
	}
	return
}
