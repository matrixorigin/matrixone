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
	"fmt"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func bool2i8(v bool) int8 {
	if v {
		return int8(1)
	} else {
		return int8(0)
	}
}

// WARNING: FillColumnRow, FillTableRow, and FillDBRow are ONLY intended for use in
// testing and dump tools. The generated batch content may have minor discrepancies
// compared to actual data. For example, rel_logical_id is inaccurate as it is directly
// populated with rel_id. Users should be aware of these limitations.

func FillColumnRow(table *catalog.TableEntry, schema *catalog.Schema, attr string, colData containers.Vector) {
	tableID := table.GetID()
	for i, colDef := range schema.ColDefs {
		switch attr {
		case pkgcatalog.SystemColAttr_UniqName:
			colData.MustAppendValue([]byte(fmt.Sprintf("%d-%s", tableID, colDef.Name)), false)
		case pkgcatalog.SystemColAttr_AccID:
			colData.MustAppendValue(schema.AcInfo.TenantID, false)
		case pkgcatalog.SystemColAttr_Name:
			colData.MustAppendValue([]byte(colDef.Name), false)
		case pkgcatalog.SystemColAttr_Num:
			colData.MustAppendValue(int32(i+1), false)
		case pkgcatalog.SystemColAttr_Type:
			//colData.Append(int32(colDef.Type.Oid))
			data, _ := types.Encode(&colDef.Type)
			colData.MustAppendValue(data, false)
		case pkgcatalog.SystemColAttr_DBID:
			colData.MustAppendValue(table.GetDB().GetID(), false)
		case pkgcatalog.SystemColAttr_DBName:
			colData.MustAppendValue([]byte(table.GetDB().GetName()), false)
		case pkgcatalog.SystemColAttr_RelID:
			colData.MustAppendValue(tableID, false)
		case pkgcatalog.SystemColAttr_RelName:
			colData.MustAppendValue([]byte(schema.Name), false)
		case pkgcatalog.SystemColAttr_ConstraintType:
			if colDef.Primary {
				colData.MustAppendValue([]byte(pkgcatalog.SystemColPKConstraint), false)
			} else {
				colData.MustAppendValue([]byte(pkgcatalog.SystemColNoConstraint), false)
			}
		case pkgcatalog.SystemColAttr_Length:
			colData.MustAppendValue(int32(colDef.Type.Width), false)
		case pkgcatalog.SystemColAttr_NullAbility:
			colData.MustAppendValue(bool2i8(!colDef.NullAbility), false)
		case pkgcatalog.SystemColAttr_HasExpr:
			colData.MustAppendValue(bool2i8(len(colDef.Default) > 0), false) // @imlinjunhong says always has Default, expect row_id
		case pkgcatalog.SystemColAttr_DefaultExpr:
			colData.MustAppendValue(colDef.Default, false)
		case pkgcatalog.SystemColAttr_IsDropped:
			colData.MustAppendValue(int8(0), false)
		case pkgcatalog.SystemColAttr_IsHidden:
			colData.MustAppendValue(bool2i8(colDef.Hidden), false)
		case pkgcatalog.SystemColAttr_IsUnsigned:
			colData.MustAppendValue(bool2i8(colDef.Type.IsUInt()), false)
		case pkgcatalog.SystemColAttr_IsAutoIncrement:
			colData.MustAppendValue(bool2i8(colDef.AutoIncrement), false)
		case pkgcatalog.SystemColAttr_Comment:
			colData.MustAppendValue([]byte(colDef.Comment), false)
		case pkgcatalog.SystemColAttr_HasUpdate:
			colData.MustAppendValue(bool2i8(len(colDef.OnUpdate) > 0), false)
		case pkgcatalog.SystemColAttr_IsClusterBy:
			colData.MustAppendValue(bool2i8(colDef.IsClusterBy()), false)
		case pkgcatalog.SystemColAttr_Update:
			colData.MustAppendValue(colDef.OnUpdate, false)
		case pkgcatalog.SystemColAttr_Seqnum:
			colData.MustAppendValue(colDef.SeqNum, false)
		case pkgcatalog.SystemColAttr_EnumValues:
			colData.MustAppendValue([]byte(colDef.EnumValues), false)
		case pkgcatalog.SystemColAttr_HasGenerated:
			colData.MustAppendValue(bool2i8(len(colDef.GeneratedCol) > 0), false)
		case pkgcatalog.SystemColAttr_Generated:
			colData.MustAppendValue(colDef.GeneratedCol, false)
		case pkgcatalog.SystemColAttr_CPKey:
			packer := types.NewPacker()
			packer.EncodeUint32(schema.AcInfo.TenantID)
			packer.EncodeStringType([]byte(table.GetDB().GetName()))
			packer.EncodeStringType([]byte(schema.Name))
			packer.EncodeStringType([]byte(colDef.Name))
			colData.MustAppendValue(packer.Bytes(), false)
			packer.Close()
		case pkgcatalog.Row_ID:
		default:
			panic("unexpected colname. if add new catalog def, fill it in this switch")
		}
	}
}

func FillTableRow(table *catalog.TableEntry, schema *catalog.Schema, attr string, colData containers.Vector) {
	switch attr {
	case pkgcatalog.SystemRelAttr_ID:
		colData.MustAppendValue(table.GetID(), false)
	case pkgcatalog.SystemRelAttr_Name:
		colData.MustAppendValue([]byte(schema.Name), false)
	case pkgcatalog.SystemRelAttr_DBName:
		colData.MustAppendValue([]byte(table.GetDB().GetName()), false)
	case pkgcatalog.SystemRelAttr_DBID:
		colData.MustAppendValue(table.GetDB().GetID(), false)
	case pkgcatalog.SystemRelAttr_Comment:
		colData.MustAppendValue([]byte(schema.Comment), false)
	case pkgcatalog.SystemRelAttr_Partitioned:
		colData.MustAppendValue(schema.Partitioned, false)
	case pkgcatalog.SystemRelAttr_Partition:
		colData.MustAppendValue([]byte(schema.Partition), false)
	case pkgcatalog.SystemRelAttr_Persistence:
		colData.MustAppendValue([]byte(pkgcatalog.SystemPersistRel), false)
	case pkgcatalog.SystemRelAttr_Kind:
		colData.MustAppendValue([]byte(schema.Relkind), false)
	case pkgcatalog.SystemRelAttr_CreateSQL:
		colData.MustAppendValue([]byte(schema.Createsql), false)
	case pkgcatalog.SystemRelAttr_ViewDef:
		colData.MustAppendValue([]byte(schema.View), false)
	case pkgcatalog.SystemRelAttr_Owner:
		colData.MustAppendValue(schema.AcInfo.RoleID, false)
	case pkgcatalog.SystemRelAttr_Creator:
		colData.MustAppendValue(schema.AcInfo.UserID, false)
	case pkgcatalog.SystemRelAttr_CreateAt:
		colData.MustAppendValue(schema.AcInfo.CreateAt, false)
	case pkgcatalog.SystemRelAttr_AccID:
		colData.MustAppendValue(schema.AcInfo.TenantID, false)
	case pkgcatalog.SystemRelAttr_Constraint:
		colData.MustAppendValue(schema.Constraint, false)
	case pkgcatalog.SystemRelAttr_Version:
		colData.MustAppendValue(schema.Version, false)
	case pkgcatalog.SystemRelAttr_CatalogVersion:
		colData.MustAppendValue(schema.CatalogVersion, false)
	case pkgcatalog.SystemRelAttr_CPKey:
		packer := types.NewPacker()
		packer.EncodeUint32(schema.AcInfo.TenantID)
		packer.EncodeStringType([]byte(table.GetDB().GetName()))
		packer.EncodeStringType([]byte(schema.Name))
		colData.MustAppendValue(packer.Bytes(), false)
		packer.Close()
	case pkgcatalog.Row_ID:
		// fill outside of this func
	case pkgcatalog.SystemRelAttr_ExtraInfo:
		colData.MustAppendValue(schema.MustGetExtraBytes(), false)
	case pkgcatalog.SystemRelAttr_LogicalID:
		colData.MustAppendValue(table.GetID(), false)
	default:
		panic(fmt.Sprintf("unexpected colname %q. if add new catalog def, fill it in this switch", attr))
	}
}

func FillDBRow(db *catalog.DBEntry, attr string, colData containers.Vector) {
	switch attr {
	case pkgcatalog.SystemDBAttr_ID:
		colData.MustAppendValue(db.GetID(), false)
	case pkgcatalog.SystemDBAttr_Name:
		colData.MustAppendValue([]byte(db.GetName()), false)
	case pkgcatalog.SystemDBAttr_CatalogName:
		colData.MustAppendValue([]byte(pkgcatalog.SystemCatalogName), false)
	case pkgcatalog.SystemDBAttr_CreateSQL:
		colData.MustAppendValue([]byte(db.GetCreateSql()), false)
	case pkgcatalog.SystemDBAttr_Owner:
		colData.MustAppendValue(db.GetRoleID(), false)
	case pkgcatalog.SystemDBAttr_Creator:
		colData.MustAppendValue(db.GetUserID(), false)
	case pkgcatalog.SystemDBAttr_CreateAt:
		colData.MustAppendValue(db.GetCreateAt(), false)
	case pkgcatalog.SystemDBAttr_AccID:
		colData.MustAppendValue(db.GetTenantID(), false)
	case pkgcatalog.SystemDBAttr_Type:
		colData.MustAppendValue([]byte(db.GetDatType()), false)
	case pkgcatalog.SystemDBAttr_CPKey:
		packer := types.NewPacker()
		packer.EncodeUint32(db.GetTenantID())
		packer.EncodeStringType([]byte(db.GetName()))
		colData.MustAppendValue(packer.Bytes(), false)
		packer.Close()
	case pkgcatalog.Row_ID:
		// fill outside of this func
	default:
		panic(fmt.Sprintf("unexpected colname %q. if add new catalog def, fill it in this switch", attr))
	}
}
