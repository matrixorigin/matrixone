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
			containers.AppendValue(colData, []byte(fmt.Sprintf("%d-%s", tableID, colDef.Name)), false)
		case pkgcatalog.SystemColAttr_AccID:
			containers.AppendValue(colData, schema.AcInfo.TenantID, false)
		case pkgcatalog.SystemColAttr_Name:
			containers.AppendValue(colData, []byte(colDef.Name), false)
		case pkgcatalog.SystemColAttr_Num:
			containers.AppendValue(colData, int32(i+1), false)
		case pkgcatalog.SystemColAttr_Type:
			//colData.Append(int32(colDef.Type.Oid))
			data, _ := types.Encode(&colDef.Type)
			containers.AppendValue(colData, data, false)
		case pkgcatalog.SystemColAttr_DBID:
			containers.AppendValue(colData, table.GetDB().GetID(), false)
		case pkgcatalog.SystemColAttr_DBName:
			containers.AppendValue(colData, []byte(table.GetDB().GetName()), false)
		case pkgcatalog.SystemColAttr_RelID:
			containers.AppendValue(colData, tableID, false)
		case pkgcatalog.SystemColAttr_RelName:
			containers.AppendValue(colData, []byte(schema.Name), false)
		case pkgcatalog.SystemColAttr_ConstraintType:
			if colDef.Primary {
				containers.AppendValue(colData, []byte(pkgcatalog.SystemColPKConstraint), false)
			} else {
				containers.AppendValue(colData, []byte(pkgcatalog.SystemColNoConstraint), false)
			}
		case pkgcatalog.SystemColAttr_Length:
			containers.AppendValue(colData, int32(colDef.Type.Width), false)
		case pkgcatalog.SystemColAttr_NullAbility:
			containers.AppendValue(colData, bool2i8(!colDef.NullAbility), false)
		case pkgcatalog.SystemColAttr_HasExpr:
			containers.AppendValue(colData, bool2i8(len(colDef.Default) > 0), false) // @imlinjunhong says always has Default, expect row_id
		case pkgcatalog.SystemColAttr_DefaultExpr:
			containers.AppendValue(colData, colDef.Default, false)
		case pkgcatalog.SystemColAttr_IsDropped:
			containers.AppendValue(colData, int8(0), false)
		case pkgcatalog.SystemColAttr_IsHidden:
			containers.AppendValue(colData, bool2i8(colDef.Hidden), false)
		case pkgcatalog.SystemColAttr_IsUnsigned:
			containers.AppendValue(colData, bool2i8(colDef.Type.IsUInt()), false)
		case pkgcatalog.SystemColAttr_IsAutoIncrement:
			containers.AppendValue(colData, bool2i8(colDef.AutoIncrement), false)
		case pkgcatalog.SystemColAttr_Comment:
			containers.AppendValue(colData, []byte(colDef.Comment), false)
		case pkgcatalog.SystemColAttr_HasUpdate:
			containers.AppendValue(colData, bool2i8(len(colDef.OnUpdate) > 0), false)
		case pkgcatalog.SystemColAttr_IsClusterBy:
			containers.AppendValue(colData, bool2i8(colDef.IsClusterBy()), false)
		case pkgcatalog.SystemColAttr_Update:
			containers.AppendValue(colData, colDef.OnUpdate, false)
		case pkgcatalog.SystemColAttr_Seqnum:
			containers.AppendValue(colData, colDef.SeqNum, false)
		case pkgcatalog.SystemColAttr_EnumValues:
			containers.AppendValue(colData, []byte(colDef.EnumValues), false)
		case pkgcatalog.SystemColAttr_HasGenerated:
			containers.AppendValue(colData, bool2i8(len(colDef.GeneratedCol) > 0), false)
		case pkgcatalog.SystemColAttr_Generated:
			containers.AppendValue(colData, colDef.GeneratedCol, false)
		case pkgcatalog.SystemColAttr_CPKey:
			packer := types.NewPacker()
			packer.EncodeUint32(schema.AcInfo.TenantID)
			packer.EncodeStringType([]byte(table.GetDB().GetName()))
			packer.EncodeStringType([]byte(schema.Name))
			packer.EncodeStringType([]byte(colDef.Name))
			containers.AppendValue(colData, packer.Bytes(), false)
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
		containers.AppendValue(colData, table.GetID(), false)
	case pkgcatalog.SystemRelAttr_Name:
		containers.AppendValue(colData, []byte(schema.Name), false)
	case pkgcatalog.SystemRelAttr_DBName:
		containers.AppendValue(colData, []byte(table.GetDB().GetName()), false)
	case pkgcatalog.SystemRelAttr_DBID:
		containers.AppendValue(colData, table.GetDB().GetID(), false)
	case pkgcatalog.SystemRelAttr_Comment:
		containers.AppendValue(colData, []byte(schema.Comment), false)
	case pkgcatalog.SystemRelAttr_Partitioned:
		containers.AppendValue(colData, schema.Partitioned, false)
	case pkgcatalog.SystemRelAttr_Partition:
		containers.AppendValue(colData, []byte(schema.Partition), false)
	case pkgcatalog.SystemRelAttr_Persistence:
		containers.AppendValue(colData, []byte(pkgcatalog.SystemPersistRel), false)
	case pkgcatalog.SystemRelAttr_Kind:
		containers.AppendValue(colData, []byte(schema.Relkind), false)
	case pkgcatalog.SystemRelAttr_CreateSQL:
		containers.AppendValue(colData, []byte(schema.Createsql), false)
	case pkgcatalog.SystemRelAttr_ViewDef:
		containers.AppendValue(colData, []byte(schema.View), false)
	case pkgcatalog.SystemRelAttr_Owner:
		containers.AppendValue(colData, schema.AcInfo.RoleID, false)
	case pkgcatalog.SystemRelAttr_Creator:
		containers.AppendValue(colData, schema.AcInfo.UserID, false)
	case pkgcatalog.SystemRelAttr_CreateAt:
		containers.AppendValue(colData, schema.AcInfo.CreateAt, false)
	case pkgcatalog.SystemRelAttr_AccID:
		containers.AppendValue(colData, schema.AcInfo.TenantID, false)
	case pkgcatalog.SystemRelAttr_Constraint:
		containers.AppendValue(colData, schema.Constraint, false)
	case pkgcatalog.SystemRelAttr_Version:
		containers.AppendValue(colData, schema.Version, false)
	case pkgcatalog.SystemRelAttr_CatalogVersion:
		containers.AppendValue(colData, schema.CatalogVersion, false)
	case pkgcatalog.SystemRelAttr_CPKey:
		packer := types.NewPacker()
		packer.EncodeUint32(schema.AcInfo.TenantID)
		packer.EncodeStringType([]byte(table.GetDB().GetName()))
		packer.EncodeStringType([]byte(schema.Name))
		containers.AppendValue(colData, packer.Bytes(), false)
		packer.Close()
	case pkgcatalog.Row_ID:
		// fill outside of this func
	case pkgcatalog.SystemRelAttr_ExtraInfo:
		containers.AppendValue(colData, schema.MustGetExtraBytes(), false)
	case pkgcatalog.SystemRelAttr_LogicalID:
		containers.AppendValue(colData, table.GetID(), false)
	default:
		panic(fmt.Sprintf("unexpected colname %q. if add new catalog def, fill it in this switch", attr))
	}
}

func FillDBRow(db *catalog.DBEntry, attr string, colData containers.Vector) {
	switch attr {
	case pkgcatalog.SystemDBAttr_ID:
		containers.AppendValue(colData, db.GetID(), false)
	case pkgcatalog.SystemDBAttr_Name:
		containers.AppendValue(colData, []byte(db.GetName()), false)
	case pkgcatalog.SystemDBAttr_CatalogName:
		containers.AppendValue(colData, []byte(pkgcatalog.SystemCatalogName), false)
	case pkgcatalog.SystemDBAttr_CreateSQL:
		containers.AppendValue(colData, []byte(db.GetCreateSql()), false)
	case pkgcatalog.SystemDBAttr_Owner:
		containers.AppendValue(colData, db.GetRoleID(), false)
	case pkgcatalog.SystemDBAttr_Creator:
		containers.AppendValue(colData, db.GetUserID(), false)
	case pkgcatalog.SystemDBAttr_CreateAt:
		containers.AppendValue(colData, db.GetCreateAt(), false)
	case pkgcatalog.SystemDBAttr_AccID:
		containers.AppendValue(colData, db.GetTenantID(), false)
	case pkgcatalog.SystemDBAttr_Type:
		containers.AppendValue(colData, []byte(db.GetDatType()), false)
	case pkgcatalog.SystemDBAttr_CPKey:
		packer := types.NewPacker()
		packer.EncodeUint32(db.GetTenantID())
		packer.EncodeStringType([]byte(db.GetName()))
		containers.AppendValue(colData, packer.Bytes(), false)
		packer.Close()
	case pkgcatalog.Row_ID:
		// fill outside of this func
	default:
		panic(fmt.Sprintf("unexpected colname %q. if add new catalog def, fill it in this switch", attr))
	}
}
