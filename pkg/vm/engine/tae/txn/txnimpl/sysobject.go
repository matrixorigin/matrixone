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

func FillColumnRow(table *catalog.TableEntry, schema *catalog.Schema, attr string, colData containers.Vector) {
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
		case pkgcatalog.SystemColAttr_CPKey:
			packer := types.NewPacker()
			packer.EncodeUint32(schema.AcInfo.TenantID)
			packer.EncodeStringType([]byte(table.GetDB().GetName()))
			packer.EncodeStringType([]byte(schema.Name))
			packer.EncodeStringType([]byte(colDef.Name))
			colData.Append(packer.Bytes(), false)
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
	case pkgcatalog.SystemRelAttr_CPKey:
		packer := types.NewPacker()
		packer.EncodeUint32(schema.AcInfo.TenantID)
		packer.EncodeStringType([]byte(table.GetDB().GetName()))
		packer.EncodeStringType([]byte(schema.Name))
		colData.Append(packer.Bytes(), false)
		packer.Close()
	case pkgcatalog.Row_ID:
		// fill outside of this func
	case pkgcatalog.SystemRelAttr_ExtraInfo:
		colData.Append(schema.MustGetExtraBytes(), false)
	default:
		panic(fmt.Sprintf("unexpected colname %q. if add new catalog def, fill it in this switch", attr))
	}
}

// FillDBRow is used for checkpoint collecting and catalog-tree replaying at the moment.
// As to Logtail and GetColumnDataById, objects in mo_database are the right place to get data.
func FillDBRow(db *catalog.DBEntry, attr string, colData containers.Vector) {
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
	case pkgcatalog.SystemDBAttr_CPKey:
		packer := types.NewPacker()
		packer.EncodeUint32(db.GetTenantID())
		packer.EncodeStringType([]byte(db.GetName()))
		colData.Append(packer.Bytes(), false)
		packer.Close()
	case pkgcatalog.Row_ID:
		// fill outside of this func
	default:
		panic(fmt.Sprintf("unexpected colname %q. if add new catalog def, fill it in this switch", attr))
	}
}
