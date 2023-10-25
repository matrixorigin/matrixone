// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// GetTableDef Get the complete `plan.TableDef` of the table based on the engine.Relation instance.
// Note: Through the `Relation(*txnTable).getTableDef()` The `plan.TableDef` obtained is incomplete,
// and may lose some key information, such as constraints. It is recommended to use it with caution
func GetTableDef(ctx context.Context, table engine.Relation, dbName, tableName string, sub *plan.SubscriptionMeta) (*plan2.ObjectRef, *plan2.TableDef) {
	tableId := table.GetTableID(ctx)
	engineDefs, err := table.TableDefs(ctx)
	if err != nil {
		return nil, nil
	}

	var clusterByDef *plan2.ClusterByDef
	var cols []*plan2.ColDef
	var schemaVersion uint32
	var defs []*plan2.TableDefType
	var properties []*plan2.Property
	var TableType, Createsql string
	var partitionInfo *plan2.PartitionByDef
	var viewSql *plan2.ViewDef
	var foreignKeys []*plan2.ForeignKeyDef
	var primarykey *plan2.PrimaryKeyDef
	var indexes []*plan2.IndexDef
	var refChildTbls []uint64

	for _, def := range engineDefs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			col := &plan2.ColDef{
				ColId: attr.Attr.ID,
				Name:  attr.Attr.Name,
				Typ: &plan2.Type{
					Id:          int32(attr.Attr.Type.Oid),
					Width:       attr.Attr.Type.Width,
					Scale:       attr.Attr.Type.Scale,
					AutoIncr:    attr.Attr.AutoIncrement,
					Table:       tableName,
					NotNullable: attr.Attr.Default != nil && !attr.Attr.Default.NullAbility,
					Enumvalues:  attr.Attr.EnumVlaues,
				},
				Primary:   attr.Attr.Primary,
				Default:   attr.Attr.Default,
				OnUpdate:  attr.Attr.OnUpdate,
				Comment:   attr.Attr.Comment,
				ClusterBy: attr.Attr.ClusterBy,
				Hidden:    attr.Attr.IsHidden,
				Seqnum:    uint32(attr.Attr.Seqnum),
			}
			// Is it a composite primary key
			//if attr.Attr.Name == catalog.CPrimaryKeyColName {
			//	continue
			//}
			if attr.Attr.ClusterBy {
				clusterByDef = &plan.ClusterByDef{
					Name: attr.Attr.Name,
				}
				//if util.JudgeIsCompositeClusterByColumn(attr.Attr.Name) {
				//	continue
				//}
			}
			cols = append(cols, col)
		} else if pro, ok := def.(*engine.PropertiesDef); ok {
			for _, p := range pro.Properties {
				switch p.Key {
				case catalog.SystemRelAttr_Kind:
					TableType = p.Value
				case catalog.SystemRelAttr_CreateSQL:
					Createsql = p.Value
				default:
				}
				properties = append(properties, &plan2.Property{
					Key:   p.Key,
					Value: p.Value,
				})
			}
		} else if viewDef, ok := def.(*engine.ViewDef); ok {
			viewSql = &plan2.ViewDef{
				View: viewDef.View,
			}
		} else if c, ok := def.(*engine.ConstraintDef); ok {
			for _, ct := range c.Cts {
				switch k := ct.(type) {
				case *engine.IndexDef:
					indexes = k.Indexes
				case *engine.ForeignKeyDef:
					foreignKeys = k.Fkeys
				case *engine.RefChildTableDef:
					refChildTbls = k.Tables
				case *engine.PrimaryKeyDef:
					primarykey = k.Pkey
				case *engine.StreamConfigsDef:
					properties = append(properties, k.Configs...)
				}
			}
		} else if commnetDef, ok := def.(*engine.CommentDef); ok {
			properties = append(properties, &plan2.Property{
				Key:   catalog.SystemRelAttr_Comment,
				Value: commnetDef.Comment,
			})
		} else if partitionDef, ok := def.(*engine.PartitionDef); ok {
			if partitionDef.Partitioned > 0 {
				p := &plan2.PartitionByDef{}
				err = p.UnMarshalPartitionInfo(([]byte)(partitionDef.Partition))
				if err != nil {
					return nil, nil
				}
				partitionInfo = p
			}
		} else if v, ok := def.(*engine.VersionDef); ok {
			schemaVersion = v.Version
		}
	}
	if len(properties) > 0 {
		defs = append(defs, &plan2.TableDefType{
			Def: &plan2.TableDef_DefType_Properties{
				Properties: &plan2.PropertiesDef{
					Properties: properties,
				},
			},
		})
	}

	if primarykey != nil && primarykey.PkeyColName == catalog.CPrimaryKeyColName {
		//cols = append(cols, plan2.MakeHiddenColDefByName(catalog.CPrimaryKeyColName))
		primarykey.CompPkeyCol = plan2.GetColDefFromTable(cols, catalog.CPrimaryKeyColName)
	}
	if clusterByDef != nil && util.JudgeIsCompositeClusterByColumn(clusterByDef.Name) {
		//cols = append(cols, plan2.MakeHiddenColDefByName(clusterByDef.Name))
		clusterByDef.CompCbkeyCol = plan2.GetColDefFromTable(cols, clusterByDef.Name)
	}
	rowIdCol := plan2.MakeRowIdColDef()
	cols = append(cols, rowIdCol)

	//convert
	var subscriptionName string
	var pubAccountId int32 = -1
	if sub != nil {
		subscriptionName = sub.SubName
		pubAccountId = sub.AccountId
		dbName = sub.DbName
	}

	obj := &plan2.ObjectRef{
		SchemaName:       dbName,
		ObjName:          tableName,
		Obj:              int64(tableId),
		SubscriptionName: subscriptionName,
	}
	if pubAccountId != -1 {
		obj.PubInfo = &plan.PubInfo{
			TenantId: pubAccountId,
		}
	}

	tableDef := &plan2.TableDef{
		TblId:        tableId,
		Name:         tableName,
		Cols:         cols,
		Defs:         defs,
		TableType:    TableType,
		Createsql:    Createsql,
		Pkey:         primarykey,
		ViewSql:      viewSql,
		Partition:    partitionInfo,
		Fkeys:        foreignKeys,
		RefChildTbls: refChildTbls,
		ClusterBy:    clusterByDef,
		Indexes:      indexes,
		Version:      schemaVersion,
		IsTemporary:  table.GetEngineType() == engine.Memory,
	}
	return obj, tableDef
}
