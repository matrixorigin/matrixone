// Copyright 2022 Matrix Origin
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

package memoryengine

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type Table struct {
	id           ID
	engine       *Engine
	txnOperator  client.TxnOperator
	databaseName string
	tableName    string
}

var _ engine.Relation = new(Table)

func (t *Table) Stats(ctx context.Context, sync bool) *pb.StatsInfo {
	return nil
}

func (t *Table) Rows(ctx context.Context) (uint64, error) {
	resps, err := DoTxnRequest[TableStatsResp](
		ctx,
		t.txnOperator,
		true,
		t.engine.anyShard,
		OpTableStats,
		&TableStatsReq{
			TableID: t.id,
		},
	)
	if err != nil {
		return 0, err
	}
	resp := resps[0]
	return uint64(resp.Rows), err
}

func (t *Table) Size(ctx context.Context, columnName string) (uint64, error) {
	return 1, nil
}

func (t *Table) AddTableDef(ctx context.Context, def engine.TableDef) error {

	_, err := DoTxnRequest[AddTableDefResp](
		ctx,
		t.txnOperator,
		false,
		t.engine.allShards,
		OpAddTableDef,
		&AddTableDefReq{
			TableID:      t.id,
			Def:          def.ToPBVersion(),
			DatabaseName: t.databaseName,
			TableName:    t.tableName,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (t *Table) DelTableDef(ctx context.Context, def engine.TableDef) error {

	_, err := DoTxnRequest[DelTableDefResp](
		ctx,
		t.txnOperator,
		false,
		t.engine.allShards,
		OpDelTableDef,
		&DelTableDefReq{
			TableID:      t.id,
			DatabaseName: t.databaseName,
			TableName:    t.tableName,
			Def:          def.ToPBVersion(),
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (t *Table) Delete(ctx context.Context, bat *batch.Batch, colName string) error {
	if bat == nil {
		return nil
	}
	vec := bat.Vecs[0]
	shards, err := t.engine.shardPolicy.Vector(
		ctx,
		t.id,
		t.TableDefs,
		colName,
		vec,
		getTNServices(t.engine.cluster),
	)
	if err != nil {
		return err
	}

	for _, shard := range shards {
		_, err := DoTxnRequest[DeleteResp](
			ctx,
			t.txnOperator,
			false,
			thisShard(shard.Shard),
			OpDelete,
			&DeleteReq{
				TableID:      t.id,
				DatabaseName: t.databaseName,
				TableName:    t.tableName,
				ColumnName:   colName,
				Vector:       shard.Vector,
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (*Table) GetHideKey() *engine.Attribute {
	return nil
}

func (*Table) GetPriKeyOrHideKey() ([]engine.Attribute, bool) {
	return nil, false
}

func (t *Table) GetPrimaryKeys(ctx context.Context) ([]*engine.Attribute, error) {

	resps, err := DoTxnRequest[GetPrimaryKeysResp](
		ctx,
		t.txnOperator,
		true,
		t.engine.anyShard,
		OpGetPrimaryKeys,
		&GetPrimaryKeysReq{
			TableID: t.id,
		},
	)
	if err != nil {
		return nil, err
	}

	resp := resps[0]

	// convert from []engine.Attribute  to []*engine.Attribute
	attrs := make([]*engine.Attribute, 0, len(resp.Attrs))
	for i := 0; i < len(resp.Attrs); i++ {
		attrs = append(attrs, &resp.Attrs[i])
	}

	return attrs, nil
}

func (t *Table) TableColumns(ctx context.Context) ([]*engine.Attribute, error) {

	resps, err := DoTxnRequest[GetTableColumnsResp](
		ctx,
		t.txnOperator,
		true,
		t.engine.anyShard,
		OpGetTableColumns,
		&GetTableColumnsReq{
			TableID: t.id,
		},
	)
	if err != nil {
		return nil, err
	}

	resp := resps[0]

	// convert from []engine.Attribute  to []*engine.Attribute
	attrs := make([]*engine.Attribute, 0, len(resp.Attrs))
	for i := 0; i < len(resp.Attrs); i++ {
		attrs = append(attrs, &resp.Attrs[i])
	}

	return attrs, nil
}

func (t *Table) TableDefs(ctx context.Context) ([]engine.TableDef, error) {

	resps, err := DoTxnRequest[GetTableDefsResp](
		ctx,
		t.txnOperator,
		true,
		t.engine.anyShard,
		OpGetTableDefs,
		&GetTableDefsReq{
			TableID: t.id,
		},
	)
	if err != nil {
		return nil, err
	}

	resp := resps[0]

	// convert from PB version to interface version
	defs := make([]engine.TableDef, 0, len(resp.Defs))
	for i := 0; i < len(resp.Defs); i++ {
		defs = append(defs, resp.Defs[i].FromPBVersion())
	}

	return defs, nil
}

func (t *Table) CopyTableDef(ctx context.Context) *plan.TableDef {
	return t.GetTableDef(ctx)
}
func (t *Table) GetTableDef(ctx context.Context) *plan.TableDef {
	engineDefs, err := t.TableDefs(ctx)
	if err != nil {
		return nil
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
				Typ: plan2.Type{
					Id:          int32(attr.Attr.Type.Oid),
					Width:       attr.Attr.Type.Width,
					Scale:       attr.Attr.Type.Scale,
					AutoIncr:    attr.Attr.AutoIncrement,
					Table:       t.tableName,
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
			if attr.Attr.ClusterBy {
				clusterByDef = &plan.ClusterByDef{
					Name: attr.Attr.Name,
				}
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
					panic(fmt.Sprintf("cannot unmarshal partition metadata information: %s", err))
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
		primarykey.CompPkeyCol = plan2.GetColDefFromTable(cols, catalog.CPrimaryKeyColName)
	}
	if clusterByDef != nil && util.JudgeIsCompositeClusterByColumn(clusterByDef.Name) {
		clusterByDef.CompCbkeyCol = plan2.GetColDefFromTable(cols, clusterByDef.Name)
	}
	rowIdCol := plan2.MakeRowIdColDef()
	cols = append(cols, rowIdCol)

	tableDef := &plan2.TableDef{
		TblId:        t.GetTableID(ctx),
		Name:         t.tableName,
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
		IsTemporary:  t.GetEngineType() == engine.Memory,
	}
	return tableDef
}

//func (t *Table) Truncate(ctx context.Context) (uint64, error) {
//
//	resps, err := DoTxnRequest[TruncateResp](
//		ctx,
//		t.txnOperator,
//		false,
//		t.engine.allShards,
//		OpTruncate,
//		&TruncateReq{
//			TableID:      t.id,
//			DatabaseName: t.databaseName,
//			TableName:    t.tableName,
//		},
//	)
//	if err != nil {
//		return 0, err
//	}
//
//	var affectedRows int64
//	for _, resp := range resps {
//		affectedRows += resp.AffectedRows
//	}
//
//	return uint64(affectedRows), nil
//}

func (t *Table) UpdateConstraint(context.Context, *engine.ConstraintDef) error {
	// implement me
	return nil
}

func (t *Table) AlterTable(ctx context.Context, c *engine.ConstraintDef, constraint [][]byte) error {
	// implement me
	return nil
}

func (t *Table) TableRenameInTxn(ctx context.Context, constraint [][]byte) error {
	// implement me
	return nil
}

func (t *Table) Update(ctx context.Context, data *batch.Batch) error {
	data.SetRowCount(data.RowCount())
	shards, err := t.engine.shardPolicy.Batch(
		ctx,
		t.id,
		t.TableDefs,
		data,
		getTNServices(t.engine.cluster),
	)
	if err != nil {
		return err
	}

	for _, shard := range shards {
		_, err := DoTxnRequest[UpdateResp](
			ctx,
			t.txnOperator,
			false,
			thisShard(shard.Shard),
			OpUpdate,
			&UpdateReq{
				TableID:      t.id,
				DatabaseName: t.databaseName,
				TableName:    t.tableName,
				Batch:        shard.Batch,
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *Table) Write(ctx context.Context, data *batch.Batch) error {
	data.SetRowCount(data.RowCount())
	shards, err := t.engine.shardPolicy.Batch(
		ctx,
		t.id,
		t.TableDefs,
		data,
		getTNServices(t.engine.cluster),
	)
	if err != nil {
		return err
	}

	for _, shard := range shards {
		_, err := DoTxnRequest[WriteResp](
			ctx,
			t.txnOperator,
			false,
			thisShard(shard.Shard),
			OpWrite,
			&WriteReq{
				TableID:      t.id,
				DatabaseName: t.databaseName,
				TableName:    t.tableName,
				Batch:        shard.Batch,
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *Table) GetHideKeys(ctx context.Context) ([]*engine.Attribute, error) {
	resps, err := DoTxnRequest[GetHiddenKeysResp](
		ctx,
		t.txnOperator,
		true,
		t.engine.anyShard,
		OpGetHiddenKeys,
		&GetHiddenKeysReq{
			TableID: t.id,
		},
	)
	if err != nil {
		return nil, err
	}

	resp := resps[0]

	// convert from []engine.Attribute  to []*engine.Attribute
	attrs := make([]*engine.Attribute, 0, len(resp.Attrs))
	for i := 0; i < len(resp.Attrs); i++ {
		attrs = append(attrs, &resp.Attrs[i])
	}

	return attrs, nil
}

func (t *Table) GetTableID(ctx context.Context) uint64 {
	return uint64(t.id)
}

// GetTableName implements the engine.Relation interface.
func (t *Table) GetTableName() string {
	return t.tableName
}

func (t *Table) GetDBID(ctx context.Context) uint64 {
	return 0
}

func (t *Table) MaxAndMinValues(ctx context.Context) ([][2]any, []uint8, error) {
	return nil, nil, nil
}

func (t *Table) GetColumMetadataScanInfo(ctx context.Context, name string) ([]*plan.MetadataScanInfo, error) {
	return nil, nil
}

func (t *Table) PrimaryKeysMayBeModified(ctx context.Context, from types.TS, to types.TS, keyVector *vector.Vector) (bool, error) {
	return true, nil
}

func (t *Table) ApproxObjectsNum(ctx context.Context) int {
	return 0
}
