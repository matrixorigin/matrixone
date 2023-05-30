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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
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

func (t *Table) Stats(ctx context.Context, statsInfoMap any) bool {
	return false
}

func (t *Table) Rows(ctx context.Context) (int64, error) {
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
	return int64(resp.Rows), err
}

func (t *Table) Size(ctx context.Context, columnName string) (int64, error) {
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
		getDNServices(t.engine.cluster),
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

func (t *Table) Update(ctx context.Context, data *batch.Batch) error {
	data.InitZsOne(data.Length())
	shards, err := t.engine.shardPolicy.Batch(
		ctx,
		t.id,
		t.TableDefs,
		data,
		getDNServices(t.engine.cluster),
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
	data.InitZsOne(data.Length())
	shards, err := t.engine.shardPolicy.Batch(
		ctx,
		t.id,
		t.TableDefs,
		data,
		getDNServices(t.engine.cluster),
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

func (t *Table) GetDBID(ctx context.Context) uint64 {
	return 0
}

func (t *Table) MaxAndMinValues(ctx context.Context) ([][2]any, []uint8, error) {
	return nil, nil, nil
}

func (t *Table) GetMetadataScanInfoBytes(ctx context.Context, name string) ([][]byte, error) {
	return nil, nil
}
