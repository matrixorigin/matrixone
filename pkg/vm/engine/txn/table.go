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

package txnengine

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type Table struct {
	engine      *Engine
	txnOperator client.TxnOperator
	id          string
}

var _ engine.Relation = new(Table)

func (*Table) Rows() int64 {
	return 1
}

func (*Table) Size(string) int64 {
	return 0
}

func (t *Table) AddTableDef(ctx context.Context, def engine.TableDef) error {

	_, err := doTxnRequest[AddTableDefResp](
		ctx,
		t.engine,
		t.txnOperator.Write,
		t.engine.allNodesShards,
		OpAddTableDef,
		AddTableDefReq{
			TableID: t.id,
			Def:     def,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (t *Table) DelTableDef(ctx context.Context, def engine.TableDef) error {

	_, err := doTxnRequest[DelTableDefResp](
		ctx,
		t.engine,
		t.txnOperator.Write,
		t.engine.allNodesShards,
		OpDelTableDef,
		DelTableDefReq{
			TableID: t.id,
			Def:     def,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (t *Table) Delete(ctx context.Context, vec *vector.Vector, _ string) error {

	clusterDetails, err := t.engine.getClusterDetails()
	if err != nil {
		return err
	}
	shards, err := t.engine.shardPolicy.Vector(
		vec,
		clusterDetails.DNNodes,
	)
	if err != nil {
		return err
	}

	for _, shard := range shards {
		_, err := doTxnRequest[DeleteResp](
			ctx,
			t.engine,
			t.txnOperator.Write,
			thisShard(shard.Shard),
			OpDelete,
			DeleteReq{
				TableID: t.id,
				Vector:  shard.Vector,
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

	resps, err := doTxnRequest[GetPrimaryKeysResp](
		ctx,
		t.engine,
		t.txnOperator.Read,
		t.engine.firstNodeShard,
		OpGetPrimaryKeys,
		GetPrimaryKeysReq{
			TableID: t.id,
		},
	)
	if err != nil {
		return nil, err
	}

	resp := resps[0]

	return resp.Attrs, nil
}

func (t *Table) Ranges(ctx context.Context) ([][]byte, error) {
	clusterDetails, err := t.engine.getClusterDetails()
	if err != nil {
		return nil, err
	}
	nodes := clusterDetails.DNNodes
	shards := make([][]byte, 0, len(nodes))
	for _, node := range nodes {
		shards = append(shards, []byte(node.UUID))
	}
	return shards, nil
}

func (t *Table) TableDefs(ctx context.Context) ([]engine.TableDef, error) {

	resps, err := doTxnRequest[GetTableDefsResp](
		ctx,
		t.engine,
		t.txnOperator.Read,
		t.engine.firstNodeShard,
		OpGetTableDefs,
		GetTableDefsReq{
			TableID: t.id,
		},
	)
	if err != nil {
		return nil, err
	}

	resp := resps[0]

	return resp.Defs, nil
}

func (t *Table) Truncate(ctx context.Context) (uint64, error) {

	resps, err := doTxnRequest[TruncateResp](
		ctx,
		t.engine,
		t.txnOperator.Write,
		t.engine.allNodesShards,
		OpTruncate,
		TruncateReq{
			TableID: t.id,
		},
	)
	if err != nil {
		return 0, err
	}

	var affectedRows int64
	for _, resp := range resps {
		affectedRows += resp.AffectedRows
	}

	return uint64(affectedRows), nil
}

func (t *Table) Update(ctx context.Context, data *batch.Batch) error {

	clusterDetails, err := t.engine.getClusterDetails()
	if err != nil {
		return err
	}

	shards, err := t.engine.shardPolicy.Batch(
		data,
		clusterDetails.DNNodes,
	)
	if err != nil {
		return err
	}

	for _, shard := range shards {
		_, err := doTxnRequest[UpdateResp](
			ctx,
			t.engine,
			t.txnOperator.Write,
			thisShard(shard.Shard),
			OpUpdate,
			UpdateReq{
				TableID: t.id,
				Batch:   shard.Batch,
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *Table) Write(ctx context.Context, data *batch.Batch) error {

	clusterDetails, err := t.engine.getClusterDetails()
	if err != nil {
		return err
	}

	shards, err := t.engine.shardPolicy.Batch(
		data,
		clusterDetails.DNNodes,
	)
	if err != nil {
		return err
	}

	for _, shard := range shards {
		_, err := doTxnRequest[WriteResp](
			ctx,
			t.engine,
			t.txnOperator.Write,
			thisShard(shard.Shard),
			OpWrite,
			WriteReq{
				TableID: t.id,
				Batch:   shard.Batch,
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *Table) GetHideKeys(ctx context.Context) (attrs []*engine.Attribute, err error) {
	//TODO
	return
}
