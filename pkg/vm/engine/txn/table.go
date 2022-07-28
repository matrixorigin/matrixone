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
	"bytes"
	"context"
	"encoding/gob"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
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

	_, err := doTxnRequest(
		ctx,
		t.txnOperator.Write,
		t.engine.getDataNodes(),
		txn.TxnMethod_Write,
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

	_, err := doTxnRequest(
		ctx,
		t.txnOperator.Write,
		t.engine.getDataNodes(),
		txn.TxnMethod_Write,
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

	shards, err := t.engine.shardPolicy.Vector(vec, t.engine.getDataNodes())
	if err != nil {
		return err
	}

	for _, shard := range shards {
		_, err := doTxnRequest(
			ctx,
			t.txnOperator.Write,
			shard.Nodes,
			txn.TxnMethod_Write,
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

	resps, err := doTxnRequest(
		ctx,
		t.txnOperator.Read,
		t.engine.getDataNodes()[:1],
		txn.TxnMethod_Read,
		OpGetPrimaryKeys,
		GetPrimaryKeysReq{
			TableID: t.id,
		},
	)
	if err != nil {
		return nil, err
	}

	var resp GetPrimaryKeysResp
	if err := gob.NewDecoder(bytes.NewReader(resps[0])).Decode(&resp); err != nil {
		return nil, err
	}

	return resp.Attrs, nil
}

func (t *Table) Ranges(ctx context.Context) ([][]byte, error) {
	nodes := t.engine.getDataNodes()
	shards := make([][]byte, 0, len(nodes))
	for _, node := range nodes {
		shards = append(shards, []byte(node.UUID))
	}
	return shards, nil
}

func (t *Table) NewReader(
	ctx context.Context,
	parallel int,
	expr *plan.Expr,
	shards [][]byte,
) (readers []engine.Reader, err error) {

	readers = make([]engine.Reader, parallel)
	nodes := t.engine.getDataNodes()

	if len(shards) > 0 {
		uuidSet := make(map[string]bool)
		for _, shard := range shards {
			uuidSet[string(shard)] = true
		}
		filteredNodes := nodes[:0]
		for _, node := range nodes {
			if uuidSet[node.UUID] {
				filteredNodes = append(filteredNodes, node)
			}
		}
		nodes = filteredNodes
	}

	resps, err := doTxnRequest(
		ctx,
		t.txnOperator.Read,
		nodes,
		txn.TxnMethod_Read,
		OpNewTableIter,
		NewTableIterReq{
			TableID: t.id,
			Expr:    expr,
			Shards:  shards,
		},
	)
	if err != nil {
		return nil, err
	}

	iterIDSets := make([][]string, parallel)
	i := 0
	for _, payload := range resps {
		var r NewTableIterResp
		if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&r); err != nil {
			return nil, err
		}
		if r.IterID != "" {
			iterIDSets[i] = append(iterIDSets[i], r.IterID)
			i++
			if i >= parallel {
				// round
				i = 0
			}
		}
	}

	for i, idSet := range iterIDSets {
		if len(idSet) == 0 {
			continue
		}
		reader := &TableReader{
			txnOperator: t.txnOperator,
			ctx:         ctx,
		}
		for _, iterID := range idSet {
			reader.iterInfos = append(reader.iterInfos, IterInfo{
				Node:   nodes[i],
				IterID: iterID,
			})
		}
		readers[i] = reader
	}

	return
}

func (t *Table) Nodes() engine.Nodes {
	return t.engine.Nodes()
}

func (t *Table) TableDefs(ctx context.Context) ([]engine.TableDef, error) {

	resps, err := doTxnRequest(
		ctx,
		t.txnOperator.Read,
		t.engine.getDataNodes()[:1],
		txn.TxnMethod_Read,
		OpGetTableDefs,
		GetTableDefsReq{
			TableID: t.id,
		},
	)
	if err != nil {
		return nil, err
	}

	var resp GetTableDefsResp
	if err := gob.NewDecoder(bytes.NewReader(resps[0])).Decode(&resp); err != nil {
		return nil, err
	}

	return resp.Defs, nil
}

func (t *Table) Truncate(ctx context.Context) (uint64, error) {

	resps, err := doTxnRequest(
		ctx,
		t.txnOperator.Write,
		t.engine.getDataNodes(),
		txn.TxnMethod_Write,
		OpTruncate,
		TruncateReq{
			TableID: t.id,
		},
	)
	if err != nil {
		return 0, err
	}

	var affectedRows int64
	for _, payload := range resps {
		var r TruncateResp
		if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&r); err != nil {
			return uint64(affectedRows), err
		}
		affectedRows += r.AffectedRows
	}

	return uint64(affectedRows), nil
}

func (t *Table) Update(ctx context.Context, data *batch.Batch) error {

	shards, err := t.engine.shardPolicy.Batch(data, t.engine.getDataNodes())
	if err != nil {
		return err
	}

	for _, shard := range shards {
		_, err := doTxnRequest(
			ctx,
			t.txnOperator.Write,
			shard.Nodes,
			txn.TxnMethod_Write,
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

	shards, err := t.engine.shardPolicy.Batch(data, t.engine.getDataNodes())
	if err != nil {
		return err
	}

	for _, shard := range shards {
		_, err := doTxnRequest(
			ctx,
			t.txnOperator.Write,
			shard.Nodes,
			txn.TxnMethod_Write,
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
