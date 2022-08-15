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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type Database struct {
	engine      *Engine
	txnOperator client.TxnOperator

	id string
}

var _ engine.Database = new(Database)

func (d *Database) Create(ctx context.Context, relName string, defs []engine.TableDef) error {

	_, err := doTxnRequest[CreateDatabaseResp](
		ctx,
		d.engine,
		d.txnOperator.Write,
		d.engine.allNodesShards,
		OpCreateRelation,
		CreateRelationReq{
			DatabaseID: d.id,
			Type:       RelationTable,
			Name:       relName,
			Defs:       defs,
		},
	)
	if err != nil {
		return nil
	}

	return nil
}

func (d *Database) Delete(ctx context.Context, relName string) error {

	_, err := doTxnRequest[DeleteRelationResp](
		ctx,
		d.engine,
		d.txnOperator.Write,
		d.engine.allNodesShards,
		OpDeleteRelation,
		DeleteRelationReq{
			DatabaseID: d.id,
			Name:       relName,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (d *Database) Relation(ctx context.Context, relName string) (engine.Relation, error) {

	resps, err := doTxnRequest[OpenRelationResp](
		ctx,
		d.engine,
		d.txnOperator.Read,
		d.engine.firstNodeShard,
		OpOpenRelation,
		OpenRelationReq{
			DatabaseID: d.id,
			Name:       relName,
		},
	)
	if err != nil {
		return nil, err
	}

	resp := resps[0]
	if resp.ErrNotFound {
		return nil, fmt.Errorf("relation not found: %s", relName)
	}

	switch resp.Type {

	case RelationTable:
		table := &Table{
			engine:      d.engine,
			txnOperator: d.txnOperator,
			id:          resp.ID,
		}
		return table, nil

	default:
		panic(fmt.Errorf("unknown type: %+v", resp))
	}

}

func (d *Database) Relations(ctx context.Context) ([]string, error) {

	resps, err := doTxnRequest[GetRelationsResp](
		ctx,
		d.engine,
		d.txnOperator.Read,
		d.engine.firstNodeShard,
		OpGetRelations,
		GetRelationsReq{
			DatabaseID: d.id,
		},
	)
	if err != nil {
		return nil, err
	}

	var relNames []string
	for _, resp := range resps {
		relNames = append(relNames, resp.Names...)
	}

	return relNames, nil
}
