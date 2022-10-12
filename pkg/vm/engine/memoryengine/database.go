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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type Database struct {
	id          ID
	name        string
	engine      *Engine
	txnOperator client.TxnOperator
}

var _ engine.Database = new(Database)

func (d *Database) Create(ctx context.Context, relName string, defs []engine.TableDef) error {

	id, err := d.engine.idGenerator.NewID(ctx)
	if err != nil {
		return err
	}

	_, err = DoTxnRequest[CreateRelationResp](
		ctx,
		d.txnOperator,
		false,
		d.engine.allShards,
		OpCreateRelation,
		CreateRelationReq{
			ID:           id,
			DatabaseID:   d.id,
			DatabaseName: d.name,
			Type:         RelationTable,
			Name:         strings.ToLower(relName),
			Defs:         defs,
		},
	)
	if err != nil {
		return nil
	}

	return nil
}

func (d *Database) Delete(ctx context.Context, relName string) error {

	_, err := DoTxnRequest[DeleteRelationResp](
		ctx,
		d.txnOperator,
		false,
		d.engine.allShards,
		OpDeleteRelation,
		DeleteRelationReq{
			DatabaseID:   d.id,
			DatabaseName: d.name,
			Name:         strings.ToLower(relName),
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (d *Database) Relation(ctx context.Context, relName string) (engine.Relation, error) {

	if relName == "" {
		return nil, moerr.NewInvalidInput("no table name")
	}

	resps, err := DoTxnRequest[OpenRelationResp](
		ctx,
		d.txnOperator,
		true,
		d.engine.anyShard,
		OpOpenRelation,
		OpenRelationReq{
			DatabaseID:   d.id,
			DatabaseName: d.name,
			Name:         strings.ToLower(relName),
		},
	)
	if err != nil {
		return nil, err
	}

	resp := resps[0]

	switch resp.Type {

	case RelationTable, RelationView:
		table := &Table{
			engine:       d.engine,
			txnOperator:  d.txnOperator,
			id:           resp.ID,
			databaseName: resp.DatabaseName,
			tableName:    resp.RelationName,
		}
		return table, nil

	default:
		panic(moerr.NewInternalError("unknown type: %+v", resp.Type))
	}

}

func (d *Database) Relations(ctx context.Context) ([]string, error) {

	resps, err := DoTxnRequest[GetRelationsResp](
		ctx,
		d.txnOperator,
		true,
		d.engine.anyShard,
		OpGetRelations,
		GetRelationsReq{
			DatabaseID: d.id,
		},
	)
	if err != nil {
		return nil, err
	}

	return resps[0].Names, nil
}
