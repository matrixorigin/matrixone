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

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
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

	_, err := doTxnRequest(
		ctx,
		d.txnOperator.Write,
		d.engine.getDataNodes(),
		txn.TxnMethod_Write,
		OpCreateRelation,
		CreateRelationReq{
			DatabaseID: d.id,
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

	_, err := doTxnRequest(
		ctx,
		d.txnOperator.Write,
		d.engine.getDataNodes(),
		txn.TxnMethod_Write,
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

	resps, err := doTxnRequest(
		ctx,
		d.txnOperator.Read,
		d.engine.getDataNodes()[:1],
		txn.TxnMethod_Read,
		OpOpenRelation,
		OpenRelationReq{
			DatabaseID: d.id,
			Name:       relName,
		},
	)
	if err != nil {
		return nil, err
	}

	var resp OpenRelationResp
	if err := gob.NewDecoder(bytes.NewReader(resps[0])).Decode(&resp); err != nil {
		return nil, err
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
		panic("unknown type")
	}

}

func (d *Database) Relations(ctx context.Context) ([]string, error) {

	resps, err := doTxnRequest(
		ctx,
		d.txnOperator.Read,
		d.engine.getDataNodes()[:1],
		txn.TxnMethod_Read,
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
		var r GetRelationsResp
		if err := gob.NewDecoder(bytes.NewReader(resp)).Decode(&r); err != nil {
			return nil, err
		}
		relNames = append(relNames, r.Names...)
	}

	return relNames, nil
}
