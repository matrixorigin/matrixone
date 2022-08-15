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

	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// Engine is an engine.Engine impl
type Engine struct {
	shardPolicy       ShardPolicy
	getClusterDetails GetClusterDetailsFunc
}

type GetClusterDetailsFunc = func() (logservicepb.ClusterDetails, error)

func New(
	ctx context.Context,
	shardPolicy ShardPolicy,
	getClusterDetails GetClusterDetailsFunc,
) *Engine {

	engine := &Engine{
		shardPolicy:       shardPolicy,
		getClusterDetails: getClusterDetails,
	}

	return engine
}

var _ engine.Engine = new(Engine)

func (e *Engine) Create(ctx context.Context, dbName string, txnOperator client.TxnOperator) error {
	txnOperator = ToOperator(txnOperator) //TODO remove this

	_, err := doTxnRequest[CreateDatabaseResp](
		ctx,
		e,
		txnOperator.Write,
		e.allNodesShards,
		OpCreateDatabase,
		CreateDatabaseReq{
			Name: dbName,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) Database(ctx context.Context, dbName string, txnOperator client.TxnOperator) (engine.Database, error) {
	txnOperator = ToOperator(txnOperator) //TODO remove this

	resps, err := doTxnRequest[OpenDatabaseResp](
		ctx,
		e,
		txnOperator.Read,
		e.firstNodeShard,
		OpOpenDatabase,
		OpenDatabaseReq{
			Name: dbName,
		},
	)
	if err != nil {
		return nil, err
	}

	resp := resps[0]

	db := &Database{
		engine:      e,
		txnOperator: txnOperator,
		id:          resp.ID,
	}

	return db, nil
}

func (e *Engine) Databases(ctx context.Context, txnOperator client.TxnOperator) ([]string, error) {
	txnOperator = ToOperator(txnOperator) //TODO remove this

	resps, err := doTxnRequest[GetDatabasesResp](
		ctx,
		e,
		txnOperator.Read,
		e.firstNodeShard,
		OpGetDatabases,
		GetDatabasesReq{},
	)
	if err != nil {
		return nil, err
	}

	var dbNames []string
	for _, resp := range resps {
		dbNames = append(dbNames, resp.Names...)
	}

	return dbNames, nil
}

func (e *Engine) Delete(ctx context.Context, dbName string, txnOperator client.TxnOperator) error {
	txnOperator = ToOperator(txnOperator) //TODO remove this

	_, err := doTxnRequest[DeleteDatabaseResp](
		ctx,
		e,
		txnOperator.Write,
		e.allNodesShards,
		OpDeleteDatabase,
		DeleteDatabaseReq{
			Name: dbName,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) Nodes() (engine.Nodes, error) {
	clusterDetails, err := e.getClusterDetails()
	if err != nil {
		return nil, err
	}

	var nodes engine.Nodes
	for _, node := range clusterDetails.CNNodes {
		nodes = append(nodes, engine.Node{
			Mcpu: 1,
			Id:   node.UUID,
			Addr: node.ServiceAddress,
		})
	}

	return nodes, nil
}
