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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// Engine is an engine.Engine impl
type Engine struct {

	// hakeeper
	hakeeperClient logservice.CNHAKeeperClient
	clusterDetails struct {
		sync.Mutex
		logservicepb.ClusterDetails
	}

	// shard
	shardPolicy ShardPolicy

	fatal func()
}

func New(
	ctx context.Context,
	hakeeperClient logservice.CNHAKeeperClient,
	shardPolicy ShardPolicy,
) *Engine {

	ctx, cancel := context.WithCancel(ctx)
	fatal := func() {
		cancel()
	}

	engine := &Engine{
		hakeeperClient: hakeeperClient,
		shardPolicy:    shardPolicy,
		fatal:          fatal,
	}
	go engine.startHAKeeperLoop(ctx)

	return engine
}

var _ engine.Engine = new(Engine)

func (e *Engine) Create(ctx context.Context, dbName string, txnOperator client.TxnOperator) error {

	_, err := doTxnRequest(
		ctx,
		txnOperator.Write,
		e.getDataNodes(),
		txn.TxnMethod_Write,
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

	resps, err := doTxnRequest(
		ctx,
		txnOperator.Read,
		e.getDataNodes()[:1],
		txn.TxnMethod_Read,
		OpOpenDatabase,
		OpenDatabaseReq{
			Name: dbName,
		},
	)
	if err != nil {
		return nil, err
	}

	var resp OpenDatabaseResp
	if err := gob.NewDecoder(bytes.NewReader(resps[0])).Decode(&resp); err != nil {
		return nil, err
	}

	db := &Database{
		engine:      e,
		txnOperator: txnOperator,
		id:          resp.ID,
	}

	return db, nil
}

func (e *Engine) Databases(ctx context.Context, txnOperator client.TxnOperator) ([]string, error) {

	resps, err := doTxnRequest(
		ctx,
		txnOperator.Read,
		e.getDataNodes()[:1],
		txn.TxnMethod_Read,
		OpGetDatabases,
		nil,
	)
	if err != nil {
		return nil, err
	}

	var dbNames []string
	for _, resp := range resps {
		var r GetDatabasesResp
		if err := gob.NewDecoder(bytes.NewReader(resp)).Decode(&r); err != nil {
			return nil, err
		}
		dbNames = append(dbNames, r.Names...)
	}

	return dbNames, nil
}

func (e *Engine) Delete(ctx context.Context, dbName string, txnOperator client.TxnOperator) error {

	_, err := doTxnRequest(
		ctx,
		txnOperator.Write,
		e.getDataNodes(),
		txn.TxnMethod_Write,
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

func (e *Engine) Nodes() engine.Nodes {

	var nodes engine.Nodes
	for _, node := range e.getComputeNodes() {
		nodes = append(nodes, engine.Node{
			Mcpu: 1,
			Id:   node.UUID,
			Addr: node.ServiceAddress,
		})
	}

	return nodes
}
