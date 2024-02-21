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
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// Engine is an engine.Engine impl
type Engine struct {
	shardPolicy ShardPolicy
	idGenerator IDGenerator
	cluster     clusterservice.MOCluster
}

func New(
	ctx context.Context,
	shardPolicy ShardPolicy,
	idGenerator IDGenerator,
	cluster clusterservice.MOCluster,
) *Engine {
	_ = ctx

	engine := &Engine{
		shardPolicy: shardPolicy,
		idGenerator: idGenerator,
		cluster:     cluster,
	}

	return engine
}

var _ engine.Engine = new(Engine)

func (e *Engine) New(_ context.Context, _ client.TxnOperator) error {
	return nil
}

func (e *Engine) Commit(_ context.Context, _ client.TxnOperator) error {
	return nil
}

func (e *Engine) Rollback(_ context.Context, _ client.TxnOperator) error {
	return nil
}

func (e *Engine) NewBlockReader(_ context.Context, _ int, _ timestamp.Timestamp,
	_ *plan.Expr, _ []byte, _ *plan.TableDef, _ any) ([]engine.Reader, error) {
	return nil, nil
}

func (e *Engine) Create(ctx context.Context, dbName string, txnOperator client.TxnOperator) error {

	id, err := e.idGenerator.NewID(ctx)
	if err != nil {
		return err
	}

	access, err := getAccessInfo(ctx)
	if err != nil {
		return err
	}
	_, err = DoTxnRequest[CreateDatabaseResp](
		ctx,
		txnOperator,
		false,
		e.allShards,
		OpCreateDatabase,
		&CreateDatabaseReq{
			ID:         id,
			AccessInfo: access,
			Name:       dbName,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) Database(ctx context.Context, dbName string, txnOperator client.TxnOperator) (engine.Database, error) {
	access, err := getAccessInfo(ctx)
	if err != nil {
		return nil, err
	}
	resps, err := DoTxnRequest[OpenDatabaseResp](
		ctx,
		txnOperator,
		true,
		e.anyShard,
		OpOpenDatabase,
		&OpenDatabaseReq{
			AccessInfo: access,
			Name:       dbName,
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
		name:        resp.Name,
		typ:         resp.DatTyp,
		createSql:   resp.CreateSql,
	}

	return db, nil
}

func (e *Engine) Databases(ctx context.Context, txnOperator client.TxnOperator) ([]string, error) {
	access, err := getAccessInfo(ctx)
	if err != nil {
		return nil, err
	}
	resps, err := DoTxnRequest[GetDatabasesResp](
		ctx,
		txnOperator,
		true,
		e.anyShard,
		OpGetDatabases,
		&GetDatabasesReq{
			AccessInfo: access,
		},
	)
	if err != nil {
		return nil, err
	}

	return resps[0].Names, nil
}

func (e *Engine) Delete(ctx context.Context, dbName string, txnOperator client.TxnOperator) error {
	access, err := getAccessInfo(ctx)
	if err != nil {
		return err
	}
	_, err = DoTxnRequest[DeleteDatabaseResp](
		ctx,
		txnOperator,
		false,
		e.allShards,
		OpDeleteDatabase,
		&DeleteDatabaseReq{
			AccessInfo: access,
			Name:       dbName,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) Nodes(isInternal bool, tenant string, _ string, cnLabel map[string]string) (engine.Nodes, error) {
	var nodes engine.Nodes
	cluster := clusterservice.GetMOCluster()
	var selector clusterservice.Selector
	if isInternal || strings.ToLower(tenant) == "sys" {
		selector = clusterservice.NewSelector()
	} else {
		selector = selector.SelectByLabel(cnLabel, clusterservice.EQ)
	}
	cluster.GetCNService(selector,
		func(c metadata.CNService) bool {
			nodes = append(nodes, engine.Node{
				Mcpu: 1,
				Id:   c.ServiceID,
				Addr: c.PipelineServiceAddress,
			})
			return true
		})
	return nodes, nil
}

func (e *Engine) Hints() (h engine.Hints) {
	h.CommitOrRollbackTimeout = time.Minute * 5
	return
}

func (e *Engine) GetNameById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, err error) {
	return "", "", moerr.NewNYI(ctx, "interface GetNameById is not implemented")
}

func (e *Engine) GetRelationById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, rel engine.Relation, err error) {
	return "", "", nil, moerr.NewNYI(ctx, "interface GetRelationById is not implemented")
}

func (e *Engine) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	id, err := e.idGenerator.NewIDByKey(ctx, key)
	return uint64(id), err
}

func (e *Engine) TryToSubscribeTable(ctx context.Context, dbID, tbID uint64) error {
	return nil
}

func (e *Engine) UnsubscribeTable(ctx context.Context, dbID, tbID uint64) error {
	return nil
}

func (e *Engine) Stats(ctx context.Context, key pb.StatsInfoKey, sync bool) *pb.StatsInfo {
	return nil
}

func getTNServices(cluster clusterservice.MOCluster) []metadata.TNService {
	var values []metadata.TNService
	cluster.GetTNService(clusterservice.NewSelector(),
		func(d metadata.TNService) bool {
			values = append(values, d)
			return true
		})
	return values
}
