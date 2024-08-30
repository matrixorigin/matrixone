// Copyright 2024 Matrix Origin
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

package testutil

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	taestorage "github.com/matrixorigin/matrixone/pkg/txn/storage/tae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	dbutil "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/rpc"
)

type TestTxnStorage struct {
	t *testing.T
	//accountId     uint32
	schema        *catalog.Schema
	taeDelegate   *dbutil.TestEngine
	txnHandler    *rpc.Handle
	logtailServer *TestLogtailServer
}

func (ts *TestTxnStorage) BindSchema(schema *catalog.Schema) {
	ts.schema = schema
}

func (ts *TestTxnStorage) GetDB() *db.DB {
	return ts.txnHandler.GetDB()
}

func (ts *TestTxnStorage) Shard() metadata.TNShard {
	return GetDefaultTNShard()
}

func (ts *TestTxnStorage) Start() error { return nil }
func (ts *TestTxnStorage) Close(destroy bool) error {
	err := ts.GetDB().Close()
	return err
}
func (ts *TestTxnStorage) Read(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}
func (ts *TestTxnStorage) Write(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) (err error) {
	req := request
	switch req.CNRequest.OpCode {
	case uint32(apipb.OpCode_OpPreCommit):
		_, err = taestorage.HandleWrite(ctx, req.Txn, req.CNRequest.Payload, ts.txnHandler.HandlePreCommitWrite)
		//response.TxnError = txn.WrapError(err, moerr.ErrTAEWrite)
	default:
		err = moerr.NewNotSupportedf(ctx, "unknown write op: %v", req.CNRequest.OpCode)
		//response.TxnError = txn.WrapError(err, moerr.ErrTAEWrite)
	}
	return err
}

func (ts *TestTxnStorage) Commit(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	prepareResponse := func(req *txn.TxnRequest, resp *txn.TxnResponse) {
		resp.Method = req.Method
		resp.Flag = req.Flag
		resp.RequestID = req.RequestID
		resp.Txn = &req.Txn
	}

	if request.CommitRequest != nil {
		for _, req := range request.CommitRequest.Payload {
			//response is shared by all requests
			prepareResponse(req, response)
			err := ts.Write(ctx, req, response)
			if err != nil {
				return err
			}
		}
	}

	prepareResponse(request, response)

	cts, err := ts.txnHandler.HandleCommit(ctx, request.Txn)
	if err == nil {
		response.Txn.Status = txn.TxnStatus_Committed
		response.Txn.CommitTS = cts
	} else {
		response.Txn.Status = txn.TxnStatus_Aborted
	}

	return err
}

func (ts *TestTxnStorage) Rollback(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}
func (ts *TestTxnStorage) Prepare(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}
func (ts *TestTxnStorage) GetStatus(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}
func (ts *TestTxnStorage) CommitTNShard(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}
func (ts *TestTxnStorage) RollbackTNShard(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}
func (ts *TestTxnStorage) Debug(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}

func NewTestTAEEngine(
	ctx context.Context, moduleName string, t *testing.T,
	rpcAgent *MockRPCAgent, opts *options.Options) (*TestTxnStorage, error) {

	blockio.Start("")
	handle := InitTxnHandle(ctx, moduleName, t, opts)
	logtailServer, err := NewMockLogtailServer(
		ctx, handle.GetDB(), defaultLogtailConfig(), runtime.DefaultRuntime(), rpcAgent.MockLogtailPRCServerFactory)
	if err != nil {
		return nil, err
	}

	err = logtailServer.Start()
	if err != nil {
		return nil, err
	}

	tc := &TestTxnStorage{
		t:             t,
		txnHandler:    handle,
		logtailServer: logtailServer,
		taeDelegate: &dbutil.TestEngine{
			DB: handle.GetDB(), T: t,
		},
	}
	blockio.Start("")
	return tc, nil
}

func InitTxnHandle(ctx context.Context, moduleName string, t *testing.T, opts *options.Options) *rpc.Handle {
	dir := InitTestEnv(moduleName, t)
	handle := rpc.NewTAEHandle(ctx, dir, opts)
	handle.GetDB().DiskCleaner.GetCleaner().AddChecker(
		func(item any) bool {
			minTS := handle.GetDB().TxnMgr.MinTSForTest()
			ckp := item.(*checkpoint.CheckpointEntry)
			end := ckp.GetEnd()
			return !end.GreaterEq(&minTS)
		}, "testdb")

	return handle
}
