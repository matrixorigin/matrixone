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
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	taestorage "github.com/matrixorigin/matrixone/pkg/txn/storage/tae"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/rpc"
)

type TestTxnStorage struct {
	schema        *catalog.Schema
	taeHandler    *rpc.Handle
	logtailServer *TestLogtailServer
}

func (ts *TestTxnStorage) BindSchema(schema *catalog.Schema) {
	ts.schema = schema
}

func (ts *TestTxnStorage) GetDB() *db.DB {
	return ts.taeHandler.GetDB()
}

//func (ts *TestTxnStorage) Start() error { return nil }

//func (ts *TestTxnStorage) StartRecovery(context.Context, chan txn.TxnMeta) {}
//func (ts *TestTxnStorage) Close(context.Context) error                     { return nil }
//func (ts *TestTxnStorage) Destroy(context.Context) error                   { return nil }
//func (ts *TestTxnStorage) Read(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) (storage.ReadResult, error) {
//	return nil, nil
//}
//func (ts *TestTxnStorage) Write(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) ([]byte, error) {
//	switch op {
//	case uint32(apipb.OpCode_OpPreCommit):
//		return taestorage.HandleWrite(ctx, txnMeta, payload, ts.taeHandler.HandlePreCommitWrite)
//	default:
//		return nil, moerr.NewNotSupported(ctx, "unknown write op: %v", op)
//	}
//}
//func (ts *TestTxnStorage) Prepare(ctx context.Context, txnMeta txn.TxnMeta) (timestamp.Timestamp, error) {
//	return timestamp.Timestamp{}, nil
//}
//func (ts *TestTxnStorage) Committing(ctx context.Context, txnMeta txn.TxnMeta) error { return nil }
//func (ts *TestTxnStorage) Commit(ctx context.Context, txnMeta txn.TxnMeta) (timestamp.Timestamp, error) {
//	return ts.taeHandler.HandleCommit(ctx, txnMeta)
//}
//func (ts *TestTxnStorage) Rollback(ctx context.Context, txnMeta txn.TxnMeta) error { return nil }
//func (ts *TestTxnStorage) Debug(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) ([]byte, error) {
//	return nil, nil
//}

func (ts *TestTxnStorage) Shard() metadata.TNShard {
	return GetDefaultTNShard()
}

func (ts *TestTxnStorage) Start() error             { return nil }
func (ts *TestTxnStorage) Close(destroy bool) error { return nil }
func (ts *TestTxnStorage) Read(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}
func (ts *TestTxnStorage) Write(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) (err error) {
	req := request
	switch req.CNRequest.OpCode {
	case uint32(apipb.OpCode_OpPreCommit):
		_, err = taestorage.HandleWrite(ctx, req.Txn, req.CNRequest.Payload, ts.taeHandler.HandlePreCommitWrite)
		//response.TxnError = txn.WrapError(err, moerr.ErrTAEWrite)
	default:
		err = moerr.NewNotSupported(ctx, "unknown write op: %v", req.CNRequest.OpCode)
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

	_, err := ts.taeHandler.HandleCommit(ctx, request.Txn)
	if err == nil {
		response.Txn.Status = txn.TxnStatus_Committed
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

	blockio.Start()
	taeHandler := InitTestDB(ctx, moduleName, t, opts)
	logtailServer, err := NewMockLogtailServer(
		ctx, taeHandler.GetDB(), defaultLogtailConfig(), runtime.DefaultRuntime(), rpcAgent.MockLogtailPRCServerFactory)
	if err != nil {
		return nil, err
	}

	err = logtailServer.Start()
	if err != nil {
		return nil, err
	}

	tc := &TestTxnStorage{
		taeHandler:    taeHandler,
		logtailServer: logtailServer,
	}

	//go tc.txnRequestListener(ctx, rpcAgent.txnRequestChan, rpcAgent.txnResponseChan)

	return tc, nil

}

func InitTestDB(ctx context.Context, moduleName string, t *testing.T, opts *options.Options) *rpc.Handle {
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

//func (ts *TestTxnStorage) txnRequestListener(
//	ctx context.Context, txnRequestReceiver chan txn.TxnRequest, txnResponseSender chan txn.TxnResponse) {
//
//	sendResponse := func(resp *txn.TxnResponse) bool {
//		select {
//		case txnResponseSender <- *resp:
//			return true
//		default:
//			return false
//		}
//	}
//
//	for reqs := range txnRequestReceiver {
//		response := new(txn.TxnResponse)
//		req := reqs.CommitRequest.Payload[0]
//
//		_, err := ts.Write(ctx, req.Txn, req.CNRequest.OpCode, req.CNRequest.Payload)
//		if err != nil {
//			util.LogTxnWriteFailed(txn.TxnMeta{}, err)
//			response.TxnError = txn.WrapError(err, moerr.ErrTAEWrite)
//
//			if !sendResponse(response) {
//				logutil.Errorf("txnStorage.Write: send txn response failed: %v\n", response)
//				break
//			}
//		}
//
//		_, err = ts.Commit(ctx, req.Txn)
//		if err != nil {
//			response.TxnError = txn.WrapError(err, moerr.ErrTAECommit)
//		}
//
//		if !sendResponse(response) {
//			logutil.Errorf("txnStorage.Commit: send txn response failed: %v\n", response)
//		}
//
//		if err != nil {
//			break
//		}
//	}
//}
