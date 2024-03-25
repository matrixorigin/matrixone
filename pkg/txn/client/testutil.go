// Copyright 2023 Matrix Origin
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

package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

// RunTxnTests runs txn tests.
func RunTxnTests(fn func(TxnClient, rpc.TxnSender), opts ...TxnClientCreateOption) {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	ts := newTestTxnSender()
	c := NewTxnClient(ts, opts...)
	c.Resume()
	fn(c, ts)
}

func newTestTxnSender() *testTxnSender {
	return &testTxnSender{auto: true}
}

type testTxnSender struct {
	sync.Mutex
	lastRequests []txn.TxnRequest
	auto         bool
	manualFunc   func(*rpc.SendResult, error) (*rpc.SendResult, error)
}

func (ts *testTxnSender) Close() error {
	return nil
}

func (ts *testTxnSender) Send(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
	ts.Lock()
	defer ts.Unlock()
	ts.lastRequests = requests

	responses := make([]txn.TxnResponse, 0, len(requests))
	for _, req := range requests {
		resp := txn.TxnResponse{
			Txn:    &req.Txn,
			Method: req.Method,
			Flag:   req.Flag,
		}
		switch resp.Method {
		case txn.TxnMethod_Read:
			resp.CNOpResponse = &txn.CNOpResponse{Payload: []byte(fmt.Sprintf("r-%d", req.CNRequest.OpCode))}
		case txn.TxnMethod_Write:
			resp.CNOpResponse = &txn.CNOpResponse{Payload: []byte(fmt.Sprintf("w-%d", req.CNRequest.OpCode))}
		case txn.TxnMethod_DEBUG:
			resp.CNOpResponse = &txn.CNOpResponse{Payload: req.CNRequest.Payload}
		case txn.TxnMethod_Rollback:
			resp.Txn.Status = txn.TxnStatus_Aborted
		case txn.TxnMethod_Commit:
			resp.Txn.CommitTS = resp.Txn.SnapshotTS.Next()
			resp.Txn.Status = txn.TxnStatus_Committed
		}

		responses = append(responses, resp)
	}

	result := &rpc.SendResult{Responses: responses}
	if !ts.auto {
		return ts.manualFunc(result, nil)
	}
	return result, nil
}

func (ts *testTxnSender) setManual(manualFunc func(*rpc.SendResult, error) (*rpc.SendResult, error)) {
	ts.Lock()
	defer ts.Unlock()
	ts.manualFunc = manualFunc
	ts.auto = false
}

func (ts *testTxnSender) getLastRequests() []txn.TxnRequest {
	ts.Lock()
	defer ts.Unlock()
	return ts.lastRequests
}

type counter struct {
	enter atomic.Uint64
	exit  atomic.Uint64
}

func (count *counter) addEnter() {
	count.enter.Add(1)
}

func (count *counter) addExit() {
	count.exit.Add(1)
}

func (count *counter) more() bool {
	return count.enter.Load() > count.exit.Load()
}

func (count *counter) String() string {
	return fmt.Sprintf("enter:%d, exit:%d", count.enter.Load(), count.exit.Load())
}
