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
	"strings"
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

func (count *counter) nonZero() bool {
	return count.exit.Load() > 0 || count.enter.Load() > 0
}

type fPrint [2]uint32

func (fp fPrint) String() string {
	return fmt.Sprintf("enter:%d exit:%d", fp[0], fp[1])
}
func (fp fPrint) nonZero() bool {
	return fp[0] != 0 || fp[1] != 0
}

type footPrints struct {
	prints [64]fPrint
}

func (fprints *footPrints) setFPrints(fp [][2]uint32) {
	flen := len(fp)
	if len(fprints.prints) < flen {
		flen = len(fprints.prints)
	}
	for i := 0; i < flen; i++ {
		fprints.prints[i] = fp[i]
	}
}

func (fprints *footPrints) String() string {
	strBuf := strings.Builder{}
	for i := 0; i < len(fprints.prints); i++ {
		if !fprints.prints[i].nonZero() {
			continue
		}
		strBuf.WriteString("[")
		strBuf.WriteString(fmt.Sprintf("%d", i))
		strBuf.WriteString(": ")
		strBuf.WriteString(fprints.prints[i].String())
		strBuf.WriteString("] ")
	}
	return strBuf.String()
}
