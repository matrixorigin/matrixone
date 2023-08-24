// Copyright 2021 - 2022 Matrix Origin
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

package dnservice

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/service"
	"github.com/stretchr/testify/assert"
)

func TestNewReplica(t *testing.T) {
	r := newReplica(newTestDNShard(1, 2, 3), runtime.DefaultRuntime())
	select {
	case <-r.startedC:
		assert.Fail(t, "cannot started")
	default:
	}
}

func TestCloseNotStartedReplica(t *testing.T) {
	r := newReplica(newTestDNShard(1, 2, 3), runtime.DefaultRuntime())
	assert.NoError(t, r.close(false))
}

func TestWaitStarted(t *testing.T) {
	r := newReplica(newTestDNShard(1, 2, 3), runtime.DefaultRuntime())
	c := make(chan struct{})
	go func() {
		r.waitStarted()
		c <- struct{}{}
	}()

	ts := service.NewTestTxnService(t, 1, service.NewTestSender(), service.NewTestClock(1))
	defer func() {
		assert.NoError(t, ts.Close(false))
	}()

	assert.NoError(t, r.start(ts))
	defer func() {
		assert.NoError(t, r.close(false))
	}()
	select {
	case <-c:
	case <-time.After(time.Minute):
		assert.Fail(t, "wait started failed")
	}
}

func TestHandleLocalCNRequestsWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()

	r := newReplica(newTestDNShard(1, 2, 3), runtime.DefaultRuntime())
	ts := service.NewTestTxnService(t, 1, service.NewTestSender(), service.NewTestClock(1))
	defer func() {
		assert.NoError(t, ts.Close(false))
	}()

	assert.NoError(t, r.start(ts))
	defer func() {
		assert.NoError(t, r.close(false))
	}()

	req := service.NewTestReadRequest(1, txn.TxnMeta{}, 1)
	assert.NoError(t, r.handleLocalRequest(context.Background(), &req, &txn.TxnResponse{}))
}
