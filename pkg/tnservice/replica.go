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

package tnservice

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/service"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
)

// replica tn shard replica.
type replica struct {
	rt       runtime.Runtime
	logger   *log.MOLogger
	shard    metadata.TNShard
	service  service.TxnService
	startedC chan struct{}

	mu struct {
		sync.RWMutex
		starting bool
	}
}

func newReplica(shard metadata.TNShard, rt runtime.Runtime) *replica {
	return &replica{
		rt:       rt,
		shard:    shard,
		logger:   rt.Logger().With(util.TxnTNShardField(shard)),
		startedC: make(chan struct{}),
	}
}

func (r *replica) start(txnService service.TxnService) error {
	r.mu.Lock()
	if r.mu.starting {
		r.mu.Unlock()
		return nil
	}
	r.mu.starting = true
	r.mu.Unlock()

	defer close(r.startedC)
	r.service = txnService
	return r.service.Start()
}

func (r *replica) close(destroy bool) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.mu.starting {
		return nil
	}
	r.waitStarted()
	return r.service.Close(destroy)
}

func (r *replica) handleLocalRequest(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r.waitStarted()
	prepareResponse(request, response)

	switch request.Method {
	case txn.TxnMethod_GetStatus:
		return r.service.GetStatus(ctx, request, response)
	case txn.TxnMethod_Prepare:
		return r.service.Prepare(ctx, request, response)
	case txn.TxnMethod_CommitTNShard:
		return r.service.CommitTNShard(ctx, request, response)
	case txn.TxnMethod_RollbackTNShard:
		return r.service.RollbackTNShard(ctx, request, response)
	default:
		return moerr.NewNotSupportedf(ctx, "unknown txn request method: %s", request.Method.String())
	}
}

func (r *replica) waitStarted() {
	<-r.startedC
}
