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

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/service"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"go.uber.org/zap"
)

// replica dn shard replica.
type replica struct {
	logger   *zap.Logger
	shard    metadata.DNShard
	service  service.TxnService
	startedC chan struct{}
}

func newReplica(shard metadata.DNShard, logger *zap.Logger) *replica {
	return &replica{
		shard:    shard,
		logger:   logutil.Adjust(logger).With(util.TxnDNShardField(shard)),
		startedC: make(chan struct{}),
	}
}

func (r *replica) start(txnService service.TxnService) error {
	defer close(r.startedC)
	r.service = txnService
	return r.service.Start()
}

func (r *replica) close(destroy bool) error {
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
	case txn.TxnMethod_CommitDNShard:
		return r.service.CommitDNShard(ctx, request, response)
	case txn.TxnMethod_RollbackDNShard:
		return r.service.RollbackDNShard(ctx, request, response)
	default:
		panic("cannot handle local CN request")
	}
}

func (r *replica) waitStarted() {
	<-r.startedC
}
