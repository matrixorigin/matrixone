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
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/trace"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

const (
	defaultRetryInterval = time.Millisecond * 100
)

func (s *store) registerRPCHandlers() {
	// request from CN node
	s.server.RegisterMethodHandler(txn.TxnMethod_Read, s.handleRead)
	s.server.RegisterMethodHandler(txn.TxnMethod_Write, s.handleWrite)
	s.server.RegisterMethodHandler(txn.TxnMethod_Commit, s.handleCommit)
	s.server.RegisterMethodHandler(txn.TxnMethod_Rollback, s.handleRollback)

	// request from other TN node
	s.server.RegisterMethodHandler(txn.TxnMethod_Prepare, s.handlePrepare)
	s.server.RegisterMethodHandler(txn.TxnMethod_CommitTNShard, s.handleCommitTNShard)
	s.server.RegisterMethodHandler(txn.TxnMethod_RollbackTNShard, s.handleRollbackTNShard)
	s.server.RegisterMethodHandler(txn.TxnMethod_GetStatus, s.handleGetStatus)

	// debug request
	s.server.RegisterMethodHandler(txn.TxnMethod_DEBUG, s.handleDebug)
}

func (s *store) dispatchLocalRequest(shard metadata.TNShard) rpc.TxnRequestHandleFunc {
	// DNShard not found, TxnSender will RPC call
	r := s.getReplica(shard.ShardID)
	if r == nil {
		return nil
	}
	return r.handleLocalRequest
}

func (s *store) handleRead(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return s.handleWithRetry(ctx, request, response, s.doRead)
}

func (s *store) handleWrite(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return s.handleWithRetry(ctx, request, response, s.doWrite)
}

func (s *store) handleDebug(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return s.handleWithRetry(ctx, request, response, s.doDebug)
}

func (s *store) doRead(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r := s.validTNShard(ctx, request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()

	prepareResponse(request, response)
	return r.service.Read(ctx, request, response)
}

func (s *store) doWrite(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r := s.validTNShard(ctx, request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()
	prepareResponse(request, response)
	return r.service.Write(ctx, request, response)
}

func (s *store) doDebug(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r := s.validTNShard(ctx, request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()

	prepareResponse(request, response)
	return r.service.Debug(ctx, request, response)
}

func (s *store) handleCommit(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	_, span := trace.Start(ctx, "store.handleCommit",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End()

	r := s.validTNShard(ctx, request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()
	if request.CommitRequest != nil {
		for _, req := range request.CommitRequest.Payload {
			//response is shared by all requests
			prepareResponse(req, response)
			err := s.handleWrite(ctx, req, response)
			if err != nil {
				return err
			}
		}
	}
	prepareResponse(request, response)
	return r.service.Commit(ctx, request, response)
}

func (s *store) handleRollback(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r := s.validTNShard(ctx, request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()
	prepareResponse(request, response)
	return r.service.Rollback(ctx, request, response)
}

func (s *store) handlePrepare(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r := s.validTNShard(ctx, request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()
	prepareResponse(request, response)
	return r.service.Prepare(ctx, request, response)
}

func (s *store) handleCommitTNShard(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r := s.validTNShard(ctx, request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()
	prepareResponse(request, response)
	return r.service.CommitTNShard(ctx, request, response)
}

func (s *store) handleRollbackTNShard(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r := s.validTNShard(ctx, request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()
	prepareResponse(request, response)
	return r.service.RollbackTNShard(ctx, request, response)
}

func (s *store) handleGetStatus(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r := s.validTNShard(ctx, request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()
	prepareResponse(request, response)
	return r.service.GetStatus(ctx, request, response)
}

func (s *store) validTNShard(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) *replica {
	shard := request.GetTargetTN()
	r := s.getReplica(shard.ShardID)
	if r == nil ||
		r.shard.GetReplicaID() != shard.GetReplicaID() {
		response.TxnError = txn.WrapError(moerr.NewTNShardNotFound(ctx, s.cfg.UUID, shard.ShardID), 0)
		return nil
	}
	return r
}

func prepareResponse(request *txn.TxnRequest, response *txn.TxnResponse) {
	response.Method = request.Method
	response.Flag = request.Flag
	response.RequestID = request.RequestID
}

func (s *store) handleWithRetry(ctx context.Context,
	request *txn.TxnRequest,
	response *txn.TxnResponse,
	delegate rpc.TxnRequestHandleFunc) error {
	for {
		response.Reset()
		err := delegate(ctx, request, response)
		if err != nil {
			return err
		}

		if !s.maybeRetry(ctx, request, response) {
			return nil
		}
	}
}

func (s *store) maybeRetry(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) bool {
	if response.TxnError == nil {
		return false
	}
	if request.Options == nil {
		return false
	}
	if len(request.Options.RetryCodes) == 0 {
		return false
	}

	select {
	case <-ctx.Done():
		return false
	default:
		for _, code := range request.Options.RetryCodes {
			if code == int32(response.TxnError.TxnErrCode) {
				wait := time.Duration(request.Options.RetryInterval)
				if wait == 0 {
					wait = defaultRetryInterval
				}
				time.Sleep(wait)
				return true
			}
		}
		return false
	}
}
