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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

func (s *store) registerRPCHandlers() {
	// request from CN node
	s.server.RegisterMethodHandler(txn.TxnMethod_Read, s.handleRead)
	s.server.RegisterMethodHandler(txn.TxnMethod_Write, s.handleWrite)
	s.server.RegisterMethodHandler(txn.TxnMethod_Commit, s.handleCommit)
	s.server.RegisterMethodHandler(txn.TxnMethod_Rollback, s.handleRollback)

	// request from other DN node
	s.server.RegisterMethodHandler(txn.TxnMethod_Prepare, s.handlePrepare)
	s.server.RegisterMethodHandler(txn.TxnMethod_CommitDNShard, s.handleCommitDNShard)
	s.server.RegisterMethodHandler(txn.TxnMethod_RollbackDNShard, s.handleRollbackDNShard)
	s.server.RegisterMethodHandler(txn.TxnMethod_GetStatus, s.handleGetStatus)
}

func (s *store) dispatchLocalRequest(shard metadata.DNShard) rpc.TxnRequestHandleFunc {
	// DNShard not found, TxnSender will RPC call
	r := s.getReplica(shard.ShardID)
	if r == nil {
		return nil
	}
	return r.handleLocalRequest
}

func (s *store) handleRead(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r := s.validDNShard(request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()
	prepareResponse(request, response)
	return r.service.Read(ctx, request, response)
}

func (s *store) handleWrite(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r := s.validDNShard(request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()
	prepareResponse(request, response)
	return r.service.Write(ctx, request, response)
}

func (s *store) handleCommit(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r := s.validDNShard(request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()
	prepareResponse(request, response)
	return r.service.Commit(ctx, request, response)
}

func (s *store) handleRollback(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r := s.validDNShard(request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()
	prepareResponse(request, response)
	return r.service.Rollback(ctx, request, response)
}

func (s *store) handlePrepare(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r := s.validDNShard(request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()
	prepareResponse(request, response)
	return r.service.Prepare(ctx, request, response)
}

func (s *store) handleCommitDNShard(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r := s.validDNShard(request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()
	prepareResponse(request, response)
	return r.service.CommitDNShard(ctx, request, response)
}

func (s *store) handleRollbackDNShard(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r := s.validDNShard(request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()
	prepareResponse(request, response)
	return r.service.RollbackDNShard(ctx, request, response)
}

func (s *store) handleGetStatus(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	r := s.validDNShard(request, response)
	if r == nil {
		return nil
	}
	r.waitStarted()
	prepareResponse(request, response)
	return r.service.GetStatus(ctx, request, response)
}

func (s *store) validDNShard(request *txn.TxnRequest, response *txn.TxnResponse) *replica {
	shard := request.GetTargetDN()
	r := s.getReplica(shard.ShardID)
	if r == nil {
		s.mu.RLock()
		defer s.mu.RUnlock()
		response.TxnError = &txn.TxnError{
			Code: txn.ErrorCode_DNShardNotFound,
			Message: fmt.Sprintf("DNShard[%s] not found on DNStore[%s]",
				shard.DebugString(),
				s.mu.metadata.UUID),
		}
		return nil
	}
	return r
}

func prepareResponse(request *txn.TxnRequest, response *txn.TxnResponse) {
	response.Method = request.Method
	response.Flag = request.Flag
	response.RequestID = request.RequestID
}
