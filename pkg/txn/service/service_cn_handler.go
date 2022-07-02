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

package service

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

func (s *service) Read(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	s.validDNShard(request)
	s.checkCNRequest(request)
	return nil
}

func (s *service) Write(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	s.validDNShard(request)
	s.checkCNRequest(request)
	return nil
}

func (s *service) Commit(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}

func (s *service) Rollback(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}

func (s *service) checkCNRequest(request *txn.TxnRequest) {
	if request.CNRequest == nil {
		s.cfg.Logger.Fatal("missing CNRequest")
	}
}
