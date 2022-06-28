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

package rpc

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

type sender struct {
	client morpc.RPCClient
}

func NewSender() *sender {
	return nil
}

func (s *sender) Send(ctx context.Context, requests []txn.TxnRequest) ([]txn.TxnResponse, error) {
	if len(requests) == 1 {
		resp, err := s.client.Send(ctx, requests[0].GetTargetDN().Address, &requests[0], morpc.SendOptions{})
	}

	responses := make([]txn.TxnResponse, len(requests))
	for idx, req := range requests {

	}
	return nil, nil
}
