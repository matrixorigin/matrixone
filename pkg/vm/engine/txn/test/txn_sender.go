// Copyright 2022 Matrix Origin
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

package testtxnengine

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

type Sender struct {
	env *testEnv
}

var _ rpc.TxnSender = new(Sender)

func (s *Sender) Send(ctx context.Context, reqs []txn.TxnRequest) (*rpc.SendResult, error) {
	result := &rpc.SendResult{}

	for _, req := range reqs {
		req := req

		targetDN := req.GetTargetDN()
		if targetDN.IsEmpty() {
			panic(fmt.Errorf("target DN not specified: %+v", req))
		}

		var node *Node
		for _, n := range s.env.nodes {
			if n.shard.Equal(targetDN) {
				node = n
				break
			}
		}
		if node == nil {
			return nil, fmt.Errorf("node not found")
		}

		fn := map[txn.TxnMethod]func(context.Context, *txn.TxnRequest, *txn.TxnResponse) error{
			txn.TxnMethod_Read:            node.service.Read,
			txn.TxnMethod_Write:           node.service.Write,
			txn.TxnMethod_Commit:          node.service.Commit,
			txn.TxnMethod_Rollback:        node.service.Rollback,
			txn.TxnMethod_Prepare:         node.service.Prepare,
			txn.TxnMethod_GetStatus:       node.service.GetStatus,
			txn.TxnMethod_CommitDNShard:   node.service.CommitDNShard,
			txn.TxnMethod_RollbackDNShard: node.service.RollbackDNShard,
		}[req.Method]

		var resp txn.TxnResponse
		if err := fn(ctx, &req, &resp); err != nil {
			return nil, err
		}
		resp.Txn = &req.Txn
		result.Responses = append(result.Responses, resp)

	}

	return result, nil
}

func (s *Sender) Close() error {
	return nil
}
