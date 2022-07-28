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

package txnengine

import (
	"context"

	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

func doTxnRequest(
	ctx context.Context,
	reqFunc func(context.Context, []txn.TxnRequest) (*rpc.SendResult, error),
	nodes []logservicepb.DNNode,
	method txn.TxnMethod,
	op uint32,
	reqPayload any,
) (
	respPayloads [][]byte,
	err error,
) {

	requests := make([]txn.TxnRequest, 0, len(nodes))
	for _, node := range nodes {
		requests = append(requests, txn.TxnRequest{
			Method: method,
			CNRequest: &txn.CNOpRequest{
				OpCode:  op,
				Payload: mustEncodePayload(reqPayload),
				Target: metadata.DNShard{
					Address: node.ServiceAddress,
				},
			},
		})
	}

	result, err := reqFunc(ctx, requests)
	if err != nil {
		return nil, err
	}
	if err := errorFromTxnResponses(result.Responses); err != nil {
		return nil, err
	}

	for _, resp := range result.Responses {
		respPayloads = append(respPayloads, resp.CNOpResponse.Payload)
	}

	return
}
