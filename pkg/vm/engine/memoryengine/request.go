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

package memoryengine

import (
	"context"
	"encoding"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

func DoTxnRequest[
	Resp any, PResp interface {
		// anonymous constraint
		encoding.BinaryUnmarshaler
		// make Resp convertible to its pointer type
		*Resp
	},
	Req any, PReq interface {
		// anonymous constraint
		encoding.BinaryMarshaler
		// make Req convertible to its pointer type
		*Req
	},
](
	ctx context.Context,
	txnOperator client.TxnOperator,
	isRead bool,
	shardsFunc shardsFunc,
	op uint32,
	preq PReq,
) (
	resps []Resp,
	err error,
) {

	if ctx == nil {
		panic("context should not be nil")
	}

	shards, err := shardsFunc()
	if err != nil {
		return nil, err
	}

	if provider, ok := txnOperator.(OperationHandlerProvider); ok {
		for _, shard := range shards {
			handler, meta := provider.GetOperationHandler(shard)
			var presp PResp = new(Resp)
			e := handle(ctx, handler, meta, shard, op, preq, presp)
			resps = append(resps, *presp)
			if e != nil {
				err = e
			}
		}
		return
	}

	requests := make([]txn.TxnRequest, 0, len(shards))
	for _, shard := range shards {
		buf, err := preq.MarshalBinary()
		if err != nil {
			panic(err)
		}
		requests = append(requests, txn.TxnRequest{
			CNRequest: &txn.CNOpRequest{
				OpCode:  op,
				Payload: buf,
				Target:  shard,
			},
			Options: &txn.TxnRequestOptions{
				RetryCodes: []int32{
					// dn shard not found
					int32(moerr.ErrDNShardNotFound),
				},
				RetryInterval: int64(time.Second),
			},
		})
	}

	ctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()

	var result *rpc.SendResult
	if isRead {
		result, err = txnOperator.Read(ctx, requests)
		if err != nil {
			return
		}
	} else {
		result, err = txnOperator.Write(ctx, requests)
		if err != nil {
			return
		}
	}
	for _, resp := range result.Responses {
		if resp.TxnError != nil {
			err = resp.TxnError.UnwrapError()
			return
		}
	}

	for _, res := range result.Responses {
		var presp PResp = new(Resp)
		err = presp.UnmarshalBinary(res.CNOpResponse.Payload)
		if err != nil {
			return
		}
		resps = append(resps, *presp)
	}

	return
}
