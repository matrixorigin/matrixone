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
	"bytes"
	"context"
	"encoding/gob"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func DoTxnRequest[
	Resp Response,
	Req Request,
](
	ctx context.Context,
	e engine.Engine,
	// TxnOperator.Read or TxnOperator.Write
	reqFunc func(context.Context, []txn.TxnRequest) (*rpc.SendResult, error),
	shardsFunc shardsFunc,
	op uint32,
	req Req,
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
	requests := make([]txn.TxnRequest, 0, len(shards))
	for _, shard := range shards {
		buf := new(bytes.Buffer)
		if err := gob.NewEncoder(buf).Encode(req); err != nil {
			panic(err)
		}
		requests = append(requests, txn.TxnRequest{
			CNRequest: &txn.CNOpRequest{
				OpCode:  op,
				Payload: buf.Bytes(),
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

	result, err := reqFunc(ctx, requests)
	if err != nil {
		return
	}
	for _, resp := range result.Responses {
		if resp.TxnError != nil {
			//TODO no way to construct moerr.Error by code and message now
			err = moerr.NewInternalError(resp.TxnError.Message)
			return
		}
	}

	for _, res := range result.Responses {
		var resp Resp
		if err = gob.NewDecoder(bytes.NewReader(res.CNOpResponse.Payload)).Decode(&resp); err != nil {
			return
		}
		resps = append(resps, resp)
	}

	return
}
