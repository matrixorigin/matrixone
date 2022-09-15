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
	"reflect"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func DoTxnRequest[
	Resp any,
	Req any,
](
	ctx context.Context,
	e engine.Engine,
	// TxnOperator.Read or TxnOperator.Write
	reqFunc func(context.Context, []txn.TxnRequest) (*rpc.SendResult, error),
	shardsFunc func() ([]Shard, error),
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
				RetryCodes: []txn.ErrorCode{
					// dn shard not found
					txn.ErrorCode_DNShardNotFound,
				},
				RetryInterval: int64(time.Second),
			},
		})
	}

	ctx, cancel := context.WithTimeout(ctx, time.Minute) //TODO get from config or argument
	defer cancel()

	result, err := reqFunc(ctx, requests)
	if err != nil {
		return
	}
	if err = errorFromTxnResponses(result.Responses); err != nil {
		return
	}

	var respErrors Errors
	for _, res := range result.Responses {
		var resp Resp
		if err = gob.NewDecoder(bytes.NewReader(res.CNOpResponse.Payload)).Decode(&resp); err != nil {
			return
		}

		respValue := reflect.ValueOf(resp)
		for i := 0; i < respValue.NumField(); i++ {
			field := respValue.Field(i)
			if field.Type().Implements(errorType) &&
				!field.IsZero() {
				respErrors = append(respErrors, field.Interface().(error))
			}
		}

		resps = append(resps, resp)
	}

	if len(respErrors) > 0 {
		err = respErrors
	}

	return
}
