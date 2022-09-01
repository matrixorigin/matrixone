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

package engine

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"go.uber.org/zap"
)

type Shard = metadata.DNShard

func DoTxnRequest[
	Resp any,
	Req any,
](
	ctx context.Context,
	e Engine,
	// TxnOperator.Read or TxnOperator.Write
	reqFunc func(context.Context, []txn.TxnRequest) (*rpc.SendResult, error),
	shardsFunc func() ([]Shard, error),
	op uint32,
	req Req,
) (
	resps []Resp,
	err error,
) {

	logutil.Debug("engine: CN request",
		zap.String("type", fmt.Sprintf("%T", req)),
		zap.Any("data", req),
	)
	defer func() {
		for _, resp := range resps {
			logutil.Debug("engine: CN response",
				zap.String("type", fmt.Sprintf("%T", resp)),
				zap.Any("data", resp),
			)
		}
		if err != nil {
			logutil.Debug("engine: CN error",
				zap.Any("error", err),
			)
		}
	}()

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
		logutil.Debug("engine: CN target shard",
			zap.Any("shard", shard),
		)
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

var errorType = reflect.TypeOf((*error)(nil)).Elem()

type TxnError struct {
	txnError *txn.TxnError
}

var _ error = TxnError{}

func (e TxnError) Error() string {
	if e.txnError != nil {
		return e.txnError.DebugString()
	}
	panic("impossible")
}

func errorFromTxnResponses(resps []txn.TxnResponse) error {
	for _, resp := range resps {
		if resp.TxnError != nil {
			return TxnError{
				txnError: resp.TxnError,
			}
		}
	}
	return nil
}

type Errors []error

var _ error = Errors{}

func (e Errors) Error() string {
	buf := new(strings.Builder)
	for i, err := range e {
		if i > 0 {
			buf.WriteRune('\n')
		}
		buf.WriteString(err.Error())
	}
	return buf.String()
}

func (e Errors) As(target any) bool {
	for _, err := range e {
		if errors.As(err, target) {
			return true
		}
	}
	return false
}
