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

package taestorage

import (
	"context"
	"encoding"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
)

// Read implements storage.TxnTAEStorage

func (s *taeStorage) Read(
	ctx context.Context,
	txnMeta txn.TxnMeta,
	op uint32,
	payload []byte) (res storage.ReadResult, err error) {

	switch op {

	case uint32(apipb.OpCode_OpGetLogTail):
		return handleRead(
			ctx, s,
			txnMeta, payload,
			s.taeHandler.HandleGetLogTail,
		)
	default:
		panic(moerr.NewInfoNoCtx("op is not supported"))
	}

}

func handleRead[
	Req any, PReq interface {
		// anonymous constraint
		encoding.BinaryUnmarshaler
		// make Req convertible to its pointer type
		*Req
	},
	Resp any, PResp interface {
		// anonymous constraint
		encoding.BinaryMarshaler
		// make Resp convertible to its pointer type
		*Resp
	},
](
	ctx context.Context,
	s *taeStorage,
	txnMeta txn.TxnMeta,
	payload []byte,
	fn func(
		ctx context.Context,
		meta txn.TxnMeta,
		preq PReq,
		presp PResp,
	) (
		err error,
	),
) (
	res storage.ReadResult,
	err error,
) {

	var preq PReq = new(Req)
	if len(payload) != 0 {
		if err := preq.UnmarshalBinary(payload); err != nil {
			return nil, err
		}
	}

	var presp PResp = new(Resp)
	defer logReq("read", preq, txnMeta, presp, &err)()
	defer func() {
		if closer, ok := (any)(presp).(io.Closer); ok {
			_ = closer.Close()
		}
	}()

	err = fn(ctx, txnMeta, preq, presp)
	if err != nil {
		return nil, err
	}

	data, err := presp.MarshalBinary()
	if err != nil {
		return nil, err
	}
	res = &readResult{
		payload: data,
	}

	return res, nil
}
