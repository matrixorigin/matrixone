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

package memorystorage

import (
	"context"
	"encoding"
	"io"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
)

func (s *Storage) Read(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) (res storage.ReadResult, err error) {

	switch op {

	case memoryengine.OpOpenDatabase:
		return handleRead(
			ctx, txnMeta, payload,
			s.handler.HandleOpenDatabase,
		)

	case memoryengine.OpGetDatabases:
		return handleRead(
			ctx, txnMeta, payload,
			s.handler.HandleGetDatabases,
		)

	case memoryengine.OpOpenRelation:
		return handleRead(
			ctx, txnMeta, payload,
			s.handler.HandleOpenRelation,
		)

	case memoryengine.OpGetRelations:
		return handleRead(
			ctx, txnMeta, payload,
			s.handler.HandleGetRelations,
		)

	case memoryengine.OpGetPrimaryKeys:
		return handleRead(
			ctx, txnMeta, payload,
			s.handler.HandleGetPrimaryKeys,
		)

	case memoryengine.OpGetTableDefs:
		return handleRead(
			ctx, txnMeta, payload,
			s.handler.HandleGetTableDefs,
		)

	case memoryengine.OpGetHiddenKeys:
		return handleRead(
			ctx, txnMeta, payload,
			s.handler.HandleGetHiddenKeys,
		)

	case memoryengine.OpNewTableIter:
		return handleRead(
			ctx, txnMeta, payload,
			s.handler.HandleNewTableIter,
		)

	case memoryengine.OpRead:
		return handleRead(
			ctx, txnMeta, payload,
			s.handler.HandleRead,
		)

	case memoryengine.OpCloseTableIter:
		return handleRead(
			ctx, txnMeta, payload,
			s.handler.HandleCloseTableIter,
		)

	case memoryengine.OpTableStats:
		return handleRead(
			ctx, txnMeta, payload,
			s.handler.HandleTableStats,
		)

	case memoryengine.OpGetTableColumns:
		return handleRead(
			ctx, txnMeta, payload,
			s.handler.HandleGetTableColumns,
		)

	}

	return
}

func handleRead[
	Req any, PReq interface {
		// anonymous constraint
		encoding.BinaryUnmarshaler
		// make Req convertible to its pointer type
		*Req
	},
	Resp any, PResp interface {
		encoding.BinaryMarshaler
		*Resp
	},
](
	ctx context.Context,
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
	if err := preq.UnmarshalBinary(payload); err != nil {
		return nil, err
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
