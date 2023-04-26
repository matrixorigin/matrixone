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

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
)

func (s *Storage) Write(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) (result []byte, err error) {

	switch op {

	case memoryengine.OpCreateDatabase:
		return handleWrite(
			ctx, txnMeta, payload,
			s.handler.HandleCreateDatabase,
		)

	case memoryengine.OpDeleteDatabase:
		return handleWrite(
			ctx, txnMeta, payload,
			s.handler.HandleDeleteDatabase,
		)

	case memoryengine.OpCreateRelation:
		return handleWrite(
			ctx, txnMeta, payload,
			s.handler.HandleCreateRelation,
		)

	case memoryengine.OpDeleteRelation:
		return handleWrite(
			ctx, txnMeta, payload,
			s.handler.HandleDeleteRelation,
		)

	case memoryengine.OpTruncateRelation:
		return handleWrite(
			ctx, txnMeta, payload,
			s.handler.HandleTruncateRelation,
		)

	case memoryengine.OpAddTableDef:
		return handleWrite(
			ctx, txnMeta, payload,
			s.handler.HandleAddTableDef,
		)

	case memoryengine.OpDelTableDef:
		return handleWrite(
			ctx, txnMeta, payload,
			s.handler.HandleDelTableDef,
		)

	case memoryengine.OpDelete:
		return handleWrite(
			ctx, txnMeta, payload,
			s.handler.HandleDelete,
		)

	//case memoryengine.OpTruncate:
	//	return handleWrite(
	//		ctx, txnMeta, payload,
	//		s.handler.HandleTruncate,
	//	)

	case memoryengine.OpUpdate:
		return handleWrite(
			ctx, txnMeta, payload,
			s.handler.HandleUpdate,
		)

	case memoryengine.OpWrite:
		return handleWrite(
			ctx, txnMeta, payload,
			s.handler.HandleWrite,
		)

	}

	return
}

func handleWrite[
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
	meta txn.TxnMeta,
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
	res []byte,
	err error,
) {

	var preq PReq = new(Req)
	if err := preq.UnmarshalBinary(payload); err != nil {
		return nil, err
	}

	var presp PResp = new(Resp)
	defer logReq("write", preq, meta, presp, &err)()

	err = fn(ctx, meta, preq, presp)
	if err != nil {
		return nil, err
	}

	data, err := presp.MarshalBinary()
	if err != nil {
		return nil, err
	}
	res = data

	return
}
