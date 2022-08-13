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

package txnstorage

import (
	"bytes"
	"context"
	"encoding/gob"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
)

func (s *Storage) Write(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) (result []byte, err error) {

	switch op {

	case txnengine.OpCreateDatabase:
		return handleWrite(
			s, txnMeta, payload,
			s.handler.HandleCreateDatabase,
		)

	case txnengine.OpDeleteDatabase:
		return handleWrite(
			s, txnMeta, payload,
			s.handler.HandleDeleteDatabase,
		)

	case txnengine.OpCreateRelation:
		return handleWrite(
			s, txnMeta, payload,
			s.handler.HandleCreateRelation,
		)

	case txnengine.OpDeleteRelation:
		return handleWrite(
			s, txnMeta, payload,
			s.handler.HandleDeleteRelation,
		)

	case txnengine.OpAddTableDef:
		return handleWrite(
			s, txnMeta, payload,
			s.handler.HandleAddTableDef,
		)

	case txnengine.OpDelTableDef:
		return handleWrite(
			s, txnMeta, payload,
			s.handler.HandleDelTableDef,
		)

	case txnengine.OpDelete:
		return handleWrite(
			s, txnMeta, payload,
			s.handler.HandleDelete,
		)

	case txnengine.OpTruncate:
		return handleWrite(
			s, txnMeta, payload,
			s.handler.HandleTruncate,
		)

	case txnengine.OpUpdate:
		return handleWrite(
			s, txnMeta, payload,
			s.handler.HandleUpdate,
		)

	case txnengine.OpWrite:
		return handleWrite(
			s, txnMeta, payload,
			s.handler.HandleWrite,
		)

	}

	return
}

func handleWrite[
	Req any,
	Resp any,
](
	s *Storage,
	meta txn.TxnMeta,
	payload []byte,
	fn func(
		meta txn.TxnMeta,
		req Req,
		resp *Resp,
	) (
		err error,
	),
) (
	res []byte,
	err error,
) {

	var req Req
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&req); err != nil {
		return nil, err
	}

	var resp Resp
	err = fn(meta, req, &resp)
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(resp); err != nil {
		return nil, err
	}
	res = buf.Bytes()

	return
}
