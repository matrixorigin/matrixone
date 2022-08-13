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
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
)

func (s *Storage) Read(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) (res storage.ReadResult, err error) {

	switch op {

	case txnengine.OpOpenDatabase:
		return handleRead(
			s, txnMeta, payload,
			s.handler.HandleOpenDatabase,
		)

	case txnengine.OpGetDatabases:
		return handleRead(
			s, txnMeta, payload,
			s.handler.HandleGetDatabases,
		)

	case txnengine.OpOpenRelation:
		return handleRead(
			s, txnMeta, payload,
			s.handler.HandleOpenRelation,
		)

	case txnengine.OpGetRelations:
		return handleRead(
			s, txnMeta, payload,
			s.handler.HandleGetRelations,
		)

	case txnengine.OpGetPrimaryKeys:
		return handleRead(
			s, txnMeta, payload,
			s.handler.HandleGetPrimaryKeys,
		)

	case txnengine.OpGetTableDefs:
		return handleRead(
			s, txnMeta, payload,
			s.handler.HandleGetTableDefs,
		)

	case txnengine.OpNewTableIter:
		return handleRead(
			s, txnMeta, payload,
			s.handler.HandleNewTableIter,
		)

	case txnengine.OpRead:
		return handleRead(
			s, txnMeta, payload,
			s.handler.HandleRead,
		)

	case txnengine.OpCloseTableIter:
		return handleRead(
			s, txnMeta, payload,
			s.handler.HandleCloseTableIter,
		)

	}

	return
}

func handleRead[Req any, Resp any](
	s *Storage,
	txnMeta txn.TxnMeta,
	payload []byte,
	fn func(
		meta txn.TxnMeta,
		req Req,
		resp *Resp,
	) (
		err error,
	),
) (
	res storage.ReadResult,
	err error,
) {

	var req Req
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&req); err != nil {
		return nil, err
	}

	var resp Resp
	err = fn(txnMeta, req, &resp)
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(resp); err != nil {
		return nil, err
	}
	res = &readResult{
		payload: buf.Bytes(),
	}

	return res, nil
}
